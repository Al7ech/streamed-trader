# StreamedTrader

An event-driven backtester and live trader for Binance USD-M Futures, with a browser UI for
inspecting results.

The point of the design is that **a strategy is written once and runs unchanged in both places**.
The backtester and the live trader drive the same `BaseStreamer` object, feed it the same `Candle`
objects in the same order, and account for positions with the same `Status` math — so what you
backtest is what you trade.

```
data.binance.vision ─┐
                     ├─► month-chunked pickle cache ─► backtester ─► run JSON + series shards ─► visualiser
Binance REST/WS ─────┘                                     │
Massive (US equities) ┘                                    └─► live trader ─► Binance Futures orders
```

- `core/` — Python: candle fetching/caching, backtesting, the strategy framework, the live trader.
- `visualise/` — React app that reads the backtester's JSON output straight off disk. No backend.

## Quickstart

```bash
uv sync
uv run python core/examples/backtest.py "my first run"
```

That downloads ETHUSDT 1m candles for 2026-H1 from data.binance.vision (cached under
`asset/candle/`), replays them through `KeltnerStreamer`, prints a summary, and writes the run to
`asset/backtest/`.

Then look at it:

```bash
cd visualise
npm install
npm start          # http://localhost:3000
```

Click **asset 폴더 선택** and pick the repo's `asset/` directory. The app lists every run and
renders candles, indicators, trade markers, an equity curve and monthly stats.

> **`core` is a real, `uv`-installed package.** `uv sync` editable-installs it, so modules import
> their siblings with the full path (`from core.streamer.candle import Candle`) and resolve the
> same way regardless of cwd. Run scripts from the repo root with `uv run python core/<script>.py`.
> Default output paths (`asset/`) are relative to the cwd, so run from the repo root, not from
> inside `core/`.

## Writing a strategy

Subclass `BaseStreamer`, declare your indicators, and implement `decide_action`:

```python
from core.backtest.status import Status
from core.streamer.action import Action
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.streamer.indicator.moving_average import MovingAverage


class MyStreamer(BaseStreamer):
    def __init__(self, symbol: str):
        super().__init__({"fast": MovingAverage(10), "slow": MovingAverage(60)})
        self.symbol = symbol

    def decide_action(self, candle: Candle, status: Status) -> Action:
        fast = self.indicators["fast"].get_latest()
        slow = self.indicators["slow"].get_latest()
        if fast is None or slow is None:      # warm-up
            return Action(0)
        if status.position == 0 and fast > slow:
            return Action(status.total_margin() / candle.close)
        if status.position > 0 and fast < slow:
            return Action(-status.position)   # close
        return Action(0)
```

Two rules matter:

1. **`Action.quantity` is a signed delta, not a target.** Positive buys, negative sells.
   `Action(-status.position)` closes; `Action(-2 * status.position)` flips.
2. **Indicators exclude the candle you are deciding on — by default.** The engine calls
   `decide_action` on a closed candle *first*, then updates the indicators with it. `get_latest()`
   is the value through the previous candle, `get_index(-2)` the one before that — while the
   current candle's OHLCV is available directly as the `candle` argument.

   This is a semantic convention, not a look-ahead guard: the candle has already closed, so
   including it would leak nothing. The reason to exclude it is that breakout comparisons need it.
   For a Donchian max channel that included the current bar you'd have
   `channel_max >= candle.high >= candle.close`, so `close > channel_max` could never fire.
   "Price broke out of the range formed by *prior* bars" is the definition, not a precaution.
   The convention is identical live and in backtest, so `get_latest()` never means two things.

   **Opting out per indicator.** Set `updates_before_decide = True` and the engine feeds that
   indicator the current candle *before* `decide_action`, so `get_latest()` includes the candle
   being decided on and `get_index(-2)` is the previous one:

   ```python
   class MyIndicator(BaseIndicator):
       updates_before_decide = True
   ```

   It also works per instance — `streamer.indicators["ATR"].updates_before_decide = True` — and
   the two kinds mix freely inside one strategy. Every indicator shipped here leaves it `False`,
   so this changes nothing unless you ask for it.

   If you turn it on, every read shifts by one index. Breakout comparisons in particular must move
   to `get_index(-2)`, for the reason above. Level-style indicators (MA, ATR, rolling std) read
   fine at `get_latest()` either way.

Point `core/examples/backtest.py` at your class and run it.

### Indicators

`BaseIndicator` is a rolling-window indicator updated one candle at a time — implement `update()`
and `get_index()`. That is all you need for correctness everywhere.

`VectorizedIndicator` additionally implements `precompute_series(open, high, low, close, volume)`,
returning the whole series as a numpy array (element *i* = `get_latest()` after *i+1* updates, NaN
during warm-up). `FastBacktester` computes those in one shot instead of looping, which is where
most of its speedup comes from. `update()` must still work — the live trader has no future to
precompute.

An indicator that reads `status` **cannot** be vectorized: account state depends on the trades the
strategy makes, which is a feedback loop. Keep those as plain `BaseIndicator`
(`indicator/position_age.py` is the example). The two kinds mix freely in one strategy.

Two class attributes tune an indicator, both settable per subclass or per instance:

| Attribute | Default | Effect |
|---|---|---|
| `scale_group` | `"price"` | Chart pane. `"price"` overlays the candles; any other value gets its own pane, shared by indicators with the same group. |
| `updates_before_decide` | `False` | When the current candle is ingested. `False` = after `decide_action` (`get_latest()` excludes it); `True` = before (`get_latest()` includes it). See rule 2 above. |

So there are three independent choices per indicator: which pane it draws on, which side of the
decision it ingests the candle on, and whether it is vectorized.

### Bundled examples

| Strategy | Idea |
|---|---|
| `CrossMovingAverageStreamer` | MA10/MA25 cross, always-in. Start here. |
| `KeltnerStreamer` | ATR channel breakout, volatility-adaptive width. Used by `core/examples/backtest.py`/`trader.py`. |
| `SupertrendStreamer` | Supertrend flip, always-in; the trailing line is the exit. |
| `MeanReversionZScoreStreamer` | Fade ±z-score deviations; stateful (timeout + stop carried on the instance). |
| `MomentumTimeExitStreamer` | Short-horizon momentum with a fixed-time exit. |
| `VolumeConfirmedMomentumStreamer` | The above, gated on a volume z-score — subclassing to add an entry filter. |
| `WickRejectionStreamer` | Long rejection-wick reversal; a candlestick-pattern strategy. |
| `TrendlineBounceStreamer` | Pivot trendline touches; a geometric, non-indicator signal. |
| `TradeStreamer` | Alternates long/short every bar. A test fixture, not a strategy. |

They are illustrations of the framework, not recommendations. None of them has a validated edge,
and most of them lose money after fees.

**On the default params and leverage.** The example strategies size positions by a `max_loss`
budget at the stop distance: `leverage = min(6, max_loss * price / stop_distance)`. On 1m candles
the ATR-based stop distance is tiny, so anything like `max_loss=0.08` pins leverage at the 6x cap
permanently. At 6x with 4 bps fees, a single round trip costs ~0.5% of equity — a few hundred
trades and the account is gone regardless of whether the signal was any good. `core/examples/backtest.py`
ships with `max_loss=0.005` (≈1.6x leverage) so the quickstart shows a legible equity curve instead of a
fee-driven wipeout. Those params were picked for legibility on the demo window, in-sample; treat
the resulting numbers as a smoke test, not as evidence.

## Backtesting

```python
from core.backtest.FastBacktester import FastBacktester

backtester = FastBacktester(streamer, candles, fee_ratio=0.0004)
report = backtester.run(metadata={...}, save_series=True)
```

`Report` gives you `trades`, `max_leverage`, the final `Status` and the equity curve. Passing
`metadata` writes `asset/backtest/<run_id>.json` (summary, Sharpe, max drawdown, trade list);
adding `save_series=True` also writes month-bucketed `<run_id>.<YYYY-MM>.series.json` shards with
per-candle OHLC and indicator values, which the frontend loads lazily per viewport.

`SingleThreadedBacktester` is the plain reference implementation. `FastBacktester` is a drop-in
subclass — same trades, same output, roughly 2x faster loop and 5x faster series writing.
`backtest_fast_check.py` asserts the two produce byte-identical results and is the closest thing
this repo has to a test suite:

```bash
uv run python core/backtest_fast_check.py
```

The backtester also force-liquidates a position when `margin <= unrealised_pnl`, so blown-up
strategies show up as blow-ups rather than as impossible recoveries.

## Data sources

- **`BinanceVisionFetcher`** (default) — bulk-downloads monthly/daily zips from
  data.binance.vision. Fast for long histories.
- **`BinanceCandleFetcher`** — the REST klines API in ≤1500-candle pages. Used by the live trader
  to prefeed indicators.
- **`MassiveStockFetcher`** — US equity minute bars from Massive (formerly Polygon.io), needs
  `MASSIVE_API_KEY`. Session-aware: `nyse_session` builds the NYSE trading-hours grid so a
  1200-candle window means the same wall-clock span on every symbol instead of silently spanning
  overnight gaps. `uv run python core/fetch_stock_check.py AAPL` smoke-tests it.

All of them share `BaseCandleFetcher.get_candles_with_cache`, which caches per **month** under
`asset/candle/<symbol>_<interval>/<YYYY-MM>.pkl`. Only missing months are fetched; completed
months are immutable; the in-progress month is kept as `.partial.pkl` holding closed candles only
and delta-fetched from its last candle on each run.

## Live trading

Copy `.env.sample` to `.env`, fill in your Binance keys, and:

```bash
uv run streamed-trader
```

> **`DRY_RUN=true` is the default and you should leave it there until you have watched a strategy
> run.** In dry run no orders are sent, no user-data socket is opened, and margin starts at a
> synthetic `1e6` with fills simulated locally. With `DRY_RUN=false` this sends real market orders
> against real money at whatever leverage your strategy asks for. Nothing here is financial advice
> and there is no warranty — read `core/trader/README.md` before you flip it.

Docker runs the trader continuously:

```bash
docker compose up -d --build
```

## Repository layout

```
pyproject.toml                 package metadata + deps (uv-managed), builds the `core` package
core/
  examples/                    backtest.py/trader.py entry points (edit for symbol/date/strategy)
  backtest_fast_check.py      fast-vs-reference parity check
  fetch_stock_check.py        US equity data smoke test
  backtest/                   Status/Trade/Report, the two backtesters, metrics, result writer
  streamer/                   BaseStreamer, Candle, Action + example strategies
    indicator/                MA, ATR, Donchian, ADX, Supertrend, rolling std, volume stats, ...
  candle_fetcher/             BaseCandleFetcher + month-chunk pickle cache
  binance_candle_fetcher/     Binance REST / data.binance.vision / funding / OI + long-short metrics
  stock_candle_fetcher/       Massive US equities + NYSE session calendar
  trader/                     BinanceTrader (asyncio + websockets), BinanceExecutor, ReliableWebsocket
  utils/                      timestamp and rounding helpers
visualise/                    React viewer for asset/backtest/*.json
asset/                        gitignored: candle cache + backtest output
```

## Requirements

[`uv`](https://docs.astral.sh/uv/) (manages the Python 3.13+ interpreter and dependencies for you —
`uv sync` will download a matching Python if none is installed) and Node 18+ for the frontend.
