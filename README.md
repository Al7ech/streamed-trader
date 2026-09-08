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
> their siblings with the full path (`from core.candle.candle import Candle`) and resolve the
> same way regardless of cwd. Run scripts from the repo root with `uv run python core/<script>.py`.
> Default output paths (`asset/`) are relative to the cwd, so run from the repo root, not from
> inside `core/`.

## Writing a strategy

Subclass `BaseStreamer`, declare your indicators, and implement `decide_action`:

```python
from typing import Dict, List

from core.order.action import Action
from core.candle.candle import Candle
from core.account.status import Status
from core.streamer.base_streamer import BaseStreamer
from core.streamer.indicator.moving_average import MovingAverage


class MyStreamer(BaseStreamer):
    def __init__(self, symbol: str):
        super().__init__([symbol], {symbol: {
            "fast": MovingAverage(10),
            "slow": MovingAverage(60),
        }})
        self.symbol = symbol

    def decide_action(self, candles: Dict[str, Candle], status: Status) -> List[Action]:
        candle = candles.get(self.symbol)
        if candle is None:                     # this symbol didn't close this event
            return []
        ind = self.indicators[self.symbol]
        fast = ind["fast"].get_latest()
        slow = ind["slow"].get_latest()
        if fast is None or slow is None:       # warm-up
            return []
        position = status.position_for(self.symbol).position
        if position == 0 and fast > slow:
            return [Action(self.symbol, status.total_margin() / candle.close)]
        if position > 0 and fast < slow:
            return [Action(self.symbol, -position)]   # close
        return []
```

Two rules matter:

1. **`Action.quantity` is a signed delta, not a target.** Positive buys, negative sells.
   `Action(sym, -position)` closes; `Action(sym, -2 * position)` flips.
2. **`decide_action` is called once per event** with `{symbol: candle}` for every symbol whose
   candle just closed (live: always one entry; a merged backtest event: one or more). Every
   traded symbol's indicators are already updated for that event, so a strategy can read and act
   on other symbols too — each `Action` carries its own `.symbol`. The engine updates every
   indicator with its closed candle *first*, then calls `decide_action`. `get_latest()` is the
   value through the candle being decided on, `get_index(-2)` the one before that — the current
   candle's OHLCV is also available directly from the `candles` dict.

   This is a semantic convention, not a look-ahead guard: the candle has already closed, so
   including it leaks nothing. It does mean breakout comparisons need `get_index(-2)`, not
   `get_latest()`: for a Donchian max channel that included the current bar you'd have
   `channel_max >= candle.high >= candle.close`, so `close > channel_max` could never fire.
   "Price broke out of the range formed by *prior* bars" is the definition, not a precaution.
   Level-style indicators (MA, ATR, rolling std) read fine at `get_latest()` either way. The
   convention is identical live and in backtest, so `get_latest()` never means two things.

Point `core/examples/backtest.py` at your class and run it.

### Indicators

`BaseIndicator` is a rolling-window indicator updated one candle at a time — implement `update()`
and `get_index()`. That is all you need for correctness everywhere.

An indicator can additionally implement `precompute_series(open, high, low, close, volume)`,
returning the whole series as a numpy array (element *i* = `get_latest()` after *i+1* updates, NaN
during warm-up). A vectorized backtest path detects the method by attribute presence
(`getattr(indicator, "precompute_series", None)`) and computes those in one shot instead of
looping. That path is currently removed pending reintroduction on the new candle-producer
structure, but the method is kept — and `update()` must still work regardless, since the live
trader has no future to precompute.

An indicator that reads `status` **cannot** be precomputed this way: account state depends on the
trades the strategy makes, which is a feedback loop. Keep those as plain `BaseIndicator` without
`precompute_series` (`streamer/indicator/position_age.py` is the example). The two kinds mix freely in one
strategy.

One class attribute tunes an indicator, settable per subclass or per instance:

| Attribute | Default | Effect |
|---|---|---|
| `scale_group` | `"price"` | Chart pane. `"price"` overlays the candles; any other value gets its own pane, shared by indicators with the same group. |

So there are two independent choices per indicator: which pane it draws on, and whether it defines
`precompute_series`.

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

A backtest is the four engine parts assembled and handed to `TradingEngine`:

```python
from core.executor.simulated import DEFAULT_INIT_MARGIN
from core.producer.historical import BinanceHistoricalCandleProducer
from core.recorder.backtest import BacktestRecorder
from core.executor.simulated import SimulatedExecutor
from core.engine.engine import TradingEngine

producer = BinanceHistoricalCandleProducer(
    start_time=start, end_time=end, symbols=["ETHUSDT"], interval="1m")
executor = SimulatedExecutor(DEFAULT_INIT_MARGIN, fee_ratio=0.0004)
recorder = BacktestRecorder(streamer, executor.status, interval_ms=producer.interval_ms,
                            metadata={...}, save_series=True)
report = TradingEngine(streamer, producer, executor, recorder).run()
```

The executor writes `fee_ratio` into the `Status` it builds, so the fee accounting
(`apply_fill`, a module-level function in `core/executor/simulated.py`) and the strategy's own
position sizing (`status.fee_ratio`) always read one value. `slippage_ratio` is a `SimulatedExecutor` argument only — it is a simulation modelling knob,
not account state, and no strategy sizes with it.

The candle source is either `BinanceHistoricalCandleProducer` (fetches the `[start, end)` range
itself via `BinanceVisionFetcher` and merges) or `InMemoryCandleProducer(candles_by_symbol)` — a
`Dict[str, List[Candle]]` you built yourself (used by the check scripts and non-Binance sources).
Everything else — wiring the fill sink, driving the loop, finishing the recorder — is the
engine's; `run()` returns the `Report`. A backtest does not warm indicators up separately: its
first `window` events are the warm-up, and indicators read NaN through them.

`Report` gives you `trades`, `max_leverage`, the final `Status` and the equity curve. Giving the
recorder `metadata` writes `asset/backtest/<run_id>.json` (summary, Sharpe, max drawdown, trade
list); adding `save_series=True` also writes month-bucketed `<run_id>.<YYYY-MM>.series.json` shards
with per-candle OHLC and indicator values, which the frontend loads lazily per viewport.

```bash
uv run python core/checks/live_check.py            # live path + dry-run == backtest
```

The engine also force-liquidates every position when mark-to-market equity falls to zero or below,
so blown-up strategies show up as blow-ups rather than as impossible recoveries.

Backtesting and live trading are the same code path: both assemble `core/engine/`'s `TradingEngine`
and differ only in which candle source, executor and recorder are plugged into it. A dry run uses
the *backtest's* executor, so it produces exactly the trades a backtest of the same candles would.

## Data sources

- **`BinanceVisionFetcher`** (default) — bulk-downloads monthly/daily zips from
  data.binance.vision. Fast for long histories.
- **`BinanceCandleFetcher`** — the REST klines API in ≤1500-candle pages. Used by the live trader
  to prefeed indicators (through a `BinanceHistoricalCandleProducer` with `use_cache=False`, so the
  warm-up asks for exactly the bars it needs instead of the whole month chunk).
- **`MassiveStockFetcher`** — US equity minute bars from Massive (formerly Polygon.io), needs
  `MASSIVE_API_KEY`. Session-aware: `nyse_session` builds the NYSE trading-hours grid so a
  1200-candle window means the same wall-clock span on every symbol instead of silently spanning
  overnight gaps. `uv run python core/checks/fetch_stock_check.py AAPL` smoke-tests it.

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
pyproject.toml            package metadata + deps (uv-managed), builds the `core` package
core/
  domain/                 Candle, Action, Status, Trade, Report, order book + fill rules
  streamer/               BaseStreamer — the strategy contract
    indicator/            MA, ATR, Donchian, ADX, Supertrend, rolling std, volume stats, ...
    strategies/           example strategies (illustrations, not tuned)
  fetcher/                BaseCandleFetcher + month-chunk pickle cache
    binance/              Binance REST / data.binance.vision / funding / OI + long-short metrics
    stock/                Massive US equities + NYSE session calendar
  engine/                 TradingEngine + the three ports (CandleProducer/Executor/Recorder)
  result/                 run JSON + series shards, Sharpe/MDD/benchmark metrics
  backtest/              SimulatedExecutor, BacktestRecorder, candle producers
  live/                   BinanceTrader (asyncio + websockets), LiveExecutor/Recorder/Producer,
                          BinanceOrderClient, ReliableWebsocket
  checks/                 live_check.py (the de-facto test suite), fetch_stock_check.py
  examples/               backtest.py/trader.py entry points (edit for symbol/date/strategy)
  utils/                  timestamp and rounding helpers
visualise/                React viewer for asset/backtest/*.json
asset/                    gitignored: candle cache + backtest output
```

Dependencies run one way, and nothing points back:

```
utils ← domain ← streamer
              ← fetcher
              ← engine ← result
                      ← backtest ← live
```

`domain/` imports nothing from the repo but `utils/` — it knows nothing about the engine, the
strategies or the exchange. `engine/` holds the order-of-operations and the three ABCs the modes
plug into, and imports neither `backtest/` nor `live/`. The one edge that looks backwards is
deliberate: `live/` imports `backtest/` for `SimulatedExecutor`, because a dry run runs the
backtest's own fill code rather than a copy of it.

## Requirements

[`uv`](https://docs.astral.sh/uv/) (manages the Python 3.13+ interpreter and dependencies for you —
`uv sync` will download a matching Python if none is installed) and Node 18+ for the frontend.
