# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this
repository.

## Project overview

An algorithmic trading system for Binance USD-M Futures with two independent halves:

- `core/` — Python engine: historical candle fetching, two backtesters (a reference
  implementation and a vectorized fast path), a pluggable streamer/indicator strategy framework,
  and an asyncio live trader/executor.
- `visualise/` — a Create React App frontend that reads the JSON files the backtester writes and
  renders them with `lightweight-charts`. It has no backend of its own; the user picks the `asset/`
  directory with the File System Access API and everything is parsed client-side.

There is no automated test suite (no `pytest`/`unittest` files in `core/`, only the default CRA
`react-scripts test` in `visualise/`). `core/backtest_fast_check.py` is the de-facto correctness
check: it asserts `FastBacktester` and `SingleThreadedBacktester` produce identical trades, final
margin, max leverage and equity curve.

## Commands

### Python (`core/`)

`core` is a real, `uv`-managed, pip-installable package (see "Import convention" below). Install
and run everything through `uv`, from the repo root:

```bash
uv sync
```

```bash
uv run python core/examples/backtest.py              # backtest over a hardcoded date range/symbol/strategy
uv run python core/examples/backtest.py "label"       # optional one-line experiment label stored in the run JSON
uv run python core/backtest_fast_check.py             # parity: FastBacktester vs SingleThreadedBacktester
uv run python core/fetch_stock_check.py               # US equity fetcher smoke test (needs MASSIVE_API_KEY)
uv run streamed-trader                                # live/dry-run trader using .env configuration
```

`core/examples/backtest.py` and `core/examples/trader.py` are edited directly to change symbol/date
range/strategy params for one-off runs; `trader.py` also reads config from environment variables
(see `.env.sample`: `API_KEY`, `API_SECRET`, `TESTNET`, `DRY_RUN`, `SYMBOL`, `INTERVAL`, plus the
current strategy's params — `WINDOW`, `M_ENTRY`, `M_EXIT`, `MAX_LOSS` for the wired-up
`KeltnerStreamer`). `streamed-trader` (a `[project.scripts]` console script defined in
`pyproject.toml`) is `core.examples.trader:run`, a sync wrapper around `trader.py`'s `async def
main()`.

### Docker

```bash
docker compose up -d --build
```

Builds from `Dockerfile` (installs deps via `uv sync --frozen`, `ENTRYPOINT ["uv", "run",
"streamed-trader"]`, run from `/app`) and runs the live trader continuously (`restart: always`),
reading secrets from `.env`.

### Visualiser (`visualise/`)

```bash
cd visualise
npm install
npm start   # dev server on http://localhost:3000
npm test    # CRA/jest tests
npm run build
```

## Architecture

### Import convention — `core` is a real, `uv`-installed package

`core/` is a proper Python package: a root-level `pyproject.toml` (build backend: `hatchling`,
`[tool.hatch.build.targets.wheel] packages = ["core"]`) makes it installable with `uv sync` (which
does an editable install of the `core` package into `.venv`). Every module imports its siblings
with the full absolute path rooted at `core`, e.g. `from core.streamer.keltner_streamer import
KeltnerStreamer`, `from core.backtest.status import Status` — this resolves via the installed
package regardless of current working directory, so scripts can be run as `uv run python
core/<script>.py` from anywhere in the repo (no more cwd-inside-`core/` requirement). The
distribution name in `pyproject.toml` is `streamed-trader`, but the importable top-level
package is still `core`.

The two CLI entry points live in `core/examples/` (`backtest.py`, `trader.py`) specifically to
avoid name collisions with the `core/backtest/` and `core/trader/` packages — a top-level
`core/backtest.py` module and a `core/backtest/` package can't both be `core.backtest`.

Default paths like `BaseCandleFetcher(save_path="../asset/")` and
`SingleThreadedBacktester(result_path="../asset/")` are relative to the *current working
directory*, not the script location — so they only land in the repo-root `asset/` (gitignored)
when the process is launched with cwd = `core/`.

### Data flow: fetch → cache → backtest → visualise

1. **Fetch.** `candle_fetcher/base.py` (`BaseCandleFetcher`) is the shared base: subclasses
   implement only `get_candles(symbol, start, end, interval) -> List[Candle]` and inherit
   `get_candles_with_cache`. Implementations:
   - `binance_candle_fetcher/fetcher.py` (`BinanceCandleFetcher`) — Binance Futures REST klines in
     ≤1500-candle chunks. Used by the live trader's indicator prefeed.
   - `binance_candle_fetcher/vision_fetcher.py` (`BinanceVisionFetcher`) — bulk-downloads
     monthly/daily zips from data.binance.vision. This is what the backtest entry points use.
   - `stock_candle_fetcher/massive_fetcher.py` (`MassiveStockFetcher`) — US equity minute bars from
     Massive (formerly Polygon.io). Overrides the non-24/7 hooks (`_cache_dir_name`,
     `_expected_candles`, `_data_available_until`, `_on_cache_open`) and forward-fills onto the
     NYSE session grid built by `stock_candle_fetcher/nyse_session.py`, so a window of N candles
     means N *trading* minutes rather than N wall-clock minutes.

   `binance_candle_fetcher/` also has `funding_fetcher.py` (funding rates) and `metrics_fetcher.py`
   (open interest, long/short account ratios) for strategies that want non-price inputs; both cache
   the same way but return dicts, not `Candle`s.

2. **Cache.** `get_candles_with_cache` caches per **month chunk** under
   `asset/candle/<symbol>_<interval>/<YYYY-MM>.pkl` (`candle_fetcher/pickle_storage.py`): only
   months missing from the cache are fetched, fully-past months are cached permanently (immutable,
   including empty ones — pre-listing months are cached as empty so they aren't re-requested), and
   the in-progress month is kept as `<YYYY-MM>.partial.pkl` holding closed candles only — each run
   delta-fetches from the partial's last candle and promotes it to a full chunk once the month
   ends. It returns `List[Candle]` only. `binance_candle_fetcher/candle_storage.py` is an older
   CSV(.gz) load/save path for the same `Candle` objects.

3. **Backtest.** `backtest/SingleThreadedBacktester.py` replays a `List[Candle]` through a
   `BaseStreamer`, updating a `Status` (margin/position/avg_price/unrealised_pnl/leverage) and
   (when `metadata` is passed to `run()`) writing a run JSON plus month-bucketed columnar series
   shards into `asset/backtest/` (`result_writer.py`). It also force-liquidates a position when
   `margin <= unrealised_pnl`.

   `backtest/FastBacktester.py` is the drop-in fast version the entry scripts use: it precomputes
   every `VectorizedIndicator` as a numpy array (swapped in as a cursor-backed `ArrayIndicator`
   shim), keeps plain `BaseIndicator`s loop-updated, rebuilds the equity curve vectorized, and
   bulk-writes shards after the loop — same trades/outputs, ~2x faster loop and ~5x faster
   `save_series` runs. `SingleThreadedBacktester` stays as the reference implementation.

4. **Visualise.** `visualise/` reads the run JSON and shards client-side — it does not talk to
   Binance or the Python code at all.

### Backtest output format (`core/backtest/result_writer.py`)

`SCHEMA_VERSION = 1`. A run with `metadata` produces:

- `asset/backtest/<run_id>.json` — `metadata`, `summary` (max leverage, final margin, profit %,
  win/lose counts, win rate, `sharpe`, `max_drawdown` — computed by `backtest/metrics.py`;
  `compute_sharpe` resamples to daily before annualising because per-candle returns are mostly
  zero-noise), a downsampled `equity` block (≤2000 points for the timeline sparkline), the full
  `trades` list, and a `series` index describing the shards.
- `asset/backtest/<run_id>.<YYYY-MM>.series.json` — one shard per month with per-candle OHLC and
  indicator values, **columnar** (parallel arrays) so column keys aren't repeated per candle.
  `SingleThreadedBacktester` streams these out through `ShardWriter` (only the current month is in
  memory); `FastBacktester` bulk-writes them after the loop. The frontend loads them lazily per
  viewport.

`run_id` is `<StreamerClassName>_<YYYYmmdd_HHMMSS>`. `series.column_groups` carries each
indicator's `scale_group`, which decides whether the frontend overlays it on the price pane or
gives it its own.

### Streamer/indicator strategy framework (`core/streamer/`)

- `BaseStreamer` (ABC) holds a `Dict[str, BaseIndicator]` and exposes
  `update_candle(candle, status) -> Action`, which delegates to the abstract `decide_action`.
  Subclasses only implement `decide_action`.
- Indicators come in two types. `BaseIndicator` (ABC) is a loop-updated rolling-window indicator:
  `update(candle, status=None)` ingests one candle plus the *pre-trade* `Status` snapshot — the
  same one `decide_action` saw for that candle (the live trader's `_prefeed_indicators` passes
  `None`, so status-aware indicators must treat `None` as warm-up); `get_index(idx)`/`get_latest()`
  read back past values (`-1` = latest, `-2` = previous, ...). An indicator that reads `status`
  cannot be vectorized (account state is a feedback loop of the strategy's own trades) and must
  stay a plain `BaseIndicator` — `indicator/position_age.py` is the canonical example.
  `VectorizedIndicator` (subclass ABC) additionally requires
  `precompute_series(open, high, low, close, volume) -> np.ndarray` (element i = `get_latest()`
  after i+1 updates, NaN during warm-up), which `FastBacktester` uses to compute the whole series
  at once — `update()` must still work for the live trader. `MovingAverage`,
  `MinDonchianIndicator`/`MaxDonchianIndicator` (monotonic-deque min/max over a window),
  `ATRIndicator`, `RollingStd` and the `volume_stats` indicators are vectorized; `ADXIndicator`,
  `SupertrendIndicator`, `PivotTrendlineIndicator`, `TakerImbalanceIndicator` and
  `PositionAgeIndicator` are path-dependent or status-aware and stay plain `BaseIndicator`s
  (correct everywhere, just on the loop path). Both kinds mix freely within one strategy.
- Each indicator's `scale_group` (default `"price"`) tells the frontend which chart pane to plot
  it on; indicators sharing a group share a pane and price scale.
- Each indicator also carries `updates_before_decide` (default `False`), which selects **which
  side of `decide_action` it ingests the current candle on**. Every shipped indicator leaves it
  `False`, so the default ordering described below is what actually runs today. Setting it `True`
  (per subclass or per instance) makes `get_latest()` include the candle being decided on, which
  shifts every read by one index — breakout comparisons must then use `get_index(-2)`, since a
  Donchian max channel including the current bar satisfies `channel_max >= candle.high >=
  candle.close` and could never fire.

  All three engines partition `streamer.indicators` on this flag: `SingleThreadedBacktester`,
  `FastBacktester` and `BinanceTrader._on_kline`. Two non-obvious consequences in
  `FastBacktester`: the flag is mirrored onto each `ArrayIndicator` shim and the shim's `cursor`
  is advanced to `i+1` *before* the decision for flagged indicators; and
  `_build_series_columns` skips its usual one-candle right-shift for them (their decide-time
  value at candle `i` is `seq[i]`, unshifted). Both backtesters record the series between the two
  update groups, so every plotted column is the value the decision actually saw regardless of
  which side it was fed on.
- **Ordering matters**: both the backtester and `BinanceTrader` call
  `streamer.decide_action/update_candle` on the *closed* candle first, then update the indicators
  with that candle and the still pre-trade `Status`, and only then apply/execute the action (the
  `updates_before_decide` indicators above are the opt-in exception, fed just before the call) — so
  a decision always sees indicator state that excludes the candle it's deciding on, and an
  indicator always sees the status that decision was made with. The current candle's OHLCV is not
  hidden from the decision — it arrives directly as the `candle` argument, and every strategy uses
  it.

  This ordering is a **semantic convention, not a look-ahead guard**. The candle has already
  closed by the time `decide_action` runs, so folding it into the indicators would leak no future
  information. The reason to exclude it is that breakout logic requires it: a Donchian max channel
  that included the current bar would satisfy `channel_max >= candle.high >= candle.close`, making
  `close > channel_max` unfireable. What the convention guarantees is that `get_latest()` means
  exactly one thing, in the backtester and in the live trader alike.

  Look-ahead is actually held out elsewhere: fills use the decision candle's `close` (not a later
  or more favourable price), the `Status` handed to both `decide_action` and `update` is the
  pre-trade snapshot, and path-dependent indicators delay their own confirmation internally
  (`PivotTrendlineIndicator` only confirms a fractal pivot k bars later).
- `Action` is just a signed `quantity` delta to apply to the current position (positive = buy/long,
  negative = sell/short); it is interpreted identically by the backtester's `_trade()` and by
  `BinanceExecutor.execute_action`.
- Example strategies (illustrations of the framework, not tuned or recommended):
  `CrossMovingAverageStreamer` (MA10/MA25 cross — the simplest one, read it first),
  `KeltnerStreamer` (ATR channel breakout; wired into `backtest.py` and `trader.py`),
  `SupertrendStreamer` (always-in trend flip), `MeanReversionZScoreStreamer` (z-score fade;
  stateful across candles), `MomentumTimeExitStreamer` (momentum + fixed-time exit),
  `VolumeConfirmedMomentumStreamer` (the previous one subclassed to add a volume entry filter),
  `WickRejectionStreamer` (candlestick pattern), `TrendlineBounceStreamer` (pivot-trendline
  geometry), and `TradeStreamer` (alternating long/short every bar — a fixture, not a strategy).

### Position & PnL accounting (`core/backtest/status.py`, `trade.py`, `report.py`)

`Status` is the single shared representation of account state (`avg_price`, `margin`,
`unrealised_pnl`, `position`, `leverage`) used by *both* the backtester and the live
`BinanceTrader` — the same averaging/PNL math (`SingleThreadedBacktester._trade`) models
opening, pyramiding (same-direction add), partial close, full close, and direction-flip. A `Trade`
is an immutable record of one fill plus a deep-copied pre-trade `Status`; `Report` bundles all
`Trade`s with `max_leverage`, the final `Status` and the equity curve.

### Live trading (`core/trader/`)

- `BinanceTrader` is asyncio-based: it opens a futures kline websocket (and, unless `dry_run`, a
  futures user-data websocket) via `ReliableWebsocket` (a thin wrapper around python-binance's
  `ReconnectingWebsocket` that recovers from a dropped `recv()` by closing/reconnecting the
  delegate). On each *closed* kline it builds a `Candle`, calls the streamer, updates indicators,
  and fires registered action/error callbacks (`add_action_callback`/`add_error_callback`).
  `_prefeed_indicators()` backfills each indicator's window with historical candles (via
  `BinanceCandleFetcher`) before going live, asserting the fetched range exactly matches what's
  expected.
  - In `dry_run` mode: margin starts at a fixed synthetic `1e6`, no user-data socket is opened, no
    orders are sent, and position/avg_price are updated locally from the `Action` instead of from
    exchange fills (there's a `TODO` noting this should really replay through the backtester).
  - In live mode: `Status` is hydrated from `futures_account()` at startup and then kept in sync by
    `ACCOUNT_UPDATE`/`ORDER_TRADE_UPDATE` events off the user-data stream.
- `BinanceExecutor` submits orders to a `ThreadPoolExecutor` (GIL-free from the asyncio loop) with
  exponential-backoff retries, returning a `concurrent.futures.Future[OrderResult]`; callers (e.g.
  `BinanceTrader._on_action`) block on `future.result(timeout=...)` from within an async callback.

## Conventions

- Code comments and docstrings are in Korean; documentation files (`README.md`, `CLAUDE.md`,
  `core/trader/README.md`) are in English. Match whatever the file you are editing already uses.
- `asset/` is gitignored and holds both the candle cache and backtest output. Never commit it.
- Never commit `.env`. `.env.sample` holds placeholders only.
