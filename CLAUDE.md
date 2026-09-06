# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this
repository.

## Project overview

An algorithmic trading system for Binance USD-M Futures with two independent halves:

- `core/` — Python engine. Backtesting and live trading are **one domain**: a single trading
  engine (`core/engine/`) holds the order-of-operations, and backtest / dry run / live differ only
  in which candle source, executor and recorder are plugged into it (see "The trading engine"
  below). Around that: historical candle fetching, a pluggable, cross-symbol-aware
  streamer/indicator strategy framework with market, limit and conditional (stop/take-profit)
  orders, and an asyncio live trader (see "Live trading").
- `visualise/` — a Create React App frontend that reads the JSON files the backtester writes and
  renders them with `lightweight-charts`. It has no backend of its own; the user picks the `asset/`
  directory with the File System Access API and everything is parsed client-side. **Does not yet
  render multi-symbol runs** — see "Backtest output format" below.

There is no automated test suite (no `pytest`/`unittest` files in `core/`, only the default CRA
`react-scripts test` in `visualise/`). The de-facto correctness check is:

- `core/checks/live_check.py` — covers the live path (it needs a socket and an exchange, so it fakes
  both) and asserts the candle producer's continuity/backfill rules, the live executor's
  account/fill handling, and that **a dry run produces exactly the trades a backtest of the same
  candles does** (over Keltner, a stateful mean-reversion strategy, and toy limit/stop-ladder
  fixtures). The vectorized backtest path and its `backtest_fast_check.py` parity harness were
  removed pending reintroduction on the current producer structure — the backtest now has a
  single reference path.

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
uv run python core/checks/live_check.py                      # live path: producer, executor, dry-run == backtest
uv run python core/checks/fetch_stock_check.py               # US equity fetcher smoke test (needs MASSIVE_API_KEY)
uv run streamed-trader                                # live/dry-run trader using .env configuration
```

`core/examples/backtest.py` and `core/examples/trader.py` are edited directly to change symbol/date
range/strategy params for one-off runs; `trader.py` also reads config from environment variables
(see `.env.sample`: `API_KEY`, `API_SECRET`, `TESTNET`, `DRY_RUN`, `SYMBOLS` (comma-separated),
`INTERVAL`, plus the current strategy's params — `WINDOW`, `M_ENTRY`, `M_EXIT`, `MAX_LOSS` for the
wired-up `KeltnerStreamer`). `streamed-trader` (a `[project.scripts]` console script defined in
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
with the full absolute path rooted at `core`, e.g. `from core.streamer.strategies.keltner_streamer import
KeltnerStreamer`, `from core.domain.status import Status` — this resolves via the installed
package regardless of current working directory, so scripts can be run as `uv run python
core/<script>.py` from anywhere in the repo (no more cwd-inside-`core/` requirement). The
distribution name in `pyproject.toml` is `streamed-trader`, but the importable top-level
package is still `core`.

The two CLI entry points live in `core/examples/` (`backtest.py`, `trader.py`) specifically to
avoid name collisions with the `core/engine/` and `core/backtest/` packages. Relative imports
(`from .foo import Bar`) are not used anywhere — everything is the full `core.`-rooted path.

Default paths like `BaseCandleFetcher(save_path="../asset/")` and
`BacktestRecorder(result_path="../asset/")` are relative to the *current working directory*, not the
script location — so they only land in the repo-root `asset/` (gitignored) when the process is
launched with cwd = `core/`.

### Package layout — the dependency direction is the point

```
core/
  domain/     Candle, Action, Status, Trade, Report, order_book (fill rules)
  streamer/   BaseStreamer + indicator/ + strategies/ (the example strategies)
  fetcher/    BaseCandleFetcher + pickle cache; binance/ and stock/ under it
  engine/     TradingEngine + the three ports: CandleProducer, Executor, Recorder
  result/     writer.py (run JSON + shards), metrics.py, indicator_columns.py
  backtest/   SimulatedExecutor, BacktestRecorder, the two backtest producers
  live/       BinanceTrader, LiveCandleProducer/Executor/Recorder, BinanceOrderClient
  checks/     live_check.py, fetch_stock_check.py
  examples/   backtest.py, trader.py
  utils/      timestamp/rounding helpers, logging_config.py alongside
```

```
utils ← domain ← streamer
              ← fetcher
              ← engine ← result
                      ← backtest ← live
```

Three rules hold this together, and a change that breaks one is a design change, not a tidy-up:

- **`domain/` imports nothing from the repo but `utils/`.** It is the vocabulary all three modes
  share — an account, a position, a fill, a candle, a resting order. It does not know that an
  engine, a strategy or an exchange exists. This is why `fetcher/` can produce `Candle`s without
  importing the strategy framework, which it used to have to do when `Candle` lived under
  `core/streamer/`.
- **`engine/` holds the contract, not the implementations.** `TradingEngine` plus the three ABCs.
  It imports neither `backtest/` nor `live/`. `TradingEngine` wires the parts together and runs
  them, but it never *constructs* them — the caller does that (`core/examples/backtest.py`,
  `live/trader.py`), which is exactly what lets the engine stay ignorant of both mode packages.
  Moving that construction into `engine/` would make `engine ↔ backtest` circular, so it is a
  design change, not a tidy-up.
- **`live/` importing `backtest/` is deliberate.** Dry run uses `SimulatedExecutor` — the
  backtest's own class, not a copy of it — so the import is the invariant made visible. It is the
  only edge that runs "backwards" and it is acyclic.

`BaseIndicator` deliberately stays in `core/streamer/indicator/` rather than moving to `domain/`:
nothing outside `core/streamer/` imports it (the engine, recorders and `indicator_columns` reach
indicators only through `streamer.indicators`, duck-typed), and a stateful rolling-window ABC
carrying a frontend hint (`scale_group`) would blur what `domain/` means.

### Data flow: fetch → cache → backtest → visualise

1. **Fetch.** `fetcher/base.py` (`BaseCandleFetcher`) is the shared base: subclasses
   implement only `get_candles(symbol, start, end, interval) -> List[Candle]` and inherit
   `get_candles_with_cache`. Implementations:
   - `fetcher/binance/rest_fetcher.py` (`BinanceCandleFetcher`) — Binance Futures REST klines in
     ≤1500-candle chunks. Used by the live trader's indicator prefeed.
   - `fetcher/binance/vision_fetcher.py` (`BinanceVisionFetcher`) — bulk-downloads
     monthly/daily zips from data.binance.vision. This is what the backtest entry points use.
   - `fetcher/stock/massive_fetcher.py` (`MassiveStockFetcher`) — US equity minute bars from
     Massive (formerly Polygon.io). Overrides the non-24/7 hooks (`_cache_dir_name`,
     `_expected_candles`, `_data_available_until`, `_on_cache_open`) and forward-fills onto the
     NYSE session grid built by `fetcher/stock/nyse_session.py`, so a window of N candles
     means N *trading* minutes rather than N wall-clock minutes.

   `fetcher/binance/` also has `funding_fetcher.py` (funding rates) and `metrics_fetcher.py`
   (open interest, long/short account ratios) for strategies that want non-price inputs; both cache
   the same way but return dicts, not `Candle`s.

2. **Cache.** `get_candles_with_cache` caches per **month chunk** under
   `asset/candle/<symbol>_<interval>/<YYYY-MM>.pkl` (`fetcher/pickle_storage.py`): only
   months missing from the cache are fetched, fully-past months are cached permanently (immutable,
   including empty ones — pre-listing months are cached as empty so they aren't re-requested), and
   the in-progress month is kept as `<YYYY-MM>.partial.pkl` holding closed candles only — each run
   delta-fetches from the partial's last candle and promotes it to a full chunk once the month
   ends. It returns `List[Candle]` only. `fetcher/binance/candle_storage.py` is an older
   CSV(.gz) load/save path for the same `Candle` objects.

   **Moving `Candle` to another module invalidates every cached chunk.** Pickle stores a class as
   its `(module path, name)` string, so a relocation makes `pickle.load` raise
   `ModuleNotFoundError` — loudly, since neither `pickle_storage` nor `get_candles_with_cache`
   catches it, so it is a crash rather than a silent multi-GB re-download. The fix is a throwaway
   migration, not a permanent shim: alias the old path (`sys.modules["core.streamer.candle"] =
   core.domain.candle`), then load-and-re-save each `.pkl`. `save_to_pickle` is tmp + `os.replace`,
   so it is atomic per file and the script is safe to interrupt and re-run. That is how the
   2026-09 `core.streamer.candle` → `core.domain.candle` move was handled, and no compatibility
   code was left behind.

   Adding a *field* is different: older chunks simply lack it in `__dict__` and `Candle`'s
   class-attribute defaults cover it, so no migration is needed. Only a class **move or rename**
   forces one. Note this applies to the backtest cache only — the live trader's indicator prefeed
   calls `get_candles` directly and never touches a `.pkl`.

3. **Backtest.** There is no `run_backtest` function any more — a backtest *is* the four engine
   parts assembled by the caller (`core/examples/backtest.py` is the reference; see "The trading
   engine" below):

   ```python
   # DEFAULT_INIT_MARGIN is core.backtest's; the executor builds and owns the Status.
   executor = SimulatedExecutor(DEFAULT_INIT_MARGIN, fee_ratio)
   recorder = BacktestRecorder(streamer, executor.status, interval_ms=producer.interval_ms,
                               metadata=metadata, save_series=True)
   report   = TradingEngine(streamer, producer, executor, recorder).run()
   ```

   `fee_ratio` is a `Status` field the executor sets — `SimulatedExecutor` from its constructor
   arg (default `DEFAULT_FEE_RATIO` in `core/domain/status.py`), `LiveExecutor.create` from the
   exchange's own taker commission rate (`futures_commission_rate`). Strategies read
   `status.fee_ratio` when sizing, so the fee the account is charged and the fee the strategy
   sized against are one value by construction — the old `resolve_fee_ratio`/`resolve_slippage_ratio`
   reconciliation is gone. `slippage_ratio` stays a `SimulatedExecutor` argument only (a
   simulation modelling knob, not account state; no strategy sizes with it).

   It is multi-symbol. The candle source is one of:
   - `InMemoryCandleProducer(candles_by_symbol)` (`backtest/in_memory_candle_producer.py`) —
     `Dict[str, List[Candle]]`, one ragged list per symbol (symbols don't need to start/end at the
     same time), merged by its own `merge_by_end_time` (same module — same `end_time` is one
     event) into one chronological stream of **events**. Used by the check scripts and
     non-Binance sources.
   - `BinanceBacktestCandleProducer` (`backtest/binance_candle_producer.py`), which subclasses
     `InMemoryCandleProducer` and fetches the `[start, end)` range itself (default fetcher
     `BinanceVisionFetcher`, injectable) before merging. This is what `core/examples/backtest.py`
     uses.

   `BacktestRecorder` owns the output as well as the in-memory `Report`: give it `metadata` and it
   writes the run JSON on `close()` (which the engine calls at the end of the run), plus the
   time-series shards when `save_series=True`. Without `metadata` it is a pure in-memory run. The
   `Report` comes back from `TradingEngine.run()` (it is `recorder.report`).

   An event bundles every symbol whose candle closes at that exact timestamp; the per-event
   ordering is `TradingEngine.process_event`'s (see "The trading engine" below). Indicators are
   loop-updated and equity is recorded per event — the single reference path. (A vectorized path
   that precomputed `precompute_series` indicators with numpy and rebuilt equity in one shot was
   removed pending reintroduction on this producer structure.)

4. **Visualise.** `visualise/` reads the run JSON and shards client-side — it does not talk to
   Binance or the Python code at all.

### The trading engine (`core/engine/`)

Backtest, dry run and live are **one domain**. The order-of-operations exists exactly once, in
`engine/engine.py`; the three modes differ only in which parts are plugged in:

| | CandleProducer | Executor | Recorder |
|---|---|---|---|
| backtest | `backtest.BinanceBacktestCandleProducer` / `backtest.InMemoryCandleProducer` | `backtest.SimulatedExecutor` | `backtest.BacktestRecorder` |
| dry run | `live.LiveCandleProducer` | **`backtest.SimulatedExecutor`** | `live.LiveRecorder` |
| live | `live.LiveCandleProducer` | `live.LiveExecutor` | `live.LiveRecorder` |

`core/engine/` holds only `TradingEngine` and the three ABCs the columns above name; every
implementation lives in `core/backtest/` or `core/live/`, and the engine imports neither.

**Assembling and running is the engine's job too.** The caller builds the four parts and hands them
over — `TradingEngine(streamer, producer, executor, recorder, on_error=...)` — and the engine does
the wiring (`executor.on_trade = recorder.record_trade`, `NullRecorder` when no recorder is given),
the indicator warm-up, the event loop, and the finish (`recorder.close()`), returning
`recorder.report`. That wiring used to be hand-repeated at each of the three entry points; the
`on_trade` line alone existed in three places. Only *constructing* the parts stays with the caller,
because those implementations live in the mode packages — which is what keeps the engine from
importing `backtest/` or `live/`.

Dry run and backtest share the executor **class**, not merely equivalent code — which is why
`core/live/trader.py` imports `SimulatedExecutor` out of `core/backtest/`. Dry run exists to be
compared against a backtest, so the fill rules, accounting and resting-order book have to be one
copy — before this, `_fill` and `_submit_book_action` were hand-copied into the live trader and
kept in sync by attention alone.

**The engine is fully synchronous.** `process_event` is a plain method — it hands each action to
the executor and moves on, never waiting for a result. All three modes go through the single
driver `run_async`, which iterates the producer with `async for` and routes any exception
`process_event` raises to `on_error` (so one transient strategy bug can't kill a weeks-long
process). `TradingEngine.run()` is the sync wrapper around it (`asyncio.run(...)`) for callers with
no event loop, which is how a backtest stays a synchronous call — inside one, use `run_async`
directly (that is what `live_check.py`'s parity harness does, and it no longer needs the
`asyncio.to_thread` hop it used to). `InMemoryCandleProducer.__aiter__` (which the Binance
subclass inherits) is an async generator that never `await`s anything, so no event loop scheduling
actually happens on that path — only `async for`'s protocol cost (~100ns/event, measured <2% of
`process_event`).
Live order submission is fire-and-forget inside `LiveExecutor`: the old "confirm order N before
sending N+1" guarantee is deliberately gone — live `status` is exchange truth (the user-data
stream updates it out of band), so a dropped order self-heals on the next candle. The flip side is
that **intra-event action execution order is not guaranteed in live** (the thread pool runs them);
backtest/dry run keep list order because `SimulatedExecutor` is synchronous. Every `CandleProducer`
implements one method, `__aiter__` — nothing else in the codebase has to know sync from async.

`process_event(event_time, candles)` — the contract every mode goes through:

```
0. executor.begin_event()          # live: drop unmatched market decisions from the last candle
1. freeze status.last_close[symbol] for every symbol in the event
2. executor.match_resting()        # fills happen here (sim only; live's exchange does it)
3. executor.mark_to_market()       # -> equity for this event
4. executor.force_liquidation()    # sim: equity <= 0 -> cancel_all + flatten every symbol
5. per symbol: update indicators -> decide_action (or flatten if bankrupt)
6. recorder.record_event()
7. per action: executor.submit()   # sim: CANCEL/register/fill in list order; live: fire the order, return
8. recorder.end_event()
```

Matching is at step 2, ahead of `decide_action`, because on a real exchange a resting order fills
before the bar closes — put it after and the strategy decides while believing it still holds a
position its stop already closed. Step 3 is where the equity point comes from, which is why a
MARKET fill's state applies from the *next* event while a resting fill's applies from this one.

**Fills flow through a sink, not a return value** (`executor.on_trade`). The simulated executor
calls it synchronously right after `apply_fill`; the live executor calls it when
`ORDER_TRADE_UPDATE` arrives, possibly hours later. This is the one place the sync/async asymmetry
of "when does a fill happen" is erased, and it is why the engine never sees a `Trade` at all.

**Recorders read indicator values themselves** (off `streamer.indicators`) rather than receiving
them, so a run that does not save series never pays for walking every indicator each event.

Live never merges symbols — `LiveCandleProducer` yields one `{symbol: candle}` event per closed
candle, and the `streamer.symbols` loop filters the rest out naturally.

### Backtest output format (`core/result/writer.py`)

`SCHEMA_VERSION = 5` (v5 added `trades[].order_type` and `trades[].submitted_at`, so a stop fill
is distinguishable from a market exit after the fact and a resting order's decision time survives
alongside its fill time — the shard shape is unchanged and the frontend drops unknown trade keys,
so old runs and the existing UI keep working; v2 added the `benchmark` block and
`summary.benchmark_profit_pct`; v3 added
`trades[].position`, the pre-trade signed position, which lets a resumed live run rebuild the
win/lose classification; v4 added multi-symbol support — `trades[].symbol`, `series.symbols` (the
traded set), `summary.by_symbol` (per-symbol win/lose breakdown alongside the unchanged aggregate
fields), and restructured the shard body to nest OHLC/indicators per symbol — see below). v1-v3
additions are purely additive — the frontend hides UI for missing blocks and picks named trade
keys — so older runs still load unchanged through their own flat shard shape; v4 is the one place
"additive" means "old files untouched, new files use a new shape" rather than "old files gain new
keys," since folding N symbols into the old flat single-`ohlc`-dict shape would be a namespace
collision, not an addition. **The frontend does not yet render the v4 nested shard shape or the
multi-symbol run JSON fields** — this is core-engine-only support so far; see the note under
"Streamer/indicator strategy framework" below. A run with `metadata` produces:

- `asset/backtest/<run_id>.json` — `metadata`, `summary` (max leverage, final margin, profit %,
  buy & hold profit %, win/lose counts, win rate, `by_symbol`, `sharpe`, `max_drawdown` — computed
  by `result/metrics.py`; `compute_sharpe` resamples to daily before annualising because
  per-candle returns are mostly zero-noise, and `result_writer._sharpe_sampling` derives the
  stride/annualisation from `interval_ms` so the resample is genuinely daily at any candle size.
  Win/lose counts only include **realising** fills — those whose signed quantity opposes the
  pre-trade position for that trade's symbol — and compare `wnl - fee`, so a close whose realised
  PnL is eaten by the fee counts as a loss), a downsampled `equity` block (≤2000 points for the
  timeline sparkline; equity is account-wide, one point per merged event, not per symbol), a
  `benchmark` block downsampled the same way, the full `trades` list (each with a `symbol`), and a
  `series` index describing the shards.

  `benchmark` is an **equal-weighted portfolio buy & hold** across every traded symbol
  (`metrics.build_multi_symbol_buy_and_hold_curve`, carried on `Report.benchmark_curve`): each
  symbol is allocated `init_margin / N` at its own first available close (so a symbol that starts
  trading later still gets a fair, un-lookahead-biased basis) and held to the end; with one symbol
  this reduces to the old `init_margin * close_i / close_0` curve. It has the same length as the
  equity curve, so `_downsample_equity` picks the same stride and `benchmark.time` is element-wise
  identical to `equity.time` — the frontend pairs them by index with no interpolation.
- `asset/backtest/<run_id>.<YYYY-MM>.series.json` — one shard per month. `time` (the merged event
  timestamps) and `balance` (account-wide `Status.total_margin()` — not per-symbol, since margin
  is a shared pool) sit at the shard's top level; a `symbols` key nests each traded symbol's own
  `ohlc`/`indicators`, **columnar** (parallel arrays) so column keys aren't repeated per event. A
  symbol absent from a given event (ragged series, staggered start) gets `null` across the board
  for that row — the existing warm-up-null convention, no new sentinel. The backtest path and the
  live recorder stream these out through `ShardWriter` (only the current month is in memory). The
  frontend loads them lazily per viewport.

`run_id` is `<StreamerClassName>_<YYYYmmdd_HHMMSS>`. `series.column_groups` carries each
indicator's `scale_group`, keyed by indicator **name** only (shared meaning across symbols, a
deliberate simplification) — it decides whether the frontend overlays it on the price pane or
gives it its own.

All writes go through `result_writer._write_json`, which wraps `boltons.fileutils.atomic_save`
(part file → fsync → atomic rename). Nothing ever observes a half-written run JSON or shard, which
matters most for the live recorder below — it rewrites the same files continuously — but also means
an interrupted backtest leaves no corrupt output. The part file is `<dest>.part`, deliberately not
ending in `.json`, because the frontend's directory scanner collects every `*.json` it finds.

### Live run output (`core/live/recorder.py`)

`LiveRecorder` writes live and dry-run sessions to `asset/live/` in **exactly** the format above,
so the visualiser loads both from one directory list (`filterRunFiles` accepts `backtest/` and
`live/`) and `metadata.mode` (`"live"`/`"dry"`) drives a LIVE/DRY badge. It reuses `ShardWriter`,
`write_run_json`, `build_summary` and `metrics.py` unchanged; the only backtester-side change was
splitting `ShardWriter._flush` into `checkpoint()` (write the current month, keep the buffer) and
buffer reset, since a live process rewrites one month's shard for weeks.

**`LiveRecorder`/`BinanceTrader` are multi-symbol**, trading every symbol in `streamer.symbols`
concurrently over one multiplexed kline socket (see "Live trading" below) and writing the same
multi-symbol `Status`/shard shapes described above — `LiveRecorder.record_event` is called once per
symbol's own candle close (live events are not merged/batched across symbols the way backtest
events are), with a single-key `{symbol: candle}` dict, which already matches `ShardWriter.add`'s
existing ragged-series contract with no changes needed there.

- **`run_id` is stable across restarts**: `<live|dry>_<Streamer>_<SYM1-SYM2-...>_<INTERVAL>`
  (symbols sorted and hyphen-joined so config order doesn't fork the run; override with
  `LIVE_RUN_ID`). Docker's `restart: always` means the process dies often, and a fresh file per
  start would fragment the equity curve into unusable pieces. On startup the recorder reloads its
  own run JSON and shards and continues appending — `equity_curve` is replayed from the shards'
  top-level `balance` column, and the per-symbol benchmark curve from each symbol's
  `ohlc.close` column (a symbol's `None` at a row is the normal ragged-series gap, not a
  warm-up/bankruptcy signal — only a `None` `balance` skips the row), never from the run JSON's
  `equity` block (that one is downsampled to ≤2000 points and would decay a little more on every
  restart). `init_margin` also comes from the saved metadata, not the current wallet, or
  `profit_pct` would reset each restart.
- **Config changes fork a new run.** If `schema_version`, `series.columns`, `params`, `symbols`,
  `interval` or `streamer` differ from the saved run, the recorder logs an ERROR and starts
  `<run_id>_<timestamp>` rather than appending mismatched data into the old shards. The
  `schema_version` check specifically exists so a v1-v3 run (flat shard shape) is never resumed by
  v4 code — it always forks a fresh run instead, deliberately, rather than special-casing the old
  shape in the resume path.
- **`metadata.last_status`** carries the account state at the last flush, and on resume
  `BinanceTrader._restore_dry_run_status` applies it **in dry-run only**. Dry-run margin is
  synthetic (`1e6`) and would otherwise reset on every process start while the equity curve
  continues, putting a jump back to the initial value at each restart — which would make a resumed
  dry-run curve useless for the backtest comparison this all exists for. Live never uses it:
  `futures_account()` and `ACCOUNT_UPDATE` are the truth there.
- **Recording points mirror the backtest exactly** — because they are literally the same call.
  `TradingEngine.process_event` invokes `recorder.record_event` right after `decide_action`
  returns, once every indicator for that symbol is already updated, so every plotted column is the
  value the decision actually saw. There is no longer a separate live recording seam that could
  drift out of step.
- **Resting orders.** Dry-run keeps its own book in `Status.open_orders` and fills it with the same
  `order_book.match_symbol` the backtest uses — in fact the same `SimulatedExecutor` object — at
  the same seam (before the indicator update and `decide_action`), so a dry run over some candles
  produces exactly the trades a backtest of those candles does, stop fills included
  (`core/checks/live_check.py` asserts this). `metadata.last_status`
  carries the serialized book, so a restart does not silently drop a stop the strategy believes is
  armed. Live never simulates: orders go to the exchange, `status.open_orders` is hydrated at
  startup from `futures_get_open_orders()` and then kept in sync by `ORDER_TRADE_UPDATE` (`NEW`
  adds, a terminal state removes), and `_reconcile_resumed_orders` warns when the resumed book and
  the exchange's actual book disagree — the exchange keeps working a stop while this process is
  down, which makes that reconciliation matter more than the position one.
- **Fills.** Dry-run records the local `Status.apply_fill` result at `status.last_close[action.
  symbol]` (the action's *target* symbol's last known close, not necessarily the triggering
  candle's own close — see the cross-symbol note below), so a dry run and a backtest of the same
  candles produce identical trades. Live records **real exchange fills** from
  `ORDER_TRADE_UPDATE`, aggregated per `(symbol, order_id)` into one `Trade` (`price` = `ap`,
  `quantity` = signed `z`, `wnl` = Σ`rp`, `fee` = Σ`n`) and emitted on a terminal order state —
  `CANCELED`/`EXPIRED` with `z > 0` included, since those are real fills. The pre-trade `Status`
  snapshot is deep-copied *before* the order is sent, because sending yields the event loop and a
  fill / `ACCOUNT_UPDATE` can overwrite `status` before the `Trade` is built; the order result is
  awaited in a detached task (`LiveExecutor._await_order_result`), not inline. For a **market** fill
  `Trade.timestamp` is the decision candle's `end_time`, not the fill's `T`, so trades bucket with
  the candle that caused them on aggregated views; for a **resting** fill it is the real fill time
  (`T`), because the decision was bars earlier and bucketing it there would be actively wrong —
  `submitted_at` carries the decision time instead. `_pending_decision` (used to pair a dispatched
  order with its pre-trade snapshot) is keyed by `(symbol, client order id)`: every order now gets
  a `newClientOrderId` (auto-generated for market orders, the strategy's `client_id` for resting
  ones) which the exchange echoes back as `c`, so pairing is exact rather than a per-symbol guess —
  which also lifts the old "one in-flight decision per symbol" limit. Only market entries are
  dropped on the next candle; a resting entry lives until its order leaves the book, since a fill
  bars later is the whole point.
- **Flush policy**: run JSON every candle (small), month shard every `LIVE_SHARD_FLUSH_EVERY`
  candles (default 60) and on every trade and on `stop()`. A crash loses at most that many candles
  of series data; the next startup replays from the shards and rewrites a self-consistent run JSON.
- **Why live numbers will not match a backtest exactly** (all expected, none are bugs): live equity
  is exchange truth, so funding fees, other symbols' PnL and deposits/withdrawals move the curve
  with no corresponding `Trade` and `Σ(wnl - fee)` will not reconcile with the equity delta;
  a resting order's `Trade.timestamp` is the exchange's fill time live but the closing time of the
  candle that triggered it in a backtest, so the two can sit up to one bar apart; a backtest infers
  intrabar fills from the bar's OHLC under the fixed assumptions listed in "Resting orders" above,
  while the exchange matched against the real tick path, so a bar that touched both a stop and a
  limit can resolve differently; `BinanceOrderClient.execute_action` does not quantize to step size,
  so the filled quantity can differ from the requested action (`LiveExecutor._emit_fill` warns past
  `_QUANTITY_DIVERGENCE_TOLERANCE`, 1%, so the size of that drift is visible rather than
  merely expected); prefeed candles are not recorded, so live indicator columns have no
  NaN warm-up prefix; `update_unrealised_pnl` marks with last price while the exchange marks with
  mark price; live sizing uses `status.fee_ratio` from the account's real taker commission tier
  (`futures_commission_rate`) while a backtest uses whatever `fee_ratio` the caller passed
  `SimulatedExecutor`, so position sizes can differ if the two rates differ; and BNB-denominated
  commissions (`N` != margin asset) are excluded from `fee` and flagged as
  `metadata.fee_asset_mismatch`.

### Logging (`core/logging_config.py`)

Every entry point calls `setup_logging()` before doing anything else — `core/examples/trader.py`,
`core/examples/backtest.py`, `core/checks/live_check.py` and `core/checks/fetch_stock_check.py`. It is
the only place `logging.basicConfig` is called. Without it, `logger.info` from the fetchers and
backtester is dropped entirely and `WARNING`+ falls through to Python's `logging.lastResort`
handler, which prints a bare message with no level, logger name or timestamp — which is how the
two warnings that decide whether a backtest is trustworthy (a month chunk with a data gap in
`fetcher/base.py`, a Sharpe truncated at bankruptcy in `result/metrics.py`) used to come
out, as anonymous lines between tqdm bars.

Level resolution is `level` argument > `LOG_LEVEL` env var > the entry point's `default` (INFO for
the trader and backtest, WARNING for the check scripts, whose verdicts are printed). The
format carries `asctime` because the live trader is a weeks-long process that docker restarts on
every crash, and omits `lineno` because it churns with every edit and breaks grep patterns.

Level guidance for the live path: `INFO` is the operational narrative (startup config, fills,
account updates that actually changed something, order submission) and is intended to stay
readable for a process that runs for weeks. `DEBUG` adds one line per candle carrying
symbol/candle/actions/status/indicators — it is guarded by `isEnabledFor`, since
`generate_dict_string` walks every indicator and would otherwise run on every candle just to have
its result discarded.
Broad `except` blocks in the message handlers use `logger.exception`, not `logger.error`: they
catch bugs, and a one-line message with no traceback is not enough to find one.

Log records go to **stderr** and every tqdm progress bar is pinned to **stdout**
(`file=sys.stdout` in the backtest producer and both Binance fetchers), so a warning raised
mid-fetch no longer shreds the bar it prints through. Library code under `core/` logs;
it does not `print`. The remaining `print` calls are deliberate CLI report output —
`examples/backtest.py`'s result summary and the `*_check.py` verdicts — which is why
those check scripts default to WARNING.

### Streamer/indicator strategy framework (`core/streamer/`)

- `BaseStreamer` (ABC) is multi-symbol and **cross-symbol aware**: `__init__(symbols,
  indicators)` takes the list of symbols it trades and `indicators: Dict[str, Dict[str,
  BaseIndicator]]` — one indicator instance per `(symbol, name)` pair (never shared across
  symbols, since each instance carries its own series state). It exposes the abstract
  `decide_action(symbol, candle, status) -> List[Action]`, called by all three engines once a
  symbol's indicators are updated. `symbol` identifies whose candle just closed and triggered the call,
  but `self.indicators` holds every traded symbol's state, so an implementation can read other
  symbols' indicators too and return `Action`s (each carrying its own `.symbol`) for symbols other
  than the trigger — this is what makes pairs/relative-strength/rotation strategies possible. A
  single-symbol strategy just ignores the `symbol` argument (`self.symbols` has one element) and
  returns `[Action(self.symbols[0], qty)]` or `[]`.
- Indicators come in two types. `BaseIndicator` (ABC) is a loop-updated rolling-window indicator:
  `update(candle, status=None)` ingests one candle plus the *pre-trade* `Status` snapshot — the
  same one `decide_action` saw for that candle (`TradingEngine.warmup` passes `None`, so
  status-aware indicators must treat `None` as warm-up); `get_index(idx)`/`get_latest()`
  read back past values (`-1` = latest, `-2` = previous, ...). An indicator that reads `status`
  cannot be vectorized (account state is a feedback loop of the strategy's own trades) and must
  stay a plain `BaseIndicator` — `streamer/indicator/position_age.py` is the canonical example (it's told
  which symbol it belongs to at construction, since it reads `status.position_for(self._symbol)`
  directly rather than only through `decide_action`).
  An indicator can additionally define
  `precompute_series(open, high, low, close, volume) -> np.ndarray` (element i = `get_latest()`
  after i+1 updates, NaN during warm-up); a vectorized backtest path detects it by attribute
  presence (`getattr(indicator, "precompute_series", None)`, no separate class involved) and uses
  it to compute each symbol's whole series at once (against that symbol's **own** OHLCV arrays).
  That path is currently removed pending reintroduction on the new producer structure, but the
  method is kept (and `update()` must still work for the live trader regardless). `MovingAverage`,
  `MinDonchianIndicator`/`MaxDonchianIndicator` (monotonic-deque min/max over a window),
  `ATRIndicator`, `RollingStd` and the `volume_stats` indicators define it; `ADXIndicator`,
  `SupertrendIndicator`, `PivotTrendlineIndicator`, `TakerImbalanceIndicator` and
  `PositionAgeIndicator` are path-dependent or status-aware and stay plain `BaseIndicator`s
  without it (correct everywhere, just on the loop path). Both kinds mix freely within one
  strategy.
- Each indicator's `scale_group` (default `"price"`) tells the frontend which chart pane to plot
  it on; indicators sharing a group share a pane and price scale (this grouping is keyed by
  indicator **name** only, shared across symbols).
- **Ordering matters**: `TradingEngine.process_event` — the single loop backtest, dry run and live
  all go through — always updates **every** indicator for a symbol with its closed candle *before*
  calling `streamer.decide_action` for that symbol, so
  `get_latest()` includes the candle being decided on and `get_index(-2)` is the previous one —
  the current candle's OHLCV is also available directly as the `candle` argument, and every
  strategy uses it. The `Status` passed to both `update` and `decide_action` is still the
  pre-trade snapshot (fills are only applied after every symbol in the event has been decided),
  so an indicator that reads `status` sees exactly what the decision it's paired with saw. The
  backtester records the series right after `decide_action` returns, once every indicator is
  already updated, so every plotted column is the value the decision actually saw.

  This ordering is a **semantic convention, not a look-ahead guard**: the candle has already
  closed by the time `decide_action` runs, so feeding it to the indicators first leaks no future
  information. It does mean any breakout/extremum comparison against the current candle's own
  OHLC must read `get_index(-2)` instead of `get_latest()` — a Donchian max channel that includes
  the current bar satisfies `channel_max >= candle.high >= candle.close`, making
  `close > channel_max` unfireable. Level-style indicators (MA, ATR, rolling std) read fine at
  `get_latest()` either way. What the convention guarantees is that `get_latest()` means exactly
  one thing, in the backtester and in the live trader alike.

  Look-ahead is actually held out elsewhere: a fill uses its target symbol's decision-candle
  `close` when that symbol is the trigger, or `Status.last_close[symbol]` — frozen before any
  symbol in the current merged event is processed — for a same-event action targeting a different
  symbol; the `Status` handed to both `decide_action` and `update` is the pre-trade snapshot; and
  path-dependent indicators delay their own confirmation internally (`PivotTrendlineIndicator`
  only confirms a fractal pivot k bars later).
- `Action(symbol, quantity, ...)` is a signed `quantity` delta to apply to `symbol`'s current
  position (positive = buy/long, negative = sell/short); it is interpreted identically by the
  backtester's `_trade()` and by `BinanceOrderClient.execute_action` (which reads `action.symbol`
  directly). The two positional arguments alone still mean "market order, filled at the decision
  candle's close", so every pre-existing strategy is unchanged. Beyond them it carries an
  `order_type: ActionType` (`MARKET`/`LIMIT`/`STOP_MARKET`/`CANCEL`) plus `price` (LIMIT),
  `trigger_price`/`trigger_above` (STOP_MARKET, which covers stop-loss *and* take-profit — the
  trigger direction is what separates them, and `trigger_above=None` derives it by comparing the
  trigger to `status.last_close`, the same rule Binance applies implicitly), `reduce_only`,
  `client_id` and `expire_after_candles`. See "Resting orders" below.

  `Action.cancel(symbol, client_id=None)` is the cancel form (a `CANCEL`-typed action with
  `quantity=0`; `client_id=None` cancels every resting order on that symbol). All three engines
  dispatch on `order_type` **before** the `quantity == 0` skip, since a cancel carries no quantity.
  `client_id` is validated against Binance's `newClientOrderId` charset in `Action.__init__`, so an
  id that works in a backtest is guaranteed to work live.
- Example strategies (illustrations of the framework, not tuned or recommended — mostly
  single-symbol, `self.symbols == [symbol]`, migrated to the multi-symbol-capable interface
  mechanically):
  `CrossMovingAverageStreamer` (MA10/MA25 cross — the simplest one, read it first),
  `KeltnerStreamer` (ATR channel breakout; wired into `backtest.py` and `trader.py` — accepts
  `symbols: List[str]` and trades each independently with its own indicator state, but with no
  cross-symbol coupling, since `decide_action` only ever reads the `symbol` it was called with),
  `SupertrendStreamer` (always-in trend flip), `MeanReversionZScoreStreamer` (z-score fade;
  stateful across candles), `MomentumTimeExitStreamer` (momentum + fixed-time exit),
  `VolumeConfirmedMomentumStreamer` (the previous one subclassed to add a volume entry filter),
  `WickRejectionStreamer` (candlestick pattern), `TrendlineBounceStreamer` (pivot-trendline
  geometry), and `TradeStreamer` (alternating long/short every bar — a fixture, not a strategy).
  `KeltnerStopStreamer` subclasses `KeltnerStreamer` to replace its hand-rolled exit with a real
  `reduce_only` `STOP_MARKET`, re-armed each bar (cancel + re-register) so the stop trails the
  channel — it is the reference example for resting orders. The
  other five strategies that emulate a stop by hand (`wick_rejection`, `momentum_time_exit`,
  `mean_reversion_zscore`, `trendline_bounce`, `keltner_streamer`) are deliberately left alone as
  illustrations of the older style; they compare the closed bar's `low`/`high` to a stored stop
  and then fill at that bar's **close**, which is exactly the pricing error resting orders remove.
  No shipped example strategy uses the cross-symbol capability yet (`decide_action` can read and
  act on other symbols' state, but the examples don't). **The `visualise/` frontend does not render multi-symbol
  runs** (multiple price panes, per-symbol trade markers, `summary.by_symbol`) — that's unbuilt
  follow-up work; only the core engine and output schema support multiple symbols today.

### Resting orders (`core/domain/order_book.py`)

`Action` can request an order that is **not** filled on the decision candle: a `LIMIT` at a price,
or a `STOP_MARKET` at a trigger. Those live in `Status.open_orders: Dict[str, List[OpenOrder]]`
until they fill, expire, or are cancelled — open orders really are account state on an exchange,
and putting them on `Status` means `decide_action(symbol, candle, status)` needs no signature
change to read them (`status.open_orders_for(symbol)`; cancel via `Action.cancel`, never by
mutating the list). `order_book.py` holds the one copy of the matching rules, and
`SimulatedExecutor` is the one caller — backtest and dry run are the *same object*, so they cannot
diverge. That matters because dry run exists to be comparable to a backtest.

**Fill rules** (`match_symbol`), against one closed candle `(o, h, l, c)`:

| order | condition | fill price |
|---|---|---|
| LIMIT buy at `L` | `o <= L` | `o` — gapped through, you get the better price |
| | else `l <= L` | `L` |
| LIMIT sell at `L` | `o >= L` | `o` |
| | else `h >= L` | `L` |
| STOP_MARKET, `trigger_above`, trigger `T` | `o >= T` | `o` |
| | else `h >= T` | `T` |
| STOP_MARKET, not `trigger_above` | `o <= T` | `o` |
| | else `l <= T` | `T` |

`slippage_ratio` (a `SimulatedExecutor` constructor argument, default `0.0` — a simulation
modelling knob only this executor and `order_book` see, never on `Status`, never read by a
strategy) is applied **only to STOP_MARKET**, in the adverse
direction. LIMIT fills at your price or better by definition, and MARKET is left alone so existing
backtests are bit-identical.

Assumptions that a candle cannot verify, all deliberate:

- **One fill per order per candle.** No partial fills, no book depth, no queue position.
- **The intrabar path is unknown, so it is resolved pessimistically**: within one symbol's candle,
  STOP_MARKET orders match before LIMIT orders, each group in submission (`seq`) order. A stop-out
  is assumed to happen before a favourable limit fill.
- **An order never fills on the candle that submitted it.** The decision comes out after that
  candle closed, so it could not have been resting during it.
- **`reduce_only` clamps to the current position, and cancels without filling if the position is
  flat or same-signed.** Without it a stop left over from a closed position opens a new opposite
  one. Fills apply one at a time (`match_symbol` is a generator, and the caller applies each fill
  before the next is produced), so later orders in the same candle see the updated position.
- **Forced liquidation cancels the whole book** (`cancel_all`) along with flattening every symbol;
  otherwise a stop left resting would open a ghost position on a bankrupt account.

**Where matching sits in the event** is step 2 of `TradingEngine.process_event` (see "The trading
engine" above) — ahead of `decide_action`, because on a real exchange a resting order fills before
the bar closes. A consequence worth keeping in mind for any future vectorized equity rebuild: a
MARKET fill happens *after* the equity point is recorded so its state applies from the next event
index, while a resting fill happens *before* it so its state applies from this one.

Live never simulates any of this: `LiveExecutor.match_resting` is a no-op and the exchange owns the
book, hydrated at startup from `futures_get_open_orders()` and kept in sync by
`ORDER_TRADE_UPDATE`.

### Position & PnL accounting (`core/domain/status.py`, `trade.py`, `report.py`)

`Status` is the single shared representation of account state, **created and owned by the
executor** (`executor.status` — nothing outside an executor constructs one: `SimulatedExecutor`
takes an `init_margin` scalar and a `fee_ratio` and builds it, `LiveExecutor.create()` reads the
exchange — wallet balance, positions, open orders, **and the taker commission rate** — and builds
it, so a `Status` that has not yet been filled in cannot be observed) and modelling one
**shared cross-margin pool** (`margin`, a bare scalar)
across per-symbol positions (`positions: Dict[str, PositionState]`, each holding `avg_price`,
`position`, `unrealised_pnl`) — matching how Binance USD-M cross margin actually works: a loss on
one symbol draws down the same pool a profit on another symbol credits. `status.position_for(
symbol)` lazily creates a flat `PositionState` for a symbol not yet touched, so callers never
`KeyError` on a symbol they haven't traded. `total_margin()` sums `margin` plus every symbol's
`unrealised_pnl`; `update_leverage()` sums notional (`avg_price * abs(position)`) across every
symbol over that. `status.last_close: Dict[str, float]` is engine-maintained (set to each
symbol's close as it's processed within a merged event, before any symbol's own `decide_action`
runs that event) — it's what lets a cross-symbol action price a non-trigger symbol correctly.
`status.fee_ratio` is the account's fee rate — a `Status` field (default `DEFAULT_FEE_RATIO` in
`core/domain/status.py`) the executor sets: `SimulatedExecutor` from its constructor arg,
`LiveExecutor` from `futures_commission_rate`. Strategies read it when sizing (`price * (1/lev +
status.fee_ratio)`), so the fee the account is charged and the fee the position was sized against
are the same number — this is why the `resolve_fee_ratio` streamer-reconciliation helper no longer
exists.
`Status.apply_fill(symbol, quantity, price)` holds the one copy of the averaging/PNL
math (charging `self.fee_ratio` on the notional) — and it derives realised PnL by **pro-rating
`unrealised_pnl`**, which is only correct when
that field is marked at the fill price. For a market fill that is automatic (the fill price *is*
the close the engine just marked at); for a resting fill at a trigger or limit price it is not, so
`SimulatedExecutor._fill` — the **one** fill path for backtest and dry run alike — calls
`update_unrealised_pnl(symbol, price)` immediately before `apply_fill`. On the market path that
recomputes the same value and changes nothing; on the resting path, omitting it silently books the
wrong realised PnL. `apply_fill` itself models opening, pyramiding (same-direction add), partial
close, full close, and direction-flip on that symbol's `PositionState`, crediting/debiting the
shared `margin`. Live never calls it — the exchange's `ACCOUNT_UPDATE` is the truth there.
A `Trade` is an immutable record of one fill (with its
`symbol`) plus a deep-copied pre-trade `Status`; its `wnl` is realised PnL **before** the fee,
which is carried separately in `fee`. It also carries `order_type` (the `ActionType` value that
produced it) and `submitted_at` — for a market fill that equals `timestamp`, but a resting order is
submitted bars before it fills and one timestamp cannot hold both. `Report` bundles all `Trade`s with `max_leverage`, the final
`Status` and the equity curve.

### Live trading (`core/live/`)

Live trading is **multi-symbol**: `BinanceTrader.symbols` is derived directly from
`streamer.symbols` (there is no separate `symbol`/`symbols` constructor argument, so the trader
can never disagree with the streamer about what it trades), every traded symbol's candles arrive
over **one** multiplexed kline socket, and `ORDER_TRADE_UPDATE`/`ACCOUNT_UPDATE` handling matches
against the full traded-symbol set rather than a single symbol.

`BinanceTrader` is assembly and lifecycle only (~400 lines). It builds the four engine pieces,
opens the sockets and shuts everything down; the trading logic itself is in `core/engine/`. The
one asyncio seam left in it is the user-data socket's lifecycle — the messages themselves go
straight to `executor.on_user_data(...)`.

- **`live/candle_producer.py` — where candles come from.** One futures kline websocket, a
  `futures_multiplex_socket` carrying every symbol's `continuousKline` stream (built as
  `<symbol>_<contract_type>@continuousKline_<interval>` per symbol), via `ReliableWebsocket` (a
  thin wrapper around python-binance's `ReconnectingWebsocket` that recovers from a dropped
  `recv()` by closing/reconnecting the delegate, logging both the failure and the recovery with a
  running `reconnects` count — a reconnect an hour and a reconnect a minute are very different
  operationally, and only logging the failures made that impossible to read off the log).
  Multiplexed messages arrive wrapped as `{"stream": ..., "data": <rawPayload>}`; since the
  continuousKline payload carries no top-level `s`/`k.s`, the symbol is read from `data["ps"]`.
  Since one consumer drives this source and fully runs each `process_event` before pulling the next
  message, *decision* processing across symbols is naturally serialized and needs no extra locking.
  Live order submissions are fired to the thread pool and not awaited, so an order result may land
  after later candles; that race, plus the kline-vs-user-data-listener race, is tolerated because
  `status` is exchange truth and the pre-trade snapshot is deep-copied before the order is sent.
  - **Gaps and duplicates are detected per symbol** (each tracks its own `_last_candle_start`,
    advanced *before* the candle is yielded so a consumer exception cannot cause a re-run). A gap
    is filled by **yielding the missing candles first** — so "backfilled candles take the same path
    as live ones" is a property of stream order rather than something the processing path has to
    re-enter. A gap raises no exception on its own (`ReliableWebsocket` only recovers from a
    throwing `recv()`, so a silently dropped kline would otherwise leave the log quiet while the
    hole propagates into every rolling-window indicator and the recorded series); the check
    compares `start_time + interval_ms`, not `end_time`, because a websocket kline's `T` is
    `start + interval - 1` (the fetcher's `end_time` is exclusive; the stream's is not) — the
    producer normalizes `end_time` to the interval boundary for exactly this reason.
  - A misaligned boundary, a gap larger than `MAX_BACKFILL_CANDLES`, or a failed backfill **ends
    the stream**; the trader then stops and a restart recovers (warm-up rebuilds the indicators,
    the exchange owns the position).
  - `warmup_candles(windows)` fetches each symbol's indicator history (via `BinanceCandleFetcher`,
    off the event loop through `asyncio.to_thread`, one symbol at a time), asserting each fetched
    range exactly matches what was expected. Every symbol shares the same interval-boundary
    `end_time` (not recomputed per symbol) so their windows stay aligned despite the sequential
    fetches, and the range is floored to the **interval** boundary, not the minute, so the
    assertion holds for every interval and not just `1m`.
- **`live/executor.py` — orders and account state.** `submit()` fires the order and returns `None`;
  a detached task (`_await_order_result`) awaits the submission result and routes a failure to
  `on_error`. The fill arrives later on the user-data stream, so the pre-trade `Status`
  snapshot is deep-copied *before* the order is sent and keyed by `(symbol, client order id)` —
  every order
  gets a `newClientOrderId` (auto-generated for market orders, the strategy's `client_id` for
  resting ones) which the exchange echoes back as `c`, so a resting order that fills hours later is
  still paired with the decision that created it.
  - **Construction is hydration.** `LiveExecutor.create()` is the only way to build one: it reads
    `futures_account()` and `futures_get_open_orders()` and hands those payloads to `build_status`
    / `order_from_exchange` — pure functions, so the accounting rules are checkable without a
    client — and returns an executor whose `Status` is already exchange truth. An unhydrated
    `Status` therefore cannot be observed; before this the caller passed in a placeholder
    `Status(margin=0.0)` that a later `load_account()` overwrote, and reading it in between (the
    live recorder takes `init_margin` from it) silently produced a run with `init_margin` 0.
    Balance comes from `walletBalance`, not `marginBalance` — the latter already includes
    unrealised PnL, which `total_margin()` adds again. A `futures_account()` failure is fatal; a
    `futures_get_open_orders()` failure is not (the book looks empty and `ORDER_TRADE_UPDATE`
    refills it). After startup the state is kept in sync by `ACCOUNT_UPDATE`/`ORDER_TRADE_UPDATE`.
  - **Clients are self-created unless injected**, and `close()` only tears down what it created.
    `BinanceTrader` passes its own `AsyncClient` (the socket manager shares it) but not the
    `BinanceOrderClient`, so that thread pool now exists only in live mode — it used to be built
    in `BinanceTrader.__init__` for dry run too, which never sends an order. The check scripts use
    the injection path with fakes.
    `ACCOUNT_UPDATE` carries **only changed** balances/positions, so the handler leaves `Status`
    untouched for symbols/assets absent from the event rather than zeroing them, and iterates the
    full `P` array without stopping at the first match — one event can carry position deltas for
    several of our symbols at once. `_order_agg` is keyed by `(symbol, order_id)`, not `order_id`
    alone, since nothing guarantees order IDs are unique across symbols for one account.
  - Resting orders are never simulated here and forced liquidation is never performed — the
    exchange does both (`match_resting` is a no-op, `force_liquidation` always returns `False`).
    `status.open_orders` is hydrated at startup from `futures_get_open_orders()` and kept in sync
    by `ORDER_TRADE_UPDATE` (`NEW` adds, a terminal state removes).
  - `reconcile_resumed(saved)` compares a resumed run's remembered positions **and** open orders
    against the exchange's actual ones and warns on any difference — a liquidation/ADL, a manual
    order, or a fill event missed while the process was down. It only reports: live `Status` is
    exchange truth and is left alone. The order reconciliation matters more than the position one,
    since the exchange keeps working a stop while this process is down.
  - Every traded symbol must settle in the same margin asset, since `Status.margin` is one pool
    shared across all of them — `resolve_margin_asset()` asserts this once at construction, in
    **both** modes.
- **Dry run uses `SimulatedExecutor`** — the backtest's own class. Margin starts at a fixed
  synthetic `1e6`, no user-data socket is opened, no orders are sent, and fills/resting orders go
  through exactly the code a backtest runs. `core/checks/live_check.py` asserts that a dry run and a
  backtest of the same candles produce identical trades.
- `start()` builds the `BinanceSocketManager` first, then **the executor** (`__init__` no longer
  builds one — `BinanceTrader.executor` is `None` until `start()`, because building the live one
  reads the exchange and so needs the `AsyncClient`; `trader.status` is a property onto
  `executor.status`), then the recorder and engine, warms up indicators *before* opening any socket (so the user-data queue
  can't overflow during a long backfill), sets `is_running = True` *before* spawning listener tasks
  (they loop on that flag), and **re-raises** on failure — a trader that could not start must not
  look like one that did. Listener tasks are kept in `self._tasks` so asyncio can't garbage-collect
  them mid-flight. On success it logs one line naming the whole configuration — mode
  (LIVE/DRY-RUN), symbols, interval, testnet, streamer, fee ratio, run id. `dry_run` appears
  nowhere else in the log, so without it a strategy that goes days without trading gives no way to
  tell whether real money is at stake.
- With `record=True` (the `RECORD` env var, default on) the trader owns a `LiveRecorder` and
  persists the session to `asset/live/` in backtest format — see "Live run output" above for the
  recording seams, restart-resume behaviour and the live/backtest divergences to expect.
- `BinanceOrderClient` is the low-level order client: it submits to a `ThreadPoolExecutor` (GIL-free
  from the asyncio loop) with exponential-backoff retries, returning a
  `concurrent.futures.Future[OrderResult]` that `LiveExecutor._await_order_result()` awaits via
  `asyncio.wrap_future` in a detached task (bounded by `ORDER_RESULT_TIMEOUT`). It returns
  `success=False` rather than raising once retries are exhausted, and `_await_order_result` checks
  that, routing the failure to `on_error` — otherwise a permanently rejected order passes without a
  trace and the strategy drifts from the exchange. `execute_action` maps `ActionType` onto the exchange's
  order types: `LIMIT` gains `timeInForce=GTC` (Binance rejects a LIMIT without it), and a
  `STOP_MARKET` becomes `STOP_MARKET` or `TAKE_PROFIT_MARKET` depending on which side of
  `reference_price` its trigger sits — the exchange rejects a conditional order whose trigger is on
  the wrong side, so the caller passes `status.last_close[symbol]` as that reference. A `CANCEL`
  action routes to `cancel_order`, which also accepts `origClientOrderId` (how a strategy addresses
  its own order) and cancels every open order on the symbol when given neither id.

There is no `add_action_callback` any more — the `Executor` is the action seam. To observe or alter
what gets executed, wrap `trader.executor`. `add_error_callback` remains.

## Conventions

- Code comments and docstrings are in Korean; documentation files (`README.md`, `CLAUDE.md`,
  `core/live/README.md`) are in English. Match whatever the file you are editing already uses.
- `asset/` is gitignored and holds the candle cache, backtest output (`asset/backtest/`) and live
  run output (`asset/live/`). Never commit it. `docker-compose.yml` bind-mounts it so live runs
  survive container recreation.
- Never commit `.env`. `.env.sample` holds placeholders only.
