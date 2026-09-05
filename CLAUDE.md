# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this
repository.

## Project overview

An algorithmic trading system for Binance USD-M Futures with two independent halves:

- `core/` — Python engine: historical candle fetching, two multi-symbol-capable backtesters (a
  reference implementation and a vectorized fast path — see "Data flow" below for how symbols'
  candles are merged into one event timeline), a pluggable, cross-symbol-aware streamer/indicator
  strategy framework with market, limit and conditional (stop/take-profit) orders, and an asyncio live trader/executor that is also multi-symbol (see "Live
  trading").
- `visualise/` — a Create React App frontend that reads the JSON files the backtester writes and
  renders them with `lightweight-charts`. It has no backend of its own; the user picks the `asset/`
  directory with the File System Access API and everything is parsed client-side. **Does not yet
  render multi-symbol runs** — see "Backtest output format" below.

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

3. **Backtest.** The backtesters are multi-symbol: `backtest/SingleThreadedBacktester.py` takes
   `Dict[str, List[Candle]]` (one ragged list per symbol — symbols don't need to start/end at the
   same time) and merges them via `backtest/candle_merge.merge_candle_timeline` into one
   chronological stream of **events**, where an event bundles every symbol whose candle closes at
   that exact timestamp. Within an event, symbols are processed in `streamer.symbols` order:
   mark-to-market → before-indicators → `decide_action` → record → after-indicators, and a
   strategy's `decide_action` can read every symbol's indicator state and return `Action`s for
   symbols other than the one that triggered the call (see "Streamer/indicator strategy
   framework" below) — a same-event action targeting another symbol fills at that symbol's
   `Status.last_close`, frozen before any symbol in the event is processed, so cross-symbol fills
   are deterministic regardless of `streamer.symbols` order. `Status.apply_fill` is updated per
   symbol; `margin` is one pool shared across all symbols (matches Binance cross margin). It also
   force-liquidates — flattening *every* open position, since the shared pool means one symbol's
   loss can force-close the rest — when mark-to-market equity (`Status.total_margin()`, margin
   plus the sum of every symbol's unrealised PnL) falls to zero or below.

   `backtest/FastBacktester.py` is the drop-in fast version the entry scripts use: it precomputes
   every indicator that defines `precompute_series` **per symbol** as a numpy array (swapped in
   as a cursor-backed `ArrayIndicator` shim whose cursor advances only on events where that
   symbol appears, not on the shared event index), keeps plain `BaseIndicator`s without it
   loop-updated, rebuilds the equity curve
   vectorized (a per-symbol forward-filled close matrix dot the per-segment position vector,
   generalizing the single-symbol piecewise-constant trick), and bulk-writes shards after the
   loop. `SingleThreadedBacktester` stays as the reference implementation; parity between the two
   (including multi-symbol cases — overlapping symbols, a staggered-start symbol, cross-symbol
   actions, forced liquidation across symbols) is asserted by `backtest_fast_check.py`.

4. **Visualise.** `visualise/` reads the run JSON and shards client-side — it does not talk to
   Binance or the Python code at all.

### Backtest output format (`core/backtest/result_writer.py`)

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
  by `backtest/metrics.py`; `compute_sharpe` resamples to daily before annualising because
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
  for that row — the existing warm-up-null convention, no new sentinel. `SingleThreadedBacktester`
  streams these out through `ShardWriter` (only the current month is in memory); `FastBacktester`
  bulk-writes them via `write_series_shards` after the loop. The frontend loads them lazily per
  viewport.

`run_id` is `<StreamerClassName>_<YYYYmmdd_HHMMSS>`. `series.column_groups` carries each
indicator's `scale_group`, keyed by indicator **name** only (shared meaning across symbols, a
deliberate simplification) — it decides whether the frontend overlays it on the price pane or
gives it its own.

All writes go through `result_writer._write_json`, which wraps `boltons.fileutils.atomic_save`
(part file → fsync → atomic rename). Nothing ever observes a half-written run JSON or shard, which
matters most for the live recorder below — it rewrites the same files continuously — but also means
an interrupted backtest leaves no corrupt output. The part file is `<dest>.part`, deliberately not
ending in `.json`, because the frontend's directory scanner collects every `*.json` it finds.

### Live run output (`core/trader/live_recorder.py`)

`LiveRecorder` writes live and dry-run sessions to `asset/live/` in **exactly** the format above,
so the visualiser loads both from one directory list (`filterRunFiles` accepts `backtest/` and
`live/`) and `metadata.mode` (`"live"`/`"dry"`) drives a LIVE/DRY badge. It reuses `ShardWriter`,
`write_run_json`, `build_summary` and `metrics.py` unchanged; the only backtester-side change was
splitting `ShardWriter._flush` into `checkpoint()` (write the current month, keep the buffer) and
buffer reset, since a live process rewrites one month's shard for weeks.

**`LiveRecorder`/`BinanceTrader` are multi-symbol**, trading every symbol in `streamer.symbols`
concurrently over one multiplexed kline socket (see "Live trading" below) and writing the same
multi-symbol `Status`/shard shapes described above — `LiveRecorder.record_candle(symbol, candle,
status)` is called once per symbol's own candle close (live events are not merged/batched across
symbols the way backtest events are), with a single-key `{symbol: (candle, values)}` dict, which
already matches `ShardWriter.add`'s existing ragged-series contract with no changes needed there.

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
- **Recording points mirror the backtester exactly.** `record_candle` is called between the two
  indicator-update groups in `_handle_candle` — the same seam `ShardWriter.add` occupies in
  `SingleThreadedBacktester.run` — so every plotted column is the value the decision actually saw.
  Moving it after the `after_indicators` loop would shift every default indicator one candle
  relative to a backtest, breaking the one comparison this feature exists for.
- **Resting orders.** Dry-run keeps its own book in `Status.open_orders` and fills it with the same
  `order_book.match_symbol` the backtesters use, called from `_handle_candle` at the same seam
  (before the indicator update and `decide_action`) — so a dry run over some candles produces
  exactly the trades a backtest of those candles does, stop fills included. `metadata.last_status`
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
  snapshot is deep-copied *before* the order is dispatched, because `_on_action` awaits the order
  future and `ACCOUNT_UPDATE` can overwrite `self.status` during that await. For a **market** fill
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
  limit can resolve differently; `BinanceExecutor.execute_action` does not quantize to step size,
  so the filled quantity can differ from the requested action (`_record_live_fill` warns past
  `_QUANTITY_DIVERGENCE_TOLERANCE`, 1%, so the size of that drift is visible rather than
  merely expected); prefeed candles are not recorded, so live indicator columns have no
  NaN warm-up prefix; `update_unrealised_pnl` marks with last price while the exchange marks with
  mark price; and BNB-denominated commissions (`N` != margin asset) are excluded from `fee` and
  flagged as `metadata.fee_asset_mismatch`.

### Logging (`core/logging_config.py`)

Every entry point calls `setup_logging()` before doing anything else — `core/examples/trader.py`,
`core/examples/backtest.py`, `core/backtest_fast_check.py` and `core/fetch_stock_check.py`. It is
the only place `logging.basicConfig` is called. Without it, `logger.info` from the fetchers and
backtester is dropped entirely and `WARNING`+ falls through to Python's `logging.lastResort`
handler, which prints a bare message with no level, logger name or timestamp — which is how the
two warnings that decide whether a backtest is trustworthy (a month chunk with a data gap in
`candle_fetcher/base.py`, a Sharpe truncated at bankruptcy in `backtest/metrics.py`) used to come
out, as anonymous lines between tqdm bars.

Level resolution is `level` argument > `LOG_LEVEL` env var > the entry point's `default` (INFO for
the trader and backtest, WARNING for the two check scripts, whose verdicts are printed). The
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
(`file=sys.stdout` in both backtesters and both Binance fetchers), so a warning raised
mid-fetch no longer shreds the bar it prints through. Library code under `core/` logs;
it does not `print`. The remaining `print` calls are deliberate CLI report output —
`examples/backtest.py`'s result summary and the two `*_check.py` verdicts — which is why
those two scripts default to WARNING.

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
  same one `decide_action` saw for that candle (the live trader's `_prefeed_indicators` passes
  `None`, so status-aware indicators must treat `None` as warm-up); `get_index(idx)`/`get_latest()`
  read back past values (`-1` = latest, `-2` = previous, ...). An indicator that reads `status`
  cannot be vectorized (account state is a feedback loop of the strategy's own trades) and must
  stay a plain `BaseIndicator` — `indicator/position_age.py` is the canonical example (it's told
  which symbol it belongs to at construction, since it reads `status.position_for(self._symbol)`
  directly rather than only through `decide_action`).
  An indicator can additionally define
  `precompute_series(open, high, low, close, volume) -> np.ndarray` (element i = `get_latest()`
  after i+1 updates, NaN during warm-up); `FastBacktester` detects it by attribute presence
  (`getattr(indicator, "precompute_series", None)`, no separate class involved) and uses it to
  compute each symbol's whole series at once (against that symbol's **own** OHLCV arrays) —
  `update()` must still work for the live trader. `MovingAverage`,
  `MinDonchianIndicator`/`MaxDonchianIndicator` (monotonic-deque min/max over a window),
  `ATRIndicator`, `RollingStd` and the `volume_stats` indicators define it; `ADXIndicator`,
  `SupertrendIndicator`, `PivotTrendlineIndicator`, `TakerImbalanceIndicator` and
  `PositionAgeIndicator` are path-dependent or status-aware and stay plain `BaseIndicator`s
  without it (correct everywhere, just on the loop path). Both kinds mix freely within one
  strategy.
- Each indicator's `scale_group` (default `"price"`) tells the frontend which chart pane to plot
  it on; indicators sharing a group share a pane and price scale (this grouping is keyed by
  indicator **name** only, shared across symbols).
- **Ordering matters**: all three engines (`SingleThreadedBacktester`, `FastBacktester`,
  `BinanceTrader._handle_candle`) always update **every** indicator for a symbol with its closed
  candle *before* calling `streamer.decide_action` for that symbol, so
  `get_latest()` includes the candle being decided on and `get_index(-2)` is the previous one —
  the current candle's OHLCV is also available directly as the `candle` argument, and every
  strategy uses it. The `Status` passed to both `update` and `decide_action` is still the
  pre-trade snapshot (fills are only applied after every symbol in the event has been decided),
  so an indicator that reads `status` sees exactly what the decision it's paired with saw. Both
  backtesters record the series right after `decide_action` returns, once every indicator is
  already updated, so every plotted column is the value the decision actually saw.

  In `FastBacktester`, each symbol's `ArrayIndicator` shims are just called via the same
  `.update()` as everything else — every appearance of that symbol in the merged event stream
  advances their `cursor` by one before the decision reads it, so no manual index bookkeeping is
  needed even though the shared merged-event index and a symbol's own candle-index don't coincide
  — and `_build_symbol_series_columns` scatters each precomputed series onto the shared event
  grid unshifted (the decide-time value at that symbol's j-th own candle is always `seq[j]`).

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
  backtester's `_trade()` and by `BinanceExecutor.execute_action` (which reads `action.symbol`
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
  channel — it is the reference example for resting orders and doubles as a parity fixture. The
  other five strategies that emulate a stop by hand (`wick_rejection`, `momentum_time_exit`,
  `mean_reversion_zscore`, `trendline_bounce`, `keltner_streamer`) are deliberately left alone as
  illustrations of the older style; they compare the closed bar's `low`/`high` to a stored stop
  and then fill at that bar's **close**, which is exactly the pricing error resting orders remove.
  A genuinely cross-symbol strategy exists only as a test fixture so far
  (`RelativeStrengthRotationStreamer` in `backtest_fast_check.py`) — no shipped example strategy
  uses the cross-symbol capability yet. **The `visualise/` frontend does not render multi-symbol
  runs** (multiple price panes, per-symbol trade markers, `summary.by_symbol`) — that's unbuilt
  follow-up work; only the core engine and output schema support multiple symbols today.

### Resting orders (`core/backtest/order_book.py`)

`Action` can request an order that is **not** filled on the decision candle: a `LIMIT` at a price,
or a `STOP_MARKET` at a trigger. Those live in `Status.open_orders: Dict[str, List[OpenOrder]]`
until they fill, expire, or are cancelled — open orders really are account state on an exchange,
and putting them on `Status` means `decide_action(symbol, candle, status)` needs no signature
change to read them (`status.open_orders_for(symbol)`; cancel via `Action.cancel`, never by
mutating the list). `order_book.py` holds the one copy of the matching rules, shared by
`SingleThreadedBacktester`, `FastBacktester` and the live trader's dry-run path — if those three
diverged, dry-run would stop being comparable to a backtest, which is the only reason dry-run
exists.

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

`slippage_ratio` (a constructor argument on both backtesters, resolved from the streamer by the
same rule as `fee_ratio`, default `0.0`) is applied **only to STOP_MARKET**, in the adverse
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

**Where matching sits in the event** — this ordering is the load-bearing part:

```
1. freeze status.last_close[symbol] for every symbol in the event
2. match resting orders against each symbol's candle  ← fills happen here
3. mark-to-market every position; append the equity point
4. bankruptcy check → cancel_all + flatten every symbol
5. per symbol: update indicators → decide_action → record the shard row
6. action loop: CANCEL → cancel; LIMIT/STOP_MARKET → register; MARKET → fill at last_close
```

Matching is at step 2, ahead of `decide_action`, because on a real exchange a resting order fills
before the bar closes — put it after and the strategy decides while believing it still holds a
position its stop already closed. That has a consequence for `FastBacktester`'s vectorized equity
rebuild: a MARKET fill happens *after* the equity point is recorded so its state applies from
`idx + 1`, while a resting fill happens *before* it so its state applies from `idx` itself. Each
entry in `trade_marks` therefore carries an explicit `effective_from` instead of the old implicit
`idx + 1`, and `_build_equity_curve` slices on that.

`SingleThreadedBacktester` owns the shared helpers (`_fill`, `_match_resting_orders`,
`_submit_book_action`) and `FastBacktester` inherits them, so both engines cannot drift apart.

### Position & PnL accounting (`core/backtest/status.py`, `trade.py`, `report.py`)

`Status` is the single shared representation of account state used by *both* the backtester and
the live `BinanceTrader`, modelling one **shared cross-margin pool** (`margin`, a bare scalar)
across per-symbol positions (`positions: Dict[str, PositionState]`, each holding `avg_price`,
`position`, `unrealised_pnl`) — matching how Binance USD-M cross margin actually works: a loss on
one symbol draws down the same pool a profit on another symbol credits. `status.position_for(
symbol)` lazily creates a flat `PositionState` for a symbol not yet touched, so callers never
`KeyError` on a symbol they haven't traded. `total_margin()` sums `margin` plus every symbol's
`unrealised_pnl`; `update_leverage()` sums notional (`avg_price * abs(position)`) across every
symbol over that. `status.last_close: Dict[str, float]` is engine-maintained (set to each
symbol's close as it's processed within a merged event, before any symbol's own `decide_action`
runs that event) — it's what lets a cross-symbol action price a non-trigger symbol correctly.
`Status.apply_fill(symbol, quantity, price, fee_ratio)` holds the one copy of the averaging/PNL
math — and it derives realised PnL by **pro-rating `unrealised_pnl`**, which is only correct when
that field is marked at the fill price. For a market fill that is automatic (the fill price *is*
the close the engine just marked at); for a resting fill at a trigger or limit price it is not, so
every fill path (`SingleThreadedBacktester._fill`, inherited by `FastBacktester`, and both of the
live trader's fill sites) calls `update_unrealised_pnl(symbol, price)` immediately before
`apply_fill`. On the market path that recomputes the same value and changes nothing; on the
resting path, omitting it silently books the wrong realised PnL. `apply_fill` itself models
opening, pyramiding (same-direction add), partial close, full close, and
direction-flip on that symbol's `PositionState`, crediting/debiting the shared `margin`;
`SingleThreadedBacktester._trade` is a thin wrapper over it (inherited unchanged by
`FastBacktester`), and `BinanceTrader`'s dry-run path calls it directly, so dry-run equity tracks
a backtest of the same candles exactly. A `Trade` is an immutable record of one fill (with its
`symbol`) plus a deep-copied pre-trade `Status`; its `wnl` is realised PnL **before** the fee,
which is carried separately in `fee`. It also carries `order_type` (the `ActionType` value that
produced it) and `submitted_at` — for a market fill that equals `timestamp`, but a resting order is
submitted bars before it fills and one timestamp cannot hold both. `Report` bundles all `Trade`s with `max_leverage`, the final
`Status` and the equity curve.

### Live trading (`core/trader/`)

Live trading is **multi-symbol**: `BinanceTrader.symbols` is derived directly from
`streamer.symbols` (there is no separate `symbol`/`symbols` constructor argument, so the trader
can never disagree with the streamer about what it trades), every traded symbol's candles arrive
over **one** multiplexed kline socket, and `ORDER_TRADE_UPDATE`/`ACCOUNT_UPDATE` handling matches
against the full traded-symbol set rather than a single symbol.

- `BinanceTrader` is asyncio-based: it opens one futures kline websocket — a
  `futures_multiplex_socket` carrying every symbol's `continuousKline` stream (built as
  `<symbol>_<contract_type>@continuousKline_<interval>` per symbol, the same stream
  `kline_futures_socket` subscribes to under the hood for one symbol) — and, unless `dry_run`, one
  futures user-data websocket, both via `ReliableWebsocket` (a thin wrapper around python-binance's
  `ReconnectingWebsocket` that recovers from a dropped `recv()` by closing/reconnecting the
  delegate, logging both the failure and the recovery with a running `reconnects` count —
  a reconnect an hour and a reconnect a minute are very different operationally, and only
  logging the failures made that impossible to read off the log). Multiplexed messages arrive
  wrapped as `{"stream": ..., "data": <rawPayload>}`; since the continuousKline payload carries no
  top-level `s`/`k.s`, the symbol is read from `data["ps"]` instead. There is exactly one listener
  task pulling from this socket, and it fully awaits each candle's processing (including order
  dispatch) before receiving the next message — so candle processing across symbols is naturally
  serialized and needs no extra locking; the only pre-existing race is kline processing vs. the
  separate user-data listener task, already handled by deep-copying `Status` before an order is
  dispatched (see the "Fills" bullet under "Live run output" above).
  On each *closed* kline it builds a `Candle`, calls the streamer with that candle's symbol
  (`streamer.decide_action(symbol, candle, status) -> List[Action]` — the returned actions may
  target other symbols too, for cross-symbol strategies), updates that symbol's indicators, and
  fires registered action/error callbacks (`add_action_callback`/`add_error_callback`).
  `_ensure_continuity`/`_fetch_missing_candles` detect and backfill gaps **per symbol** (each
  symbol tracks its own `_last_candle_start`), replaying missed candles through the same
  `_handle_candle` path a live candle takes — a gap raises no exception on its own (`
  ReliableWebsocket` only recovers from a throwing `recv()`, so a silently dropped kline would
  otherwise leave the log quiet while the hole propagates into every rolling-window indicator and
  the recorded series); it compares `start_time + interval_ms`, not `end_time`, because a
  websocket kline's `T` is `start + interval - 1` (the fetcher's `end_time` is exclusive; the
  stream's is not). `_prefeed_indicators()` backfills each symbol's indicator windows with
  historical candles (via `BinanceCandleFetcher`, off the event loop through `asyncio.to_thread`,
  one symbol at a time) before going live, asserting each fetched range exactly matches what's
  expected; every symbol shares the same interval-boundary `end_time` (not recomputed per symbol)
  so their windows stay aligned with each other despite the sequential fetches. The range is
  floored to the **interval** boundary, not the minute, so the assertion holds for every interval
  and not just `1m`.
  - Every traded symbol must settle in the same margin asset, since `Status.margin` is one pool
    shared across all of them — `_resolve_margin_asset()` asserts this once at construction and
    caches the result as `self._margin_asset`, used everywhere a per-symbol margin-asset lookup
    used to happen.
  - `start()` builds the `BinanceSocketManager` first, prefeeds *before* opening any socket (so the
    user-data queue can't overflow during a long backfill), sets `is_running = True` *before*
    spawning listener tasks (they loop on that flag), and **re-raises** on failure — a trader that
    could not start must not look like one that did. Listener tasks are kept in `self._tasks` so
    asyncio can't garbage-collect them mid-flight. On success it logs one line naming the whole
    configuration — mode (LIVE/DRY-RUN), symbols, interval, testnet, streamer, fee ratio, run id.
    `dry_run` appears nowhere else in the log, so without it a strategy that goes days without
    trading gives no way to tell whether real money is at stake.
  - `_reconcile_resumed_position` (live only) compares, **per symbol**, the position the resumed
    run remembers (`LiveRecorder.resumed_status`) against the exchange's actual position and warns
    when any differ — a liquidation/ADL, a manual order, or a fill event missed while the process
    was down. It only reports: live `Status` is exchange truth and is left alone.
  - In `dry_run` mode: margin starts at a fixed synthetic `1e6`, no user-data socket is opened and
    no orders are sent; fills are applied locally through `Status.apply_fill` with the streamer's
    `fee_ratio`, which is the same accounting the backtester runs.
  - In live mode: `Status` is hydrated from `futures_account()` at startup for every traded symbol
    (from `walletBalance`, not `marginBalance` — the latter already includes unrealised PnL, which
    `total_margin()` adds again) and then kept in sync by `ACCOUNT_UPDATE`/`ORDER_TRADE_UPDATE`
    events off the user-data stream. `ACCOUNT_UPDATE` carries **only changed** balances/positions,
    so the handler leaves `Status` untouched for symbols/assets absent from the event rather than
    zeroing them, and iterates the full `P` array without stopping at the first match — one event
    can carry position deltas for several of our symbols at once. `_order_agg` is keyed by
    `(symbol, order_id)`, not `order_id` alone, since nothing guarantees order IDs are unique
    across symbols for one account.
  - With `record=True` (the `RECORD` env var, default on) the trader owns a `LiveRecorder` and
    persists the session to `asset/live/` in backtest format — see "Live run output" above for the
    recording seams, restart-resume behaviour and the live/backtest divergences to expect.
- `BinanceExecutor` submits orders to a `ThreadPoolExecutor` (GIL-free from the asyncio loop) with
  exponential-backoff retries, returning a `concurrent.futures.Future[OrderResult]`; callers (e.g.
  `BinanceTrader._on_action`) block on `future.result(timeout=...)` from within an async callback.
  It was already symbol-agnostic before this — `execute_action` reads `action.symbol` directly and
  needed no changes to support multiple symbols. `execute_action` maps `ActionType` onto the
  exchange's order types: `LIMIT` gains `timeInForce=GTC` (Binance rejects a LIMIT without it),
  and a `STOP_MARKET` becomes `STOP_MARKET` or `TAKE_PROFIT_MARKET` depending on which side of
  `reference_price` its trigger sits — the exchange rejects a conditional order whose trigger is on
  the wrong side, so the caller passes `status.last_close[symbol]` as that reference. A `CANCEL`
  action routes to `cancel_order`, which now also accepts `origClientOrderId` (how a strategy
  addresses its own order) and cancels every open order on the symbol when given neither id.

## Conventions

- Code comments and docstrings are in Korean; documentation files (`README.md`, `CLAUDE.md`,
  `core/trader/README.md`) are in English. Match whatever the file you are editing already uses.
- `asset/` is gitignored and holds the candle cache, backtest output (`asset/backtest/`) and live
  run output (`asset/live/`). Never commit it. `docker-compose.yml` bind-mounts it so live runs
  survive container recreation.
- Never commit `.env`. `.env.sample` holds placeholders only.
