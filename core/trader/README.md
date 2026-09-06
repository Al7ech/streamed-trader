# Trader package

Live Binance USD-M Futures trading: it feeds closed candles from a websocket into the same
`BaseStreamer` you backtested, and executes the resulting `Action`s.

The entry point is `core/examples/trader.py` — this package is the machinery behind it. See the
repository `README.md` for the end-to-end setup and `CLAUDE.md` for the architecture.

## How it fits together

The trading order-of-operations lives **once**, in `core/engine/` (`TradingEngine.process_event`).
This package supplies the live implementations of the three pluggable pieces around it:

| | CandleProducer | Executor | Recorder |
|---|---|---|---|
| backtest | `BacktestCandleProducer` | `SimulatedExecutor` | `BacktestRecorder` |
| dry run | `LiveCandleProducer` | **`SimulatedExecutor`** | `LiveRecorder` |
| live | `LiveCandleProducer` | `LiveExecutor` | `LiveRecorder` |

Dry run and backtest use **literally the same executor class**. Dry run exists to be compared
against a backtest, so the fill rules, accounting and resting-order book must be one copy, not two
that happen to agree. `core/live_check.py` asserts that a dry run and a backtest of the same
candles produce identical trades.

## Components

### `BinanceTrader.py`

Assembly and lifecycle only (~400 lines). It builds the producer / executor / recorder / engine,
opens the sockets, and shuts everything down. It owns the user-data socket's *lifecycle* but not
its meaning — messages are routed straight to `executor.on_user_data(...)`.

- Every traded symbol must settle in the same margin asset, since `Status.margin` is one pool
  shared across all of them; this is asserted at construction in **both** modes.
- Startup order is load-bearing and commented in `start()`: client → socket manager + producer →
  (live) account/open-order hydration → recorder → engine → indicator warm-up → `is_running` →
  sockets. Warm-up happens before any socket opens because it can take tens of seconds, and an
  unread user-data stream overflows python-binance's queue.
- `start()` re-raises on failure — a trader that could not start must not look like one that did.

### `live_candle_producer.py`

Everything about *where candles come from*:

- Opens **one** futures kline websocket — a `futures_multiplex_socket` carrying every traded
  symbol's continuous-kline stream — wrapped in `ReliableWebsocket` (reconnects when `recv()`
  drops). Combined-stream messages arrive as `{"stream": ..., "data": <rawPayload>}`; since the
  continuousKline payload carries no top-level `s`/`k.s`, the symbol is read from `data["ps"]`.
- Yields one event per closed candle, `{symbol: candle}` — live does **not** merge symbols the way
  a backtest does. `end_time` is normalized to the interval boundary (the websocket's `T` is
  `boundary - 1ms`, the fetcher uses the boundary) so live and backfilled candles line up.
- Detects gaps and duplicates **per symbol**. A gap is filled by *yielding the missing candles
  first* — so "backfilled candles take the same path as live ones" is a property of the stream
  order rather than something the processing path has to re-enter. A misaligned boundary, a gap
  larger than `MAX_BACKFILL_CANDLES`, or a failed backfill ends the stream (the trader then stops,
  and a restart recovers: warm-up rebuilds the indicators and the exchange owns the position).
- `warmup_candles(windows)` fetches each symbol's indicator history. All symbols share the same
  interval-boundary `end_time` so their windows stay aligned even though the fetches are sequential.

Because a single consumer drives this source and fully runs each `process_event` before pulling the
next message, *decision* processing across symbols is naturally serialized — no extra locking is
needed. Live order submissions are fired to the thread pool and not awaited, so an order result may
land after later candles; that is tolerated because `status` is exchange truth.

### `live_executor.py`

Order execution and account state, against the real exchange:

- `submit(action)` fires the order and returns `None`; a detached task (`_await_order_result`)
  awaits the submission result and routes a failure to `on_error`. The **fill arrives later** on
  the user-data stream. The pre-trade `Status` snapshot is deep-copied before the order is sent and
  keyed by client order id, so a resting order that fills hours later is still paired with the
  decision that created it.
- `status` is exchange truth. `ACCOUNT_UPDATE` sets margin/positions (only for entries actually
  present in the event — it carries *changed* items only), `ORDER_TRADE_UPDATE` syncs the open-order
  book. `apply_fill` is never called here.
- Fills are aggregated per `(symbol, order_id)` and emitted as **one** `Trade` on a terminal order
  state, so "one action = one fill" matches the backtest shape even when the exchange fills in
  parts. Commissions in a non-margin asset (BNB) are excluded from `fee` and flagged as
  `metadata.fee_asset_mismatch`.
- Resting orders are not simulated and forced liquidation is not performed — the exchange does both
  (`match_resting` is a no-op, `force_liquidation` always returns `False`).
- `reconcile_resumed(saved)` compares a resumed run's remembered positions and open orders against
  the exchange's actual ones and warns on any difference. It only reports; the exchange wins.

### `live_recorder.py`

Persists the session to `<result_path>/live/` in **exactly** the format a backtest writes to
`asset/backtest/`, so a live run and a backtest of the same strategy can be loaded side by side in
`visualise/`. Reuses `ShardWriter` / `write_run_json` / `build_summary` unchanged.

- `run_id` is fixed (`<live|dry>_<Streamer>_<SYM1-SYM2-...>_<INTERVAL>`, symbols sorted and
  hyphen-joined), so a restart resumes the same run rather than starting a new file. State is
  replayed from the shards on startup.
- Changing strategy params, the traded symbol set, or indicator columns forks a new run instead of
  appending mismatched data to the old shards.
- Writes are atomic (`boltons.fileutils.atomic_save`). `record_trade` checkpoints immediately —
  live fills arrive outside the event loop's turn, so they cannot wait for the end of an event.
  `end_event` rewrites the run JSON every candle and the month shard every `shard_flush_every`.

See the "Live run output" section of the repo-root `CLAUDE.md` for the recording seams and the list
of reasons live numbers legitimately diverge from a backtest.

### `BinanceExecutor.py`

Low-level order client, off the asyncio loop:

- Submits orders to a `ThreadPoolExecutor` (GIL-free from the event loop) and returns a
  `concurrent.futures.Future[OrderResult]`.
- Retries with exponential backoff (`max_retries`, `base_retry_delay`), and returns
  `success=False` rather than raising once retries are exhausted.
- `execute_action(action)` maps `ActionType` onto exchange order types: `LIMIT` gains
  `timeInForce=GTC`, and a `STOP_MARKET` becomes `STOP_MARKET` or `TAKE_PROFIT_MARKET` depending on
  which side of `reference_price` its trigger sits. `Action.quantity` is a signed delta to apply to
  `action.symbol`'s current position — the same meaning the simulated executor gives it.

### `ReliableWebsocket.py`

Thin wrapper over python-binance's `ReconnectingWebsocket` that recovers from a dropped `recv()`
by closing and reconnecting the delegate, logging the failure and the recovery with a running
reconnect count.

## Dry run vs live

| | `dry_run=True` | `dry_run=False` |
|---|---|---|
| Executor | `SimulatedExecutor` (the backtest's) | `LiveExecutor` |
| Margin | synthetic `1e6` at startup, restored from the saved run on restart | hydrated from `futures_account()` |
| User-data socket | not opened | opened; `ACCOUNT_UPDATE` / `ORDER_TRADE_UPDATE` keep `Status` in sync |
| Orders | none sent; resting orders live in `Status.open_orders` and are matched against each candle | submitted through `BinanceExecutor`; the exchange owns the book |
| Position / avg price | updated through `Status.apply_fill` | updated from exchange fills |
| Recorded trades | local fill at `status.last_close[action.symbol]` | real exchange fill (`ap` / `z` / `rp` / `n`) |
| Forced liquidation | simulated when equity ≤ 0 | the exchange's |
| Recorded run id | `dry_<Streamer>_<SYM1-SYM2-...>_<INTERVAL>` | `live_<Streamer>_<SYM1-SYM2-...>_<INTERVAL>` |

## Usage

`core` is an installed package, so run from anywhere in the repo with `uv run` and import via the
full `core.*` path.

```python
import asyncio

from core.streamer.keltner_streamer import KeltnerStreamer
from core.trader.BinanceTrader import BinanceTrader

streamer = KeltnerStreamer(["ETHUSDT", "BTCUSDT"], window=20 * 60, m_entry=2.0, m_exit=0.0,
                           max_loss=0.08)

trader = BinanceTrader(
    api_key="...",
    api_secret="...",
    interval="1m",
    streamer=streamer,  # traded symbols come from streamer.symbols
    dry_run=True,     # no orders are sent
    testnet=False,
    record=True,      # writes asset/live/dry_KeltnerStreamer_BTCUSDT-ETHUSDT_1m.json
)


async def main():
    await trader.start()
    try:
        while trader.is_running:
            await asyncio.sleep(1)
    finally:
        await trader.stop()


asyncio.run(main())
```

To observe or alter what gets executed, wrap the executor rather than registering a callback —
`trader.executor` is the seam, and `Executor.submit` sees every action. `add_error_callback` is
still there for error notification.

`BinanceTrader` constructs its own `BinanceExecutor` internally, so you only need to instantiate
one directly if you want to place orders outside the trader's loop.

## Constructor parameters

**`BinanceTrader`** — `api_key`, `api_secret`, `interval`, `streamer` (traded symbols are taken
from `streamer.symbols` — there is no separate `symbol`/`symbols` argument),
`dry_run` (default `False`), `testnet` (default `True`), `fee_ratio` (default: the streamer's),
`record` (default `False`), `result_path` (`"asset/"`), `run_id` (default: derived and stable
across restarts), `run_metadata` (extra keys for the run JSON, e.g. `{"params": {...}}`),
`shard_flush_every` (`60`).

**`BinanceExecutor`** — `api_key`, `api_secret`, `testnet` (default `True`),
`max_workers` (`4`), `max_retries` (`2`), `base_retry_delay` (`0.1` s).

## Checks

```bash
uv run python core/live_check.py            # producer + executor + dry-run/backtest parity
uv run python core/live_check.py --offline  # skip the parts that need a candle cache
```

## Dependencies

`python-binance` (API client + websockets), plus stdlib `asyncio` and `concurrent.futures`.
