# Trader package

Live Binance USD-M Futures trading: it feeds closed candles from a websocket into the same
`BaseStreamer` you backtested, and executes the resulting `Action`s.

The entry point is `core/examples/trader.py` — this package is the machinery behind it. See the repository
`README.md` for the end-to-end setup and `CLAUDE.md` for the architecture.

## Components

### `BinanceTrader.py`

Asyncio trader that trades every symbol in `streamer.symbols` concurrently:

- Opens **one** futures kline websocket — a `futures_multiplex_socket` carrying every traded
  symbol's continuous-kline stream — plus a futures user-data websocket when not in dry run, both
  wrapped in `ReliableWebsocket` (reconnects when `recv()` drops). Combined-stream messages arrive
  wrapped as `{"stream": ..., "data": <rawPayload>}`; since the underlying continuousKline payload
  carries no top-level `s`/`k.s`, the symbol is read from `data["ps"]` instead. Because there is a
  single listener task pulling from this one socket and it fully awaits each candle's processing
  (including order dispatch) before receiving the next message, candle processing across symbols
  is naturally serialized — no extra locking is needed.
- Backfills every symbol's indicators with historical candles (`_prefeed_indicators`, via
  `BinanceCandleFetcher`, one symbol at a time) before going live, asserting each fetched range is
  exactly what was expected. All symbols share the same interval-boundary `end_time` so their
  windows stay aligned with each other even though the fetches themselves are sequential.
- On each **closed** kline: builds a `Candle`, calls `streamer.decide_action(symbol, candle,
  status)` (returns a list of `Action`s — which may target symbols other than the one that
  triggered the call, for cross-symbol strategies), then updates that symbol's indicators with the
  same pre-trade `Status`, then executes each action — the same ordering the backtester uses, so
  live and backtest see identical indicator state. Every traded symbol must settle in the same
  margin asset, since `Status.margin` is one pool shared across all of them; the trader asserts
  this at startup.
- Fires registered callbacks (`add_action_callback` / `add_error_callback`).
- With `record=True`, hands every closed candle and every fill to a `LiveRecorder`.

### `live_recorder.py`

Persists the session to `<result_path>/live/` in **exactly** the format the backtester writes to
`asset/backtest/`, so a live run and a backtest of the same strategy can be loaded side by side in
`visualise/`. Reuses `ShardWriter` / `write_run_json` / `build_summary` unchanged.

- `run_id` is fixed (`<live|dry>_<Streamer>_<SYM1-SYM2-...>_<INTERVAL>`, symbols sorted and
  hyphen-joined), so a restart resumes the same run rather than starting a new file. State is
  replayed from the shards on startup.
- Changing strategy params, the traded symbol set, or indicator columns forks a new run instead of
  appending mismatched data to the old shards.
- Writes are atomic (`boltons.fileutils.atomic_save`); the run JSON is rewritten every candle and
  the month shard every `shard_flush_every` candles, on every trade, and on `stop()`.

See the "Live run output" section of the repo-root `CLAUDE.md` for the recording seams and the list
of reasons live numbers legitimately diverge from a backtest.

### `BinanceExecutor.py`

Order execution off the asyncio loop:

- Submits orders to a `ThreadPoolExecutor` (GIL-free from the event loop) and returns a
  `concurrent.futures.Future[OrderResult]`.
- Retries with exponential backoff (`max_retries`, `base_retry_delay`).
- `execute_action(action)` interprets `Action.quantity` exactly like the backtester's `_trade()`:
  a signed delta to apply to `action.symbol`'s current position.

### `ReliableWebsocket.py`

Thin wrapper over python-binance's `ReconnectingWebsocket` that recovers from a dropped `recv()`
by closing and reconnecting the delegate.

## Dry run vs live

| | `dry_run=True` | `dry_run=False` |
|---|---|---|
| Margin | synthetic `1e6` at startup | hydrated from `futures_account()` |
| User-data socket | not opened | opened; `ACCOUNT_UPDATE` / `ORDER_TRADE_UPDATE` keep `Status` in sync |
| Orders | none sent | submitted through `BinanceExecutor` |
| Position / avg price | updated through `Status.apply_fill` (the backtester's accounting) | updated from exchange fills |
| Recorded trades | local fill at `status.last_close[action.symbol]` | real exchange fill (`ap` / `z` / `rp` / `n`) |
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


async def on_action(action):
    print("action:", action)


async def main():
    trader.add_action_callback(on_action)
    await trader.start()
    try:
        while True:
            await asyncio.sleep(1)
    finally:
        await trader.stop()


asyncio.run(main())
```

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

## Order types

`OrderType` covers `MARKET`, `LIMIT`, `STOP_LOSS`, `STOP_LOSS_LIMIT`, `TAKE_PROFIT`,
`TAKE_PROFIT_LIMIT`. `execute_action` uses market orders.

## Dependencies

`python-binance` (API client + websockets), plus stdlib `asyncio` and `concurrent.futures`.
