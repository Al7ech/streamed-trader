# Trader package

Live Binance USD-M Futures trading: it feeds closed candles from a websocket into the same
`BaseStreamer` you backtested, and executes the resulting `Action`s.

The entry point is `core/examples/trader.py` — this package is the machinery behind it. See the repository
`README.md` for the end-to-end setup and `CLAUDE.md` for the architecture.

## Components

### `BinanceTrader.py`

Asyncio trader that:

- Opens a futures kline websocket, plus a futures user-data websocket when not in dry run, both
  wrapped in `ReliableWebsocket` (reconnects when `recv()` drops).
- Backfills every indicator's rolling window with historical candles (`_prefeed_indicators`, via
  `BinanceCandleFetcher`) before going live, asserting the fetched range is exactly what was
  expected.
- On each **closed** kline: builds a `Candle`, calls `streamer.update_candle(candle, status)`,
  then updates the indicators with that same pre-trade `Status`, then executes the action — the
  same ordering the backtester uses, so live and backtest see identical indicator state.
- Fires registered callbacks (`add_action_callback` / `add_error_callback`).

### `BinanceExecutor.py`

Order execution off the asyncio loop:

- Submits orders to a `ThreadPoolExecutor` (GIL-free from the event loop) and returns a
  `concurrent.futures.Future[OrderResult]`.
- Retries with exponential backoff (`max_retries`, `base_retry_delay`).
- `execute_action(action, symbol)` interprets `Action.quantity` exactly like the backtester's
  `_trade()`: a signed delta to apply to the current position.

### `ReliableWebsocket.py`

Thin wrapper over python-binance's `ReconnectingWebsocket` that recovers from a dropped `recv()`
by closing and reconnecting the delegate.

## Dry run vs live

| | `dry_run=True` | `dry_run=False` |
|---|---|---|
| Margin | synthetic `1e6` at startup | hydrated from `futures_account()` |
| User-data socket | not opened | opened; `ACCOUNT_UPDATE` / `ORDER_TRADE_UPDATE` keep `Status` in sync |
| Orders | none sent | submitted through `BinanceExecutor` |
| Position / avg price | updated locally from the `Action` | updated from exchange fills |

## Usage

Run from `core/` as the working directory — modules import their siblings as top-level packages
(`from trader.BinanceTrader import BinanceTrader`), which only resolves when `core/` is on
`sys.path`.

```python
import asyncio

from streamer.keltner_streamer import KeltnerStreamer
from trader.BinanceTrader import BinanceTrader

streamer = KeltnerStreamer("ETHUSDT", window=20 * 60, m_entry=2.0, m_exit=0.0, max_loss=0.08)

trader = BinanceTrader(
    api_key="...",
    api_secret="...",
    symbol="ETHUSDT",
    interval="1m",
    streamer=streamer,
    dry_run=True,     # no orders are sent
    testnet=False,
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

**`BinanceTrader`** — `api_key`, `api_secret`, `symbol`, `interval`, `streamer`,
`dry_run` (default `False`), `testnet` (default `True`).

**`BinanceExecutor`** — `api_key`, `api_secret`, `testnet` (default `True`),
`max_workers` (`4`), `max_retries` (`2`), `base_retry_delay` (`0.1` s).

## Order types

`OrderType` covers `MARKET`, `LIMIT`, `STOP_LOSS`, `STOP_LOSS_LIMIT`, `TAKE_PROFIT`,
`TAKE_PROFIT_LIMIT`. `execute_action` uses market orders.

## Dependencies

`python-binance` (API client + websockets), plus stdlib `asyncio` and `concurrent.futures`.
