"""
BinanceTrader: Real-time WebSocket-based trading module for the StreamedTrader project.

This module connects to Binance WebSocket streams to receive real-time kline (candle) data
and integrates with the streamer system to get trading actions.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, Dict, Any, Coroutine, List

from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException, BinanceWebsocketClosed

from core.backtest.status import Status
from core.binance_candle_fetcher.fetcher import BinanceCandleFetcher
from core.streamer.action import Action
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.trader.BinanceExecutor import BinanceExecutor
from core.trader.ReliableWebsocket import ReliableWebsocket
from core.utils import generate_dict_string, ms_timestamp_to_datetime, interval_to_minutes


class BinanceTrader:
    """
    Real-time trader that connects to Binance WebSocket streams for live trading.
    
    This class handles:
    - WebSocket connection to Binance kline streams
    - Real-time candle data processing
    - Integration with streamer for trading decisions
    - Automatic reconnection on connection failures
    - Async/await pattern for non-blocking operations
    """

    def __init__(self,
                 api_key: str,
                 api_secret: str,
                 symbol: str,
                 interval: str,
                 streamer: BaseStreamer,
                 dry_run: bool = False,
                 testnet: bool = True):
        """
        Initialize the BinanceTrader.
        
        Args:
            api_key: Binance API key
            api_secret: Binance API secret
            symbol: Trading symbol (e.g., 'ETHUSDT')
            interval: Kline interval (e.g., '1h', '1m', '5m')
            streamer: Streamer instance for trading decisions
            testnet: Whether to use testnet (default: True)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol.upper()
        self.interval = interval
        self.streamer = streamer
        self.status = Status(margin=1e6 if dry_run else 0.0)
        self.dry_run = dry_run
        self.testnet = testnet

        # WebSocket and client instances
        self.client: Optional[AsyncClient] = None
        self.socket_manager: Optional[BinanceSocketManager] = None
        self.kline_socket: Optional[ReliableWebsocket] = None
        self.user_socket: Optional[ReliableWebsocket] = None
        self.max_retries = 5

        # Executor
        self._executor = BinanceExecutor(
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet,
            max_workers=2
        )

        # Control flags
        self.is_running = False
        self.should_reconnect = True

        # Logging
        self.logger = logging.getLogger(__name__)

        # Callbacks
        self.on_action_callbacks: List[Callable[[Action], Coroutine[None, None, None]]] = [self._on_action]
        self.on_error_callbacks: List[Callable[[Exception], Coroutine[None, None, None]]] = [self._on_error]

    async def start(self):
        """Start the trader and establish WebSocket connection."""
        if self.is_running:
            self.logger.warning("Trader is already running")
            return

        try:
            # Initialize Binance Futures client
            self.client = await AsyncClient.create(
                api_key=self.api_key,
                api_secret=self.api_secret,
                testnet=self.testnet
            )

            if not self.dry_run:
                # 1. Load futures wallet status and apply to status object
                await self._load_futures_wallet_status()

                # 2. Load futures wallet user data stream
                await self._connect_user_websocket()

            # 3. Pre-feed indicators with historical candle data
            await self._prefeed_indicators()

            # 4. Create socket manager
            self.socket_manager = BinanceSocketManager(self.client)

            # 5. Start WebSocket connections
            await self._connect_kline_websocket()

            self.logger.info(f"BinanceTrader started for symbol: {self.symbol}, interval: {self.interval}")
            self.is_running = True

        except Exception as e:
            self.logger.error(f"Failed to start BinanceTrader: {e}")

    async def stop(self):
        """Stop the trader and close connections."""
        self.is_running = False
        self.should_reconnect = False

        try:
            if self.kline_socket:
                await self.kline_socket.close()
                self.kline_socket = None

            if self.user_socket:
                await self.user_socket.close()
                self.user_socket = None

            if self.client:
                await self.client.close_connection()
                self.client = None

            self._executor.shutdown()
            self.logger.info("BinanceTrader stopped")

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")

    async def _connect_kline_websocket(self):
        """Establish WebSocket connection to Binance kline stream."""
        try:
            # Create futures kline socket
            self.kline_socket = ReliableWebsocket(self.socket_manager.kline_futures_socket(
                symbol=self.symbol,
                interval=self.interval
            ))
            # noinspection PyProtectedMember
            self.logger.info(f"connecting to kline: {self.kline_socket._url}{self.kline_socket._path} ({self.kline_socket.id()})")

            # Start the socket
            await self.kline_socket.connect()

            # Start listening for messages
            asyncio.create_task(self._listen_kline_websocket())

        except Exception as e:
            self.logger.error(f"Failed to connect kline WebSocket: {e}")
            raise

    async def _listen_kline_websocket(self):
        """Listen for WebSocket messages and process kline data."""
        while self.is_running:
            try:
                data = await self.kline_socket.recv()
                if not self.is_running:
                    break

                try:
                    await self._process_kline_message(data)
                except Exception as e:
                    self.logger.error(f"Error processing kline message: {e}")
                    await self._handle_error(e)

            # Something is very wrong at this point. Stop trader
            except Exception as e:
                if not self.is_running:
                    break
                self.logger.fatal(f"Stopping trader. Error receiving kline message: {e}")
                await self.stop()
                break

    async def _process_kline_message(self, data: dict):
        """Process incoming kline message and trigger streamer update."""
        kline_data = data.get('k', {})

        # Check if kline is closed (completed candle)
        if not kline_data.get('x', False):  # x = is_closed
            return

        # Extract kline data
        open_price = float(kline_data['o'])
        high_price = float(kline_data['h'])
        low_price = float(kline_data['l'])
        close_price = float(kline_data['c'])
        volume = float(kline_data['v'])
        start_time = int(kline_data['t'])
        end_time = int(kline_data['T'])

        # Create Candle object
        candle = Candle(
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            start_time=start_time,
            end_time=end_time
        )

        self.logger.debug(f"candle closed: {candle}")

        # Update status with current price
        self.status.update_unrealised_pnl(close_price)

        self.logger.debug(f"status: {self.status}")

        # Indicators that opt into updates_before_decide ingest this candle first, so the
        # decision sees them including it. Same split the backtester applies.
        for indicator_name, indicator in self.streamer.indicators.items():
            if indicator.updates_before_decide:
                indicator.update(candle, self.status)

        # Get action from streamer
        action = self.streamer.update_candle(candle, self.status)

        self.logger.debug(f"streamer action: {action}")

        # The rest are updated after the decision (the default).
        # Indicators receive the same pre-trade status decide_action saw (action not yet executed).
        for indicator_name, indicator in self.streamer.indicators.items():
            if not indicator.updates_before_decide:
                indicator.update(candle, self.status)

        self.logger.debug(f"successfully updated indicators: {generate_dict_string(self.streamer.indicators)}")

        # Execute action if callback is set
        if self.on_action_callbacks and action.quantity != 0:
            await asyncio.gather(*[c(action) for c in self.on_action_callbacks])

        # TODO: 여기서 하는건 이상함
        if self.dry_run:
            # margin은 바꾸지 않음. TODO: Backtester이용해서 live update
            self.status.position += action.quantity
            self.status.avg_price = candle.close if self.status.position != 0 else 0.0
            self.status.update_leverage()

        self.logger.debug(f"Processed completed")

    async def _connect_user_websocket(self):
        """Establish WebSocket connection to Binance futures user data stream."""
        try:
            # Create futures user data socket
            self.user_socket = ReliableWebsocket(self.socket_manager.futures_user_socket())

            # noinspection PyProtectedMember
            self.logger.info(f"connecting to user stream: {self.user_socket._conn.uri} ({self.kline_socket.id()})")

            # Start the socket
            await self.user_socket.connect()

            # Start listening for messages
            asyncio.create_task(self._listen_user_websocket())

        except Exception as e:
            self.logger.error(f"Failed to connect user WebSocket: {e}")
            raise

    async def _listen_user_websocket(self):
        """Listen for WebSocket messages and process user data."""
        while self.is_running:
            try:
                data = await self.user_socket.recv()
                if not self.is_running:
                    break

                self.logger.debug(f"user data update: {data}")
                try:
                    event_type = data.get('e')

                    if event_type == 'ACCOUNT_UPDATE':
                        await self._process_account_update(data)
                    elif event_type == 'ORDER_TRADE_UPDATE':
                        await self._process_order_trade_update(data)

                except Exception as e:
                    self.logger.error(f"Error processing user message: {e}")
                    await self._handle_error(e)

            # Something is very wrong at this point. Stop trader
            except Exception as e:
                if not self.is_running:
                    break
                self.logger.fatal(f"Stopping trader. Error receiving user message: {e}")
                await self.stop()
                break

    async def _process_account_update(self, data: dict):
        """Process ACCOUNT_UPDATE event and update status."""
        try:
            account_data = data.get('a', {})

            # Update margin balance
            margin_balances = account_data.get('B')
            margin_balance = 0.0
            for m in margin_balances:
                if m['a'] == self._get_base_asset():
                    margin_balance = float(m.get("wb", 0.0))
                    break
            self.status.margin = margin_balance

            # exclude m=FUNDING_FEE
            if account_data.get("m", None) == "ORDER":
                # Process position updates
                positions = account_data.get('P', [])
                avg_price = 0.0
                position_size = 0.0
                unrealised_pnl = 0.0
                for position in positions:
                    symbol = position.get('s', '')
                    if symbol == self.symbol:
                        # Update position information
                        avg_price = float(position.get('ep', 0.0))  # entry price
                        position_size = float(position.get('pa', 0.0))  # position amount
                        unrealised_pnl = float(position.get('up', 0.0))  # unrealized profit
                        break

                self.status.avg_price = avg_price
                self.status.position = position_size
                self.status.unrealised_pnl = unrealised_pnl
                self.status.update_leverage()

            self.logger.info(f"Status updated from account: {self.status}")

        except Exception as e:
            self.logger.error(f"Error processing account update: {e}")
            raise

    async def _process_order_trade_update(self, data):
        """Process ORDER_TRADE_UPDATE event and update status."""
        try:
            order_data = data.get('o')
            if order_data.get('s', "") != self.symbol:
                return
            if order_data.get('X', "") != "FILLED":
                return

            avg_price = float(order_data.get('ap', 0.0))
            quantity = float(order_data.get('z', 0.0))
            self.logger.info(f"order filled: [quantity={quantity},avg_price={avg_price}]")
        except Exception as e:
            self.logger.error(f"Error processing order trade update: {e}")
            raise

    async def _handle_error(self, error: Exception):
        """Handle errors and notify callback if set."""
        self.logger.error(f"BinanceTrader error: {error}")

        if self.on_error_callbacks:
            try:
                await asyncio.gather(*[c(error) for c in self.on_error_callbacks])
            except Exception as e:
                self.logger.error(f"Error in error callback: {e}")

    def add_action_callback(self, callback: Callable[[Action], Coroutine[None, None, None]]):
        """Add callback function for trading actions."""
        self.on_action_callbacks.append(callback)

    def add_error_callback(self, callback: Callable[[Exception], Coroutine[None, None, None]]):
        """Add callback function for error handling."""
        self.on_error_callbacks.append(callback)

    async def get_account_info(self) -> Dict[str, Any]:
        """Get futures account information from Binance."""
        if not self.client:
            raise RuntimeError("Client not initialized")

        try:
            account_info = await self.client.futures_account()
            return account_info
        except BinanceAPIException as e:
            self.logger.error(f"Failed to get account info: {e}")
            raise

    async def _load_futures_wallet_status(self):
        """Load futures wallet status and apply to status object."""
        try:
            self.logger.info("Loading futures wallet status...")

            # Get futures account info
            account_info = await self.get_account_info()

            if 'error' in account_info:
                raise Exception(f"Failed to get account info: {account_info['error']}")

            # Extract relevant information
            margin_balance = 0.0
            for asset in account_info['assets']:
                if asset["asset"] == self._get_base_asset():
                    margin_balance = float(asset.get("marginBalance", 0.0))
                    break

            # Get position info for the current symbol
            positions = account_info.get('positions', [])
            position_info = None

            for position in positions:
                if position['symbol'] == self.symbol:
                    position_info = position
                    break

            if position_info:
                avg_price = float(position_info.get('entryPrice', 0.0))
                position_size = float(position_info.get('positionAmt', 0.0))
                unrealised_pnl = float(position_info.get('unrealizedProfit', 0.0))

                # Update status with current position
                self.status.avg_price = avg_price
                self.status.unrealised_pnl = unrealised_pnl
                self.status.margin = margin_balance
                self.status.position = position_size
                self.status.update_leverage()

            else:
                # No position for this symbol
                self.status.avg_price = 0.0
                self.status.unrealised_pnl = 0.0
                self.status.margin = margin_balance
                self.status.position = 0.0
                self.status.leverage = 0.0

            self.logger.info(f"successfully loaded status: {self.status}")

        except Exception as e:
            self.logger.error(f"Failed to load futures wallet status: {e}")
            raise

    async def _prefeed_indicators(self):
        """Pre-feed indicators with historical candle data."""
        try:
            self.logger.info("Pre-feeding indicators with historical data...")

            # Find maximum window size among all indicators
            max_window = 0
            for indicator_name, indicator in self.streamer.indicators.items():
                max_window = max(max_window, indicator.window)

            if max_window == 0:
                self.logger.info("No indicators found, skipping pre-feeding")
                return

            self.logger.info(f"Maximum indicator window size: {max_window}")

            # Calculate time range (max_window candles before current time)
            # Convert interval to minutes for calculation
            interval_minutes = interval_to_minutes(self.interval)
            total_minutes = max_window * interval_minutes

            current_sec = datetime.now().second
            if 55 <= current_sec:
                sleep_sec = 61 - current_sec
                self.logger.info(f"skipping to next minute ({sleep_sec} seconds)")
                time.sleep(sleep_sec)

            # Get historical klines
            end_time = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
            start_time = end_time - timedelta(minutes=total_minutes)

            self.logger.info(f"Fetching historical data from {start_time} to {end_time}")

            # Get historical klines from Binance
            candle_fetcher = BinanceCandleFetcher()
            candles = candle_fetcher.get_candles(
                symbol=self.symbol,
                interval=self.interval,
                start_date=start_time,
                end_date=end_time
            )

            # Assert
            if max_window != len(candles) \
                    or ms_timestamp_to_datetime(candles[0].start_time) != start_time \
                    or ms_timestamp_to_datetime(candles[-1].end_time) != end_time:
                raise ValueError("historical kline assert error")

            start_datetime = candles[0].start_time
            end_datetime = candles[-1].end_time
            candle_count = len(candles)

            # Convert to Candle objects and update indicators
            for candle in candles:
                # Update all indicators
                for indicator_name, indicator in self.streamer.indicators.items():
                    indicator.update(candle)

            s = ms_timestamp_to_datetime(start_datetime)
            e = ms_timestamp_to_datetime(end_datetime)
            self.logger.info(
                f"Pre-fed indicators with {candle_count} historical candles: {s} ~ {e}")

            self.logger.info(f"Pre-fed indicators: {generate_dict_string(self.streamer.indicators)}")

        except Exception as e:
            self.logger.error(f"Failed to pre-feed indicators: {e}")
            raise

    async def _on_action(self, action: Action):
        """Handle trading actions from streamer."""
        if self.dry_run:
            self.logger.info(f"dry run: {action}")

        else:
            # Execute order asynchronously
            future = self._executor.execute_action(action, self.symbol)

            # You can wait for result or handle it asynchronously
            try:
                future.result(timeout=10)  # Wait up to 10 seconds
            except Exception as e:
                print(f"Error waiting for order result: {e}")

    # TODO: 조금 더 구체적인 동작 필요
    async def _on_error(self, error: Exception):
        """Handle errors from trader."""
        print(f"Trader error: {error}")

    # TODO: Symbol class로 분리
    def _get_base_asset(self) -> str:
        return self.symbol[3:]
