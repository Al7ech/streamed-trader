"""
BinanceTrader: Real-time WebSocket-based trading module for the StreamedTrader project.

This module connects to Binance WebSocket streams to receive real-time kline (candle) data
and integrates with the streamer system to get trading actions.
"""

import asyncio
import logging
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


DEFAULT_FEE_RATIO = 0.0004

#: 선물 정산(마진) 자산 후보. 심볼에서 접미사로 떼어내 잔고 항목을 찾는다.
#: 긴 것부터 검사해야 USDT/USDC 같은 4자리가 USD류 접두와 헷갈리지 않는다.
_MARGIN_ASSETS = ("FDUSD", "BUSD", "USDT", "USDC", "BNB", "BTC", "ETH")


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
                 testnet: bool = True,
                 fee_ratio: Optional[float] = None):
        """
        Initialize the BinanceTrader.

        Args:
            api_key: Binance API key
            api_secret: Binance API secret
            symbol: Trading symbol (e.g., 'ETHUSDT')
            interval: Kline interval (e.g., '1h', '1m', '5m')
            streamer: Streamer instance for trading decisions
            testnet: Whether to use testnet (default: True)
            fee_ratio: dry-run 회계에 적용할 수수료율. None이면 **스트리머의 값을 따라간다**
                (백테스터와 같은 규칙 — 스트리머는 자기 값으로 사이징하므로 어긋나면
                실효 레버리지가 의도와 달라진다). 라이브에서는 거래소 체결이 정답이라 미사용.
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol.upper()
        self.interval = interval
        self.streamer = streamer
        self.status = Status(margin=1e6 if dry_run else 0.0)
        self.dry_run = dry_run
        self.testnet = testnet
        streamer_fee = getattr(streamer, "fee_ratio", None)
        if fee_ratio is not None:
            self.fee_ratio = fee_ratio
        elif streamer_fee is not None:
            self.fee_ratio = streamer_fee
        else:
            self.fee_ratio = DEFAULT_FEE_RATIO

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

        # 리스너 태스크는 강한 참조를 들고 있어야 한다 — asyncio는 실행 중 태스크를 약한
        # 참조로만 잡아서, 놔두면 중간에 GC될 수 있다.
        self._tasks: set = set()

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

            # 1. Create socket manager.
            #    소켓을 여는 쪽(_connect_*_websocket)이 이걸 참조하므로 반드시 먼저 만든다.
            self.socket_manager = BinanceSocketManager(self.client)

            # 2. Load futures wallet status and apply to status object
            if not self.dry_run:
                await self._load_futures_wallet_status()

            # 3. Pre-feed indicators with historical candle data.
            #    소켓을 열기 **전에** 한다 — 이 작업은 수십 초가 걸릴 수 있는데, 그동안
            #    유저 데이터 스트림을 읽지 않으면 python-binance의 큐가 넘쳐 죽는다.
            await self._prefeed_indicators()

            # 4. 리스너 태스크 생성 전에 플래그를 세운다. 리스너 루프가 `while self.is_running`
            #    으로 시작하므로, 나중에 세우면 첫 await에서 태스크가 곧바로 빠져나간다.
            self.is_running = True

            # 5. Start WebSocket connections
            if not self.dry_run:
                await self._connect_user_websocket()
            await self._connect_kline_websocket()

            self.logger.info(f"BinanceTrader started for symbol: {self.symbol}, interval: {self.interval}")

        except Exception as e:
            # 삼키면 안 된다. 호출자(examples/trader.py)는 기동 성공/실패를 구분하지 못하고
            # `while trader.is_running` 없이 sleep 루프를 영원히 돈다.
            self.is_running = False
            self.logger.error(f"Failed to start BinanceTrader: {e}")
            raise

    def _spawn(self, coro) -> asyncio.Task:
        """리스너 태스크를 만들고 강한 참조를 유지한다 (GC 방지)."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def stop(self):
        """Stop the trader and close connections."""
        self.is_running = False
        self.should_reconnect = False

        # 자기 자신(리스너 태스크의 에러 경로)에서 불릴 수 있으므로 현재 태스크는 건너뛴다.
        current = asyncio.current_task()
        for task in list(self._tasks):
            if task is not current:
                task.cancel()

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

            # shutdown()의 기본값은 wait=True라 스레드가 끝날 때까지 이벤트 루프를 막는다.
            await asyncio.to_thread(self._executor.shutdown)
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
            self._spawn(self._listen_kline_websocket())

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
        if await self._handle_stream_error_frame(data, "kline"):
            return

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

        if self.dry_run and action.quantity != 0:
            # 라이브에서는 거래소 체결(ACCOUNT_UPDATE)이 status를 갱신하지만, dry-run에는
            # 체결이 없으므로 백테스터와 **같은** 회계를 직접 돌린다. 예전에는 자체 근사식이라
            # 부분 청산에서 avg_price를 현재가로 덮어써 미실현을 날렸고 margin은 아예 갱신하지
            # 않아서, dry-run 자본이 초기값에 영원히 고정됐다.
            wnl, fee = self.status.apply_fill(action.quantity, candle.close, self.fee_ratio)
            self.status.update_leverage()
            self.logger.info(f"[dry-run] filled qty={action.quantity} @ {candle.close} "
                             f"wnl={wnl:.4f} fee={fee:.4f} -> {self.status}")

        self.logger.debug(f"Processed completed")

    async def _connect_user_websocket(self):
        """Establish WebSocket connection to Binance futures user data stream."""
        try:
            # Create futures user data socket
            self.user_socket = ReliableWebsocket(self.socket_manager.futures_user_socket())

            # Start the socket. _conn은 connect() 안에서 만들어지므로 로그는 그 뒤에 찍는다
            # (예전엔 connect 전의 None._conn과 아직 만들어지지 않은 kline_socket을 읽었다).
            await self.user_socket.connect()

            # noinspection PyProtectedMember
            self.logger.info(f"connected to user stream: {self.user_socket._conn.uri} "
                             f"({self.user_socket.id()})")

            # Start listening for messages
            self._spawn(self._listen_user_websocket())

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
                    if await self._handle_stream_error_frame(data, "user"):
                        continue

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
        """Process ACCOUNT_UPDATE event and update status.

        ACCOUNT_UPDATE는 **변경된 항목만** 싣는다 (Binance 명세). 우리 자산/심볼이 없는
        이벤트(다른 심볼의 주문, 잔고만 움직인 이벤트)에서 status를 0으로 덮어쓰면 안 된다 —
        마진이 0이 되면 모든 스트리머의 사이징이 붕괴하고, 포지션이 0이 되면 다음 종가 캔들에
        flat으로 보여 재진입해 **실제 거래소 포지션이 2배**가 된다.
        """
        try:
            account_data = data.get('a') or {}
            margin_asset = self._get_margin_asset()

            # Update margin balance — 해당 자산 항목이 있을 때만
            for m in (account_data.get('B') or []):
                if m.get('a') == margin_asset:
                    self.status.margin = float(m.get("wb", 0.0))
                    break

            # exclude m=FUNDING_FEE
            if account_data.get("m", None) == "ORDER":
                # Process position updates — 해당 심볼 항목이 있을 때만
                for position in (account_data.get('P') or []):
                    if position.get('s', '') == self.symbol:
                        self.status.avg_price = float(position.get('ep', 0.0))  # entry price
                        self.status.position = float(position.get('pa', 0.0))  # position amount
                        self.status.unrealised_pnl = float(position.get('up', 0.0))  # unrealized
                        self.status.update_leverage()
                        break

            self.logger.info(f"Status updated from account: {self.status}")

        except Exception as e:
            self.logger.error(f"Error processing account update: {e}")
            raise

    async def _process_order_trade_update(self, data):
        """Log ORDER_TRADE_UPDATE fills.

        status 자체는 ACCOUNT_UPDATE가 거래소 값으로 갱신하므로 여기서는 건드리지 않는다
        (두 곳에서 쓰면 어느 쪽이 정답인지 모호해진다).
        """
        try:
            order_data = data.get('o') or {}
            if order_data.get('s', "") != self.symbol:
                return
            status = order_data.get('X', "")
            if status not in ("FILLED", "PARTIALLY_FILLED"):
                return

            avg_price = float(order_data.get('ap', 0.0))
            # z = 누적 체결 수량. 부호가 없으므로 side(S)로 방향을 복원한다.
            filled = float(order_data.get('z', 0.0))
            if order_data.get('S', "") == "SELL":
                filled = -filled
            self.logger.info(
                f"order {status.lower()}: [quantity={filled},avg_price={avg_price}]")
        except Exception as e:
            self.logger.error(f"Error processing order trade update: {e}")
            raise

    async def _handle_stream_error_frame(self, data: dict, stream: str) -> bool:
        """웹소켓 에러 프레임이면 에러 경로로 넘기고 True를 반환한다.

        python-binance는 끊김/재연결 실패를 예외가 아니라 ``{"e": "error", ...}`` **메시지**로
        큐에 올린다. 예전에는 kline 쪽이 ``data.get('k', {})`` → ``{}`` 로 조용히 return하고
        user 쪽은 이벤트 타입 비교에서 그냥 빠져나가, 이 프레임들이 전부 소리 없이 버려졌다.
        """
        if not isinstance(data, dict) or data.get('e') != 'error':
            return False
        message = data.get('m') or data.get('type') or str(data)
        await self._handle_error(RuntimeError(f"{stream} stream error: {message}"))
        return True

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

            # Extract relevant information.
            # walletBalance를 쓴다 — marginBalance는 walletBalance + unrealizedProfit 이고
            # status.total_margin()이 margin + unrealised_pnl 이라서, 아래에서 미실현을 따로
            # 넣는 순간 미실현이 두 번 세어진다. 스트림의 `wb`와도 이쪽이 같은 뜻이다.
            margin_balance = 0.0
            margin_asset = self._get_margin_asset()
            for asset in account_info['assets']:
                if asset["asset"] == margin_asset:
                    margin_balance = float(asset.get("walletBalance", 0.0))
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
                await asyncio.sleep(sleep_sec)

            # Get historical klines.
            # **인터벌 경계**로 내림한다. 예전엔 분 단위로만 잘라서, 1h 인터벌을 13:37에
            # 기동하면 start_time이 :37이 되고 Binance가 주는 정시 정렬 캔들과 어긋나
            # 아래 검증이 무조건 실패했다 — 사실상 1m 외에는 라이브 기동이 불가능했다.
            interval_delta = timedelta(minutes=interval_minutes)
            now = datetime.now(tz=timezone.utc)
            epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            end_time = epoch + (now - epoch) // interval_delta * interval_delta
            start_time = end_time - timedelta(minutes=total_minutes)

            self.logger.info(f"Fetching historical data from {start_time} to {end_time}")

            # Get historical klines from Binance.
            # 동기 HTTP + tqdm이라 스레드로 뺀다 (수십 초간 이벤트 루프를 막으면 안 된다).
            candle_fetcher = BinanceCandleFetcher()
            candles = await asyncio.to_thread(
                candle_fetcher.get_candles,
                symbol=self.symbol,
                interval=self.interval,
                start_date=start_time,
                end_date=end_time
            )

            # Assert
            actual_start = ms_timestamp_to_datetime(candles[0].start_time) if candles else None
            actual_end = ms_timestamp_to_datetime(candles[-1].end_time) if candles else None
            if max_window != len(candles) or actual_start != start_time or actual_end != end_time:
                raise ValueError(
                    f"historical kline assert error: "
                    f"count {len(candles)} != {max_window}, "
                    f"start {actual_start} != {start_time}, "
                    f"end {actual_end} != {end_time}")

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

            # concurrent.futures.Future라서 .result()는 **블로킹**이다 — 코루틴 안에서 부르면
            # 유저 데이터 스트림을 포함한 이벤트 루프 전체가 최대 10초 멈춘다. wrap_future로 감싼다.
            try:
                result = await asyncio.wait_for(asyncio.wrap_future(future), timeout=10)
            except Exception as e:
                self.logger.error(f"Error waiting for order result: {e}")
                await self._handle_error(e)
                return

            # 실행기는 재시도 소진 후 예외 대신 success=False를 **반환**한다. 여기서 확인하지
            # 않으면 영구 거부된 주문이 아무 흔적 없이 지나가고 전략이 거래소와 어긋난다.
            if result is None or not result.success:
                error = RuntimeError(
                    f"order failed: {getattr(result, 'error', 'no result')} (action={action})")
                self.logger.error(str(error))
                await self._handle_error(error)

    # TODO: 조금 더 구체적인 동작 필요
    async def _on_error(self, error: Exception):
        """Handle errors from trader."""
        self.logger.error(f"Trader error: {error}")

    # TODO: Symbol class로 분리
    def _get_margin_asset(self) -> str:
        """심볼의 정산(마진) 자산. 잔고 목록에서 우리 자산 항목을 찾는 데 쓴다.

        예전 구현은 ``symbol[3:]``이라 base가 3글자인 심볼에서만 우연히 맞았다.
        ``AVAXUSDT`` → ``"XUSDT"``, ``1000PEPEUSDT`` → ``"0PEPEUSDT"`` 가 되어 어떤 잔고
        항목에도 매치되지 않았고, 그 결과 ``status.margin``이 ``0.0``에 고정됐다.
        """
        for asset in _MARGIN_ASSETS:
            if self.symbol.endswith(asset) and len(self.symbol) > len(asset):
                return asset
        self.logger.warning(
            f"{self.symbol}의 정산 자산을 알 수 없다 — USDT로 가정한다. "
            f"알려진 자산: {_MARGIN_ASSETS}")
        return "USDT"
