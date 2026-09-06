"""BinanceTrader: 라이브/드라이런 실행의 조립과 수명주기.

이 클래스가 하는 일은 네 부품을 엮고 켜고 끄는 것뿐이다:

===========  =========================  ==================  ================
             CandleProducer             Executor            Recorder
===========  =========================  ==================  ================
드라이런     LiveCandleProducer         SimulatedExecutor   LiveRecorder
라이브       LiveCandleProducer         LiveExecutor        LiveRecorder
===========  =========================  ==================  ================

매매 순서 규약은 :class:`~core.engine.engine.TradingEngine`에, 캔들 소스는
:class:`~core.trader.live_candle_producer.LiveCandleProducer`에, 주문 실행과 계좌 상태는
실행기에 있다. **드라이런이 백테스트와 문자 그대로 같은 실행기 클래스를 쓴다** —
드라이런은 백테스트와 대조하기 위해 존재하므로, 체결 규칙이 갈라지면 기능 자체가 무의미해진다.
"""

import asyncio
import copy
import logging
from typing import Any, Callable, Coroutine, Dict, List, Optional

from binance import AsyncClient, BinanceSocketManager

from core.engine.engine import TradingEngine
from core.engine.executor import (
    Executor, SimulatedExecutor, resolve_fee_ratio, resolve_slippage_ratio)
from core.engine.recorder import NullRecorder
from core.engine.status import Status
from core.streamer.base_streamer import BaseStreamer
from core.trader.BinanceExecutor import BinanceExecutor
from core.trader.ReliableWebsocket import ReliableWebsocket
from core.trader.live_candle_producer import LiveCandleProducer
from core.trader.live_executor import LiveExecutor, resolve_margin_asset
from core.trader.live_recorder import DEFAULT_SHARD_FLUSH_EVERY, LiveRecorder, default_run_id
from core.utils import interval_to_minutes

#: 드라이런의 합성 초기 증거금. 실제 지갑이 없으므로 고정값에서 시작한다.
DRY_RUN_MARGIN = 1e6


class BinanceTrader:
    """Real-time trader that connects to Binance WebSocket streams for live trading."""

    def __init__(self,
                 api_key: str,
                 api_secret: str,
                 interval: str,
                 streamer: BaseStreamer,
                 dry_run: bool = False,
                 testnet: bool = True,
                 fee_ratio: Optional[float] = None,
                 record: bool = False,
                 result_path: str = "asset/",
                 run_id: Optional[str] = None,
                 run_metadata: Optional[Dict] = None,
                 shard_flush_every: int = DEFAULT_SHARD_FLUSH_EVERY):
        """
        :param streamer: 매매 결정을 내리는 스트리머. 다루는 심볼은 ``streamer.symbols``에서
            가져오므로 별도 인자가 없다 — 트레이더가 스트리머와 무엇을 거래하는지에 대해
            어긋날 수 없다.
        :param fee_ratio: 드라이런 회계에 적용할 수수료율. None이면 **스트리머의 값을
            따라간다**. 라이브에서는 거래소 체결이 정답이라 쓰이지 않는다.
        :param record: True면 실행 결과를 ``<result_path>/live/`` 에 **백테스트와 같은
            포맷**으로 기록한다.
        :param run_id: 기록에 쓸 런 식별자. None이면 전략/심볼/인터벌에서 고정 id를 만들어
            재기동해도 같은 런에 이어쓴다.
        :param run_metadata: 런 JSON metadata에 실을 추가 정보 (예: ``{"params": {...}}``).
        :param shard_flush_every: 월별 시계열 샤드를 다시 쓰는 주기(캔들 수).
        """
        self.logger = logging.getLogger(__name__)

        self.api_key = api_key
        self.api_secret = api_secret
        self.streamer = streamer
        self.symbols: List[str] = list(streamer.symbols)
        self.interval = interval
        #: 인터벌 길이(ms). 레코더 샤드 메타가 쓴다.
        self._interval_ms = interval_to_minutes(interval) * 60_000
        self.dry_run = dry_run
        self.testnet = testnet

        # 정산 자산은 **두 모드 모두** 검증한다 — Status.margin이 전 심볼 공유 풀이라는 전제가
        # 드라이런에서도 똑같이 성립해야 하고, 설정 오류는 일찍 잡을수록 좋다.
        self._margin_asset = resolve_margin_asset(self.symbols)
        self.status = Status(margin=DRY_RUN_MARGIN if dry_run else 0.0)
        self.fee_ratio = resolve_fee_ratio(streamer, fee_ratio)
        self.slippage_ratio = resolve_slippage_ratio(streamer)

        self._order_client = BinanceExecutor(
            api_key=api_key, api_secret=api_secret, testnet=testnet, max_workers=2)

        #: 액션을 체결로 바꾸는 실행기. 드라이런과 라이브의 차이는 **거의 전부** 여기 있다.
        self.executor: Executor
        if dry_run:
            # 백테스트와 같은 클래스다. 체결 규칙·회계·미체결 장부가 한 벌이라 드라이런 결과를
            # 같은 구간의 백테스트와 그대로 대조할 수 있다.
            self.executor = SimulatedExecutor(self.status, self.fee_ratio, self.slippage_ratio,
                                              log_label="dry-run")
        else:
            self.executor = LiveExecutor(self._order_client, self.status, self.symbols,
                                         self._margin_asset, on_error=self._handle_error)

        self.client: Optional[AsyncClient] = None
        self.socket_manager: Optional[BinanceSocketManager] = None
        #: 마감 캔들 공급자. 소켓 연결/연속성/백필/워밍업 페치를 전부 소유한다.
        self.producer: Optional[LiveCandleProducer] = None
        self.user_socket: Optional[ReliableWebsocket] = None
        self.engine: Optional[TradingEngine] = None

        self.is_running = False
        # 리스너 태스크는 강한 참조를 들고 있어야 한다 — asyncio는 실행 중 태스크를 약한
        # 참조로만 잡아서, 놔두면 중간에 GC될 수 있다.
        self._tasks: set = set()

        # 결과 기록. recorder는 start()에서 만들어진다 — 라이브 init_margin이 지갑 조회 뒤에
        # 정해지기 때문이다.
        self.record = record
        self.result_path = result_path
        self.run_id = run_id
        self.run_metadata = run_metadata or {}
        self.shard_flush_every = shard_flush_every
        self.recorder: Optional[LiveRecorder] = None

        self.on_error_callbacks: List[Callable[[Exception], Coroutine[None, None, None]]] = [
            self._on_error]

    # ----------------------------------------------------------------- 수명주기

    async def start(self):
        """Start the trader and establish WebSocket connections."""
        if self.is_running:
            self.logger.warning("Trader is already running")
            return

        try:
            self.client = await AsyncClient.create(
                api_key=self.api_key, api_secret=self.api_secret, testnet=self.testnet)

            # 1. 소켓 매니저. 캔들 공급자와 유저 데이터 소켓이 이걸 참조하므로 먼저 만든다.
            self.socket_manager = BinanceSocketManager(self.client)
            self.producer = LiveCandleProducer(self.socket_manager, self.symbols,
                                               self.interval, self._handle_error)

            # 2. 라이브는 거래소가 계좌의 정답이다. 걸어둔 미체결 주문까지 끌어온다 — 프로세스가
            #    죽어 있어도 손절은 거래소에서 계속 살아 있으므로, 이걸 안 하면 전략이
            #    "손절이 없다"고 보고 하나 더 걸어 이중으로 청산된다.
            if not self.dry_run:
                self.executor.attach_client(self.client)
                await self.executor.load_account()
                await self.executor.load_open_orders()

            # 3. 결과 레코더. 지갑 조회(2) 뒤여야 라이브 init_margin이 실제 잔고다.
            self._setup_recorder()
            recorder = self.recorder or NullRecorder()
            self.executor.on_trade = recorder.record_trade
            if not self.dry_run:
                self.executor.on_metadata = (
                    self.recorder.set_metadata if self.recorder else (lambda k, v: None))
                self.executor.reconcile_resumed(
                    self.recorder.resumed_status if self.recorder else None)

            self.engine = TradingEngine(self.streamer, self.executor, recorder)

            # 4. 지표 워밍업. 소켓을 열기 **전에** 한다 — 수십 초가 걸릴 수 있는데 그동안
            #    유저 데이터 스트림을 읽지 않으면 python-binance의 큐가 넘쳐 죽는다.
            await self._prefeed_indicators()

            # 5. 리스너 태스크 생성 전에 플래그를 세운다. 리스너 루프가 `while self.is_running`
            #    으로 시작하므로, 나중에 세우면 첫 await에서 태스크가 곧바로 빠져나간다.
            self.is_running = True

            # 6. Start WebSocket connections
            if not self.dry_run:
                await self._connect_user_websocket()
            await self.producer.connect()
            self._spawn(self._run_engine())

            # 이 프로세스가 무엇인지 한 줄로 남긴다. 특히 **dry_run**은 여기 말고는 어디에도
            # 기록되지 않는다 — 체결이 나면 "[dry-run]" 접두사로 알 수 있지만, 전략이 며칠간
            # 매매를 안 하면 로그만 보고 실제 돈이 걸려 있는지 판단할 방법이 없었다.
            self.logger.info(
                "BinanceTrader started: mode=%s symbols=%s interval=%s testnet=%s "
                "streamer=%s fee_ratio=%s slippage_ratio=%s open_orders=%d record=%s run_id=%s",
                "DRY-RUN" if self.dry_run else "LIVE", self.symbols, self.interval,
                self.testnet, type(self.streamer).__name__, self.fee_ratio,
                self.slippage_ratio, self.status.total_open_orders(),
                bool(self.recorder), self.recorder.run_id if self.recorder else None)

        except Exception as e:
            # 삼키면 안 된다. 호출자(examples/trader.py)는 기동 성공/실패를 구분하지 못하고
            # `while trader.is_running` 없이 sleep 루프를 영원히 돈다.
            self.is_running = False
            self.logger.error(f"Failed to start BinanceTrader: {e}")
            raise

    async def stop(self):
        """Stop the trader and close connections."""
        self.is_running = False

        # 소켓/실행기 정리보다 **먼저** 기록을 마무리한다. 아래 try에서 예외가 나면 남은
        # 정리가 통째로 건너뛰어지므로, 마지막 flush가 거기 묻히면 안 된다.
        # close()는 멱등이고 예외를 밖으로 내지 않는다 — stop()은 main의 finally와 리스너의
        # 치명적 오류 경로에서 최대 두 번 불릴 수 있다.
        if self.recorder:
            self.recorder.close()

        if self.producer:
            self.producer.request_stop()

        # 자기 자신(리스너 태스크의 에러 경로)에서 불릴 수 있으므로 현재 태스크는 건너뛴다.
        current = asyncio.current_task()
        for task in list(self._tasks):
            if task is not current:
                task.cancel()

        try:
            if self.producer:
                await self.producer.close()
                self.producer = None

            if self.user_socket:
                await self.user_socket.close()
                self.user_socket = None

            if self.client:
                await self.client.close_connection()
                self.client = None

            # shutdown()의 기본값은 wait=True라 스레드가 끝날 때까지 이벤트 루프를 막는다.
            await asyncio.to_thread(self._order_client.shutdown)

            # 풀이 모든 주문 future를 resolve한 뒤라, 결과-대기 태스크들은 한 틱이면 끝난다.
            # 마지막 실패 로그를 확실히 흘리고 shutdown을 deterministic하게 만든다.
            if isinstance(self.executor, LiveExecutor):
                await self.executor.drain_pending_orders()

            self.logger.info("BinanceTrader stopped")

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")

    def _spawn(self, coro) -> asyncio.Task:
        """리스너 태스크를 만들고 강한 참조를 유지한다 (GC 방지)."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    # ----------------------------------------------------------------- 매매 루프

    async def _run_engine(self):
        """캔들 공급자를 엔진에 흘려보낸다. 스트림이 끝나면 트레이더를 멈춘다."""
        await self.engine.run_async(self.producer, on_error=self._handle_error)
        if self.is_running:
            await self.stop()

    async def _prefeed_indicators(self):
        """Pre-feed every symbol's indicators with historical candle data.

        과거 캔들을 어디서 어떻게 가져오는지는 :class:`LiveCandleProducer`가 알고, 그것을
        지표에 먹이는 것은 엔진이 안다. ``status``는 넘어가지 않는다: 이 구간에는 대응하는
        계좌 상태가 없으므로 status를 읽는 지표는 ``None``을 워밍업으로 다뤄야 한다.
        """
        try:
            self.logger.info("Pre-feeding indicators with historical data...")
            windows = self.engine.warmup_windows()
            self.engine.warmup(await self.producer.warmup_candles(windows))
        except Exception as e:
            self.logger.error(f"Failed to pre-feed indicators: {e}")
            raise

    # ----------------------------------------------------------------- 결과 기록

    def _setup_recorder(self):
        """결과 레코더를 만든다. 실패해도 트레이더는 계속 간다 — 매매가 본업이고 기록은 관측이다."""
        if not self.record:
            return
        try:
            self.recorder = LiveRecorder(
                result_path=self.result_path,
                run_id=self.run_id or default_run_id(self.streamer, self.symbols,
                                                     self.interval, self.dry_run),
                streamer=self.streamer,
                status=self.status,
                interval_ms=self._interval_ms,
                init_margin=self.status.total_margin(),
                metadata={
                    **self.run_metadata,
                    "symbols": self.symbols,
                    "interval": self.interval,
                    "mode": "dry" if self.dry_run else "live",
                    "streamer": type(self.streamer).__name__,
                },
                shard_flush_every=self.shard_flush_every,
            )
            self._restore_dry_run_status()
        except Exception as e:
            self.recorder = None
            self.logger.error(f"결과 기록을 비활성화한다 (레코더 생성 실패): {e}", exc_info=True)

    def _restore_dry_run_status(self):
        """재개한 런의 계좌 상태를 되살린다 — **드라이런 전용**.

        드라이런 자본은 합성값이라 프로세스가 죽으면 초기값으로 돌아간다. 자본 곡선은 이어붙는데
        상태만 초기화되면 재기동 지점에서 곡선이 초기값으로 튀어, 재개형 런이 만들어내는
        데이터가 통째로 못 쓰게 된다. 라이브는 거래소가 정답이므로 절대 여기서 덮지 않는다.

        실행기와 레코더가 ``self.status``를 참조로 들고 있으므로 **제자리에서** 갱신한다.
        """
        if not self.dry_run or not self.recorder or not self.recorder.resumed_status:
            return
        saved = self.recorder.resumed_status
        for symbol in self.symbols:
            saved_pos = saved.position_for(symbol)
            pos = self.status.position_for(symbol)
            pos.avg_price = saved_pos.avg_price
            pos.unrealised_pnl = saved_pos.unrealised_pnl
            pos.position = saved_pos.position
        self.status.margin = saved.margin
        self.status.leverage = saved.leverage
        # 미체결 주문도 되살린다. 드라이런의 장부는 이 프로세스 안에만 있어서, 복원하지
        # 않으면 재기동할 때마다 걸어둔 손절이 조용히 사라진다.
        self.status.open_orders = copy.deepcopy(saved.open_orders)
        self.logger.info(f"[dry-run] 이전 런의 계좌 상태를 복원했다: {self.status}")
        if self.status.total_open_orders():
            self.logger.info("[dry-run] 미체결 주문 %d건을 복원했다",
                             self.status.total_open_orders())

    # ----------------------------------------------------------- 유저 데이터 소켓

    async def _connect_user_websocket(self):
        """Establish WebSocket connection to Binance futures user data stream."""
        try:
            self.user_socket = ReliableWebsocket(self.socket_manager.futures_user_socket())

            # Start the socket. _conn은 connect() 안에서 만들어지므로 로그는 그 뒤에 찍는다.
            await self.user_socket.connect()

            # noinspection PyProtectedMember
            self.logger.info(f"connected to user stream: {self.user_socket._conn.uri} "
                             f"({self.user_socket.id()})")

            self._spawn(self._listen_user_websocket())

        except Exception as e:
            self.logger.error(f"Failed to connect user WebSocket: {e}")
            raise

    async def _listen_user_websocket(self):
        """유저 데이터 메시지를 받아 실행기로 넘긴다 — 계좌/체결의 진실은 실행기가 안다."""
        while self.is_running:
            try:
                data = await self.user_socket.recv()
                if not self.is_running:
                    break

                self.logger.debug(f"user data update: {data}")
                try:
                    if await self._handle_stream_error_frame(data, "user"):
                        continue
                    await self.executor.on_user_data(data)
                except Exception as e:
                    self.logger.exception("Error processing user message: %s", e)
                    await self._handle_error(e)

            # Something is very wrong at this point. Stop trader
            except Exception as e:
                if not self.is_running:
                    break
                self.logger.critical(f"Stopping trader. Error receiving user message: {e}")
                await self.stop()
                break

    # ----------------------------------------------------------------- 오류/조회

    async def _handle_stream_error_frame(self, data: dict, stream: str) -> bool:
        """웹소켓 에러 프레임이면 에러 경로로 넘기고 True를 반환한다.

        python-binance는 끊김/재연결 실패를 예외가 아니라 ``{"e": "error", ...}`` **메시지**로
        큐에 올린다. 이벤트 타입 비교에서 그냥 빠져나가면 이 프레임들이 소리 없이 버려진다.
        """
        if not isinstance(data, dict) or data.get("e") != "error":
            return False
        message = data.get("m") or data.get("type") or str(data)
        await self._handle_error(RuntimeError(f"{stream} stream error: {message}"))
        return True

    async def _handle_error(self, error: Exception):
        """Handle errors and notify callbacks."""
        self.logger.error(f"BinanceTrader error: {error}")

        if self.on_error_callbacks:
            try:
                await asyncio.gather(*[c(error) for c in self.on_error_callbacks])
            except Exception as e:
                self.logger.error(f"Error in error callback: {e}")

    # TODO: 조금 더 구체적인 동작 필요
    async def _on_error(self, error: Exception):
        """Handle errors from trader."""
        self.logger.error(f"Trader error: {error}")

    def add_error_callback(self, callback: Callable[[Exception], Coroutine[None, None, None]]):
        """Add callback function for error handling."""
        self.on_error_callbacks.append(callback)

    async def get_account_info(self) -> Dict[str, Any]:
        """Get futures account information from Binance."""
        if not self.client:
            raise RuntimeError("Client not initialized")
        return await self.client.futures_account()
