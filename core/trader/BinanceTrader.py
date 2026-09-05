"""
BinanceTrader: Real-time WebSocket-based trading module for the StreamedTrader project.

This module connects to Binance WebSocket streams to receive real-time kline (candle) data
and integrates with the streamer system to get trading actions.
"""

import asyncio
import copy
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, Dict, Any, Coroutine, List, Set, Tuple

from binance import AsyncClient, BinanceSocketManager
from binance.enums import ContractType
from binance.exceptions import BinanceAPIException, BinanceWebsocketClosed

from core.engine import order_book
from core.engine.order_book import OpenOrder
from core.engine.status import Status
from core.binance_candle_fetcher.fetcher import BinanceCandleFetcher
from core.streamer.action import Action, ActionType
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.trader.BinanceExecutor import BinanceExecutor
from core.trader.ReliableWebsocket import ReliableWebsocket
from core.trader.live_recorder import DEFAULT_SHARD_FLUSH_EVERY, LiveRecorder, default_run_id
from core.utils import generate_dict_string, ms_timestamp_to_datetime, interval_to_minutes


DEFAULT_FEE_RATIO = 0.0004

#: 선물 정산(마진) 자산 후보. 심볼에서 접미사로 떼어내 잔고 항목을 찾는다.
#: 긴 것부터 검사해야 USDT/USDC 같은 4자리가 USD류 접두와 헷갈리지 않는다.
_MARGIN_ASSETS = ("FDUSD", "BUSD", "USDT", "USDC", "BNB", "BTC", "ETH")

#: 주문이 종결된 것으로 보는 ORDER_TRADE_UPDATE 상태들. CANCELED/EXPIRED도 부분 체결된
#: 수량이 남아 있을 수 있으므로 여기 포함된다 (예전 핸들러는 이걸 통째로 버렸다).
_TERMINAL_ORDER_STATES = ("FILLED", "CANCELED", "EXPIRED", "REJECTED")

#: 기록/로그 대상이 되는 상태. 종결 상태 + 진행 중인 부분 체결.
_TRACKED_ORDER_STATES = _TERMINAL_ORDER_STATES + ("PARTIALLY_FILLED",)

#: 거래소가 주문을 접수했다고 알리는 상태. 지정가/조건부 주문은 여기서부터 **미체결 상태로
#: 살아 있으므로**, 이 이벤트로 status.open_orders에 등록한다 (시장가는 곧바로 체결돼
#: 종결되므로 사실상 지나가는 상태다).
_ACK_ORDER_STATE = "NEW"

#: 결정 수량과 실제 체결 수량의 상대 차이가 이 값을 넘으면 경고한다.
#: ``BinanceExecutor.execute_action``은 거래소의 step size로 양자화하지 않으므로 약간의
#: 차이는 정상이고 문서화돼 있다. 다만 "약간"이 얼마인지는 아무도 보지 않고 있었다.
_QUANTITY_DIVERGENCE_TOLERANCE = 0.01

#: 진행 중인 주문 집계를 들고 있을 최대 개수. 지정가/조건부 주문이 생기면서 한 심볼에
#: 여러 주문이 동시에 미체결로 살 수 있게 됐으므로 (예전 주석의 "MARKET만 내므로 1을 넘지
#: 않는다"는 더 이상 사실이 아니다), 심볼 수 × 심볼당 장부 상한을 감당할 만큼 넉넉히 잡는다.
#: 종결 이벤트를 놓치면 항목이 영영 남는데, 이 프로세스는 몇 주씩 사는 게 정상이라 상한을
#: 둬서 무한히 쌓이지 않게 한다.
_MAX_OPEN_ORDER_AGGREGATES = 256

#: 체결을 기다리는 결정 스냅샷의 최대 개수. 미체결 주문은 몇 봉에서 몇 시간까지 살 수
#: 있으므로 캔들 단위로 비울 수 없고 (아래 _PendingDecision.resting 참고), 대신 상한을 둔다.
_MAX_PENDING_DECISIONS = 256

#: 백필로 메울 수 있는 최대 캔들 수. 이걸 넘으면 메우지 않고 stop() 해서 재기동에 맡긴다.
#: 두 가지를 한꺼번에 묶는 값이다: (a) 백필 재생은 캔들마다 주문 실행을 await 하므로 재생이
#: 길어지면 그동안 kline 큐(python-binance 기본 100건)가 넘쳐 소켓이 죽는다, (b) 아주 오래된
#: 구간의 결정을 무더기로 재생하면 주문 churn 이 커진다.
#: 재기동 경로는 이미 멀쩡하다 — _prefeed_indicators 가 지표를 이력에서 다시 세우고 포지션은
#: 거래소가 정답이다.
_MAX_BACKFILL_CANDLES = 60


@dataclass
class _PendingDecision:
    """스트리머가 방금 내린 결정. 거래소 체결 이벤트와 짝지어 Trade를 만들기 위해 들고 있는다.

    체결 이벤트(ORDER_TRADE_UPDATE)에는 **거래 전 Status**가 없다. ACCOUNT_UPDATE가 먼저
    도착해 self.status를 이미 갈아엎었을 수 있어서 그 시점에 스냅샷을 떠도 늦다. 그래서
    주문을 내보내기 직전의 상태를 여기 보관한다.

    키는 **client order id**다. 예전에는 심볼이었는데, 그러면 심볼당 in-flight 결정이 하나로
    제한되고 무엇보다 지정가/조건부 주문을 짝지을 수 없다 — 그 주문은 몇 봉 뒤에 체결되는데
    심볼 키 방식은 다음 캔들에서 스냅샷을 버렸다.
    """
    timestamp: int
    quantity: float
    pre_status: Status
    symbol: str = ""
    order_type: str = "MARKET"
    #: 장부에 남는 주문인가. 시장가 결정은 그 캔들 안에 결과가 나와야 하므로 다음 캔들에
    #: 버려지지만, 미체결 주문은 종결 이벤트가 올 때까지 살아 있어야 한다.
    resting: bool = False


@dataclass
class _OrderAggregate:
    """한 주문의 부분 체결들을 모아 Trade 하나로 만든다.

    백테스트의 Trade는 "액션 하나 = 체결 하나"인데 거래소는 한 주문을 여러 번에 나눠 채울 수
    있다. 부분 체결마다 Trade를 만들면 백테스트와 비교할 수 없는 모양이 되므로 주문 단위로 합친다.
    """
    order_id: int
    side: str
    symbol: str
    cum_qty: float = 0.0        # z: 누적 체결 수량 (부호 없음)
    avg_price: float = 0.0      # ap: 주문 전체의 평균 체결가
    wnl: float = 0.0            # Σ rp: 수수료 차감 **전** 실현손익 — 백테스트 wnl과 같은 정의
    fee: float = 0.0            # Σ n: 마진 자산으로 낸 수수료만
    fee_other: float = 0.0      # BNB 등 다른 자산으로 낸 수수료
    last_trade_ms: int = 0      # T


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
        Initialize the BinanceTrader.

        Args:
            api_key: Binance API key
            api_secret: Binance API secret
            interval: Kline interval (e.g., '1h', '1m', '5m')
            streamer: Streamer instance for trading decisions. Traded symbols are taken from
                ``streamer.symbols`` — there is no separate ``symbols`` argument here, so the
                trader can never disagree with the streamer about what it's trading.
            testnet: Whether to use testnet (default: True)
            fee_ratio: dry-run 회계에 적용할 수수료율. None이면 **스트리머의 값을 따라간다**
                (백테스터와 같은 규칙 — 스트리머는 자기 값으로 사이징하므로 어긋나면
                실효 레버리지가 의도와 달라진다). 라이브에서는 거래소 체결이 정답이라 미사용.
            record: True면 실행 결과를 ``<result_path>/live/`` 에 **백테스트와 같은 포맷**으로
                기록한다 (:class:`~core.trader.live_recorder.LiveRecorder`).
            result_path: 기록 루트. 백테스터의 같은 인자와 마찬가지로 **현재 작업 디렉토리**
                기준 상대 경로다.
            run_id: 기록에 쓸 런 식별자. None이면 전략/심볼/인터벌에서 고정 id를 만들어
                재기동해도 같은 런에 이어쓴다.
            run_metadata: 런 JSON metadata에 실을 추가 정보 (예: ``{"params": {...}}``).
            shard_flush_every: 월별 시계열 샤드를 다시 쓰는 주기(캔들 수).
        """
        self.logger = logging.getLogger(__name__)

        self.api_key = api_key
        self.api_secret = api_secret
        self.streamer = streamer
        self.symbols: List[str] = list(streamer.symbols)
        self._symbol_set: Set[str] = set(self.symbols)
        self.interval = interval
        #: 인터벌 길이(ms). 캔들 연속성 판정과 레코더 샤드 메타가 같이 쓴다.
        self._interval_ms = interval_to_minutes(interval) * 60_000
        #: 심볼별 마지막으로 처리한 캔들의 시작 시각 — 중복/구멍 판정의 기준점. 값이 None이면
        #: 그 심볼은 아직 기준이 없다 (지표 prefeed 전, 또는 prefeed 할 지표가 하나도 없는 경우).
        self._last_candle_start: Dict[str, Optional[int]] = {s: None for s in self.symbols}
        #: 정산(마진) 자산. Status.margin이 전 심볼 공유 풀이라 모든 심볼이 같은 자산이어야
        #: 하고, 여기서 한 번만 검증/계산해 캐시한다.
        self._margin_asset: str = self._resolve_margin_asset()
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
        #: 드라이런에서 조건부 시장가 체결에 얹을 슬리피지. 백테스터와 같은 규칙으로
        #: 스트리머에서 읽는다. 라이브는 실제 체결가가 오므로 쓰이지 않는다.
        self.slippage_ratio = getattr(streamer, "slippage_ratio", None) or 0.0
        #: 드라이런 장부의 주문 제출 순서 (백테스터의 같은 이름 필드와 같은 역할).
        self._order_seq = 0

        # WebSocket and client instances
        self.client: Optional[AsyncClient] = None
        self.socket_manager: Optional[BinanceSocketManager] = None
        self.kline_socket: Optional[ReliableWebsocket] = None
        self.user_socket: Optional[ReliableWebsocket] = None

        # Executor
        self._executor = BinanceExecutor(
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet,
            max_workers=2
        )

        # Control flags
        self.is_running = False

        # 리스너 태스크는 강한 참조를 들고 있어야 한다 — asyncio는 실행 중 태스크를 약한
        # 참조로만 잡아서, 놔두면 중간에 GC될 수 있다.
        self._tasks: set = set()

        # 결과 기록 (백테스트와 같은 포맷). recorder는 start()에서 만들어진다 — 라이브
        # init_margin이 지갑 조회 뒤에 정해지기 때문이다.
        self.record = record
        self.result_path = result_path
        self.run_id = run_id
        self.run_metadata = run_metadata or {}
        self.shard_flush_every = shard_flush_every
        self.recorder: Optional[LiveRecorder] = None
        # 키는 (심볼, client order id). 심볼만으로는 심볼당 in-flight 결정이 하나로 제한되고,
        # 무엇보다 몇 봉 뒤에 체결되는 미체결 주문을 짝지을 수 없다.
        self._pending_decision: Dict[Tuple[str, str], _PendingDecision] = {}
        self._order_agg: Dict[Tuple[str, int], _OrderAggregate] = {}
        #: 시장가 주문에 붙일 client order id의 일련번호. 지정가/조건부는 전략이 지은
        #: client_id를 그대로 쓴다 (그래야 전략이 나중에 그 id로 취소할 수 있다).
        self._client_order_seq = 0
        self._warned_fee_asset = False

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
                # 거래소에 살아 있는 미체결 주문을 장부로 끌어온다. 프로세스가 죽어 있어도
                # 걸어둔 손절은 거래소에서 계속 살아 있으므로, 이걸 안 하면 전략이 "손절이
                # 없다"고 보고 하나 더 걸어 이중으로 청산된다.
                await self._load_open_orders()

            # 3. Pre-feed indicators with historical candle data.
            #    소켓을 열기 **전에** 한다 — 이 작업은 수십 초가 걸릴 수 있는데, 그동안
            #    유저 데이터 스트림을 읽지 않으면 python-binance의 큐가 넘쳐 죽는다.
            await self._prefeed_indicators()

            # 3-1. 결과 레코더. 지갑 조회(2) 뒤여야 라이브 init_margin이 실제 잔고이고,
            #      소켓을 열기(5) 전이어야 recorder가 None인 채로 캔들이 들어오지 않는다.
            self._setup_recorder()
            self._reconcile_resumed_position()
            self._reconcile_resumed_orders()

            # 4. 리스너 태스크 생성 전에 플래그를 세운다. 리스너 루프가 `while self.is_running`
            #    으로 시작하므로, 나중에 세우면 첫 await에서 태스크가 곧바로 빠져나간다.
            self.is_running = True

            # 5. Start WebSocket connections
            if not self.dry_run:
                await self._connect_user_websocket()
            await self._connect_kline_websocket()

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

        드라이런 자본은 합성값이라 프로세스가 죽으면 1e6으로 돌아간다. 자본 곡선은 이어붙는데
        상태만 초기화되면 재기동 지점에서 곡선이 초기값으로 튀어, 재개형 런이 만들어내는
        데이터가 통째로 못 쓰게 된다. 라이브는 거래소(``_load_futures_wallet_status``와
        ACCOUNT_UPDATE)가 정답이므로 절대 여기서 덮지 않는다.

        레코더가 ``self.status``를 참조로 들고 있으므로 **제자리에서** 갱신한다.
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

    def _new_client_order_id(self) -> str:
        """시장가 주문에 붙일 client order id. 체결 이벤트를 그 결정과 정확히 짝짓는 데 쓴다.

        예전에는 심볼만으로 짝지어서, 한 심볼에 결정이 둘 이상 떠 있으면 뒤엣것이 앞엣것을
        덮어썼다. 거래소 규격(``^[.A-Z:/a-z0-9_-]{1,36}$``)을 넘지 않는 짧은 형태로 만든다.
        """
        self._client_order_seq += 1
        return f"st-{self._client_order_seq}"

    async def _load_open_orders(self):
        """거래소의 미체결 주문을 ``status.open_orders``로 끌어온다 — **라이브 전용**."""
        try:
            raw = await self.client.futures_get_open_orders()
        except Exception as e:
            # 치명적이지 않다 — 장부가 비어 보일 뿐이고, 이후 ORDER_TRADE_UPDATE로 채워진다.
            # 다만 그 사이 전략이 손절을 중복으로 걸 수 있으므로 조용히 넘기면 안 된다.
            self.logger.error("미체결 주문을 불러오지 못했다: %s", e, exc_info=True)
            return

        self.status.open_orders = {}
        loaded = 0
        for o in raw:
            symbol = o.get("symbol", "")
            if symbol not in self._symbol_set:
                continue
            order = self._order_from_exchange(o)
            if order is None:
                continue
            self.status.open_orders_for(symbol).append(order)
            loaded += 1
        if loaded:
            self.logger.info("거래소의 미체결 주문 %d건을 불러왔다: %s", loaded,
                             {s: len(v) for s, v in self.status.open_orders.items() if v})

    def _order_from_exchange(self, o: Dict) -> Optional[OpenOrder]:
        """거래소 주문 표현(REST의 open order, 또는 ORDER_TRADE_UPDATE의 ``o``)을 OpenOrder로.

        REST와 스트림이 필드 이름을 다르게 쓰므로 (``origQty``/``q``, ``type``/``o`` …) 둘 다
        받는다. 이 엔진이 모르는 주문 타입(트레일링 스탑 등, 사람이 앱에서 낸 것)은 None을
        돌려 장부에 넣지 않는다 — 체결 판정 규칙이 없는 주문을 들고 있어봐야 오해만 낳는다.
        """
        raw_type = o.get("type") or o.get("o") or ""
        if raw_type in ("LIMIT", "STOP", "TAKE_PROFIT"):
            order_type = ActionType.LIMIT
        elif raw_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            order_type = ActionType.STOP_MARKET
        else:
            return None

        side = o.get("side") or o.get("S") or ""
        try:
            qty = float(o.get("origQty", o.get("q", 0.0)) or 0.0)
            price = float(o.get("price", o.get("p", 0.0)) or 0.0)
            trigger = float(o.get("stopPrice", o.get("sp", 0.0)) or 0.0)
        except (TypeError, ValueError):
            return None
        if qty <= 0:
            return None

        quantity = -qty if side == "SELL" else qty
        reference = self.status.last_close.get(o.get("symbol") or o.get("s") or "")
        trigger_above = trigger >= reference if (reference and trigger) else quantity > 0
        return OpenOrder(
            symbol=o.get("symbol") or o.get("s") or "",
            quantity=quantity,
            order_type=order_type,
            price=price or None,
            trigger_price=trigger or None,
            trigger_above=bool(trigger_above),
            reduce_only=bool(o.get("reduceOnly", o.get("R", False))),
            client_id=o.get("clientOrderId") or o.get("c") or None,
            created_at=int(o.get("time", o.get("T", 0)) or 0),
            exchange_order_id=str(o.get("orderId", o.get("i", "")) or ""),
        )

    def _reconcile_resumed_orders(self):
        """재개한 런이 기억하는 미체결 주문과 거래소의 실제 장부를 대조한다 — **라이브 전용**.

        포지션 대조(``_reconcile_resumed_position``)와 같은 이유이고, 오히려 더 중요하다:
        프로세스가 죽어 있는 동안에도 거래소에 걸어둔 손절 주문은 **계속 살아 있고 체결될 수
        있다**. 저장된 장부에는 있는데 거래소에 없다면 그 사이 체결됐거나 취소된 것이고,
        거래소에만 있다면 이 프로세스가 모르는 주문이 자기 포지션을 건드릴 수 있다.

        되살리지는 않는다 — 라이브에서는 거래소가 정답이고 ``status.open_orders``는 이미
        거래소 값이다 (``_load_open_orders``).
        """
        if self.dry_run or not self.recorder or not self.recorder.resumed_status:
            return
        saved = {(o.symbol, o.client_id) for book in
                 self.recorder.resumed_status.open_orders.values() for o in book}
        actual = {(o.symbol, o.client_id) for book in self.status.open_orders.values()
                  for o in book}
        gone = saved - actual
        unknown = actual - saved
        if not gone and not unknown:
            if actual:
                self.logger.info("재개한 런의 미체결 주문이 거래소와 일치한다 (%d건)", len(actual))
            return
        self.logger.warning(
            "재개한 런의 미체결 주문이 거래소와 다르다 — 저장됐지만 거래소에 없음: %s / "
            "거래소에만 있음: %s. 멈춰 있는 동안 체결·취소됐거나 이 프로세스가 모르는 주문이다. "
            "거래소 값으로 계속한다", sorted(gone) or "없음", sorted(unknown) or "없음")

    def _reconcile_resumed_position(self):
        """재개한 런이 기억하는 포지션과 거래소의 실제 포지션을 대조한다 — **라이브 전용**.

        드라이런은 저장된 상태가 곧 정답이라 (``_restore_dry_run_status``가 그대로 되살린다)
        대조할 대상이 없다. 라이브는 다르다: 프로세스가 죽어 있는 동안 청산/ADL이 일어났거나,
        앱에서 수동으로 포지션을 건드렸거나, 마지막 주문의 체결 이벤트를 못 받고 죽었을 수
        있다. 그러면 전략이 이어서 계산하는 포지션과 거래소의 실제 포지션이 어긋난 채로
        매매가 재개되는데, 지금까지는 거래소 상태와 복원된 상태를 각각 따로 찍기만 하고
        둘을 비교하는 곳이 없어서 이 상황이 조용히 지나갔다.

        되살리지는 않는다 — 라이브에서는 거래소가 정답이고 ``status``는 이미 거래소 값이다.
        여기서 하는 일은 사람이 알아챌 수 있게 남기는 것뿐이다.
        """
        if self.dry_run or not self.recorder or not self.recorder.resumed_status:
            return
        saved_status = self.recorder.resumed_status
        mismatched = {
            symbol: (saved_status.position_for(symbol).position, self.status.position_for(symbol).position)
            for symbol in self.symbols
            if not math.isclose(saved_status.position_for(symbol).position,
                                self.status.position_for(symbol).position,
                                rel_tol=1e-9, abs_tol=1e-12)
        }
        if not mismatched:
            self.logger.info("재개한 런의 포지션이 거래소와 일치한다: %s",
                             {s: self.status.position_for(s).position for s in self.symbols})
            return
        self.logger.warning(
            "재개한 런의 포지션이 거래소의 실제 포지션과 다르다 (심볼: 저장값 -> 실제값 — %s) — "
            "프로세스가 멈춘 사이의 청산/ADL/수동 주문이거나 마지막 체결 이벤트를 놓친 것이다. "
            "거래소 값으로 계속한다",
            ", ".join(f"{s}: {saved} -> {actual}" for s, (saved, actual) in mismatched.items()))

    def _spawn(self, coro) -> asyncio.Task:
        """리스너 태스크를 만들고 강한 참조를 유지한다 (GC 방지)."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def stop(self):
        """Stop the trader and close connections."""
        self.is_running = False

        # 소켓/실행기 정리보다 **먼저** 기록을 마무리한다. 아래 try에서 예외가 나면 남은
        # 정리가 통째로 건너뛰어지므로, 마지막 flush가 거기 묻히면 안 된다.
        # close()는 멱등이고 예외를 밖으로 내지 않는다 — stop()은 main의 finally와 리스너의
        # 치명적 오류 경로에서 최대 두 번 불릴 수 있다.
        if self.recorder:
            self.recorder.close()

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
        """Establish one multiplexed WebSocket connection carrying every symbol's kline stream.

        ``kline_futures_socket`` 자체가 내부적으로 continuousKline 스트림
        (``<symbol>_<contract_type>@continuousKline_<interval>``)을 구독한다 — 여러 심볼을
        같은 이름 규칙으로 만들어 ``futures_multiplex_socket``으로 묶는다. 묶인 메시지는
        ``{"stream": "...", "data": <rawPayload>}``로 오고, continuousKline 페이로드에는
        최상위 ``s``/``k.s``가 없다 — 대신 ``ps``(pair)로 심볼을 식별한다
        (``_process_kline_message`` 참고).
        """
        try:
            streams = [f"{sym.lower()}_{ContractType.PERPETUAL.value}@continuousKline_{self.interval}"
                      for sym in self.symbols]
            self.kline_socket = ReliableWebsocket(
                self.socket_manager.futures_multiplex_socket(streams=streams))
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
                    # exception()으로 스택트레이스까지 남긴다. 예전에는 한 줄뿐이라, 핸들러
                    # 안에서 KeyError가 나면 로그에 키 이름 하나만 남고 어느 줄인지 알 수 없었다.
                    self.logger.exception("Error processing kline message: %s", e)
                    await self._handle_error(e)

            # Something is very wrong at this point. Stop trader
            except Exception as e:
                if not self.is_running:
                    break
                self.logger.critical(f"Stopping trader. Error receiving kline message: {e}")
                await self.stop()
                break

    async def _process_kline_message(self, data: dict):
        """Parse an incoming kline message and hand the closed candle to the pipeline.

        메시지는 멀티플렉스 봉투 ``{"stream": "...", "data": <rawPayload>}``로 온다. 에러
        프레임은 이 봉투로 오지 않고 그대로 큐에 얹히므로(``ReconnectingWebsocket``이 합성),
        풀기 **전에** 먼저 검사해야 한다.
        """
        if await self._handle_stream_error_frame(data, "kline"):
            return

        payload = data.get("data") or {}
        symbol = (payload.get("ps") or "").upper()
        if symbol not in self._symbol_set:
            self.logger.warning(f"알 수 없는 심볼의 kline 메시지를 버린다: {payload.get('ps')!r}")
            return

        kline_data = payload.get('k', {})

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

        # Create Candle object.
        # 마감 시각은 **인터벌 경계**로 정규화한다. 웹소켓의 T(closeTime)는 경계 - 1ms 인데
        # BinanceCandleFetcher 는 경계(k[6] + 1)를 쓴다. 그대로 두면 백필한 캔들과 라이브 캔들의
        # 타임스탬프가 1ms 어긋나고, 백테스트 시계열과도 짝이 맞지 않는다.
        candle = Candle(
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            start_time=start_time,
            end_time=start_time + self._interval_ms
        )

        if not await self._ensure_continuity(symbol, candle):
            return

        await self._handle_candle(symbol, candle)

    async def _ensure_continuity(self, symbol: str, candle: Candle) -> bool:
        """이 캔들을 처리해도 되는지 판단하고, 앞에 빠진 캔들이 있으면 메워서 재생한다.

        python-binance 는 끊김을 스스로 재연결하고(``ReconnectingWebsocket._run_reconnect``)
        그 사이 메시지를 버린다. 게다가 그 사실은 예외가 아니라 ``{"e": "error"}`` 메시지로만
        알려지므로, 예전에는 두 가지가 조용히 일어났다: 마감 캔들이 통째로 빠져 지표가
        백테스트와 영구히 갈라지거나(청산 조건이 걸린 캔들을 놓치면 포지션이 그대로 남는다),
        재연결 직후 같은 마감 캔들이 다시 와서 ``decide_action`` 이 두 번 불리고 **주문이 두 번**
        나갔다.

        :return: 호출자가 이 캔들을 이어서 처리해야 하면 True.
        """
        last = self._last_candle_start[symbol]
        if last is None:
            return True

        if candle.start_time <= last:
            self.logger.warning(
                f"이미 처리한 캔들을 버린다 (symbol={symbol}): "
                f"start={ms_timestamp_to_datetime(candle.start_time)} <= "
                f"마지막 처리 {ms_timestamp_to_datetime(last)}")
            return False

        elapsed = candle.start_time - last
        if elapsed % self._interval_ms != 0:
            self.logger.fatal(
                f"Stopping trader. 캔들 경계가 인터벌과 맞지 않는다 (symbol={symbol}): "
                f"{elapsed}ms 는 {self._interval_ms}ms 의 배수가 아니다 "
                f"({ms_timestamp_to_datetime(last)} -> "
                f"{ms_timestamp_to_datetime(candle.start_time)})")
            await self.stop()
            return False

        missing = elapsed // self._interval_ms - 1
        if missing == 0:
            return True

        if missing > _MAX_BACKFILL_CANDLES:
            self.logger.fatal(
                f"Stopping trader. 캔들 {missing}개가 비었다 (symbol={symbol}) — 백필 상한 "
                f"{_MAX_BACKFILL_CANDLES}개를 넘어 재기동으로 복구한다")
            await self.stop()
            return False

        gap_start = last + self._interval_ms
        self.logger.warning(
            f"캔들 {missing}개가 비었다 (symbol={symbol}) — 백필해서 재생한다: "
            f"{ms_timestamp_to_datetime(gap_start)} ~ "
            f"{ms_timestamp_to_datetime(candle.start_time)}")
        try:
            missed = await self._fetch_missing_candles(symbol, gap_start, candle.start_time)
        except Exception as e:
            # 지표는 이미 갈라졌다. 이 상태로 계속 매매하면 전략이 백테스트와 다른 것을 본다.
            self.logger.fatal(f"Stopping trader. 빠진 캔들을 백필하지 못했다 (symbol={symbol}): {e}")
            await self.stop()
            return False

        for missed_candle in missed:
            await self._handle_candle(symbol, missed_candle)

        # 재생 도중 치명적 오류로 stop() 이 불렸을 수 있다.
        return self.is_running

    async def _fetch_missing_candles(self, symbol: str, start_ms: int, end_ms: int) -> List[Candle]:
        """``[start_ms, end_ms)`` 구간의 마감 캔들을 REST 로 가져온다.

        ``get_candles`` 의 구간 규약이 반열린 구간이라(``_calculate_chunk_dates`` 가 끝을 1ms
        당겨 초 단위로 포맷한다) 정확히 빠진 캔들만 돌아온다 — ``_prefeed_indicators`` 가 쓰는
        규약과 같다. 개수와 정렬을 검증해서, 어긋난 캔들이 조용히 지표에 먹히는 일이 없게 한다.
        """
        # 동기 HTTP + tqdm 이라 스레드로 뺀다 (_prefeed_indicators 와 같은 이유 — 이벤트 루프를
        # 막으면 그동안 유저 데이터 스트림을 읽지 못한다).
        candle_fetcher = BinanceCandleFetcher()
        candles = await asyncio.to_thread(
            candle_fetcher.get_candles,
            symbol=symbol,
            interval=self.interval,
            start_date=ms_timestamp_to_datetime(start_ms),
            end_date=ms_timestamp_to_datetime(end_ms)
        )

        expected = (end_ms - start_ms) // self._interval_ms
        if len(candles) != expected:
            raise ValueError(f"백필 캔들 개수가 맞지 않는다: {len(candles)} != {expected}")
        for i, c in enumerate(candles):
            want = start_ms + i * self._interval_ms
            if c.start_time != want:
                raise ValueError(
                    f"백필 캔들 {i}의 시작 시각이 어긋난다: "
                    f"{ms_timestamp_to_datetime(c.start_time)} != "
                    f"{ms_timestamp_to_datetime(want)}")
        return candles

    def _remember_decision(self, pending: _PendingDecision, symbol: str,
                           client_order_id: str) -> None:
        """체결 이벤트와 짝지을 거래 전 스냅샷을 보관한다.

        미체결 주문은 몇 시간씩 살 수 있어서 캔들 단위로 비울 수 없으므로, 대신 상한을 두고
        가장 오래된 것부터 버린다 (``_order_agg``와 같은 정책).
        """
        if len(self._pending_decision) >= _MAX_PENDING_DECISIONS:
            stale = next(iter(self._pending_decision))
            self.logger.warning(f"짝지어지지 않은 결정을 버린다: {stale}")
            self._pending_decision.pop(stale, None)
        self._pending_decision[(symbol, client_order_id)] = pending

    def _match_dry_run_orders(self, symbol: str, candle: Candle) -> bool:
        """드라이런에서 미체결 주문을 이 캔들로 체결시킨다. 체결이 있었으면 True.

        백테스터의 ``_match_resting_orders``와 **같은** ``order_book`` 코드를 쓴다 — 드라이런은
        백테스트와 대조하기 위해 존재하므로 체결 규칙이 갈라지면 기능 자체가 무의미해진다.
        호출 위치도 같다: 지표 갱신과 decide_action보다 앞이라, 전략이 "이미 손절된 포지션을
        아직 들고 있다"고 착각하지 않는다.
        """
        filled = False
        for order, price, quantity in order_book.match_symbol(
                self.status, symbol, candle, self.slippage_ratio):
            # 체결가 기준으로 다시 시가평가한 뒤 체결한다 — apply_fill의 실현손익이
            # unrealised_pnl 안분이라 그렇다 (백테스터 _fill과 같은 이유/같은 순서).
            self.status.update_unrealised_pnl(symbol, price)
            pre_status = copy.deepcopy(self.status)
            wnl, fee = self.status.apply_fill(symbol, quantity, price, self.fee_ratio)
            leverage = self.status.update_leverage()
            self.logger.info(
                f"[dry-run] {order.order_type.value} filled symbol={symbol} qty={quantity} "
                f"@ {price} wnl={wnl:.4f} fee={fee:.4f} -> {self.status}")
            if self.recorder:
                self.recorder.record_trade(symbol, candle.end_time, quantity, price, wnl, fee,
                                           pre_status, leverage,
                                           order_type=order.order_type.value,
                                           submitted_at=order.created_at)
            filled = True
        expired = order_book.tick_expiry(self.status, symbol)
        for order in expired:
            self.logger.info("[dry-run] 미체결 주문 만료: %s", order)
        return filled

    async def _handle_candle(self, symbol: str, candle: Candle):
        """마감 캔들 하나를 지표/스트리머/레코더에 흘려보내고, 나온 액션을 실행한다.

        라이브로 받은 캔들과 백필한 캔들이 **같은 경로**를 타야 하므로 수신부와 분리돼 있다.
        """
        # 기준점은 작업 **전에** 세운다. 아래에서 예외가 나더라도 같은 캔들이 다음번에 "구멍"
        # 으로 다시 잡혀 두 번 실행되면 안 된다.
        self._last_candle_start[symbol] = candle.start_time

        # 지난 캔들의 결정이 아직 남아 있다면 그 주문은 체결 이벤트를 못 받은 것이다
        # (주문 실패 등). 그대로 두면 이번 캔들의 체결에 엉뚱한 거래 전 스냅샷이 붙는다.
        # 액션은 자기 심볼과 다른 심볼을 겨냥할 수 있으므로 (교차 심볼 전략), pending은
        # **액션의 대상 심볼** 기준으로 키가 잡혀 있다 — 여기서는 지금 마감한 심볼 몫만 지운다.
        # 미체결(지정가/조건부) 주문은 몇 봉 뒤에 체결되는 게 정상이므로 여기서 버리면 안 된다
        # — 그 항목은 주문이 종결될 때(_process_order_trade_update) 지워진다. 시장가 결정만
        # 그 캔들 안에 결과가 나와야 한다.
        for key in [k for k, p in self._pending_decision.items()
                    if k[0] == symbol and not p.resting]:
            self.logger.warning(
                f"체결되지 않은 이전 결정을 버린다 (symbol={symbol}): "
                f"{self._pending_decision.pop(key)}")

        # last_close는 다른 심볼을 겨냥한 액션의 체결가/벤치마크 곡선에 쓰인다 — decide_action
        # 을 부르기 전에 갱신해야 한다 (백테스터의 이벤트별 last_close 갱신과 같은 순서).
        self.status.last_close[symbol] = candle.close
        self.status.update_unrealised_pnl(symbol, candle.close)

        # 드라이런은 거래소가 없으니 미체결 주문을 여기서 직접 채운다 — 백테스터와 **같은**
        # 자리(지표 갱신/decide_action 앞)에서, 같은 order_book 코드로. 라이브는 거래소가
        # 채우고 ORDER_TRADE_UPDATE로 알려주므로 이 경로를 타지 않는다.
        resting_filled = self._match_dry_run_orders(symbol, candle) if self.dry_run else False

        symbol_indicators = self.streamer.indicators.get(symbol, {})

        # 모든 지표가 decide_action보다 먼저 이 캔들을 반영한다 (백테스터와 같은 규약).
        # status는 decide_action이 보는 것과 같은 거래 전 스냅샷이다.
        for indicator_name, indicator in symbol_indicators.items():
            indicator.update(candle, self.status)

        # 교차 심볼 전략은 다른 심볼을 겨냥하는 액션도 반환할 수 있다 — 이후 처리는 각
        # 액션의 action.symbol을 그대로 따라간다.
        actions = self.streamer.decide_action(symbol, candle, self.status)

        # 백테스터와 **같은 자리**에서 기록한다: 지표가 이미 전부 갱신된 직후, decide_action이
        # 반환한 바로 뒤. 그래서 모든 컬럼이 결정이 실제로 본 값이다.
        if self.recorder:
            self.recorder.record_candle(symbol, candle, self.status)

        # 캔들 하나당 DEBUG 한 줄. 예전에는 candle/status/action/indicators/"Processed
        # completed"로 다섯 줄이었다 (1분봉이면 하루 7천 줄).
        # isEnabledFor로 감싸는 게 핵심이다: generate_dict_string은 전 지표를 순회하며
        # get_latest()를 부르는데, 인자로 넘기면 DEBUG가 꺼져 있어도 매 캔들 실행된 뒤 버려진다.
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("symbol=%s candle=%s actions=%s status=%s indicators=%s",
                              symbol, candle, actions, self.status,
                              generate_dict_string(symbol_indicators))

        traded = resting_filled
        for action in actions:
            # 취소는 quantity가 0이라 아래 스킵에 걸린다 — 타입 분기가 먼저다.
            if action.order_type is ActionType.CANCEL:
                cancelled = order_book.cancel_orders(self.status, action.symbol,
                                                     action.client_id)
                if cancelled:
                    self.logger.info("주문 취소: symbol=%s client_id=%s (%d건)",
                                     action.symbol, action.client_id or "*", len(cancelled))
                if not self.dry_run and self.on_action_callbacks:
                    await asyncio.gather(*[c(action) for c in self.on_action_callbacks])
                continue

            if action.quantity == 0:
                continue

            # 지정가/조건부는 client_id가 곧 취소 키이므로 전략의 것을 그대로 쓰고, 시장가는
            # 여기서 하나 지어 붙인다 — 체결 이벤트를 이 결정과 정확히 짝짓기 위해서다.
            if action.client_id is None:
                action.client_id = self._new_client_order_id()

            # 드라이런은 주문을 보내지 않으므로 장부를 직접 채운다 (백테스터와 같은 코드).
            if self.dry_run and action.is_resting:
                self._order_seq += 1
                if order_book.register_order(self.status, action, candle.end_time,
                                             self._order_seq):
                    self.logger.info("[dry-run] 미체결 주문 등록: %s", action)
                continue

            # 거래 전 스냅샷은 주문을 내보내기 **전에** 떠야 한다. _on_action이 주문 future를
            # await하며 이벤트 루프에 양보하므로, 그 사이 체결/계정 갱신 이벤트가 도착해
            # self.status를 이미 바꿔놓을 수 있다.
            # dry-run은 유저 데이터 소켓을 열지 않아 ORDER_TRADE_UPDATE가 오지 않는다. 짝지을
            # 상대가 없으므로 여기서 만들지 않고, 아래 dry-run 블록이 직접 스냅샷을 뜬다.
            if self.recorder and not self.dry_run:
                self._remember_decision(_PendingDecision(
                    timestamp=candle.end_time,
                    quantity=action.quantity,
                    pre_status=copy.deepcopy(self.status),
                    symbol=action.symbol,
                    order_type=action.order_type.value,
                    resting=action.is_resting,
                ), action.symbol, action.client_id)

            # Execute action if callback is set
            if self.on_action_callbacks:
                await asyncio.gather(*[c(action) for c in self.on_action_callbacks])

            if self.dry_run:
                # 라이브에서는 거래소 체결(ACCOUNT_UPDATE)이 status를 갱신하지만, dry-run에는
                # 체결이 없으므로 백테스터와 **같은** 회계를 직접 돌린다. 예전에는 자체 근사식이라
                # 부분 청산에서 avg_price를 현재가로 덮어써 미실현을 날렸고 margin은 아예 갱신하지
                # 않아서, dry-run 자본이 초기값에 영원히 고정됐다.
                # 가격은 액션의 **대상 심볼**의 마지막 알려진 종가로 매긴다 — 지금 마감한 캔들의
                # 종가가 아니다. 교차 심볼 액션이면 둘이 다를 수 있다 (SingleThreadedBacktester
                # 와 같은 규칙, status.last_close.get(action.symbol)).
                price = self.status.last_close.get(action.symbol)
                if price is None:
                    self.logger.warning(
                        f"[dry-run] 가격을 알 수 없는 심볼에 대한 액션을 건너뛴다: {action}")
                    continue
                # 백테스터 _fill과 같은 순서: 체결가로 시가평가 후 체결. 교차 심볼 액션은
                # 위에서 시가평가한 심볼과 대상 심볼이 다를 수 있어 이 줄이 필요하다.
                self.status.update_unrealised_pnl(action.symbol, price)
                pre_status = copy.deepcopy(self.status)
                wnl, fee = self.status.apply_fill(action.symbol, action.quantity, price,
                                                  self.fee_ratio)
                leverage = self.status.update_leverage()
                self.logger.info(f"[dry-run] filled symbol={action.symbol} qty={action.quantity} "
                                 f"@ {price} wnl={wnl:.4f} fee={fee:.4f} -> {self.status}")
                if self.recorder:
                    self.recorder.record_trade(action.symbol, candle.end_time, action.quantity,
                                               price, wnl, fee, pre_status, leverage,
                                               order_type=ActionType.MARKET.value,
                                               submitted_at=candle.end_time)
                    traded = True

        if self.recorder:
            # 체결이 있었으면 샤드까지 체크포인트한다 (라이브 체결 경로와 같은 정책).
            # 이걸 빼면 크래시 시 체결은 남았는데 그 체결을 낳은 캔들 구간의 시계열이
            # 통째로 없는, 앞뒤가 맞지 않는 런이 남는다.
            self.recorder.flush(force=traded)

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
                    self.logger.exception("Error processing user message: %s", e)
                    await self._handle_error(e)

            # Something is very wrong at this point. Stop trader
            except Exception as e:
                if not self.is_running:
                    break
                self.logger.critical(f"Stopping trader. Error receiving user message: {e}")
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
            updated: List[str] = []

            # Update margin balance — 해당 자산 항목이 있을 때만 (전 심볼 공유 자산 하나뿐)
            for m in (account_data.get('B') or []):
                if m.get('a') == self._margin_asset:
                    self.status.margin = float(m.get("wb", 0.0))
                    updated.append("margin")
                    break

            # exclude m=FUNDING_FEE
            if account_data.get("m", None) == "ORDER":
                # Process position updates — 우리가 다루는 심볼 항목만. 한 이벤트가 여러
                # 심볼의 포지션을 동시에 실어 올 수 있으므로 끝까지 훑는다 (첫 매치에서
                # break하지 않는다).
                position_updated = False
                for position in (account_data.get('P') or []):
                    pos_symbol = position.get('s', '')
                    if pos_symbol not in self._symbol_set:
                        continue
                    pos = self.status.position_for(pos_symbol)
                    pos.avg_price = float(position.get('ep', 0.0))  # entry price
                    pos.position = float(position.get('pa', 0.0))  # position amount
                    pos.unrealised_pnl = float(position.get('up', 0.0))  # unrealized
                    position_updated = True
                if position_updated:
                    updated.append("position")
                    self.status.update_leverage()

            # 실제로 뭔가 반영됐을 때만 INFO. 이 핸들러는 우리 자산/심볼 항목이 없으면
            # status를 건드리지 않는 게 설계인데(위 주석 참고), 예전에는 그런 이벤트 —
            # 다른 심볼의 주문, 펀딩피 정산 — 에서도 "Status updated"를 찍어서 아무것도
            # 바뀌지 않은 줄이 로그의 대부분을 차지했다.
            if updated:
                self.logger.info("Status updated from account (%s): %s",
                                 "+".join(updated), self.status)
            else:
                self.logger.debug("ACCOUNT_UPDATE에 %s/%s 항목이 없어 status를 유지한다 (m=%s)",
                                  self._margin_asset, self.symbols, account_data.get("m"))

        except Exception as e:
            # 바로 아래에서 re-raise 하므로 스택트레이스는 이걸 받는
            # _listen_user_websocket의 logger.exception이 남긴다. 여기서 또 쓰면 중복된다.
            self.logger.error("Error processing account update: %s", e)
            raise

    async def _process_order_trade_update(self, data):
        """Log ORDER_TRADE_UPDATE fills, and record them when a recorder is attached.

        status 자체는 ACCOUNT_UPDATE가 거래소 값으로 갱신하므로 여기서는 건드리지 않는다
        (두 곳에서 쓰면 어느 쪽이 정답인지 모호해진다).

        기록 쪽은 **주문 단위로 합친다**: 거래소는 한 주문을 여러 번에 나눠 채울 수 있는데
        부분 체결마다 Trade를 만들면 "액션 하나 = 체결 하나"인 백테스트와 모양이 달라진다.
        """
        try:
            order_data = data.get('o') or {}
            order_symbol = order_data.get('s', "")
            if order_symbol not in self._symbol_set:
                return
            status = order_data.get('X', "")

            # 거래소 장부의 진실을 그대로 따라간다: NEW면 미체결로 등록, 종결이면 제거.
            # 지정가/조건부 주문은 여기서부터 몇 시간씩 살 수 있으므로, 이 동기화가 없으면
            # 전략의 status.open_orders가 거래소와 어긋난 채로 돈다.
            self._sync_open_order(order_data, status)

            if status not in _TRACKED_ORDER_STATES:
                return

            order_id = int(order_data.get('i', 0) or 0)
            key = (order_symbol, order_id)
            if key not in self._order_agg:
                if len(self._order_agg) >= _MAX_OPEN_ORDER_AGGREGATES:
                    # 종결 이벤트를 못 받은 주문들이다. 가장 오래된 것부터 버린다 (dict는
                    # 삽입 순서를 유지한다).
                    stale_key = next(iter(self._order_agg))
                    self.logger.warning(f"종결되지 않은 주문 집계를 버린다: {stale_key}")
                    self._order_agg.pop(stale_key, None)
                self._order_agg[key] = _OrderAggregate(order_id, order_data.get('S', ""),
                                                        order_symbol)
            agg = self._order_agg[key]

            # rp/n/T는 실제 체결이 일어난 이벤트(x=TRADE)에서만 의미가 있다. 상태 전이만
            # 알리는 이벤트에서 더하면 손익과 수수료가 부풀려진다.
            if order_data.get('x', "") == "TRADE":
                agg.wnl += float(order_data.get('rp', 0.0) or 0.0)
                commission = float(order_data.get('n', 0.0) or 0.0)
                fee_asset = order_data.get('N') or self._margin_asset
                if fee_asset == self._margin_asset:
                    agg.fee += commission
                else:
                    # BNB 수수료 할인을 켜면 n이 BNB 단위로 온다. 마진 자산 손익에 그대로
                    # 더하면 wnl - fee 가 오염되므로 분리해서 담고 한 번만 경고한다.
                    agg.fee_other += commission
                    if not self._warned_fee_asset:
                        self._warned_fee_asset = True
                        self.logger.warning(
                            f"수수료가 마진 자산이 아닌 {fee_asset}(으)로 부과됐다 — "
                            f"기록되는 fee에서 제외된다 (metadata.fee_asset_mismatch 참고)")
                        if self.recorder:
                            self.recorder.set_metadata("fee_asset_mismatch", fee_asset)
            # z(누적 체결 수량)와 ap(평균 체결가)는 항상 주문 전체 기준의 최신값이다.
            agg.cum_qty = float(order_data.get('z', 0.0) or 0.0)
            agg.avg_price = float(order_data.get('ap', 0.0) or 0.0)
            agg.last_trade_ms = int(order_data.get('T', 0) or 0) or agg.last_trade_ms

            filled = -agg.cum_qty if agg.side == "SELL" else agg.cum_qty
            self.logger.info(
                f"order {status.lower()}: [quantity={filled},avg_price={agg.avg_price}]")

            if status not in _TERMINAL_ORDER_STATES:
                return  # 아직 진행 중 — 종결될 때 하나의 Trade로 합쳐 기록한다

            self._order_agg.pop(key, None)
            client_order_id = order_data.get('c') or ""
            if self.recorder and agg.cum_qty > 0:
                self._record_live_fill(agg, filled, client_order_id)
            else:
                # 한 건도 안 채워지고 취소/거절된 주문 — 짝지을 체결이 영영 없으므로
                # 스냅샷을 붙들고 있을 이유가 없다.
                self._pending_decision.pop((order_symbol, client_order_id), None)
        except Exception as e:
            # 바로 아래에서 re-raise 하므로 스택트레이스는 이걸 받는
            # _listen_user_websocket의 logger.exception이 남긴다. 여기서 또 쓰면 중복된다.
            self.logger.error("Error processing order trade update: %s", e)
            raise

    def _sync_open_order(self, order_data: Dict, order_status: str) -> None:
        """ORDER_TRADE_UPDATE를 ``status.open_orders``에 반영한다 — **라이브 전용**.

        거래소가 주문을 접수하면(NEW) 장부에 넣고, 종결되면(체결/취소/만료/거절) 뺀다.
        ``PARTIALLY_FILLED``은 아직 살아 있으므로 그대로 둔다 — 이 엔진은 부분 체결을
        모델링하지 않지만, 장부에서 지워버리면 남은 수량이 보이지 않게 된다.
        """
        symbol = order_data.get('s', "")
        client_id = order_data.get('c') or None
        book = self.status.open_orders_for(symbol)

        if order_status == _ACK_ORDER_STATE:
            if any(o.client_id == client_id for o in book):
                return  # 이미 등록됨 (재연결 후 중복 이벤트 등)
            order = self._order_from_exchange(order_data)
            if order is not None:
                book.append(order)
                self.logger.info("미체결 주문 등록: %s", order)
            return

        if order_status in _TERMINAL_ORDER_STATES:
            removed = [o for o in book if o.client_id == client_id]
            if removed:
                self.status.open_orders[symbol] = [o for o in book if o.client_id != client_id]
                self.logger.info("미체결 주문 해제 (%s): client_id=%s", order_status.lower(),
                                 client_id)

    def _record_live_fill(self, agg: _OrderAggregate, quantity: float,
                          client_order_id: str = ""):
        """종결된 주문 하나를 Trade로 기록한다.

        짝짓기 키는 거래소가 그대로 돌려주는 ``c``(clientOrderId)다 — 심볼만으로 짝지으면
        몇 봉 뒤에 체결되는 미체결 주문을 원래 결정과 이을 수 없다.
        """
        pending = self._pending_decision.pop((agg.symbol, client_order_id), None)

        if pending is None:
            # 청산/ADL/앱에서 낸 수동 주문/재기동 직후 남은 체결 등. 실제 자본을 움직이므로
            # 기록은 하되, 거래 전 스냅샷이 없어 현재 status로 대신한다 — ACCOUNT_UPDATE가
            # 이미 반영된 뒤일 수 있어 승패 분류가 틀릴 수 있다.
            self.logger.warning(
                f"결정과 짝지어지지 않은 체결 (order_id={agg.order_id}, qty={quantity}) — "
                f"거래 전 스냅샷을 근사한다")
            pre_status = copy.deepcopy(self.status)
            timestamp = agg.last_trade_ms
        else:
            if pending.quantity * quantity <= 0:
                self.logger.warning(
                    f"체결 수량 {quantity} 이 직전 결정 {pending.quantity} 과 방향이 다르다")
            elif abs(quantity - pending.quantity) > \
                    abs(pending.quantity) * _QUANTITY_DIVERGENCE_TOLERANCE:
                # 실행기가 step size로 양자화하지 않아서 생기는, 예상된 종류의 차이다
                # (CLAUDE.md의 "라이브가 백테스트와 다를 수밖에 없는 이유" 참고). 다만
                # 지금까지는 주문 요청과 체결이 서로 다른 줄에 찍힐 뿐 아무도 대조하지
                # 않아서, 실제 괴리가 얼마인지 로그에서 알 수 없었다.
                self.logger.warning(
                    "체결 수량이 결정과 %.2f%% 어긋났다: 결정 %s → 체결 %s "
                    "(step size 양자화 미적용, order_id=%s)",
                    abs(quantity - pending.quantity) / abs(pending.quantity) * 100,
                    pending.quantity, quantity, agg.order_id)
            pre_status = pending.pre_status
            if pending.resting:
                # 미체결 주문은 결정이 몇 봉 전이므로, 결정 캔들에 버킷하면 오히려 틀리다 —
                # **실제 체결 시각**을 쓴다. 백테스터는 체결을 감지한 봉의 마감 시각을 쓰므로
                # 둘이 최대 한 봉 어긋날 수 있다 (CLAUDE.md의 라이브/백테스트 차이 목록 참고).
                timestamp = agg.last_trade_ms or pending.timestamp
            else:
                # 체결 시각(T)이 아니라 **결정 캔들의 마감 시각**을 쓴다. T는 캔들 경계보다
                # 수백 ms 뒤라, 집계 뷰(1h/1d)에서 원인이 된 캔들과 다른 버킷에 떨어질 수 있다.
                timestamp = pending.timestamp

        order_type = pending.order_type if pending is not None else ActionType.MARKET.value
        submitted_at = pending.timestamp if pending is not None else timestamp
        self.recorder.record_trade(agg.symbol, timestamp, quantity, agg.avg_price, agg.wnl,
                                   agg.fee, pre_status, self.status.update_leverage(),
                                   order_type=order_type, submitted_at=submitted_at)
        self.recorder.flush(force=True)

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
            for asset in account_info['assets']:
                if asset["asset"] == self._margin_asset:
                    margin_balance = float(asset.get("walletBalance", 0.0))
                    break
            self.status.margin = margin_balance

            # Get position info for every traded symbol
            positions_by_symbol = {p['symbol']: p for p in account_info.get('positions', [])}
            for symbol in self.symbols:
                pos = self.status.position_for(symbol)
                position_info = positions_by_symbol.get(symbol)
                if position_info:
                    pos.avg_price = float(position_info.get('entryPrice', 0.0))
                    pos.unrealised_pnl = float(position_info.get('unrealizedProfit', 0.0))
                    pos.position = float(position_info.get('positionAmt', 0.0))
                else:
                    # No position for this symbol
                    pos.avg_price = 0.0
                    pos.unrealised_pnl = 0.0
                    pos.position = 0.0
            # 레버리지는 전 심볼을 다 채운 뒤 한 번만 계산한다 — 심볼별로 부르면 아직 값을
            # 채우지 않은 다른 심볼 때문에 중간값이 잘못 계산된다.
            self.status.update_leverage()

            self.logger.info(f"successfully loaded status: {self.status}")

        except Exception as e:
            self.logger.error(f"Failed to load futures wallet status: {e}")
            raise

    async def _prefeed_indicators(self):
        """Pre-feed every symbol's indicators with historical candle data.

        심볼마다 독립적으로(순차) 페치하지만, 봉 경계로 내림한 ``end_time``은 전 심볼이
        공유한다 — 심볼마다 ``datetime.now()``에서 다시 계산하면, 순차 페치가 실제로
        걸리는 시간만큼 뒤 심볼의 창이 앞 심볼보다 늦은 경계로 밀려 서로 어긋난다
        (``max_window``에서 나온 ``start_time``은 심볼마다 달라도 무방하다).
        """
        try:
            self.logger.info("Pre-feeding indicators with historical data...")

            interval_minutes = interval_to_minutes(self.interval)

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

            candle_fetcher = BinanceCandleFetcher()
            for symbol in self.symbols:
                symbol_indicators = self.streamer.indicators.get(symbol, {})
                max_window = 0
                for indicator_name, indicator in symbol_indicators.items():
                    max_window = max(max_window, indicator.window)

                if max_window == 0:
                    self.logger.info(f"No indicators found for {symbol}, skipping pre-feeding")
                    continue

                self.logger.info(f"[{symbol}] Maximum indicator window size: {max_window}")

                total_minutes = max_window * interval_minutes
                start_time = end_time - timedelta(minutes=total_minutes)

                self.logger.info(f"[{symbol}] Fetching historical data from {start_time} to {end_time}")

                # 동기 HTTP + tqdm이라 스레드로 뺀다 (수십 초간 이벤트 루프를 막으면 안 된다).
                candles = await asyncio.to_thread(
                    candle_fetcher.get_candles,
                    symbol=symbol,
                    interval=self.interval,
                    start_date=start_time,
                    end_date=end_time
                )

                # Assert
                actual_start = ms_timestamp_to_datetime(candles[0].start_time) if candles else None
                actual_end = ms_timestamp_to_datetime(candles[-1].end_time) if candles else None
                if max_window != len(candles) or actual_start != start_time or actual_end != end_time:
                    raise ValueError(
                        f"historical kline assert error for {symbol}: "
                        f"count {len(candles)} != {max_window}, "
                        f"start {actual_start} != {start_time}, "
                        f"end {actual_end} != {end_time}")

                start_datetime = candles[0].start_time
                end_datetime = candles[-1].end_time
                candle_count = len(candles)

                for candle in candles:
                    for indicator_name, indicator in symbol_indicators.items():
                        indicator.update(candle)

                # 라이브 캔들은 여기서부터 이어져야 한다. 기준점을 세워 두면 prefeed 와 첫
                # 라이브 캔들 사이에 생긴 구멍도 _ensure_continuity 가 잡아 백필한다 —
                # prefeed는 페처의 레이트리밋 대기까지 포함해 수십 초가 걸릴 수 있어
                # 실제로 벌어지는 일이다.
                self._last_candle_start[symbol] = candles[-1].start_time

                s = ms_timestamp_to_datetime(start_datetime)
                e = ms_timestamp_to_datetime(end_datetime)
                self.logger.info(
                    f"[{symbol}] Pre-fed indicators with {candle_count} historical candles: {s} ~ {e}")
                self.logger.info(f"[{symbol}] Pre-fed indicators: {generate_dict_string(symbol_indicators)}")

        except Exception as e:
            self.logger.error(f"Failed to pre-feed indicators: {e}")
            raise

    async def _on_action(self, action: Action):
        """Handle trading actions from streamer."""
        if self.dry_run:
            self.logger.info(f"dry run: {action}")

        else:
            # Execute order asynchronously. action.symbol이 대상 심볼이다.
            # reference_price는 조건부 주문을 STOP_MARKET / TAKE_PROFIT_MARKET 중 어느 쪽으로
            # 보낼지 고르는 데 쓴다 — 거래소는 트리거가 현재가의 반대쪽에 있는 주문을 거부한다.
            future = self._executor.execute_action(
                action, reference_price=self.status.last_close.get(action.symbol))

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
    def _suffix_margin_asset(self, symbol: str) -> str:
        """심볼의 정산(마진) 자산. 잔고 목록에서 우리 자산 항목을 찾는 데 쓴다.

        예전 구현은 ``symbol[3:]``이라 base가 3글자인 심볼에서만 우연히 맞았다.
        ``AVAXUSDT`` → ``"XUSDT"``, ``1000PEPEUSDT`` → ``"0PEPEUSDT"`` 가 되어 어떤 잔고
        항목에도 매치되지 않았고, 그 결과 ``status.margin``이 ``0.0``에 고정됐다.
        """
        for asset in _MARGIN_ASSETS:
            if symbol.endswith(asset) and len(symbol) > len(asset):
                return asset
        # _resolve_margin_asset이 __init__에서 심볼당 한 번만 부르므로(이벤트마다가 아니다),
        # _warned_fee_asset과 달리 warn-once 가드가 필요 없다.
        self.logger.warning(
            f"{symbol}의 정산 자산을 알 수 없다 — USDT로 가정한다. "
            f"알려진 자산: {_MARGIN_ASSETS}")
        return "USDT"

    def _resolve_margin_asset(self) -> str:
        """모든 ``self.symbols``가 정산되는 단일 자산. ``Status.margin``이 전 심볼이 공유하는
        증거금 풀 스칼라 하나뿐이므로, 이 트레이더가 다루는 심볼은 전부 같은 자산으로
        정산돼야만 회계가 성립한다 — 여기서 그 전제를 검증하고 한 번만 계산해 캐시한다.
        """
        assets = {sym: self._suffix_margin_asset(sym) for sym in self.symbols}
        unique = set(assets.values())
        if len(unique) > 1:
            mismatched = ", ".join(f"{s}->{a}" for s, a in assets.items())
            raise ValueError(
                f"self.symbols가 서로 다른 정산 자산으로 해석된다 ({mismatched}) — "
                f"Status.margin은 전 심볼 공유 풀이라 모든 심볼이 같은 자산이어야 한다.")
        return next(iter(unique))
