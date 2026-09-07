"""실제 거래소에 주문을 내고 계좌 상태를 거래소 값으로 유지하는 실행기.

:class:`~core.backtest.simulated_executor.SimulatedExecutor`와 대비되는 지점이 이 모듈의 요점이다:

- **체결이 비동기로 도착한다.** ``submit``은 주문을 fire-and-forget으로 보내고 곧바로 돌아온다
  (반환값 없음). 실제 체결은 나중에 유저 데이터 스트림(``ORDER_TRADE_UPDATE``)으로 오고, 그때
  ``on_trade`` 싱크로 흘러간다. 그래서 결정 시점의 거래 전 스냅샷을 client order id로 보관해
  뒀다가 체결이 왔을 때 짝짓는다. 주문 제출의 성공/실패는 detached 태스크
  (:meth:`LiveExecutor._await_order_result`)가 기다렸다가 실패면 ``_on_error``로 흘려보낸다 —
  엔진 루프는 그걸 기다리지 않는다.
- **``status``는 거래소가 정답이다.** ``ACCOUNT_UPDATE``가 margin/포지션을 덮고, 미체결 장부는
  ``ORDER_TRADE_UPDATE``가 동기화한다. 여기서 ``apply_fill``을 부르지 않는다.
- **미체결 주문을 시뮬레이션하지 않는다.** ``begin_event``는 낡은 시장가 결정만 폐기하고
  미체결 매칭은 하지 않는다 (:class:`SimulatedExecutor`의 ``begin_event`` 안에서 캔들로
  판정하는 그 로직이 여기엔 없다). 강제청산도 거래소가 한다 (``force_liquidation``이 항상 False).
- **생성이 곧 수화(hydration)다.** 유일한 생성 경로인 :meth:`LiveExecutor.create`가 거래소에서
  지갑/포지션/미체결 주문을 읽어 **완성된** ``Status``를 만들어 들고 돌아온다. 그래서 "아직
  채워지지 않은 ``Status``"라는 중간 상태가 존재하지 않는다 — 예전에는 호출자가
  ``Status(margin=0.0)``을 만들어 넘기고 나중에 ``load_account()``가 덮었기 때문에, 그 사이에
  누가 읽어도 예외 없이 통과했다 (런 JSON의 ``init_margin``이 조용히 0이 되는 식으로).
  거래소 응답의 해석은 :func:`build_status`/:func:`order_from_exchange`라는 순수 함수에 있어
  클라이언트 없이도 검증할 수 있다.

유저 데이터 소켓의 **수명주기**는 :class:`~core.live.trader.BinanceTrader`가 들고
있고, 받은 메시지만 :meth:`LiveExecutor.on_user_data`로 넘어온다.
"""

import asyncio
import copy
import logging
import math
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple

from binance import AsyncClient

from core.domain import order_book
from core.domain.action import Action, ActionType
from core.domain.candle import Candle
from core.domain.order_book import OpenOrder
from core.domain.status import DEFAULT_FEE_RATIO, Status
from core.domain.trade import Trade
from core.engine.executor import Executor
from core.live.binance_order_client import BinanceOrderClient, OrderResult

#: 선물 정산(마진) 자산 후보. 심볼에서 접미사로 떼어내 잔고 항목을 찾는다.
#: 긴 것부터 검사해야 USDT/USDC 같은 4자리가 USD류 접두와 헷갈리지 않는다.
MARGIN_ASSETS = ("FDUSD", "BUSD", "USDT", "USDC", "BNB", "BTC", "ETH")

#: 주문이 종결된 것으로 보는 ORDER_TRADE_UPDATE 상태들. CANCELED/EXPIRED도 부분 체결된
#: 수량이 남아 있을 수 있으므로 여기 포함된다.
_TERMINAL_ORDER_STATES = ("FILLED", "CANCELED", "EXPIRED", "REJECTED")
_TRACKED_ORDER_STATES = _TERMINAL_ORDER_STATES + ("PARTIALLY_FILLED",)
_ACK_ORDER_STATE = "NEW"

#: 결정 수량과 실제 체결 수량의 허용 괴리. 실행기가 step size로 양자화하지 않아 생긴다.
_QUANTITY_DIVERGENCE_TOLERANCE = 0.01

#: 종결 이벤트를 못 받은 항목이 무한히 쌓이지 않도록 하는 상한. 넘으면 가장 오래된 것부터 버린다.
_MAX_OPEN_ORDER_AGGREGATES = 256
_MAX_PENDING_DECISIONS = 256

#: 주문 하나의 거래소 확인을 기다리는 상한. 넘으면 오류로 보고 _on_error로 흘려보낸다.
#: 엔진 루프는 이걸 기다리지 않는다 — 결과 라우팅은 detached 태스크에서 일어난다.
ORDER_RESULT_TIMEOUT = 10.0

_logger = logging.getLogger(__name__)


def suffix_margin_asset(symbol: str) -> str:
    """심볼의 정산(마진) 자산. 잔고 목록에서 우리 자산 항목을 찾는 데 쓴다.

    예전 구현은 ``symbol[3:]``이라 base가 3글자인 심볼에서만 우연히 맞았다. ``AVAXUSDT`` →
    ``"XUSDT"``, ``1000PEPEUSDT`` → ``"0PEPEUSDT"`` 가 되어 어떤 잔고 항목에도 매치되지 않았고,
    그 결과 ``status.margin``이 ``0.0``에 고정됐다.
    """
    for asset in MARGIN_ASSETS:
        if symbol.endswith(asset) and len(symbol) > len(asset):
            return asset
    _logger.warning(f"{symbol}의 정산 자산을 알 수 없다 — USDT로 가정한다. "
                    f"알려진 자산: {MARGIN_ASSETS}")
    return "USDT"


def resolve_margin_asset(symbols: List[str]) -> str:
    """모든 심볼이 정산되는 단일 자산.

    ``Status.margin``이 전 심볼이 공유하는 증거금 풀 스칼라 하나뿐이므로, 한 트레이더가 다루는
    심볼은 전부 같은 자산으로 정산돼야만 회계가 성립한다 — 여기서 그 전제를 검증한다.
    """
    assets = {sym: suffix_margin_asset(sym) for sym in symbols}
    unique = set(assets.values())
    if len(unique) > 1:
        mismatched = ", ".join(f"{s}->{a}" for s, a in assets.items())
        raise ValueError(
            f"심볼들이 서로 다른 정산 자산으로 해석된다 ({mismatched}) — "
            f"Status.margin은 전 심볼 공유 풀이라 모든 심볼이 같은 자산이어야 한다.")
    return next(iter(unique))


def order_from_exchange(o: Dict) -> Optional[OpenOrder]:
    """거래소 주문 표현(REST의 open order, 또는 ``ORDER_TRADE_UPDATE``의 ``o``)을 OpenOrder로.

    REST와 스트림이 필드 이름을 다르게 쓰므로 (``origQty``/``q``, ``type``/``o`` …) 둘 다
    받는다. 이 엔진이 모르는 주문 타입(트레일링 스탑 등, 사람이 앱에서 낸 것)은 None을
    돌려 장부에 넣지 않는다 — 체결 판정 규칙이 없는 주문을 들고 있어봐야 오해만 낳는다.

    트리거 방향은 거래소가 준 ``type``+``side``로 그대로 복원한다 (STOP_MARKET+BUY 또는
    TAKE_PROFIT_MARKET+SELL이면 위로 관통 시 발동) — 기준가 추측이 필요 없다.
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
    trigger_above = (raw_type == "STOP_MARKET") == (side == "BUY")
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


def build_status(account_info: Dict, raw_open_orders: List[Dict], symbols: List[str],
                 margin_asset: str, fee_ratio: float = DEFAULT_FEE_RATIO) -> Status:
    """거래소 응답을 계좌 상태로 옮긴다 — **I/O 없는 순수 함수**.

    :meth:`LiveExecutor.create`가 받아온 ``futures_account()`` / ``futures_get_open_orders()``
    응답을 그대로 넣으면 완성된 ``Status``가 나온다. I/O와 해석을 갈라 둔 덕에 회계 규칙
    (아래 walletBalance 선택 등)을 클라이언트 없이 검증할 수 있다.

    ``fee_ratio``는 거래소 커미션 티어(``futures_commission_rate``의 taker 값)에서 온다 —
    라이브 사이징이 계좌 실제 수수료율을 쓰도록 ``Status``에 실어 준다. 회계는 여기서
    쓰지 않는다 (라이브 체결은 ``ORDER_TRADE_UPDATE``의 실제 ``n`` 금액이 정답).
    """
    if "error" in account_info:
        raise RuntimeError(f"Failed to get account info: {account_info['error']}")

    # walletBalance를 쓴다 — marginBalance는 walletBalance + unrealizedProfit 이고
    # status.total_margin()이 margin + unrealised_pnl 이라서, 아래에서 미실현을 따로
    # 넣는 순간 미실현이 두 번 세어진다. 스트림의 `wb`와도 이쪽이 같은 뜻이다.
    margin_balance = 0.0
    for asset in account_info["assets"]:
        if asset["asset"] == margin_asset:
            margin_balance = float(asset.get("walletBalance", 0.0))
            break
    status = Status(margin=margin_balance, fee_ratio=fee_ratio)

    positions_by_symbol = {p["symbol"]: p for p in account_info.get("positions", [])}
    for symbol in symbols:
        pos = status.position_for(symbol)
        info = positions_by_symbol.get(symbol)
        pos.avg_price = float(info.get("entryPrice", 0.0)) if info else 0.0
        pos.unrealised_pnl = float(info.get("unrealizedProfit", 0.0)) if info else 0.0
        pos.position = float(info.get("positionAmt", 0.0)) if info else 0.0
    # 레버리지는 전 심볼을 다 채운 뒤 한 번만 계산한다 — 심볼별로 부르면 아직 값을 채우지
    # 않은 다른 심볼 때문에 중간값이 잘못 계산된다.
    status.update_leverage()

    # 미체결 주문도 거래소에서 끌어온다. 프로세스가 죽어 있어도 걸어둔 손절은 거래소에서
    # 계속 살아 있으므로, 이걸 안 하면 전략이 "손절이 없다"고 보고 하나 더 걸어 이중으로
    # 청산된다.
    symbol_set = set(symbols)
    for o in raw_open_orders:
        symbol = o.get("symbol", "")
        if symbol not in symbol_set:
            continue
        order = order_from_exchange(o)
        if order is not None:
            status.open_orders_for(symbol).append(order)
    return status


@dataclass
class _PendingDecision:
    """주문을 내보내기 직전의 스냅샷. 나중에 도착할 체결 이벤트와 짝짓는다."""
    timestamp: int          # 결정 캔들의 end_time
    quantity: float
    pre_status: Status      # 주문을 보내기 **전에** 뜬 깊은 복사
    symbol: str = ""
    order_type: str = "MARKET"
    resting: bool = False   # True면 캔들 경계에서 버리지 않는다 (몇 봉 뒤 체결이 정상)


@dataclass
class _OrderAggregate:
    """한 주문의 부분 체결들을 합쳐 하나의 Trade로 만들기 위한 누적기."""
    order_id: int
    side: str
    symbol: str
    cum_qty: float = 0.0    # z
    avg_price: float = 0.0  # ap
    wnl: float = 0.0        # Σ rp (수수료 차감 전 실현손익 — 백테스트 wnl과 같은 정의)
    fee: float = 0.0        # Σ n, 마진 자산 기준
    fee_other: float = 0.0  # BNB 등 다른 자산으로 부과된 수수료
    last_trade_ms: int = 0  # T


class LiveExecutor(Executor):
    """실제 바이낸스 선물 계좌에 대고 주문을 실행한다.

    **직접 만들지 말고 :meth:`create`를 쓴다.** 이 생성자는 이미 수화된 ``status``와 이미
    연결된 클라이언트를 전제하므로, 그 둘을 마련하는 유일한 경로인 ``create``만 부른다.

    :param order_client: 저수준 주문 클라이언트 (스레드풀 + 재시도).
    :param status: :func:`build_status`가 거래소 응답으로 만든 **완성된** 계좌 상태.
    :param client: 계좌/주문 조회에 쓰는 ``AsyncClient``.
    :param owns_order_client: True면 :meth:`close`가 ``order_client``를 정리한다.
    :param owns_client: True면 :meth:`close`가 ``client``의 연결을 닫는다.
    :param on_error: 주문 실패/처리 오류를 흘려보낼 곳.
    :param on_metadata: 런 메타데이터에 남길 사실을 흘려보낼 곳 (수수료 자산 불일치 등).
    """

    def __init__(self, order_client: BinanceOrderClient, status: Status, symbols: List[str],
                 margin_asset: str, client: Optional[AsyncClient] = None,
                 owns_order_client: bool = False, owns_client: bool = False,
                 on_trade: Optional[Callable[[Trade], None]] = None,
                 on_error: Optional[Callable[[Exception], Awaitable[None]]] = None,
                 on_metadata: Optional[Callable[[str, object], None]] = None):
        super().__init__(status, on_trade)
        self._orders = order_client
        self.symbols = list(symbols)
        self._symbol_set: Set[str] = set(self.symbols)
        self.margin_asset = margin_asset
        self._on_error = on_error or self._default_on_error
        self.on_metadata = on_metadata or (lambda key, value: None)
        self.client = client
        #: 소유권은 "만든 쪽이 닫는다". 주입받은 클라이언트는 close()에서 건드리지 않는다 —
        #: 트레이더는 자기 AsyncClient를 소켓 매니저와 공유하므로 여기서 닫으면 안 된다.
        self._owns_order_client = owns_order_client
        self._owns_client = owns_client

        #: 키는 (심볼, client order id). 심볼만으로는 심볼당 in-flight 결정이 하나로 제한되고,
        #: 무엇보다 몇 봉 뒤에 체결되는 미체결 주문을 짝지을 수 없다.
        self._pending_decision: Dict[Tuple[str, str], _PendingDecision] = {}
        #: 키에 심볼이 들어가는 이유: 주문 ID가 계좌 안에서 심볼을 가로질러 유일하다는 보장이 없다.
        self._order_agg: Dict[Tuple[str, int], _OrderAggregate] = {}
        #: 아직 결과가 안 온 주문의 결과-대기 태스크. BinanceTrader._spawn과 같은 강한 참조 +
        #: done_callback 관용구 (GC 방지). stop()/테스트에서 drain한다.
        self._pending_order_tasks: Set[asyncio.Task] = set()
        self._client_order_seq = 0
        self._warned_fee_asset = False

    @classmethod
    async def create(cls, symbols: List[str], *, margin_asset: Optional[str] = None,
                     order_client: Optional[BinanceOrderClient] = None,
                     client: Optional[AsyncClient] = None,
                     api_key: Optional[str] = None, api_secret: Optional[str] = None,
                     testnet: bool = False,
                     on_trade: Optional[Callable[[Trade], None]] = None,
                     on_error: Optional[Callable[[Exception], Awaitable[None]]] = None,
                     on_metadata: Optional[Callable[[str, object], None]] = None
                     ) -> "LiveExecutor":
        """거래소에서 계좌를 읽어 **완성된** 실행기를 만든다. 라이브 실행기의 유일한 생성 경로.

        클라이언트는 **주지 않으면 스스로 만들고, 주면 그것을 쓴다** — 만든 것만
        :meth:`close`에서 정리한다. 트레이더는 소켓 매니저와 공유해야 하므로 자기
        ``AsyncClient``를 넘기고, 주문 클라이언트는 넘기지 않는다 (라이브에서만 필요한
        스레드풀이라 실행기가 만드는 게 맞다 — 예전에는 드라이런에서도 만들어졌다).

        ``futures_account()`` 실패는 치명적이지만 미체결 주문 조회 실패는 아니다
        (:meth:`_fetch_open_orders` 참고).
        """
        margin_asset = margin_asset or resolve_margin_asset(symbols)
        owns_order_client = order_client is None
        owns_client = client is None

        if owns_order_client:
            # binance.Client 생성자가 블로킹 HTTP(서버 시간/거래소 정보)를 친다 — 이벤트
            # 루프에서 그냥 부르면 그동안 소켓 수신이 멈춘다.
            order_client = await asyncio.to_thread(
                BinanceOrderClient, api_key=api_key, api_secret=api_secret,
                testnet=testnet, max_workers=2)
        try:
            if owns_client:
                client = await AsyncClient.create(api_key=api_key, api_secret=api_secret,
                                                  testnet=testnet)
            _logger.info("Loading futures wallet status...")
            account_info = await client.futures_account()
            fee_ratio = await cls._fetch_taker_commission(client, symbols)
            status = build_status(account_info, await cls._fetch_open_orders(client),
                                  symbols, margin_asset, fee_ratio)
        except Exception:
            # 기동에 실패했으면 **우리가 만든 것만** 되돌린다. 주입받은 것은 호출자 것이다.
            if owns_order_client:
                await asyncio.to_thread(order_client.shutdown)
            if owns_client and client is not None:
                await client.close_connection()
            raise

        executor = cls(order_client, status, symbols, margin_asset, client=client,
                       owns_order_client=owns_order_client, owns_client=owns_client,
                       on_trade=on_trade, on_error=on_error, on_metadata=on_metadata)
        executor.logger.info("successfully loaded status: %s (fee_ratio=%s)",
                             status, status.fee_ratio)
        if status.total_open_orders():
            executor.logger.info("거래소의 미체결 주문 %d건을 불러왔다: %s",
                                 status.total_open_orders(),
                                 {s: len(v) for s, v in status.open_orders.items() if v})
        return executor

    @staticmethod
    async def _fetch_open_orders(client: AsyncClient) -> List[Dict]:
        """거래소의 미체결 주문. 실패는 **치명적이지 않다** — 장부가 비어 보일 뿐이고 이후
        ``ORDER_TRADE_UPDATE``로 채워진다. 다만 그 사이 전략이 손절을 중복으로 걸 수 있으므로
        조용히 넘기지 않는다.
        """
        try:
            return await client.futures_get_open_orders()
        except Exception as e:
            _logger.error("미체결 주문을 불러오지 못했다: %s", e, exc_info=True)
            return []

    @staticmethod
    async def _fetch_taker_commission(client: AsyncClient, symbols: List[str]) -> float:
        """계좌의 taker 커미션율. ``Status.fee_ratio``로 실려 라이브 사이징이 실제 티어를 쓴다.

        커미션율은 심볼별이지만 ``Status.fee_ratio``는 하나뿐이라 ``symbols[0]``의 값을 쓰고,
        심볼끼리 다르면 경고한다 (``resolve_margin_asset``과 같은 "하나로 합의, 어긋나면 경고"
        관용구). 조회 실패는 **치명적이지 않다** — 기본값으로 떨어져도 사이징은 돈다.
        """
        try:
            rates = {}
            for symbol in symbols:
                resp = await client.futures_commission_rate(symbol=symbol)
                rates[symbol] = float(resp["takerCommissionRate"])
        except Exception as e:
            _logger.warning("커미션율을 불러오지 못했다 (기본값 %s 사용): %s",
                            DEFAULT_FEE_RATIO, e)
            return DEFAULT_FEE_RATIO
        chosen = rates[symbols[0]]
        if len(set(rates.values())) > 1:
            _logger.warning("심볼별 taker 커미션율이 다르다: %s — %s의 %s를 쓴다",
                            rates, symbols[0], chosen)
        return chosen

    async def close(self) -> None:
        """**자기가 만든** 자원만 정리한다. 주입받은 클라이언트는 만든 쪽이 닫는다."""
        if self._owns_order_client:
            # shutdown()의 기본값은 wait=True라 스레드가 끝날 때까지 이벤트 루프를 막는다.
            await asyncio.to_thread(self._orders.shutdown)
        # 풀이 모든 주문 future를 resolve한 뒤라, 결과-대기 태스크들은 한 틱이면 끝난다.
        # 마지막 실패 로그를 확실히 흘리고 shutdown을 deterministic하게 만든다.
        await self.drain_pending_orders()
        if self._owns_client and self.client is not None:
            await self.client.close_connection()
            self.client = None

    async def _default_on_error(self, error: Exception) -> None:
        self.logger.error(f"LiveExecutor error: {error}")

    # ------------------------------------------------------- Executor 인터페이스

    def begin_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        """지난 캔들의 시장가 결정 중 체결 이벤트를 못 받은 것을 버린다.

        그대로 두면 이번 캔들의 체결에 엉뚱한 거래 전 스냅샷이 붙는다. 미체결(지정가/조건부)
        주문은 몇 봉 뒤에 체결되는 게 정상이므로 버리면 안 된다 — 그 항목은 주문이 종결될 때
        지워진다. 시장가 결정만 그 캔들 안에 결과가 나와야 한다.

        액션은 다른 심볼을 겨냥할 수 있으므로(교차 심볼 전략) pending은 **대상 심볼** 기준으로
        키가 잡혀 있다 — 여기서는 이번 이벤트에 등장한 심볼 몫만 지운다.
        """
        for key in [k for k, p in self._pending_decision.items()
                    if k[0] in candles and not p.resting]:
            self.logger.warning(
                f"체결되지 않은 이전 결정을 버린다 (symbol={key[0]}): "
                f"{self._pending_decision.pop(key)}")

    def submit(self, action: Action, event_time: int) -> None:
        """액션을 거래소로 보낸다 (fire-and-forget). 실제 체결은 나중에 유저 데이터 스트림으로 온다.

        주문 성공/실패를 여기서 기다리지 않는다. 실패는 :meth:`_await_order_result`가 비동기로
        ``_on_error``에 흘려보내고, 거래소 진실인 ``status``가 다음 캔들에 스스로 복구한다.
        한 이벤트 안 액션들의 실행 순서는 보장되지 않는다.
        """
        if action.order_type is ActionType.CANCEL:
            # 장부는 여기서 바로 비운다. 거래소의 CANCELED 이벤트가 오면 _sync_open_order가
            # 한 번 더 지우려 하지만 이미 없으므로 무해하다.
            cancelled = order_book.cancel_orders(self.status, action.symbol, action.client_id)
            if cancelled:
                self.logger.info("주문 취소: symbol=%s client_id=%s (%d건)",
                                 action.symbol, action.client_id or "*", len(cancelled))
            self._dispatch(action)
            return

        if action.quantity == 0:
            return

        # 지정가/조건부는 client_id가 곧 취소 키이므로 전략의 것을 그대로 쓰고, 시장가는 여기서
        # 하나 지어 붙인다 — 체결 이벤트를 이 결정과 정확히 짝짓기 위해서다.
        if action.client_id is None:
            action.client_id = self._new_client_order_id()

        # 거래 전 스냅샷은 주문을 내보내기 **전에** 떠야 한다. 주문을 보낸 뒤 이벤트 루프가
        # 양보되면 그 사이 체결/계정 갱신 이벤트가 self.status를 이미 바꿔놓을 수 있다.
        self._remember_decision(_PendingDecision(
            timestamp=event_time,
            quantity=action.quantity,
            pre_status=copy.deepcopy(self.status),
            symbol=action.symbol,
            order_type=action.order_type.value,
            resting=action.is_resting,
        ), action.symbol, action.client_id)

        self._dispatch(action)

    def _dispatch(self, action: Action) -> None:
        future = self._orders.execute_action(action)
        # submit은 run_async가 도는 이벤트 루프 스레드에서 불리므로 create_task가 가능하다.
        task = asyncio.create_task(self._await_order_result(future, action))
        self._pending_order_tasks.add(task)
        task.add_done_callback(self._pending_order_tasks.discard)

    async def _await_order_result(self, future, action: Action) -> None:
        """스레드풀의 주문 결과를 기다렸다가 실패면 ``_on_error``로 흘려보낸다.

        엔진 루프는 이걸 기다리지 않는다. ``concurrent.futures.Future.result()``는 블로킹이라
        ``wrap_future``로 이벤트 루프에 브리지한다 — 이 저장소에서 유일하게 허용된 스레드→루프
        다리다.
        """
        try:
            result: Optional[OrderResult] = await asyncio.wait_for(
                asyncio.wrap_future(future), timeout=ORDER_RESULT_TIMEOUT)
        except Exception as e:
            self.logger.error("주문 결과 대기 실패 (action=%s): %s", action, e)
            await self._on_error(e)
            return

        # 실행기는 재시도 소진 후 예외 대신 success=False를 **반환**한다. 확인하지 않으면
        # 영구 거부된 주문이 흔적 없이 지나가고 전략이 거래소와 어긋난다.
        if result is None or not result.success:
            error = RuntimeError(
                f"order failed: {getattr(result, 'error', 'no result')} (action={action})")
            self.logger.error(str(error))
            await self._on_error(error)

    async def drain_pending_orders(self) -> None:
        """미해결 주문 결과-대기 태스크가 전부 끝날 때까지 기다린다 (stop / 테스트용)."""
        if self._pending_order_tasks:
            await asyncio.gather(*self._pending_order_tasks, return_exceptions=True)

    def _new_client_order_id(self) -> str:
        """시장가 주문에 붙일 client order id. 거래소 규격(``^[.A-Z:/a-z0-9_-]{1,36}$``) 이내."""
        self._client_order_seq += 1
        return f"st-{self._client_order_seq}"

    def _remember_decision(self, pending: _PendingDecision, symbol: str,
                           client_order_id: str) -> None:
        """미체결 주문은 몇 시간씩 살 수 있어서 캔들 단위로 비울 수 없으므로, 대신 상한을 두고
        가장 오래된 것부터 버린다 (``_order_agg``와 같은 정책)."""
        if len(self._pending_decision) >= _MAX_PENDING_DECISIONS:
            stale = next(iter(self._pending_decision))
            self.logger.warning(f"짝지어지지 않은 결정을 버린다: {stale}")
            self._pending_decision.pop(stale, None)
        self._pending_decision[(symbol, client_order_id)] = pending

    # ------------------------------------------------------- 계좌 상태 적재

    def reconcile_resumed(self, saved: Optional[Status]) -> None:
        """재개한 런이 기억하는 포지션/미체결 주문을 거래소의 실제 값과 대조한다.

        되살리지는 않는다 — 라이브에서는 거래소가 정답이고 ``status``는 이미 거래소 값이다.
        여기서 하는 일은 사람이 알아챌 수 있게 남기는 것뿐이다. 프로세스가 죽어 있는 동안
        청산/ADL이 일어났거나, 앱에서 수동으로 포지션을 건드렸거나, 마지막 주문의 체결
        이벤트를 못 받고 죽었을 수 있다.
        """
        if saved is None:
            return

        mismatched = {
            symbol: (saved.position_for(symbol).position,
                     self.status.position_for(symbol).position)
            for symbol in self.symbols
            if not math.isclose(saved.position_for(symbol).position,
                                self.status.position_for(symbol).position,
                                rel_tol=1e-9, abs_tol=1e-12)
        }
        if mismatched:
            self.logger.warning(
                "재개한 런의 포지션이 거래소와 다르다 (저장 -> 실제): %s. 멈춰 있는 동안 "
                "청산/ADL이나 수동 주문이 있었을 수 있다. 거래소 값으로 계속한다", mismatched)
        else:
            self.logger.info("재개한 런의 포지션이 거래소와 일치한다: %s",
                             {s: self.status.position_for(s).position for s in self.symbols})

        # 미체결 주문 대조가 오히려 더 중요하다: 프로세스가 죽어 있는 동안에도 거래소에 걸어둔
        # 손절 주문은 계속 살아 있고 체결될 수 있다.
        saved_ids = {(o.symbol, o.client_id) for book in saved.open_orders.values()
                     for o in book}
        actual_ids = {(o.symbol, o.client_id) for book in self.status.open_orders.values()
                      for o in book}
        gone, unknown = saved_ids - actual_ids, actual_ids - saved_ids
        if not gone and not unknown:
            if actual_ids:
                self.logger.info("재개한 런의 미체결 주문이 거래소와 일치한다 (%d건)",
                                 len(actual_ids))
            return
        self.logger.warning(
            "재개한 런의 미체결 주문이 거래소와 다르다 — 저장됐지만 거래소에 없음: %s / "
            "거래소에만 있음: %s. 멈춰 있는 동안 체결·취소됐거나 이 프로세스가 모르는 주문이다. "
            "거래소 값으로 계속한다", sorted(gone) or "없음", sorted(unknown) or "없음")

    # ------------------------------------------------------- 유저 데이터 스트림

    async def on_user_data(self, data: dict) -> None:
        """유저 데이터 스트림 메시지 하나를 처리한다 (트레이더가 라우팅해 준다)."""
        event_type = data.get("e")
        if event_type == "ACCOUNT_UPDATE":
            await self._process_account_update(data)
        elif event_type == "ORDER_TRADE_UPDATE":
            await self._process_order_trade_update(data)

    async def _process_account_update(self, data: dict) -> None:
        """ACCOUNT_UPDATE로 margin/포지션을 갱신한다.

        ACCOUNT_UPDATE는 **변경된 항목만** 싣는다 (Binance 명세). 우리 자산/심볼이 없는
        이벤트(다른 심볼의 주문, 잔고만 움직인 이벤트)에서 status를 0으로 덮어쓰면 안 된다 —
        마진이 0이 되면 모든 스트리머의 사이징이 붕괴하고, 포지션이 0이 되면 다음 종가 캔들에
        flat으로 보여 재진입해 **실제 거래소 포지션이 2배**가 된다.
        """
        try:
            account_data = data.get("a") or {}
            updated: List[str] = []

            # margin — 해당 자산 항목이 있을 때만 (전 심볼 공유 자산 하나뿐)
            for m in (account_data.get("B") or []):
                if m.get("a") == self.margin_asset:
                    self.status.margin = float(m.get("wb", 0.0))
                    updated.append("margin")
                    break

            # exclude m=FUNDING_FEE
            if account_data.get("m", None) == "ORDER":
                # 우리가 다루는 심볼 항목만. 한 이벤트가 여러 심볼의 포지션을 동시에 실어 올 수
                # 있으므로 끝까지 훑는다 (첫 매치에서 break하지 않는다).
                position_updated = False
                for position in (account_data.get("P") or []):
                    pos_symbol = position.get("s", "")
                    if pos_symbol not in self._symbol_set:
                        continue
                    pos = self.status.position_for(pos_symbol)
                    pos.avg_price = float(position.get("ep", 0.0))
                    pos.position = float(position.get("pa", 0.0))
                    pos.unrealised_pnl = float(position.get("up", 0.0))
                    position_updated = True
                if position_updated:
                    updated.append("position")
                    self.status.update_leverage()

            # 실제로 뭔가 반영됐을 때만 INFO. 이 핸들러는 우리 자산/심볼 항목이 없으면 status를
            # 건드리지 않는 게 설계인데, 그런 이벤트에서도 찍으면 아무것도 바뀌지 않은 줄이
            # 로그의 대부분을 차지한다.
            if updated:
                self.logger.info("Status updated from account (%s): %s",
                                 "+".join(updated), self.status)
            else:
                self.logger.debug("ACCOUNT_UPDATE에 %s/%s 항목이 없어 status를 유지한다 (m=%s)",
                                  self.margin_asset, self.symbols, account_data.get("m"))
        except Exception as e:
            # 호출자(트레이더의 유저 소켓 리스너)가 logger.exception으로 스택트레이스를 남긴다.
            self.logger.error("Error processing account update: %s", e)
            raise

    async def _process_order_trade_update(self, data: dict) -> None:
        """체결을 주문 단위로 합쳐 하나의 ``Trade``로 만든다.

        ``status`` 자체는 ACCOUNT_UPDATE가 거래소 값으로 갱신하므로 여기서는 건드리지 않는다
        (두 곳에서 쓰면 어느 쪽이 정답인지 모호해진다).

        **주문 단위로 합치는** 이유: 거래소는 한 주문을 여러 번에 나눠 채울 수 있는데 부분
        체결마다 Trade를 만들면 "액션 하나 = 체결 하나"인 백테스트와 모양이 달라진다.
        """
        try:
            order_data = data.get("o") or {}
            order_symbol = order_data.get("s", "")
            if order_symbol not in self._symbol_set:
                return
            order_status = order_data.get("X", "")

            # 거래소 장부의 진실을 그대로 따라간다: NEW면 미체결로 등록, 종결이면 제거.
            self._sync_open_order(order_data, order_status)

            if order_status not in _TRACKED_ORDER_STATES:
                return

            order_id = int(order_data.get("i", 0) or 0)
            key = (order_symbol, order_id)
            if key not in self._order_agg:
                if len(self._order_agg) >= _MAX_OPEN_ORDER_AGGREGATES:
                    # 종결 이벤트를 못 받은 주문들이다. 가장 오래된 것부터 버린다
                    # (dict는 삽입 순서를 유지한다).
                    stale_key = next(iter(self._order_agg))
                    self.logger.warning(f"종결되지 않은 주문 집계를 버린다: {stale_key}")
                    self._order_agg.pop(stale_key, None)
                self._order_agg[key] = _OrderAggregate(order_id, order_data.get("S", ""),
                                                       order_symbol)
            agg = self._order_agg[key]

            # rp/n/T는 실제 체결이 일어난 이벤트(x=TRADE)에서만 의미가 있다. 상태 전이만
            # 알리는 이벤트에서 더하면 손익과 수수료가 부풀려진다.
            if order_data.get("x", "") == "TRADE":
                agg.wnl += float(order_data.get("rp", 0.0) or 0.0)
                commission = float(order_data.get("n", 0.0) or 0.0)
                fee_asset = order_data.get("N") or self.margin_asset
                if fee_asset == self.margin_asset:
                    agg.fee += commission
                else:
                    # BNB 수수료 할인을 켜면 n이 BNB 단위로 온다. 마진 자산 손익에 그대로 더하면
                    # wnl - fee 가 오염되므로 분리해서 담고 한 번만 경고한다.
                    agg.fee_other += commission
                    if not self._warned_fee_asset:
                        self._warned_fee_asset = True
                        self.logger.warning(
                            f"수수료가 마진 자산이 아닌 {fee_asset}(으)로 부과됐다 — "
                            f"기록되는 fee에서 제외된다 (metadata.fee_asset_mismatch 참고)")
                        self.on_metadata("fee_asset_mismatch", fee_asset)

            # z(누적 체결 수량)와 ap(평균 체결가)는 항상 주문 전체 기준의 최신값이다.
            agg.cum_qty = float(order_data.get("z", 0.0) or 0.0)
            agg.avg_price = float(order_data.get("ap", 0.0) or 0.0)
            agg.last_trade_ms = int(order_data.get("T", 0) or 0) or agg.last_trade_ms

            filled = -agg.cum_qty if agg.side == "SELL" else agg.cum_qty
            self.logger.info(
                f"order {order_status.lower()}: [quantity={filled},avg_price={agg.avg_price}]")

            if order_status not in _TERMINAL_ORDER_STATES:
                return  # 아직 진행 중 — 종결될 때 하나의 Trade로 합쳐 기록한다

            self._order_agg.pop(key, None)
            client_order_id = order_data.get("c") or ""
            if agg.cum_qty > 0:
                self._emit_fill(agg, filled, client_order_id)
            else:
                # 한 건도 안 채워지고 취소/거절된 주문 — 짝지을 체결이 영영 없으므로 스냅샷을
                # 붙들고 있을 이유가 없다.
                self._pending_decision.pop((order_symbol, client_order_id), None)
        except Exception as e:
            self.logger.error("Error processing order trade update: %s", e)
            raise

    def _sync_open_order(self, order_data: Dict, order_status: str) -> None:
        """ORDER_TRADE_UPDATE를 ``status.open_orders``에 반영한다.

        거래소가 주문을 접수하면(NEW) 장부에 넣고, 종결되면(체결/취소/만료/거절) 뺀다.
        ``PARTIALLY_FILLED``은 아직 살아 있으므로 그대로 둔다 — 이 엔진은 부분 체결을
        모델링하지 않지만, 장부에서 지워버리면 남은 수량이 보이지 않게 된다.
        """
        symbol = order_data.get("s", "")
        client_id = order_data.get("c") or None
        book = self.status.open_orders_for(symbol)

        if order_status == _ACK_ORDER_STATE:
            if any(o.client_id == client_id for o in book):
                return  # 이미 등록됨 (재연결 후 중복 이벤트 등)
            order = order_from_exchange(order_data)
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

    def _emit_fill(self, agg: _OrderAggregate, quantity: float,
                   client_order_id: str = "") -> None:
        """종결된 주문 하나를 ``Trade``로 만들어 ``on_trade``로 흘려보낸다.

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
                # 실행기가 step size로 양자화하지 않아서 생기는, 예상된 종류의 차이다. 다만
                # 대조하는 곳이 없으면 실제 괴리가 얼마인지 로그에서 알 수 없다.
                self.logger.warning(
                    "체결 수량이 결정과 %.2f%% 어긋났다: 결정 %s → 체결 %s "
                    "(step size 양자화 미적용, order_id=%s)",
                    abs(quantity - pending.quantity) / abs(pending.quantity) * 100,
                    pending.quantity, quantity, agg.order_id)
            pre_status = pending.pre_status
            if pending.resting:
                # 미체결 주문은 결정이 몇 봉 전이므로, 결정 캔들에 버킷하면 오히려 틀리다 —
                # **실제 체결 시각**을 쓴다. 백테스트는 체결을 감지한 봉의 마감 시각을 쓰므로
                # 둘이 최대 한 봉 어긋날 수 있다.
                timestamp = agg.last_trade_ms or pending.timestamp
            else:
                # 체결 시각(T)이 아니라 **결정 캔들의 마감 시각**을 쓴다. T는 캔들 경계보다
                # 수백 ms 뒤라, 집계 뷰(1h/1d)에서 원인이 된 캔들과 다른 버킷에 떨어질 수 있다.
                timestamp = pending.timestamp

        self.on_trade(Trade(
            timestamp=timestamp,
            symbol=agg.symbol,
            quantity=quantity,
            price=agg.avg_price,
            wnl=agg.wnl,
            fee=agg.fee,
            status=pre_status,
            leverage=self.status.update_leverage(),
            order_type=pending.order_type if pending is not None else ActionType.MARKET.value,
            submitted_at=pending.timestamp if pending is not None else timestamp,
        ))
