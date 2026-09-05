"""미체결(resting) 주문 장부와 봉 내 체결 판정.

두 백테스터와 라이브 트레이더의 dry-run 경로가 **이 한 벌**을 공유한다. 체결 규칙이 세 곳에
흩어지면 dry-run과 백테스트가 조용히 갈라지는데, 이 프로젝트에서 dry-run은 백테스트와 대조하기
위해 존재하므로 그 갈라짐은 기능 자체를 무의미하게 만든다.

봉 내 체결의 가정 (전부 명시적이고, 캔들 데이터만으로는 검증 불가능한 부분이다):

* **한 캔들에 한 주문당 최대 1회 체결.** 부분 체결도, 호가 잔량도 모델링하지 않는다.
* **봉 내 경로는 알 수 없으므로 보수적으로 고정한다.** 한 심볼의 한 캔들 안에서
  STOP_MARKET을 먼저, 그다음 LIMIT을 처리하고, 각 그룹 안에서는 제출 순서(``seq``)를 따른다.
  즉 손절이 유리한 지정가 체결보다 먼저 일어났다고 본다. high가 먼저였는지 low가 먼저였는지
  캔들로는 알 수 없으니, 불리한 쪽을 택한다.
* **제출된 이벤트에서는 체결되지 않는다.** 결정은 봉이 마감된 뒤에 나오므로 그 봉 안에서
  채워질 수 없다. 다음 이벤트부터 매칭 대상이다.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

from core.backtest.status import Status
from core.streamer.action import Action, ActionType
from core.streamer.candle import Candle


logger = logging.getLogger(__name__)

#: 심볼당 미체결 주문 상한. 거래소에도 상한이 있고, 여기서는 매 캔들 주문을 새로 거는
#: 버그 있는 전략이 장부를 무한히 불리는 것을 막는 안전장치다.
MAX_OPEN_ORDERS_PER_SYMBOL = 64


@dataclass
class OpenOrder:
    """장부에 남아 있는 주문 하나."""

    symbol: str
    quantity: float
    order_type: ActionType
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    #: True면 가격이 트리거 이상으로 올라올 때, False면 트리거 이하로 내려갈 때 발동.
    #: 등록 시점에 확정된다 (Action.trigger_above가 None이면 last_close로 유도).
    trigger_above: bool = True
    reduce_only: bool = False
    client_id: Optional[str] = None
    #: 남은 캔들 수. None이면 GTC.
    remaining_candles: Optional[int] = None
    #: 제출 순서. 같은 봉 안의 결정적 정렬에 쓴다.
    seq: int = 0
    #: 제출 시각(ms). 진단용이자 Trade.submitted_at의 출처.
    created_at: int = 0
    #: 거래소가 매긴 주문 ID. 라이브에서만 채워진다.
    exchange_order_id: Optional[str] = None

    def to_dict(self) -> Dict:
        """dry-run 재시작 시 복원하기 위한 직렬화 (LiveRecorder의 last_status에 실린다)."""
        return {
            "symbol": self.symbol,
            "quantity": self.quantity,
            "order_type": self.order_type.value,
            "price": self.price,
            "trigger_price": self.trigger_price,
            "trigger_above": self.trigger_above,
            "reduce_only": self.reduce_only,
            "client_id": self.client_id,
            "remaining_candles": self.remaining_candles,
            "seq": self.seq,
            "created_at": self.created_at,
            "exchange_order_id": self.exchange_order_id,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "OpenOrder":
        return cls(
            symbol=d["symbol"],
            quantity=float(d["quantity"]),
            order_type=ActionType(d["order_type"]),
            price=d.get("price"),
            trigger_price=d.get("trigger_price"),
            trigger_above=bool(d.get("trigger_above", True)),
            reduce_only=bool(d.get("reduce_only", False)),
            client_id=d.get("client_id"),
            remaining_candles=d.get("remaining_candles"),
            seq=int(d.get("seq", 0)),
            created_at=int(d.get("created_at", 0)),
            exchange_order_id=d.get("exchange_order_id"),
        )


def register_order(status: Status, action: Action, event_time: int, seq: int) -> Optional[OpenOrder]:
    """지정가/조건부 액션을 장부에 올린다. 등록된 OpenOrder, 거부됐으면 None.

    ``action.trigger_above``가 None이면 그 심볼의 마지막 알려진 종가로 방향을 유도한다 —
    트리거가 현재가보다 위면 위로 관통할 때 발동(= Binance의 STOP_MARKET/TAKE_PROFIT_MARKET
    구분과 같은 규칙).
    """
    if not action.is_resting:
        raise ValueError(f"장부에 올릴 수 없는 액션이다: {action}")

    book = status.open_orders_for(action.symbol)

    if action.client_id is not None and any(o.client_id == action.client_id for o in book):
        # 거래소도 같은 newClientOrderId를 거부한다. 조용히 덮어쓰면 "손절을 새 가격으로
        # 다시 걸었다"고 착각한 채 옛 주문이 남으므로, 거절하고 드러낸다.
        logger.warning("client_id가 이미 장부에 있다 — 주문을 거부한다: %s", action)
        return None

    if len(book) >= MAX_OPEN_ORDERS_PER_SYMBOL:
        logger.warning("%s의 미체결 주문이 상한(%d)에 도달했다 — 주문을 거부한다: %s",
                       action.symbol, MAX_OPEN_ORDERS_PER_SYMBOL, action)
        return None

    trigger_above = action.trigger_above
    if action.order_type is ActionType.STOP_MARKET and trigger_above is None:
        ref = status.last_close.get(action.symbol)
        if ref is None:
            # 엔진은 결정 전에 last_close를 채우므로 여기 오면 비정상이다. 매수=위로 돌파,
            # 매도=아래로 이탈이라는 가장 흔한 모양으로 떨어뜨리고 경고한다.
            trigger_above = action.quantity > 0
            logger.warning("%s의 마지막 종가를 몰라 트리거 방향을 수량 부호로 유도한다: %s",
                           action.symbol, action)
        else:
            trigger_above = action.trigger_price >= ref

    order = OpenOrder(
        symbol=action.symbol,
        quantity=action.quantity,
        order_type=action.order_type,
        price=action.price,
        trigger_price=action.trigger_price,
        trigger_above=bool(trigger_above),
        reduce_only=action.reduce_only,
        client_id=action.client_id,
        remaining_candles=action.expire_after_candles,
        seq=seq,
        created_at=event_time,
    )
    book.append(order)
    return order


def cancel_orders(status: Status, symbol: str,
                  client_id: Optional[str] = None) -> List[OpenOrder]:
    """심볼의 미체결 주문을 취소한다. ``client_id``가 None이면 그 심볼 전부. 취소된 주문 목록 반환."""
    book = status.open_orders.get(symbol)
    if not book:
        return []
    if client_id is None:
        cancelled = list(book)
        book.clear()
        return cancelled
    cancelled = [o for o in book if o.client_id == client_id]
    if cancelled:
        status.open_orders[symbol] = [o for o in book if o.client_id != client_id]
    return cancelled


def cancel_all(status: Status) -> List[OpenOrder]:
    """전 심볼의 미체결 주문을 취소한다. 강제청산 경로에서 쓴다 — 파산 후에도 손절 주문이
    장부에 남아 있으면 flat인 계좌에 새 포지션을 여는 유령 체결이 생긴다."""
    cancelled: List[OpenOrder] = []
    for book in status.open_orders.values():
        cancelled.extend(book)
        book.clear()
    return cancelled


def tick_expiry(status: Status, symbol: str) -> List[OpenOrder]:
    """그 심볼의 캔들이 하나 지났음을 장부에 알리고, 만료된 주문을 제거해 반환한다.

    ``expire_after_candles=N``으로 제출한 주문은 이후 N개의 캔들에서 체결 기회를 갖는다 —
    매칭이 끝난 뒤에 이걸 부르므로 N번째 캔들에서도 체결될 수 있다.
    """
    book = status.open_orders.get(symbol)
    if not book:
        return []
    expired: List[OpenOrder] = []
    alive: List[OpenOrder] = []
    for order in book:
        if order.remaining_candles is None:
            alive.append(order)
            continue
        order.remaining_candles -= 1
        (expired if order.remaining_candles <= 0 else alive).append(order)
    if expired:
        status.open_orders[symbol] = alive
    return expired


def _fill_price(order: OpenOrder, candle: Candle) -> Optional[float]:
    """이 캔들이 주문을 체결시키는가. 체결가, 아니면 None. (슬리피지 적용 전)"""
    if order.order_type is ActionType.LIMIT:
        if order.quantity > 0:  # 지정가 매수 — 가격이 지정가 이하로 내려와야 한다
            if candle.open <= order.price:
                return candle.open  # 갭 관통: 시가가 이미 유리하므로 그 가격에 받는다
            return order.price if candle.low <= order.price else None
        if candle.open >= order.price:
            return candle.open
        return order.price if candle.high >= order.price else None

    if order.order_type is ActionType.STOP_MARKET:
        if order.trigger_above:
            if candle.open >= order.trigger_price:
                return candle.open  # 갭 상승: 트리거보다 위에서 봉이 시작 — 시가에 체결
            return order.trigger_price if candle.high >= order.trigger_price else None
        if candle.open <= order.trigger_price:
            return candle.open
        return order.trigger_price if candle.low <= order.trigger_price else None

    raise ValueError(f"장부에 있을 수 없는 주문 타입이다: {order.order_type}")


def _apply_slippage(order: OpenOrder, price: float, slippage_ratio: float) -> float:
    """조건부 시장가에만 불리한 방향으로 슬리피지를 얹는다.

    LIMIT은 정의상 지정가이거나 그보다 유리한 가격에만 체결되므로 대상이 아니고, MARKET은
    이 함수를 타지 않는다 (기존 백테스트 결과를 바꾸지 않기 위해).
    """
    if slippage_ratio <= 0.0 or order.order_type is not ActionType.STOP_MARKET:
        return price
    return price * (1.0 + slippage_ratio) if order.quantity > 0 else price * (1.0 - slippage_ratio)


def _sort_key(order: OpenOrder) -> Tuple[int, int]:
    # STOP_MARKET(0)을 LIMIT(1)보다 먼저 — 봉 내 경로를 모르므로 불리한 쪽을 택한다.
    return (0 if order.order_type is ActionType.STOP_MARKET else 1, order.seq)


def match_symbol(status: Status, symbol: str, candle: Candle,
                 slippage_ratio: float = 0.0) -> Iterator[Tuple[OpenOrder, float, float]]:
    """이 캔들이 체결시키는 주문들을 ``(order, fill_price, quantity)``로 하나씩 내놓는다.

    **제너레이터인 것이 설계의 일부다**: 호출자가 yield 사이에서 실제로 체결을 반영하므로,
    같은 캔들의 뒤쪽 주문이 갱신된 포지션을 본다 — ``reduce_only`` clamp가 올바르려면 필요하다.
    내놓기 전에 장부에서 제거하므로, 소비자가 중간에 멈춰도 이미 체결된 주문이 남지 않는다.

    ``reduce_only`` 주문이 포지션이 flat이거나 같은 방향일 때 발동하면 **체결 없이 취소**된다.
    그렇지 않으면 이미 청산된 포지션에 걸어둔 손절이 반대 방향 포지션을 새로 열어버린다.
    """
    book = status.open_orders.get(symbol)
    if not book:
        return

    for order in sorted(book, key=_sort_key):
        raw_price = _fill_price(order, candle)
        if raw_price is None:
            continue

        # 발동한 주문은 체결이든 reduce_only 취소든 장부를 떠난다.
        try:
            book.remove(order)
        except ValueError:  # 소비자가 앞선 yield에서 취소했다면 이미 없을 수 있다
            continue

        quantity = order.quantity
        if order.reduce_only:
            position = status.position_for(symbol).position
            if position == 0.0 or position * quantity > 0:
                logger.debug("reduce_only 주문이 발동했지만 줄일 포지션이 없다 — 취소한다: %s",
                             order)
                continue
            if abs(quantity) > abs(position):
                quantity = -position
        if quantity == 0.0:
            continue

        yield order, _apply_slippage(order, raw_price, slippage_ratio), quantity


def serialize(open_orders: Dict[str, List[OpenOrder]]) -> Dict[str, List[Dict]]:
    return {sym: [o.to_dict() for o in book] for sym, book in open_orders.items() if book}


def deserialize(raw: Optional[Dict]) -> Dict[str, List[OpenOrder]]:
    if not raw:
        return {}
    out: Dict[str, List[OpenOrder]] = {}
    for sym, orders in raw.items():
        try:
            out[sym] = [OpenOrder.from_dict(d) for d in orders]
        except (KeyError, TypeError, ValueError) as e:
            logger.error("미체결 주문 복원 실패 (symbol=%s): %s", sym, e)
    return out
