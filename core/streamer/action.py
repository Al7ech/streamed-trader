import re
from enum import Enum
from typing import Optional


#: Binance가 newClientOrderId에 허용하는 문자 집합. 백테스트에서 통과한 client_id는
#: 라이브에서도 반드시 통과해야 하므로, 엔진 중립 지점인 여기서 미리 검증한다.
_CLIENT_ID_RE = re.compile(r"^[.A-Z:/a-z0-9_-]{1,36}$")


class ActionType(Enum):
    """Action이 요청하는 주문의 종류.

    ``core.trader.BinanceExecutor.OrderType``과 일부러 분리된 타입이다 — ``core/streamer``와
    ``core/backtest``는 python-binance에 의존하지 않아야 하므로, 실행기 쪽에서 이 값을
    거래소 주문 타입으로 매핑한다.
    """

    #: 결정 캔들의 종가에 즉시 체결. 기존 동작이자 기본값.
    MARKET = "MARKET"
    #: 지정가. 이후 캔들이 지정가를 관통할 때 체결된다.
    LIMIT = "LIMIT"
    #: 조건부 시장가. 손절/익절 양쪽을 모두 표현한다 (트리거 방향으로 구분).
    STOP_MARKET = "STOP_MARKET"
    #: 미체결 주문 취소. quantity는 항상 0이다.
    CANCEL = "CANCEL"


class Action:
    """한 심볼에 대한 주문 요청.

    위치 인자 ``(symbol, quantity)``만 주면 예전과 똑같은 "종가 즉시 체결" 시장가 주문이다 —
    기존 전략과 강제청산 경로가 무수정으로 동작하도록 기본값을 그렇게 잡았다.

    ``quantity``는 **현재 포지션에 더할 부호 있는 수량**이다 (양수=매수/롱, 음수=매도/숏).
    지정가·조건부 주문에서도 의미는 같지만, 체결이 몇 봉 뒤에 일어나므로 그 사이 포지션이
    변했다면 ``reduce_only``로 보호해야 한다.
    """

    def __init__(self, symbol: str, quantity: float,
                 order_type: ActionType = ActionType.MARKET,
                 price: Optional[float] = None,
                 trigger_price: Optional[float] = None,
                 trigger_above: Optional[bool] = None,
                 reduce_only: bool = False,
                 client_id: Optional[str] = None,
                 expire_after_candles: Optional[int] = None):
        """
        :param price: LIMIT 지정가. LIMIT이면 필수.
        :param trigger_price: STOP_MARKET 트리거 가격. STOP_MARKET이면 필수.
        :param trigger_above: True면 가격이 트리거 **이상**으로 올라올 때, False면 트리거
            **이하**로 내려갈 때 발동한다. None이면 등록 시점의 ``status.last_close``와
            비교해 자동으로 정한다 (트리거가 현재가보다 높으면 위로 관통) — Binance가
            STOP_MARKET / TAKE_PROFIT_MARKET을 암묵적으로 갈라내는 것과 같은 규칙이다.
        :param reduce_only: 체결 시 현재 포지션 크기로 수량을 clamp하고, flat이면 체결하지
            않는다. 조건부 주문에는 사실상 필수다 — 없으면 이미 청산된 포지션에 걸어둔
            손절이 반대 방향 포지션을 새로 열어버린다.
        :param client_id: 취소용 식별자. 전략이 직접 짓는다. 같은 심볼의 미체결 주문 사이에서
            유일해야 한다 (거래소의 newClientOrderId 제약과 동일).
        :param expire_after_candles: 이 수만큼의 캔들이 지나도 체결되지 않으면 자동 취소한다.
            None이면 GTC — 체결되거나 명시적으로 취소될 때까지 남는다.
        """
        if not isinstance(order_type, ActionType):
            raise ValueError(f"order_type은 ActionType이어야 한다: {order_type!r}")
        if client_id is not None and not _CLIENT_ID_RE.match(client_id):
            raise ValueError(
                f"client_id가 거래소 규격에 맞지 않는다 (^[.A-Z:/a-z0-9_-]{{1,36}}$): {client_id!r}")
        if order_type is ActionType.LIMIT and price is None:
            raise ValueError("LIMIT 주문에는 price가 필요하다")
        if order_type is ActionType.STOP_MARKET and trigger_price is None:
            raise ValueError("STOP_MARKET 주문에는 trigger_price가 필요하다")
        if order_type is ActionType.CANCEL and quantity != 0:
            raise ValueError("CANCEL 액션의 quantity는 0이어야 한다")
        if expire_after_candles is not None and expire_after_candles < 1:
            raise ValueError(f"expire_after_candles는 1 이상이어야 한다: {expire_after_candles}")

        self.symbol = symbol
        self.quantity = quantity
        self.order_type = order_type
        self.price = price
        self.trigger_price = trigger_price
        self.trigger_above = trigger_above
        self.reduce_only = reduce_only
        self.client_id = client_id
        self.expire_after_candles = expire_after_candles

    @classmethod
    def cancel(cls, symbol: str, client_id: Optional[str] = None) -> "Action":
        """미체결 주문 취소 액션. ``client_id``가 None이면 그 심볼의 미체결 주문을 전부 취소한다."""
        return cls(symbol, 0.0, order_type=ActionType.CANCEL, client_id=client_id)

    @property
    def is_resting(self) -> bool:
        """제출 즉시 체결되지 않고 장부에 남는 주문인가."""
        return self.order_type in (ActionType.LIMIT, ActionType.STOP_MARKET)

    def __repr__(self):
        if self.order_type is ActionType.CANCEL:
            target = self.client_id if self.client_id is not None else "*"
            return f"[symbol: {self.symbol}, CANCEL {target}]"
        if self.order_type is ActionType.MARKET:
            # 기존 포맷을 그대로 유지한다 — 로그 grep 패턴이 여기 걸려 있다.
            return f"[symbol: {self.symbol}, quantity: {self.quantity}]"
        extra = f", price: {self.price}" if self.price is not None else ""
        extra += f", trigger: {self.trigger_price}" if self.trigger_price is not None else ""
        extra += f", trigger_above: {self.trigger_above}" if self.trigger_above is not None else ""
        extra += ", reduce_only" if self.reduce_only else ""
        extra += f", client_id: {self.client_id}" if self.client_id is not None else ""
        extra += (f", expire_after: {self.expire_after_candles}"
                  if self.expire_after_candles is not None else "")
        return (f"[symbol: {self.symbol}, quantity: {self.quantity}, "
                f"type: {self.order_type.value}{extra}]")
