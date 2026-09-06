"""주문 실행과 계좌 상태의 도메인 경계.

엔진은 "언제 무엇을 결정하는가"만 알고, "그 결정이 어떻게 체결되는가"는 전부 여기 있다.
그래서 백테스트/드라이런/라이브가 같은 :class:`~core.engine.engine.TradingEngine`을 쓰면서
Executor만 갈아끼우는 것으로 갈린다:

- :class:`SimulatedExecutor` — 백테스트와 드라이런. 캔들로 체결을 판정하고 ``Status``를 직접
  갱신한다. **두 경로가 문자 그대로 같은 클래스를 쓰는 것**이 요점이다 — 드라이런은 백테스트와
  대조하기 위해 존재하므로, 체결 규칙이 갈라지면 기능 자체가 무의미해진다.
- ``LiveExecutor`` (:mod:`core.trader.live_executor`) — 실제 거래소. 주문을 fire-and-forget으로
  보내고, 체결은 나중에 유저 데이터 스트림으로 도착한다. 한 이벤트 안 액션들의 실행 순서는
  보장되지 않는다 (스레드풀) — 위 ``SimulatedExecutor``는 동기라 리스트 순서를 지킨다.

**체결은 반환값이 아니라 싱크(``on_trade``)로 흐른다.** 시뮬레이션은 체결 직후 동기적으로,
라이브는 소켓 이벤트가 도착했을 때 비동기적으로 부른다 — 이 비대칭을 인터페이스에서 지우는
유일한 방법이다. 엔진은 ``Trade``를 아예 보지 않는다.
"""

import copy
import logging
from abc import ABC, abstractmethod
from typing import Callable, Dict, Optional

from core.engine import order_book
from core.engine.status import Status
from core.engine.trade import Trade
from core.streamer.action import Action, ActionType
from core.streamer.candle import Candle

DEFAULT_FEE_RATIO = 0.0004
DEFAULT_SLIPPAGE_RATIO = 0.0

_logger = logging.getLogger(__name__)


def resolve_fee_ratio(streamer, explicit: Optional[float] = None) -> float:
    """실제로 부과할 수수료율. ``explicit``이 None이면 **스트리머의 값을 따라간다**.

    스트리머는 자기 ``fee_ratio``로 사이징하고 엔진은 자기 값으로 과금하므로, 둘이 어긋나면
    실효 레버리지가 의도와 달라진다. 그래서 기본값을 고정 상수로 두지 않고 스트리머를 따라가고,
    명시값이 어긋나면 경고한다.
    """
    streamer_value = getattr(streamer, "fee_ratio", None)
    if explicit is None:
        return streamer_value if streamer_value is not None else DEFAULT_FEE_RATIO
    if streamer_value is not None and streamer_value != explicit:
        _logger.warning(
            "수수료율 불일치: 엔진 %s vs 스트리머 %s — 스트리머는 자기 값으로 사이징하므로 "
            "실효 레버리지가 의도와 달라진다", explicit, streamer_value)
    return explicit


def resolve_slippage_ratio(streamer, explicit: Optional[float] = None) -> float:
    """조건부 시장가(STOP_MARKET) 체결에 불리한 방향으로 얹을 비율. 규칙은 수수료율과 같다."""
    streamer_value = getattr(streamer, "slippage_ratio", None)
    if explicit is None:
        return streamer_value if streamer_value is not None else DEFAULT_SLIPPAGE_RATIO
    if streamer_value is not None and streamer_value != explicit:
        _logger.warning("슬리피지율 불일치: 엔진 %s vs 스트리머 %s", explicit, streamer_value)
    return explicit


class Executor(ABC):
    """계좌 상태(``status``)를 소유하고 액션을 체결로 바꾼다.

    :param status: 이 실행기가 소유하는 계좌 상태. 엔진은 ``executor.status``를 통해서만
        계좌를 읽는다.
    :param on_trade: 체결 싱크. 보통 :meth:`~core.engine.recorder.Recorder.record_trade`.
    """

    def __init__(self, status: Status, on_trade: Optional[Callable[[Trade], None]] = None):
        self.status = status
        self.on_trade: Callable[[Trade], None] = on_trade or (lambda trade: None)
        self.logger = logging.getLogger(type(self).__module__)

    # ------------------------------------------------------------ 이벤트 훅

    def begin_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        """이벤트 처리 시작 훅. 기본은 아무것도 하지 않는다."""

    def match_resting(self, symbol: str, candle: Candle, event_time: int) -> None:
        """미체결 주문을 이 캔들로 체결시킨다. 기본은 아무것도 하지 않는다 (거래소가 채운다)."""

    def mark_to_market(self) -> float:
        """열린 포지션 전부를 각자의 최근 종가로 시가평가하고 자본을 돌려준다.

        이 이벤트에 캔들이 없는 심볼은 직전 알려진 종가를 그대로 쓴다.

        ``st.positions.items()``를 순회하며 이미 ``pos_state``를 손에 쥐고 있으므로,
        같은 포지션을 심볼로 다시 조회하는 ``st.update_unrealised_pnl(symbol, ...)``를
        부르지 않고 그 공식(``position * (price - avg_price)``)을 인라인한다 — 계산은 동일하다.
        이벤트·심볼당 불리므로 조회 한 번이 수백만 회 쌓인다.
        """
        st = self.status
        last_close = st.last_close
        for symbol, pos_state in st.positions.items():
            if pos_state.position != 0.0:
                price = last_close.get(symbol)
                if price is not None:
                    pos_state.unrealised_pnl = pos_state.position * (price - pos_state.avg_price)
        return st.total_margin()

    def force_liquidation(self, equity: float) -> bool:
        """파산이면 장부를 비우고 True. 기본은 항상 False (거래소가 청산한다)."""
        return False

    @abstractmethod
    def submit(self, action: Action, event_time: int) -> None:
        """액션 하나를 처리한다. 반환값은 없다 — 체결은 ``on_trade`` 싱크로 흐른다.

        백테스트/드라이런(:class:`SimulatedExecutor`)은 동기적으로 즉시 체결하거나 장부에
        올리므로 한 이벤트 안 액션들이 리스트 순서대로 반영된다. 라이브
        (:class:`~core.trader.live_executor.LiveExecutor`)는 주문을 스레드풀로 보내고 곧바로
        돌아오므로 **한 이벤트 안 액션들의 실행 순서는 보장되지 않는다.**
        """


class SimulatedExecutor(Executor):
    """캔들로 체결을 판정하는 가상 실행기. 백테스트와 드라이런이 공유한다.

    :param log_label: 체결 로그에 붙일 접두사. 드라이런은 ``"dry-run"``을 넘겨 실제 돈이 걸린
        체결과 구분되게 한다 (백테스트는 접두사가 없다).
    """

    def __init__(self, status: Status, fee_ratio: float, slippage_ratio: float = 0.0,
                 on_trade: Optional[Callable[[Trade], None]] = None, log_label: str = ""):
        super().__init__(status, on_trade)
        self.fee_ratio = fee_ratio
        self.slippage_ratio = slippage_ratio
        #: 미체결 주문 제출 순서. 같은 봉 안의 체결 순서를 결정적으로 만든다.
        self._order_seq = 0
        self._prefix = f"[{log_label}] " if log_label else ""

    def match_resting(self, symbol: str, candle: Candle, event_time: int) -> None:
        """``match_symbol``이 제너레이터라 체결은 한 건씩 즉시 반영되고, 같은 봉의 뒤쪽 주문은
        갱신된 포지션을 본다 (``reduce_only`` clamp가 올바르려면 필요하다).

        만료 처리는 매칭 **뒤에** 온다 — ``expire_after_candles=N``인 주문이 N번째 캔들에서도
        체결 기회를 갖도록.
        """
        for order, price, quantity in order_book.match_symbol(
                self.status, symbol, candle, self.slippage_ratio):
            self._fill(symbol, quantity, price, event_time,
                       order.order_type.value, order.created_at)
        for order in order_book.tick_expiry(self.status, symbol):
            self.logger.info("%s미체결 주문 만료: %s", self._prefix, order)

    def force_liquidation(self, equity: float) -> bool:
        """시가평가 자본이 0 이하로 떨어지면 파산이다. 증거금이 공유 풀이므로 한 심볼이 자본을
        다 태우면 나머지 심볼도 전부 청산된다 (flatten 액션은 엔진이 만든다).

        장부를 여기서 비우지 않으면 파산 후에도 손절 주문이 남아, flat이 된 계좌에 나중에 유령
        포지션을 여는 체결이 생긴다.
        """
        if equity > 0.0:
            return False
        cancelled = order_book.cancel_all(self.status)
        if cancelled:
            self.logger.warning("강제청산: 미체결 주문 %d건을 취소한다", len(cancelled))
        return True

    def submit(self, action: Action, event_time: int) -> None:
        """가상 실행기는 즉시 체결하거나 장부에 올린다 — 반환값이 없다.

        장부 조작(취소/등록)이 수량 검사보다 **먼저**다: CANCEL은 quantity가 0이라 뒤에 두면
        조용히 사라진다.
        """
        if action.order_type is ActionType.CANCEL:
            cancelled = order_book.cancel_orders(self.status, action.symbol, action.client_id)
            if cancelled:
                self.logger.debug("%s주문 취소: symbol=%s client_id=%s (%d건)", self._prefix,
                                  action.symbol, action.client_id or "*", len(cancelled))
            return None

        if action.is_resting:
            self._order_seq += 1
            if order_book.register_order(self.status, action, event_time, self._order_seq):
                self.logger.debug("%s미체결 주문 등록: %s", self._prefix, action)
            return None

        if action.quantity == 0:
            return None

        # 체결가는 액션의 **대상 심볼**의 마지막 알려진 종가다 — 트리거 심볼의 종가가 아니다.
        # 교차 심볼 액션이면 둘이 다르다.
        price = self.status.last_close.get(action.symbol)
        if price is None:
            self.logger.warning("%s가격을 알 수 없는 심볼 %s 에 대한 액션을 건너뛴다: %s",
                                self._prefix, action.symbol, action)
            return None

        self._fill(action.symbol, action.quantity, price, event_time,
                   ActionType.MARKET.value, event_time)
        return None

    def _fill(self, symbol: str, quantity: float, price: float, event_time: int,
              order_type: str, submitted_at: int) -> Trade:
        """체결 하나를 반영하고 ``on_trade``로 흘려보낸다.

        거래 전 스냅샷은 ``apply_fill`` **전에** 떠야 한다 — ``Trade.status``의 계약이고,
        승패 분류(``result_writer._win_lose_counts``)가 그 시점의 포지션 부호를 본다.

        ``apply_fill``의 청산 손익은 ``unrealised_pnl``을 안분해서 구하므로, 그 값이 **체결가
        기준**이어야 실현손익이 맞는다. 시장가는 체결가가 곧 이벤트 종가라 직전 시가평가가 이미
        그 값이지만, 지정가/조건부는 봉 중간 가격에 체결되므로 여기서 다시 매긴다 (시장가에는
        같은 값을 다시 계산하는 무해한 no-op이다).
        """
        self.status.update_unrealised_pnl(symbol, price)
        prev_status = copy.deepcopy(self.status)
        wnl, fee = self.status.apply_fill(symbol, quantity, price, self.fee_ratio)
        leverage = self.status.update_leverage()
        trade = Trade(
            timestamp=event_time,
            symbol=symbol,
            quantity=quantity,
            price=price,
            wnl=wnl,
            fee=fee,
            status=prev_status,
            leverage=leverage,
            order_type=order_type,
            submitted_at=submitted_at,
        )
        if self._prefix:
            # 백테스트는 체결이 수만 건이라 로그를 남기지 않는다. 드라이런만 남긴다 —
            # 라이브 체결 로그와 나란히 읽히는 것이 이 로그의 용도다.
            self.logger.info("%s%s filled symbol=%s qty=%s @ %s wnl=%.4f fee=%.4f -> %s",
                             self._prefix, order_type, symbol, quantity, price, wnl, fee,
                             self.status)
        self.on_trade(trade)
        return trade
