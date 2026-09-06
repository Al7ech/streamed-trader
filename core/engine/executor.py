"""주문 실행과 계좌 상태의 도메인 경계 — **포트(계약)만** 여기 있다.

엔진은 "언제 무엇을 결정하는가"만 알고, "그 결정이 어떻게 체결되는가"는 전부 실행기에 있다.
그래서 백테스트/드라이런/라이브가 같은 :class:`~core.engine.engine.TradingEngine`을 쓰면서
Executor만 갈아끼우는 것으로 갈린다. 구현체는 각자의 패키지에 있다:

- :class:`~core.backtest.simulated_executor.SimulatedExecutor` — 백테스트와 드라이런. 캔들로
  체결을 판정하고 ``Status``를 직접 갱신한다. **두 경로가 문자 그대로 같은 클래스를 쓰는 것**이
  요점이다 — 드라이런은 백테스트와 대조하기 위해 존재하므로, 체결 규칙이 갈라지면 기능 자체가
  무의미해진다. 그래서 라이브 패키지가 백테스트 패키지를 import한다.
- :class:`~core.live.executor.LiveExecutor` — 실제 거래소. 주문을 fire-and-forget으로
  보내고, 체결은 나중에 유저 데이터 스트림으로 도착한다. 한 이벤트 안 액션들의 실행 순서는
  보장되지 않는다 (스레드풀) — ``SimulatedExecutor``는 동기라 리스트 순서를 지킨다.

**체결은 반환값이 아니라 싱크(``on_trade``)로 흐른다.** 시뮬레이션은 체결 직후 동기적으로,
라이브는 소켓 이벤트가 도착했을 때 비동기적으로 부른다 — 이 비대칭을 인터페이스에서 지우는
유일한 방법이다. 엔진은 ``Trade``를 아예 보지 않는다.
"""

import logging
from abc import ABC, abstractmethod
from typing import Callable, Dict, Optional

from core.domain.action import Action
from core.domain.candle import Candle
from core.domain.status import Status
from core.domain.trade import Trade

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
        (:class:`~core.live.executor.LiveExecutor`)는 주문을 스레드풀로 보내고 곧바로
        돌아오므로 **한 이벤트 안 액션들의 실행 순서는 보장되지 않는다.**
        """
