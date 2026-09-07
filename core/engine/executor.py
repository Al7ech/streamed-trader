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


class Executor(ABC):
    """계좌 상태(``status``)를 소유하고 액션을 체결로 바꾼다.

    **``Status``는 구현체가 자기 생성 경로에서 만든다.** 바깥에서 만들어 넘기지 않는다 —
    초기 상태가 무엇인지는 모드마다 다르고(백테스트/드라이런은 초기 증거금이라는 숫자 하나,
    라이브는 거래소가 정답) 그건 실행기만 아는 사실이기 때문이다. 특히 라이브에서 호출자가
    자리채우기 ``Status(margin=0.0)``을 만들어 넘기면 수화되기 전의 그 빈 껍데기를 누가
    읽어도 예외 없이 통과한다 — 런 JSON의 ``init_margin``이 조용히 0이 되는 식으로.
    그래서 이 생성자는 각 구현체의 생성 경로
    (:class:`~core.backtest.simulated_executor.SimulatedExecutor` 의 ``__init__``,
    :meth:`~core.live.executor.LiveExecutor.create`)만 부른다.

    엔진과 레코더는 ``executor.status``를 통해서만 계좌를 읽는다.

    :param status: 이 실행기가 소유하는 계좌 상태. 위 설명대로 구현체가 만들어 넘긴다.
    :param on_trade: 체결 싱크. 보통 :meth:`~core.engine.recorder.Recorder.record_trade`.
    """

    def __init__(self, status: Status, on_trade: Optional[Callable[[Trade], None]] = None):
        self.status = status
        self.on_trade: Callable[[Trade], None] = on_trade or (lambda trade: None)
        self.logger = logging.getLogger(type(self).__module__)

    # ------------------------------------------------------------ 이벤트 훅

    def begin_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        """이벤트 처리 시작 훅. 기본은 아무것도 하지 않는다.

        :class:`~core.backtest.simulated_executor.SimulatedExecutor`는 여기서 심볼별 최근
        종가를 갱신하고, **미체결 주문을 이 이벤트 캔들로 체결시키고**, 열린 포지션을
        시가평가하고, 시가평가 자본이 0 이하면 장부를 비우고 전 포지션을 강제청산한다 (그
        결과가 이벤트의 자본곡선 값이 된다). 라이브는 미체결 결정 폐기에만 쓴다 — 미체결
        매칭도 강제청산도 거래소 몫이고, 자본은 ``ACCOUNT_UPDATE``가 정답이다.

        파산은 캔들로만 판정되는 백테스트 고유 개념이라 이 포트에 등장하지 않는다 —
        엔진은 파산을 모르고, 강제청산은 위 구현체가 자기 ``begin_event`` 안에서 끝낸다.
        """

    @abstractmethod
    def submit(self, action: Action, event_time: int) -> None:
        """액션 하나를 처리한다. 반환값은 없다 — 체결은 ``on_trade`` 싱크로 흐른다.

        백테스트/드라이런(:class:`SimulatedExecutor`)은 동기적으로 즉시 체결하거나 장부에
        올리므로 한 이벤트 안 액션들이 리스트 순서대로 반영된다. 라이브
        (:class:`~core.live.executor.LiveExecutor`)는 주문을 스레드풀로 보내고 곧바로
        돌아오므로 **한 이벤트 안 액션들의 실행 순서는 보장되지 않는다.**
        """
