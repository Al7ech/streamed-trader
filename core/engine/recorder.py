"""실행 결과 적재 계층.

엔진은 이벤트 하나가 끝날 때마다 :meth:`Recorder.record_event`를, 체결이 날 때마다
:meth:`Recorder.record_trade`를 부른다. 무엇을 어디에 남길지는 전부 구현체가 정한다:

- :class:`~core.backtest.recorder.BacktestRecorder` — 메모리에 모았다가 ``Report``를 만들고,
  선택적으로 샤드를 쓴다.
- :class:`~core.live.recorder.LiveRecorder` — 주기적으로 체크포인트를 남기고 재기동 시
  이어쓴다.

지표 값은 인자로 받지 않고 **레코더가 직접 읽는다** (``streamer.indicators``). 필요 없는
레코더가 매 이벤트 전 지표를 순회하는 낭비를 없애기 위해서다 — 시리즈를 저장하지 않는
백테스트가 가장 흔한 경우다.
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional

from core.domain.candle import Candle
from core.domain.report import Report
from core.domain.trade import Trade


class Recorder(ABC):
    """엔진이 흘려보내는 이벤트/체결을 받는다."""

    @abstractmethod
    def record_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        """이벤트 하나를 적재한다.

        엔진은 모든 지표를 갱신하고 ``decide_action``을 부른 **직후**에 이것을 부른다. 따라서
        지금 ``streamer.indicators``에서 읽는 값은 그 결정이 실제로 본 값이다. 자본곡선 값이
        필요하면 이 시점의 ``self._status.total_margin()``을 읽으면 된다 — 미체결 주문 체결은
        이미 반영돼 있고, 이번 이벤트의 시장가 체결은 아직 반영되지 않았다.

        :param candles: 이번 이벤트에 캔들이 마감한 심볼만 담긴다.
        """

    @abstractmethod
    def record_trade(self, trade: Trade) -> None:
        """체결 하나를 적재한다. 호출 시점의 ``status``는 이미 이 체결이 반영된 상태다."""

    def end_event(self, event_time: int) -> None:
        """이벤트의 모든 액션 처리가 끝난 뒤. 기본은 아무것도 하지 않는다."""

    def close(self) -> None:
        """더 이상 기록하지 않는다. 기본은 아무것도 하지 않는다.

        엔진이 루프를 정상적으로 마친 뒤 불러 준다
        (:meth:`~core.engine.engine.TradingEngine.run_async`). 산출물을 파일로 남기는
        레코더는 여기서 마무리한다 — **멱등이어야 한다**. 엔진이 부른 뒤 호출자가 또 부를 수
        있기 때문이다 (라이브는 ``BinanceTrader.stop()``이 다시 부른다).
        """

    @property
    def report(self) -> Optional[Report]:
        """실행이 끝난 뒤의 결과. 기본은 ``None``.

        :meth:`~core.engine.engine.TradingEngine.run` / ``run_async``가 이것을 그대로
        돌려주므로, 백테스트처럼 결과를 메모리로 받아야 하는 쪽은
        (:class:`~core.backtest.recorder.BacktestRecorder`) 이 프로퍼티를 오버라이드한다.
        라이브 레코더처럼 파일로만 남기는 구현은 ``None``인 채로 둔다.
        """
        return None


class NullRecorder(Recorder):
    """아무것도 남기지 않는 레코더. 기록이 꺼진 라이브 실행처럼 엔진은 돌려야 하지만 결과를
    적재할 필요가 없을 때 쓴다 — 매번 ``if recorder`` 로 감싸지 않기 위한 것이다."""

    def record_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        pass

    def record_trade(self, trade: Trade) -> None:
        pass
