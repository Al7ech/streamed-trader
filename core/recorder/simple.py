"""성과 지표에 필요한 것만 모으는 가벼운 백테스트 레코더.

:class:`~core.recorder.full.FullRecorder`의 베이스다 — 그쪽에서 buy & hold 기준선(심볼별 종가
수집 + ``close()`` 때의 곡선 생성)과 런 JSON/샤드 산출물을 뺀 것이다. 체결 목록, 자본곡선,
최대 레버리지만 남기므로 승률·수익·Sharpe·MDD는 그대로 계산된다. 계약은
:class:`core.recorder.base.Recorder`에 있다.

ETH 1m 6.5년(345만 이벤트) edit2 런에서 ``FullRecorder``(샤드 없음) 대비 약 2~3초가
빠진다. 파라미터 스윕처럼 같은 전략을 여러 번 돌리며 숫자만 볼 때 쓴다. 시각화 도구에 올릴
산출물이 필요하면 ``FullRecorder``를 쓴다.
"""

from typing import Dict, List, Optional, Tuple

from core.candle.candle import Candle
from core.account.report import Report
from core.account.status import Status
from core.account.trade import Trade
from core.recorder.base import Recorder


class SimpleRecorder(Recorder):
    """체결, 자본곡선, 최대 레버리지만 메모리에 모은다. ``Report.benchmark_curve``는 빈 리스트다.

    :param status: 실행기의 ``Status`` **객체 자체**. ``Report.status``가 최종 상태여야 하므로
        참조로 들고 있는다.
    """

    def __init__(self, status: Status):
        self._status = status
        self.equity_curve: List[Tuple[int, float]] = []
        self.trades: List[Trade] = []
        self.max_leverage = 0.0
        self._report: Optional[Report] = None

    def record_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        self.equity_curve.append((event_time, self._status.total_margin()))

    def record_trade(self, trade: Trade) -> None:
        self.trades.append(trade)
        if trade.leverage > self.max_leverage:
            self.max_leverage = trade.leverage

    @property
    def report(self) -> Optional[Report]:
        """:meth:`close` 뒤의 결과. 엔진이 실행을 마치고 돌려주는 값이다."""
        return self._report

    def close(self) -> None:
        """결과를 확정한다 — **멱등**이다."""
        if self._report is None:
            self._report = Report(self.trades, self.max_leverage, self._status,
                                  self.equity_curve)
