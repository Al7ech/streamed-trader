"""계좌 상태 도메인 — 공유 증거금 풀 + 심볼별 포지션 + 체결/결과 기록.

- :class:`~core.account.status.Status` — 계좌 상태 (공유 증거금 풀 + 심볼별 포지션 + 미체결 장부)
- :class:`~core.account.trade.Trade` — 체결 하나의 불변 기록
- :class:`~core.account.report.Report` — 실행 하나의 결과 묶음

재export는 ``status → trade → report`` 순으로만 한다 (``trade``/``report``가 ``status``를 쓴다).
"""

from core.account.status import DEFAULT_FEE_RATIO, PositionState, Status
from core.account.trade import Trade
from core.account.report import Report

__all__ = ["DEFAULT_FEE_RATIO", "PositionState", "Status", "Trade", "Report"]
