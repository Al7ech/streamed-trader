"""매매 도메인의 값/상태 타입. 백테스트·드라이런·라이브가 **공유**한다.

이 계층은 `core.utils` 외에 저장소 안의 어떤 것도 import하지 않는다 — 엔진도, 전략도,
실행기도, 페처도 모른다. 그래서 이 방향으로만 의존이 흐른다::

    utils ← domain ← streamer / fetcher / engine ← result / backtest ← live

- :class:`~core.domain.candle.Candle` — 마감된 봉 하나 (시장 데이터의 최소 단위)
- :class:`~core.domain.action.Action` — 전략이 내놓는 주문 요청 (시장가/지정가/조건부/취소)
- :class:`~core.domain.status.Status` — 계좌 상태 (공유 증거금 풀 + 심볼별 포지션 + 미체결 장부)
- :class:`~core.domain.trade.Trade` — 체결 하나의 불변 기록
- :class:`~core.domain.report.Report` — 실행 하나의 결과 묶음
- :mod:`core.domain.order_book` — 미체결 주문 장부와 봉 내 체결 판정 규칙 (한 벌만 존재한다)
"""

from core.domain.action import Action, ActionType
from core.domain.candle import Candle
from core.domain.order_book import OpenOrder
from core.domain.report import Report
from core.domain.status import PositionState, Status
from core.domain.trade import Trade

__all__ = ["Action", "ActionType", "Candle", "OpenOrder", "PositionState", "Report", "Status",
           "Trade"]
