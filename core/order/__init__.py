"""주문 도메인 — 전략이 내놓는 주문 요청과 미체결 장부·봉 내 체결 규칙.

- :class:`~core.order.action.Action` — 주문 요청 (시장가/지정가/조건부/취소)
- :mod:`core.order.order_book` — 미체결 주문 장부와 봉 내 체결 판정 규칙 (한 벌만 존재한다)

``order_book``이 :class:`core.account.status.Status`를 런타임에 쓰고 ``Status``는
``order_book``을 ``TYPE_CHECKING``으로만 되받으므로, 두 모듈은 항상 서브모듈 경로로
참조한다 (패키지 속성 import 금지 — 부분 초기화 순환을 부른다).
"""

from core.order.action import Action, ActionType
from core.order.order_book import OpenOrder

__all__ = ["Action", "ActionType", "OpenOrder"]
