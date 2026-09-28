"""주문 도메인 — 전략이 내놓는 주문 요청과 미체결 장부·봉 내 체결 규칙.

- :class:`~core.order.action.Action` — 주문 요청 (시장가/지정가/조건부/취소)
- :mod:`core.order.order_book` — 미체결 주문 장부와 봉 내 체결 판정 규칙 (한 벌만 존재한다)
- :mod:`core.order.symbol_rules` — 심볼별 거래 규칙 (수량/가격 단위, 최소 수량·명목가치)

``account``가 ``order``를 import한다 (``Status``가 ``symbol_rules``를 든다). 그래서
``order_book``은 ``Status``를 ``TYPE_CHECKING``으로만 참조한다 — 런타임 import는 부분 초기화
순환을 부른다.
"""

from core.order.action import Action, ActionType
from core.order.order_book import OpenOrder

__all__ = ["Action", "ActionType", "OpenOrder"]
