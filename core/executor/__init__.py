"""주문 실행 + 계좌 상태 소유 포트 — ABC는 여기, 구현은 서브모듈에.

- :class:`~core.executor.base.Executor` — 포트 ABC (``begin_event`` 훅 + ``submit``)
- :class:`~core.executor.simulated.SimulatedExecutor` — 캔들로 체결 판정 (백테스트 + 드라이런),
  ``DEFAULT_INIT_MARGIN``도 여기 있다
- :class:`~core.executor.live.LiveExecutor` — 실제 거래소 주문 + 거래소가 정답인 계좌 상태
- :class:`~core.executor.binance_order_client.BinanceOrderClient` — 저수준 REST 주문 클라이언트
  (``Executor`` 포트를 구현하지 **않는다**)

``live``/``binance_order_client``는 python-binance를 끌어오므로 여기서 eager import하지 않는다.
"""

from core.executor.base import Executor

__all__ = ["Executor"]
