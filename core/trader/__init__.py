"""라이브/드라이런 오케스트레이터.

:class:`~core.trader.trader.BinanceTrader` — 네 엔진 부품을 조립하고 소켓을 열고 수명주기를
관리한다. 매매 로직 자체는 :mod:`core.engine`에 있다. ``core/examples/trader.py`` 참고.

드라이런은 :class:`~core.executor.simulated.SimulatedExecutor` — 백테스트와 **같은 클래스** —
를 꽂는다.
"""

from core.trader.trader import BinanceTrader

__all__ = ["BinanceTrader"]
