"""미국 주식 데이터 소스.

:class:`~core.fetcher.stock.massive_fetcher.MassiveStockFetcher`는 non-24/7 훅을 덮어써
:mod:`core.fetcher.stock.nyse_session`이 만든 NYSE 세션 격자 위로 forward-fill한다 —
그래서 N개 캔들의 window가 wall-clock N분이 아니라 **거래 N분**을 뜻하게 된다.
"""

from core.fetcher.stock.massive_fetcher import MassiveStockFetcher

__all__ = ["MassiveStockFetcher"]
