"""과거 캔들 조회 포트 — ABC는 여기, 구현은 서브모듈에.

- :class:`~core.history.base.CandleHistory` — 포트 ABC
- :class:`~core.history.fetcher.FetcherCandleHistory` — 아무
  :class:`~core.fetcher.base.BaseCandleFetcher`나 이 포트에 맞춰 주는 어댑터 (거래소별 코드는
  fetcher 쪽에 있고 여기엔 없다)

구현 서브모듈은 여기서 eager import하지 않는다 — 필요한 곳에서 서브모듈로 import한다.
"""

from core.history.base import CandleHistory

__all__ = ["CandleHistory"]
