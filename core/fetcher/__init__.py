"""캔들/시장데이터 페처와 월 청크 캐시.

:class:`~core.fetcher.base.BaseCandleFetcher`가 공용 뼈대다 — 서브클래스는
``get_candles(symbol, start, end, interval)`` 하나만 구현하고 월 청크 캐시
(``get_candles_with_cache``)를 물려받는다. 24/7이 아닌 시장은 non-24/7 훅을 덮어쓴다
(:mod:`core.fetcher.stock.massive_fetcher` 참고).

- :mod:`core.fetcher.binance` — 바이낸스 선물 (REST klines / data.binance.vision 대량
  다운로드 / 펀딩비 / OI·롱숏비율)
- :mod:`core.fetcher.stock` — 미국 주식 (Massive) + NYSE 세션 캘린더
"""

from core.fetcher.base import BaseCandleFetcher
from core.fetcher.pickle_storage import PickleStorage

__all__ = ["BaseCandleFetcher", "PickleStorage"]
