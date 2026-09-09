""":class:`~core.fetcher.base.BaseCandleFetcher`를 엔진의 조회 포트에 맞춰 주는 어댑터.

거래소별 코드는 여기 없다 — fetcher가 그걸 갖고 있고, 이 클래스는 **묶기**만 한다:
interval과 캐시 정책을 미리 고정하고(그 둘은 조립 계층의 결정이지 엔진이 알 일이 아니다),
심볼을 한 번에 받고, 동기 fetcher를 :func:`asyncio.to_thread`로 감춘다.
"""

import asyncio
from datetime import datetime
from typing import Dict, List

from core.candle.candle import Candle
from core.fetcher.base import BaseCandleFetcher
from core.history.base import CandleHistory


class FetcherCandleHistory(CandleHistory):
    """아무 :class:`~core.fetcher.base.BaseCandleFetcher`나 조회 포트로 쓰게 해 준다.

    :param fetcher: 캔들 fetcher. 바이낸스든 주식이든 상관없다 — 이 클래스는
        ``get_candles``/``get_candles_with_cache`` 만 부른다. 라이브는 REST
        (:class:`~core.fetcher.binance.rest_fetcher.BinanceCandleFetcher`)를 넘겨야 한다:
        Vision 벌크 덤프는 하루쯤 지연돼 "직전 N개 봉"을 아예 돌려주지 못한다.
    :param interval: 봉 간격 문자열 (``"1m"``, ``"1h"`` 등). 한 조회 객체는 한 간격만 본다 —
        엔진이 다루는 것도 한 간격이다.
    :param use_cache: 월청크 pkl 캐시를 쓸지. 기본이 ``False``인 것은 라이브 쪽 요구다 —
        캐시는 요청 구간이 아니라 **달 경계로** fetch하므로, 1m 캔들 200개를 원하는 워밍업이
        월말에 기동하면 그 달 전체(~4만 캔들)를 받고서야 매매를 시작한다. 여러 달을 한 번에
        받는 백테스트는 반대로 켜는 편이 이득이라 그쪽이 ``True``를 넘긴다.
    """

    def __init__(self, fetcher: BaseCandleFetcher, interval: str, *, use_cache: bool = False):
        self.fetcher = fetcher
        self.interval = interval
        self.use_cache = use_cache

    async def fetch(self, symbols: List[str], start: datetime,
                    end: datetime) -> Dict[str, List[Candle]]:
        return await asyncio.to_thread(self._fetch, symbols, start, end)

    def _fetch(self, symbols: List[str], start: datetime,
               end: datetime) -> Dict[str, List[Candle]]:
        """심볼별로 순차 조회한다. **스레드 안에서** 불린다 (fetcher가 동기다)."""
        get = (self.fetcher.get_candles_with_cache if self.use_cache
               else self.fetcher.get_candles)
        return {symbol: get(symbol, start, end, self.interval) for symbol in symbols}
