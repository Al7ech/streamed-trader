""":class:`~core.fetcher.base.BaseCandleFetcher`를 엔진의 조회 포트에 맞춰 주는 어댑터.

거래소별 코드는 여기 없다 — fetcher가 그걸 갖고 있고, 이 클래스는 **묶기**만 한다:
interval과 캐시 정책을 미리 고정하고(그 둘은 조립 계층의 결정이지 엔진이 알 일이 아니다),
심볼을 한 번에 받고, 동기 fetcher를 :func:`asyncio.to_thread`로 감춘다.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List

from core.candle.candle import Candle
from core.fetcher.base import BaseCandleFetcher
from core.history.base import CandleHistory

logger = logging.getLogger(__name__)


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
    :param retries: 한 심볼의 조회가 예외로 실패했을 때 다시 해 볼 횟수. 엔진은 조회 실패를
        치명으로 다루므로(백필이면 스트림을 끊고 재기동한다), REST 5xx·타임아웃 한 번에 프로세스를
        내리지 않도록 여기서 먼저 흡수한다. 다 실패하면 마지막 예외를 그대로 올린다 — 그
        뒤의 판단은 엔진 몫이다. 결과가 **짧은** 것은 재시도하지 않는다: 그건 데이터가 그렇다는
        뜻이고, 충분한지는 부르는 쪽이 검증한다.
    :param retry_delay_s: 첫 재시도 전 대기(초). 매번 두 배로 는다.
    """

    def __init__(self, fetcher: BaseCandleFetcher, interval: str, *, use_cache: bool = False,
                 retries: int = 2, retry_delay_s: float = 1.0):
        self.fetcher = fetcher
        self.interval = interval
        self.use_cache = use_cache
        self.retries = retries
        self.retry_delay_s = retry_delay_s

    async def fetch(self, symbols: List[str], start: datetime,
                    end: datetime) -> Dict[str, List[Candle]]:
        """심볼별로 순차 조회한다 — 재시도도 심볼 단위라, 한 심볼의 실패가 이미 받은 심볼을
        다시 받게 하지 않는다."""
        return {symbol: await self._fetch_one(symbol, start, end) for symbol in symbols}

    async def _fetch_one(self, symbol: str, start: datetime, end: datetime) -> List[Candle]:
        get = (self.fetcher.get_candles_with_cache if self.use_cache
               else self.fetcher.get_candles)
        for attempt in range(self.retries + 1):
            try:
                return await asyncio.to_thread(get, symbol, start, end, self.interval)
            except Exception as e:
                if attempt == self.retries:
                    raise
                delay = self.retry_delay_s * 2 ** attempt
                logger.warning("캔들 조회 실패 (symbol=%s, %s ~ %s) — %.1f초 뒤 재시도 %d/%d: %s",
                               symbol, start, end, delay, attempt + 1, self.retries, e)
                await asyncio.sleep(delay)
