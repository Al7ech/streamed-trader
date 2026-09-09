"""바이낸스 과거 구간을 이벤트 타임라인으로 내주는 백테스트 캔들 공급자.

``start/end/symbols/interval``만 받아, 캔들은 :class:`~core.history.base.CandleHistory` 조회
포트로 받아오고 :class:`~core.producer.in_memory.InMemoryCandleProducer`로 병합해 내준다.
이 클래스 자체는 그 둘을 잇는 얇은 겹이다 — "거래소에서 과거 캔들을 가져온다"는 코드는 조회
포트에 한 벌만 있고, 워밍업·구멍 백필도 같은 것을 쓴다.

``LiveCandleProducer``가 웹소켓을 자기 안에 들고 있는 것과 대칭이다: 백테스트 캔들 소스도
producer 안으로 들어온다.

**조회는 생성이 아니라 첫 순회에서 일어난다.** 몇 달치 다운로드를 생성자가 하면 그 자리에서
동기 HTTP가 도는데, 라이브 경로가 같은 조회를 이벤트 루프 위에서 쓰기 때문이다. ``interval_ms``
는 캔들이 아니라 interval 문자열에서 나오므로, 순회 전에 읽어도 (레코더 샤드 메타 등) 문제없다.
"""

from datetime import datetime
from typing import AsyncIterator, List, Optional

from core.fetcher.base import BaseCandleFetcher
from core.fetcher.binance.vision_fetcher import BinanceVisionFetcher
from core.history.base import CandleHistory
from core.history.fetcher import FetcherCandleHistory
from core.producer.base import CandleProducer, Event
from core.producer.in_memory import InMemoryCandleProducer
from core.utils import interval_to_minutes


class BinanceHistoricalCandleProducer(CandleProducer):
    """``[start_time, end_time)`` 구간의 바이낸스 캔들을 조회·병합해 내주는 공급자.

    :param start_time: 구간 시작 (포함). tz-aware 권장.
    :param end_time: 구간 끝 (배타).
    :param symbols: 다룰 심볼 목록 (보통 ``streamer.symbols``).
    :param interval: 바이낸스 interval 문자열 (``"1m"``, ``"1h"`` 등).
    :param fetcher: 캔들 fetcher. 기본값은 :class:`BinanceVisionFetcher` (data.binance.vision
        벌크 덤프 — 여러 달치도 빠르다). 월청크 pkl 캐시는 fetcher가 알아서 한다.
    :param use_cache: 월청크 pkl 캐시(``get_candles_with_cache``)를 쓸지. 여러 달을 반복해서
        도는 백테스트는 켜는 편이 이득이라 기본이 ``True``다 (라이브 조회는 반대다 —
        :class:`~core.history.fetcher.FetcherCandleHistory` 참고).
    :param history: 조회 포트를 직접 넣고 싶을 때 (검사/다른 거래소). 주면 위 두 인자는 무시된다.
    :param progress: 이벤트 진행바 표시 여부.
    """

    def __init__(self, start_time: datetime, end_time: datetime, symbols: List[str],
                 interval: str, *, fetcher: Optional[BaseCandleFetcher] = None,
                 use_cache: bool = True, progress: bool = True,
                 history: Optional[CandleHistory] = None):
        # 데이터에서 재는 대신 interval 문자열에서 확정한다 — 심볼이 캔들 0~1개만 돌려주는
        # 짧은 구간에서도 Sharpe 리샘플링/샤드 메타가 올바른 주기를 갖고, 조회 전에 읽어도 된다.
        super().__init__(interval_to_minutes(interval) * 60_000)
        self.start_time = start_time
        self.end_time = end_time
        self.symbols = list(symbols)
        self.interval = interval
        self.progress = progress
        self.history = history if history is not None else FetcherCandleHistory(
            fetcher or BinanceVisionFetcher(compress=False), interval, use_cache=use_cache)

    async def __aiter__(self) -> AsyncIterator[Event]:
        """구간을 조회한 뒤(첫 순회 시점) 병합해 시간순으로 내준다."""
        candles_by_symbol = await self.history.fetch(self.symbols, self.start_time, self.end_time)
        merged = InMemoryCandleProducer(candles_by_symbol, progress=self.progress)
        async for event in merged:
            yield event
