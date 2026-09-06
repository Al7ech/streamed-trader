"""바이낸스 캔들을 스스로 fetch하는 백테스트용 캔들 공급자.

``start/end/symbols/interval``만 받아 **내부에서 fetcher로 심볼별 캔들을 불러오고** 하나의
이벤트 타임라인으로 병합해 내준다. 병합/이터레이션 로직은
:class:`~core.engine.in_memory_candle_producer.InMemoryCandleProducer`를 그대로 상속한다 —
이 클래스가 더하는 건 "캔들이 어디서 오는가"뿐이다.

``LiveCandleProducer``가 웹소켓을 자기 안에 들고 있는 것과 대칭이다: 백테스트 캔들 소스도
producer 안으로 들어온다.
"""

from datetime import datetime
from typing import Dict, List, Optional

from core.binance_candle_fetcher.fetcher import BinanceCandleFetcher
from core.binance_candle_fetcher.vision_fetcher import BinanceVisionFetcher
from core.engine.in_memory_candle_producer import InMemoryCandleProducer
from core.streamer.candle import Candle
from core.utils import interval_to_minutes


class BinanceBacktestCandleProducer(InMemoryCandleProducer):
    """``[start_time, end_time)`` 구간의 바이낸스 캔들을 fetch·병합해 내주는 공급자.

    :param start_time: 구간 시작 (포함). tz-aware 권장.
    :param end_time: 구간 끝 (배타).
    :param symbols: fetch할 심볼 목록 (보통 ``streamer.symbols``).
    :param interval: 바이낸스 interval 문자열 (``"1m"``, ``"1h"`` 등).
    :param fetcher: 캔들 fetcher. 기본값은 :class:`BinanceVisionFetcher` (data.binance.vision
        벌크 덤프 — 여러 달치도 빠르다). 월청크 pkl 캐시는 fetcher가 알아서 한다.
    :param progress: 이벤트 진행바 표시 여부.
    """

    def __init__(self, start_time: datetime, end_time: datetime, symbols: List[str],
                 interval: str, *, fetcher: Optional[BinanceCandleFetcher] = None,
                 progress: bool = True):
        self.interval = interval
        fetcher = fetcher if fetcher is not None else BinanceVisionFetcher(compress=False)
        candles_by_symbol: Dict[str, List[Candle]] = {
            symbol: fetcher.get_candles_with_cache(symbol, start_time, end_time, interval)
            for symbol in symbols
        }
        super().__init__(candles_by_symbol, progress=progress)
        # 데이터에서 재는 대신 interval 문자열에서 확정한다 — 심볼이 캔들 0~1개만 돌려주는
        # 짧은 구간에서도 Sharpe 리샘플링/샤드 메타가 올바른 주기를 갖게.
        self.interval_ms = interval_to_minutes(interval) * 60_000
