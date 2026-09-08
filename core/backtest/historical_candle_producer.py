"""바이낸스 캔들을 스스로 fetch하는 과거 구간 공급자.

``start/end/symbols/interval``만 받아 **내부에서 fetcher로 심볼별 캔들을 불러오고** 하나의
이벤트 타임라인으로 병합해 내준다. 병합/이터레이션 로직은
:class:`~core.backtest.in_memory_candle_producer.InMemoryCandleProducer`를 그대로 상속한다 —
이 클래스가 더하는 건 "캔들이 어디서 오는가"뿐이다.

``LiveCandleProducer``가 웹소켓을 자기 안에 들고 있는 것과 대칭이다: 백테스트 캔들 소스도
producer 안으로 들어온다.

**쓰는 곳이 둘이다.** 백테스트의 캔들 소스(기본값 그대로: Vision 벌크 + 월청크 캐시)이고,
라이브/드라이런의 **지표 워밍업 소스**이기도 하다 — 후자는 REST fetcher에 ``use_cache=False``
로 쓴다 (아래 두 인자 설명 참고). 워밍업이 "직전 N개 봉을 받아 이벤트로 내주는 것" 이상이
아니라서 두 번째 구현이 필요 없었다.
"""

from datetime import datetime
from typing import Dict, List, Optional

from core.backtest.in_memory_candle_producer import InMemoryCandleProducer
from core.domain.candle import Candle
from core.fetcher.binance.rest_fetcher import BinanceCandleFetcher
from core.fetcher.binance.vision_fetcher import BinanceVisionFetcher
from core.utils import interval_to_minutes


class BinanceHistoricalCandleProducer(InMemoryCandleProducer):
    """``[start_time, end_time)`` 구간의 바이낸스 캔들을 fetch·병합해 내주는 공급자.

    :param start_time: 구간 시작 (포함). tz-aware 권장.
    :param end_time: 구간 끝 (배타).
    :param symbols: fetch할 심볼 목록 (보통 ``streamer.symbols``).
    :param interval: 바이낸스 interval 문자열 (``"1m"``, ``"1h"`` 등).
    :param fetcher: 캔들 fetcher. 기본값은 :class:`BinanceVisionFetcher` (data.binance.vision
        벌크 덤프 — 여러 달치도 빠르다). 월청크 pkl 캐시는 fetcher가 알아서 한다.
        **라이브 워밍업은 반드시 :class:`BinanceCandleFetcher`(REST)를 넘겨야 한다** — Vision
        덤프는 하루쯤 지연되므로 "직전 N개 봉"을 아예 돌려주지 못한다.
    :param use_cache: 월청크 pkl 캐시(``get_candles_with_cache``)를 쓸지. 캐시는 요청 구간이
        아니라 **달 경계로** fetch하므로(빠진 달을 통째로 받아 굳힌다), 1m 캔들 200개를
        원하는 라이브 워밍업이 이걸 켜면 월말 기동에서 그 달 전체(~4만 캔들)를 받고서야
        매매를 시작한다. 그래서 워밍업은 ``False``로 요청 구간만 REST로 받는다 — 라이브
        프리피드가 ``.pkl``을 건드리지 않는다는 규약도 그대로 유지된다.
    :param progress: 이벤트 진행바 표시 여부.
    """

    def __init__(self, start_time: datetime, end_time: datetime, symbols: List[str],
                 interval: str, *, fetcher: Optional[BinanceCandleFetcher] = None,
                 use_cache: bool = True, progress: bool = True):
        self.interval = interval
        fetcher = fetcher if fetcher is not None else BinanceVisionFetcher(compress=False)
        fetch = fetcher.get_candles_with_cache if use_cache else fetcher.get_candles
        candles_by_symbol: Dict[str, List[Candle]] = {
            symbol: fetch(symbol, start_time, end_time, interval)
            for symbol in symbols
        }
        super().__init__(candles_by_symbol, progress=progress)
        # 데이터에서 재는 대신 interval 문자열에서 확정한다 — 심볼이 캔들 0~1개만 돌려주는
        # 짧은 구간에서도 Sharpe 리샘플링/샤드 메타가 올바른 주기를 갖게.
        self.interval_ms = interval_to_minutes(interval) * 60_000
