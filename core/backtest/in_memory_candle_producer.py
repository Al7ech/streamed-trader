"""메모리에 이미 로딩된 캔들을 병합해 내주는 범용 캔들 공급자.

심볼별 캔들 리스트를 하나의 이벤트 타임라인으로 병합해, 아무것도 ``await``하지 않는 async
generator로 내준다 — 그래서 이 경로도 드라이런/라이브와 같은 소비 경로
(:meth:`~core.engine.engine.TradingEngine.run_async`)를 지나간다.

캔들을 스스로 가져오지 않는다. 이미 만들어진 ``candles_by_symbol``을 받는다 — 드라이런 대조
검사(:mod:`core.checks.live_check`)나 비-바이낸스 소스(주식 등)처럼 캔들을 직접 넘기는 쪽이 쓴다.
바이낸스에서 구간을 fetch해 백테스트하려면 :class:`~core.backtest.binance_candle_producer.BinanceBacktestCandleProducer`.
"""

import sys
from typing import AsyncIterator, Dict, Iterator, List, Optional

from tqdm import tqdm

from core.backtest.candle_merge import merge_candle_timeline
from core.domain.candle import Candle
from core.engine.candle_producer import CandleProducer, Event


class InMemoryCandleProducer(CandleProducer):
    """메모리에 로딩된 심볼별 캔들을 하나의 이벤트 타임라인으로 병합해 내준다.

    :param candles_by_symbol: 심볼별 캔들 리스트 (각자 end_time 오름차순). 스트리머가 다루는
        심볼과 정확히 일치할 필요는 없다 — 여기 없는 심볼은 이벤트에 아예 등장하지 않는다.
        다룰 심볼 목록은 이 dict의 키에서 나온다 (엔진은 ``streamer.symbols``만 보므로 별도
        인자가 필요 없다).
    :param start_time: 주면 이 시각(포함) 이전 이벤트를 건너뛴다. ms epoch.
    :param end_time: 주면 이 시각(포함) 이후 이벤트에서 멈춘다. ms epoch.
    """

    def __init__(self, candles_by_symbol: Dict[str, List[Candle]],
                 start_time: Optional[int] = None, end_time: Optional[int] = None,
                 progress: bool = True):
        self.candles_by_symbol = candles_by_symbol
        self.symbols = list(candles_by_symbol)
        self.start_time = start_time
        self.end_time = end_time
        self.progress = progress
        # 데이터에서 간격을 재고(심볼 간 불일치는 여기서 fail-fast), 베이스로 올린다.
        super().__init__(self._measure_interval_ms())
        #: 지금까지 내준 이벤트 수. 런 메타데이터의 candle_count.
        self.event_count = 0

    def _measure_interval_ms(self) -> int:
        """캔들 간격을 아무 심볼에서나 재고, 다른 심볼과 어긋나면 에러를 낸다.

        병합 타임라인은 "같은 시각 = 같은 봉 경계"를 전제하므로, 심볼마다 간격이 다르면 병합
        자체가 의미를 잃는다. 혼합 간격 멀티심볼 백테스트는 지원 범위 밖이다.
        """
        ref_ms: Optional[int] = None
        ref_symbol: Optional[str] = None
        for symbol, candles in self.candles_by_symbol.items():
            if len(candles) < 2:
                continue
            ms = candles[1].start_time - candles[0].start_time
            if ref_ms is None:
                ref_ms, ref_symbol = ms, symbol
            elif ms != ref_ms:
                raise ValueError(
                    f"심볼별 캔들 간격이 다르다: {ref_symbol}={ref_ms}ms vs {symbol}={ms}ms — "
                    f"멀티심볼 백테스트는 모든 심볼이 같은 간격이어야 한다.")
        return ref_ms or 0

    def _iter_events(self) -> Iterator[Event]:
        for event_time, batch in merge_candle_timeline(self.candles_by_symbol):
            if self.start_time and event_time < self.start_time:
                continue
            if self.end_time and event_time > self.end_time:
                break
            yield event_time, dict(batch)

    async def __aiter__(self) -> AsyncIterator[Event]:
        """메모리 캔들을 시간순으로 내준다. 아무것도 ``await``하지 않는 async generator라
        이벤트 루프 스케줄러는 개입하지 않는다 — ``async for``의 프로토콜 비용만 든다."""
        source = self._iter_events()
        if self.progress:
            source = tqdm(source, desc="Backtesting", unit="event", file=sys.stdout)
        count = 0
        for event in source:
            count += 1
            yield event
        self.event_count = count
