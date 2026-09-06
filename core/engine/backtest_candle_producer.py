"""백테스트용 캔들 공급자.

메모리에 로딩된 심볼별 캔들을 하나의 이벤트 타임라인으로 병합해, 아무것도 ``await``하지
않는 async generator로 내준다 — 그래서 백테스트도 드라이런/라이브와 같은 소비 경로
(:meth:`~core.engine.engine.TradingEngine.run_async`)를 지나간다.
"""

import sys
from typing import AsyncIterator, Dict, Iterator, List, Optional

from tqdm import tqdm

from core.engine.candle_merge import merge_candle_timeline
from core.engine.candle_producer import CandleProducer, Event
from core.streamer.candle import Candle


class BacktestCandleProducer(CandleProducer):
    """메모리에 로딩된 심볼별 캔들을 하나의 이벤트 타임라인으로 병합해 내준다.

    :param candles_by_symbol: 심볼별 캔들 리스트 (각자 end_time 오름차순). 스트리머가 다루는
        심볼과 정확히 일치할 필요는 없다 — 여기 없는 심볼은 이벤트에 아예 등장하지 않는다.
    :param symbols: 간격 검증에 쓸 심볼 목록 (보통 ``streamer.symbols``).
    :param materialize: True면 이벤트를 리스트로 미리 만들어 둔다. 벡터화 경로가 심볼별
        캔들 배열과 이벤트 인덱스 맵을 필요로 하므로 그때 쓴다. 기본 False (스트리밍).
    """

    def __init__(self, candles_by_symbol: Dict[str, List[Candle]], symbols: List[str],
                 start_time: Optional[int] = None, end_time: Optional[int] = None,
                 materialize: bool = False, progress: bool = True):
        self.candles_by_symbol = candles_by_symbol
        self.symbols = list(symbols)
        self.start_time = start_time
        self.end_time = end_time
        self.progress = progress
        # 데이터에서 간격을 재고(심볼 간 불일치는 여기서 fail-fast), 베이스로 올린다.
        super().__init__(self._measure_interval_ms())
        #: 지금까지 내준 이벤트 수. 런 메타데이터의 candle_count.
        self.event_count = 0
        self.events: Optional[List[Event]] = list(self._iter_events()) if materialize else None
        if self.events is not None:
            self.event_count = len(self.events)

    def _measure_interval_ms(self) -> int:
        """캔들 간격을 아무 심볼에서나 재고, 다른 심볼과 어긋나면 에러를 낸다.

        병합 타임라인은 "같은 시각 = 같은 봉 경계"를 전제하므로, 심볼마다 간격이 다르면 병합
        자체가 의미를 잃는다. 혼합 간격 멀티심볼 백테스트는 지원 범위 밖이다.
        """
        ref_ms: Optional[int] = None
        ref_symbol: Optional[str] = None
        for symbol in self.symbols:
            candles = self.candles_by_symbol.get(symbol) or []
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
        if self.events is not None:
            source = tqdm(self.events, desc="Backtesting", unit="event",
                          file=sys.stdout) if self.progress else self.events
            for event in source:
                yield event
            return

        source = self._iter_events()
        if self.progress:
            source = tqdm(source, desc="Backtesting", unit="event", file=sys.stdout)
        count = 0
        for event in source:
            count += 1
            yield event
        self.event_count = count
