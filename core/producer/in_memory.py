"""메모리에 이미 로딩된 캔들을 병합해 내주는 범용 캔들 공급자.

심볼별 캔들 리스트를 하나의 이벤트 타임라인으로 병합해(:func:`merge_by_end_time`),
아무것도 ``await``하지 않는 async generator로 내준다 — 그래서 이 경로도 드라이런/라이브와
같은 소비 경로 (:meth:`~core.engine.engine.TradingEngine.run_async`)를 지나간다.

캔들을 스스로 가져오지 않는다. 이미 만들어진 ``candles_by_symbol``을 받는다 — 드라이런 대조
검사(:mod:`core.checks.live_check`)나 비-바이낸스 소스(주식 등)처럼 캔들을 직접 넘기는 쪽이 쓴다.
바이낸스에서 구간을 fetch해 백테스트하려면
:class:`~core.producer.historical.BinanceHistoricalCandleProducer`.
"""

import heapq
import sys
from typing import AsyncIterator, Dict, Iterator, List, Optional, Tuple

from tqdm import tqdm

from core.candle.candle import Candle
from core.producer.base import CandleProducer, Event


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
        for event_time, batch in merge_by_end_time(self.candles_by_symbol):
            if self.start_time and event_time < self.start_time:
                continue
            if self.end_time and event_time > self.end_time:
                break
            yield Event(event_time, dict(batch))

    async def __aiter__(self) -> AsyncIterator[Event]:
        """메모리 캔들을 시간순으로 내준다. 아무것도 ``await``하지 않는 async generator라
        이벤트 루프 스케줄러는 개입하지 않는다 — ``async for``의 프로토콜 비용만 든다."""
        source = self._iter_events()
        if self.progress:
            source = tqdm(source, desc="Backtesting", unit="event", file=sys.stdout)
        for event in source:
            yield event


def merge_by_end_time(candles_by_symbol: Dict[str, List[Candle]]
                      ) -> Iterator[Tuple[int, List[Tuple[str, Candle]]]]:
    """N개 심볼의 캔들 리스트(각자 end_time 오름차순)를 하나의 시간순 이벤트 스트림으로 합친다.

    이벤트 하나는 같은 end_time에 마감한 모든 심볼의 캔들을 묶는다 — 크로스심볼 전략이 "이
    시각 마감한 모든 심볼"을 한 번에 보고 판단할 수 있어야 하기 때문이다 (한 심볼의 candle
    close가 다른 심볼에 대한 액션을 낼 수 있으려면, decide 시점에 그 다른 심볼의 이번 이벤트
    상태까지는 아니어도 최소한 지금까지의 상태를 볼 수 있어야 한다).

    심볼별 리스트 길이가 달라도(상장일이 다르거나 구간에 구멍이 있어도) 문제없이 동작한다 —
    각 심볼은 자기 리스트에 남아 있는 동안만 이벤트에 등장하고, 리스트가 끝나면 조용히
    빠진다. 배치 안 심볼들의 순서는 정해져 있지 않다 — 호출자가 자기 필요한 순서
    (예: streamer.symbols 선언 순서)로 다시 정렬해서 처리해야 한다.
    """
    symbols = list(candles_by_symbol.keys())
    iters = {s: iter(candles_by_symbol[s]) for s in symbols}
    heads: List[Tuple[int, str, Candle]] = []
    for s in symbols:
        c = next(iters[s], None)
        if c is not None:
            heapq.heappush(heads, (c.end_time, s, c))

    while heads:
        end_time = heads[0][0]
        batch: List[Tuple[str, Candle]] = []
        while heads and heads[0][0] == end_time:
            _, s, c = heapq.heappop(heads)
            batch.append((s, c))
            nxt = next(iters[s], None)
            if nxt is not None:
                heapq.heappush(heads, (nxt.end_time, s, nxt))
        yield end_time, batch
