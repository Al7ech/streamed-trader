"""여러 심볼의 캔들 리스트를 하나의 시간순 이벤트 스트림으로 합친다.

멀티심볼 실행의 "이벤트" 개념이 여기 있다 — 같은 시각에 마감한 캔들들의 묶음. 백테스트
엔진(:mod:`core.engine.backtest`)과 메모리 캔들 공급자
(:class:`~core.producer.in_memory.InMemoryCandleProducer`)가 함께 쓰므로, 어느 한쪽이 아니라
캔들 어휘 계층에 둔다.
"""

import heapq
from typing import Dict, Iterator, List, Tuple

from core.candle.candle import Candle


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

    **각 심볼의 캔들은 자기 리스트 순서대로 정확히 한 번씩 등장한다.** 벡터화 백테스트가
    심볼별 커서를 "그 심볼이 배치에 나올 때마다 하나씩 전진"으로 둘 수 있는 근거다.
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
