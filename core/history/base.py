"""과거 캔들 **조회** 포트.

:mod:`core.producer`와 나란히 있는 별개의 포트다. 그쪽은 "한 번 끝까지 흘려보내는 스트림"이라
구간이 **생성 시점에 굳는다**. 이쪽은 ``[start, end)``를 **호출 인자로** 받는 질의다 — 엔진이
필요로 하는 두 구간이 런타임에야 정해지기 때문이다:

- **지표 워밍업** — 얼마나 과거로 거슬러 갈지는 프로세스가 기동한 순간에 정해진다
  (:meth:`~core.engine.engine.TradingEngine.warmup`).
- **구멍 백필** — 어떤 캔들이 빠졌는지는 스트림이 앞으로 건너뛴 순간에 정해진다
  (``TradingEngine._backfill_gap``).

엔진은 이 포트의 구현을 **하나만** 들고 두 곳에서 같은 :meth:`CandleHistory.fetch`를 부른다.
워밍업과 백필이 서로 다른 경로로 캔들을 구하면 조용히 갈라질 수 있는데, 그 둘은 "지금이 아닌
과거 구간을 받아온다"는 한 가지 일이다.

백테스트의 캔들 소스(:class:`~core.producer.historical.BinanceHistoricalCandleProducer`)도
결국 이 포트 위에 있다 — 구간이 미리 정해져 있을 뿐 같은 조회이므로, 그쪽은 이 결과를 이벤트
타임라인으로 흘려보내는 얇은 겹이다. 그래서 "거래소에서 과거 캔들을 가져온다"는 코드는 저장소에
한 벌만 있다.

의존 방향은 **엔진 → CandleHistory 한 방향**이다. 조회는 실행기도, 레코더도, 전략도 모른다.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List

from core.candle.candle import Candle


class CandleHistory(ABC):
    """구간을 인자로 받아 심볼별 과거 캔들을 돌려주는 조회 포트."""

    @abstractmethod
    async def fetch(self, symbols: List[str], start: datetime,
                    end: datetime) -> Dict[str, List[Candle]]:
        """``[start, end)`` 구간의 심볼별 캔들 (각자 ``end_time`` 오름차순).

        돌려주는 모양이 :class:`~core.producer.in_memory.InMemoryCandleProducer`가 받는 모양과
        같다 — 이벤트로 흘려보내고 싶은 쪽은 그대로 넘기면 되고, 지표에 먹이거나 개수를
        검증하는 쪽(워밍업/백필)은 이벤트로 감쌌다 다시 벗길 필요가 없다.

        캔들이 하나도 없는 심볼은 빈 리스트로 온다 — 요청 구간에 데이터가 없는 것은 오류가
        아니다(상장 전 구간 등). "충분한가"의 판정은 부르는 쪽 몫이다.

        **비동기인 것은 계약이다.** 구현은 보통 동기 HTTP를 치는데, 그것을
        :func:`asyncio.to_thread`로 빼는 것은 구현의 몫이지 호출자가 매번 기억할 일이 아니다 —
        라이브에서 이벤트 루프를 수십 초 막으면 유저 데이터 스트림이 밀린다.
        """
        raise NotImplementedError
