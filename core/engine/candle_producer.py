"""캔들 공급 계층.

엔진에 흘려보낼 **이벤트**(같은 시각에 마감한 심볼별 캔들 묶음)를 시간순으로 내준다.

모든 구현체가 **비동기 iterator**(``__aiter__``)로 이벤트를 내주고,
:meth:`~core.engine.engine.TradingEngine.run_async`가 그것을 소비한다. 백테스트/드라이런/
라이브가 같은 소비 경로를 지나가고, 백테스트 진입점만 그 코루틴을 ``asyncio.run``으로 감싼다:

- :class:`~core.backtest.in_memory_candle_producer.InMemoryCandleProducer` — 이미 메모리에
  로딩된 캔들을 병합해 내준다 (내부에서 아무것도 ``await``하지 않는 async generator).
- :class:`~core.backtest.binance_candle_producer.BinanceBacktestCandleProducer` — 위를
  상속해, 구간을 바이낸스에서 스스로 fetch한 뒤 병합한다.
- ``LiveCandleProducer`` (:mod:`core.live.candle_producer`) — 웹소켓에서 캔들이 마감할
  때마다 내준다.

의존 방향은 **엔진 → Producer 한 방향**이다. Producer는 실행기도, 레코더도, 액션도 모른다.
"""

from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, List, Tuple

from core.domain.candle import Candle

#: 이벤트 하나 — (병합 시각, 그 시각에 마감한 심볼별 캔들)
Event = Tuple[int, Dict[str, Candle]]


class CandleProducer(ABC):
    """이벤트를 시간순으로 내주는 소스.

    구현체는 **``__aiter__``(비동기 iterator)** 를 제공하고,
    :meth:`~core.engine.engine.TradingEngine.run_async`가 그것을 소비한다. 동기 소스라도
    아무것도 ``await``하지 않는 async generator로 감싸면 되고
    (:class:`~core.backtest.in_memory_candle_producer.InMemoryCandleProducer` 참고), 그러면
    백테스트·드라이런·라이브가 단 하나의 소비 경로를 공유한다.

    :param interval_ms: 캔들 간격(ms). 서브클래스는 반드시 ``super().__init__(interval_ms)``로
        값을 정해야 한다 — 샤드 메타와 Sharpe 리샘플링 주기가 여기서 나온다. 출처는 소스마다
        다르다: 백테스트/Replay는 캔들 데이터에서 측정하고, 라이브는 config에서 온다. ``0``은
        "미상/degenerate"(빈 구간 등)을 뜻한다.
    """

    def __init__(self, interval_ms: int):
        self.interval_ms = interval_ms

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[Event]:
        """이벤트를 시간순으로 내주는 비동기 iterator를 돌려준다.

        보통 서브클래스에서 ``async def __aiter__(self): ... yield ...`` 형태의 async
        generator로 구현한다.
        """
        raise NotImplementedError

    # TODO: warmup이 여기 들어가는게 맞는지 확인
    async def warmup_candles(self, windows: Dict[str, int]) -> Dict[str, List[Candle]]:
        """루프를 시작하기 전 지표에만 먹일 과거 캔들. 기본은 없음.

        이 캔들들은 ``process_event``를 타지 않는다 — 주문도 기록도 일어나면 안 되기 때문이다
        (:meth:`~core.engine.engine.TradingEngine.warmup` 참고).

        :param windows: 심볼별로 필요한 캔들 수. 보통
            :meth:`~core.engine.engine.TradingEngine.warmup_windows`가 만들어 준다.
        """
        return {}
