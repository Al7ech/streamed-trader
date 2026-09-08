"""캔들 공급 계층.

엔진에 흘려보낼 **이벤트**(같은 시각에 마감한 심볼별 캔들 묶음)를 시간순으로 내준다.

모든 구현체가 **비동기 iterator**(``__aiter__``)로 이벤트를 내주고,
:meth:`~core.engine.engine.TradingEngine.run_async`가 그것을 소비한다. 백테스트/드라이런/
라이브가 같은 소비 경로를 지나가고, 이벤트 루프가 없는 호출자(백테스트)만 그 코루틴을
``asyncio.run``으로 감싼 :meth:`~core.engine.engine.TradingEngine.run`을 쓴다:

- :class:`~core.backtest.in_memory_candle_producer.InMemoryCandleProducer` — 이미 메모리에
  로딩된 캔들을 병합해 내준다 (내부에서 아무것도 ``await``하지 않는 async generator).
- :class:`~core.backtest.historical_candle_producer.BinanceHistoricalCandleProducer` — 위를
  상속해, 구간을 바이낸스에서 스스로 fetch한 뒤 병합한다. 백테스트의 캔들 소스이자 라이브의
  **지표 워밍업 소스**다 (:meth:`~core.engine.engine.TradingEngine.warmup_from` 참고).
- ``LiveCandleProducer`` (:mod:`core.live.candle_producer`) — 웹소켓에서 캔들이 마감할
  때마다 내준다.

의존 방향은 **엔진 → Producer 한 방향**이다. Producer는 실행기도, 레코더도, 액션도 모른다.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict

from core.domain.candle import Candle


@dataclass(frozen=True)
class Event:
    """이벤트 하나 — 같은 시각에 마감한 심볼별 캔들.

    :param time: 병합 시각 (캔들들의 ``end_time``).
    :param candles: 이 시각에 마감한 심볼 → 캔들. 라이브는 항상 원소 하나, 백테스트 병합
        이벤트는 하나 이상.
    :param decide: ``False``면 엔진이 이 이벤트로 ``streamer.decide_action``을 부르지 않고
        액션도 제출하지 않는다 — **이미 지나간 봉**이라 새 주문이 나오면 안 되기 때문이다.
        지표 갱신과 실행기의 이벤트 경계 훅(미체결 매칭·시가평가), 기록은 그대로 일어난다:
        그 봉이 도는 동안 이미 장부에 얹혀 있던 주문의 체결은 지나간 봉이 만든 새 결정이
        아니라 그 사이 거래소에서 실제로 벌어진 일이기 때문이다. 지금 이걸 쓰는 곳은
        ``LiveCandleProducer``의 구멍 백필뿐이다.
    """

    time: int
    candles: Dict[str, Candle] = field(default_factory=dict)
    decide: bool = True


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

    def resume_after(self, last_starts: Dict[str, int]) -> None:
        """"이 심볼들은 여기까지 이미 소비됐다"를 알린다. 기본은 무시.

        :meth:`~core.engine.engine.TradingEngine.warmup_from`이 워밍업을 마친 뒤 부른다 —
        워밍업 소스와 본 소스는 서로 다른 객체라(전자는 과거 구간 fetch, 후자는 웹소켓),
        이어붙일 지점을 누군가는 넘겨줘야 한다. 이 값 하나가 양방향을 막는다: 워밍업이
        먹인 캔들이 다시 오면 **중복**(지표 이중 투입 + 결정 재실행)이고, 워밍업과 첫
        실시간 캔들 사이가 벌어졌으면 **구멍**(지표가 영구히 갈라진다)이다.

        :param last_starts: 심볼 → 마지막으로 소비된 캔들의 ``start_time``. ``end_time``이
            아니다 — 연속성 판정이 ``start_time + interval_ms`` 기준이다. 캔들을 실제로
            소비한 심볼만 담는다 (빠진 심볼은 "기준 없음"이지 0이 아니다).
        """
