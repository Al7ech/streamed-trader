"""캔들 공급 계층.

엔진에 흘려보낼 **이벤트**(같은 시각에 마감한 심볼별 캔들 묶음)를 시간순으로 내준다.

모든 구현체가 **비동기 iterator**(``__aiter__``)로 이벤트를 내주고,
:meth:`~core.engine.engine.TradingEngine.run_async`가 그것을 소비한다. 백테스트/드라이런/
라이브가 같은 소비 경로를 지나가고, 이벤트 루프가 없는 호출자(백테스트)만 그 코루틴을
``asyncio.run``으로 감싼 :meth:`~core.engine.engine.TradingEngine.run`을 쓴다:

- :class:`~core.producer.in_memory.InMemoryCandleProducer` — 이미 메모리에
  로딩된 캔들을 병합해 내준다 (내부에서 아무것도 ``await``하지 않는 async generator).
- :class:`~core.producer.historical.BinanceHistoricalCandleProducer` — 위를
  상속해, 구간을 바이낸스에서 스스로 fetch한 뒤 병합한다. 백테스트의 캔들 소스이자 라이브의
  **지표 워밍업 소스**이며, 라이브 스트림에 구멍이 났을 때 엔진이 그 구간용으로 즉석에서
  만들어 소비하는 **백필 소스**이기도 하다
  (:meth:`~core.engine.engine.TradingEngine.warmup_from` 참고).
- ``LiveCandleProducer`` (:mod:`core.producer.live`) — 웹소켓에서 캔들이 마감할
  때마다 내준다. 연속성 판정(중복·구멍)은 하지 않는다 — 엔진 몫이다.

의존 방향은 **엔진 → Producer 한 방향**이다. Producer는 실행기도, 레코더도, 액션도 모른다.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, Optional

from core.candle.candle import Candle


@dataclass(frozen=True)
class Event:
    """이벤트 하나 — 같은 시각에 마감한 심볼별 캔들.

    :param time: 병합 시각 (캔들들의 ``end_time``).
    :param candles: 이 시각에 마감한 심볼 → 캔들. 라이브는 항상 원소 하나, 백테스트 병합
        이벤트는 하나 이상.

    이벤트는 "새 결정을 내는 봉"인지 "이미 지나간 봉(백필/프리피드)"인지를 스스로 알지
    못한다 — 그 판정은 :class:`~core.engine.engine.TradingEngine`이 심볼별 연속성 앵커로
    직접 한다 (:meth:`TradingEngine.process_event`의 ``decide`` 파라미터).
    """

    time: int
    candles: Dict[str, Candle] = field(default_factory=dict)


class CandleProducer(ABC):
    """이벤트를 시간순으로 내주는 소스.

    구현체는 **``__aiter__``(비동기 iterator)** 를 제공하고,
    :meth:`~core.engine.engine.TradingEngine.run_async`가 그것을 소비한다. 동기 소스라도
    아무것도 ``await``하지 않는 async generator로 감싸면 되고
    (:class:`~core.producer.in_memory.InMemoryCandleProducer` 참고), 그러면
    백테스트·드라이런·라이브가 단 하나의 소비 경로를 공유한다.

    :param interval_ms: 캔들 간격(ms). 서브클래스는 반드시 ``super().__init__(interval_ms)``로
        값을 정해야 한다 — 샤드 메타와 Sharpe 리샘플링 주기가 여기서 나온다. 출처는 소스마다
        다르다: 백테스트/Replay는 캔들 데이터에서 측정하고, 라이브는 config에서 온다. ``0``은
        "미상/degenerate"(빈 구간 등)을 뜻한다.
    """

    def __init__(self, interval_ms: int):
        self.interval_ms = interval_ms
        #: 치명적 사유로 스트림을 끊었다면 그 이유. 소비자(트레이더)가 근거로 읽는다.
        #: 엔진이 복구 불가한 구멍을 만나면 여기 채우고 :meth:`request_stop`을 부른다.
        self.fatal_reason: Optional[str] = None

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[Event]:
        """이벤트를 시간순으로 내주는 비동기 iterator를 돌려준다.

        보통 서브클래스에서 ``async def __aiter__(self): ... yield ...`` 형태의 async
        generator로 구현한다.
        """
        raise NotImplementedError

    def request_stop(self) -> None:
        """다음 수신 후 스트림을 정상 종료한다. 기본은 무시 (유한 소스는 스스로 끝난다).

        실시간 소스만 의미가 있다 — :class:`~core.producer.live.LiveCandleProducer`가
        ``_running`` 플래그를 내려 :meth:`__aiter__` 루프를 끝낸다.
        """
