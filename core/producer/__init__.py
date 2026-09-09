"""캔들/이벤트 공급 포트 — ABC는 여기, 구현은 서브모듈에.

- :class:`~core.producer.base.CandleProducer` (+ ``Event``) — 포트 ABC
- :class:`~core.producer.in_memory.InMemoryCandleProducer` — 메모리 캔들 병합/재생
- :class:`~core.producer.historical.BinanceHistoricalCandleProducer` — 구간을
  :class:`~core.history.base.CandleHistory`로 조회해 병합 (백테스트 소스)
- :class:`~core.producer.live.LiveCandleProducer` — kline 웹소켓 (파싱만; 연속성/백필은 엔진 몫)

구현 서브모듈은 여기서 eager import하지 않는다 — ``live``는 python-binance/asyncio를 끌어오므로
순수 백테스트 경로가 그 비용을 치르지 않도록 필요한 곳에서 서브모듈로 import한다.
"""

from core.producer.base import CandleProducer, Event

__all__ = ["CandleProducer", "Event"]
