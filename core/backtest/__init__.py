"""백테스트 — 엔진에 꽂히는 **시뮬레이션 쪽 구현체**들과 진입점.

매매 순서 규약 자체는 :mod:`core.engine`에 있고, 여기 있는 것은 그 포트의 구현이다:

- :func:`~core.backtest.run.run_backtest` — 부품을 조립해 ``Report``를 돌려주는 진입점
- :class:`~core.backtest.simulated_executor.SimulatedExecutor` — 캔들로 체결을 판정하는
  가상 실행기. **드라이런도 이 클래스를 그대로 쓴다** (:mod:`core.live.trader` 참고)
- :class:`~core.backtest.recorder.BacktestRecorder` — 결과를 메모리에 모아 ``Report``로
- :class:`~core.backtest.in_memory_candle_producer.InMemoryCandleProducer` — 이미 로딩된
  캔들을 병합해 내주는 범용 공급자
- :class:`~core.backtest.binance_candle_producer.BinanceBacktestCandleProducer` — 위를
  상속해 구간을 바이낸스에서 스스로 fetch한다
"""

from core.backtest.run import DEFAULT_INIT_MARGIN, run_backtest

__all__ = ["DEFAULT_INIT_MARGIN", "run_backtest"]
