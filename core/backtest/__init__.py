"""백테스트 — 엔진에 꽂히는 **시뮬레이션 쪽 구현체**들.

매매 순서 규약도, 부품을 엮어 돌리는 것도 :mod:`core.engine`에 있다
(:class:`~core.engine.engine.TradingEngine`). 여기 있는 것은 그 포트의 구현이다:

- :class:`~core.backtest.simulated_executor.SimulatedExecutor` — 캔들로 체결을 판정하는
  가상 실행기. **드라이런도 이 클래스를 그대로 쓴다** (:mod:`core.live.trader` 참고)
- :class:`~core.backtest.recorder.BacktestRecorder` — 결과를 메모리에 모아 ``Report``로
  만들고, ``metadata``를 받았으면 런 JSON/시계열 샤드까지 쓴다
- :class:`~core.backtest.in_memory_candle_producer.InMemoryCandleProducer` — 이미 로딩된
  캔들을 병합해 내주는 범용 공급자
- :class:`~core.backtest.binance_candle_producer.BinanceBacktestCandleProducer` — 위를
  상속해 구간을 바이낸스에서 스스로 fetch한다

백테스트 한 번은 이 부품들을 만들어 엔진에 넘기는 것이다 (``core/examples/backtest.py`` 참고)::

    executor = SimulatedExecutor(DEFAULT_INIT_MARGIN, fee_ratio)
    recorder = BacktestRecorder(streamer, executor.status, interval_ms=producer.interval_ms,
                                metadata=metadata, save_series=True)
    report   = TradingEngine(streamer, producer, executor, recorder).run()

``fee_ratio``는 실행기가 만드는 ``Status.fee_ratio``에 실려 회계(``apply_fill``)와 전략
사이징(``status.fee_ratio``)이 같은 값을 본다. ``slippage_ratio``는 순전히 시뮬레이션
모델링 값이라 ``SimulatedExecutor`` 인자로만 있고 ``Status``에 얹지 않는다.
"""

#: 백테스트의 기본 초기 증거금. 수익률의 기준선이 되는 값이라 한 곳에 둔다.
DEFAULT_INIT_MARGIN = 100_000.0

__all__ = ["DEFAULT_INIT_MARGIN"]
