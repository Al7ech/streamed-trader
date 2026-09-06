"""매매 순서 규약과 그 **포트(계약)**. 구현체는 여기 없다.

백테스트·드라이런·라이브는 하나의 도메인이다. 순서 규약은 이 저장소에 한 벌만 존재하고
(:meth:`core.engine.engine.TradingEngine.process_event`), 세 모드는 어떤 부품이 꽂히느냐로만
갈린다:

===========  ==============================  ==================  ================
             CandleProducer                  Executor            Recorder
===========  ==============================  ==================  ================
백테스트     backtest.BinanceBacktest        backtest.Simulated  backtest.Backtest
             / InMemoryCandleProducer        Executor            Recorder
드라이런     live.LiveCandleProducer         backtest.Simulated  live.LiveRecorder
                                             Executor
라이브       live.LiveCandleProducer         live.LiveExecutor   live.LiveRecorder
===========  ==============================  ==================  ================

이 패키지가 담는 것은 :class:`~core.engine.engine.TradingEngine`과 세 개의 ABC뿐이다
(:class:`~core.engine.candle_producer.CandleProducer`,
:class:`~core.engine.executor.Executor`, :class:`~core.engine.recorder.Recorder`).
구현체가 :mod:`core.backtest`와 :mod:`core.live`로 갈라져 있어서 계약을 중립 지점에 둔다 —
**엔진은 그 어느 쪽도 import하지 않는다.**
"""
