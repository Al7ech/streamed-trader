"""매매 순서 규약. 이 패키지가 담는 것은 :class:`~core.engine.engine.TradingEngine` 하나뿐이다.

백테스트·드라이런·라이브는 하나의 도메인이다. 순서 규약은 이 저장소에 한 벌만 존재하고
(:meth:`core.engine.engine.TradingEngine.process_event`), 세 모드는 어떤 부품이 꽂히느냐로만
갈린다:

===========  ==============================  ==================  ================
             CandleProducer                  Executor            Recorder
===========  ==============================  ==================  ================
백테스트     producer.BinanceHistorical      executor.Simulated  recorder.Backtest
             / InMemoryCandleProducer        Executor            Recorder
드라이런     producer.LiveCandleProducer     executor.Simulated  recorder.LiveRecorder
                                             Executor
라이브       producer.LiveCandleProducer     executor.LiveExec   recorder.LiveRecorder
===========  ==============================  ==================  ================

세 포트의 **ABC와 구현이 각 포트 패키지에 함께** 산다 — :mod:`core.producer`,
:mod:`core.executor`, :mod:`core.recorder`. 엔진은 각 포트의 ``base`` 서브모듈(ABC)만
import하고, 어떤 구현체도 import하지 않는다.

포트가 하나 더 있다: :mod:`core.history` (:class:`~core.history.base.CandleHistory`). 위 셋은
런 하나에 하나씩 꽂히는 부품이지만 이건 **조회**다 — 구간을 인자로 받아 과거 캔들을 돌려준다.
지표 워밍업(:meth:`core.engine.engine.TradingEngine.warmup`)과 라이브 스트림의 구멍 백필이
필요로 하는 구간은 런타임에야 정해져서 ``CandleProducer``로는 표현되지 않기 때문이다.
백테스트는 이것 없이 돌아간다 (첫 window개 이벤트가 워밍업이고, ragged 시계열의 빈 구간은
구멍이 아니다).

**부품을 엮어 돌리는 것까지가 엔진의 일이다.** 호출자는 네 부품을 만들어 넘기기만 한다::

    report = TradingEngine(streamer, producer, executor, recorder).run()

체결 싱크 연결(``executor.on_trade``), 레코더 기본값, 이벤트 루프,
마무리(``recorder.close()``)는 전부 엔진 안에 있다 — 예전에는 이 배선이 진입점마다 손으로
반복됐다. 만드는 것만 호출자 몫인 이유는 그 구현체들이 포트 패키지에 있기 때문이고, 그래서
위 의존 방향(엔진 → 포트 ABC 한 방향)이 그대로 유지된다.
"""
