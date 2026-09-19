"""매매 순서 규약. 이 패키지가 담는 것은 엔진뿐이다 — 지금은 두 벌.

- :class:`~core.engine.trading.TradingEngine` — 드라이런/라이브. 캔들 공급자를 ``async for``로
  소비하며, 연속성 앵커·구멍 백필·결정 기한·지표 워밍업을 갖는다.
- :class:`~core.engine.backtest.BacktestEngine` — 백테스트. 캔들을 메모리에 다 들고 시작하고,
  ``VectorizableNumericIndicator``를 상속한 지표를 먼저 통째로 계산한 뒤 동기 루프를 돈다.

===========  ==============================  ==================  ================
             캔들                            Executor            Recorder
===========  ==============================  ==================  ================
백테스트     Dict[str, List[Candle]]         executor.Simulated  recorder.Backtest
             (BacktestEngine이 직접 받는다)   Executor            Recorder
드라이런     producer.LiveCandleProducer     executor.Simulated  recorder.LiveRecorder
                                             Executor
라이브       producer.LiveCandleProducer     executor.LiveExec   recorder.LiveRecorder
===========  ==============================  ==================  ================

**왜 둘인가.** ``TradingEngine``이 갖는 것들은 전부 "스트림은 끊기고 시계는 흐른다"는 전제
위에 있는데, 메모리에 다 들고 있는 과거 구간에는 그 전제가 없다. 그 전제를 걷어내면 백테스트
에서만 가능한 것이 열린다 — 전 구간을 미리 알고 있으니 지표를 캔들마다 갱신하지 않고 한 번에
계산해 둘 수 있다. 그 하나 때문에 루프가 갈라졌다.

**둘이 갈라지지 않는다는 보장은 문서가 아니라 검사다.** ``core/checks/live_check.py`` 3절이
같은 캔들에서 두 엔진이 같은 체결을 내는지 다섯 전략(시장가/상태 있는 전략/조건부 주문/
지정가+취소/reduce_only+슬리피지)으로 확인한다. 선계산이 루프 갱신과 **비트 단위로** 같다는
것은 ``core/checks/backtest_check.py``가 지표마다 따로 확인한다.

포트의 **ABC와 구현이 각 포트 패키지에 함께** 산다 — :mod:`core.producer`, :mod:`core.executor`,
:mod:`core.recorder`. 엔진은 각 포트의 ``base`` 서브모듈(ABC)과 어휘 계층
(:mod:`core.candle`, :mod:`core.order`, :mod:`core.account`)만 import하고, 어떤 구현체도
import하지 않는다.

포트가 하나 더 있다: :mod:`core.history` (:class:`~core.history.base.CandleHistory`). 위 셋은
런 하나에 하나씩 꽂히는 부품이지만 이건 **조회**다 — 구간을 인자로 받아 과거 캔들을 돌려준다.
지표 워밍업(:meth:`core.engine.trading.TradingEngine.warmup`)과 라이브 스트림의 구멍 백필이
필요로 하는 구간은 런타임에야 정해져서 ``CandleProducer``로는 표현되지 않기 때문이다.
백테스트 엔진은 이것을 들지 않는다 — 진입점이 같은 조회로 캔들을 받아 통째로 넘기고, 첫
window개 이벤트가 워밍업이며, ragged 시계열의 빈 구간은 구멍이 아니라 데이터 그대로다.

**부품을 엮어 돌리는 것까지가 엔진의 일이다.** 호출자는 부품을 만들어 넘기기만 한다::

    report = TradingEngine(streamer, producer, executor, recorder).run()      # 드라이런/라이브
    report = BacktestEngine(streamer, candles_by_symbol, executor, recorder).run()

체결 싱크 연결(``executor.on_trade``), 레코더 기본값, 이벤트 루프,
마무리(``recorder.close()``)는 전부 엔진 안에 있다 — 예전에는 이 배선이 진입점마다 손으로
반복됐다. 만드는 것만 호출자 몫인 이유는 그 구현체들이 포트 패키지에 있기 때문이고, 그래서
위 의존 방향(엔진 → 포트 ABC 한 방향)이 그대로 유지된다.
"""
