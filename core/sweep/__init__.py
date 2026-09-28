"""파라미터 스윕 — 같은 캔들 위에서 전략 파라미터 격자를 병렬로 돌리는 조립 계층.

라이브 쪽 조립 계층(:mod:`core.trader`)과 나란히 있다. 둘 다 부품을 만들어 엔진에 넘길 뿐
이벤트 루프를 갖지 않는다 — 순서 규약은 :mod:`core.engine`에만 있다. 의존 방향은
``core.sweep → core.engine → 포트 ABC·어휘 → core.utils`` 한 방향이다.

    from core.sweep import ParameterSweep, SweepJob, load_columnar

    candles = load_columnar(FetcherCandleHistory(BinanceVisionFetcher(compress=False), "1m",
                                                 use_cache=True), ["ETHUSDT"], START, END)
    sweep = ParameterSweep(lambda syms, p: RyulStreamer(syms, **p), candles,
                           init_margin=100_000.0, fee_ratio=0.0004)
    rows = sweep.run([SweepJob("ETHUSDT", {"max_loss": ml}) for ml in (0.04, 0.08)])

자세한 설계는 :mod:`core.sweep.runner` 참고.
"""

from core.sweep.runner import ParameterSweep, SweepJob, load_columnar

__all__ = ["ParameterSweep", "SweepJob", "load_columnar"]
