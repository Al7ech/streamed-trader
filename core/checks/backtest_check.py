"""Backtest engine check: 벡터화 == 루프.

:class:`~core.engine.backtest.BacktestEngine`이 지표를 미리 계산해 두고 도는 것이 캔들마다
``update()``를 부르는 것과 **완전히 같은 결과**를 낸다는 것을 확인한다. 같아야 하는 이유는
성능이 아니라 신뢰다: 백테스트가 라이브와 같은 값을 본다는 보장이 여기서 나오고, 그 보장이
있어야 ``core/checks/live_check.py`` 3절(드라이런 == 백테스트)이 허용오차 없이 성립한다.

1. **지표 단위 정확성** — ``precompute_series``를 정의한 모든 지표에 대해, 미리 계산한 수열이
   ``update()`` 루프의 ``get_latest()`` 수열과 **비트 단위로** 같은지. 이어서 그 값을
   ``precomputed_sink()``로 재생했을 때 ``read(idx)``가 전 인덱스에서 루프와 같은지.
   가장 싸고 가장 정확히 원인을 짚는 검사라 맨 앞에 둔다 — 여기가 깨지면 아래는 볼 필요가 없다.
2. **전략 단위 대조** — 실제 캔들 위에서 ``vectorize=True``와 ``False``의 ``Report``가 같은지.
   1절이 지표 하나를 보는 반면 여기는 지표가 전략·실행기·레코더를 거쳐 나온 결과를 본다.
3. **멀티심볼 ragged** — 상장일이 다르고 구멍이 있는 두 심볼. 심볼별 재생 커서가 병합
   타임라인과 어긋나지 않는지 보는 유일한 검사다.
4. **퇴화 입력** — 빈 캔들, 캔들 1개, 지표 없는 심볼 등에서 죽지 않는지.

    uv run python core/checks/backtest_check.py            # 전부
    uv run python core/checks/backtest_check.py --offline  # 캔들 캐시가 필요 없는 1, 4 만
"""
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

from core.candle.candle import Candle
from core.checks.compare import compare_reports
from core.engine.backtest import BacktestEngine
from core.executor.simulated import SimulatedExecutor
from core.fetcher.binance.vision_fetcher import BinanceVisionFetcher
from core.logging_config import setup_logging
from core.recorder.backtest import BacktestRecorder
from core.streamer.indicator.atr import ATRIndicator
from core.streamer.indicator.donchian_channel import MaxDonchianIndicator, MinDonchianIndicator
from core.streamer.indicator.moving_average import MovingAverage
from core.streamer.indicator.rolling_std import RollingStd
from core.streamer.indicator.volume_stats import VolumeMovingAverage, VolumeRollingStd
from core.streamer.strategies.keltner_stop_streamer import KeltnerStopStreamer
from core.streamer.strategies.keltner_streamer import KeltnerStreamer
from core.streamer.strategies.mean_reversion_zscore import MeanReversionZScoreStreamer
from core.streamer.strategies.momentum_time_exit import MomentumTimeExitStreamer
from core.streamer.strategies.supertrend_streamer import SupertrendStreamer

MIN = 60_000
SYM, SYM2 = "ETHUSDT", "BTCUSDT"
INIT_MARGIN = 100_000.0

_failures = []


def check(label, cond, detail=""):
    if not cond:
        _failures.append(label)
    print(f"[{label}] {'OK' if cond else 'FAIL'}{(' — ' + detail) if detail else ''}")


def synthetic_candles(n: int, seed: int = 0) -> List[Candle]:
    """난수 워크 캔들. 실제 시세처럼 종가가 움직이고 고가/저가가 그것을 감싼다."""
    if n == 0:
        return []
    rng = np.random.default_rng(seed)
    close = 2000.0 * np.exp(np.cumsum(rng.normal(0, 3e-4, n)))
    high = close * (1 + np.abs(rng.normal(0, 5e-4, n)))
    low = close * (1 - np.abs(rng.normal(0, 5e-4, n)))
    open_ = np.concatenate(([close[0]], close[:-1]))
    volume = np.abs(rng.normal(1000, 300, n)) + 1.0
    return [Candle(open_[i], high[i], low[i], close[i], volume[i], i * MIN, (i + 1) * MIN)
            for i in range(n)]


# ==================================================== 1. 지표 단위 정확성


#: (이름, 생성자, window) — ``precompute_series``를 정의한 지표 전부.
#: 새 지표에 ``precompute_series``를 붙이면 여기 한 줄을 더해야 한다.
def _indicator_cases(window: int):
    cases = [
        ("MovingAverage", lambda: MovingAverage(window)),
        ("ATRIndicator", lambda: ATRIndicator(window)),
        ("MinDonchianIndicator", lambda: MinDonchianIndicator(window)),
        ("MaxDonchianIndicator", lambda: MaxDonchianIndicator(window)),
        ("VolumeMovingAverage", lambda: VolumeMovingAverage(window)),
    ]
    if window >= 2:  # 표본표준편차는 window >= 2 가 필요하다
        cases += [
            ("RollingStd", lambda: RollingStd(window)),
            ("VolumeRollingStd", lambda: VolumeRollingStd(window)),
        ]
    return cases


def _ohlcv(candles: List[Candle]):
    n = len(candles)
    return tuple(np.fromiter((getattr(c, k) for c in candles), np.float64, n)
                 for k in ("open", "high", "low", "close", "volume"))


def _loop_series(make, candles) -> List[Optional[float]]:
    ind = make()
    out = []
    for candle in candles:
        ind.update(candle)
        out.append(ind.get_latest())
    return out


def _as_array(values: List[Optional[float]]) -> np.ndarray:
    return np.array([np.nan if v is None else v for v in values], dtype=np.float64)


def check_indicator_exactness():
    """미리 계산한 수열이 루프의 ``get_latest()`` 수열과 비트 단위로 같은가."""
    # (캔들 수, window): 워밍업 전/딱/후, 창보다 짧은 구간, window=1, 보관 이력보다 큰 창.
    shapes = [(0, 10), (1, 10), (5, 10), (10, 10), (11, 10), (3000, 1), (3000, 50),
              (3000, 720), (9000, 8200)]
    for n, window in shapes:
        candles = synthetic_candles(n, seed=n + window)
        arrays = _ohlcv(candles)
        for name, make in _indicator_cases(window):
            ref = _as_array(_loop_series(make, candles))
            got = make().precompute_series(*arrays)
            label = f"지표 정확성: {name} (n={n}, window={window})"
            if len(got) != n or got.dtype != np.float64:
                check(label, False, f"길이/dtype: {len(got)} vs {n}, {got.dtype}")
                continue
            if n == 0:
                check(label, True)
                continue
            same_nan = np.array_equal(np.isnan(ref), np.isnan(got))
            m = ~np.isnan(ref)
            same_val = np.array_equal(ref[m], got[m])
            detail = ""
            if not same_val:
                diff = np.abs(ref[m] - got[m])
                worst = int(np.argmax(diff))
                detail = (f"최대 절대차 {diff[worst]:.3e} "
                          f"(상대 {diff[worst] / abs(ref[m][worst]):.3e}), "
                          f"불일치 {int(np.count_nonzero(diff))}/{len(diff)}")
            elif not same_nan:
                detail = "NaN 위치가 다르다 (워밍업 길이 불일치)"
            check(label, same_nan and same_val, detail)


def check_sink_playback():
    """``precomputed_sink``로 재생한 지표의 ``read(idx)``가 루프와 전 인덱스에서 같은가.

    ``get_latest()``만 같아서는 부족하다 — 대부분의 전략이 ``read(-2)``로 직전 값을 읽고,
    루프와 재생은 워밍업 구간에서 deque 길이가 다를 수 있기 때문이다 (루프는 아무것도 얹지
    않거나 None을 얹고, 재생은 NaN을 얹는다). 보관 이력 경계의 ``IndexError``와 양수 인덱스
    거부까지 같은지도 여기서 본다.
    """
    for n, window in [(200, 1), (200, 50), (3000, 720)]:
        candles = synthetic_candles(n, seed=n * 7 + window)
        arrays = _ohlcv(candles)
        for name, make in _indicator_cases(window):
            loop_ind, play_ind = make(), make()
            sink = play_ind.precomputed_sink()
            values = make().precompute_series(*arrays).tolist()
            probes = [-1, -2, -3, -window, -window - 1, -window - 2,
                      -play_ind.history_size, -play_ind.history_size - 1, 0]
            bad = None
            for i, candle in enumerate(candles):
                loop_ind.update(candle)
                sink(values[i])
                for idx in probes:
                    a, b = _read_or_error(loop_ind, idx), _read_or_error(play_ind, idx)
                    if a != b:
                        bad = f"캔들 {i}, read({idx}): 루프 {a!r} vs 재생 {b!r}"
                        break
                if bad:
                    break
            check(f"sink 재생: {name} (n={n}, window={window})", bad is None, bad or "")


def _read_or_error(indicator, idx):
    try:
        return indicator.read(idx)
    except IndexError:
        return "IndexError"


# ==================================================== 2~3. 전략 단위 대조


def _run(streamer, candles_by_symbol, vectorize, slippage_ratio=None):
    executor = SimulatedExecutor(INIT_MARGIN, slippage_ratio=(slippage_ratio or 0.0))
    recorder = BacktestRecorder(streamer, executor.status, interval_ms=MIN)
    return BacktestEngine(streamer, candles_by_symbol, executor, recorder,
                          vectorize=vectorize, progress=False).run()


def _load(symbol, start, end):
    return BinanceVisionFetcher(compress=False).get_candles_with_cache(
        symbol, start, end, "1m")


def check_strategy_parity():
    """실제 캔들 위에서 vectorize=True와 False의 Report가 같은가."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 2, 1, tzinfo=timezone.utc)
    candles = _load(SYM, start, end)
    print(f"Loaded {len(candles)} candles for strategy parity")
    by_symbol = {SYM: candles}
    kp = dict(window=20 * 60, m_entry=2.0, m_exit=0.0, max_loss=0.02)

    cases = [
        # 시장가 전용. MA + ATR 둘 다 미리 계산된다.
        ("Keltner (시장가)", lambda: KeltnerStreamer(symbols=[SYM], **kp), None),
        # 상태를 캔들 사이에 들고 가는 전략 + RollingStd.
        ("MeanReversionZScore (상태 있는 전략)",
         lambda: MeanReversionZScoreStreamer(symbol=SYM, window=60, entry_z=2.0,
                                             timeout_candles=60, max_loss=0.02), None),
        # 조건부 주문 — 트리거 가격이 **지표값**이라 지표 반올림이 체결가에 직접 들어간다.
        ("KeltnerStop (조건부 주문)",
         lambda: KeltnerStopStreamer(symbols=[SYM], window=72 * 60, m_entry=4.0,
                                     m_exit=3.0, max_loss=0.005), None),
        # 벡터화 불가 지표만 쓰는 전략 — 선계산이 하나도 없는 경로.
        ("Supertrend (벡터화 불가 지표)",
         lambda: SupertrendStreamer(symbol=SYM, atr_window=6 * 60, multiplier=3.0,
                                    max_loss=0.02), None),
        # MovingAverage(1) + 파라미터로 정해지는 history_size = 깊은 조회 경로.
        ("MomentumTimeExit (깊은 read)",
         lambda: MomentumTimeExitStreamer(symbol=SYM, mom_lookback=90,
                                          entry_threshold_pct=0.5, hold_candles=120,
                                          max_loss=0.02), None),
    ]
    for label, make_streamer, slippage in cases:
        vec = _run(make_streamer(), by_symbol, True, slippage)
        ref = _run(make_streamer(), by_symbol, False, slippage)
        check(f"vectorize == loop: {label}", compare_reports(label, ref, vec),
              f"trades={len(ref.trades)} final={ref.status.total_margin():.2f}")


def check_multi_symbol_ragged():
    """상장일이 다르고 구멍이 있는 두 심볼 — 심볼별 재생 커서가 병합과 어긋나지 않는가."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 2, 1, tzinfo=timezone.utc)
    eth = _load(SYM, start, end)
    btc = _load(SYM2, start, end)
    # BTC는 늦게 시작하고 중간에 구멍이 있다. 두 심볼의 캔들 수가 서로 다르고, 이벤트마다
    # 등장하는 심볼 집합이 바뀐다 — 커서를 전역 이벤트 인덱스로 잡았다면 여기서 어긋난다.
    btc = btc[500:]
    btc = btc[:5000] + btc[5800:]
    by_symbol = {SYM: eth, SYM2: btc}
    print(f"Loaded {len(eth)} / {len(btc)} candles for ragged multi-symbol parity")

    kp = dict(window=6 * 60, m_entry=2.0, m_exit=0.0, max_loss=0.02)
    vec = _run(KeltnerStreamer(symbols=[SYM, SYM2], **kp), by_symbol, True)
    ref = _run(KeltnerStreamer(symbols=[SYM, SYM2], **kp), by_symbol, False)
    check("vectorize == loop: 멀티심볼 ragged", compare_reports("ragged", ref, vec),
          f"trades={len(ref.trades)} final={ref.status.total_margin():.2f}")


# ==================================================== 4. 퇴화 입력


def check_degenerate_inputs():
    """빈/짧은 입력에서 죽지 않고 Report를 돌려주는가."""
    kp = dict(window=20, m_entry=2.0, m_exit=0.0, max_loss=0.02)
    cases = [
        ("심볼 자체가 없다", {}),
        ("캔들 0개", {SYM: []}),
        ("캔들 1개", {SYM: synthetic_candles(1)}),
        ("창보다 짧다", {SYM: synthetic_candles(5)}),
    ]
    for label, by_symbol in cases:
        ok, detail = True, ""
        for vectorize in (True, False):
            try:
                report = _run(KeltnerStreamer(symbols=[SYM], **kp), by_symbol, vectorize)
                if report is None or report.trades:
                    ok, detail = False, f"vectorize={vectorize}: 예상 밖 결과 {report}"
            except Exception as e:  # noqa: BLE001 - 검사라 무엇이 터졌든 보고한다
                ok, detail = False, f"vectorize={vectorize}: {type(e).__name__}: {e}"
        check(f"퇴화 입력: {label}", ok, detail)

    # 스트리머가 다루지 않는 심볼이 섞여 있어도 통과해야 한다 (지표가 없을 뿐이다).
    by_symbol = {SYM: synthetic_candles(300), SYM2: synthetic_candles(300, seed=9)}
    try:
        report = _run(KeltnerStreamer(symbols=[SYM], **kp), by_symbol, True)
        check("퇴화 입력: 전략이 모르는 심볼이 섞여 있다", report is not None,
              f"trades={len(report.trades)}")
    except Exception as e:  # noqa: BLE001
        check("퇴화 입력: 전략이 모르는 심볼이 섞여 있다", False, f"{type(e).__name__}: {e}")

    # precompute_series가 캔들 수와 다른 길이를 주면 조용히 밀리지 않고 죽어야 한다.
    class _BadLength(MovingAverage):
        def precompute_series(self, open, high, low, close, volume):
            return super().precompute_series(open, high, low, close, volume)[:-1]

    streamer = KeltnerStreamer(symbols=[SYM], **kp)
    streamer.indicators[SYM]["MA"] = _BadLength(20)
    try:
        _run(streamer, {SYM: synthetic_candles(300)}, True)
        check("precompute_series 길이 불일치는 치명적", False, "예외가 나지 않았다")
    except ValueError:
        check("precompute_series 길이 불일치는 치명적", True)


# ====================================================


def main():
    setup_logging(default="WARNING")
    check_indicator_exactness()
    check_sink_playback()
    check_degenerate_inputs()
    if "--offline" not in sys.argv:
        check_strategy_parity()
        check_multi_symbol_ragged()

    if _failures:
        print(f"BACKTEST CHECK: FAILED ({len(_failures)}건) — {_failures}")
        sys.exit(1)
    print("BACKTEST CHECK: ALL OK")


if __name__ == "__main__":
    main()
