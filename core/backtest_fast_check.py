"""Parity + benchmark check: FastBacktester vs SingleThreadedBacktester (reference).

Runs both backtesters over the same cached candles and asserts identical trades, final
margin, max leverage and equity curve. Also exercises the mixed path by wrapping one
indicator so it loses its VectorizedIndicator type and must be loop-updated.

    uv run python core/backtest_fast_check.py          # last 12 months
    uv run python core/backtest_fast_check.py --full   # 2020-01 ~ now (slow reference run)

KNOWN: ``--full`` currently reports a mismatch on some cases, and it is a pre-existing
numerical issue rather than a logic divergence. An incremental running sum (`MovingAverage`,
`ATRIndicator`) rounds differently from pandas' rolling sum used by `precompute_series`, so
the two paths disagree on ~99% of candles by up to ~4e-14 (MA) / ~3e-12 (ATR) relative,
starting at the first post-warm-up candle. Over 3.4M candles that flips a handful of
threshold comparisons and changes a few trades out of several thousand. The 12-month run
happens to have no such coincidence. Exact bit-parity between an O(1) incremental update and
a vectorized rolling sum is not achievable without changing one side's arithmetic; treat a
small `--full` trade-count delta as this, and any structural difference as a real bug.
"""
import math
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import numpy as np

from core.backtest.FastBacktester import ArrayIndicator, FastBacktester
from core.backtest.SingleThreadedBacktester import SingleThreadedBacktester
from core.backtest.status import Status
from core.binance_candle_fetcher.vision_fetcher import BinanceVisionFetcher
from core.streamer.action import Action
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.streamer.indicator.atr import ATRIndicator
from core.streamer.indicator.base_indicator import BaseIndicator
from core.streamer.keltner_streamer import KeltnerStreamer
from core.streamer.mean_reversion_zscore import MeanReversionZScoreStreamer


class LoopOnlyIndicator(BaseIndicator):
    """Hides the VectorizedIndicator type of the wrapped indicator, forcing the loop path."""

    def __init__(self, inner: BaseIndicator):
        super().__init__(inner.window)
        self._inner = inner

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        self._inner.update(candle, status)

    def get_index(self, idx: int) -> Optional[float]:
        return self._inner.get_index(idx)

    def get_latest(self) -> Optional[float]:
        return self._inner.get_latest()


class BlowUpStreamer(BaseStreamer):
    """파산하도록 과레버리지로 한 방 잡고 버티는 픽스처 (전략이 아니라 검사용).

    강제청산은 두 백테스터가 **스트리머를 우회해서** 내는 유일한 액션인데, 위의 정상 케이스들은
    이 경로를 한 번도 밟지 않는다. 자본의 `leverage` 배 명목가치를 잡으면 반대로 몇 % 만 움직여도
    시가평가 자본이 0을 뚫으므로, 손실측 강제청산과 그 뒤의 '파산 & flat' 분기가 함께 검사된다.
    """

    def __init__(self, leverage: float = 50.0, fee_ratio: float = 0.0004):
        super().__init__({})
        self.leverage = leverage
        self.fee_ratio = fee_ratio

    def decide_action(self, candle: Candle, status: Status) -> Action:
        if status.position != 0.0:
            return Action(0)
        return Action(round(status.total_margin() * self.leverage / candle.close, 3))


def check_forced_liquidation(candles) -> bool:
    """손실측 강제청산이 실제로 발화하고, 두 백테스터가 그 지점까지 정확히 일치하는지."""
    ref_bt = SingleThreadedBacktester(BlowUpStreamer(), candles)
    ref_report = ref_bt.run()
    fast_bt = FastBacktester(BlowUpStreamer(), candles)
    fast_report = fast_bt.run()

    ok = compare_reports("forced liquidation", ref_report, ref_bt.status.total_margin(),
                         fast_report, fast_bt.status.total_margin())

    # 침묵이 곧 성공은 아니다 — 경로를 실제로 밟았는지 단언한다.
    if len(ref_report.trades) < 2:
        print(f"  [forced liquidation] FAIL: 체결이 {len(ref_report.trades)}건뿐 — "
              "강제청산 경로를 밟지 못했다. 픽스처 레버리지를 올려라.")
        return False
    exit_trade = ref_report.trades[1]
    if exit_trade.status.total_margin() > 0:
        print(f"  [forced liquidation] FAIL: 청산 시점 자본이 "
              f"{exit_trade.status.total_margin():.2f} > 0 — 전략 청산이지 강제청산이 아니다.")
        ok = False
    if ref_bt.status.total_margin() > 0:
        print(f"  [forced liquidation] FAIL: 파산했어야 하는데 자본이 "
              f"{ref_bt.status.total_margin():.2f} 남았다.")
        ok = False
    print(f"[forced liquidation] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)} "
          f"청산 시점 자본={exit_trade.status.total_margin():.2f} "
          f"최종 자본={ref_bt.status.total_margin():.2f}")
    return ok


def check_history_bound(candles) -> bool:
    """지표 이력 경계에서 루프 경로와 ArrayIndicator가 같게 동작하는지.

    예전에는 ArrayIndicator가 경계 밖에서도 실제 값을 돌려줘 루프 경로(None/IndexError)와
    갈렸다 — 그 불일치가 닫혔음을 단언한다.
    """
    n = min(len(candles), 20_000)
    sub = candles[:n]
    hist = 128
    ok = True

    loop = ATRIndicator(60, history_size=hist)
    for c in sub:
        loop.update(c)

    seq = ATRIndicator(60, history_size=hist).precompute_series(
        *(np.fromiter((getattr(c, f) for c in sub), np.float64, n)
          for f in ("open", "high", "low", "close", "volume")))
    shim = ArrayIndicator(seq, 60, hist)
    shim.cursor = n

    for idx in (-1, -2, -hist + 1, -hist):
        a, b = loop.get_index(idx), shim.get_index(idx)
        if a is None or b is None or not math.isclose(a, b, rel_tol=1e-9):
            print(f"  [history bound] FAIL: get_index({idx}) 루프={a} 배열={b}")
            ok = False

    for idx in (-hist - 1, -hist - 1000):
        for name, ind in (("loop", loop), ("array", shim)):
            try:
                ind.get_index(idx)
            except IndexError:
                continue
            print(f"  [history bound] FAIL: {name}.get_index({idx})가 IndexError를 안 냈다")
            ok = False

    print(f"[history bound] {'OK' if ok else 'MISMATCH'} — history_size={hist}, "
          f"경계 안 일치 / 경계 밖 양쪽 IndexError")
    return ok


def compare_reports(label, ref_report, ref_final, fast_report, fast_final) -> bool:
    ok = True
    if len(ref_report.trades) != len(fast_report.trades):
        print(f"  [{label}] FAIL: trade count {len(ref_report.trades)} != {len(fast_report.trades)}")
        return False
    for i, (a, b) in enumerate(zip(ref_report.trades, fast_report.trades)):
        if not (a.timestamp == b.timestamp and a.quantity == b.quantity and a.price == b.price
                and math.isclose(a.wnl, b.wnl, rel_tol=1e-12, abs_tol=1e-9)
                and math.isclose(a.fee, b.fee, rel_tol=1e-12, abs_tol=1e-9)
                and math.isclose(a.leverage, b.leverage, rel_tol=1e-12, abs_tol=1e-9)):
            print(f"  [{label}] FAIL: trade #{i} differs:\n    ref : {a}\n    fast: {b}")
            ok = False
            break
    if not math.isclose(ref_report.max_leverage, fast_report.max_leverage, rel_tol=1e-12, abs_tol=1e-9):
        print(f"  [{label}] FAIL: max_leverage {ref_report.max_leverage} != {fast_report.max_leverage}")
        ok = False
    if not math.isclose(ref_final, fast_final, rel_tol=1e-12, abs_tol=1e-6):
        print(f"  [{label}] FAIL: final margin {ref_final} != {fast_final}")
        ok = False
    if len(ref_report.equity_curve) != len(fast_report.equity_curve):
        print(f"  [{label}] FAIL: equity curve length "
              f"{len(ref_report.equity_curve)} != {len(fast_report.equity_curve)}")
        ok = False
    else:
        max_diff = 0.0
        for (ts_a, eq_a), (ts_b, eq_b) in zip(ref_report.equity_curve, fast_report.equity_curve):
            if ts_a != ts_b:
                print(f"  [{label}] FAIL: equity curve timestamps diverge at {ts_a} vs {ts_b}")
                ok = False
                break
            max_diff = max(max_diff, abs(eq_a - eq_b))
        else:
            if max_diff > 1e-6:
                print(f"  [{label}] FAIL: equity curve max diff {max_diff}")
                ok = False
    # buy & hold 기준선은 종가에서만 유도되므로 사실상 회귀 가드 — 두 경로가 같은 캔들 구간을
    # 잘라냈는지(reference 는 루프 필터, fast 는 searchsorted 슬라이스)까지 함께 확인한다
    if len(ref_report.benchmark_curve) != len(fast_report.benchmark_curve):
        print(f"  [{label}] FAIL: benchmark curve length "
              f"{len(ref_report.benchmark_curve)} != {len(fast_report.benchmark_curve)}")
        ok = False
    else:
        for (ts_a, eq_a), (ts_b, eq_b) in zip(ref_report.benchmark_curve, fast_report.benchmark_curve):
            if ts_a != ts_b or not math.isclose(eq_a, eq_b, rel_tol=1e-12, abs_tol=1e-6):
                print(f"  [{label}] FAIL: benchmark curve differs at {ts_a}: {eq_a} vs {eq_b}")
                ok = False
                break
    return ok


if __name__ == "__main__":
    symbol = "ETHUSDT"
    interval = "1m"
    end_date = datetime(2026, 7, 4, tzinfo=timezone.utc)
    if "--full" in sys.argv:
        start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
    else:
        start_date = datetime(2025, 7, 4, tzinfo=timezone.utc)

    fetcher = BinanceVisionFetcher(compress=False)
    candles = fetcher.get_candles_with_cache(symbol, start_date, end_date, interval)
    print(f"Loaded {len(candles)} candles ({start_date.date()} ~ {end_date.date()})")

    keltner_params = dict(window=20 * 60, m_entry=2.0, m_exit=0.0,
                          max_loss=0.08, fee_ratio=0.0004)

    def make_mixed():
        streamer = KeltnerStreamer(symbol=symbol, **keltner_params)
        # force the ATR onto the loop path to exercise mixed precomputed/live execution
        streamer.indicators["ATR"] = LoopOnlyIndicator(streamer.indicators["ATR"])
        return streamer

    def make_before():
        # every indicator ingests the current candle before decide_action (per-instance override)
        streamer = KeltnerStreamer(symbol=symbol, **keltner_params)
        for indicator in streamer.indicators.values():
            indicator.updates_before_decide = True
        return streamer

    def make_split():
        # MA stays vectorized+after (shims_after), ATR becomes loop-only+before (live_before).
        # Together with the two cases above this covers all four partition branches:
        # shims_before/shims_after/live_before/live_after.
        streamer = KeltnerStreamer(symbol=symbol, **keltner_params)
        streamer.indicators["ATR"] = LoopOnlyIndicator(streamer.indicators["ATR"])
        streamer.indicators["ATR"].updates_before_decide = True
        return streamer

    cases = {
        "KeltnerStreamer": lambda: KeltnerStreamer(symbol=symbol, **keltner_params),
        # stateful streamer: carries _timeout_remaining/_stop_price across candles, so it also
        # checks that the fast path doesn't disturb streamer-side state
        "MeanReversionZScoreStreamer": lambda: MeanReversionZScoreStreamer(
            symbol=symbol, window=60, entry_z=2.0, timeout_candles=60,
            max_loss=0.08, fee_ratio=0.0004),
        "KeltnerStreamer (mixed: ATR loop-only)": make_mixed,
        "KeltnerStreamer (all updates_before_decide)": make_before,
        "KeltnerStreamer (split: MA after / ATR loop-only before)": make_split,
    }

    all_ok = True
    for label, make_streamer in cases.items():
        ref_bt = SingleThreadedBacktester(make_streamer(), candles)
        t0 = time.time()
        ref_report = ref_bt.run()
        ref_dt = time.time() - t0

        fast_bt = FastBacktester(make_streamer(), candles)
        t0 = time.time()
        fast_report = fast_bt.run()
        fast_dt = time.time() - t0

        ok = compare_reports(label, ref_report, ref_bt.status.total_margin(),
                             fast_report, fast_bt.status.total_margin())
        all_ok &= ok
        print(f"[{label}] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)} "
              f"ref={ref_dt:.1f}s fast={fast_dt:.1f}s ({ref_dt / fast_dt:.1f}x)")

    # 정상 케이스가 밟지 않는 두 경로를 따로 검사한다
    all_ok &= check_forced_liquidation(candles)
    all_ok &= check_history_bound(candles)

    print("PARITY:", "ALL OK" if all_ok else "FAILED")
    sys.exit(0 if all_ok else 1)
