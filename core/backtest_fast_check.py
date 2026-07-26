"""Parity + benchmark check: FastBacktester vs SingleThreadedBacktester (reference).

Runs both backtesters over the same cached candles and asserts identical trades, final
margin, max leverage and equity curve. Also exercises the mixed path by wrapping one
indicator so it loses its VectorizedIndicator type and must be loop-updated.

    uv run python core/backtest_fast_check.py          # last 12 months
    uv run python core/backtest_fast_check.py --full   # 2020-01 ~ now (slow reference run)
"""
import math
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from core.backtest.FastBacktester import FastBacktester
from core.backtest.SingleThreadedBacktester import SingleThreadedBacktester
from core.backtest.status import Status
from core.binance_candle_fetcher.vision_fetcher import BinanceVisionFetcher
from core.streamer.candle import Candle
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

    print("PARITY:", "ALL OK" if all_ok else "FAILED")
    sys.exit(0 if all_ok else 1)
