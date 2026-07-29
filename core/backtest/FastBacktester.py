import copy
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
from tqdm import tqdm

from core.backtest.SingleThreadedBacktester import SingleThreadedBacktester
from core.backtest.metrics import build_buy_and_hold_curve
from core.backtest.report import Report
from core.backtest.result_writer import write_run_json, write_series_shards
from core.backtest.status import Status
from core.backtest.trade import Trade
from core.streamer import Action
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import BaseIndicator, VectorizedIndicator


class ArrayIndicator(BaseIndicator):
    """Cursor-backed view over a precomputed indicator series.

    ``cursor`` is the number of candles already applied and ``seq[i]`` is the value after i+1
    updates, so the cursor encodes the ingest ordering: an ``updates_before_decide=False``
    indicator sits at cursor == i during candle i and ``get_latest()`` excludes candle i, while
    a flagged one is advanced to i+1 first so ``get_latest()`` includes it. Either way this
    matches what a loop-updated indicator on the same side of ``decide_action`` would return.

    ``history_size`` is mirrored from the original indicator so a read deeper than the loop
    path retains raises ``IndexError`` here too, instead of quietly returning a value the loop
    path could not have produced.
    """

    def __init__(self, seq: np.ndarray, window: int, history_size: int):
        super().__init__(window, history_size=history_size)
        self.seq = seq
        self.cursor = 0

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        self.cursor += 1

    def get_index(self, idx: int) -> Optional[float]:
        if idx < -self.history_size:
            raise IndexError(
                f"ArrayIndicator.get_index({idx}): 보관 이력 {self.history_size}개를 넘는 "
                f"조회다. 지표 생성자에 history_size를 키워 넘겨라.")
        i = self.cursor + idx  # idx is negative (-1 = latest)
        if i < 0:
            return None
        v = self.seq[i]
        return None if v != v else v

    def get_latest(self) -> Optional[float]:
        i = self.cursor - 1
        if i < 0:
            return None
        v = self.seq[i]
        return None if v != v else v


class FastBacktester(SingleThreadedBacktester):
    """Drop-in faster ``SingleThreadedBacktester``.

    ``VectorizedIndicator``s are precomputed in one shot from numpy candle arrays and swapped
    for :class:`ArrayIndicator` shims during the run; plain ``BaseIndicator``s keep being
    updated candle-by-candle exactly like the reference implementation, so both kinds can be
    mixed freely. The trade accounting (``_trade``) is inherited unchanged.
    """

    def run(self, start_time: Optional[int] = None, end_time: Optional[int] = None,
            metadata: Optional[Dict] = None, save_series: bool = False,
            has_ohlc: bool = True) -> Report:
        indicator_names = list(self.streamer.indicators.keys())
        column_groups = {name: self.streamer.indicators[name].scale_group for name in indicator_names}
        streamer_name = type(self.streamer).__name__
        run_id = f"{streamer_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        init_margin = self.status.total_margin()
        interval_ms = (self.candles[1].start_time - self.candles[0].start_time
                       if len(self.candles) >= 2 else 0)

        # start/end filtering as one slice instead of per-candle checks
        end_times_all = np.fromiter((c.end_time for c in self.candles), dtype=np.int64,
                                    count=len(self.candles))
        lo = int(np.searchsorted(end_times_all, start_time, side="left")) if start_time else 0
        hi = int(np.searchsorted(end_times_all, end_time, side="right")) if end_time else len(self.candles)
        candles = self.candles[lo:hi]
        n = len(candles)

        open_arr = np.fromiter((c.open for c in candles), dtype=np.float64, count=n)
        high_arr = np.fromiter((c.high for c in candles), dtype=np.float64, count=n)
        low_arr = np.fromiter((c.low for c in candles), dtype=np.float64, count=n)
        close_arr = np.fromiter((c.close for c in candles), dtype=np.float64, count=n)
        volume_arr = np.fromiter((c.volume for c in candles), dtype=np.float64, count=n)
        end_times = end_times_all[lo:hi]

        # split indicators: vectorized ones become array shims, the rest stay loop-updated
        original_indicators = self.streamer.indicators
        run_indicators: Dict = {}
        precomputed: Dict[str, np.ndarray] = {}
        live: Dict[str, BaseIndicator] = {}
        for name, indicator in original_indicators.items():
            if isinstance(indicator, VectorizedIndicator):
                seq = indicator.precompute_series(open_arr, high_arr, low_arr, close_arr, volume_arr)
                precomputed[name] = seq
                shim = ArrayIndicator(seq, indicator.window, indicator.history_size)
                shim.updates_before_decide = indicator.updates_before_decide
                run_indicators[name] = shim
            else:
                live[name] = indicator
                run_indicators[name] = indicator

        # the originals are the authority for the flag; shims mirror it above
        before_names = {name for name, ind in original_indicators.items()
                        if ind.updates_before_decide}

        write_output = metadata is not None
        collect_live_series = write_output and save_series and bool(live)
        live_series: Dict[str, List[Optional[float]]] = {name: [] for name in live}
        live_items = list(live.items())

        trades: List[Trade] = []
        max_leverage = 0.0
        # (candle idx, post-trade margin/position/avg_price) marks for equity reconstruction
        trade_marks: List[Tuple[int, float, float, float]] = []

        status = self.status
        decide = self.streamer.update_candle
        live_before = [ind for name, ind in live.items() if name in before_names]
        live_after = [ind for name, ind in live.items() if name not in before_names]
        shims_before = [ind for name, ind in run_indicators.items()
                        if isinstance(ind, ArrayIndicator) and name in before_names]
        shims_after = [ind for name, ind in run_indicators.items()
                       if isinstance(ind, ArrayIndicator) and name not in before_names]

        self.streamer.indicators = run_indicators
        try:
            for i, candle in enumerate(tqdm(candles, desc="Backtesting", unit="candle",
                                            file=sys.stdout)):
                price = candle.close

                # opt-in indicators ingest this candle before the decision sees them
                cursor = i + 1
                for shim in shims_before:
                    shim.cursor = cursor
                for indicator in live_before:
                    indicator.update(candle, status)

                if status.position != 0.0:
                    status.unrealised_pnl = status.position * (price - status.avg_price)
                    # 강제청산: 시가평가 자본이 0 이하 = 파산 (reference와 같은 조건)
                    if status.margin + status.unrealised_pnl <= 0.0:
                        action = Action(-status.position)
                    else:
                        action = decide(candle, status)
                else:
                    status.unrealised_pnl = 0.0
                    # 미실현이 0이라 reference의 total_margin() <= 0 과 같은 조건이다
                    if status.margin <= 0.0:  # bankrupt & flat: reference skips the streamer too
                        action = Action(0)
                    else:
                        action = decide(candle, status)

                if collect_live_series:
                    # collected between the two update groups, so each value is the one
                    # decide_action saw regardless of which side it was fed on
                    for name, indicator in live_items:
                        live_series[name].append(indicator.get_latest())

                # indicators see the same pre-trade status decide_action saw for this candle
                for indicator in live_after:
                    indicator.update(candle, status)
                for shim in shims_after:
                    shim.cursor = cursor

                if action.quantity != 0:
                    prev_status = copy.deepcopy(status)
                    wnl, fee = self._trade(action, price)
                    leverage = status.update_leverage()
                    if leverage > max_leverage:
                        max_leverage = leverage
                    trades.append(Trade(
                        timestamp=candle.end_time,
                        quantity=action.quantity,
                        price=price,
                        wnl=wnl,
                        fee=fee,
                        status=prev_status,
                        leverage=leverage,
                    ))
                    trade_marks.append((i, status.margin, status.position, status.avg_price))
        finally:
            self.streamer.indicators = original_indicators

        equity_curve = self._build_equity_curve(end_times, close_arr, trade_marks, init_margin)
        benchmark_curve = build_buy_and_hold_curve(end_times, close_arr, init_margin)
        report = Report(trades, max_leverage, self.status, equity_curve, benchmark_curve)

        if write_output:
            backtest_dir = self._prepare_backtest_dir()
            shards = []
            if save_series:
                columns = self._build_series_columns(indicator_names, precomputed, live_series, n,
                                                     before_names)
                # pre-trade equity at each candle — unlike indicator columns this belongs to its
                # own candle, so no decide-time shift
                columns["balance"] = np.fromiter((v for _, v in equity_curve),
                                                 dtype=np.float64, count=n)
                ohlc = ({"open": open_arr, "high": high_arr, "low": low_arr, "close": close_arr}
                        if has_ohlc else None)
                shards = write_series_shards(backtest_dir, run_id, end_times, ohlc, columns,
                                             interval_ms)
            meta = dict(metadata)
            meta.setdefault("streamer", streamer_name)
            meta["run_at"] = datetime.now(timezone.utc).isoformat()
            meta["init_margin"] = init_margin
            meta["candle_count"] = n
            meta.setdefault("interval_ms", interval_ms)
            write_run_json(backtest_dir, run_id, report, meta, shards,
                           columns=indicator_names + ["balance"],
                           column_groups={**column_groups, "balance": "balance"},
                           has_ohlc=has_ohlc, interval_ms=interval_ms, init_margin=init_margin)

        return report

    def _prepare_backtest_dir(self) -> str:
        backtest_dir = os.path.join(self.result_path, "backtest")
        os.makedirs(backtest_dir, exist_ok=True)
        return backtest_dir

    @staticmethod
    def _build_equity_curve(end_times: np.ndarray, close_arr: np.ndarray,
                            trade_marks: List[Tuple[int, float, float, float]],
                            init_margin: float) -> List[Tuple[int, float]]:
        """Rebuild the per-candle equity curve vectorized.

        Between trades margin/position/avg_price are constant, so each segment is just
        ``margin + position * (close - avg_price)``. The reference records equity *before*
        the trade at a trade candle, so the state from trade k applies to candles
        (idx_k, idx_{k+1}].
        """
        n = len(end_times)
        if n == 0:
            return []
        equity = np.empty(n, dtype=np.float64)
        seg_start = 0
        margin, position, avg_price = init_margin, 0.0, 0.0
        for idx, m, p, a in trade_marks:
            seg_end = idx + 1
            if position != 0.0:
                equity[seg_start:seg_end] = margin + position * (close_arr[seg_start:seg_end] - avg_price)
            else:
                equity[seg_start:seg_end] = margin
            seg_start = seg_end
            margin, position, avg_price = m, p, a
        if seg_start < n:
            if position != 0.0:
                equity[seg_start:] = margin + position * (close_arr[seg_start:] - avg_price)
            else:
                equity[seg_start:] = margin
        return list(zip(end_times.tolist(), equity.tolist()))

    @staticmethod
    def _build_series_columns(indicator_names: List[str], precomputed: Dict[str, np.ndarray],
                              live_series: Dict[str, List[Optional[float]]], n: int,
                              before_names: Optional[set] = None) -> Dict:
        """Assemble decide-time indicator columns.

        ``precomputed[name][i]`` is the value after i+1 updates. An ``updates_before_decide``
        indicator was already advanced when the streamer ran, so its decide-time value at candle
        i is ``seq[i]`` — used as-is. Everything else was still one candle behind, so its column
        is shifted one candle right. Live (loop) columns were collected at decide time already
        and are correct for both groups.
        """
        before_names = before_names or set()
        columns: Dict = {}
        for name in indicator_names:
            if name in precomputed:
                if name in before_names:
                    columns[name] = precomputed[name]
                else:
                    shifted = np.empty(n, dtype=np.float64)
                    if n > 0:
                        shifted[0] = np.nan
                        shifted[1:] = precomputed[name][:-1]
                    columns[name] = shifted
            else:
                columns[name] = live_series[name]
        return columns
