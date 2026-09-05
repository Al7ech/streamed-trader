import copy
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
from tqdm import tqdm

from core.backtest.candle_merge import merge_candle_timeline
from core.backtest.metrics import build_multi_symbol_buy_and_hold_curve, forward_fill_nan
from core.backtest.SingleThreadedBacktester import SingleThreadedBacktester
from core.backtest.report import Report
from core.backtest.result_writer import write_run_json, write_series_shards
from core.backtest.status import Status
from core.backtest.trade import Trade
from core.streamer import Action
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import BaseIndicator


class ArrayIndicator(BaseIndicator):
    """Cursor-backed view over a precomputed indicator series.

    ``cursor`` is the number of candles (of the owning symbol's **own** series) already applied,
    and ``seq[i]`` is the value after i+1 updates. Every symbol's appearance in the merged event
    stream advances the cursor to j+1 *before* the decision reads it, so ``get_latest()`` returns
    ``seq[j]`` — the value through that symbol's j-th own candle, matching what a loop-updated
    indicator would return now that every indicator ingests the candle before ``decide_action``.

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
        if idx >= 0:
            # 여기서 막지 않으면 seq[cursor] = 아직 반영되지 않은 캔들의 값이 나온다 (룩어헤드).
            raise IndexError(
                f"ArrayIndicator.get_index({idx}): 인덱스는 음수여야 한다 "
                f"(-1 = 최신, -2 = 직전).")
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
    """Drop-in faster ``SingleThreadedBacktester``, generalized to multiple symbols.

    Indicators that define ``precompute_series`` are precomputed in one shot **per symbol** from
    that symbol's own numpy candle arrays and swapped for :class:`ArrayIndicator` shims during the
    run; plain ``BaseIndicator``s without it keep being updated candle-by-candle exactly like the
    reference implementation, so both kinds can be mixed freely. The trade accounting (``_trade``)
    is inherited unchanged.

    Candles from every symbol are still merged into one chronological event timeline (see
    :func:`core.backtest.candle_merge.merge_candle_timeline`) — the vectorization is entirely
    about how each symbol's own indicator series and equity contribution are computed, not about
    skipping the merge.
    """

    def run(self, start_time: Optional[int] = None, end_time: Optional[int] = None,
            metadata: Optional[Dict] = None, save_series: bool = False,
            has_ohlc: bool = True) -> Report:
        symbols = self.streamer.symbols
        indicator_names: List[str] = []
        seen = set()
        for symbol in symbols:
            for name in self.streamer.indicators.get(symbol, {}):
                if name not in seen:
                    seen.add(name)
                    indicator_names.append(name)
        column_groups: Dict[str, str] = {}
        for name in indicator_names:
            for symbol in symbols:
                ind = self.streamer.indicators.get(symbol, {}).get(name)
                if ind is not None:
                    column_groups[name] = ind.scale_group
                    break

        streamer_name = type(self.streamer).__name__
        run_id = f"{streamer_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        init_margin = self.status.total_margin()
        # 첫 거래 이전 구간의 자본을 되살리려면 진입 시점의 실제 상태가 필요하다.
        entry_margin = self.status.margin
        entry_positions: Dict[str, Tuple[float, float]] = {
            sym: (p.position, p.avg_price) for sym, p in self.status.positions.items()}
        interval_ms = self._interval_ms()

        # 심볼별 슬라이스 + numpy 배열. start/end 필터는 병합 이벤트 시각 기준 필터와 동치다 —
        # 한 이벤트의 모든 후보 캔들이 같은 end_time을 공유하므로, 심볼별로 미리 잘라내도
        # 참조 구현이 이벤트 단위로 자르는 것과 같은 부분집합이 남는다.
        sliced_candles: Dict[str, List[Candle]] = {}
        arrays: Dict[str, Dict[str, np.ndarray]] = {}
        for symbol in symbols:
            candles = self.candles_by_symbol.get(symbol, [])
            end_times_all = np.fromiter((c.end_time for c in candles), dtype=np.int64,
                                        count=len(candles))
            lo = int(np.searchsorted(end_times_all, start_time, side="left")) if start_time else 0
            hi = (int(np.searchsorted(end_times_all, end_time, side="right"))
                 if end_time else len(candles))
            sub = candles[lo:hi]
            n_sym = len(sub)
            sliced_candles[symbol] = sub
            arrays[symbol] = {
                "open": np.fromiter((c.open for c in sub), np.float64, n_sym),
                "high": np.fromiter((c.high for c in sub), np.float64, n_sym),
                "low": np.fromiter((c.low for c in sub), np.float64, n_sym),
                "close": np.fromiter((c.close for c in sub), np.float64, n_sym),
                "volume": np.fromiter((c.volume for c in sub), np.float64, n_sym),
            }

        # split indicators per symbol: vectorized ones become array shims, the rest stay loop-updated
        original_indicators = self.streamer.indicators
        run_indicators: Dict[str, Dict] = {symbol: {} for symbol in symbols}
        precomputed: Dict[str, Dict[str, np.ndarray]] = {symbol: {} for symbol in symbols}
        live: Dict[str, Dict[str, BaseIndicator]] = {symbol: {} for symbol in symbols}
        for symbol in symbols:
            a = arrays[symbol]
            for name, indicator in original_indicators.get(symbol, {}).items():
                precompute = getattr(indicator, "precompute_series", None)
                if precompute is not None:
                    seq = precompute(a["open"], a["high"], a["low"], a["close"], a["volume"])
                    precomputed[symbol][name] = seq
                    shim = ArrayIndicator(seq, indicator.window, indicator.history_size)
                    run_indicators[symbol][name] = shim
                else:
                    live[symbol][name] = indicator
                    run_indicators[symbol][name] = indicator

        # merged event timeline over the (already sliced) per-symbol candle lists
        events = list(merge_candle_timeline(sliced_candles))
        n_events = len(events)
        event_times = np.fromiter((t for t, _ in events), dtype=np.int64, count=n_events)

        # 심볼별로 "자기 몇 번째 캔들이 어느 이벤트 인덱스에 있는지" — ArrayIndicator 커서
        # 진행과 종가/지표 값을 이벤트 그리드에 흩뿌리는 데 둘 다 쓰인다.
        event_index_by_symbol: Dict[str, List[int]] = {symbol: [] for symbol in symbols}
        for i, (_, batch) in enumerate(events):
            for symbol, _ in batch:
                event_index_by_symbol[symbol].append(i)

        # 종가를 이벤트 그리드에 흩뿌리고 직전값으로 채운다 — 자본 곡선 재구성과 buy & hold
        # 기준선 양쪽에 쓰인다 (그 심볼의 캔들이 없는 이벤트는 최신 알려진 종가로 시가평가).
        close_on_grid: Dict[str, np.ndarray] = {}
        ohlc_on_grid: Dict[str, Dict[str, np.ndarray]] = {}
        for symbol in symbols:
            idxs = np.asarray(event_index_by_symbol[symbol], dtype=np.int64)
            a = arrays[symbol]
            grid_close = np.full(n_events, np.nan, dtype=np.float64)
            if len(idxs) > 0:
                grid_close[idxs] = a["close"]
            close_on_grid[symbol] = forward_fill_nan(grid_close)
            if has_ohlc:
                oh: Dict[str, np.ndarray] = {}
                for k in ("open", "high", "low", "close"):
                    g = np.full(n_events, np.nan, dtype=np.float64)
                    if len(idxs) > 0:
                        g[idxs] = a[k]
                    # 실제 구멍을 그대로 둔다 (직전값으로 채우지 않는다) — 그 심볼의 캔들이
                    # 없는 이벤트에서는 OHLC를 null로 남겨 프론트가 그 구간을 건너뛰게 한다.
                    oh[k] = g
                ohlc_on_grid[symbol] = oh

        write_output = metadata is not None
        collect_live_series = write_output and save_series and any(live[s] for s in symbols)
        live_series_on_grid: Dict[str, Dict[str, List[Optional[float]]]] = {
            symbol: {name: [None] * n_events for name in live[symbol]} for symbol in symbols
        } if collect_live_series else {}

        trades: List[Trade] = []
        max_leverage = 0.0
        # (event idx, post-trade margin, {symbol: (position, avg_price)}) marks for equity
        # reconstruction — a fill only changes one symbol's position, but reconstructing equity
        # for the segment needs every symbol's state, so the full snapshot is kept each time.
        trade_marks: List[Tuple[int, float, Dict[str, Tuple[float, float]]]] = []

        status = self.status
        self.streamer.indicators = run_indicators
        try:
            for event_idx, (event_time, batch) in enumerate(
                    tqdm(events, desc="Backtesting", unit="event", file=sys.stdout)):
                event_candles: Dict[str, Candle] = {}
                for symbol, candle in batch:
                    event_candles[symbol] = candle
                    status.last_close[symbol] = candle.close

                # 시가평가를 먼저 한다 (before-indicator가 거래 전 스냅샷을 보게 하려면 이 순서).
                for symbol, pos_state in status.positions.items():
                    if pos_state.position != 0.0 and symbol in status.last_close:
                        status.update_unrealised_pnl(symbol, status.last_close[symbol])
                event_equity = status.total_margin()

                # 강제청산: 시가평가 자본이 0 이하면 파산. 지표 갱신은 파산 여부와 무관하게
                # 계속된다 — 건너뛰는 것은 스트리머의 결정 호출뿐이다 (reference와 동일).
                bankrupt = event_equity <= 0.0
                actions: List[Action] = []
                for symbol in symbols:
                    candle = event_candles.get(symbol)
                    if candle is None:
                        continue

                    # ArrayIndicator.update()는 자기 cursor를 1 증가시킬 뿐이라, 그 심볼의
                    # 이벤트마다 정확히 한 번씩만 불러도 "자기 몇 번째 캔들까지 반영했는지"가
                    # 맞게 유지된다 — live(루프) 지표와 같은 호출 하나로 충분하다. 모든 지표가
                    # decide_action보다 먼저 갱신된다.
                    for indicator in run_indicators[symbol].values():
                        indicator.update(candle, status)

                    if bankrupt:
                        position = status.position_for(symbol).position
                        symbol_actions = [Action(symbol, -position)] if position != 0.0 else []
                    else:
                        symbol_actions = self.streamer.update_candle(symbol, candle, status)

                    if collect_live_series:
                        for name, indicator in live[symbol].items():
                            live_series_on_grid[symbol][name][event_idx] = indicator.get_latest()

                    actions.extend(symbol_actions)

                for action in actions:
                    if action.quantity == 0:
                        continue
                    price = status.last_close.get(action.symbol)
                    if price is None:
                        continue
                    prev_status = copy.deepcopy(status)
                    wnl, fee = self._trade(action, price)
                    leverage = status.update_leverage()
                    if leverage > max_leverage:
                        max_leverage = leverage
                    trades.append(Trade(
                        timestamp=event_time,
                        symbol=action.symbol,
                        quantity=action.quantity,
                        price=price,
                        wnl=wnl,
                        fee=fee,
                        status=prev_status,
                        leverage=leverage,
                    ))
                    trade_marks.append((event_idx, status.margin,
                                        {s: (p.position, p.avg_price)
                                        for s, p in status.positions.items()}))
        finally:
            self.streamer.indicators = original_indicators

        equity_curve = self._build_equity_curve(event_times, close_on_grid, trade_marks,
                                                entry_margin, entry_positions)
        benchmark_curve = build_multi_symbol_buy_and_hold_curve(
            event_times, close_on_grid, init_margin)
        report = Report(trades, max_leverage, self.status, equity_curve, benchmark_curve)

        if write_output:
            backtest_dir = self._prepare_backtest_dir()
            shards = []
            if save_series:
                symbol_columns: Dict[str, Dict[str, Optional[Dict[str, object]]]] = {}
                for symbol in symbols:
                    idxs = event_index_by_symbol[symbol]
                    cols = self._build_symbol_series_columns(
                        indicator_names, precomputed[symbol], live_series_on_grid.get(symbol, {}),
                        n_events, idxs)
                    symbol_columns[symbol] = {
                        "indicators": cols,
                        "ohlc": ohlc_on_grid.get(symbol) if has_ohlc else None,
                    }
                balance = np.fromiter((v for _, v in equity_curve), dtype=np.float64,
                                      count=n_events)
                shards = write_series_shards(backtest_dir, run_id, event_times, balance,
                                             symbol_columns, interval_ms, has_ohlc)
            meta = dict(metadata)
            meta.setdefault("streamer", streamer_name)
            meta["run_at"] = datetime.now(timezone.utc).isoformat()
            meta["init_margin"] = init_margin
            meta["candle_count"] = n_events
            meta.setdefault("interval_ms", interval_ms)
            write_run_json(backtest_dir, run_id, report, meta, shards,
                           symbols=symbols,
                           columns=indicator_names + ["balance"],
                           column_groups={**column_groups, "balance": "balance"},
                           has_ohlc=has_ohlc, interval_ms=interval_ms, init_margin=init_margin)

        return report

    def _prepare_backtest_dir(self) -> str:
        backtest_dir = os.path.join(self.result_path, "backtest")
        os.makedirs(backtest_dir, exist_ok=True)
        return backtest_dir

    @staticmethod
    def _build_equity_curve(event_times: np.ndarray, close_on_grid: Dict[str, np.ndarray],
                            trade_marks: List[Tuple[int, float, Dict[str, Tuple[float, float]]]],
                            entry_margin: float,
                            entry_positions: Dict[str, Tuple[float, float]]
                            ) -> List[Tuple[int, float]]:
        """Rebuild the per-event equity curve vectorized.

        Between trades margin and every symbol's (position, avg_price) are constant, so each
        segment is ``margin + Σ_sym position_sym * (close_sym[seg] - avg_price_sym)``. The
        reference records equity *before* the trade at a trade event, so the state from trade k
        applies to events (idx_k, idx_{k+1}].

        첫 거래 이전 구간은 **진입 시점의 실제 상태**로 시드한다 (``run()``을 같은 인스턴스로
        두 번 부르거나 포지션을 들고 시작할 때를 위해).
        """
        n = len(event_times)
        if n == 0:
            return []
        equity = np.empty(n, dtype=np.float64)
        seg_start = 0
        margin = entry_margin
        positions = entry_positions
        for idx, m, pos_snapshot in trade_marks:
            seg_end = idx + 1
            equity[seg_start:seg_end] = margin
            for sym, (pos, avg) in positions.items():
                if pos != 0.0:
                    equity[seg_start:seg_end] += pos * (close_on_grid[sym][seg_start:seg_end] - avg)
            seg_start = seg_end
            margin, positions = m, pos_snapshot
        if seg_start < n:
            equity[seg_start:] = margin
            for sym, (pos, avg) in positions.items():
                if pos != 0.0:
                    equity[seg_start:] += pos * (close_on_grid[sym][seg_start:] - avg)
        return list(zip(event_times.tolist(), equity.tolist()))

    @staticmethod
    def _build_symbol_series_columns(indicator_names: List[str], precomputed_sym: Dict[str, np.ndarray],
                                     live_series_sym: Dict[str, List[Optional[float]]],
                                     n_events: int, event_indices_sym: List[int]) -> Dict[str, object]:
        """한 심볼의 지표 컬럼을 결정 시점 값으로 조립해 이벤트 그리드에 흩뿌린다.

        ``precomputed_sym[name][j]``는 그 심볼의 j+1번째 캔들 반영 후 값이자 그 캔들의 결정이
        본 값이므로(모든 지표가 decide_action보다 먼저 갱신된다) 시프트 없이 그대로 쓴다.
        live(루프) 컬럼은 결정 시점에 이미 이벤트 그리드 위에서 수집됐다.
        """
        idxs = np.asarray(event_indices_sym, dtype=np.int64) if event_indices_sym else None
        columns: Dict[str, object] = {}
        for name in indicator_names:
            if name in precomputed_sym:
                seq = precomputed_sym[name]
                grid = np.full(n_events, np.nan, dtype=np.float64)
                if idxs is not None and len(seq) > 0:
                    grid[idxs] = seq
                columns[name] = grid
            elif name in live_series_sym:
                columns[name] = live_series_sym[name]
            else:
                columns[name] = [None] * n_events
        return columns
