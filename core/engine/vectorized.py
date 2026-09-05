"""백테스트 벡터화 경로.

같은 :class:`~core.engine.engine.TradingEngine`을 쓰되 두 가지만 바꾼다:

1. **지표** — ``precompute_series``를 정의한 지표는 그 심볼의 OHLCV 배열로 전 구간을 한 번에
   계산해 :class:`ArrayIndicator` 커서 심으로 갈아끼운다. 없는 지표(경로 의존적이거나 status를
   읽는 것)는 그대로 캔들마다 갱신되므로 둘을 섞어 써도 된다.
2. **자본 곡선** — 이벤트마다 재는 대신 :class:`VectorizedRecorder`가 체결 시점의 상태
   스냅샷만 쌓았다가, 루프가 끝난 뒤 구간별로 한 번에 재구성한다.

순서 규약(:meth:`~core.engine.engine.TradingEngine.process_event`)은 참조 경로와 **같은 코드**를
지나간다 — 벡터화는 지표와 자본 곡선을 어떻게 계산하느냐의 문제일 뿐, 어떤 순서로 결정하느냐의
문제가 아니다.
"""

from typing import Dict, List, Optional, Tuple

import numpy as np

from core.engine.candle_producer import BacktestCandleProducer
from core.engine.engine import TradingEngine
from core.engine.executor import SimulatedExecutor
from core.engine.metrics import build_multi_symbol_buy_and_hold_curve, forward_fill_nan
from core.engine.recorder import Recorder
from core.engine.report import Report
from core.engine.result_writer import write_series_shards
from core.engine.status import Status
from core.engine.trade import Trade
from core.streamer import BaseStreamer
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import BaseIndicator

_OHLC_KEYS = ("open", "high", "low", "close")

#: 체결 하나가 만드는 자본 곡선 마크 — (적용 시작 이벤트 인덱스, 체결 후 margin,
#: {심볼: (포지션, 평단)}). 체결은 한 심볼만 바꾸지만 구간 자본을 재구성하려면 전 심볼의
#: 상태가 필요하므로 매번 전체 스냅샷을 남긴다.
TradeMark = Tuple[int, float, Dict[str, Tuple[float, float]]]


class ArrayIndicator(BaseIndicator):
    """미리 계산된 지표 시계열 위의 커서 뷰.

    ``cursor``는 그 지표가 속한 심볼의 **자기** 캔들 중 몇 개를 반영했는지이고, ``seq[i]``는
    i+1개를 반영한 뒤의 값이다. 병합 이벤트 스트림에서 그 심볼이 등장할 때마다 커서가 결정
    **전에** 1씩 오르므로, ``get_latest()``는 ``seq[j]`` — 그 심볼의 j번째 자기 캔들까지
    반영한 값 — 를 돌려준다. 루프로 갱신되는 지표와 정확히 같은 규약이다.

    ``history_size``는 원래 지표에서 그대로 가져온다. 루프 경로가 보관하지 않는 깊이의 조회는
    여기서도 ``IndexError``여야 한다 — 벡터화 경로만 조용히 값을 돌려주면 두 경로가 갈라진다.
    """

    def __init__(self, seq: np.ndarray, window: int, history_size: int):
        self.window = window
        self.history_size = history_size
        self.seq = seq
        self.cursor = 0

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        self.cursor += 1

    def read(self, idx: int) -> Optional[float]:
        if idx >= 0:
            # 여기서 막지 않으면 seq[cursor] = 아직 반영되지 않은 캔들의 값이 나온다 (룩어헤드).
            raise IndexError(
                f"ArrayIndicator.read({idx}): 인덱스는 음수여야 한다 "
                f"(-1 = 최신, -2 = 직전).")
        if idx < -self.history_size:
            raise IndexError(
                f"ArrayIndicator.read({idx}): 보관 이력 {self.history_size}개를 넘는 "
                f"조회다. 지표 생성자에 history_size를 키워 넘겨라.")
        i = self.cursor + idx  # idx is negative (-1 = latest)
        if i < 0:
            return None
        v = self.seq[i]
        return None if v != v else v


class VectorizedRecorder(Recorder):
    """자본 곡선을 루프 후에 재구성하는 레코더.

    ``effective_from``(그 체결 상태가 처음 적용되는 이벤트 인덱스)이 **이벤트 카운터 그
    자체**다: 미체결 주문 체결은 그 이벤트의 :meth:`record_event` **전에**, 시장가 체결은
    **후에** 도착하므로, 체결 시점에 지금까지 기록된 이벤트 수를 읽기만 하면 된다. 참조 경로가
    자본을 미체결 체결 뒤·시장가 체결 앞에서 재는 것과 정확히 맞물린다.

    :param loop_indicator_names: 심볼별로 루프 갱신되는(=미리 계산되지 않은) 지표 이름. 이
        컬럼만 결정 시점에 이벤트 그리드 위에서 수집하면 되고, 나머지는 미리 계산된 배열을
        흩뿌리면 된다.
    """

    def __init__(self, streamer: BaseStreamer, status: Status, n_events: int,
                 loop_indicator_names: Dict[str, List[str]], collect_series: bool):
        self._streamer = streamer
        self._status = status
        self._loop_names = loop_indicator_names
        self.trades: List[Trade] = []
        self.max_leverage = 0.0
        self.trade_marks: List[TradeMark] = []
        self._n_events = 0
        self.loop_series: Dict[str, Dict[str, List[Optional[float]]]] = {
            symbol: {name: [None] * n_events for name in names}
            for symbol, names in loop_indicator_names.items() if names
        } if collect_series else {}

    def record_event(self, event_time: int, equity: float,
                     candles: Dict[str, Candle]) -> None:
        idx = self._n_events
        for symbol in candles:
            series = self.loop_series.get(symbol)
            if not series:
                continue
            indicators = self._streamer.indicators.get(symbol, {})
            for name in self._loop_names[symbol]:
                series[name][idx] = indicators[name].get_latest()
        self._n_events += 1

    def record_trade(self, trade: Trade) -> None:
        self.trades.append(trade)
        if trade.leverage > self.max_leverage:
            self.max_leverage = trade.leverage
        self.trade_marks.append((
            self._n_events,
            self._status.margin,
            {s: (p.position, p.avg_price) for s, p in self._status.positions.items()},
        ))


def run_vectorized(streamer: BaseStreamer, producer: BacktestCandleProducer, status: Status,
                   fee_ratio: float, slippage_ratio: float, entry_equity: float, *,
                   indicator_names: List[str], has_ohlc: bool,
                   shard_dir: Optional[str], run_id: str) -> Tuple[Report, List[Dict]]:
    """벡터화 경로로 백테스트를 돌리고 ``(Report, 샤드 인덱스)``를 돌려준다.

    ``producer``는 ``materialize=True``로 만들어져 있어야 한다 — 심볼별 캔들 배열과 이벤트
    인덱스 맵이 필요하다.
    """
    symbols = list(streamer.symbols)
    events = producer.events
    if events is None:
        raise ValueError("벡터화 경로는 materialize=True인 producer가 필요하다")
    n_events = len(events)
    event_times = np.fromiter((t for t, _ in events), dtype=np.int64, count=n_events)

    # 심볼별로 "자기 몇 번째 캔들이 어느 이벤트 인덱스에 있는지" — 지표 배열과 종가를 이벤트
    # 그리드에 흩뿌리는 데 쓴다.
    event_index_by_symbol: Dict[str, List[int]] = {s: [] for s in symbols}
    candles_by_symbol: Dict[str, List[Candle]] = {s: [] for s in symbols}
    for i, (_, batch) in enumerate(events):
        for symbol, candle in batch.items():
            if symbol in event_index_by_symbol:
                event_index_by_symbol[symbol].append(i)
                candles_by_symbol[symbol].append(candle)

    arrays: Dict[str, Dict[str, np.ndarray]] = {}
    for symbol in symbols:
        sub = candles_by_symbol[symbol]
        n = len(sub)
        arrays[symbol] = {
            "open": np.fromiter((c.open for c in sub), np.float64, n),
            "high": np.fromiter((c.high for c in sub), np.float64, n),
            "low": np.fromiter((c.low for c in sub), np.float64, n),
            "close": np.fromiter((c.close for c in sub), np.float64, n),
            "volume": np.fromiter((c.volume for c in sub), np.float64, n),
        }

    # 지표를 심볼별로 나눈다: 벡터화 가능한 것은 배열 심으로, 나머지는 루프 갱신 그대로.
    original_indicators = streamer.indicators
    run_indicators: Dict[str, Dict[str, BaseIndicator]] = {s: {} for s in symbols}
    precomputed: Dict[str, Dict[str, np.ndarray]] = {s: {} for s in symbols}
    loop_names: Dict[str, List[str]] = {s: [] for s in symbols}
    for symbol in symbols:
        a = arrays[symbol]
        for name, indicator in original_indicators.get(symbol, {}).items():
            precompute = getattr(indicator, "precompute_series", None)
            if precompute is not None:
                seq = precompute(a["open"], a["high"], a["low"], a["close"], a["volume"])
                precomputed[symbol][name] = seq
                run_indicators[symbol][name] = ArrayIndicator(
                    seq, indicator.window, indicator.history_size)
            else:
                loop_names[symbol].append(name)
                run_indicators[symbol][name] = indicator

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
            for k in _OHLC_KEYS:
                g = np.full(n_events, np.nan, dtype=np.float64)
                if len(idxs) > 0:
                    g[idxs] = a[k]
                # 실제 구멍은 그대로 둔다 (직전값으로 채우지 않는다) — 그 심볼의 캔들이 없는
                # 이벤트에서는 OHLC를 null로 남겨 프론트가 그 구간을 건너뛰게 한다.
                oh[k] = g
            ohlc_on_grid[symbol] = oh

    collect_series = shard_dir is not None
    recorder = VectorizedRecorder(streamer, status, n_events, loop_names,
                                  collect_series and any(loop_names.values()))
    executor = SimulatedExecutor(status, fee_ratio, slippage_ratio,
                                 on_trade=recorder.record_trade)

    # 첫 거래 이전 구간의 자본을 되살리려면 진입 시점의 실제 상태가 필요하다.
    entry_margin = status.margin
    entry_positions = {s: (p.position, p.avg_price) for s, p in status.positions.items()}

    streamer.indicators = run_indicators
    try:
        TradingEngine(streamer, executor, recorder).run(producer)
    finally:
        streamer.indicators = original_indicators

    equity_curve = build_equity_curve(event_times, close_on_grid, recorder.trade_marks,
                                      entry_margin, entry_positions)
    benchmark_curve = build_multi_symbol_buy_and_hold_curve(
        event_times, close_on_grid, entry_equity)
    report = Report(recorder.trades, recorder.max_leverage, status,
                    equity_curve, benchmark_curve)

    shards: List[Dict] = []
    if collect_series:
        symbol_columns: Dict[str, Dict[str, Optional[Dict[str, object]]]] = {}
        for symbol in symbols:
            symbol_columns[symbol] = {
                "indicators": build_symbol_series_columns(
                    indicator_names, precomputed[symbol],
                    recorder.loop_series.get(symbol, {}), n_events,
                    event_index_by_symbol[symbol]),
                "ohlc": ohlc_on_grid.get(symbol) if has_ohlc else None,
            }
        balance = np.fromiter((v for _, v in equity_curve), dtype=np.float64, count=n_events)
        shards = write_series_shards(shard_dir, run_id, event_times, balance,
                                     symbol_columns, producer.interval_ms, has_ohlc)
    return report, shards


def build_equity_curve(event_times: np.ndarray, close_on_grid: Dict[str, np.ndarray],
                       trade_marks: List[TradeMark], entry_margin: float,
                       entry_positions: Dict[str, Tuple[float, float]]
                       ) -> List[Tuple[int, float]]:
    """이벤트별 자본 곡선을 구간별로 재구성한다.

    체결 사이에는 margin과 모든 심볼의 (포지션, 평단)이 고정이므로, 한 구간은
    ``margin + Σ_sym position_sym * (close_sym[구간] - avg_price_sym)``이다.

    첫 거래 이전 구간은 **진입 시점의 실제 상태**로 시드한다 (포지션을 들고 시작하는 경우).
    """
    n = len(event_times)
    if n == 0:
        return []
    equity = np.empty(n, dtype=np.float64)
    seg_start = 0
    margin = entry_margin
    positions = entry_positions
    for effective_from, m, pos_snapshot in trade_marks:
        seg_end = effective_from
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


def build_symbol_series_columns(indicator_names: List[str],
                                precomputed_sym: Dict[str, np.ndarray],
                                loop_series_sym: Dict[str, List[Optional[float]]],
                                n_events: int,
                                event_indices_sym: List[int]) -> Dict[str, object]:
    """한 심볼의 지표 컬럼을 결정 시점 값으로 조립해 이벤트 그리드에 흩뿌린다.

    ``precomputed_sym[name][j]``는 그 심볼의 j+1번째 캔들 반영 후 값이자 그 캔들의 결정이 본
    값이므로(모든 지표가 decide_action보다 먼저 갱신된다) 시프트 없이 그대로 쓴다. 루프 컬럼은
    결정 시점에 이미 이벤트 그리드 위에서 수집됐다.
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
        elif name in loop_series_sym:
            columns[name] = loop_series_sym[name]
        else:
            columns[name] = [None] * n_events
    return columns
