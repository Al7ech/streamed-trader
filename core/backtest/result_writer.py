"""JSON result writer for backtest runs.

Replaces the old CSV output. A single run produces:

- ``<result_path>/backtest/<run_id>.json`` — small "run" file with structured metadata,
  summary metrics (Sharpe / max-drawdown / win-rate / profit) and the (small) trade list,
  plus an index of the heavy time-series shards.
- ``<result_path>/backtest/<run_id>.<YYYY-MM>.series.json`` — one month-bucketed columnar
  shard per month, holding per-candle OHLC and indicator values. These are streamed out
  during the backtest (see :class:`ShardWriter`) so they never all sit in memory at once, and
  are loaded lazily/per-viewport by the frontend.

Columnar (parallel arrays) encoding is used for the shards so column keys are not repeated per
candle — this keeps them smaller than the old per-row CSV and faster to parse.
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

import numpy as np

from core.backtest.metrics import compute_max_drawdown, compute_sharpe
from core.backtest.report import Report

SCHEMA_VERSION = 1

# Columns whose empty/warm-up value should be stored as JSON null.
_OHLC_KEYS = ("open", "high", "low", "close")


def _month_key(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m")


class ShardWriter:
    """Streams per-candle OHLC + indicator values into month-bucketed columnar JSON shards.

    Only the current month's columns are held in memory; a shard file is flushed whenever the
    month rolls over (and once more on :meth:`close`). :meth:`close` returns the shard index
    (``[{"file", "start", "end"}, ...]``) to embed in the run JSON.
    """

    def __init__(self, dir_path: str, run_id: str, indicator_names: List[str],
                 has_ohlc: bool, interval_ms: int):
        self.dir_path = dir_path
        self.run_id = run_id
        self.indicator_names = indicator_names
        self.has_ohlc = has_ohlc
        self.interval_ms = interval_ms
        self.shards: List[Dict] = []
        self._month: Optional[str] = None
        self._reset_buffers()

    def _reset_buffers(self) -> None:
        self._time: List[int] = []
        self._ohlc: Dict[str, List[float]] = {k: [] for k in _OHLC_KEYS}
        self._ind: Dict[str, List[Optional[float]]] = {name: [] for name in self.indicator_names}

    def add(self, candle, indicator_values: Dict[str, Optional[float]]) -> None:
        key = _month_key(candle.end_time)
        if self._month is None:
            self._month = key
        elif key != self._month:
            self._flush()
            self._month = key

        self._time.append(candle.end_time)
        if self.has_ohlc:
            self._ohlc["open"].append(candle.open)
            self._ohlc["high"].append(candle.high)
            self._ohlc["low"].append(candle.low)
            self._ohlc["close"].append(candle.close)
        for name in self.indicator_names:
            v = indicator_values.get(name)
            # Donchian/MA indicators return a falsy value before their window is warmed up;
            # store null so the frontend can skip those points (matches the old CSV blanks).
            self._ind[name].append(v if v else None)

    def _flush(self) -> None:
        if not self._time:
            return
        file_name = f"{self.run_id}.{self._month}.series.json"
        shard = {
            "start": self._time[0],
            "end": self._time[-1],
            "interval_ms": self.interval_ms,
            "time": self._time,
        }
        if self.has_ohlc:
            shard["ohlc"] = self._ohlc
        shard["indicators"] = self._ind
        with open(os.path.join(self.dir_path, file_name), "w") as f:
            json.dump(shard, f, separators=(",", ":"))
        self.shards.append({"file": file_name, "start": self._time[0], "end": self._time[-1]})
        self._reset_buffers()

    def close(self) -> List[Dict]:
        self._flush()
        return self.shards


def _clean_column(values: Sequence) -> List[Optional[float]]:
    """Falsy/NaN -> null, matching ShardWriter.add (warm-up blanks skipped by the frontend)."""
    if isinstance(values, np.ndarray):
        values = values.tolist()
    return [v if v and v == v else None for v in values]


def write_series_shards(dir_path: str, run_id: str, times: Sequence[int],
                        ohlc: Optional[Dict[str, Sequence[float]]],
                        indicators: Dict[str, Sequence], interval_ms: int) -> List[Dict]:
    """Bulk counterpart of :class:`ShardWriter`: writes the same month-bucketed columnar shard
    files from whole-run columns (numpy arrays or lists) in one pass after the backtest loop.

    ``indicators`` columns must already hold decide-time values (i.e. the value the streamer saw
    for that candle, which excludes the candle itself). Returns the same shard index as
    ``ShardWriter.close()``.
    """
    n = len(times)
    if n == 0:
        return []
    times_arr = np.asarray(times, dtype=np.int64)

    # month-start boundaries (ms) covering the run, then one searchsorted to split all shards
    first = datetime.fromtimestamp(int(times_arr[0]) / 1000, tz=timezone.utc)
    year, month = first.year, first.month
    last_ms = int(times_arr[-1])
    bounds: List[int] = []
    while True:
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
        bound_ms = int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp() * 1000)
        if bound_ms > last_ms:
            break
        bounds.append(bound_ms)
    splits = np.searchsorted(times_arr, bounds, side="left")
    starts = [0] + [int(s) for s in splits]
    ends = [int(s) for s in splits] + [n]

    shards: List[Dict] = []
    for lo, hi in zip(starts, ends):
        if lo == hi:
            continue
        month_times = times_arr[lo:hi].tolist()
        file_name = f"{run_id}.{_month_key(month_times[0])}.series.json"
        shard = {
            "start": month_times[0],
            "end": month_times[-1],
            "interval_ms": interval_ms,
            "time": month_times,
        }
        if ohlc is not None:
            shard["ohlc"] = {k: np.asarray(v[lo:hi], dtype=np.float64).tolist()
                             for k, v in ohlc.items()}
        shard["indicators"] = {name: _clean_column(col[lo:hi]) for name, col in indicators.items()}
        with open(os.path.join(dir_path, file_name), "w") as f:
            # dumps + write: json.dump streams via the pure-Python encoder, dumps uses the C one
            f.write(json.dumps(shard, separators=(",", ":")))
        shards.append({"file": file_name, "start": month_times[0], "end": month_times[-1]})
    return shards


def _downsample_equity(equity_curve: Sequence, max_points: int = 2000) -> Optional[Dict]:
    """Uniform-stride downsample of the per-candle equity curve (always keeping the last
    point) into a small columnar block for the run JSON — the frontend timeline sparkline
    needs the whole run's balance at once, while the accurate per-candle series lives in the
    lazily-loaded shards."""
    n = len(equity_curve)
    if n == 0:
        return None
    stride = max(1, -(-n // max_points))  # ceil(n / max_points)
    sampled = list(equity_curve[::stride])
    if sampled[-1][0] != equity_curve[-1][0]:
        sampled.append(equity_curve[-1])
    return {
        "time": [int(t) for t, _ in sampled],
        "value": [float(v) for _, v in sampled],
    }


def build_summary(report: Report, init_margin: float) -> Dict:
    """Compute the summary block from the in-memory Report (reuses metrics.py)."""
    wins = sum(1 for t in report.trades if t.wnl > 0)
    losses = sum(1 for t in report.trades if t.wnl < 0)
    total = wins + losses
    final_margin = report.status.total_margin()
    return {
        "max_leverage": report.max_leverage,
        "final_margin": final_margin,
        "profit_pct": (final_margin / init_margin - 1) * 100 if init_margin else 0.0,
        "win_trades": wins,
        "lose_trades": losses,
        "total_trades": total,
        "win_rate": (wins / total) if total else 0.0,
        "sharpe": compute_sharpe(report.equity_curve),
        "max_drawdown": compute_max_drawdown(report.equity_curve),
    }


def write_run_json(dir_path: str, run_id: str, report: Report, metadata: Dict,
                   shards: List[Dict], columns: List[str], column_groups: Dict[str, str],
                   has_ohlc: bool, interval_ms: int, init_margin: float) -> str:
    """Assemble and write the run JSON. Returns the written file path."""
    trades = [
        {
            "timestamp": t.timestamp,
            "quantity": t.quantity,
            "price": t.price,
            "wnl": t.wnl,
            "fee": t.fee,
            "margin": t.status.total_margin(),
            "leverage": t.leverage,
        }
        for t in report.trades
    ]

    doc = {
        "schema_version": SCHEMA_VERSION,
        "metadata": metadata,
        "summary": build_summary(report, init_margin),
        "equity": _downsample_equity(report.equity_curve),
        "trades": trades,
        "series": {
            "columns": columns,
            "column_groups": column_groups,
            "has_ohlc": has_ohlc,
            "interval_ms": interval_ms,
            "shards": shards,
        },
    }

    file_path = os.path.join(dir_path, f"{run_id}.json")
    with open(file_path, "w") as f:
        json.dump(doc, f, separators=(",", ":"))
    return file_path
