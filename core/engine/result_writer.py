"""JSON result writer for backtest runs.

Replaces the old CSV output. A single run produces:

- ``<result_path>/backtest/<run_id>.json`` — small "run" file with structured metadata,
  summary metrics (Sharpe / max-drawdown / win-rate / profit) and the (small) trade list,
  plus an index of the heavy time-series shards.
- ``<result_path>/backtest/<run_id>.<YYYY-MM>.series.json`` — one month-bucketed columnar
  shard per month, holding per-candle OHLC and indicator values, nested per symbol. These are
  streamed out during the backtest (see :class:`ShardWriter`) so they never all sit in memory
  at once, and are loaded lazily/per-viewport by the frontend.

Columnar (parallel arrays) encoding is used for the shards so column keys are not repeated per
candle — this keeps them smaller than the old per-row CSV and faster to parse.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
from boltons.fileutils import atomic_save

from core.engine.metrics import compute_max_drawdown, compute_sharpe
from core.engine.report import Report

# 2: added the top-level "benchmark" block (buy & hold curve) + summary.benchmark_profit_pct.
# 3: added trades[].position (거래 **전** 포지션). 라이브 런은 재기동 시 자기 run JSON을 다시
#    읽어 이어쓰는데, build_summary의 승패 판정이 그 심볼의 거래 전 포지션 부호를 보므로
#    (v4부터는 Trade.status.position_for(symbol).position) 이게 없으면 재개 후 승패 집계를
#    복원할 수 없다.
# 4: 멀티심볼 — trades[]에 "symbol", series에 "symbols"(거래 대상 심볼 목록) 추가. 샤드의
#    "ohlc"/"indicators"가 심볼별로 "symbols" 아래 중첩되고, "time"/"balance"(계좌 전체
#    시가평가)는 최상위에 남는다 (증거금이 심볼 간 공유 풀이라 balance는 심볼별 값이 아니다).
#    v1-v3 샤드의 평평한 ohlc/indicators 모양은 그대로 두고 건드리지 않는다 — 심볼 N개를
#    평평한 ohlc dict 하나에 욱여넣으면 어느 심볼의 close인지 알 수 없어져 addition이 아니라
#    네임스페이스 충돌이 되기 때문이다. summary에 by_symbol(심볼별 승패 집계)도 추가됐다.
# 5: 지정가/조건부 주문 — trades[]에 "order_type"(ActionType의 값)과 "submitted_at"(주문이
#    제출된 시각) 추가. MARKET은 submitted_at == timestamp지만, 지정가/조건부 주문은 몇 봉
#    전에 제출돼 나중에 체결되므로 timestamp 하나로는 결정 시점과 체결 시점을 함께 담을 수
#    없다. 샤드 모양은 v4 그대로다.
# v2-v3, v5는 순수 additive — 프론트는 없는 블록의 UI를 숨기고 키를 골라 읽으므로 구 런도
# 그대로 로드된다 (v4만 샤드의 물리적 모양을 바꿨다).
SCHEMA_VERSION = 5

# Columns whose empty/warm-up value should be stored as JSON null.
_OHLC_KEYS = ("open", "high", "low", "close")

_logger = logging.getLogger(__name__)


def _month_key(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m")


def _write_json(path: str, doc: Dict) -> None:
    """JSON을 **원자적으로** 쓴다.

    ``atomic_save``는 part 파일에 쓰고 fsync한 뒤 원자적으로 rename하며, 도중에 예외가 나면
    part를 지우고 원본을 건드리지 않는다. 라이브 레코더는 같은 파일을 캔들마다 덮어쓰므로
    중간에 죽어도 직전 런 JSON이 온전해야 하고, 백테스트도 Ctrl-C 시 깨진 샤드를 남기지 않게 된다.

    part 경로 기본값이 ``<dest>.part``라 ``.json``으로 끝나지 않는 것도 중요하다 — 프론트의
    디렉토리 스캐너는 ``*.json``을 전부 긁어가므로, 반쯤 쓰인 파일이 ``.json``이었다면 런 목록에
    깨진 항목으로 떴을 것이다.
    """
    with atomic_save(path, text_mode=True) as f:
        # dumps + write: json.dump은 순수 파이썬 인코더로 스트리밍하고 dumps는 C 인코더를 쓴다
        f.write(json.dumps(doc, separators=(",", ":")))


def read_shard(dir_path: str, file_name: str) -> Optional[Dict]:
    """샤드 파일을 읽어 dict로 돌려준다. 없거나 깨졌으면 None.

    라이브 런 재개 경로에서만 쓴다 — 마지막 샤드가 크래시로 유실됐을 수 있는데, 그 한 달치
    시계열을 잃는 것과 트레이더가 기동하지 못하는 것 중에서는 전자가 낫다.
    """
    path = os.path.join(dir_path, file_name)
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        _logger.warning("샤드를 읽지 못했다 (%s): %s", path, e)
        return None


def _empty_symbol_buffer(indicator_names: List[str]) -> Dict:
    return {
        "ohlc": {k: [] for k in _OHLC_KEYS},
        "indicators": {name: [] for name in indicator_names},
    }


class ShardWriter:
    """Streams per-event OHLC + indicator values into month-bucketed columnar JSON shards.

    한 "이벤트"는 하나 이상의 심볼이 같은 시각에 마감한 캔들들의 묶음이다 (멀티심볼 병합
    타임라인의 단위, :func:`core.engine.candle_merge.merge_candle_timeline` 참고). 이번
    이벤트에 캔들이 없는 심볼은 그 행에서 OHLC/지표가 전부 null이 된다.

    Only the current month's columns are held in memory; a shard file is flushed whenever the
    month rolls over (and once more on :meth:`close`). :meth:`close` returns the shard index
    (``[{"file", "start", "end"}, ...]``) to embed in the run JSON.

    라이브 레코더는 **같은 달 샤드를 여러 번** 써야 하므로 (프로세스가 몇 주씩 살아 있고 그
    사이 계속 체크포인트를 남긴다) 쓰기/인덱스 반영/버퍼 초기화가 분리돼 있다: :meth:`checkpoint`
    는 버퍼를 유지한 채 파일만 갱신하고, :meth:`_flush`는 거기에 버퍼 초기화를 더한 것이다.
    백테스트는 이벤트가 시간순이라 같은 달을 두 번 방문하지 않으므로 동작이 이전과 같다.
    """

    def __init__(self, dir_path: str, run_id: str, symbols: List[str],
                 indicator_names: List[str], has_ohlc: bool, interval_ms: int):
        self.dir_path = dir_path
        self.run_id = run_id
        self.symbols = symbols
        self.indicator_names = indicator_names
        self.has_ohlc = has_ohlc
        self.interval_ms = interval_ms
        self.shards: List[Dict] = []
        self._month: Optional[str] = None
        self._reset_buffers()

    @classmethod
    def resume(cls, dir_path: str, run_id: str, symbols: List[str], indicator_names: List[str],
               has_ohlc: bool, interval_ms: int, shards: List[Dict]) -> "ShardWriter":
        """이전 실행이 남긴 샤드 인덱스로 writer를 복원한다 (라이브 런 재개 전용).

        마지막 샤드를 버퍼로 되읽어, 이어지는 :meth:`add`가 같은 달이면 그 파일을 덮어쓰고
        달이 바뀌면 원본 그대로 flush한 뒤 새 달로 넘어간다. 마지막 샤드가 유실/손상이면
        그 달의 꼬리를 포기하고 (차트에 구멍이 남는다) 빈 버퍼로 이어간다.
        """
        writer = cls(dir_path, run_id, symbols, indicator_names, has_ohlc, interval_ms)
        if not shards:
            return writer
        writer.shards = list(shards[:-1])
        last = shards[-1]
        shard = read_shard(dir_path, last["file"])
        if not shard or not shard.get("time"):
            _logger.warning("마지막 샤드를 복원하지 못했다 — %s 구간은 비어 있게 된다",
                            last["file"])
            return writer
        writer._time = list(shard["time"])
        n = len(writer._time)
        writer._month = _month_key(writer._time[0])
        writer._balance = list(shard.get("balance") or [None] * n)
        saved_symbols = shard.get("symbols") or {}
        for sym in symbols:
            saved = saved_symbols.get(sym) or {}
            if has_ohlc:
                ohlc = saved.get("ohlc") or {}
                writer._symbols[sym]["ohlc"] = {k: list(ohlc.get(k) or [None] * n)
                                                for k in _OHLC_KEYS}
            # 지표 구성이 바뀐 채로 재개하는 것은 상위(LiveRecorder)에서 막지만, 방어적으로
            # 현재 컬럼 집합에 맞춰 정렬한다 — 없던 컬럼은 그 구간만 null이 된다.
            saved_ind = saved.get("indicators") or {}
            writer._symbols[sym]["indicators"] = {name: list(saved_ind.get(name) or [None] * n)
                                                  for name in indicator_names}
        return writer

    def _reset_buffers(self) -> None:
        self._time: List[int] = []
        self._balance: List[Optional[float]] = []
        self._symbols: Dict[str, Dict] = {sym: _empty_symbol_buffer(self.indicator_names)
                                          for sym in self.symbols}

    def add(self, time_ms: int, balance: float,
            symbol_data: Dict[str, Tuple[object, Dict[str, Optional[float]]]]) -> None:
        """이벤트 하나를 적재한다.

        :param time_ms: 이벤트 시각 (병합 타임라인의 end_time).
        :param balance: 이 이벤트 시점의 계좌 전체 시가평가 자본 (margin + Σ unrealised_pnl).
        :param symbol_data: 이번 이벤트에 캔들이 마감한 심볼만 담는다 — ``{symbol: (candle,
            indicator_values)}``. 나머지 심볼은 이 행에서 OHLC/지표가 전부 null이 된다.
        """
        key = _month_key(time_ms)
        if self._month is None:
            self._month = key
        elif key != self._month:
            self._flush()
            self._month = key

        self._time.append(time_ms)
        self._balance.append(_clean_value(balance))
        for sym in self.symbols:
            buf = self._symbols[sym]
            data = symbol_data.get(sym)
            candle = data[0] if data else None
            values = data[1] if data else {}
            if self.has_ohlc:
                for k in _OHLC_KEYS:
                    buf["ohlc"][k].append(getattr(candle, k) if candle is not None else None)
            for name in self.indicator_names:
                buf["indicators"][name].append(_clean_value(values.get(name)))

    def _write_current(self) -> Optional[Dict]:
        """현재 월 버퍼를 파일로 쓰고 인덱스 엔트리를 돌려준다 (버퍼는 유지). 비었으면 None."""
        if not self._time:
            return None
        file_name = f"{self.run_id}.{self._month}.series.json"
        shard = {
            "start": self._time[0],
            "end": self._time[-1],
            "interval_ms": self.interval_ms,
            "time": self._time,
            "balance": self._balance,
            "symbols": {
                sym: ({"ohlc": buf["ohlc"], "indicators": buf["indicators"]} if self.has_ohlc
                     else {"indicators": buf["indicators"]})
                for sym, buf in self._symbols.items()
            },
        }
        _write_json(os.path.join(self.dir_path, file_name), shard)
        return {"file": file_name, "start": self._time[0], "end": self._time[-1]}

    def _commit(self, entry: Dict) -> None:
        """같은 파일의 기존 엔트리를 갈아끼우고, 없으면 append.

        라이브는 같은 달을 여러 번 쓰므로 무조건 append하면 인덱스에 중복이 쌓인다. 백테스트는
        이벤트가 시간순이라 같은 달을 두 번 방문하지 않아 항상 append로 퇴화한다.
        """
        for i, s in enumerate(self.shards):
            if s["file"] == entry["file"]:
                self.shards[i] = entry
                return
        self.shards.append(entry)

    def checkpoint(self) -> None:
        """버퍼를 비우지 않고 현재 월 샤드를 디스크에 반영한다 (라이브 전용).

        인덱스는 파일 rename이 성공한 **뒤에** 갱신되므로, 인덱스가 존재하지 않는 파일을
        가리키는 상태는 생기지 않는다.
        """
        entry = self._write_current()
        if entry:
            self._commit(entry)

    def _flush(self) -> None:
        self.checkpoint()
        self._reset_buffers()

    def close(self) -> List[Dict]:
        self._flush()
        return self.shards


def _clean_value(v) -> Optional[float]:
    """Falsy/NaN -> null.

    Donchian/MA 류 지표는 워밍업 동안 falsy를 반환하고, 벡터화 경로는 NaN을 반환한다. 둘 다
    null로 눕혀야 프론트가 그 지점을 건너뛴다. NaN 검사(``v == v``)가 특히 중요한데,
    ``bool(float('nan'))``은 True라 그냥 두면 ``json.dump``가 bare ``NaN`` 토큰을 뱉고
    ``JSON.parse``가 그 샤드 전체를 거부한다.
    """
    return v if v and v == v else None


def _clean_column(values: Sequence) -> List[Optional[float]]:
    """Falsy/NaN -> null, matching ShardWriter.add (warm-up blanks skipped by the frontend)."""
    if isinstance(values, np.ndarray):
        values = values.tolist()
    return [_clean_value(v) for v in values]


def write_series_shards(dir_path: str, run_id: str, times: Sequence[int],
                        balance: Sequence[float],
                        symbol_columns: Dict[str, Dict[str, Optional[Dict[str, Sequence]]]],
                        interval_ms: int, has_ohlc: bool) -> List[Dict]:
    """Bulk counterpart of :class:`ShardWriter`: writes the same month-bucketed columnar shard
    files from whole-run columns (numpy arrays or lists) in one pass after the backtest loop.

    :param symbol_columns: ``{symbol: {"ohlc": {...} or None, "indicators": {...}}}``, every
        array the same length as ``times``. Indicator columns must already hold decide-time
        values (i.e. the value the streamer saw for that event, which includes the event's own
        candle for that symbol — every indicator ingests it before ``decide_action`` runs).
    :param has_ohlc: whether to embed OHLC per symbol (mirrors ``symbol_columns[*]["ohlc"]``
        being non-None).

    Returns the same shard index as ``ShardWriter.close()``.
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
            "balance": _clean_column(balance[lo:hi]),
            "symbols": {},
        }
        for sym, cols in symbol_columns.items():
            entry = {"indicators": {name: _clean_column(col[lo:hi])
                                    for name, col in cols["indicators"].items()}}
            if has_ohlc and cols.get("ohlc") is not None:
                entry["ohlc"] = {k: np.asarray(v[lo:hi], dtype=np.float64).tolist()
                                 for k, v in cols["ohlc"].items()}
            shard["symbols"][sym] = entry
        _write_json(os.path.join(dir_path, file_name), shard)
        shards.append({"file": file_name, "start": month_times[0], "end": month_times[-1]})
    return shards


def _downsample_equity(equity_curve: Sequence, max_points: int = 2000) -> Optional[Dict]:
    """Uniform-stride downsample of the per-event equity curve (always keeping the last
    point) into a small columnar block for the run JSON — the frontend timeline sparkline
    needs the whole run's balance at once, while the accurate per-event series lives in the
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


_MS_PER_DAY = 86_400_000


def _sharpe_sampling(interval_ms: int) -> Tuple[int, float]:
    """캔들 간격에서 Sharpe의 (리샘플 stride, 연간 기간 수)를 구한다.

    예전에는 stride가 1440으로 고정이라 **1분봉에서만** 일간 리샘플이었다. 1시간봉에서는
    1440캔들이 60일인데도 365 periods/yr로 연율화해 Sharpe가 약 7.7배 과대했고, 일봉에서는
    표본이 2개로 줄어 8년 미만 런의 Sharpe가 조용히 0.0이 됐다.

    간격이 하루보다 짧으면 하루 단위로 묶고, 하루 이상이면 캔들 자체가 표본이 된다.
    """
    if interval_ms <= 0:
        return 24 * 60, 365.0
    sample_every = max(1, round(_MS_PER_DAY / interval_ms))
    periods_per_year = 365.0 * _MS_PER_DAY / (sample_every * interval_ms)
    return sample_every, periods_per_year


def _win_lose_counts(trades) -> Tuple[int, int, int]:
    """승패는 **실현이 일어난 체결**만 센다. 순수 진입은 wnl=0인데 수수료는 붙으므로 그냥
    (wnl - fee)로 재면 진입이 전부 패배로 집계된다. 거래 전 스냅샷의 포지션과 체결 수량의
    부호가 반대면 축소 또는 방향 전환 = 실현이 발생한 체결이다."""
    closes = [t for t in trades if t.status.position_for(t.symbol).position * t.quantity < 0]
    wins = sum(1 for t in closes if t.wnl - t.fee > 0)
    losses = sum(1 for t in closes if t.wnl - t.fee < 0)
    return wins, losses, wins + losses


def build_summary(report: Report, init_margin: float, interval_ms: int = 0) -> Dict:
    """Compute the summary block from the in-memory Report (reuses metrics.py).

    ``report.benchmark_curve`` (buy & hold) adds ``benchmark_profit_pct`` — same unit as
    ``profit_pct`` (percent, x100), None when the run has no benchmark so the frontend can tell
    "no data" from "0%".

    ``by_symbol``은 심볼별 승패 집계를 추가로 담는다 — 위 aggregate 수치는 전 심볼 합산으로
    그대로 유지된다.

    ``interval_ms``는 Sharpe의 리샘플 간격을 캔들 간격에 맞추는 데 쓴다. 0이면 예전 기본값
    (1분봉 가정)으로 되돌아간다.
    """
    wins, losses, total = _win_lose_counts(report.trades)
    final_margin = report.status.total_margin()
    benchmark_profit_pct = (
        (report.benchmark_curve[-1][1] / init_margin - 1) * 100
        if report.benchmark_curve and init_margin
        else None
    )

    by_symbol: Dict[str, Dict] = {}
    trades_by_symbol: Dict[str, List] = {}
    for t in report.trades:
        trades_by_symbol.setdefault(t.symbol, []).append(t)
    for symbol, trades in trades_by_symbol.items():
        s_wins, s_losses, s_total = _win_lose_counts(trades)
        by_symbol[symbol] = {
            "win_trades": s_wins,
            "lose_trades": s_losses,
            "total_trades": s_total,
            "win_rate": (s_wins / s_total) if s_total else 0.0,
        }

    return {
        "max_leverage": report.max_leverage,
        "final_margin": final_margin,
        "profit_pct": (final_margin / init_margin - 1) * 100 if init_margin else 0.0,
        "benchmark_profit_pct": benchmark_profit_pct,
        "win_trades": wins,
        "lose_trades": losses,
        "total_trades": total,
        "win_rate": (wins / total) if total else 0.0,
        "by_symbol": by_symbol,
        "sharpe": compute_sharpe(report.equity_curve, *_sharpe_sampling(interval_ms)),
        "max_drawdown": compute_max_drawdown(report.equity_curve),
    }


def write_run_json(dir_path: str, run_id: str, report: Report, metadata: Dict,
                   shards: List[Dict], symbols: List[str], columns: List[str],
                   column_groups: Dict[str, str], has_ohlc: bool, interval_ms: int,
                   init_margin: float) -> str:
    """Assemble and write the run JSON. Returns the written file path.

    ``report.benchmark_curve`` (동일가중 포트폴리오 buy & hold, equity_curve와 같은 길이/
    타임스탬프)는 프론트가 시리즈 샤드를 불러오지 않고도 자본 곡선에 겹쳐 그릴 수 있도록
    별도 ``benchmark`` 블록으로 쓴다.
    """
    trades = [
        {
            "timestamp": t.timestamp,
            "symbol": t.symbol,
            "quantity": t.quantity,
            "price": t.price,
            "wnl": t.wnl,
            "fee": t.fee,
            "margin": t.status.total_margin(),
            # 거래 **전** 포지션. build_summary의 승패 판정이 이 부호를 보므로, 라이브 런이
            # 재기동 후 자기 run JSON에서 Trade를 복원할 때 이게 없으면 집계가 무너진다.
            "position": t.status.position_for(t.symbol).position,
            "leverage": t.leverage,
            # 이 체결을 낳은 주문 종류와 그 주문이 제출된 시각. 지정가/조건부 주문은 둘이
            # 갈라진다 — "이 청산은 손절 체결이었다"를 사후에 구분하려면 필요하다.
            "order_type": t.order_type,
            "submitted_at": t.submitted_at,
        }
        for t in report.trades
    ]

    doc = {
        "schema_version": SCHEMA_VERSION,
        "metadata": metadata,
        "summary": build_summary(report, init_margin, interval_ms),
        "equity": _downsample_equity(report.equity_curve),
        # same downsampler on an equal-length curve -> identical stride, so benchmark.time is
        # element-wise identical to equity.time and the frontend can pair them by index
        "benchmark": _downsample_equity(report.benchmark_curve),
        "trades": trades,
        "series": {
            "symbols": symbols,
            "columns": columns,
            "column_groups": column_groups,
            "has_ohlc": has_ohlc,
            "interval_ms": interval_ms,
            "shards": shards,
        },
    }

    file_path = os.path.join(dir_path, f"{run_id}.json")
    _write_json(file_path, doc)
    return file_path
