import hashlib
import logging
import math
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np


def compute_sharpe(equity_curve: List[Tuple[int, float]], sample_every: int = 24 * 60,
                   periods_per_year: float = 365.0) -> float:
    """
    :param equity_curve: chronological list of (timestamp_ms, equity) pairs sampled once per candle.
    :param sample_every: resample stride in candles (default 1440 = daily for 1m candles) —
        per-candle returns are mostly zero-noise, so Sharpe is computed on resampled returns.
    :param periods_per_year: how many resampled periods fit in a year, used for annualisation.
    :return: annualised Sharpe ratio (risk-free rate assumed 0). 0.0 if not enough data or flat equity.
    """
    samples = [eq for _, eq in equity_curve[::sample_every]]
    if equity_curve and equity_curve[-1][1] != samples[-1]:
        samples.append(equity_curve[-1][1])

    # 계좌가 0 이하로 죽으면 그 이후 구간은 의미가 없다. 예전에는 (b-a)/a 계산에서 a <= 0 인
    # 쌍만 조용히 걸러내 **죽은 계좌의 Sharpe를 살아남은 표본으로** 계산했다. 여기서 잘라내고
    # 경고를 남겨, 파산이 수치에 묻히지 않게 한다.
    dead = next((i for i, eq in enumerate(samples) if eq <= 0), None)
    if dead is not None:
        logging.getLogger(__name__).warning(
            "compute_sharpe: equity가 %d번째 표본에서 0 이하가 됐다 — 그 지점에서 잘라 계산한다", dead)
        samples = samples[: dead + 1]

    if len(samples) < 2:
        return 0.0

    returns = [(b - a) / a for a, b in zip(samples, samples[1:]) if a > 0]
    if len(returns) < 2:
        return 0.0

    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    if var == 0:
        return 0.0
    return mean / math.sqrt(var) * math.sqrt(periods_per_year)


def forward_fill_nan(arr: np.ndarray) -> np.ndarray:
    """직전 유효값으로 NaN 구간을 채운다. 선행 NaN은 그대로 남는다.

    ``build_multi_symbol_buy_and_hold_curve``가 심볼별 종가를 병합 이벤트 그리드에 흩뿌린 뒤
    채워 넣는 데 쓴다 (그 심볼의 캔들이 없는 이벤트는 직전 알려진 종가를 써야 하므로).
    """
    idx = np.where(~np.isnan(arr), np.arange(len(arr)), 0)
    np.maximum.accumulate(idx, out=idx)
    return arr[idx]


def build_multi_symbol_buy_and_hold_curve(
        times: Sequence[int], closes_by_symbol: Dict[str, Sequence[Optional[float]]],
        init_margin: float) -> List[Tuple[int, float]]:
    """동일가중 포트폴리오 buy & hold — 각 심볼이 자기 첫 유효 종가 시점에 init_margin/N씩
    배분받아 그 시점부터 끝까지 보유했을 때의 합산 자산 곡선.

    상장 시점이 다른 심볼(늦게 시작하거나 구간에 구멍이 있는)도 다룬다: 그 심볼의 몫은
    첫 유효 종가가 나타나는 시점부터 곡선에 반영되고, 그 전에는 0으로 취급된다 — 총
    투입 자본이 심볼이 하나씩 합류할 때마다 계단식으로 늘어난다는 뜻이다. 심볼이 하나뿐이면
    옛 단일 심볼 곡선(``init_margin * close_i / close_0``)과 같은 결과를 낸다.

    :param times: equity_curve와 같은 길이/타임스탬프의 이벤트 시각.
    :param closes_by_symbol: 심볼별 종가 시퀀스, times와 같은 길이. 그 심볼의 캔들이 아직
        마감하지 않은 인덱스는 None (내부 구멍은 이 함수가 직전값으로 채운다).
    :param init_margin: 시작 자산 (심볼 수만큼 균등 분할).
    """
    times_arr = np.asarray(times, dtype=np.int64)
    n = len(times_arr)
    symbols = list(closes_by_symbol.keys())
    if n == 0 or not symbols or not (init_margin > 0):
        return []

    alloc = init_margin / len(symbols)
    values = np.zeros(n, dtype=np.float64)
    for sym in symbols:
        raw = closes_by_symbol[sym]
        closes = (raw.astype(np.float64) if isinstance(raw, np.ndarray) else
                 np.asarray([v if v is not None else np.nan for v in raw], dtype=np.float64))
        if len(closes) != n:
            continue
        valid = ~np.isnan(closes)
        if not valid.any():
            continue
        first_idx = int(np.argmax(valid))
        basis = closes[first_idx]
        if not (basis > 0):
            continue
        filled = forward_fill_nan(closes)
        values[first_idx:] += alloc * filled[first_idx:] / basis

    return list(zip(times_arr.tolist(), values.tolist()))


def compute_max_drawdown(equity_curve: List[Tuple[int, float]]) -> Dict[str, float]:
    """
    :param equity_curve: chronological list of (timestamp_ms, equity) pairs, where equity is
        typically status.total_margin() (margin + unrealised_pnl) sampled once per candle.
    :return: dict with keys:
        max_drawdown   - peak-to-trough drawdown ratio (0..1)
        peak_timestamp / peak_equity
        trough_timestamp / trough_equity
      Returns an all-zero dict if equity_curve is empty.
    """
    if not equity_curve:
        return {"max_drawdown": 0.0, "peak_timestamp": 0, "peak_equity": 0.0,
                "trough_timestamp": 0, "trough_equity": 0.0}

    peak_ts, peak_eq = equity_curve[0]
    best = {"max_drawdown": 0.0, "peak_timestamp": peak_ts, "peak_equity": peak_eq,
            "trough_timestamp": peak_ts, "trough_equity": peak_eq}

    for ts, eq in equity_curve:
        if eq > peak_eq:
            peak_ts, peak_eq = ts, eq
            continue
        if peak_eq <= 0:
            continue
        dd = (peak_eq - eq) / peak_eq
        if dd > best["max_drawdown"]:
            best = {"max_drawdown": dd, "peak_timestamp": peak_ts, "peak_equity": peak_eq,
                    "trough_timestamp": ts, "trough_equity": eq}

    return best


def top_drawdowns(equity_curve: List[Tuple[int, float]], k: int = 5) -> List[float]:
    """겹치지 않는 드로다운 에피소드(고점 갱신으로 구분)별 최대 낙폭 상위 k개, %.

    MDD는 max 통계라 1위 에피소드 하나만 깎아도 개선으로 보인다. 위험이 줄었는지 재분배됐는지
    보려면 상위 몇 개를 같이 봐야 한다.
    """
    if not equity_curve:
        return []
    eq = np.fromiter((v for _, v in equity_curve), np.float64, len(equity_curve))
    peak = np.maximum.accumulate(eq)
    with np.errstate(divide="ignore", invalid="ignore"):
        dd = np.where(peak > 0, 1 - eq / peak, 0.0)
    new_peak = np.r_[True, peak[1:] > peak[:-1]]
    ep_max = np.maximum.reduceat(dd, np.flatnonzero(new_peak))
    return sorted((ep_max * 100).tolist(), reverse=True)[:k]


def trades_digest(trades) -> str:
    """체결 목록의 지문 — ``(timestamp, symbol, quantity, price)``의 sha1. 두 실행이 같은
    체결을 냈는지 요약 dict 하나로 비교할 때 쓴다 (``Report``를 통째로 옮기지 않고)."""
    h = hashlib.sha1()
    for t in trades:
        h.update(repr((t.timestamp, t.symbol, t.quantity, t.price)).encode())
    return h.hexdigest()


def summarise_report(report, init_margin: float) -> Dict[str, float]:
    """스윕 한 칸의 표준 요약. ``report``는 :class:`~core.account.report.Report`.

    - ``log_growth``: ln(최종/초기). MDD를 맞춘 프런티어 비교는 이 값으로 보간한다.
    - ``closes``: 포지션을 줄이는 체결 수 (``pre_position``과 부호가 반대인 체결).
    - ``fees``: 수수료 합. ``Trade.wnl``은 수수료를 빼지 않은 총액이라 따로 둔다.
    - ``trades_digest``: :func:`trades_digest`.
    """
    eq = report.equity_curve
    final = eq[-1][1] if eq else init_margin
    years = (eq[-1][0] - eq[0][0]) / 1000 / 86400 / 365.25 if len(eq) > 1 else 0.0
    mdd = compute_max_drawdown(eq)["max_drawdown"] * 100
    if final <= 0:
        cagr = -100.0
    elif years > 0:
        cagr = ((final / init_margin) ** (1 / years) - 1) * 100
    else:
        cagr = 0.0
    top = top_drawdowns(eq)
    return {
        "profit_pct": (final / init_margin - 1) * 100,
        "log_growth": math.log(max(final, 1e-9) / init_margin),
        "cagr_pct": cagr,
        "mdd_pct": mdd,
        "top5_dd_avg": float(np.mean(top)) if top else 0.0,
        "top5_dd": top,
        "mar": cagr / mdd if mdd > 0 else float("nan"),
        "sharpe": compute_sharpe(eq),
        "fills": len(report.trades),
        "closes": sum(1 for t in report.trades if t.pre_position * t.quantity < 0),
        "fees": sum(t.fee for t in report.trades),
        "max_leverage": report.max_leverage,
        "trades_digest": trades_digest(report.trades),
    }
