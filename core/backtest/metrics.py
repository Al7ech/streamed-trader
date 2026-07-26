import math
from typing import Dict, List, Tuple


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
