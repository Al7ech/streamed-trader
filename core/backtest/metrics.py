import logging
import math
from typing import Dict, List, Sequence, Tuple

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


def build_buy_and_hold_curve(times: Sequence[int], closes: Sequence[float],
                             init_margin: float) -> List[Tuple[int, float]]:
    """기초자산을 첫 캔들 종가에 전액 매수해 끝까지 보유했을 때의 자산 곡선.

    ``init_margin * close_i / close_0`` — 전략 equity_curve 와 같은 단위(둘 다 init_margin 에서
    시작)이자 같은 길이/타임스탬프라, 프론트에서 별도 정규화 없이 같은 가격축에 겹쳐 그릴 수 있고
    ``_downsample_equity`` 를 그대로 재사용해도 두 곡선의 샘플 시각이 정확히 일치한다.

    :param times: 캔들별 타임스탬프(ms). equity_curve 의 타임스탬프와 같아야 한다.
    :param closes: 같은 길이의 종가 배열 (list/numpy 모두 허용).
    :param init_margin: 시작 자산.
    :return: (timestamp_ms, equity) 리스트. 데이터가 없거나 첫 종가가 0 이하면 빈 리스트.
    """
    times_arr = np.asarray(times, dtype=np.int64)
    closes_arr = np.asarray(closes, dtype=np.float64)
    n = len(times_arr)
    if n == 0 or len(closes_arr) != n:
        return []
    first_close = float(closes_arr[0])
    if not (first_close > 0) or not (init_margin > 0):
        return []
    values = closes_arr * (init_margin / first_close)
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
