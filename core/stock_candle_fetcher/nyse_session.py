"""NYSE 거래 세션 캘린더.

미국 주식은 24/7이 아니므로 "캔들이 있어야 할 시각"을 캘린더에서 얻어야 한다.
pandas_market_calendars의 NYSE 캘린더는 휴장일과 조기폐장을 모두 반영한다
(조기폐장일은 정규장이 13:00 ET로 끝나면서 애프터마켓도 17:00 ET로 함께 단축된다).

프리/애프터마켓을 포함한 세션은 [04:00, 20:00) ET.
"""
import functools
from datetime import datetime
from typing import List, Tuple

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

from core.utils import interval_to_minutes

# pandas_market_calendars의 NYSE market_times 이름 (pre=04:00 ET, post=20:00 ET)
SESSION_START = "pre"
SESSION_END = "post"

MINUTES_PER_DAY = 1440


@functools.lru_cache(maxsize=1)
def _calendar():
    return mcal.get_calendar("NYSE")


def session_bounds(start: datetime, end: datetime) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """[start, end)와 겹치는 각 거래일의 (세션 시작, 세션 끝) UTC 경계."""
    schedule = _calendar().schedule(
        start_date=start.date(), end_date=end.date(),
        start=SESSION_START, end=SESSION_END)
    return list(zip(schedule[SESSION_START], schedule[SESSION_END]))


def candle_grid(start: datetime, end: datetime, interval: str) -> np.ndarray:
    """[start, end) 안에서 캔들이 존재해야 할 start_time(ms) 오름차순 배열.

    일봉 이상은 세션 안을 나누는 개념이 아니므로 지원하지 않는다 (거래일마다 반드시
    거래가 있어서 forward-fill 자체가 필요없다).
    """
    step_minutes = interval_to_minutes(interval)
    if step_minutes >= MINUTES_PER_DAY:
        raise ValueError(f"candle_grid는 일중(intraday) interval만 지원합니다: {interval}")

    step_ms = step_minutes * 60_000
    per_session = [
        np.arange(int(session_start.timestamp() * 1000),
                  int(session_end.timestamp() * 1000), step_ms)
        for session_start, session_end in session_bounds(start, end)
    ]
    if not per_session:
        return np.empty(0, dtype=np.int64)

    grid = np.concatenate(per_session)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    return grid[(grid >= start_ms) & (grid < end_ms)]


def expected_count(start: datetime, end: datetime, interval: str) -> int:
    """[start, end)에 있어야 할 캔들 수."""
    if interval_to_minutes(interval) >= MINUTES_PER_DAY:
        return len(session_bounds(start, end))
    return len(candle_grid(start, end, interval))


def last_closed_session_end(now: datetime) -> pd.Timestamp:
    """now 시점에 이미 끝난 마지막 세션의 종료 시각(UTC).

    Massive 무료 티어는 EOD라 진행 중인 세션의 데이터를 주지 않는다. 이 상한을 넘겨
    fetch하면 아직 존재하지 않는 구간이 빈 청크로 캐시에 굳어버린다.
    """
    # 세션은 자정을 넘길 수 있으므로(post = 익일 01:00 UTC) 며칠 여유를 두고 조회한다
    lookback = now - pd.Timedelta(days=10)
    ended = [session_end for _, session_end in session_bounds(lookback, now)
             if session_end <= now]
    if not ended:
        raise RuntimeError(f"{lookback} ~ {now} 사이에 완료된 NYSE 세션이 없습니다")
    return ended[-1]
