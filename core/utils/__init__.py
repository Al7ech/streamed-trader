import math
from datetime import datetime, timezone


def ms_timestamp_to_datetime(timestamp: int):
    return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)


def generate_dict_string(d: dict) -> str:
    return "[" + ",".join(f"{k}={v.get_latest()}" for k, v in d.items()) + "]"


def trunc_by_sign(x: float, n: int) -> float:
    """소수 ``n``자리에서 0 방향으로 자른다.

    곱셈 결과를 먼저 소수 6자리로 반올림한 뒤 자른다 — ``4.35 * 100``이
    ``434.99999999999994``라 그대로 내리면 한 단위가 통째로 깎인다. 주문 수량은 이 함수가
    아니라 ``status.rules_for(symbol).floor_qty``로 맞춘다 (심볼마다 단위가 다르다).
    """
    factor = 10 ** n
    scaled = round(x * factor, 6)
    if x >= 0:
        return math.floor(scaled) / factor  # 양수 → 내림
    else:
        return math.ceil(scaled) / factor  # 음수 → 올림


_suffix_map = {
    'm': 1,
    'h': 60,
    'd': 1440,
    'w': 10080,
    'M': 43200
}


def interval_to_minutes(interval: str) -> int:
    """Convert interval string to minutes."""
    return int(interval[:-1]) * _suffix_map[interval[-1]]


def ensure_utc(dt: datetime) -> datetime:
    """Ensure datetime is UTC timezone-aware.

    naive datetime은 **UTC로 해석한다**. 예전에는 ``dt.astimezone()``으로 로컬 타임존을
    가정해서, ``datetime(2024, 1, 1)`` 같은 흔한 인자가 머신의 UTC 오프셋만큼 밀린 구간을
    의미했다 (KST에서는 2023-12-31T15:00Z). 월 청크 경계까지 함께 밀렸다.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
