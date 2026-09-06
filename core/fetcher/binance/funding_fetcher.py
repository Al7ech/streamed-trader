"""Binance USDT-M 선물 펀딩레이트 이력 로더 (data.binance.vision 월간 zip).

vision_fetcher와 같은 소스의 fundingRate 덤프를 월 단위로 받아
`asset/funding/{symbol}/{YYYY-MM}.pkl`에 캐시한다. 아직 발행되지 않은 월(404)은
빈 리스트로 취급하고 캐시하지 않는다 (다음 달 발행 후 자동 채워짐).
"""
import csv
import io
import os
import pickle
import zipfile
from datetime import datetime
from typing import List, Tuple

import requests

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/fundingRate"
DEFAULT_CACHE_DIR = "asset/funding"


def _iter_months(start: datetime, end: datetime):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def _fetch_month(symbol: str, year: int, month: int) -> List[Tuple[int, float]]:
    url = f"{BASE_URL}/{symbol}/{symbol}-fundingRate-{year:04d}-{month:02d}.zip"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()

    entries = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open(zf.namelist()[0]) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
            for row in reader:
                # 헤더 행 skip: calc_time,funding_interval_hours,last_funding_rate
                if len(row) < 3 or not row[0].isdigit():
                    continue
                calc_time = int(row[0])
                if calc_time >= 10 ** 14:  # microseconds → ms (2025~ vision 규칙)
                    calc_time //= 1000
                entries.append((calc_time, float(row[2])))
    return entries


def get_funding_with_cache(symbol: str, start: datetime, end: datetime,
                           cache_dir: str = DEFAULT_CACHE_DIR) -> List[Tuple[int, float]]:
    """[start, end) 범위의 (calc_time_ms, rate) 오름차순 리스트."""
    sym_dir = os.path.join(cache_dir, symbol)
    os.makedirs(sym_dir, exist_ok=True)

    entries: List[Tuple[int, float]] = []
    for year, month in _iter_months(start, end):
        path = os.path.join(sym_dir, f"{year:04d}-{month:02d}.pkl")
        if os.path.exists(path):
            with open(path, "rb") as f:
                month_entries = pickle.load(f)
        else:
            month_entries = _fetch_month(symbol, year, month)
            if month_entries:  # 미발행 월은 캐시하지 않음
                with open(path, "wb") as f:
                    pickle.dump(month_entries, f)
        entries.extend(month_entries)

    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    return sorted(e for e in entries if start_ms <= e[0] < end_ms)
