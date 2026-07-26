"""Binance USDT-M 선물 metrics(OI/롱숏비율) 이력 로더 (data.binance.vision 일별 zip).

funding_fetcher와 같은 소스의 metrics 덤프(5분 스냅샷, 2021-12-01~)를 일 단위로 받아
월 단위로 묶어 `asset/metrics/{symbol}/{YYYY-MM}.pkl`에 캐시한다.
결측일(404)은 skip하고, 진행 중인 월은 캐시하지 않는다.

행 스키마: (ts_ms, sum_open_interest, sum_open_interest_value,
            count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio,
            count_long_short_ratio, sum_taker_long_short_vol_ratio)
"""
import calendar
import csv
import io
import os
import pickle
import zipfile
from datetime import datetime, timezone
from typing import List, Tuple

import requests

BASE_URL = "https://data.binance.vision/data/futures/um/daily/metrics"
DEFAULT_CACHE_DIR = "asset/metrics"

MetricsRow = Tuple[int, float, float, float, float, float, float]


def _iter_months(start: datetime, end: datetime):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def _parse_ts(s: str) -> int:
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _fetch_day(symbol: str, year: int, month: int, day: int) -> List[MetricsRow]:
    url = f"{BASE_URL}/{symbol}/{symbol}-metrics-{year:04d}-{month:02d}-{day:02d}.zip"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()

    rows: List[MetricsRow] = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open(zf.namelist()[0]) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
            for row in reader:
                if len(row) < 8 or row[0] == "create_time":
                    continue
                try:
                    rows.append((_parse_ts(row[0]), float(row[2]), float(row[3]),
                                 float(row[4]), float(row[5]), float(row[6]), float(row[7])))
                except ValueError:
                    continue  # 드물게 빈 필드가 있는 행은 버림
    return rows


def _fetch_month(symbol: str, year: int, month: int) -> List[MetricsRow]:
    rows: List[MetricsRow] = []
    missing = []
    for day in range(1, calendar.monthrange(year, month)[1] + 1):
        day_rows = _fetch_day(symbol, year, month, day)
        if not day_rows:
            missing.append(day)
        rows.extend(day_rows)
    if missing:
        print(f"  {symbol} {year:04d}-{month:02d}: 결측일 {missing}", flush=True)
    return rows


def get_metrics_with_cache(symbol: str, start: datetime, end: datetime,
                           cache_dir: str = DEFAULT_CACHE_DIR) -> List[MetricsRow]:
    """[start, end) 범위의 metrics 행 ts 오름차순 리스트. 데이터는 2021-12-01부터 존재."""
    sym_dir = os.path.join(cache_dir, symbol)
    os.makedirs(sym_dir, exist_ok=True)
    now = datetime.now(timezone.utc)

    rows: List[MetricsRow] = []
    for year, month in _iter_months(start, end):
        path = os.path.join(sym_dir, f"{year:04d}-{month:02d}.pkl")
        if os.path.exists(path):
            with open(path, "rb") as f:
                month_rows = pickle.load(f)
        else:
            print(f"Fetching metrics {symbol} {year:04d}-{month:02d}...", flush=True)
            month_rows = _fetch_month(symbol, year, month)
            in_progress = (year, month) == (now.year, now.month)
            if month_rows and not in_progress:
                with open(path, "wb") as f:
                    pickle.dump(month_rows, f)
        rows.extend(month_rows)

    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    return sorted(r for r in rows if start_ms <= r[0] < end_ms)
