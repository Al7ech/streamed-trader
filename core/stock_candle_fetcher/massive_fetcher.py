"""Massive(구 Polygon.io) 미국 주식 aggregates fetcher.

Polygon.io는 2025-10-30에 Massive로 리브랜딩했다. api.polygon.io도 당분간 병행 지원되며
API 키는 양쪽에서 동일하게 동작한다.
"""
import json
import os
import time
from datetime import datetime
from typing import List, Optional

import requests

from core.candle_fetcher import BaseCandleFetcher
from core.streamer import Candle
from core.utils import ensure_utc, interval_to_minutes, ms_timestamp_to_datetime
from . import nyse_session


class MassiveStockFetcher(BaseCandleFetcher):
    """미국 주식 캔들 fetcher.

    - 가격은 split 조정(adjusted=true). 배당은 미조정이라 실제 체결가에 가깝다.
    - 프리/애프터마켓 포함 (aggregates는 장외를 기본 포함한다).
    - Massive는 거래가 없는 분에 봉을 만들지 않으므로 NYSE 세션 그리드에 맞춰
      forward-fill해 Binance 데이터와 동일한 연속성을 만든다.
    """

    BASE_URL = "https://api.massive.com"
    MAX_LIMIT = 50_000
    RETRIES = 3
    HTTP_TIMEOUT = 60

    # Basic(무료) 티어 기준. 유료 전환 시 생성자 인자로 상향.
    RATE_LIMIT_PER_MIN = 5

    # 액면분할 조회 경로 — 리브랜딩 과정에서 신/구 경로가 공존한다
    SPLIT_PATHS = ("/stocks/v1/splits", "/v3/reference/splits")

    _TIMESPANS = {
        "1m": (1, "minute"), "5m": (5, "minute"), "15m": (15, "minute"),
        "30m": (30, "minute"), "1h": (1, "hour"), "1d": (1, "day"),
    }

    def __init__(self, api_key: Optional[str] = None, save_path: str = "asset/",
                 rate_limit_per_min: Optional[int] = None):
        super().__init__(save_path=save_path)
        self.api_key = api_key or os.environ.get("MASSIVE_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Massive API 키가 없습니다. .env에 MASSIVE_API_KEY를 넣거나 api_key 인자로 전달하세요.")
        self.rate_limit_per_min = rate_limit_per_min or self.RATE_LIMIT_PER_MIN
        self.session = requests.Session()
        self._last_call = 0.0
        self._split_path: Optional[str] = None

    # --- HTTP ---

    def _throttle(self) -> None:
        """무료 티어는 5회/분이라 호출 간격을 벌려야 429를 안 맞는다."""
        if self.rate_limit_per_min <= 0:
            return
        wait = self._last_call + 60.0 / self.rate_limit_per_min - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _get(self, url: str, params: Optional[dict] = None) -> dict:
        for attempt in range(self.RETRIES):
            self._throttle()
            response = self.session.get(url, params=params, timeout=self.HTTP_TIMEOUT)
            if response.status_code == 429:
                backoff = 15 * 2 ** attempt
                self.logger.warning(f"rate limited (429), {backoff}s 대기 후 재시도")
                time.sleep(backoff)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"rate limit 재시도 {self.RETRIES}회 실패: {url}")

    # --- 캔들 ---

    def get_candles(self, symbol: str, start_date: datetime, end_date: datetime,
                    interval: str) -> List[Candle]:
        if interval not in self._TIMESPANS:
            raise ValueError(
                f"지원하지 않는 interval: {interval} (지원: {sorted(self._TIMESPANS)})")

        start_date = ensure_utc(start_date)
        end_date = ensure_utc(end_date)
        if end_date <= start_date:
            return []

        multiplier, timespan = self._TIMESPANS[interval]
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)

        # Massive의 range end는 inclusive라 1ms 당긴다
        url = (f"{self.BASE_URL}/v2/aggs/ticker/{symbol}/range/"
               f"{multiplier}/{timespan}/{start_ms}/{end_ms - 1}")
        params = {"adjusted": "true", "sort": "asc",
                  "limit": self.MAX_LIMIT, "apiKey": self.api_key}

        rows = []
        while url:
            data = self._get(url, params=params)
            rows.extend(data.get("results") or [])
            # next_url에는 apiKey가 붙어 있지 않다
            url = data.get("next_url")
            params = {"apiKey": self.api_key}

        interval_ms = interval_to_minutes(interval) * 60_000
        candles = [
            Candle(
                open=float(r["o"]), high=float(r["h"]), low=float(r["l"]),
                close=float(r["c"]), volume=float(r["v"]),
                start_time=int(r["t"]), end_time=int(r["t"]) + interval_ms,
                trade_count=int(r["n"]) if r.get("n") is not None else None,
                taker_buy_volume=None,  # Massive는 taker 매수량을 제공하지 않는다
            )
            for r in rows
        ]

        candles.sort(key=lambda c: c.start_time)
        unique_candles = []
        seen_times = set()
        for candle in candles:
            if candle.start_time not in seen_times:
                unique_candles.append(candle)
                seen_times.add(candle.start_time)

        if not unique_candles:
            self.logger.warning(f"no candles fetched for {symbol} {start_date} ~ {end_date}")
            return []

        if interval_to_minutes(interval) >= nyse_session.MINUTES_PER_DAY:
            return unique_candles  # 거래일마다 거래가 있으므로 채울 것이 없다
        return self._fill_session_grid(unique_candles, start_date, end_date, interval)

    def _fill_session_grid(self, candles: List[Candle], start_date: datetime,
                           end_date: datetime, interval: str) -> List[Candle]:
        """무거래 구간을 직전 종가로 채워 세션 그리드를 빈틈없이 만든다.

        Massive는 거래가 없는 분에 봉을 만들지 않는데, 장외시간은 무거래 분이 많다.
        그대로 두면 Donchian(4500)이 "4500분"이 아니라 종목마다 다른 시간 폭이 된다.
        합성봉은 O=H=L=C=직전 종가, volume=0, trade_count=0.
        """
        grid = nyse_session.candle_grid(start_date, end_date, interval)
        if len(grid) == 0:
            return []

        by_time = {c.start_time: c for c in candles}
        interval_ms = interval_to_minutes(interval) * 60_000

        filled: List[Candle] = []
        prev_close: Optional[float] = None
        synthetic = 0
        for ts in grid.tolist():
            candle = by_time.get(ts)
            if candle is None:
                if prev_close is None:
                    continue  # 첫 실거래 이전 — 채울 가격이 없다. 가격을 지어내지 않는다.
                candle = Candle(open=prev_close, high=prev_close, low=prev_close,
                                close=prev_close, volume=0.0, start_time=ts,
                                end_time=ts + interval_ms, trade_count=0)
                synthetic += 1
            prev_close = candle.close
            filled.append(candle)

        off_grid = len(candles) - (len(filled) - synthetic)
        if off_grid:
            self.logger.warning(
                f"{off_grid} candle(s) outside the NYSE session grid dropped "
                f"(late-reported trades)")
        if filled:
            s = ms_timestamp_to_datetime(filled[0].start_time)
            e = ms_timestamp_to_datetime(filled[-1].end_time)
            self.logger.info(
                f"fetched {len(filled)} candles: {s} ~ {e}, interval: {interval} "
                f"({synthetic} synthetic, {synthetic / len(filled) * 100:.1f}%)")
        return filled

    # --- BaseCandleFetcher hook ---

    def _cache_dir_name(self, symbol: str, interval: str) -> str:
        # split 조정 데이터를 자체 네임스페이스에 격리 — raw 변종이 생겨도 섞이지 않는다
        return f"{symbol}_{interval}_adj"

    def _expected_candles(self, month: datetime, month_end: datetime, interval: str) -> int:
        return nyse_session.expected_count(month, month_end, interval)

    def _data_available_until(self, now: datetime) -> datetime:
        # 무료 티어는 EOD라 진행 중인 세션의 데이터가 없다
        return nyse_session.last_closed_session_end(now).to_pydatetime()

    def _on_cache_open(self, symbol: str, interval: str, chunk_dir: str) -> None:
        """새 액면분할이 생기면 캐시를 버린다.

        캐시는 "지난 달은 불변"을 가정하지만 adjusted=true는 그 가정을 깬다. 분할 전에
        캐시된 달과 분할 후에 fetch한 달을 섞으면 시계열 중간에 가짜 갭이 생긴다.
        """
        meta_path = os.path.join(chunk_dir, "_meta.json")
        # 파일의 존재 자체가 "확인한 적 있음"을 뜻한다. 분할 이력이 없는 종목은 커서가
        # None이라 값만으로는 "확인했고 분할 없음"과 "한 번도 확인 안 함"을 구분할 수 없다.
        checked_before = os.path.exists(meta_path)
        cursor = None
        if checked_before:
            with open(meta_path) as f:
                cursor = json.load(f).get("last_split_date")

        latest = self._latest_split_date(symbol)
        changed = latest != cursor

        if checked_before and changed:
            stale = [n for n in os.listdir(chunk_dir) if n.endswith(".pkl")]
            for name in stale:
                os.remove(os.path.join(chunk_dir, name))
            self.logger.warning(
                f"{symbol}의 액면분할 이력이 {cursor} → {latest}로 바뀌어 조정 가격이 "
                f"소급 변경되었습니다 — 캐시 청크 {len(stale)}개를 삭제하고 다시 받습니다")

        if not checked_before or changed:
            with open(meta_path, "w") as f:
                json.dump({"last_split_date": latest}, f)

    def _latest_split_date(self, symbol: str) -> Optional[str]:
        """가장 최근 액면분할 execution_date (YYYY-MM-DD). 분할 이력이 없으면 None."""
        params = {"ticker": symbol, "limit": 1, "sort": "execution_date",
                  "order": "desc", "apiKey": self.api_key}
        paths = (self._split_path,) if self._split_path else self.SPLIT_PATHS

        for path in paths:
            try:
                data = self._get(f"{self.BASE_URL}{path}", params=params)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    continue  # 리브랜딩으로 신/구 경로가 공존 — 다음 경로 시도
                raise
            self._split_path = path
            results = data.get("results") or []
            return results[0].get("execution_date") if results else None

        raise RuntimeError(
            f"액면분할 조회 실패 — 시도한 경로: {self.SPLIT_PATHS}. "
            f"분할을 확인하지 못하면 조정 가격 캐시가 오염될 수 있어 중단합니다.")
