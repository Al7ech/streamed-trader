import concurrent.futures
import csv
import io
import time
import zipfile
from collections import namedtuple
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Tuple

import requests
from tqdm import tqdm

from core.streamer import Candle
from core.utils import ensure_utc, ms_timestamp_to_datetime
from .fetcher import BinanceCandleFetcher

# kind: "monthly" (day=None) or "daily"
FileSpec = namedtuple("FileSpec", ["kind", "year", "month", "day"])


class BinanceVisionFetcher(BinanceCandleFetcher):
    """
    data.binance.vision (바이낸스 공식 벌크 데이터 CDN)에서 월/일 단위 zip을 병렬 다운로드하는
    빠른 캔들 fetcher. 덤프에 아직 없는 최근 구간(오늘/미발행 일자)만 REST API로 보충한다.

    BinanceCandleFetcher를 상속하므로 get_candles_with_cache와 pkl 캐시 경로가 동일하다.
    """

    VISION_BASE = "https://data.binance.vision/data/futures/um"
    # data.binance.vision에 klines 디렉토리가 존재하는 interval (1M은 vision에서 "1mo"라 미지원)
    VISION_INTERVALS = {"1m", "3m", "5m", "15m", "30m",
                        "1h", "2h", "4h", "6h", "8h", "12h", "1d", "1w"}
    MAX_WORKERS = 12
    RETRIES = 3
    HTTP_TIMEOUT = 60

    def get_candles(self, symbol: str, start_date: datetime, end_date: datetime,
                    interval: str) -> List[Candle]:
        if interval not in self.VISION_INTERVALS:
            self.logger.info(
                f"interval {interval} not available on data.binance.vision, falling back to REST")
            return super().get_candles(symbol, start_date, end_date, interval)

        start_date = ensure_utc(start_date)
        end_date = ensure_utc(end_date)
        if end_date <= start_date:
            return []

        now = datetime.now(timezone.utc)
        specs, rest_tail_start = self._plan_files(start_date, end_date, now)

        all_candles: List[Candle] = []
        failed_months: List[FileSpec] = []
        missing_days: List[date] = []

        with requests.Session() as session:
            # 기본 커넥션 풀(10)이 워커 수보다 작으면 커넥션을 버리고 재생성하게 됨
            adapter = requests.adapters.HTTPAdapter(pool_maxsize=self.MAX_WORKERS)
            session.mount("https://", adapter)
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as pool:
                for spec, candles in self._download_all(pool, session, symbol, interval, specs,
                                                        desc=f"Downloading {symbol} {interval} (vision)"):
                    if candles is None:
                        if spec.kind == "monthly":
                            failed_months.append(spec)
                        else:
                            missing_days.append(date(spec.year, spec.month, spec.day))
                    else:
                        all_candles.extend(candles)

                # 404된 월은 일 단위 파일로 재시도 (미발행 최신 월 / 상장 이전 월 모두 커버)
                if failed_months:
                    fallback_specs: List[FileSpec] = []
                    for fm in failed_months:
                        fallback_specs.extend(
                            self._daily_specs_for_month(fm.year, fm.month, start_date, end_date, now))
                    fallback_total = {}
                    for s in fallback_specs:
                        key = (s.year, s.month)
                        fallback_total[key] = fallback_total.get(key, 0) + 1
                    fallback_missing = {}
                    for spec, candles in self._download_all(pool, session, symbol, interval,
                                                            fallback_specs,
                                                            desc=f"Retrying missing months as daily files"):
                        if candles is None:
                            key = (spec.year, spec.month)
                            fallback_missing.setdefault(key, []).append(
                                date(spec.year, spec.month, spec.day))
                        else:
                            all_candles.extend(candles)
                    for key, days in fallback_missing.items():
                        if len(days) == fallback_total[key]:
                            # 월/일 파일이 모두 없음 → 상장 이전 등 데이터가 존재하지 않는 달
                            self.logger.warning(
                                f"no vision data for {symbol} {key[0]:04d}-{key[1]:02d}, skipping")
                        else:
                            missing_days.extend(days)

        # 마지막 daily부터 이어지는 미발행 일자는 REST 꼬리로 흡수
        missing_days.sort()
        while missing_days:
            d = missing_days[-1]
            day_start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            if day_start + timedelta(days=1) >= rest_tail_start:
                rest_tail_start = day_start
                missing_days.pop()
            else:
                break

        # 중간에 빠진 일자(드문 CDN 공백)는 개별로 REST 보충
        for d in missing_days:
            day_start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            all_candles.extend(self._rest_range(
                symbol, max(day_start, start_date),
                min(day_start + timedelta(days=1), end_date), interval))

        rest_tail_start = max(rest_tail_start, start_date)
        if rest_tail_start < end_date:
            all_candles.extend(self._rest_range(symbol, rest_tail_start, end_date, interval))

        # 기존 fetcher와 동일한 조립: 정렬 → dedupe. 월 파일은 월 전체를 담으므로 요청 범위로 필터.
        all_candles.sort(key=lambda c: c.start_time)
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)
        unique_candles = []
        seen_times = set()
        for candle in all_candles:
            if start_ms <= candle.start_time < end_ms and candle.start_time not in seen_times:
                unique_candles.append(candle)
                seen_times.add(candle.start_time)

        if not unique_candles:
            self.logger.warning(f"no candles fetched for {symbol} {start_date} ~ {end_date}")
            return []

        s = ms_timestamp_to_datetime(unique_candles[0].start_time)
        e = ms_timestamp_to_datetime(unique_candles[-1].end_time)
        self.logger.info(f"fetched {len(unique_candles)} candles: {s} ~ {e}, interval: {interval}")

        return unique_candles

    def _plan_files(self, start_date: datetime, end_date: datetime,
                    now: datetime) -> Tuple[List[FileSpec], datetime]:
        """다운로드할 monthly/daily 파일 목록과, 파일이 커버하지 못해 REST로 보충할
        꼬리 구간의 시작 시각(coverage의 exclusive end)을 반환한다."""
        current_month_start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)

        specs: List[FileSpec] = []
        y, m = start_date.year, start_date.month
        while True:
            month_start = datetime(y, m, 1, tzinfo=timezone.utc)
            if month_start >= end_date or month_start >= current_month_start:
                break
            specs.append(FileSpec("monthly", y, m, None))
            y, m = y + m // 12, m % 12 + 1

        # 루프 종료 시 (y, m)은 마지막 monthly의 다음 달
        coverage_end = datetime(y, m, 1, tzinfo=timezone.utc) if specs else start_date

        if coverage_end < end_date:
            # 남은 구간은 현재(부분) 월 → 어제까지의 daily 파일로 커버
            daily_specs = self._daily_specs_for_month(now.year, now.month,
                                                      start_date, end_date, now)
            if daily_specs:
                specs.extend(daily_specs)
                last = daily_specs[-1]
                coverage_end = datetime(last.year, last.month, last.day,
                                        tzinfo=timezone.utc) + timedelta(days=1)

        return specs, max(coverage_end, start_date)

    @staticmethod
    def _daily_specs_for_month(year: int, month: int, start_date: datetime,
                               end_date: datetime, now: datetime) -> List[FileSpec]:
        month_start = date(year, month, 1)
        month_end = date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)
        first = max(month_start, start_date.date())
        last = min(month_end,
                   (end_date - timedelta(milliseconds=1)).date(),
                   (now - timedelta(days=1)).date())  # 오늘 파일은 아직 없음
        specs = []
        d = first
        while d <= last:
            specs.append(FileSpec("daily", d.year, d.month, d.day))
            d += timedelta(days=1)
        return specs

    def _download_all(self, pool, session, symbol: str, interval: str,
                      specs: List[FileSpec], desc: str):
        """specs를 병렬 다운로드해 (spec, candles-or-None) 을 완료 순서대로 yield."""
        if not specs:
            return
        futures = [pool.submit(self._fetch_one, session, symbol, interval, s) for s in specs]
        with tqdm(total=len(futures), desc=desc, unit="file") as pbar:
            for future in concurrent.futures.as_completed(futures):
                yield future.result()
                pbar.update(1)

    def _url_for(self, symbol: str, interval: str, spec: FileSpec) -> str:
        if spec.kind == "monthly":
            return (f"{self.VISION_BASE}/monthly/klines/{symbol}/{interval}/"
                    f"{symbol}-{interval}-{spec.year:04d}-{spec.month:02d}.zip")
        return (f"{self.VISION_BASE}/daily/klines/{symbol}/{interval}/"
                f"{symbol}-{interval}-{spec.year:04d}-{spec.month:02d}-{spec.day:02d}.zip")

    def _fetch_one(self, session: requests.Session, symbol: str, interval: str,
                   spec: FileSpec) -> Tuple[FileSpec, Optional[List[Candle]]]:
        """한 파일을 다운로드/파싱. 404면 (spec, None). 그 외 에러는 재시도 후 raise
        (조용히 누락시키면 백테스트 데이터가 오염되므로)."""
        url = self._url_for(symbol, interval, spec)
        for attempt in range(self.RETRIES):
            try:
                response = session.get(url, timeout=self.HTTP_TIMEOUT)
                if response.status_code == 404:
                    return spec, None
                response.raise_for_status()
                return spec, self._parse_zip_bytes(response.content)
            except Exception:
                if attempt == self.RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)

    @staticmethod
    def _parse_zip_bytes(blob: bytes) -> List[Candle]:
        candles = []
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            with zf.open(zf.namelist()[0]) as raw:
                reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
                for row in reader:
                    # 헤더 행(파일 시기에 따라 유무가 다름)과 비정상 행 skip
                    if len(row) < 7 or not row[0].isdigit():
                        continue
                    open_time = int(row[0])
                    close_time = int(row[6])
                    # 2025-01-01부터 vision 데이터는 microseconds → ms로 변환
                    if open_time >= 10 ** 14:
                        open_time //= 1000
                        close_time //= 1000
                    candles.append(Candle(
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                        start_time=open_time,
                        end_time=close_time + 1,
                        trade_count=int(row[8]) if len(row) > 8 else None,
                        taker_buy_volume=float(row[9]) if len(row) > 9 else None
                    ))
        return candles

    def _rest_range(self, symbol: str, start_date: datetime, end_date: datetime,
                    interval: str) -> List[Candle]:
        # REST 경로는 이제 결과가 비면 빈 리스트를 돌려준다 (예전엔 IndexError를 냈다).
        return super().get_candles(symbol, start_date, end_date, interval)
