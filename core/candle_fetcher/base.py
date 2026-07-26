import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from core.streamer import Candle
from core.utils import ensure_utc, interval_to_minutes, ms_timestamp_to_datetime
from .pickle_storage import PickleStorage


class BaseCandleFetcher(ABC):
    """월 청크 캐시를 제공하는 캔들 fetcher의 공통 base.

    거래소별 서브클래스는 get_candles만 구현하면 get_candles_with_cache를 그대로 얻는다.
    24/7이 아닌 시장(주식 등)은 아래 hook들을 오버라이드해 캐시 규칙을 조정한다.
    """

    def __init__(self, save_path: str = "asset/"):
        self.save_path = save_path
        self.logger = logging.getLogger(self.__class__.__module__)

    @abstractmethod
    def get_candles(self, symbol: str, start_date: datetime, end_date: datetime,
                    interval: str) -> List[Candle]:
        """[start_date, end_date) 구간의 캔들을 start_time 오름차순으로 반환한다."""

    # --- 서브클래스 hook (기본값 = 24/7 시장) ---

    def _cache_dir_name(self, symbol: str, interval: str) -> str:
        """캐시 디렉토리명. 같은 심볼이라도 데이터 변종(가격 조정 등)이 다르면 분리해야 한다."""
        return f"{symbol}_{interval}"

    def _expected_candles(self, month: datetime, month_end: datetime, interval: str) -> int:
        """해당 달에 있어야 할 캔들 수. 결측 경고의 기준이며 24/7 시장은 달 전체가 거래 시간이다."""
        return int((month_end - month).total_seconds() // 60) // interval_to_minutes(interval)

    def _data_available_until(self, now: datetime) -> datetime:
        """데이터가 존재한다고 볼 수 있는 시각의 exclusive 상한.

        이 시각 이후는 fetch하지도, 캐시에 굳히지도 않는다. 실시간 소스는 now 그대로지만
        EOD 소스는 마지막으로 완결된 세션까지만 돌려줘야 한다 (안 그러면 아직 데이터가
        없는 구간이 빈 청크로 굳어버린다).
        """
        return now

    def _on_cache_open(self, symbol: str, interval: str, chunk_dir: str) -> None:
        """캐시 디렉토리를 열 때 호출된다. 캐시 무효화가 필요한 소스를 위한 hook."""

    # --- 월 청크 캐시 ---

    def get_candles_with_cache(self, symbol: str, start_date: datetime, end_date: datetime,
                               interval: str) -> List[Candle]:
        """
        월 단위 청크(asset/candle/{symbol}_{interval}/{YYYY-MM}.pkl)로 캐시하며 캔들을 반환합니다.
        요청 범위에서 캐시에 없는 달만 새로 fetch하므로, 범위가 바뀌어도 겹치는 과거 달은
        다시 다운로드하지 않습니다. 진행 중인 달은 {YYYY-MM}.partial.pkl로 저장해 두고
        다음 실행 시 마지막 캔들 이후 구간만 델타 fetch하며, 달이 끝나면 정식 청크로 승격합니다.
        """
        # Ensure datetimes are UTC
        start_date = ensure_utc(start_date)
        end_date = ensure_utc(end_date)

        chunk_dir = os.path.join(self.save_path, "candle", self._cache_dir_name(symbol, interval))
        os.makedirs(chunk_dir, exist_ok=True)
        self._on_cache_open(symbol, interval, chunk_dir)

        available_until = self._data_available_until(datetime.now(timezone.utc))
        all_candles: List[Candle] = []
        delta_months: List[datetime] = []
        missing_months: List[datetime] = []

        load_start = datetime.now()
        loaded_chunks = 0
        for month in self._months_in_range(start_date, end_date):
            if os.path.exists(os.path.join(chunk_dir, self._chunk_name(month))):
                all_candles.extend(PickleStorage.load_from_pickle(
                    os.path.join(chunk_dir, self._chunk_name(month)), verbose=False))
                loaded_chunks += 1
            elif os.path.exists(os.path.join(chunk_dir, self._partial_name(month))):
                delta_months.append(month)
            else:
                missing_months.append(month)
        if loaded_chunks:
            elapsed_ms = (datetime.now() - load_start).total_seconds() * 1000
            print(f"Loaded {loaded_chunks} cached month chunk(s) from {chunk_dir} in {elapsed_ms:.0f}ms")

        # partial 청크가 있는 달은 마지막 캔들 이후 구간만 델타 fetch해 이어붙임
        for month in delta_months:
            month_end = self._next_month(month)
            candles = PickleStorage.load_from_pickle(
                os.path.join(chunk_dir, self._partial_name(month)), verbose=False)
            delta_start = ms_timestamp_to_datetime(candles[-1].end_time) if candles else month
            fetch_end = min(month_end, available_until)
            if delta_start < fetch_end:
                candles.extend(self.get_candles(symbol, delta_start, fetch_end, interval))
            all_candles.extend(candles)
            self._save_month_chunks(chunk_dir, candles, month, month_end, available_until, interval)

        # 캐시가 전혀 없는 달들은 연속 구간으로 묶어 한 번에 fetch (달 경계로 정렬해 청크를 완결시킴)
        for run_start, run_end in self._month_runs(missing_months):
            fetch_end = min(run_end, available_until)
            if fetch_end <= run_start:
                continue  # 아직 데이터가 없는 구간
            fetched = self.get_candles(symbol, run_start, fetch_end, interval)
            all_candles.extend(fetched)
            self._save_month_chunks(chunk_dir, fetched, run_start, run_end, available_until, interval)

        all_candles.sort(key=lambda c: c.start_time)
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)
        return [c for c in all_candles if start_ms <= c.start_time < end_ms]

    def _save_month_chunks(self, chunk_dir: str, candles: List[Candle], run_start: datetime,
                           run_end: datetime, available_until: datetime, interval: str) -> None:
        """fetch한 구간을 달별로 나눠 저장합니다. 지난 달의 데이터는 불변이므로 빈/부분 달
        (상장 이전 등)도 정식 청크로 저장해 재조회를 막습니다. 진행 중인 달은 닫힌 캔들만
        partial 청크로 저장합니다 (진행 중인 캔들을 저장하면 다음 델타 fetch에서 미완성
        OHLCV가 굳어버림)."""
        by_month: Dict[datetime, List[Candle]] = {}
        for candle in candles:
            month = self._month_start(ms_timestamp_to_datetime(candle.start_time))
            by_month.setdefault(month, []).append(candle)

        month = run_start
        while month < run_end:
            month_end = self._next_month(month)
            month_candles = by_month.get(month, [])
            partial_path = os.path.join(chunk_dir, self._partial_name(month))
            if month_end <= available_until:
                expected = self._expected_candles(month, month_end, interval)
                if len(month_candles) < expected * 0.9:
                    self.logger.warning(
                        f"month chunk {month:%Y-%m} has {len(month_candles)}/{expected} candles "
                        f"(listing gap or missing data); caching as-is")
                PickleStorage.save_to_pickle(month_candles, os.path.join(chunk_dir, self._chunk_name(month)))
                if os.path.exists(partial_path):
                    os.remove(partial_path)  # 정식 청크로 승격 완료
            else:
                available_ms = int(available_until.timestamp() * 1000)
                closed = [c for c in month_candles if c.end_time <= available_ms]
                PickleStorage.save_to_pickle(closed, partial_path)
                break  # 이후 달은 아직 데이터가 없음
            month = month_end

    @staticmethod
    def _chunk_name(month: datetime) -> str:
        return f"{month.strftime('%Y-%m')}.pkl"

    @staticmethod
    def _partial_name(month: datetime) -> str:
        return f"{month.strftime('%Y-%m')}.partial.pkl"

    @staticmethod
    def _month_start(dt: datetime) -> datetime:
        return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)

    @staticmethod
    def _next_month(month: datetime) -> datetime:
        return datetime(month.year + month.month // 12, month.month % 12 + 1, 1, tzinfo=timezone.utc)

    @classmethod
    def _months_in_range(cls, start_date: datetime, end_date: datetime) -> List[datetime]:
        """[start_date, end_date)와 겹치는 달의 시작 시각 목록."""
        months = []
        month = cls._month_start(start_date)
        while month < end_date:
            months.append(month)
            month = cls._next_month(month)
        return months

    @classmethod
    def _month_runs(cls, months: List[datetime]) -> List[Tuple[datetime, datetime]]:
        """정렬된 달 목록을 연속 구간 [run_start, run_end)들로 묶습니다."""
        runs: List[List[datetime]] = []
        for month in months:
            if runs and runs[-1][1] == month:
                runs[-1][1] = cls._next_month(month)
            else:
                runs.append([month, cls._next_month(month)])
        return [(start, end) for start, end in runs]
