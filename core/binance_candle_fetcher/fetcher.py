import time
from datetime import datetime, timedelta
from typing import List, Tuple

from binance.client import Client
from tqdm import tqdm

from core.candle_fetcher import BaseCandleFetcher
from core.streamer import Candle
from core.utils import interval_to_minutes, ensure_utc, ms_timestamp_to_datetime


class BinanceCandleFetcher(BaseCandleFetcher):
    # Binance API limit: maximum 1500 candles per request
    MAX_CANDLES_PER_REQUEST = 1500

    def __init__(self, compress: bool = True, save_path: str = "asset/"):
        super().__init__(save_path=save_path)
        self.client = Client()
        self.compress = compress

    def _calculate_chunk_dates(self, start_datetime: datetime, end_datetime: datetime, interval: str) -> List[
        Tuple[datetime, datetime]]:
        """Calculate date chunks to stay within API limits."""
        interval_minutes = interval_to_minutes(interval)
        total_minutes = int((end_datetime - start_datetime).total_seconds() / 60)

        # Calculate how many candles we need
        total_candles = total_minutes // interval_minutes

        # If within limit, return single chunk
        if total_candles <= self.MAX_CANDLES_PER_REQUEST:
            return [(start_datetime, end_datetime - timedelta(milliseconds=1))]

        # Calculate chunk size in minutes
        chunk_minutes = self.MAX_CANDLES_PER_REQUEST * interval_minutes  # Leave some buffer

        chunks = []
        current_start = start_datetime

        while current_start < end_datetime:
            # Calculate chunk end datetime
            chunk_end_dt = current_start + timedelta(minutes=chunk_minutes)
            current_end = min(chunk_end_dt, end_datetime)

            chunks.append((current_start, current_end - timedelta(milliseconds=1)))

            # Move to next chunk (start from next interval after current end)
            current_start = current_end

        return chunks

    def get_candles(self, symbol: str, start_date: datetime, end_date: datetime,
                    interval: str) -> \
            List[Candle]:
        """
        지정한 날짜 사이의 봉(OHLCV) 리스트를 반환합니다.
        Binance API 제한(1500개 캔들)을 고려하여 큰 요청을 여러 개로 나누어 처리합니다.
        :param symbol: 예) 'BTCUSDT'
        :param start_date: datetime.datetime 객체 (UTC timezone 권장)
        :param end_date: datetime.datetime 객체 (UTC timezone 권장)
        :param interval: 바이낸스 interval 문자열 (예: '1h', '15m', '1d' 등)
        :return: Candle 객체 리스트
        """
        # Ensure datetimes are UTC
        start_date = ensure_utc(start_date)
        end_date = ensure_utc(end_date)

        # Calculate date chunks to stay within API limits
        date_chunks = self._calculate_chunk_dates(start_date, end_date, interval)

        all_candles = []

        # Process each chunk with progress bar
        with tqdm(total=len(date_chunks), desc=f"Fetching {symbol} candles", unit="chunk") as pbar:
            for i, (chunk_start, chunk_end) in enumerate(date_chunks):
                # Respect limit
                if i % 23 == 0 and i != 0:
                    time.sleep(60 - datetime.now().second)

                # Convert to naive datetime for Binance API (API expects naive UTC)
                chunk_start_naive = chunk_start.replace(tzinfo=None)
                chunk_end_naive = chunk_end.replace(tzinfo=None)

                pbar.set_description(
                    f"Fetching {symbol} chunk {i + 1}/{len(date_chunks)}: {chunk_start_naive} to {chunk_end_naive}")

                try:
                    klines = self.client.futures_historical_klines(
                        symbol,
                        interval,
                        chunk_start_naive.strftime('%Y-%m-%d %H:%M:%S'),
                        chunk_end_naive.strftime('%Y-%m-%d %H:%M:%S'),
                        limit=self.MAX_CANDLES_PER_REQUEST
                    )

                    # Convert klines to Candle objects
                    for k in klines:
                        candle = Candle(
                            open=float(k[1]),
                            high=float(k[2]),
                            low=float(k[3]),
                            close=float(k[4]),
                            volume=float(k[5]),
                            start_time=int(k[0]),
                            end_time=int(k[6]) + 1,
                            trade_count=int(k[8]) if len(k) > 8 else None,
                            taker_buy_volume=float(k[9]) if len(k) > 9 else None
                        )
                        all_candles.append(candle)

                    pbar.set_postfix(candles=len(klines), total_candles=len(all_candles))

                except Exception as e:
                    # 여기서 삼키면 안 된다. 잘린 결과는 상위(_save_month_chunks)에서 **완전한
                    # 불변 월 청크**로 캐시되고, 완전성 경고는 90% 미만에서만 뜨므로 44,640캔들
                    # 월에 1500캔들짜리 구멍이 영구히 묻힌다.
                    pbar.set_postfix(error=str(e)[:50])
                    self.logger.error(
                        f"chunk fetch failed ({chunk_start_naive} ~ {chunk_end_naive}, "
                        f"{symbol} {interval}): {e}")
                    raise
                finally:
                    pbar.update(1)

        # Sort candles by start_time to ensure chronological order
        all_candles.sort(key=lambda c: c.start_time)

        # Remove duplicates (in case of overlapping chunks)
        unique_candles = []
        seen_times = set()
        for candle in all_candles:
            if candle.start_time not in seen_times:
                unique_candles.append(candle)
                seen_times.add(candle.start_time)

        if not unique_candles:
            # 상장 이전 구간 등 결과가 비는 경우가 실제로 있다. 예전에는 아래 [0] 인덱싱에서
            # IndexError가 나 캐시 페치 전체가 중단됐다.
            self.logger.info(
                f"fetched 0 candles: {start_date} ~ {end_date}, interval: {interval}")
            return []

        s = ms_timestamp_to_datetime(unique_candles[0].start_time)
        e = ms_timestamp_to_datetime(unique_candles[-1].end_time)
        self.logger.info(f"fetched {len(unique_candles)} candles: {s} ~ {e}, interval: {interval}")

        return unique_candles
