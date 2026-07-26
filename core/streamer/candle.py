from typing import Optional

from core.utils import ms_timestamp_to_datetime


class Candle:
    # 2026-07 이전에 캐시된 pickle 객체는 __dict__에 아래 두 필드가 없다 —
    # 클래스 속성 기본값으로 폴백시켜 하위호환 (None = 데이터 없음, 지표는 워밍업 취급)
    taker_buy_volume: Optional[float] = None
    trade_count: Optional[int] = None

    def __init__(self, open: float, high: float, low: float, close: float, volume: float, start_time: int,
                 end_time: int, taker_buy_volume: Optional[float] = None, trade_count: Optional[int] = None):
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.start_time = start_time  # ms timestamp
        self.end_time = end_time  # ms timestamp
        self.taker_buy_volume = taker_buy_volume
        self.trade_count = trade_count

    def __repr__(self):
        s = ms_timestamp_to_datetime(self.start_time).strftime("%Y-%m-%d %H:%M:%S")
        e = ms_timestamp_to_datetime(self.end_time).strftime("%Y-%m-%d %H:%M:%S")
        return f"[{s}~{e},O={self.open},H={self.high},L={self.low},C={self.close},V={self.volume}]"
