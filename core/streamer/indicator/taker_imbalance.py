from collections import deque
from typing import Optional

from core.backtest.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import BaseIndicator


class TakerImbalanceIndicator(BaseIndicator):
    """
    롤링 윈도우의 taker 매수/매도 임밸런스: (2·Σtaker_buy − Σvolume) / Σvolume ∈ [−1, 1].

    +1 = 전량 공격적 매수, −1 = 전량 공격적 매도. kline의 taker_buy_volume 필드 필요 —
    필드가 없는(구 캐시) 캔들이 윈도우에 섞여 있으면 워밍업 취급(None 반환).

    precompute_series 시그니처(o/h/l/c/v)에 taker 배열이 없어 벡터화 불가 → plain 루프 경로.
    """

    scale_group = "taker_imbalance"

    def __init__(self, window: int):
        super().__init__(window)
        self._buy = deque(maxlen=window)
        self._vol = deque(maxlen=window)
        self._buy_sum = 0.0
        self._vol_sum = 0.0
        self._missing = deque(maxlen=window)
        self._missing_count = 0
        self._values = deque()

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        if len(self._buy) == self.window:
            self._buy_sum -= self._buy[0]
            self._vol_sum -= self._vol[0]
            self._missing_count -= self._missing[0]

        missing = 1 if candle.taker_buy_volume is None else 0
        self._buy.append(0.0 if missing else candle.taker_buy_volume)
        self._vol.append(candle.volume)
        self._missing.append(missing)
        self._buy_sum += self._buy[-1]
        self._vol_sum += self._vol[-1]
        self._missing_count += missing

        if len(self._buy) < self.window:
            return
        if self._missing_count > 0 or self._vol_sum <= 0:
            self._values.append(None)
            return
        self._values.append((2.0 * self._buy_sum - self._vol_sum) / self._vol_sum)

    def get_index(self, idx: int) -> Optional[float]:
        try:
            return self._values[idx]
        except IndexError:
            return None
