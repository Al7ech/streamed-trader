from collections import deque
from typing import Optional

import numpy as np

from core.candle.candle import Candle
from core.account.status import Status
from core.streamer.indicator.base_indicator import VectorizableNumericIndicator
from core.streamer.indicator.vector_ops import rolling_mean_exact, rolling_std_exact


class VolumeMovingAverage(VectorizableNumericIndicator):
    """
    Rolling mean of candle volume over `window` candles.
    """

    scale_group = "volume"

    def __init__(self, window: int):
        super().__init__()
        self.window = window
        self.values = deque(maxlen=window)
        self._sum = 0.0

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        if len(self.values) == self.window:
            self._sum -= self.values[0]
        self.values.append(candle.volume)
        self._sum += candle.volume

        # return premature indicators
        if len(self.values) < self.window:
            return
        self._deque.append(self._sum / self.window)

    def compute(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        # 위 update()의 증분 합산을 그대로 펼친다 — 비트 단위로 같다 (vector_ops 참고).
        return rolling_mean_exact(volume, self.window)


class VolumeRollingStd(VectorizableNumericIndicator):
    """
    Rolling sample standard deviation (ddof=1) of candle volume over `window` candles.
    """

    scale_group = "volume"

    def __init__(self, window: int):
        if window < 2:
            # RollingStd와 같은 이유 — 표본 1개의 ddof=1 표준편차는 NaN이다.
            raise ValueError(
                f"VolumeRollingStd(window={window}): 표본표준편차는 window >= 2 가 필요하다.")
        super().__init__()
        self.window = window
        self.values = deque(maxlen=window)

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        self.values.append(candle.volume)

        # return premature indicators
        if len(self.values) < self.window:
            return
        self._deque.append(float(np.std(self.values, ddof=1)))

    def compute(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        # 위 update()와 같은 창 단위 np.std(ddof=1) — 비트 단위로 같다 (vector_ops 참고).
        return rolling_std_exact(volume, self.window)
