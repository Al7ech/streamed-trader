from collections import deque
from typing import Optional

import numpy as np
import pandas as pd

from core.backtest.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import VectorizedIndicator


class VolumeMovingAverage(VectorizedIndicator):
    """
    Rolling mean of candle volume over `window` candles.
    """

    scale_group = "volume"

    def __init__(self, window: int):
        super().__init__(window)
        self.values = deque(maxlen=window)
        self._sum = 0.0
        self.ma_values = deque()

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        if len(self.values) == self.window:
            self._sum -= self.values[0]
        self.values.append(candle.volume)
        self._sum += candle.volume

        # return premature indicators
        if len(self.values) < self.window:
            return
        self.ma_values.append(self._sum / self.window)

    def get_index(self, idx: int) -> Optional[float]:
        try:
            return self.ma_values[idx]
        except IndexError:
            return None

    def precompute_series(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        return pd.Series(volume).rolling(self.window).mean().to_numpy()


class VolumeRollingStd(VectorizedIndicator):
    """
    Rolling sample standard deviation (ddof=1) of candle volume over `window` candles.
    """

    scale_group = "volume"

    def __init__(self, window: int):
        super().__init__(window)
        self.values = deque(maxlen=window)
        self.std_values = deque()

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        self.values.append(candle.volume)

        # return premature indicators
        if len(self.values) < self.window:
            return
        self.std_values.append(float(np.std(self.values, ddof=1)))

    def get_index(self, idx: int) -> Optional[float]:
        try:
            return self.std_values[idx]
        except IndexError:
            return None

    def precompute_series(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        return pd.Series(volume).rolling(self.window).std().to_numpy()
