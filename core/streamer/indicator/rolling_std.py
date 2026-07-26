from collections import deque
from typing import Optional

import numpy as np
import pandas as pd

from core.backtest.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import VectorizedIndicator


class RollingStd(VectorizedIndicator):
    """
    Rolling sample standard deviation (ddof=1) of close over `window` candles.
    """

    scale_group = "std"

    def __init__(self, window: int):
        super().__init__(window)
        self.values = deque(maxlen=window)
        self.std_values = deque()

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        self.values.append(candle.close)

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
        # pandas rolling std defaults to ddof=1, matching the loop path
        return pd.Series(close).rolling(self.window).std().to_numpy()
