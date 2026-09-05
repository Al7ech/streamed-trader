from collections import deque
from typing import Optional

import numpy as np
import pandas as pd

from core.engine.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import NumericIndicator


class MinDonchianIndicator(NumericIndicator):
    def __init__(self, window: int):
        super().__init__()
        self.window = window
        self.min_deque = deque(maxlen=window)
        self.cnt = 0

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        price = candle.low
        self.cnt += 1
        idx = self.cnt

        # 새 값보다 큰 값은 뒤에서 제거
        while self.min_deque and self.min_deque[-1][0] > price:
            self.min_deque.pop()

        self.min_deque.append((price, idx))

        # 윈도우 밖의 값 제거
        if self.min_deque[0][1] <= idx - self.window:
            self.min_deque.popleft()

        v, _ = self.min_deque[0]
        self._deque.append(v if self.window <= idx else None)

    def precompute_series(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        return pd.Series(low).rolling(self.window).min().to_numpy()


class MaxDonchianIndicator(NumericIndicator):
    def __init__(self, window: int):
        super().__init__()
        self.window = window
        self.max_deque = deque(maxlen=window)
        self.cnt = 0

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        price = candle.high
        self.cnt += 1
        idx = self.cnt

        # 새 값보다 작은 값은 뒤에서 제거
        while self.max_deque and self.max_deque[-1][0] < price:
            self.max_deque.pop()

        self.max_deque.append((price, idx))

        # 윈도우 밖의 값 제거
        if self.max_deque[0][1] <= idx - self.window:
            self.max_deque.popleft()

        v, _ = self.max_deque[0]
        self._deque.append(v if self.window <= idx else None)

    def precompute_series(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        return pd.Series(high).rolling(self.window).max().to_numpy()
