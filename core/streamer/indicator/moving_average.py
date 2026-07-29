from collections import deque
from typing import Optional

import numpy as np
import pandas as pd

from core.backtest.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import VectorizedIndicator


class MovingAverage(VectorizedIndicator):
    def __init__(self, window: int, history_size: Optional[int] = None):
        # MovingAverage(1)을 종가 시계열로 재사용하는 전략(MomentumTimeExitStreamer)이 있어
        # 조회 깊이가 파라미터로 정해진다. 그래서 이 지표만 history_size를 열어 둔다.
        super().__init__(window, history_size=history_size)
        self.values = deque(maxlen=window)
        self.ma_values = self._new_history()
        self._sum = 0

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        price = candle.close
        if len(self.values) == self.window:
            self._sum -= self.values[0]

        self.values.append(price)
        self._sum += price

        # return premature indicators
        if len(self.values) < self.window:
            return

        ma = self._sum / len(self.values)
        self.ma_values.append(ma)

    def get_index(self, idx: int) -> Optional[float]:
        return self._read(self.ma_values, idx)

    def get_latest(self) -> Optional[float]:
        return self.get_index(-1)

    def precompute_series(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        return pd.Series(close).rolling(self.window).mean().to_numpy()
