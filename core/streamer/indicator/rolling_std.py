from collections import deque
from typing import Optional

import numpy as np
import pandas as pd

from core.domain.candle import Candle
from core.domain.status import Status
from core.streamer.indicator.base_indicator import NumericIndicator


class RollingStd(NumericIndicator):
    """
    Rolling sample standard deviation (ddof=1) of close over `window` candles.
    """

    scale_group = "std"

    def __init__(self, window: int):
        if window < 2:
            # 표본 1개짜리 ddof=1 표준편차는 NaN이다. 조용히 NaN을 흘리면
            # `std is None or std <= 0` 류의 가드를 전부 통과해 사이징/스탑까지 오염된다.
            raise ValueError(f"RollingStd(window={window}): 표본표준편차는 window >= 2 가 필요하다.")
        super().__init__()
        self.window = window
        self.values = deque(maxlen=window)

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        self.values.append(candle.close)

        # return premature indicators
        if len(self.values) < self.window:
            return

        self._deque.append(float(np.std(self.values, ddof=1)))

    def precompute_series(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        # pandas rolling std defaults to ddof=1, matching the loop path
        return pd.Series(close).rolling(self.window).std().to_numpy()
