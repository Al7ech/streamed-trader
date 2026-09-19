from collections import deque
from typing import Optional

import numpy as np

from core.candle.candle import Candle
from core.account.status import Status
from core.streamer.indicator.base_indicator import VectorizableNumericIndicator
from core.streamer.indicator.vector_ops import rolling_std_exact


class RollingStd(VectorizableNumericIndicator):
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

    def compute(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        # 위 update()와 같은 창 단위 np.std(ddof=1) — 비트 단위로 같다 (vector_ops 참고).
        return rolling_std_exact(close, self.window)
