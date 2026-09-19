from collections import deque
from typing import Optional

import numpy as np

from core.candle.candle import Candle
from core.account.status import Status
from core.streamer.indicator.base_indicator import VectorizableNumericIndicator
from core.streamer.indicator.vector_ops import rolling_mean_exact, true_range


class ATRIndicator(VectorizableNumericIndicator):
    """
    Simple (non-Wilder) moving average of True Range over `window` candles.
    True Range = max(high-low, |high-prev_close|, |low-prev_close|).
    """

    scale_group = "atr"

    def __init__(self, window: int, history_size: Optional[int] = None):
        # ATR 기울기를 보는 전략(RyulStreamer_edit2)이 read(-1-lookback)으로 읽어
        # 조회 깊이가 파라미터에 걸린다. 그래서 이 지표도 history_size를 열어 둔다.
        super().__init__(history_size=history_size)
        self.window = window
        self._tr_values = deque(maxlen=window)
        self._sum = 0.0
        self._prev_close: Optional[float] = None

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        if self._prev_close is None:
            tr = candle.high - candle.low
        else:
            tr = max(
                candle.high - candle.low,
                abs(candle.high - self._prev_close),
                abs(candle.low - self._prev_close),
            )
        self._prev_close = candle.close

        if len(self._tr_values) == self.window:
            self._sum -= self._tr_values[0]
        self._tr_values.append(tr)
        self._sum += tr

        if len(self._tr_values) < self.window:
            return
        self._deque.append(self._sum / self.window)

    def compute(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        # TR 계산도 위 update()와 같은 식이고, 그 위의 롤링 평균도 같은 증분 합산을 펼친
        # 것이라 전 구간이 비트 단위로 같다 (vector_ops 참고).
        return rolling_mean_exact(true_range(high, low, close), self.window)
