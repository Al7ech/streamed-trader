from collections import deque
from typing import Optional

import numpy as np
import pandas as pd

from core.backtest.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import VectorizedIndicator


class ATRIndicator(VectorizedIndicator):
    """
    Simple (non-Wilder) moving average of True Range over `window` candles.
    True Range = max(high-low, |high-prev_close|, |low-prev_close|).
    """

    scale_group = "atr"

    def __init__(self, window: int, history_size: Optional[int] = None):
        # ATR 기울기를 보는 전략(RyulStreamer_edit2)이 get_index(-1-lookback)으로 읽어
        # 조회 깊이가 파라미터에 걸린다. 그래서 이 지표도 history_size를 열어 둔다.
        super().__init__(window, history_size=history_size)
        self._tr_values = deque(maxlen=window)
        self._atr_values = self._new_history()
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
        self._atr_values.append(self._sum / self.window)

    def get_index(self, idx: int) -> Optional[float]:
        return self._read(self._atr_values, idx)

    def precompute_series(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        # pandas' rolling mean sums in a different order than the incremental `_sum` above,
        # so values can differ from the loop path by ~1e-13 relative (rounding only).
        if len(close) == 0:
            # 빈 슬라이스에서 아래 [0] 인덱싱이 IndexError를 낸다. reference는 빈 Report를
            # 정상 반환하므로 여기서도 조용히 비워서 동작을 맞춘다.
            return np.empty(0, dtype=np.float64)
        prev_close = np.empty_like(close)
        prev_close[0] = np.nan
        prev_close[1:] = close[:-1]
        tr = np.maximum(high - low,
                        np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
        tr[0] = high[0] - low[0]  # first candle has no previous close
        return pd.Series(tr).rolling(self.window).mean().to_numpy()
