from collections import deque
from typing import Optional

from core.backtest.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import BaseIndicator


class SupertrendIndicator(BaseIndicator):
    """
    Supertrend: ATR 밴드 래칫 기반 트레일링 라인.

    - ATR = True Range의 SMA(window) (ATRIndicator와 동일 방식)
    - basic band = (high+low)/2 ± multiplier×ATR
    - final band는 표준 래칫 재귀로 갱신, close가 반대 final band를 넘으면 추세 플립
    - get_index(idx) = supertrend 라인 (업트렌드면 final lower band, 다운트렌드면 final upper band)
    - get_direction(idx) = +1(업트렌드) / -1(다운트렌드)

    래칫이 재귀적(경로 의존)이라 벡터화하지 않는다 — plain BaseIndicator로 두면
    FastBacktester가 루프 경로로 정확히 업데이트한다.
    """

    def __init__(self, window: int, multiplier: float):
        super().__init__(window)
        self.multiplier = multiplier

        self._tr_values = deque(maxlen=window)
        self._tr_sum = 0.0
        self._prev_close: Optional[float] = None

        self._final_ub: Optional[float] = None
        self._final_lb: Optional[float] = None
        self._trend = 1

        self._lines = deque()
        self._directions = deque()

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        if self._prev_close is None:
            tr = candle.high - candle.low
        else:
            tr = max(
                candle.high - candle.low,
                abs(candle.high - self._prev_close),
                abs(candle.low - self._prev_close),
            )
        if len(self._tr_values) == self.window:
            self._tr_sum -= self._tr_values[0]
        self._tr_values.append(tr)
        self._tr_sum += tr

        prev_close = self._prev_close
        self._prev_close = candle.close

        # return premature indicators
        if len(self._tr_values) < self.window:
            return

        atr = self._tr_sum / self.window
        mid = (candle.high + candle.low) / 2
        basic_ub = mid + self.multiplier * atr
        basic_lb = mid - self.multiplier * atr

        if self._final_ub is None:
            final_ub, final_lb = basic_ub, basic_lb
            trend = 1 if candle.close > mid else -1
        else:
            final_ub = basic_ub if (basic_ub < self._final_ub or prev_close > self._final_ub) \
                else self._final_ub
            final_lb = basic_lb if (self._final_lb < basic_lb or prev_close < self._final_lb) \
                else self._final_lb

            trend = self._trend
            if trend == 1 and candle.close < final_lb:
                trend = -1
            elif trend == -1 and final_ub < candle.close:
                trend = 1

        self._final_ub, self._final_lb, self._trend = final_ub, final_lb, trend
        self._lines.append(final_lb if trend == 1 else final_ub)
        self._directions.append(trend)

    def get_index(self, idx: int) -> Optional[float]:
        try:
            return self._lines[idx]
        except IndexError:
            return None

    def get_direction(self, idx: int = -1) -> Optional[int]:
        try:
            return self._directions[idx]
        except IndexError:
            return None
