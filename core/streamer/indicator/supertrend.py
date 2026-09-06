from collections import deque
from typing import Optional, Tuple

from core.domain.candle import Candle
from core.domain.status import Status
from core.streamer.indicator.base_indicator import NumericIndicator


class SupertrendIndicator(NumericIndicator):
    """
    Supertrend: ATR 밴드 래칫 기반 트레일링 라인.

    - ATR = True Range의 SMA(window) (ATRIndicator와 동일 방식)
    - basic band = (high+low)/2 ± multiplier×ATR
    - final band는 표준 래칫 재귀로 갱신, close가 반대 final band를 넘으면 추세 플립
    - read(idx) = supertrend 라인 (업트렌드면 final lower band, 다운트렌드면 final upper band)
    - get_direction(idx) = +1(업트렌드) / -1(다운트렌드)
    - read_both(idx) = (라인, 방향) — 대부분 둘을 같이 쓰므로 한 번에 읽는 편의 메서드

    라인 값과 방향 값, 두 개의 출력 시계열을 갖는다. 주 출력인 라인 값은 NumericIndicator가
    상속해 주는 self._deque/read()를 그대로 쓰고, 방향 값만 자기 소유 deque(_direction) +
    상속받은 self._read_series(...) 재사용으로 추가한다 — MinDonchianIndicator가 상속받은
    출력 deque 외에 자기만의 작업용 deque(min_deque)를 따로 갖는 것과 같은 모양이다. 라인과
    방향은 하나의 래칫 재귀 계산에서 동시에 나오므로 update() 한 곳에서 둘 다 채운다.

    래칫이 재귀적(경로 의존)이라 벡터화하지 않는다 — plain NumericIndicator로 두면
    벡터화 경로에서도 루프로 정확히 업데이트된다.
    """

    def __init__(self, window: int, multiplier: float):
        super().__init__()
        self.window = window
        self.multiplier = multiplier

        self._tr_values = deque(maxlen=window)
        self._tr_sum = 0.0
        self._prev_close: Optional[float] = None

        self._final_ub: Optional[float] = None
        self._final_lb: Optional[float] = None
        self._trend = 1

        self._direction = deque(maxlen=self.history_size)

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
        self._deque.append(final_lb if trend == 1 else final_ub)
        self._direction.append(trend)

    def get_direction(self, idx: int = -1) -> Optional[int]:
        return self._read_series(self._direction, idx, self.history_size)

    def read_both(self, idx: int = -1) -> Tuple[Optional[float], Optional[int]]:
        """라인 값과 방향 값을 한 번에 읽는다 — 대부분의 소비자가 둘을 같이 쓴다."""
        return self.read(idx), self.get_direction(idx)
