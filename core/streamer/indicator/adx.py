from collections import deque
from typing import Optional

from core.backtest.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import BaseIndicator


class ADXIndicator(BaseIndicator):
    """
    표준 Wilder ADX (Average Directional Index), 0~100.

    +DM/-DM과 TR을 Wilder smoothing(초기 n개 단순평균 시드 후 재귀)으로 평활,
    DI+/DI- -> DX -> DX의 Wilder smoothing = ADX.

    Wilder 재귀는 경로 의존이라 벡터화하지 않는다(plain BaseIndicator,
    FastBacktester 루프 경로 — Supertrend와 동일 전례). 워밍업은 약 2*window 캔들.
    """

    scale_group = "adx"

    def __init__(self, window: int):
        super().__init__(window)
        self._prev_high: Optional[float] = None
        self._prev_low: Optional[float] = None
        self._prev_close: Optional[float] = None

        # Wilder smoothing 시드용 누적
        self._init_tr = []
        self._init_pdm = []
        self._init_ndm = []
        self._init_dx = []

        self._atr: Optional[float] = None
        self._pdm_s: Optional[float] = None
        self._ndm_s: Optional[float] = None
        self._adx: Optional[float] = None

        self._adx_values = deque()

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        h, l, c = candle.high, candle.low, candle.close
        if self._prev_close is None:
            self._prev_high, self._prev_low, self._prev_close = h, l, c
            return

        tr = max(h - l, abs(h - self._prev_close), abs(l - self._prev_close))
        up = h - self._prev_high
        dn = self._prev_low - l
        pdm = up if (up > dn and up > 0) else 0.0
        ndm = dn if (dn > up and dn > 0) else 0.0
        self._prev_high, self._prev_low, self._prev_close = h, l, c

        n = self.window
        if self._atr is None:
            self._init_tr.append(tr)
            self._init_pdm.append(pdm)
            self._init_ndm.append(ndm)
            if len(self._init_tr) < n:
                return
            self._atr = sum(self._init_tr) / n
            self._pdm_s = sum(self._init_pdm) / n
            self._ndm_s = sum(self._init_ndm) / n
        else:
            self._atr = (self._atr * (n - 1) + tr) / n
            self._pdm_s = (self._pdm_s * (n - 1) + pdm) / n
            self._ndm_s = (self._ndm_s * (n - 1) + ndm) / n

        if self._atr <= 0:
            dx = 0.0
        else:
            di_p = 100.0 * self._pdm_s / self._atr
            di_n = 100.0 * self._ndm_s / self._atr
            dx = 100.0 * abs(di_p - di_n) / (di_p + di_n) if (di_p + di_n) > 0 else 0.0

        if self._adx is None:
            self._init_dx.append(dx)
            if len(self._init_dx) < n:
                return
            self._adx = sum(self._init_dx) / n
        else:
            self._adx = (self._adx * (n - 1) + dx) / n

        self._adx_values.append(self._adx)

    def get_index(self, idx: int) -> Optional[float]:
        try:
            return self._adx_values[idx]
        except IndexError:
            return None
