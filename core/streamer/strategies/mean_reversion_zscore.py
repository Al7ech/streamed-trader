import logging
from typing import List

from core.domain.action import Action
from core.domain.candle import Candle
from core.domain.status import Status
from core.streamer.base_streamer import BaseStreamer
from core.streamer.indicator.moving_average import MovingAverage
from core.streamer.indicator.rolling_std import RollingStd
from core.utils import trunc_by_sign


class MeanReversionZScoreStreamer(BaseStreamer):
    """
    평균회귀 z-score fade.

    돌파 추종 전략들의 역방향 — 단기 이탈을 따라가는 대신 평균으로의 되돌림에
    베팅한다. 스테이트풀 스트리머 예제이기도 하다 (진입 시점의 타임아웃 카운터와
    스탑 가격을 인스턴스에 들고 다닌다).

    - 진입: z = (close - MA_w) / STD_w 가 ±entry_z 이상 이탈하면 역방향 진입
      (z <= -entry_z 롱, z >= +entry_z 숏)
    - 청산: z가 0을 되짚으면(평균 복귀 완료), 또는 timeout_candles 경과, 또는
      진입 시점 기준 entry_z*sigma 만큼 추가 역행 시 하드스탑
    - 사이징: 스탑 거리(entry_z*sigma)에서의 손실이 max_loss가 되도록 레버리지, 6x 캡
    """

    def __init__(self, symbol: str,
                 window: int = 60,
                 entry_z: float = 2.0,
                 timeout_candles: int = 60,
                 max_loss: float = 0.08,
                 fee_ratio: float = 0.0004,
                 use_stop: bool = True):
        super().__init__([symbol], {symbol: {
            "MA": MovingAverage(window),
            "STD": RollingStd(window),
        }})
        self.symbol = symbol
        self.entry_z = entry_z
        self.timeout_candles = timeout_candles
        self.max_loss = max_loss
        self.fee_ratio = fee_ratio
        # 스탑 없이 z-복귀/타임아웃 청산만 쓰는 진단 모드 (인트라바 꼬리 체결 효과 분리용)
        self.use_stop = use_stop

        self._timeout_remaining = 0
        self._stop_price = 0.0

        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"MeanReversionZScoreStreamer initialized with params: [window={window},"
            f"entry_z={entry_z},timeout_candles={timeout_candles},max_loss={max_loss}]")

    def decide_action(self, symbol: str, candle: Candle, status: Status) -> List[Action]:
        ind = self.indicators[symbol]
        ma = ind["MA"].get_latest()
        std = ind["STD"].get_latest()
        price = candle.close
        position = status.position_for(symbol).position

        if position != 0:
            self._timeout_remaining -= 1

            if self.use_stop:
                if position > 0 and candle.low <= self._stop_price:
                    return [Action(symbol, -position)]
                if position < 0 and self._stop_price <= candle.high:
                    return [Action(symbol, -position)]

            if ma is not None and std is not None and std > 0:
                z = (price - ma) / std
                if position > 0 and z >= 0:
                    return [Action(symbol, -position)]
                if position < 0 and z <= 0:
                    return [Action(symbol, -position)]

            if self._timeout_remaining <= 0:
                return [Action(symbol, -position)]
            return []

        if ma is None or std is None or std <= 0:
            return []

        z = (price - ma) / std
        if abs(z) < self.entry_z:
            return []

        sign = -1 if z > 0 else 1  # 이탈의 역방향 (fade)
        stop_frac = self.entry_z * std / price
        lev = min(6.0, self.max_loss / stop_frac)
        qty = trunc_by_sign(sign * status.total_margin() / (price * (1 / lev + self.fee_ratio)), 3)

        self._timeout_remaining = self.timeout_candles
        self._stop_price = price * (1 - sign * stop_frac)
        return [Action(symbol, qty)]
