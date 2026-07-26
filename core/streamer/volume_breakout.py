import logging
from typing import Optional

from core.backtest.status import Status
from core.streamer.action import Action
from core.streamer.candle import Candle
from core.streamer.indicator.volume_stats import VolumeMovingAverage, VolumeRollingStd
from core.streamer.momentum_time_exit import MomentumTimeExitStreamer


class VolumeConfirmedMomentumStreamer(MomentumTimeExitStreamer):
    """
    볼륨 확인 단기 모멘텀 — 기존 전략을 상속해 진입 필터만 얹는 예제.

    가격 단독 신호에 거래량이라는 다른 정보원을 게이트로 건다: 현재 캔들 volume의
    z-score (직전 vol_window 캔들의 평균/표준편차 대비)가 min_vol_z 이상일 때만
    모멘텀 진입을 허용. 청산/사이징은 MomentumTimeExitStreamer 그대로다.

    min_vol_z=None 이면 게이트 off (부모와 완전 동일 — 동등성 검증용).
    """

    def __init__(self, symbol: str,
                 mom_lookback: int = 60,
                 entry_threshold_pct: float = 1.0,
                 hold_candles: int = 120,
                 max_loss: float = 0.08,
                 fee_ratio: float = 0.0004,
                 use_stop: bool = True,
                 vol_window: int = 24 * 60,
                 min_vol_z: Optional[float] = 2.0):
        super().__init__(symbol, mom_lookback, entry_threshold_pct, hold_candles,
                         max_loss, fee_ratio, use_stop)
        self.min_vol_z = min_vol_z
        if min_vol_z is not None:
            self.indicators["vol_MA"] = VolumeMovingAverage(vol_window)
            self.indicators["vol_STD"] = VolumeRollingStd(vol_window)

        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"VolumeConfirmedMomentumStreamer initialized with params: "
            f"[mom_lookback={mom_lookback},entry_threshold_pct={entry_threshold_pct},"
            f"hold_candles={hold_candles},vol_window={vol_window},min_vol_z={min_vol_z}]")

    def decide_action(self, candle: Candle, status: Status) -> Action:
        action = super().decide_action(candle, status)

        # 플랫 상태의 진입 액션만 볼륨 게이트로 필터
        if self.min_vol_z is not None and status.position == 0 and action.quantity != 0:
            vol_ma = self.indicators["vol_MA"].get_latest()
            vol_std = self.indicators["vol_STD"].get_latest()
            if vol_ma is None or vol_std is None or vol_std <= 0:
                return Action(0)
            vol_z = (candle.volume - vol_ma) / vol_std
            if vol_z < self.min_vol_z:
                return Action(0)

        return action
