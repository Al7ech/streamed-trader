import logging

from core.backtest.status import Status
from core.streamer.action import Action
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.streamer.indicator.moving_average import MovingAverage
from core.utils import trunc_by_sign


class MomentumTimeExitStreamer(BaseStreamer):
    """
    단기 모멘텀 + 고정시간 청산.

    - 진입: 최근 mom_lookback 캔들 수익률이 ±entry_threshold_pct% 이상이면 그 방향으로 진입
    - 청산: hold_candles 경과 시 무조건 청산 (추적 청산의 churn 없이 모멘텀 지속성만
      순수하게 본다), 또는 진입가 대비 threshold만큼 역행 시 하드스탑
    - 사이징: 스탑 거리(threshold%)에서의 손실이 max_loss가 되도록 레버리지 설정, 6x 캡

    "close_hist"는 MovingAverage(1)을 종가 시계열로 재사용한 것 (get_index(-N) =
    N캔들 전 종가, 현재 캔들 제외 — 인디케이터는 decide_action 후에 업데이트되므로).
    """

    def __init__(self, symbol: str,
                 mom_lookback: int = 60,
                 entry_threshold_pct: float = 1.0,
                 hold_candles: int = 120,
                 max_loss: float = 0.08,
                 fee_ratio: float = 0.0004,
                 use_stop: bool = True):
        super().__init__({
            # get_index(-mom_lookback)로 읽으므로 조회 깊이가 파라미터에 걸린다.
            # 기본 이력(BaseIndicator.history_size)을 넘는 lookback을 줘도 깨지지 않게 맞춰 둔다.
            "close_hist": MovingAverage(
                1, history_size=max(MovingAverage.history_size, mom_lookback + 2)),
        })
        self.symbol = symbol
        self.mom_lookback = mom_lookback
        self.entry_threshold_pct = entry_threshold_pct
        self.hold_candles = hold_candles
        self.max_loss = max_loss
        self.fee_ratio = fee_ratio
        # 스탑 없이 시간 청산만 쓰는 진단 모드 (스탑의 인트라바 꼬리 체결 효과 분리용).
        # 사이징은 동일하게 유지해 비교 가능성 확보.
        self.use_stop = use_stop

        self._hold_remaining = 0
        self._stop_price = 0.0

        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"MomentumTimeExitStreamer initialized with params: [mom_lookback={mom_lookback},"
            f"entry_threshold_pct={entry_threshold_pct},hold_candles={hold_candles},"
            f"max_loss={max_loss}]")

    def decide_action(self, candle: Candle, status: Status) -> Action:
        if status.position != 0:
            self._hold_remaining -= 1

            if self.use_stop:
                if status.position > 0 and candle.low <= self._stop_price:
                    return Action(-status.position)
                if status.position < 0 and self._stop_price <= candle.high:
                    return Action(-status.position)

            if self._hold_remaining <= 0:
                return Action(-status.position)
            return Action(0)

        ref = self.indicators["close_hist"].get_index(-self.mom_lookback)
        if ref is None:
            return Action(0)

        price = candle.close
        momentum_pct = (price / ref - 1) * 100
        if abs(momentum_pct) < self.entry_threshold_pct:
            return Action(0)

        sign = 1 if momentum_pct > 0 else -1
        stop_frac = self.entry_threshold_pct / 100
        lev = min(6.0, self.max_loss / stop_frac)
        qty = trunc_by_sign(sign * status.total_margin() / (price * (1 / lev + self.fee_ratio)), 3)

        self._hold_remaining = self.hold_candles
        self._stop_price = price * (1 - sign * stop_frac)
        return Action(qty)
