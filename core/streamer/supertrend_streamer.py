import logging

from core.backtest.status import Status
from core.streamer.action import Action
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.streamer.indicator.supertrend import SupertrendIndicator
from core.utils import trunc_by_sign


class SupertrendStreamer(BaseStreamer):
    """
    Supertrend always-in 플립 추세추종.

    ATR 트레일링 라인이 청산 역할까지 겸하므로 별도 청산 조건이 없다 — 항상 롱
    아니면 숏이고, 추세가 뒤집히는 순간에만 거래가 발생한다.

    - 추세 방향(+1/−1)과 포지션 방향이 다르면 반전(플립), 같으면 유지
    - 사이징: 스탑 거리 = |price − supertrend 라인|에서 max_loss, 6x 캡
    """

    def __init__(self, symbol: str,
                 atr_window: int = 24 * 60,
                 multiplier: float = 3.0,
                 max_loss: float = 0.08,
                 fee_ratio: float = 0.0004):
        super().__init__({
            "ST": SupertrendIndicator(atr_window, multiplier),
        })
        self.symbol = symbol
        self.max_loss = max_loss
        self.fee_ratio = fee_ratio

        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"SupertrendStreamer initialized with params: [atr_window={atr_window},"
            f"multiplier={multiplier},max_loss={max_loss}]")

    def decide_action(self, candle: Candle, status: Status) -> Action:
        st = self.indicators["ST"]
        line = st.get_latest()
        direction = st.get_direction(-1)
        if line is None or direction is None:
            return Action(0)

        # 이미 추세 방향 포지션이면 유지
        if status.position > 0 and direction == 1:
            return Action(0)
        if status.position < 0 and direction == -1:
            return Action(0)

        price = candle.close
        dist = abs(price - line)
        if dist <= 0:
            return Action(0)

        lev = min(6.0, self.max_loss * price / dist)
        target_qty = trunc_by_sign(
            direction * status.total_margin() / (price * (1 / lev + self.fee_ratio)), 3)
        return Action(target_qty - status.position)
