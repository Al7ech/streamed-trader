import logging

from core.backtest.status import Status
from core.streamer.action import Action
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.streamer.indicator.atr import ATRIndicator
from core.streamer.indicator.moving_average import MovingAverage
from core.utils import trunc_by_sign


class KeltnerStreamer(BaseStreamer):
    """
    Keltner/ATR 채널 돌파 추세추종.

    고정폭 채널과 달리 채널폭이 변동성 적응형(m×ATR)이라, 이미 크게 움직인 뒤에
    뒤늦게 따라 들어가는 추격 진입이 구조적으로 줄어든다.

    - 진입(플랫): close > MA + m_entry×ATR → 롱, close < MA − m_entry×ATR → 숏
    - 청산: 롱은 candle.low < MA − m_exit×ATR (m_exit=0이면 중심선 복귀), 숏 대칭
    - 사이징: 스탑 거리 ≈ (m_entry+m_exit)×ATR에서의 손실이 max_loss가 되도록
      레버리지 설정, 6x 캡
    """

    def __init__(self, symbol: str,
                 window: int = 20 * 60,
                 m_entry: float = 2.0,
                 m_exit: float = 0.0,
                 max_loss: float = 0.08,
                 fee_ratio: float = 0.0004):
        super().__init__({
            "MA": MovingAverage(window),
            "ATR": ATRIndicator(window),
        })
        self.symbol = symbol
        self.m_entry = m_entry
        self.m_exit = m_exit
        self.max_loss = max_loss
        self.fee_ratio = fee_ratio

        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"KeltnerStreamer initialized with params: [window={window},m_entry={m_entry},"
            f"m_exit={m_exit},max_loss={max_loss}]")

    def decide_action(self, candle: Candle, status: Status) -> Action:
        ma = self.indicators["MA"].get_latest()
        atr = self.indicators["ATR"].get_latest()
        if ma is None or atr is None or atr <= 0:
            return Action(0)

        price = candle.close

        if status.position == 0:
            upper = ma + self.m_entry * atr
            lower = ma - self.m_entry * atr
            dist = (self.m_entry + self.m_exit) * atr
            lev = min(6.0, self.max_loss * price / dist)

            if upper <= price:
                qty = trunc_by_sign(status.total_margin() / (price * (1 / lev + self.fee_ratio)), 3)
                return Action(qty)
            if price <= lower:
                qty = trunc_by_sign(-status.total_margin() / (price * (1 / lev + self.fee_ratio)), 3)
                return Action(qty)

        if status.position > 0 and candle.low < ma - self.m_exit * atr:
            return Action(-status.position)
        if status.position < 0 and ma + self.m_exit * atr < candle.high:
            return Action(-status.position)

        return Action(0)
