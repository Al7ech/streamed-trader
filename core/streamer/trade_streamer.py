from core.backtest.status import Status
from core.streamer import BaseStreamer, Candle, Action
from core.streamer.indicator import MovingAverage
from core.utils import trunc_by_sign


class TradeStreamer(BaseStreamer):
    def __init__(self, symbol):
        super().__init__({
            'a': MovingAverage(10)
        })

        self.symbol = symbol
        self.dir = 1

    def decide_action(self, candle: Candle, status: Status):
        if status.position == 0:
            if self.dir == 1:
                qty = trunc_by_sign(status.total_margin() / candle.close * (1 + 0.0004), 3)
                self.dir = -1
                return Action(qty)
            else:
                qty = trunc_by_sign(-status.total_margin() / candle.close * (1 + 0.0004), 3)
                self.dir = 1
                return Action(qty)
        else:
            return Action(-status.position)
