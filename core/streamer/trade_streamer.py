from typing import List

from core.backtest.status import Status
from core.streamer import BaseStreamer, Candle, Action
from core.streamer.indicator import MovingAverage
from core.utils import trunc_by_sign


class TradeStreamer(BaseStreamer):
    def __init__(self, symbol):
        super().__init__([symbol], {symbol: {
            'a': MovingAverage(10)
        }})

        self.symbol = symbol
        self.dir = 1

    def decide_action(self, symbol: str, candle: Candle, status: Status) -> List[Action]:
        position = status.position_for(symbol).position
        if position == 0:
            if self.dir == 1:
                qty = trunc_by_sign(status.total_margin() / candle.close * (1 + 0.0004), 3)
                self.dir = -1
                return [Action(symbol, qty)]
            else:
                qty = trunc_by_sign(-status.total_margin() / candle.close * (1 + 0.0004), 3)
                self.dir = 1
                return [Action(symbol, qty)]
        else:
            return [Action(symbol, -position)]
