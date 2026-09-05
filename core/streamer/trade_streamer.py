from typing import List

from core.engine.status import Status
from core.streamer import BaseStreamer, Candle, Action
from core.streamer.indicator import MovingAverage
from core.utils import trunc_by_sign


class TradeStreamer(BaseStreamer):
    """실전 전략이 아니라 매 봉마다 롱/숏을 번갈아 진입하는 픽스처(fixture)용 스트리머."""

    def __init__(self, symbol):
        super().__init__([symbol], {symbol: {
            # 실제 진입 판단에는 쓰이지 않는, 지표 배관(indicator plumbing) 검증용 더미 지표
            'a': MovingAverage(10)
        }})

        self.symbol = symbol
        self.dir = 1  # 다음 진입 방향: 1이면 롱, -1이면 숏

    def decide_action(self, symbol: str, candle: Candle, status: Status) -> List[Action]:
        position = status.position_for(symbol).position
        if position == 0:
            # 포지션이 없으면 dir 방향으로 전액 진입하고 다음 번을 위해 방향을 뒤집는다.
            # 수수료(0.0004)만큼 여유를 두어 반올림으로 마진을 초과 주문하지 않도록 한다.
            if self.dir == 1:
                qty = trunc_by_sign(status.total_margin() / candle.close * (1 + 0.0004), 3)
                self.dir = -1
                return [Action(symbol, qty)]
            else:
                qty = trunc_by_sign(-status.total_margin() / candle.close * (1 + 0.0004), 3)
                self.dir = 1
                return [Action(symbol, qty)]
        else:
            # 포지션이 있으면 바로 다음 봉에서 청산 -> 매 봉 진입/청산이 반복된다.
            return [Action(symbol, -position)]
