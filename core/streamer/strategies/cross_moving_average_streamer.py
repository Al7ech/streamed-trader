from typing import Dict, List

from core.domain.action import Action
from core.domain.candle import Candle
from core.domain.status import Status
from core.streamer.base_streamer import BaseStreamer
from core.streamer.indicator.moving_average import MovingAverage
from core.utils import trunc_by_sign


class CrossMovingAverageStreamer(BaseStreamer):
    """
    MA10/MA25 크로스 always-in 전략 — 이 프레임워크의 가장 단순한 예제.

    - 진입/반전: MA10이 MA25를 상향 돌파(골든크로스)하면 풀 롱, 하향 돌파(데드크로스)하면
      풀 숏. 크로스가 일어난 캔들에서만 액션을 내고, 그 사이에는 포지션을 유지한다.
    - 청산: 별도 청산 조건 없음 — 반대 크로스가 곧 청산이다.
    - 사이징: 레버리지 1x. 목표 수량은 `long_safe_qty`/`short_safe_qty`가 계산하는데,
      진입 수수료를 먼저 떼고 남은 증거금으로 살 수 있는 양을 구하므로 체결 직후
      마진이 음수로 떨어지지 않는다.

    `decide_action`이 반환하는 `Action`은 목표 포지션이 아니라 **현재 포지션에 더할 증감분**
    이라는 점에 주의 (`target_qty - position`).
    """

    def __init__(self, symbol: str):
        super().__init__([symbol], {symbol: {
            "ma10": MovingAverage(10),
            "ma25": MovingAverage(25),
        }})
        self.symbol = symbol

    def decide_action(self, candles: Dict[str, Candle], status: Status) -> List[Action]:
        symbol = self.symbols[0]
        candle = candles.get(symbol)
        if candle is None:
            return []

        ind = self.indicators[symbol]
        ma10 = ind["ma10"].get_latest()
        ma25 = ind["ma25"].get_latest()
        prev_ma10 = ind["ma10"].read(-2)
        prev_ma25 = ind["ma25"].read(-2)

        # 워밍업 중에는 지표가 None이다 (MA25 기준 25봉 + 이전 값 1봉).
        if prev_ma10 is None or prev_ma25 is None or ma10 is None or ma25 is None:
            return []

        position = status.position_for(symbol).position

        # 크로스가 발생할 때만 매수/매도
        if prev_ma10 <= prev_ma25 and ma10 > ma25:  # 골든크로스
            target_qty = long_safe_qty(status.total_margin(), position, candle.close, 3,
                                       status.fee_ratio)
            return [Action(symbol, target_qty - position)]
        if prev_ma10 >= prev_ma25 and ma10 < ma25:  # 데드크로스
            target_qty = short_safe_qty(status.total_margin(), position, candle.close, 3,
                                        status.fee_ratio)
            return [Action(symbol, target_qty - position)]

        return []


def long_safe_qty(margin: float, position: float, price: float, ndigits: int, fee_ratio: float) -> float:
    ideal_target_qty = trunc_by_sign(margin / price, ndigits)
    expected_fee = abs(ideal_target_qty - position) * price * fee_ratio
    return trunc_by_sign((margin - expected_fee) / price, ndigits)


def short_safe_qty(margin: float, position: float, price: float, ndigits: int, fee_ratio: float) -> float:
    ideal_target_qty = trunc_by_sign(-margin / price, ndigits)
    expected_fee = abs(ideal_target_qty - position) * price * fee_ratio
    return trunc_by_sign(-(margin - expected_fee) / price, ndigits)
