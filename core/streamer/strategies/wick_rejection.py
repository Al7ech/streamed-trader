import logging
from typing import Dict, List

from core.domain.action import Action
from core.domain.candle import Candle
from core.domain.status import Status
from core.streamer.base_streamer import BaseStreamer
from core.streamer.indicator.atr import ATRIndicator
from core.utils import trunc_by_sign


class WickRejectionStreamer(BaseStreamer):
    """
    긴 아래꼬리 거부(rejection wick) 반전 롱 — 캔들스틱 패턴 전략 예제.

    아래꼬리 >= wick_atr_mult x ATR(60) 이고 종가가 봉 레인지 상단
    (close - low >= close_pos x range) 이면 다음 봉에 롱 진입, hold_candles 경과 시
    시간 청산. ATR은 인디케이터가 decide_action보다 먼저 업데이트되므로 이번 봉을 포함한
    최근 60봉 기준이다 (봉은 이미 닫혔으므로 look-ahead는 아니다).

    - 사이징: 스탑 거리(stop_atr_mult x ATR)에서의 손실이 max_loss가 되도록 레버리지, 6x 캡
    - use_stop=False면 같은 사이징을 유지한 채 시간 청산만 사용
    """

    def __init__(self, symbol: str,
                 wick_atr_mult: float = 2.5,
                 close_pos: float = 0.6,
                 hold_candles: int = 15,
                 stop_atr_mult: float = 2.0,
                 max_loss: float = 0.08,
                 use_stop: bool = True,
                 side: int = 1):
        super().__init__([symbol], {symbol: {
            "atr": ATRIndicator(60),
        }})
        self.wick_atr_mult = wick_atr_mult
        self.close_pos = close_pos
        self.hold_candles = hold_candles
        self.stop_atr_mult = stop_atr_mult
        self.max_loss = max_loss
        self.use_stop = use_stop
        self.side = side  # 1=아래꼬리 롱, -1=윗꼬리 숏 (대칭 진단용)

        self._hold_remaining = 0
        self._stop_price = 0.0

        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"WickRejectionStreamer initialized with params: [wick_atr_mult={wick_atr_mult},"
            f"close_pos={close_pos},hold_candles={hold_candles},stop_atr_mult={stop_atr_mult},"
            f"max_loss={max_loss},use_stop={use_stop},side={side}]")

    def decide_action(self, candles: Dict[str, Candle], status: Status) -> List[Action]:
        symbol = self.symbols[0]
        candle = candles.get(symbol)
        if candle is None:
            return []

        position = status.position_for(symbol).position
        if position != 0:
            self._hold_remaining -= 1

            if self.use_stop:
                if position > 0 and candle.low <= self._stop_price:
                    return [Action(symbol, -position)]
                if position < 0 and self._stop_price <= candle.high:
                    return [Action(symbol, -position)]

            if self._hold_remaining <= 0:
                return [Action(symbol, -position)]
            return []

        atr = self.indicators[symbol]["atr"].get_latest()
        if atr is None or atr <= 0:
            return []

        rng = candle.high - candle.low
        if rng <= 0:
            return []

        if self.side > 0:
            wick = min(candle.open, candle.close) - candle.low
            in_pos = (candle.close - candle.low) >= self.close_pos * rng
        else:
            wick = candle.high - max(candle.open, candle.close)
            in_pos = (candle.high - candle.close) >= self.close_pos * rng
        if wick < self.wick_atr_mult * atr or not in_pos:
            return []

        price = candle.close
        stop_frac = self.stop_atr_mult * atr / price
        lev = min(6.0, self.max_loss / stop_frac)
        qty = trunc_by_sign(self.side * status.total_margin() / (price * (1 / lev + status.fee_ratio)), 3)

        self._hold_remaining = self.hold_candles
        self._stop_price = price * (1 - self.side * stop_frac)
        return [Action(symbol, qty)]
