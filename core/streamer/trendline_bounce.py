import logging

from core.backtest.status import Status
from core.streamer.action import Action
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.streamer.indicator.atr import ATRIndicator
from core.streamer.indicator.pivot_trendline import PivotTrendlineIndicator
from core.utils import trunc_by_sign


class _TouchTracker:
    """같은 피벗페어(선) 위에서의 누적 터치 수. 연속 터치 봉 덩어리는 한 번으로 센다."""

    def __init__(self):
        self.pair = -1
        self.count = 0
        self._prev = False

    def update(self, touched: bool, pair_id: int) -> None:
        if pair_id != self.pair:
            self.pair = pair_id
            self.count = 0
            self._prev = False
        if touched and not self._prev:
            self.count += 1
        self._prev = touched


class TrendlineBounceStreamer(BaseStreamer):
    """
    피벗 추세선 터치 전략 — 기하학적(비-지표) 신호 예제.

    PivotTrendlineIndicator가 그린 지지/저항선에 가격이 닿는 순간을 신호로 쓴다.
    두 방향을 모두 지원한다:
    - side="bounce": 지지선 터치 롱 / 저항선 터치 숏 (선에서 튕긴다는 가설)
    - side="inverse": 저항선 터치 롱 / 지지선 터치 숏 (선을 뚫고 추세가 이어진다는 가설)
    lines로 사용할 선을 고른다 ("res"/"sup"/"both").

    - 터치: 저항선은 high >= 선 − eps×ATR60 AND close < 선, 지지선은 대칭.
      같은 선(피벗페어) 누적 터치 수가 min_touch 이상일 때만 진입, 양쪽 동시 터치 봉은 스킵.
    - 청산: hold_candles 경과 시 무조건.
      use_stop=True면 진입가 대비 stop_atr_mult×ATR 역행 시 하드스탑도 추가.
    - 사이징: 스탑 거리(stop_atr_mult×ATR)에서 max_loss, 6x 캡 (use_stop과 무관하게 동일).
    """

    def __init__(self, symbol: str,
                 pivot_k: int = 720,
                 eps: float = 0.5,
                 min_touch: int = 1,
                 hold_candles: int = 240,
                 side: str = "inverse",
                 lines: str = "res",
                 stop_atr_mult: float = 15.0,
                 max_loss: float = 0.08,
                 fee_ratio: float = 0.0004,
                 use_stop: bool = False):
        assert side in ("inverse", "bounce")
        assert lines in ("res", "sup", "both")
        indicators = {"ATR": ATRIndicator(60)}
        if lines in ("res", "both"):
            indicators["RES"] = PivotTrendlineIndicator(pivot_k, mode="high")
        if lines in ("sup", "both"):
            indicators["SUP"] = PivotTrendlineIndicator(pivot_k, mode="low")
        super().__init__(indicators)
        self.symbol = symbol
        self.eps = eps
        self.min_touch = min_touch
        self.hold_candles = hold_candles
        self.side = side
        self.lines = lines
        self.stop_atr_mult = stop_atr_mult
        self.max_loss = max_loss
        self.fee_ratio = fee_ratio
        self.use_stop = use_stop

        self._hold_remaining = 0
        self._stop_price = 0.0
        self._res_touches = _TouchTracker()
        self._sup_touches = _TouchTracker()

        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"TrendlineBounceStreamer initialized with params: [pivot_k={pivot_k},eps={eps},"
            f"min_touch={min_touch},hold_candles={hold_candles},side={side},lines={lines},"
            f"max_loss={max_loss}]")

    def _touch(self, name: str, tracker: _TouchTracker, candle: Candle, atr: float) -> bool:
        """해당 선의 이번 봉 터치 여부 + 트래커 갱신. 인디케이터는 decide 후 업데이트되므로
        선값은 직전 봉 기준 — 기울기로 1봉 외삽해 현재 봉의 선값을 얻는다."""
        ind = self.indicators.get(name)
        if ind is None:
            return False
        line = ind.get_latest()
        if line is None:
            return False
        line += ind.slope
        if name == "RES":
            touched = candle.high >= line - self.eps * atr and candle.close < line
        else:
            touched = candle.low <= line + self.eps * atr and candle.close > line
        tracker.update(touched, ind.pair_id)
        return touched and tracker.count >= self.min_touch

    def decide_action(self, candle: Candle, status: Status) -> Action:
        atr = self.indicators["ATR"].get_latest()
        if atr is None or atr <= 0:
            return Action(0)

        # 터치 상태는 포지션 여부와 무관하게 매 봉 갱신한다.
        res_entry = self._touch("RES", self._res_touches, candle, atr)
        sup_entry = self._touch("SUP", self._sup_touches, candle, atr)

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

        if res_entry == sup_entry:  # 무신호 또는 양쪽 동시 터치(상충) — 스킵
            return Action(0)

        if self.side == "bounce":
            sign = 1 if sup_entry else -1
        else:
            sign = 1 if res_entry else -1

        price = candle.close
        stop_frac = self.stop_atr_mult * atr / price
        lev = min(6.0, self.max_loss / stop_frac)
        qty = trunc_by_sign(sign * status.total_margin() / (price * (1 / lev + self.fee_ratio)), 3)

        self._hold_remaining = self.hold_candles
        self._stop_price = price * (1 - sign * stop_frac)
        return Action(qty)
