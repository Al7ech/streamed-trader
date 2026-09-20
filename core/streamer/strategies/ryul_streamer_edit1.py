import logging
from typing import Dict, List

from core.account.status import Status
from core.candle.candle import Candle
from core.order.action import Action
from core.streamer.base_streamer import BaseStreamer
from core.streamer.indicator.atr import ATRIndicator
from core.streamer.indicator.donchian_channel import MinDonchianIndicator, MaxDonchianIndicator
from core.utils import trunc_by_sign


class RyulStreamer_edit1(BaseStreamer):
    """
    RyulStreamer 개선판. 돌파/청산 로직은 baseline과 동일하고, 진입 레짐 필터 두 개를 추가했다.

    ETH 1년(2025-07~2026-07, 1m) 진입별 분석 결과 baseline의 손실은
    (1) 이미 크게 움직인 뒤의 넓은 채널 돌파 추격 진입,
    (2) 고변동성 구간 진입에 집중되어 있었다. 이를 걸러낸다:

    - max_channel_pct: 진입 채널 폭(BV-SV)이 가격 대비 이 비율(%)보다 넓으면 진입 스킵 (0=off)
    - max_atr_pct: ATR이 가격 대비 이 비율(%)을 넘으면 진입 스킵 (0=off)

    같은 기간 ETH 백테스트 기준 baseline 대비 profit +5.47% -> +54.48%,
    MDD 33.93% -> 15.78% ((75,6,3) 세트). (55,8,3) 세트와 XRP 3개월에서도 일관되게 개선 확인.

    ATR/BV/SV/win/lose 채널은 모두 read(-2)로 읽는다 (지표가 decide_action보다 먼저
    갱신되므로 get_latest()는 이번 봉을 포함해, -2가 baseline과 같은 "직전 봉까지" 값이다).

    사이징의 수수료율은 ``status.fee_ratio``를 읽는다 — 계좌가 실제로 부과하는 값과 사이징
    기준이 한 값이 되도록 (core 규약). 백테스트는 ``SimulatedExecutor(fee_ratio=...)``로,
    라이브는 거래소 taker 수수료 티어로 정해진다.

    ``~/ryul-streamer``의 원본을 streamed-trader의 core API(``read(idx)``,
    ``decide_action(candles, status)``)로 옮긴 것 — 로직은 동일하다.
    """

    def __init__(self, symbols: List[str],
                 entry_length: int = 75 * 60,
                 win_exit_length: int = 6 * 60,
                 lose_exit_length: int = 3 * 60,
                 max_loss: float = 0.08,
                 atr_length: int = 24 * 60,
                 max_channel_pct: float = 7.0,
                 max_atr_pct: float = 0.11):
        super().__init__(symbols, {s: {
            "BV": MaxDonchianIndicator(entry_length),
            "SV": MinDonchianIndicator(entry_length),
            "win_BV": MaxDonchianIndicator(win_exit_length),
            "win_SV": MinDonchianIndicator(win_exit_length),
            "lose_BV": MaxDonchianIndicator(lose_exit_length),
            "lose_SV": MinDonchianIndicator(lose_exit_length),
            "ATR": ATRIndicator(atr_length),
        } for s in symbols})
        self.max_loss = max_loss
        self.max_channel_pct = max_channel_pct
        self.max_atr_pct = max_atr_pct

        # Circuit Breaker Config (심볼별 독립)
        self._entry_safe_price_ratio = 0.015  # 1.5%
        self._circuit_breaker_duration_candles = 24 * 60  # 1 day
        self._circuit_breaker_count: Dict[str, int] = {s: 0 for s in symbols}

        # Logging
        self.logger = logging.getLogger(__name__)

        self.logger.info(
            f"RyulStreamer_edit1 initialized with params: [entry_length={entry_length},"
            f"win_exit_length={win_exit_length},lose_exit_length={lose_exit_length},max_loss={max_loss},"
            f"atr_length={atr_length},max_channel_pct={max_channel_pct},max_atr_pct={max_atr_pct}]")

    def decide_action(self, candles: Dict[str, Candle], status: Status) -> List[Action]:
        # 심볼별 독립 판단 (크로스심볼 커플링 없음). 이 이벤트에 마감한 심볼만 순회한다.
        actions: List[Action] = []
        for symbol in self.symbols:
            candle = candles.get(symbol)
            if candle is not None:
                actions.extend(self._decide_symbol(symbol, candle, status))
        return actions

    def _decide_symbol(self, symbol: str, candle: Candle, status: Status) -> List[Action]:
        if self._circuit_breaker_count[symbol] > 0:
            self._circuit_breaker_count[symbol] -= 1
            return []

        price = candle.close
        position = status.position_for(symbol)
        if position.position == 0:
            BV = self.indicators[symbol]["BV"].read(-2)
            SV = self.indicators[symbol]["SV"].read(-2)

            if not BV or not SV:
                return []

            if self._is_entry_blocked(symbol, price, BV, SV):
                return []

            if BV <= price and self._is_safe_price(symbol, price, BV):
                delta = BV - SV
                lev = min(6.0, self.max_loss * price / delta)
                # 체결은 BV/SV가 아니라 종가에 일어나므로(백테스터도, 실거래도 봉이 닫힐 때
                # 시장가) 사이징 기준가도 종가여야 실효 레버리지가 lev와 일치한다. 레벨을
                # 기준으로 재면 돌파 오버슈트만큼 롱은 과대·숏은 과소 사이징된다.
                qty = trunc_by_sign(status.total_margin() / (price * (1 / lev + status.fee_ratio)), 3)
                return [Action(symbol, qty)]

            if price <= SV and self._is_safe_price(symbol, price, SV):
                delta = BV - SV
                lev = min(6.0, self.max_loss * price / delta)
                qty = trunc_by_sign(-status.total_margin() / (price * (1 / lev + status.fee_ratio)), 3)
                return [Action(symbol, qty)]

        if position.position > 0:
            if candle.low < self._long_exit_price(symbol, status):
                return [Action(symbol, -position.position)]

        if position.position < 0:
            if self._short_exit_price(symbol, status) < candle.high:
                return [Action(symbol, -position.position)]

        return []

    def _long_exit_price(self, symbol: str, status: Status) -> float:
        """롱 청산선: 손실 구간이면 lose 채널, 수익 구간이면 win 채널."""
        win_exit_price = self.indicators[symbol]["win_SV"].read(-2)
        lose_exit_price = self.indicators[symbol]["lose_SV"].read(-2)
        return lose_exit_price if lose_exit_price < status.position_for(symbol).avg_price else win_exit_price

    def _short_exit_price(self, symbol: str, status: Status) -> float:
        """숏 청산선: 손실 구간이면 lose 채널, 수익 구간이면 win 채널."""
        win_exit_price = self.indicators[symbol]["win_BV"].read(-2)
        lose_exit_price = self.indicators[symbol]["lose_BV"].read(-2)
        return lose_exit_price if status.position_for(symbol).avg_price < lose_exit_price else win_exit_price

    def _is_entry_blocked(self, symbol: str, price: float, BV: float, SV: float) -> bool:
        """진입 레짐 필터. 하나라도 걸리면 이번 캔들 진입을 막는다."""
        # 채널 폭이 가격 대비 너무 넓음 -> 이미 크게 움직인 뒤의 추격 진입
        if self.max_channel_pct > 0 and (BV - SV) / price * 100 > self.max_channel_pct:
            return True

        # 변동성이 너무 큼 -> 고변동성 구간 회피
        if self.max_atr_pct > 0:
            atr = self.indicators[symbol]["ATR"].read(-2)
            if atr is None or atr / price * 100 > self.max_atr_pct:
                return True

        return False

    def _is_safe_price(self, symbol: str, current_price: float, target_price: float) -> bool:
        """
        decides if current price is normal, so it is safe to trade
        :param current_price: current price of symbol. (normally close value of candle)
        :param target_price: the ideal entry price
        :return: True if safe, False otherwise
        """
        if self._entry_safe_price_ratio <= abs(target_price - current_price) / target_price:
            self.logger.warning("circuit breaker activated")
            self._circuit_breaker_count[symbol] = self._circuit_breaker_duration_candles
            return False
        return True
