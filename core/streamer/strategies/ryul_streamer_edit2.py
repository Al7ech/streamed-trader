from typing import Dict, List, Optional

from core.account.status import Status
from core.candle.candle import Candle
from core.order.action import Action
from core.streamer.indicator.atr import ATRIndicator
from core.streamer.strategies.ryul_streamer_edit1 import RyulStreamer_edit1


class RyulStreamer_edit2(RyulStreamer_edit1):
    """edit1 개선판 (2026-07-12 채택). edit1 대비 바뀐 것:

    - max_channel_pct 7.0 -> 6.0 (진입 채널폭 필터 강화)
    - win 청산 ATR 게이트: 수익 구간 청산 신호가 떠도 ATR(24h)이 직전 캔들보다 상승
      중이면 보류하고, ATR이 식었을 때만 청산 (atr_slope_lookback=1)
    - 단, 진입가 대비 수익 방향 이동이 3% 이상인(추세가 확인된) 포지션만 보류
      (min_defer_move_pct=3.0) — 횡보장에서 이익 없는 포지션까지 보류하며 생기던
      MDD를 잘라낸다. lose 청산·강제청산은 edit1 그대로 무조건 실행.

    ETH 6.5년(2020~2026-07, 1m) 검증: edit1 대비
    - (75,6,3): profit 9441% -> 10285%, Sharpe 1.93 -> 1.99, MDD 33.1% -> 28.1%
    - (55,8,3): profit 9257% -> 15882%, Sharpe 1.59 -> 1.74, MDD 38.3% -> 37.6%
    5심볼×2세트 strict(수익>= AND MDD<) 통과: 6.5년 5/10, 최근 1년 7/10 (BTC 양 세트
    일관 개선, XRP는 부적합). 연도별로 2020~2026 전 연도 흑자, 개선이 특정 연도에
    집중되지 않음. 민감도: minp 1~5%·lookback 1~240은 완만한 고원, max_channel_pct만
    5.5 이하에서 수익 절벽 있으니 조일 때 주의. max_loss는 수익<->MDD 다이얼
    (0.07이면 MDD 25.5%/34.3%, 수익은 복리로 감소).

    ``~/ryul-streamer``의 원본을 streamed-trader의 core API로 옮긴 것 — 로직은 동일하다.
    2026-09-11 백테스트 검증(ETHUSDT 1m, 2020-01-01~2026-07-24)으로 옛 FastBacktester 결과와
    끝자리까지 일치 확인: profit 9009.03%, Sharpe 1.9258, MDD 28.21%, 체결 727건
    (``docs/2609-backtest-profiling.md`` 참고 — 이 값은 그 문서의 end date 기준이라
    클래스 docstring 위 (75,6,3) 벤치마크의 end date와 다르다).
    """

    def __init__(self, symbols: List[str],
                 entry_length: int = 75 * 60,
                 win_exit_length: int = 6 * 60,
                 lose_exit_length: int = 3 * 60,
                 max_loss: float = 0.08,
                 atr_length: int = 24 * 60,
                 max_channel_pct: float = 6.0,
                 max_atr_pct: float = 0.11,
                 atr_slope_lookback: int = 1,
                 min_defer_move_pct: float = 3.0):
        super().__init__(symbols=symbols, entry_length=entry_length,
                         win_exit_length=win_exit_length, lose_exit_length=lose_exit_length,
                         max_loss=max_loss, atr_length=atr_length,
                         max_channel_pct=max_channel_pct, max_atr_pct=max_atr_pct)
        self.atr_slope_lookback = atr_slope_lookback
        self.min_defer_move_pct = min_defer_move_pct
        self._current_candle: Dict[str, Optional[Candle]] = {s: None for s in symbols}

        # _atr_decreasing이 read(-2-lookback)으로 읽으므로 조회 깊이가 파라미터에 걸린다.
        # 지표의 보관 이력을 넘으면 IndexError가 나므로, 필요한 만큼 잡아 다시 만든다.
        # (속성만 올리면 이미 만들어진 deque의 maxlen은 그대로라 조용한 불일치가 생긴다.
        #  같은 키에 다시 넣는 것이라 지표 순서는 유지된다.)
        needed = atr_slope_lookback + 3
        if needed > ATRIndicator.history_size:
            for s in symbols:
                self.indicators[s]["ATR"] = ATRIndicator(atr_length, history_size=needed)

        self.logger.info(
            f"RyulStreamer_edit2 params: [atr_slope_lookback={atr_slope_lookback},"
            f"min_defer_move_pct={min_defer_move_pct}]")

    def _decide_symbol(self, symbol: str, candle: Candle, status: Status) -> List[Action]:
        self._current_candle[symbol] = candle  # _long/_short_exit_price에서 close를 쓰기 위해 보관
        return super()._decide_symbol(symbol, candle, status)

    def _atr_decreasing(self, symbol: str) -> bool:
        # read(-2)가 "직전 봉까지" ATR (지표가 decide_action보다 먼저 갱신되므로
        # read(-1)은 이번 봉을 포함한다 — baseline/edit1과 같은 채널 규약).
        atr_now = self.indicators[symbol]["ATR"].read(-2)
        atr_prev = self.indicators[symbol]["ATR"].read(-2 - self.atr_slope_lookback)
        if atr_now is None or atr_prev is None:
            return True  # 워밍업 중에는 청산을 막지 않는다
        return atr_now < atr_prev

    def _is_win_exit_deferred(self, symbol: str, status: Status, side: float) -> bool:
        """수익 구간 청산을 보류할지. side=+1 롱, -1 숏 (숏은 부호 반전 대칭)."""
        if self._atr_decreasing(symbol):
            return False  # 변동성이 식기 시작했다 -> 예정대로 청산
        current_candle = self._current_candle[symbol]
        if current_candle is None:
            return False
        avg_price = status.position_for(symbol).avg_price
        move_pct = side * (current_candle.close - avg_price) / avg_price * 100
        return move_pct >= self.min_defer_move_pct  # 추세가 확인된 승자만 보류

    def _long_exit_price(self, symbol: str, status: Status) -> float:
        lose_exit_price = self.indicators[symbol]["lose_SV"].read(-2)
        if lose_exit_price < status.position_for(symbol).avg_price:
            return lose_exit_price  # 손실 구간: edit1 그대로 무조건 청산
        if self._is_win_exit_deferred(symbol, status, side=1.0):
            return float("-inf")  # 청산선 무효화 = 홀드
        return self.indicators[symbol]["win_SV"].read(-2)

    def _short_exit_price(self, symbol: str, status: Status) -> float:
        lose_exit_price = self.indicators[symbol]["lose_BV"].read(-2)
        if status.position_for(symbol).avg_price < lose_exit_price:
            return lose_exit_price
        if self._is_win_exit_deferred(symbol, status, side=-1.0):
            return float("inf")
        return self.indicators[symbol]["win_BV"].read(-2)
