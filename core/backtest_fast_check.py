"""Parity + benchmark check: run_backtest(vectorized=True) vs vectorized=False (reference).

Runs both engine paths over the same cached candles and asserts identical trades, final
margin, max leverage and equity curve. Also exercises the mixed path by wrapping one
indicator so it loses its `precompute_series` override and must be loop-updated, and a set of
multi-symbol cases (overlapping/staggered symbols, cross-symbol actions, shared-margin forced
liquidation across symbols).

    uv run python core/backtest_fast_check.py          # last 12 months
    uv run python core/backtest_fast_check.py --full   # 2020-01 ~ now (slow reference run)

KNOWN: ``--full`` currently reports a mismatch on some cases, and it is a pre-existing
numerical issue rather than a logic divergence. An incremental running sum (`MovingAverage`,
`ATRIndicator`) rounds differently from pandas' rolling sum used by `precompute_series`, so
the two paths disagree on ~99% of candles by up to ~4e-14 (MA) / ~3e-12 (ATR) relative,
starting at the first post-warm-up candle. Over 3.4M candles that flips a handful of
threshold comparisons and changes a few trades out of several thousand. The 12-month run
happens to have no such coincidence. Exact bit-parity between an O(1) incremental update and
a vectorized rolling sum is not achievable without changing one side's arithmetic; treat a
small `--full` trade-count delta as this, and any structural difference as a real bug.

The same rounding reaches `Trade.price` now that resting orders exist: a stop whose trigger is
an indicator value (`ma - m_exit*atr`) fills *at that value*, so the MA divergence lands in the
fill price on **every** such fill rather than only when it flips a threshold. `compare_reports`
therefore compares `price` with the same tolerance it already uses for wnl/fee/leverage. A
structurally wrong fill price (close instead of trigger, wrong bar, wrong side) differs by
orders of magnitude, so the tolerance costs no real coverage.
"""
import math
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np

from core.binance_candle_fetcher.vision_fetcher import BinanceVisionFetcher
from core.engine.backtest import run_backtest
from core.engine.report import Report
from core.engine.status import PositionState, Status
from core.engine.vectorized import ArrayIndicator
from core.logging_config import setup_logging
from core.streamer.action import Action, ActionType
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.streamer.indicator.atr import ATRIndicator
from core.streamer.indicator.base_indicator import BaseIndicator, NumericIndicator
from core.streamer.indicator.moving_average import MovingAverage
from core.streamer.keltner_stop_streamer import KeltnerStopStreamer
from core.streamer.keltner_streamer import KeltnerStreamer
from core.streamer.mean_reversion_zscore import MeanReversionZScoreStreamer
from core.utils import trunc_by_sign


def run_pair(make_streamer, candles_by_symbol, make_initial_status=None,
             **kw) -> Tuple[Report, Report]:
    """같은 입력을 참조 경로와 벡터화 경로로 각각 돌려 (ref, fast) Report를 돌려준다.

    스트리머도 Status도 **인스턴스가 아니라 팩토리**로 받는다. 둘 다 실행 중에 변형되므로
    (스트리머는 MeanReversionZScoreStreamer의 타임아웃 카운터 같은 자체 상태를, Status는
    포지션/증거금을) 하나를 공유하면 앞의 실행이 뒤의 시작 상태를 오염시킨다.
    """
    ref = run_backtest(make_streamer(), candles_by_symbol, vectorized=False,
                       initial_status=make_initial_status() if make_initial_status else None,
                       **kw)
    fast = run_backtest(make_streamer(), candles_by_symbol, vectorized=True,
                        initial_status=make_initial_status() if make_initial_status else None,
                        **kw)
    return ref, fast


class LoopOnlyIndicator(BaseIndicator):
    """Hides the wrapped indicator's `precompute_series` override, forcing the loop path."""

    def __init__(self, inner: BaseIndicator):
        super().__init__()
        self.window = inner.window
        self._inner = inner

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        self._inner.update(candle, status)

    def read(self, idx: int) -> Optional[float]:
        return self._inner.read(idx)


class EquityIndicator(NumericIndicator):
    """매 캔들의 거래 전 시가평가 자본을 기록하는 status 의존 지표 (검사용).

    ``status``를 읽으므로 벡터화가 불가능하고 항상 루프 경로에 남는다. decide_action보다 먼저
    갱신되므로 "지표 갱신은 시가평가 **후**의 status를 봐야 한다"는 규약을 직접 찌른다.
    """

    scale_group = "balance"

    def __init__(self):
        super().__init__()
        self.window = 1

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        # 라이브 프리피드는 status=None으로 부른다 — 워밍업으로 취급한다.
        if status is None:
            return
        self._deque.append(status.total_margin())


class EquityGatedKeltner(KeltnerStreamer):
    """Keltner에 "자본이 직전 대비 줄면 즉시 청산" 규칙을 얹은 픽스처.

    청산 판단이 EquityIndicator의 값에 직접 걸리므로, 지표 갱신이 보는 status가 한 캔들
    어긋나면 청산 시점이 달라지고 체결 목록이 갈린다.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        equity = EquityIndicator()
        self.indicators[self.symbols[0]]["EQ"] = equity

    def decide_action(self, symbol, candle: Candle, status: Status):
        position = status.position_for(symbol).position
        if position != 0.0:
            now = self.indicators[symbol]["EQ"].read(-1)
            prev = self.indicators[symbol]["EQ"].read(-2)
            if now is not None and prev is not None and now < prev:
                return [Action(symbol, -position)]
        return super().decide_action(symbol, candle, status)


class BlowUpStreamer(BaseStreamer):
    """파산하도록 과레버리지로 한 방 잡고 버티는 픽스처 (전략이 아니라 검사용).

    강제청산은 두 백테스터가 **스트리머를 우회해서** 내는 유일한 액션인데, 위의 정상 케이스들은
    이 경로를 한 번도 밟지 않는다. 자본의 `leverage` 배 명목가치를 심볼 수만큼 나눠 잡으면
    반대로 몇 % 만 움직여도 시가평가 자본이 0을 뚫으므로, 손실측 강제청산과 그 뒤의
    '파산 & flat' 분기가 함께 검사된다. 심볼이 여럿이면 증거금 공유 풀에서 한 심볼의 손실이
    나머지 심볼까지 전부 청산시키는 경로를 검사한다.
    """

    def __init__(self, symbols: List[str], leverage: float = 50.0, fee_ratio: float = 0.0004):
        super().__init__(symbols, {s: {} for s in symbols})
        self.leverage = leverage
        self.fee_ratio = fee_ratio

    def decide_action(self, symbol, candle: Candle, status: Status):
        position = status.position_for(symbol).position
        if position != 0.0:
            return []
        qty = round(status.total_margin() * self.leverage / len(self.symbols) / candle.close, 3)
        return [Action(symbol, qty)]


class DualSymbolMomentum(BaseStreamer):
    """두 심볼에 대해 완전히 독립된 단순 모멘텀 로직을 한 스트리머에서 돌리는 픽스처.

    "여러 심볼, 서로 무관한 결정" 케이스 — 각 심볼의 이벤트가 그 심볼 자신의 행동만 낳는지
    (다른 심볼로 새는 액션이 없는지) 검사한다.
    """

    def __init__(self, symbols: List[str], window: int = 20, fee_ratio: float = 0.0004):
        indicators = {s: {"MA": MovingAverage(window)} for s in symbols}
        super().__init__(symbols, indicators)
        self.fee_ratio = fee_ratio

    def decide_action(self, symbol, candle: Candle, status: Status):
        ma = self.indicators[symbol]["MA"].get_latest()
        if ma is None:
            return []
        position = status.position_for(symbol).position
        if position == 0 and candle.close > ma * 1.001:
            qty = trunc_by_sign(status.total_margin() / len(self.symbols) / candle.close, 3)
            return [Action(symbol, qty)]
        if position != 0 and candle.close < ma * 0.999:
            return [Action(symbol, -position)]
        return []


class LimitLadderStreamer(BaseStreamer):
    """토이 지정가 전략 (검사 전용) — 미체결 주문이 여러 봉에 걸쳐 사는 경로를 만든다.

    플랫이면 종가보다 ``spread`` 만큼 아래에 지정가 매수를 걸고, 포지션이 있으면 진입가보다
    ``spread`` 위에 지정가 매도(익절)를 건다. 주문이 ``ttl`` 캔들 안에 안 채워지면 만료된다.
    가끔 전량 취소를 섞어 CANCEL 경로도 태운다.

    지정가는 ``candle.close``에서만 유도되므로 (지표를 거치지 않는다) 두 엔진의 체결가가
    비트 단위로 같다 — 구조적 버그가 float 반올림에 묻히지 않는다.
    """

    def __init__(self, symbols: List[str], spread: float = 0.002, ttl: int = 30,
                 fee_ratio: float = 0.0004):
        super().__init__(symbols, {s: {} for s in symbols})
        self.spread = spread
        self.ttl = ttl
        self.fee_ratio = fee_ratio
        self._bar = 0

    def decide_action(self, symbol, candle: Candle, status: Status):
        self._bar += 1
        position = status.position_for(symbol).position
        resting = status.open_orders_for(symbol)

        # 200봉마다 전량 취소 — CANCEL 경로와, 취소 뒤 장부가 실제로 비는지를 태운다.
        if self._bar % 200 == 0 and resting:
            return [Action.cancel(symbol)]
        if resting:
            return []  # 이미 걸어둔 주문이 있으면 그대로 둔다

        if position == 0:
            qty = trunc_by_sign(status.total_margin() / len(self.symbols) / candle.close * 0.5, 3)
            if qty == 0:
                return []
            return [Action(symbol, qty, order_type=ActionType.LIMIT,
                           price=candle.close * (1 - self.spread),
                           client_id="entry", expire_after_candles=self.ttl)]

        avg = status.position_for(symbol).avg_price
        return [Action(symbol, -position, order_type=ActionType.LIMIT,
                       price=avg * (1 + self.spread), reduce_only=True, client_id="exit")]


class StopLadderStreamer(LimitLadderStreamer):
    """위와 같지만 청산을 ``reduce_only`` 조건부 시장가(손절)로 건다.

    ``reduce_only`` clamp, 트리거 방향 자동 유도, 갭 관통 체결(트리거보다 아래에서 봉이
    시작하면 시가 체결)을 모두 태운다.
    """

    def decide_action(self, symbol, candle: Candle, status: Status):
        self._bar += 1
        position = status.position_for(symbol).position
        resting = status.open_orders_for(symbol)
        if self._bar % 200 == 0 and resting:
            return [Action.cancel(symbol)]
        if resting:
            return []
        if position == 0:
            qty = trunc_by_sign(status.total_margin() / len(self.symbols) / candle.close * 0.5, 3)
            if qty == 0:
                return []
            return [Action(symbol, qty)]  # 시장가 진입
        avg = status.position_for(symbol).avg_price
        # 일부러 포지션보다 큰 수량을 건다 — reduce_only clamp가 안 걸리면 반대 포지션이 열린다.
        return [Action(symbol, -position * 3, order_type=ActionType.STOP_MARKET,
                       trigger_price=avg * (1 - self.spread), reduce_only=True,
                       client_id="stop")]


class RestingBlowUpStreamer(BaseStreamer):
    """강제청산 시 미체결 주문까지 취소되는지 확인하기 위한 픽스처.

    첫 캔들에 큰 레버리지로 진입하면서, 절대 체결되지 않을 지정가 주문(현재가의 1%)을 같이
    걸어둔다. 파산 후에도 그 주문이 장부에 남아 있으면 flat인 계좌에 유령 포지션이 열린다.
    """

    def __init__(self, symbols: List[str], leverage: float = 50.0, fee_ratio: float = 0.0004):
        super().__init__(symbols, {s: {} for s in symbols})
        self.leverage = leverage
        self.fee_ratio = fee_ratio

    def decide_action(self, symbol, candle: Candle, status: Status):
        if status.position_for(symbol).position != 0:
            return []
        qty = trunc_by_sign(
            status.total_margin() * self.leverage / len(self.symbols) / candle.close, 3)
        if qty == 0:
            return []
        return [Action(symbol, qty),
                Action(symbol, qty, order_type=ActionType.LIMIT,
                       price=candle.close * 0.01, client_id="never")]


class RelativeStrengthRotationStreamer(BaseStreamer):
    """토이 크로스심볼 전략 (검사 전용) — 기준 심볼(``symbols[0]``)의 캔들이 마감할 때만
    판단해서, lookback 구간 수익률이 가장 높은 심볼로 전액 로테이션한다.

    트리거가 아닌 심볼에 대한 Action이 ``status.last_close``로 체결되는 경로 — 즉 "여러
    심볼을 동시에 보고 여러 심볼에 주문을 내는" 요구사항의 핵심 경로 — 를 검사한다.
    """

    def __init__(self, symbols: List[str], lookback: int = 30, fee_ratio: float = 0.0004):
        indicators = {s: {"close_hist": MovingAverage(1, history_size=lookback + 5)}
                     for s in symbols}
        super().__init__(symbols, indicators)
        self.lookback = lookback
        self.fee_ratio = fee_ratio

    def decide_action(self, symbol, candle: Candle, status: Status):
        if symbol != self.symbols[0]:
            return []
        scores: Dict[str, float] = {}
        for s in self.symbols:
            ref = self.indicators[s]["close_hist"].read(-self.lookback)
            latest = status.last_close.get(s)
            if ref is None or latest is None or ref <= 0:
                return []
            scores[s] = latest / ref - 1

        winner = max(scores, key=scores.get)
        actions = []
        equity = status.total_margin()
        for s in self.symbols:
            position = status.position_for(s).position
            price = status.last_close[s]
            target = trunc_by_sign(equity / price, 3) if s == winner else 0.0
            if target != position:
                actions.append(Action(s, target - position))
        return actions


def check_nonflat_start(candles) -> bool:
    """포지션을 들고 시작해도 두 백테스터의 자본 곡선이 일치하는지.

    벡터화 경로는 자본 곡선을 사후에 재구성하는데, 예전에는 첫 거래 이전 구간을 flat이라
    가정해 상수로 채웠다. 참조 경로는 매 이벤트 실시간 계산이라 옳고, 이 곡선에서 파생되는
    max_drawdown/sharpe/balance 컬럼까지 함께 틀어졌다.
    """
    sub = candles[:20_000]
    entry_price = sub[0].close

    candles_by_symbol = {"X": sub}
    ref_report, fast_report = run_pair(
        lambda: KeltnerStreamer(symbols=["X"], window=600), candles_by_symbol,
        make_initial_status=lambda: Status(
            margin=100_000.0,
            positions={"X": PositionState(avg_price=entry_price, position=10.0)}))

    ok = compare_reports("non-flat start", ref_report, fast_report)

    # 첫 거래 이전 구간이 실제로 존재하고 상수가 아닌지 — 아니면 검사가 무의미하다
    first_trade_idx = next((i for i, (ts, _) in enumerate(ref_report.equity_curve)
                            if ref_report.trades and ts >= ref_report.trades[0].timestamp), 0)
    head = [eq for _, eq in ref_report.equity_curve[:first_trade_idx]]
    if len(head) < 2 or len(set(head)) < 2:
        print(f"  [non-flat start] FAIL: 첫 거래 이전 구간이 {len(head)}점/상수 — 경로를 못 밟았다")
        ok = False

    print(f"[non-flat start] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)}, "
          f"첫 거래 이전 {len(head)}개 캔들의 자본이 종가를 따라 변한다")
    return ok


def check_empty_range(candles) -> bool:
    """데이터 범위 밖을 지정해 0개로 잘렸을 때 양쪽이 똑같이 빈 Report를 내는지.

    예전에는 ATRIndicator.precompute_series가 빈 배열에서 IndexError를 냈다.
    """
    far_future = candles[-1].end_time + 10 ** 12
    ok = True
    reports = {}
    candles_by_symbol = {"X": candles}
    for name, vectorized in (("ref", False), ("fast", True)):
        try:
            reports[name] = run_backtest(
                KeltnerStreamer(symbols=["X"], window=600), candles_by_symbol,
                vectorized=vectorized,
                start_time=far_future, end_time=far_future + 1000)
        except Exception as e:
            print(f"  [empty range] FAIL: {name}가 {type(e).__name__}: {e}")
            ok = False
    if ok and not all(not r.trades and not r.equity_curve for r in reports.values()):
        print(f"  [empty range] FAIL: 빈 Report가 아니다 — "
              f"{ {k: (len(r.trades), len(r.equity_curve)) for k, r in reports.items()} }")
        ok = False
    print(f"[empty range] {'OK' if ok else 'MISMATCH'} — 양쪽 모두 빈 Report")
    return ok


def check_forced_liquidation(candles) -> bool:
    """손실측 강제청산이 실제로 발화하고, 두 백테스터가 그 지점까지 정확히 일치하는지 (단일 심볼)."""
    candles_by_symbol = {"X": candles}
    ref_report, fast_report = run_pair(lambda: BlowUpStreamer(["X"]), candles_by_symbol)

    ok = compare_reports("forced liquidation", ref_report, fast_report)

    # 침묵이 곧 성공은 아니다 — 경로를 실제로 밟았는지 단언한다.
    if len(ref_report.trades) < 2:
        print(f"  [forced liquidation] FAIL: 체결이 {len(ref_report.trades)}건뿐 — "
              "강제청산 경로를 밟지 못했다. 픽스처 레버리지를 올려라.")
        return False
    exit_trade = ref_report.trades[1]
    if exit_trade.status.total_margin() > 0:
        print(f"  [forced liquidation] FAIL: 청산 시점 자본이 "
              f"{exit_trade.status.total_margin():.2f} > 0 — 전략 청산이지 강제청산이 아니다.")
        ok = False
    if ref_report.status.total_margin() > 0:
        print(f"  [forced liquidation] FAIL: 파산했어야 하는데 자본이 "
              f"{ref_report.status.total_margin():.2f} 남았다.")
        ok = False
    print(f"[forced liquidation] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)} "
          f"청산 시점 자본={exit_trade.status.total_margin():.2f} "
          f"최종 자본={ref_report.status.total_margin():.2f}")
    return ok


def check_forced_liquidation_multi(candles_a, candles_b) -> bool:
    """증거금 공유 풀에서, 손실측 강제청산이 **두 심볼 모두**를 청산하는지 (멀티심볼)."""
    candles_by_symbol = {"AAA": candles_a, "BBB": candles_b}
    # 실제 시세는 합성 랜덤워크보다 변동성이 낮으므로, 짧은 구간에서도 확실히 터지도록
    # 레버리지를 크게 잡는다 (단일심볼 check_forced_liquidation과 같은 이유).
    ref_report, fast_report = run_pair(
        lambda: BlowUpStreamer(["AAA", "BBB"], leverage=500.0), candles_by_symbol)

    ok = compare_reports("forced liquidation (multi-symbol)", ref_report, fast_report)

    if ref_report.status.total_margin() > 0 or fast_report.status.total_margin() > 0:
        print(f"  [forced liquidation (multi-symbol)] FAIL: 파산했어야 하는데 자본이 남았다 — "
              f"ref={ref_report.status.total_margin():.2f} fast={fast_report.status.total_margin():.2f}")
        ok = False
    for name, st in (("ref", ref_report.status), ("fast", fast_report.status)):
        nonflat = {s: p.position for s, p in st.positions.items() if p.position != 0.0}
        if nonflat:
            print(f"  [forced liquidation (multi-symbol)] FAIL: {name}에 청산되지 않은 포지션 "
                  f"{nonflat}")
            ok = False
    print(f"[forced liquidation (multi-symbol)] {'OK' if ok else 'MISMATCH'} — "
          f"trades={len(ref_report.trades)} 최종 자본={ref_report.status.total_margin():.2f}")
    return ok


def check_multi_symbol_overlap(candles_a, candles_b) -> bool:
    """완전히 겹치는 두 심볼에서 독립된 결정이 서로에게 새지 않고, 두 엔진이 일치하는지."""
    candles_by_symbol = {"AAA": candles_a, "BBB": candles_b}
    ref_report, fast_report = run_pair(
        lambda: DualSymbolMomentum(["AAA", "BBB"]), candles_by_symbol)

    ok = compare_reports("multi-symbol overlap", ref_report, fast_report)
    symbols_traded = {t.symbol for t in ref_report.trades}
    if symbols_traded != {"AAA", "BBB"}:
        print(f"  [multi-symbol overlap] FAIL: 두 심볼 모두 거래됐어야 하는데 {symbols_traded}")
        ok = False
    print(f"[multi-symbol overlap] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)}")
    return ok


def check_staggered_start(candles_a, candles_b) -> bool:
    """상장 시점이 다른(한 심볼이 늦게 시작하는) 두 심볼에서 크로스심볼 전략과 두 엔진이 일치하는지."""
    candles_by_symbol = {"AAA": candles_a, "BBB": candles_b}
    ref_report, fast_report = run_pair(
        lambda: RelativeStrengthRotationStreamer(["AAA", "BBB"]), candles_by_symbol)

    ok = compare_reports("staggered start + rotation", ref_report, fast_report)
    if not any(t.symbol == "BBB" for t in ref_report.trades):
        print("  [staggered start + rotation] FAIL: 늦게 시작한 심볼(BBB)이 한 번도 거래되지 않았다")
        ok = False
    print(f"[staggered start + rotation] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)}")
    return ok


def check_limit_orders(candles) -> bool:
    """지정가 주문: 두 엔진 일치 + 체결이 실제로 "종가가 아닌 가격"에 일어났는지.

    ``LimitLadderStreamer``의 지정가는 종가에서만 유도되므로 체결가가 비트 단위로 같아야 한다.
    체결가가 그 봉의 종가와 다른 거래가 하나도 없으면, 매칭을 타지 않고 시장가로 떨어졌다는
    뜻이므로 실패로 본다 — 침묵은 성공이 아니다.
    """
    by_symbol = {"X": candles}
    close_at = {c.end_time: c.close for c in candles}

    ref_report, fast_report = run_pair(lambda: LimitLadderStreamer(["X"]), by_symbol)

    ok = compare_reports("limit orders", ref_report, fast_report)

    limits = [t for t in ref_report.trades if t.order_type == "LIMIT"]
    if not limits:
        print("  [limit orders] FAIL: 지정가 체결이 하나도 없다 — 경로를 타지 않았다")
        return False
    off_close = [t for t in limits if t.price != close_at.get(t.timestamp)]
    if not off_close:
        print("  [limit orders] FAIL: 모든 지정가 체결이 종가와 같다 — 봉 내 체결이 아니다")
        ok = False
    delayed = [t for t in limits if t.submitted_at is not None and t.submitted_at < t.timestamp]
    if not delayed:
        print("  [limit orders] FAIL: 제출 봉보다 나중에 체결된 주문이 없다")
        ok = False
    if ref_report.status.total_open_orders() != fast_report.status.total_open_orders():
        print(f"  [limit orders] FAIL: 남은 미체결 주문 수가 다르다 "
              f"{ref_report.status.total_open_orders()} != {fast_report.status.total_open_orders()}")
        ok = False
    print(f"[limit orders] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)} "
          f"limit={len(limits)} 종가와 다른 체결={len(off_close)} 지연체결={len(delayed)}")
    return ok


def check_stop_orders(candles) -> bool:
    """조건부 시장가 + reduce_only clamp + 슬리피지.

    ``StopLadderStreamer``는 일부러 포지션의 3배 수량으로 손절을 건다 — clamp가 없으면 반대
    방향 포지션이 열리므로, 체결 수량이 언제나 직전 포지션의 정확한 반대인지 확인한다.
    """
    by_symbol = {"X": candles}
    ok = True
    reports = {}
    for slippage in (0.0, 0.0005):
        ref_report, fast_report = run_pair(lambda: StopLadderStreamer(["X"]), by_symbol,
                                           slippage_ratio=slippage)
        ok &= compare_reports(f"stop orders (slippage={slippage})", ref_report, fast_report)
        reports[slippage] = ref_report

    stops = [t for t in reports[0.0].trades if t.order_type == "STOP_MARKET"]
    if not stops:
        print("  [stop orders] FAIL: 조건부 체결이 하나도 없다 — 경로를 타지 않았다")
        return False
    for t in stops:
        pre = t.status.position_for(t.symbol).position
        if t.quantity != -pre:
            print(f"  [stop orders] FAIL: reduce_only clamp 실패 — 직전 포지션 {pre}, "
                  f"체결 수량 {t.quantity}")
            ok = False
            break

    # 슬리피지는 반드시 불리한 방향으로만 작용해야 한다 (매도 체결가는 내려간다).
    plain = {t.timestamp: t.price for t in stops}
    slipped = [t for t in reports[0.0005].trades if t.order_type == "STOP_MARKET"]
    compared = 0
    for t in slipped:
        base = plain.get(t.timestamp)
        if base is None:
            continue
        compared += 1
        worse = t.price < base if t.quantity < 0 else t.price > base
        if not worse:
            print(f"  [stop orders] FAIL: 슬리피지가 불리한 방향이 아니다 — "
                  f"{base} -> {t.price} (qty={t.quantity})")
            ok = False
            break
    if compared == 0:
        print("  [stop orders] FAIL: 슬리피지 비교 대상이 없다")
        ok = False
    print(f"[stop orders] {'OK' if ok else 'MISMATCH'} — stop 체결={len(stops)} "
          f"슬리피지 대조={compared}")
    return ok


def check_order_expiry(candles) -> bool:
    """``expire_after_candles`` 만료: 절대 체결되지 않는 지정가는 장부에 쌓이지 않아야 한다."""
    class NeverFillStreamer(BaseStreamer):
        def __init__(self, symbols):
            super().__init__(symbols, {s: {} for s in symbols})
            self.fee_ratio = 0.0004

        def decide_action(self, symbol, candle: Candle, status: Status):
            if status.open_orders_for(symbol):
                return []
            # 현재가의 1% — 이 데이터로는 절대 닿지 않는다.
            return [Action(symbol, 1.0, order_type=ActionType.LIMIT,
                           price=candle.close * 0.01, expire_after_candles=5)]

    by_symbol = {"X": candles}
    ref_report, fast_report = run_pair(lambda: NeverFillStreamer(["X"]), by_symbol)

    ok = compare_reports("order expiry", ref_report, fast_report)
    if ref_report.trades or fast_report.trades:
        print(f"  [order expiry] FAIL: 닿을 수 없는 지정가가 체결됐다 "
              f"({len(ref_report.trades)}건)")
        ok = False
    for name, st in (("ref", ref_report.status), ("fast", fast_report.status)):
        # 매 5봉마다 만료되고 다시 걸리므로 장부에는 항상 1건 이하만 남는다.
        if st.total_open_orders() > 1:
            print(f"  [order expiry] FAIL: {name}의 장부에 만료되지 않은 주문이 쌓였다 "
                  f"({st.total_open_orders()}건)")
            ok = False
    print(f"[order expiry] {'OK' if ok else 'MISMATCH'} — 남은 미체결 주문="
          f"{ref_report.status.total_open_orders()}")
    return ok


def check_liquidation_cancels_orders(candles_a, candles_b) -> bool:
    """강제청산이 미체결 주문까지 거두는지 (멀티심볼, 증거금 공유 풀)."""
    by_symbol = {"AAA": candles_a, "BBB": candles_b}
    ref_report, fast_report = run_pair(
        lambda: RestingBlowUpStreamer(["AAA", "BBB"], leverage=500.0), by_symbol)

    ok = compare_reports("liquidation cancels orders", ref_report, fast_report)
    if ref_report.status.total_margin() > 0:
        print("  [liquidation cancels orders] FAIL: 파산했어야 하는데 자본이 남았다")
        ok = False
    for name, st in (("ref", ref_report.status), ("fast", fast_report.status)):
        if st.total_open_orders() != 0:
            print(f"  [liquidation cancels orders] FAIL: {name}의 장부에 미체결 주문이 남았다 "
                  f"({st.total_open_orders()}건)")
            ok = False
        nonflat = {s: p.position for s, p in st.positions.items() if p.position != 0.0}
        if nonflat:
            print(f"  [liquidation cancels orders] FAIL: {name}에 청산되지 않은 포지션 {nonflat}")
            ok = False
    print(f"[liquidation cancels orders] {'OK' if ok else 'MISMATCH'} — "
          f"trades={len(ref_report.trades)} 최종 자본={ref_report.status.total_margin():.2f}")
    return ok


def check_history_bound(candles) -> bool:
    """지표 이력 경계에서 루프 경로와 ArrayIndicator가 같게 동작하는지.

    예전에는 ArrayIndicator가 경계 밖에서도 실제 값을 돌려줘 루프 경로(None/IndexError)와
    갈렸다 — 그 불일치가 닫혔음을 단언한다.
    """
    n = min(len(candles), 20_000)
    sub = candles[:n]
    hist = 128
    ok = True

    loop = ATRIndicator(60, history_size=hist)
    for c in sub:
        loop.update(c)

    seq = ATRIndicator(60, history_size=hist).precompute_series(
        *(np.fromiter((getattr(c, f) for c in sub), np.float64, n)
          for f in ("open", "high", "low", "close", "volume")))
    shim = ArrayIndicator(seq, 60, hist)
    shim.cursor = n

    for idx in (-1, -2, -hist + 1, -hist):
        a, b = loop.read(idx), shim.read(idx)
        if a is None or b is None or not math.isclose(a, b, rel_tol=1e-9):
            print(f"  [history bound] FAIL: read({idx}) 루프={a} 배열={b}")
            ok = False

    for idx in (-hist - 1, -hist - 1000):
        for name, ind in (("loop", loop), ("array", shim)):
            try:
                ind.read(idx)
            except IndexError:
                continue
            print(f"  [history bound] FAIL: {name}.read({idx})가 IndexError를 안 냈다")
            ok = False

    print(f"[history bound] {'OK' if ok else 'MISMATCH'} — history_size={hist}, "
          f"경계 안 일치 / 경계 밖 양쪽 IndexError")
    return ok


def compare_reports(label, ref_report, fast_report) -> bool:
    ok = True
    ref_final = ref_report.status.total_margin()
    fast_final = fast_report.status.total_margin()
    if len(ref_report.trades) != len(fast_report.trades):
        print(f"  [{label}] FAIL: trade count {len(ref_report.trades)} != {len(fast_report.trades)}")
        return False
    for i, (a, b) in enumerate(zip(ref_report.trades, fast_report.trades)):
        # 실현손익은 quantity * (체결가 - 평단)이라 **거의 같은 두 수의 차**다. 체결가가
        # 지표값인 조건부 주문에서는 지표 반올림(1e-14 상대)이 그 뺄셈에서 세 자릿수쯤
        # 증폭되므로, wnl의 허용오차는 wnl 자신이 아니라 **명목가치**에 비례해야 한다.
        # 구조적으로 틀린 손익은 명목가치의 유의미한 비율만큼 어긋나므로 이걸로도 충분히 걸린다.
        notional_tol = max(1e-9, 1e-11 * abs(a.quantity * a.price))
        if not (a.timestamp == b.timestamp and a.symbol == b.symbol and a.quantity == b.quantity
                and a.order_type == b.order_type and a.submitted_at == b.submitted_at
                and math.isclose(a.price, b.price, rel_tol=1e-12, abs_tol=1e-9)
                and math.isclose(a.wnl, b.wnl, rel_tol=1e-12, abs_tol=notional_tol)
                and math.isclose(a.fee, b.fee, rel_tol=1e-12, abs_tol=1e-9)
                and math.isclose(a.leverage, b.leverage, rel_tol=1e-12, abs_tol=1e-9)):
            print(f"  [{label}] FAIL: trade #{i} differs:\n    ref : {a}\n    fast: {b}")
            ok = False
            break
    if not math.isclose(ref_report.max_leverage, fast_report.max_leverage, rel_tol=1e-12, abs_tol=1e-9):
        print(f"  [{label}] FAIL: max_leverage {ref_report.max_leverage} != {fast_report.max_leverage}")
        ok = False
    if not math.isclose(ref_final, fast_final, rel_tol=1e-12, abs_tol=1e-6):
        print(f"  [{label}] FAIL: final margin {ref_final} != {fast_final}")
        ok = False
    if len(ref_report.equity_curve) != len(fast_report.equity_curve):
        print(f"  [{label}] FAIL: equity curve length "
              f"{len(ref_report.equity_curve)} != {len(fast_report.equity_curve)}")
        ok = False
    else:
        # 허용오차는 **상대**가 본질이다. 지표 반올림 차이(위 docstring)가 체결가와 avg_price를
        # 통해 자본에 누적되므로, 오차는 계좌 크기에 비례해서 커진다. 고정 1e-6 절대값만 쓰면
        # 100배로 불어난 계좌에서 순수 반올림이 실패로 잡힌다 — 구조적 어긋남은 자본의 유의미한
        # 비율만큼 벌어지므로 rel_tol 1e-11로도 충분히 걸린다.
        worst = (0.0, 0.0, 0)  # (abs, rel, index)
        for i, ((ts_a, eq_a), (ts_b, eq_b)) in enumerate(
                zip(ref_report.equity_curve, fast_report.equity_curve)):
            if ts_a != ts_b:
                print(f"  [{label}] FAIL: equity curve timestamps diverge at {ts_a} vs {ts_b}")
                ok = False
                break
            diff = abs(eq_a - eq_b)
            if diff > worst[0]:
                worst = (diff, diff / abs(eq_a) if eq_a else 0.0, i)
            if not math.isclose(eq_a, eq_b, rel_tol=1e-11, abs_tol=1e-6):
                print(f"  [{label}] FAIL: equity curve diverges at index {i} ({ts_a}): "
                      f"{eq_a} vs {eq_b}")
                ok = False
                break
        else:
            if worst[0] > 1e-6:
                print(f"  [{label}] note: equity curve max abs diff {worst[0]:.3e} "
                      f"(relative {worst[1]:.3e}) — 지표 반올림 누적")
    # buy & hold 기준선은 종가에서만 유도되므로 사실상 회귀 가드 — 두 경로가 같은 이벤트 구간을
    # 잘라냈는지(reference 는 병합 루프 필터, fast 는 심볼별 searchsorted 슬라이스)까지 확인한다
    if len(ref_report.benchmark_curve) != len(fast_report.benchmark_curve):
        print(f"  [{label}] FAIL: benchmark curve length "
              f"{len(ref_report.benchmark_curve)} != {len(fast_report.benchmark_curve)}")
        ok = False
    else:
        for (ts_a, eq_a), (ts_b, eq_b) in zip(ref_report.benchmark_curve, fast_report.benchmark_curve):
            if ts_a != ts_b or not math.isclose(eq_a, eq_b, rel_tol=1e-12, abs_tol=1e-6):
                print(f"  [{label}] FAIL: benchmark curve differs at {ts_a}: {eq_a} vs {eq_b}")
                ok = False
                break
    return ok


if __name__ == "__main__":
    # 페처/백테스터의 경고(월 청크 데이터 구멍 등)가 형식을 갖춰 나오게 한다. 여기서는
    # 판정 결과를 print로 읽는 게 본론이라, 기본 레벨을 WARNING으로 두고 노이즈를 줄인다.
    setup_logging(default="WARNING")

    symbol = "ETHUSDT"
    symbol2 = "BTCUSDT"
    interval = "1m"
    end_date = datetime(2026, 7, 4, tzinfo=timezone.utc)
    if "--full" in sys.argv:
        start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
    else:
        start_date = datetime(2025, 7, 4, tzinfo=timezone.utc)

    fetcher = BinanceVisionFetcher(compress=False)
    candles = fetcher.get_candles_with_cache(symbol, start_date, end_date, interval)
    print(f"Loaded {len(candles)} candles ({start_date.date()} ~ {end_date.date()})")
    candles2 = fetcher.get_candles_with_cache(symbol2, start_date, end_date, interval)
    print(f"Loaded {len(candles2)} candles for {symbol2}")

    keltner_params = dict(window=20 * 60, m_entry=2.0, m_exit=0.0,
                          max_loss=0.08, fee_ratio=0.0004)

    def make_mixed():
        streamer = KeltnerStreamer(symbols=[symbol], **keltner_params)
        # force the ATR onto the loop path to exercise mixed precomputed/live execution
        streamer.indicators[symbol]["ATR"] = LoopOnlyIndicator(streamer.indicators[symbol]["ATR"])
        return streamer

    cases = {
        "KeltnerStreamer": lambda: KeltnerStreamer(symbols=[symbol], **keltner_params),
        # stateful streamer: carries _timeout_remaining/_stop_price across candles, so it also
        # checks that the fast path doesn't disturb streamer-side state
        "MeanReversionZScoreStreamer": lambda: MeanReversionZScoreStreamer(
            symbol=symbol, window=60, entry_z=2.0, timeout_candles=60,
            max_loss=0.08, fee_ratio=0.0004),
        "KeltnerStreamer (mixed: ATR loop-only)": make_mixed,
        # status를 읽는 지표 — 지표 갱신이 시가평가 전/후 어느 status를 보는지 검사한다. 위
        # 케이스들은 ATR/MA만 써서 status를 아예 읽지 않으므로 이 규약을 못 잡는다.
        "EquityGatedKeltner (status-aware indicator)":
            lambda: EquityGatedKeltner(symbols=[symbol], **keltner_params),
        # 조건부 시장가로 손절을 거는 전략. 트리거가 지표값(ma)이라 체결가가 지표 반올림을
        # 그대로 물고 들어온다 — compare_reports의 price/wnl 허용오차가 존재하는 이유다.
        # 위 keltner_params를 쓰지 않는다: max_loss=0.08은 1분봉에서 레버리지를 6x 캡에
        # 붙여놓고 왕복 수수료로 계좌를 태우는데 (examples/backtest.py의 같은 주석 참고),
        # 자본이 초기값의 1/500,000까지 줄면 상대 허용오차로 재는 패리티 자체가 무의미해진다.
        "KeltnerStopStreamer (real stop orders)":
            lambda: KeltnerStopStreamer(symbols=[symbol], window=72 * 60, m_entry=4.0,
                                        m_exit=3.0, max_loss=0.005, fee_ratio=0.0004),
    }

    all_ok = True
    for label, make_streamer in cases.items():
        candles_by_symbol = {symbol: candles}
        t0 = time.time()
        ref_report = run_backtest(make_streamer(), candles_by_symbol, vectorized=False)
        ref_dt = time.time() - t0

        t0 = time.time()
        fast_report = run_backtest(make_streamer(), candles_by_symbol, vectorized=True)
        fast_dt = time.time() - t0

        ok = compare_reports(label, ref_report, fast_report)
        all_ok &= ok
        print(f"[{label}] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)} "
              f"ref={ref_dt:.1f}s fast={fast_dt:.1f}s ({ref_dt / fast_dt:.1f}x)")

    # 정상 케이스가 밟지 않는 두 경로를 따로 검사한다
    all_ok &= check_forced_liquidation(candles)
    all_ok &= check_history_bound(candles)
    all_ok &= check_nonflat_start(candles)
    all_ok &= check_empty_range(candles)

    # 멀티심볼 전용 경로들 — 병합 타임라인, 상장 시점이 다른 심볼, 크로스심볼 액션, 공유 증거금
    # 강제청산을 검사한다. 두 케이스는 같은 심볼 쌍(ETHUSDT/BTCUSDT)이지만 한쪽은 BBB를
    # 잘라내 상장 시점이 다른 것처럼 만든다.
    n_stagger = min(len(candles2), 20_000)
    stagger_offset = min(500, n_stagger - 1) if n_stagger > 0 else 0
    all_ok &= check_multi_symbol_overlap(candles[:20_000], candles2[:20_000])
    all_ok &= check_staggered_start(candles[:20_000], candles2[stagger_offset:n_stagger])
    all_ok &= check_forced_liquidation_multi(candles[:500], candles2[:500])

    # 지정가/조건부 주문 전용 경로 — 봉 내 체결가, 여러 봉을 사는 미체결 주문, reduce_only
    # clamp, 슬리피지 방향, 만료, 강제청산 시 장부 정리. 위 케이스들은 전부 시장가라 이 중
    # 어느 것도 밟지 않는다. 참조 구현이 느려서 구간을 잘라 쓴다.
    all_ok &= check_limit_orders(candles[:60_000])
    all_ok &= check_stop_orders(candles[:60_000])
    all_ok &= check_order_expiry(candles[:20_000])
    all_ok &= check_liquidation_cancels_orders(candles[:500], candles2[:500])

    print("PARITY:", "ALL OK" if all_ok else "FAILED")
    sys.exit(0 if all_ok else 1)
