"""Parity + benchmark check: FastBacktester vs SingleThreadedBacktester (reference).

Runs both backtesters over the same cached candles and asserts identical trades, final
margin, max leverage and equity curve. Also exercises the mixed path by wrapping one
indicator so it loses its VectorizedIndicator type and must be loop-updated, and a set of
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
"""
import math
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

from core.backtest.FastBacktester import ArrayIndicator, FastBacktester
from core.backtest.SingleThreadedBacktester import SingleThreadedBacktester
from core.backtest.status import PositionState, Status
from core.binance_candle_fetcher.vision_fetcher import BinanceVisionFetcher
from core.streamer.action import Action
from core.streamer.base_streamer import BaseStreamer
from core.streamer.candle import Candle
from core.streamer.indicator.atr import ATRIndicator
from core.streamer.indicator.base_indicator import BaseIndicator
from core.streamer.indicator.moving_average import MovingAverage
from core.streamer.keltner_streamer import KeltnerStreamer
from core.streamer.mean_reversion_zscore import MeanReversionZScoreStreamer
from core.utils import trunc_by_sign


class LoopOnlyIndicator(BaseIndicator):
    """Hides the VectorizedIndicator type of the wrapped indicator, forcing the loop path."""

    def __init__(self, inner: BaseIndicator):
        super().__init__(inner.window)
        self._inner = inner

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        self._inner.update(candle, status)

    def get_index(self, idx: int) -> Optional[float]:
        return self._inner.get_index(idx)

    def get_latest(self) -> Optional[float]:
        return self._inner.get_latest()


class EquityIndicator(BaseIndicator):
    """매 캔들의 거래 전 시가평가 자본을 기록하는 status 의존 지표 (검사용).

    ``status``를 읽으므로 벡터화가 불가능하고 항상 루프 경로에 남는다. ``updates_before_decide``
    를 켜면 "before 그룹은 시가평가 **후**의 status를 봐야 한다"는 규약을 직접 찌른다.
    """

    scale_group = "balance"

    def __init__(self):
        super().__init__(1)
        self.values = self._new_history()

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        # 라이브 프리피드는 status=None으로 부른다 — 워밍업으로 취급한다.
        if status is None:
            return
        self.values.append(status.total_margin())

    def get_index(self, idx: int) -> Optional[float]:
        return self._read(self.values, idx)

    def get_latest(self) -> Optional[float]:
        return self._read(self.values, -1)


class EquityGatedKeltner(KeltnerStreamer):
    """Keltner에 "자본이 직전 대비 줄면 즉시 청산" 규칙을 얹은 픽스처.

    청산 판단이 EquityIndicator의 값에 직접 걸리므로, before 그룹이 보는 status가 한 캔들
    어긋나면 청산 시점이 달라지고 체결 목록이 갈린다.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        equity = EquityIndicator()
        equity.updates_before_decide = True
        self.indicators[self.symbols[0]]["EQ"] = equity

    def decide_action(self, symbol, candle: Candle, status: Status):
        position = status.position_for(symbol).position
        if position != 0.0:
            now = self.indicators[symbol]["EQ"].get_index(-1)
            prev = self.indicators[symbol]["EQ"].get_index(-2)
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
            ref = self.indicators[s]["close_hist"].get_index(-self.lookback)
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

    FastBacktester는 자본 곡선을 사후에 재구성하는데, 예전에는 첫 거래 이전 구간을 flat이라
    가정해 상수로 채웠다. reference는 매 캔들 실시간 계산이라 옳고, 이 곡선에서 파생되는
    max_drawdown/sharpe/balance 컬럼까지 함께 틀어졌다.
    """
    sub = candles[:20_000]
    entry_price = sub[0].close

    def seed(bt):
        bt.status = Status(margin=100_000.0,
                           positions={"X": PositionState(avg_price=entry_price, position=10.0)})
        return bt

    candles_by_symbol = {"X": sub}
    ref_bt = seed(SingleThreadedBacktester(KeltnerStreamer(symbols=["X"], window=600), candles_by_symbol))
    ref_report = ref_bt.run()
    fast_bt = seed(FastBacktester(KeltnerStreamer(symbols=["X"], window=600), candles_by_symbol))
    fast_report = fast_bt.run()

    ok = compare_reports("non-flat start", ref_report, ref_bt.status.total_margin(),
                         fast_report, fast_bt.status.total_margin())

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
    for name, cls in (("ref", SingleThreadedBacktester), ("fast", FastBacktester)):
        bt = cls(KeltnerStreamer(symbols=["X"], window=600), candles_by_symbol)
        try:
            reports[name] = bt.run(start_time=far_future, end_time=far_future + 1000)
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
    ref_bt = SingleThreadedBacktester(BlowUpStreamer(["X"]), candles_by_symbol)
    ref_report = ref_bt.run()
    fast_bt = FastBacktester(BlowUpStreamer(["X"]), candles_by_symbol)
    fast_report = fast_bt.run()

    ok = compare_reports("forced liquidation", ref_report, ref_bt.status.total_margin(),
                         fast_report, fast_bt.status.total_margin())

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
    if ref_bt.status.total_margin() > 0:
        print(f"  [forced liquidation] FAIL: 파산했어야 하는데 자본이 "
              f"{ref_bt.status.total_margin():.2f} 남았다.")
        ok = False
    print(f"[forced liquidation] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)} "
          f"청산 시점 자본={exit_trade.status.total_margin():.2f} "
          f"최종 자본={ref_bt.status.total_margin():.2f}")
    return ok


def check_forced_liquidation_multi(candles_a, candles_b) -> bool:
    """증거금 공유 풀에서, 손실측 강제청산이 **두 심볼 모두**를 청산하는지 (멀티심볼)."""
    candles_by_symbol = {"AAA": candles_a, "BBB": candles_b}
    # 실제 시세는 합성 랜덤워크보다 변동성이 낮으므로, 짧은 구간에서도 확실히 터지도록
    # 레버리지를 크게 잡는다 (단일심볼 check_forced_liquidation과 같은 이유).
    ref_bt = SingleThreadedBacktester(BlowUpStreamer(["AAA", "BBB"], leverage=500.0), candles_by_symbol)
    ref_report = ref_bt.run()
    fast_bt = FastBacktester(BlowUpStreamer(["AAA", "BBB"], leverage=500.0), candles_by_symbol)
    fast_report = fast_bt.run()

    ok = compare_reports("forced liquidation (multi-symbol)", ref_report, ref_bt.status.total_margin(),
                         fast_report, fast_bt.status.total_margin())

    if ref_bt.status.total_margin() > 0 or fast_bt.status.total_margin() > 0:
        print(f"  [forced liquidation (multi-symbol)] FAIL: 파산했어야 하는데 자본이 남았다 — "
              f"ref={ref_bt.status.total_margin():.2f} fast={fast_bt.status.total_margin():.2f}")
        ok = False
    for name, bt in (("ref", ref_bt), ("fast", fast_bt)):
        nonflat = {s: p.position for s, p in bt.status.positions.items() if p.position != 0.0}
        if nonflat:
            print(f"  [forced liquidation (multi-symbol)] FAIL: {name}에 청산되지 않은 포지션 "
                  f"{nonflat}")
            ok = False
    print(f"[forced liquidation (multi-symbol)] {'OK' if ok else 'MISMATCH'} — "
          f"trades={len(ref_report.trades)} 최종 자본={ref_bt.status.total_margin():.2f}")
    return ok


def check_multi_symbol_overlap(candles_a, candles_b) -> bool:
    """완전히 겹치는 두 심볼에서 독립된 결정이 서로에게 새지 않고, 두 엔진이 일치하는지."""
    candles_by_symbol = {"AAA": candles_a, "BBB": candles_b}
    ref_bt = SingleThreadedBacktester(DualSymbolMomentum(["AAA", "BBB"]), candles_by_symbol)
    ref_report = ref_bt.run()
    fast_bt = FastBacktester(DualSymbolMomentum(["AAA", "BBB"]), candles_by_symbol)
    fast_report = fast_bt.run()

    ok = compare_reports("multi-symbol overlap", ref_report, ref_bt.status.total_margin(),
                         fast_report, fast_bt.status.total_margin())
    symbols_traded = {t.symbol for t in ref_report.trades}
    if symbols_traded != {"AAA", "BBB"}:
        print(f"  [multi-symbol overlap] FAIL: 두 심볼 모두 거래됐어야 하는데 {symbols_traded}")
        ok = False
    print(f"[multi-symbol overlap] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)}")
    return ok


def check_staggered_start(candles_a, candles_b) -> bool:
    """상장 시점이 다른(한 심볼이 늦게 시작하는) 두 심볼에서 크로스심볼 전략과 두 엔진이 일치하는지."""
    candles_by_symbol = {"AAA": candles_a, "BBB": candles_b}
    ref_bt = SingleThreadedBacktester(RelativeStrengthRotationStreamer(["AAA", "BBB"]), candles_by_symbol)
    ref_report = ref_bt.run()
    fast_bt = FastBacktester(RelativeStrengthRotationStreamer(["AAA", "BBB"]), candles_by_symbol)
    fast_report = fast_bt.run()

    ok = compare_reports("staggered start + rotation", ref_report, ref_bt.status.total_margin(),
                         fast_report, fast_bt.status.total_margin())
    if not any(t.symbol == "BBB" for t in ref_report.trades):
        print("  [staggered start + rotation] FAIL: 늦게 시작한 심볼(BBB)이 한 번도 거래되지 않았다")
        ok = False
    print(f"[staggered start + rotation] {'OK' if ok else 'MISMATCH'} — trades={len(ref_report.trades)}")
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
        a, b = loop.get_index(idx), shim.get_index(idx)
        if a is None or b is None or not math.isclose(a, b, rel_tol=1e-9):
            print(f"  [history bound] FAIL: get_index({idx}) 루프={a} 배열={b}")
            ok = False

    for idx in (-hist - 1, -hist - 1000):
        for name, ind in (("loop", loop), ("array", shim)):
            try:
                ind.get_index(idx)
            except IndexError:
                continue
            print(f"  [history bound] FAIL: {name}.get_index({idx})가 IndexError를 안 냈다")
            ok = False

    print(f"[history bound] {'OK' if ok else 'MISMATCH'} — history_size={hist}, "
          f"경계 안 일치 / 경계 밖 양쪽 IndexError")
    return ok


def compare_reports(label, ref_report, ref_final, fast_report, fast_final) -> bool:
    ok = True
    if len(ref_report.trades) != len(fast_report.trades):
        print(f"  [{label}] FAIL: trade count {len(ref_report.trades)} != {len(fast_report.trades)}")
        return False
    for i, (a, b) in enumerate(zip(ref_report.trades, fast_report.trades)):
        if not (a.timestamp == b.timestamp and a.symbol == b.symbol and a.quantity == b.quantity
                and a.price == b.price
                and math.isclose(a.wnl, b.wnl, rel_tol=1e-12, abs_tol=1e-9)
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
        max_diff = 0.0
        for (ts_a, eq_a), (ts_b, eq_b) in zip(ref_report.equity_curve, fast_report.equity_curve):
            if ts_a != ts_b:
                print(f"  [{label}] FAIL: equity curve timestamps diverge at {ts_a} vs {ts_b}")
                ok = False
                break
            max_diff = max(max_diff, abs(eq_a - eq_b))
        else:
            if max_diff > 1e-6:
                print(f"  [{label}] FAIL: equity curve max diff {max_diff}")
                ok = False
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

    def make_before():
        # every indicator ingests the current candle before decide_action (per-instance override)
        streamer = KeltnerStreamer(symbols=[symbol], **keltner_params)
        for indicator in streamer.indicators[symbol].values():
            indicator.updates_before_decide = True
        return streamer

    def make_split():
        # MA stays vectorized+after (shims_after), ATR becomes loop-only+before (live_before).
        # Together with the two cases above this covers all four partition branches:
        # shims_before/shims_after/live_before/live_after.
        streamer = KeltnerStreamer(symbols=[symbol], **keltner_params)
        streamer.indicators[symbol]["ATR"] = LoopOnlyIndicator(streamer.indicators[symbol]["ATR"])
        streamer.indicators[symbol]["ATR"].updates_before_decide = True
        return streamer

    cases = {
        "KeltnerStreamer": lambda: KeltnerStreamer(symbols=[symbol], **keltner_params),
        # stateful streamer: carries _timeout_remaining/_stop_price across candles, so it also
        # checks that the fast path doesn't disturb streamer-side state
        "MeanReversionZScoreStreamer": lambda: MeanReversionZScoreStreamer(
            symbol=symbol, window=60, entry_z=2.0, timeout_candles=60,
            max_loss=0.08, fee_ratio=0.0004),
        "KeltnerStreamer (mixed: ATR loop-only)": make_mixed,
        "KeltnerStreamer (all updates_before_decide)": make_before,
        "KeltnerStreamer (split: MA after / ATR loop-only before)": make_split,
        # status를 읽는 before 지표 — before 그룹이 시가평가 전/후 어느 status를 보는지 검사한다.
        # 위 케이스들은 ATR/MA만 써서 status를 아예 읽지 않으므로 이 규약을 못 잡는다.
        "EquityGatedKeltner (status-aware before indicator)":
            lambda: EquityGatedKeltner(symbols=[symbol], **keltner_params),
    }

    all_ok = True
    for label, make_streamer in cases.items():
        candles_by_symbol = {symbol: candles}
        ref_bt = SingleThreadedBacktester(make_streamer(), candles_by_symbol)
        t0 = time.time()
        ref_report = ref_bt.run()
        ref_dt = time.time() - t0

        fast_bt = FastBacktester(make_streamer(), candles_by_symbol)
        t0 = time.time()
        fast_report = fast_bt.run()
        fast_dt = time.time() - t0

        ok = compare_reports(label, ref_report, ref_bt.status.total_margin(),
                             fast_report, fast_bt.status.total_margin())
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

    print("PARITY:", "ALL OK" if all_ok else "FAILED")
    sys.exit(0 if all_ok else 1)
