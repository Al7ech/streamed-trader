"""Backtest engine check: 벡터화 == 루프.

:class:`~core.engine.backtest.BacktestEngine`이 지표를 미리 계산해 두고 도는 것이 캔들마다
``update()``를 부르는 것과 **완전히 같은 결과**를 낸다는 것을 확인한다. 같아야 하는 이유는
성능이 아니라 신뢰다: 백테스트가 라이브와 같은 값을 본다는 보장이 여기서 나오고, 그 보장이
있어야 ``core/checks/live_check.py`` 3절(드라이런 == 백테스트)이 허용오차 없이 성립한다.

1. **지표 단위 정확성** — ``VectorizableNumericIndicator``를 상속한 모든 지표에 대해, 미리
   계산한 수열이 ``update()`` 루프의 ``get_latest()`` 수열과 **비트 단위로** 같은지. 이어서
   그 값을 ``sink()``로 재생했을 때 ``read(idx)``가 전 인덱스에서 루프와 같은지.
   가장 싸고 가장 정확히 원인을 짚는 검사라 맨 앞에 둔다 — 여기가 깨지면 아래는 볼 필요가 없다.
2. **전략 단위 대조** — 실제 캔들 위에서 ``vectorize=True``와 ``False``의 ``Report``가 같은지.
   1절이 지표 하나를 보는 반면 여기는 지표가 전략·실행기·레코더를 거쳐 나온 결과를 본다.
3. **멀티심볼 ragged** — 상장일이 다르고 구멍이 있는 두 심볼. 심볼별 재생 커서가 병합
   타임라인과 어긋나지 않는지 보는 유일한 검사다.
4. **퇴화 입력** — 빈 캔들, 캔들 1개, 지표 없는 심볼 등에서 죽지 않는지.
5. **심볼 규칙과 체결 회계** — 수량/가격 양자화 도우미, 규칙 위반 주문 거부, 체결 후 포지션
   정규화(부동소수점 잔여가 남지 않는지).
6. **열 캔들, 선계산 캐시, 스윕** — ``ColumnarCandles``가 원본 캔들을 필드·타입까지 돌려주는지,
   ``cache_key``가 같으면 계산이 같은지, 리스트 입력 == 열 입력 == 캐시 주입 Report인지,
   ``ParameterSweep``(순차/fork) 요약이 엔진을 직접 돌린 요약과 같은지. 합성 캔들 부분은
   오프라인, 실제 캔들 부분은 온라인에서 돈다.

    uv run python core/checks/backtest_check.py            # 전부
    uv run python core/checks/backtest_check.py --offline  # 캔들 캐시가 필요 없는 1, 4, 5, 6(합성) 만
"""
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

from core.candle.candle import Candle
from core.candle.columnar import ColumnarCandles
from core.checks.compare import compare_reports
from core.engine.backtest import BacktestEngine
from core.executor.simulated import SimulatedExecutor
from core.order.action import Action, ActionType
from core.order.symbol_rules import DEFAULT_RULES, SymbolRules
from core.fetcher.binance.vision_fetcher import BinanceVisionFetcher
from core.logging_config import setup_logging
from core.recorder.full import FullRecorder
from core.recorder.simple import SimpleRecorder
from core.result.metrics import summarise_report
from core.streamer.indicator.atr import ATRIndicator
from core.streamer.indicator.donchian_channel import MaxDonchianIndicator, MinDonchianIndicator
from core.streamer.indicator.moving_average import MovingAverage
from core.streamer.indicator.rolling_std import RollingStd
from core.streamer.indicator.volume_stats import VolumeMovingAverage, VolumeRollingStd
from core.streamer.strategies.keltner_stop_streamer import KeltnerStopStreamer
from core.streamer.strategies.keltner_streamer import KeltnerStreamer
from core.streamer.strategies.mean_reversion_zscore import MeanReversionZScoreStreamer
from core.streamer.strategies.momentum_time_exit import MomentumTimeExitStreamer
from core.streamer.strategies.supertrend_streamer import SupertrendStreamer
from core.sweep import ParameterSweep, SweepJob

MIN = 60_000
SYM, SYM2 = "ETHUSDT", "BTCUSDT"
INIT_MARGIN = 100_000.0

_failures = []


def check(label, cond, detail=""):
    if not cond:
        _failures.append(label)
    print(f"[{label}] {'OK' if cond else 'FAIL'}{(' — ' + detail) if detail else ''}")


def synthetic_candles(n: int, seed: int = 0) -> List[Candle]:
    """난수 워크 캔들. 실제 시세처럼 종가가 움직이고 고가/저가가 그것을 감싼다."""
    if n == 0:
        return []
    rng = np.random.default_rng(seed)
    close = 2000.0 * np.exp(np.cumsum(rng.normal(0, 3e-4, n)))
    high = close * (1 + np.abs(rng.normal(0, 5e-4, n)))
    low = close * (1 - np.abs(rng.normal(0, 5e-4, n)))
    open_ = np.concatenate(([close[0]], close[:-1]))
    volume = np.abs(rng.normal(1000, 300, n)) + 1.0
    return [Candle(open_[i], high[i], low[i], close[i], volume[i], i * MIN, (i + 1) * MIN)
            for i in range(n)]


# ==================================================== 1. 지표 단위 정확성


#: (이름, 생성자, window) — ``VectorizableNumericIndicator``를 상속한 지표 전부.
#: 새 지표를 그렇게 만들면 여기 한 줄을 더해야 한다.
def _indicator_cases(window: int):
    cases = [
        ("MovingAverage", lambda: MovingAverage(window)),
        ("ATRIndicator", lambda: ATRIndicator(window)),
        ("MinDonchianIndicator", lambda: MinDonchianIndicator(window)),
        ("MaxDonchianIndicator", lambda: MaxDonchianIndicator(window)),
        ("VolumeMovingAverage", lambda: VolumeMovingAverage(window)),
    ]
    if window >= 2:  # 표본표준편차는 window >= 2 가 필요하다
        cases += [
            ("RollingStd", lambda: RollingStd(window)),
            ("VolumeRollingStd", lambda: VolumeRollingStd(window)),
        ]
    return cases


def _ohlcv(candles: List[Candle]):
    n = len(candles)
    return tuple(np.fromiter((getattr(c, k) for c in candles), np.float64, n)
                 for k in ("open", "high", "low", "close", "volume"))


def _loop_series(make, candles) -> List[Optional[float]]:
    ind = make()
    out = []
    for candle in candles:
        ind.update(candle)
        out.append(ind.get_latest())
    return out


def _as_array(values: List[Optional[float]]) -> np.ndarray:
    return np.array([np.nan if v is None else v for v in values], dtype=np.float64)


def check_indicator_exactness():
    """미리 계산한 수열이 루프의 ``get_latest()`` 수열과 비트 단위로 같은가."""
    # (캔들 수, window): 워밍업 전/딱/후, 창보다 짧은 구간, window=1, 보관 이력보다 큰 창.
    shapes = [(0, 10), (1, 10), (5, 10), (10, 10), (11, 10), (3000, 1), (3000, 50),
              (3000, 720), (9000, 8200)]
    for n, window in shapes:
        candles = synthetic_candles(n, seed=n + window)
        arrays = _ohlcv(candles)
        for name, make in _indicator_cases(window):
            ref = _as_array(_loop_series(make, candles))
            got = make().compute(*arrays)
            label = f"지표 정확성: {name} (n={n}, window={window})"
            if len(got) != n or got.dtype != np.float64:
                check(label, False, f"길이/dtype: {len(got)} vs {n}, {got.dtype}")
                continue
            if n == 0:
                check(label, True)
                continue
            same_nan = np.array_equal(np.isnan(ref), np.isnan(got))
            m = ~np.isnan(ref)
            same_val = np.array_equal(ref[m], got[m])
            detail = ""
            if not same_val:
                diff = np.abs(ref[m] - got[m])
                worst = int(np.argmax(diff))
                detail = (f"최대 절대차 {diff[worst]:.3e} "
                          f"(상대 {diff[worst] / abs(ref[m][worst]):.3e}), "
                          f"불일치 {int(np.count_nonzero(diff))}/{len(diff)}")
            elif not same_nan:
                detail = "NaN 위치가 다르다 (워밍업 길이 불일치)"
            check(label, same_nan and same_val, detail)


def check_sink_playback():
    """``sink()``로 재생한 지표의 ``read(idx)``가 루프와 전 인덱스에서 같은가.

    ``get_latest()``만 같아서는 부족하다 — 대부분의 전략이 ``read(-2)``로 직전 값을 읽고,
    루프와 재생은 워밍업 구간에서 deque 길이가 다를 수 있기 때문이다 (루프는 아무것도 얹지
    않거나 None을 얹고, 재생은 NaN을 얹는다). 보관 이력 경계의 ``IndexError``와 양수 인덱스
    거부까지 같은지도 여기서 본다.
    """
    for n, window in [(200, 1), (200, 50), (3000, 720)]:
        candles = synthetic_candles(n, seed=n * 7 + window)
        arrays = _ohlcv(candles)
        for name, make in _indicator_cases(window):
            loop_ind, play_ind = make(), make()
            sink = play_ind.sink()
            values = make().compute(*arrays).tolist()
            probes = [-1, -2, -3, -window, -window - 1, -window - 2,
                      -play_ind.history_size, -play_ind.history_size - 1, 0]
            bad = None
            for i, candle in enumerate(candles):
                loop_ind.update(candle)
                sink(values[i])
                for idx in probes:
                    a, b = _read_or_error(loop_ind, idx), _read_or_error(play_ind, idx)
                    if a != b:
                        bad = f"캔들 {i}, read({idx}): 루프 {a!r} vs 재생 {b!r}"
                        break
                if bad:
                    break
            check(f"sink 재생: {name} (n={n}, window={window})", bad is None, bad or "")


def _read_or_error(indicator, idx):
    try:
        return indicator.read(idx)
    except IndexError:
        return "IndexError"


# ==================================================== 2~3. 전략 단위 대조


def _run(streamer, candles_by_symbol, vectorize, slippage_ratio=None):
    executor = SimulatedExecutor(INIT_MARGIN, slippage_ratio=(slippage_ratio or 0.0))
    recorder = FullRecorder(streamer, executor.status, interval_ms=MIN)
    return BacktestEngine(streamer, candles_by_symbol, executor, recorder,
                          vectorize=vectorize, progress=False).run()


def _load(symbol, start, end):
    return BinanceVisionFetcher(compress=False).get_candles_with_cache(
        symbol, start, end, "1m")


def check_strategy_parity():
    """실제 캔들 위에서 vectorize=True와 False의 Report가 같은가."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 2, 1, tzinfo=timezone.utc)
    candles = _load(SYM, start, end)
    print(f"Loaded {len(candles)} candles for strategy parity")
    by_symbol = {SYM: candles}
    kp = dict(window=20 * 60, m_entry=2.0, m_exit=0.0, max_loss=0.02)

    cases = [
        # 시장가 전용. MA + ATR 둘 다 미리 계산된다.
        ("Keltner (시장가)", lambda: KeltnerStreamer(symbols=[SYM], **kp), None),
        # 상태를 캔들 사이에 들고 가는 전략 + RollingStd.
        ("MeanReversionZScore (상태 있는 전략)",
         lambda: MeanReversionZScoreStreamer(symbol=SYM, window=60, entry_z=2.0,
                                             timeout_candles=60, max_loss=0.02), None),
        # 조건부 주문 — 트리거 가격이 **지표값**이라 지표 반올림이 체결가에 직접 들어간다.
        ("KeltnerStop (조건부 주문)",
         lambda: KeltnerStopStreamer(symbols=[SYM], window=72 * 60, m_entry=4.0,
                                     m_exit=3.0, max_loss=0.005), None),
        # 벡터화 불가 지표만 쓰는 전략 — 선계산이 하나도 없는 경로.
        ("Supertrend (벡터화 불가 지표)",
         lambda: SupertrendStreamer(symbol=SYM, atr_window=6 * 60, multiplier=3.0,
                                    max_loss=0.02), None),
        # MovingAverage(1) + 파라미터로 정해지는 history_size = 깊은 조회 경로.
        ("MomentumTimeExit (깊은 read)",
         lambda: MomentumTimeExitStreamer(symbol=SYM, mom_lookback=90,
                                          entry_threshold_pct=0.5, hold_candles=120,
                                          max_loss=0.02), None),
    ]
    for label, make_streamer, slippage in cases:
        vec = _run(make_streamer(), by_symbol, True, slippage)
        ref = _run(make_streamer(), by_symbol, False, slippage)
        check(f"vectorize == loop: {label}", compare_reports(label, ref, vec),
              f"trades={len(ref.trades)} final={ref.status.total_margin():.2f}")


def check_multi_symbol_ragged():
    """상장일이 다르고 구멍이 있는 두 심볼 — 심볼별 재생 커서가 병합과 어긋나지 않는가."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 2, 1, tzinfo=timezone.utc)
    eth = _load(SYM, start, end)
    btc = _load(SYM2, start, end)
    # BTC는 늦게 시작하고 중간에 구멍이 있다. 두 심볼의 캔들 수가 서로 다르고, 이벤트마다
    # 등장하는 심볼 집합이 바뀐다 — 커서를 전역 이벤트 인덱스로 잡았다면 여기서 어긋난다.
    btc = btc[500:]
    btc = btc[:5000] + btc[5800:]
    by_symbol = {SYM: eth, SYM2: btc}
    print(f"Loaded {len(eth)} / {len(btc)} candles for ragged multi-symbol parity")

    kp = dict(window=6 * 60, m_entry=2.0, m_exit=0.0, max_loss=0.02)
    vec = _run(KeltnerStreamer(symbols=[SYM, SYM2], **kp), by_symbol, True)
    ref = _run(KeltnerStreamer(symbols=[SYM, SYM2], **kp), by_symbol, False)
    check("vectorize == loop: 멀티심볼 ragged", compare_reports("ragged", ref, vec),
          f"trades={len(ref.trades)} final={ref.status.total_margin():.2f}")


# ==================================================== 4. 퇴화 입력


def check_degenerate_inputs():
    """빈/짧은 입력에서 죽지 않고 Report를 돌려주는가."""
    kp = dict(window=20, m_entry=2.0, m_exit=0.0, max_loss=0.02)
    cases = [
        ("심볼 자체가 없다", {}),
        ("캔들 0개", {SYM: []}),
        ("캔들 1개", {SYM: synthetic_candles(1)}),
        ("창보다 짧다", {SYM: synthetic_candles(5)}),
    ]
    for label, by_symbol in cases:
        ok, detail = True, ""
        for vectorize in (True, False):
            try:
                report = _run(KeltnerStreamer(symbols=[SYM], **kp), by_symbol, vectorize)
                if report is None or report.trades:
                    ok, detail = False, f"vectorize={vectorize}: 예상 밖 결과 {report}"
            except Exception as e:  # noqa: BLE001 - 검사라 무엇이 터졌든 보고한다
                ok, detail = False, f"vectorize={vectorize}: {type(e).__name__}: {e}"
        check(f"퇴화 입력: {label}", ok, detail)

    # 스트리머가 다루지 않는 심볼이 섞여 있어도 통과해야 한다 (지표가 없을 뿐이다).
    by_symbol = {SYM: synthetic_candles(300), SYM2: synthetic_candles(300, seed=9)}
    try:
        report = _run(KeltnerStreamer(symbols=[SYM], **kp), by_symbol, True)
        check("퇴화 입력: 전략이 모르는 심볼이 섞여 있다", report is not None,
              f"trades={len(report.trades)}")
    except Exception as e:  # noqa: BLE001
        check("퇴화 입력: 전략이 모르는 심볼이 섞여 있다", False, f"{type(e).__name__}: {e}")

    # compute()가 캔들 수와 다른 길이를 주면 조용히 밀리지 않고 죽어야 한다.
    class _BadLength(MovingAverage):
        def compute(self, open, high, low, close, volume):
            return super().compute(open, high, low, close, volume)[:-1]

    streamer = KeltnerStreamer(symbols=[SYM], **kp)
    streamer.indicators[SYM]["MA"] = _BadLength(20)
    try:
        _run(streamer, {SYM: synthetic_candles(300)}, True)
        check("compute() 길이 불일치는 치명적", False, "예외가 나지 않았다")
    except ValueError:
        check("compute() 길이 불일치는 치명적", True)


# ==================================================== 5. 심볼 규칙과 체결 회계


def check_symbol_rules():
    eth = DEFAULT_RULES[SYM]  # step 0.001, 최소 명목가치 20, tick 0.01

    # 도우미: 곱셈 오차가 한 단위를 깎지 않는다 (4.35 * 100 == 434.99999999999994).
    cent = SymbolRules("0.01", "0.01", "0.01", "0.01", "5", "0.01")
    ten = SymbolRules("10", "10", "10", "10", "5", "10")
    odd = SymbolRules("0.0005", "0.0005", "0.0005", "0.0005", "5", "0.0005")
    cases = [
        ("floor_qty 곱셈 오차", cent.floor_qty(4.35), 4.35),
        ("floor_qty 음수는 0 방향", eth.floor_qty(-1.23456), -1.234),
        ("floor_qty 단위 미만은 0", eth.floor_qty(0.0009), 0.0),
        ("floor_qty 10 단위", ten.floor_qty(129.9), 120.0),
        ("floor_qty 0.0005 단위", odd.floor_qty(0.0019), 0.0015),
        ("floor_qty 증감분 뺄셈 잔여", eth.floor_qty(0.3 - 0.1), 0.2),
        ("round_price", eth.round_price(3123.456789), 3123.46),
        ("round_price 0.0005 단위", odd.round_price(1.00026), 1.0005),
    ]
    for label, got, want in cases:
        check(f"심볼 규칙: {label}", got == want, f"got {got!r}, want {want!r}")

    # 체결 후 포지션 정규화 — 전부 단위 배수인 수량이어도 float 덧셈은 잔여를 남긴다.
    def fills(*quantities):
        ex = SimulatedExecutor(100_000.0)
        # 가격 1000: 0.1 단위 주문도 최소 명목가치(20)를 넘는다.
        ex.begin_event(MIN, {SYM: Candle(1000, 1000, 1000, 1000, 1, 0, MIN)})
        for q in quantities:
            ex.submit(Action(SYM, q), MIN)
        return ex.status.position_for(SYM)

    for qs, want in [((0.1, 0.2, -0.3), 0.0), ((0.1, 0.1, -0.3), -0.1),
                     ((0.7, -0.1, -0.1, -0.1, -0.1, -0.1, -0.1, -0.1), 0.0)]:
        p = fills(*qs)
        check(f"정규화: {' + '.join(map(repr, qs))} == {want!r}", p.position == want
              and (p.avg_price == 0.0) == (want == 0.0), repr(p))

    # 규칙 위반 주문은 체결도 등록도 되지 않는다. 순수 청산과 reduce_only는 명목가치 면제.
    ex = SimulatedExecutor(100_000.0)
    ex.begin_event(MIN, {SYM: Candle(3000, 3000, 3000, 3000, 1, 0, MIN)})
    rejected = [
        Action(SYM, 0.0015),                                              # step
        Action(SYM, 0.005),                                               # 명목가치 15 < 20
        Action(SYM, 1.0, order_type=ActionType.LIMIT, price=2990.005),    # tick
        Action(SYM, -1.0, order_type=ActionType.STOP_MARKET, trigger_price=2990.001,
               trigger_above=False),                                      # tick
    ]
    for a in rejected:
        ex.submit(a, MIN)
    check("심볼 규칙: 위반 주문은 체결·등록되지 않는다",
          ex.status.position_for(SYM).position == 0.0 and not ex.status.total_open_orders())
    for q in (0.01, -0.006):          # 진입(명목 30) 후 명목 18짜리 부분 청산
        ex.submit(Action(SYM, q), MIN)
    ex.submit(Action(SYM, -0.004, order_type=ActionType.STOP_MARKET, trigger_price=2900.0,
                     trigger_above=False, reduce_only=True), MIN)
    check("심볼 규칙: 순수 청산·reduce_only는 명목가치 면제",
          ex.status.position_for(SYM).position == 0.004 and ex.status.total_open_orders() == 1,
          repr(ex.status))

    # 모르는 심볼은 조용히 넘어가지 않는다.
    try:
        SimulatedExecutor(1.0).status.rules_for("NOSUCHUSDT")
        check("심볼 규칙: 모르는 심볼은 KeyError", False)
    except KeyError:
        check("심볼 규칙: 모르는 심볼은 KeyError", True)


# ==================================================== 6. 열 캔들, 선계산 캐시, 스윕


_CANDLE_FIELDS = ("open", "high", "low", "close", "volume", "start_time", "end_time",
                  "taker_buy_volume", "trade_count")


def _fields(c: Candle):
    return tuple(getattr(c, k) for k in _CANDLE_FIELDS)


def _pure_python(c: Candle) -> bool:
    """열에서 나온 캔들의 필드는 numpy 스칼라가 아니라 파이썬 float/int/None이어야 한다 —
    ``np.float64``가 새면 ``Trade.price``와 샤드 JSON까지 흘러간다 (``_iter_floats`` 참고)."""
    return all(type(getattr(c, k)) in (float, int, type(None)) for k in _CANDLE_FIELDS)


def check_columnar_roundtrip():
    """``ColumnarCandles``가 원본 캔들을 값 그대로, 파이썬 스칼라로 돌려주는가."""
    base = synthetic_candles(3 * (1 << 16) + 17, seed=3)  # 블록 경계를 넘는 길이
    mixed = synthetic_candles(1000, seed=4)
    for i, c in enumerate(mixed):  # 일부만 있는 선택 필드 — None이 그 자리에 돌아와야 한다
        c.taker_buy_volume = None if i % 3 == 0 else c.volume * 0.4
        c.trade_count = None if i % 5 == 0 else i * 7
    for label, candles in [("합성 (선택 필드 없음)", base), ("선택 필드 일부 None", mixed),
                           ("빈 시퀀스", [])]:
        col = ColumnarCandles.from_candles(candles)
        same = (len(col) == len(candles)
                and all(_fields(a) == _fields(b) and _pure_python(b)
                        for a, b in zip(candles, col)))
        check(f"열 캔들 왕복: {label}", same)
    col = ColumnarCandles.from_candles(mixed)
    check("열 캔들 인덱싱: [0], [-1], 슬라이스",
          _fields(col[0]) == _fields(mixed[0]) and _fields(col[-1]) == _fields(mixed[-1])
          and [_fields(c) for c in col[10:20]] == [_fields(c) for c in mixed[10:20]])
    try:
        col.close[0] = 0.0
        check("열 캔들은 읽기 전용", False, "배열에 쓰기가 됐다")
    except ValueError:
        check("열 캔들은 읽기 전용", True)


def check_cache_keys():
    """``cache_key``가 같으면 ``compute()``가 비트 단위로 같고, 계산을 바꾼 서브클래스는 키가
    없는가."""
    arrays = _ohlcv(synthetic_candles(5000, seed=5))
    for name, make in _indicator_cases(60):
        a, b = make(), make()
        ka, kb = a.cache_key(), b.cache_key()
        check(f"cache_key: {name}", ka is not None and ka == kb
              and np.array_equal(a.compute(*arrays), b.compute(*arrays), equal_nan=True),
              f"{ka!r} vs {kb!r}")
    check("cache_key: 창이 다르면 키가 다르다",
          MovingAverage(60).cache_key() != MovingAverage(61).cache_key())
    check("cache_key: 종류가 다르면 키가 다르다",
          MaxDonchianIndicator(60).cache_key() != MinDonchianIndicator(60).cache_key())

    class _Shifted(MovingAverage):  # compute를 바꾼 서브클래스 — 부모 키를 물려받으면 안 된다
        def compute(self, open, high, low, close, volume):
            return super().compute(open, high, low, close, volume) + 1.0

    class _Plain(MovingAverage):  # 계산은 그대로 — 부모와 키를 공유해도 된다
        pass

    check("cache_key: compute를 오버라이드한 서브클래스는 None", _Shifted(60).cache_key() is None)
    check("cache_key: 상속만 한 서브클래스는 부모와 같은 키",
          _Plain(60).cache_key() == MovingAverage(60).cache_key())


def _run_simple(streamer, candles_by_symbol, precomputed=None):
    executor = SimulatedExecutor(INIT_MARGIN)
    return BacktestEngine(streamer, candles_by_symbol, executor, SimpleRecorder(executor.status),
                          progress=False, precomputed=precomputed).run()


def check_columnar_and_cache_parity(by_symbol: Dict[str, List[Candle]], label: str):
    """리스트 입력 == 열 입력 == 캐시 주입(빈 캐시 → 채운 캐시) Report."""
    symbols = list(by_symbol)
    kp = dict(window=6 * 60, m_entry=2.0, m_exit=0.0, max_loss=0.02)
    make = lambda: KeltnerStreamer(symbols=symbols, **kp)  # noqa: E731
    columnar = {s: ColumnarCandles.from_candles(c) for s, c in by_symbol.items()}
    ref = _run_simple(make(), by_symbol)
    col = _run_simple(make(), columnar)
    check(f"리스트 == 열 캔들: {label}", compare_reports(label, ref, col),
          f"trades={len(ref.trades)} final={ref.status.total_margin():.2f}")
    cache: Dict = {}
    first = _run_simple(make(), columnar, cache)
    filled = len(cache)
    second = _run_simple(make(), columnar, cache)
    check(f"캐시 주입 == 미주입: {label}",
          filled > 0 and len(cache) == filled
          and compare_reports(label, ref, first) and compare_reports(label, ref, second),
          f"캐시 {filled}개")


def check_sweep_parity(by_symbol: Dict[str, List[Candle]], label: str):
    """``ParameterSweep``(순차/fork)의 요약 == 엔진을 직접 돌린 요약."""
    sym = next(iter(by_symbol))
    candles = {sym: ColumnarCandles.from_candles(by_symbol[sym])}
    grid = [dict(window=w, m_entry=2.0, m_exit=0.0, max_loss=ml)
            for w in (3 * 60, 6 * 60) for ml in (0.01, 0.02, 0.04)]
    factory = lambda syms, p: KeltnerStreamer(symbols=syms, **p)  # noqa: E731
    jobs = [SweepJob(sym, p) for p in grid]

    def strip(summary):
        return repr(sorted((k, v) for k, v in summary.items()
                           if k not in ("elapsed_s", "worker_private_gb")))

    ref = [strip(summarise_report(_run_simple(factory([sym], p), by_symbol), INIT_MARGIN))
           for p in grid]
    for workers in (1, 3):
        rows = ParameterSweep(factory, candles, init_margin=INIT_MARGIN,
                              workers=workers).run(jobs)
        got = [strip(summary) for _, summary in rows]
        order_ok = [job.params for job, _ in rows] == grid
        bad = [i for i, (a, b) in enumerate(zip(ref, got)) if a != b]
        check(f"스윕 == 엔진 직접 실행: {label}, workers={workers}", order_ok and not bad,
              f"불일치 칸 {bad}" if bad else f"{len(grid)}칸")


def _pure_synthetic(n: int, seed: int) -> List[Candle]:
    """필드가 파이썬 float인 합성 캔들. ``synthetic_candles``는 ``np.float64``를 담는데, 그걸로
    돌린 엔진은 결과에도 numpy 스칼라가 섞이고 수수료 합의 끝자리까지 달라진다 — 실제 캔들
    (vision/REST)은 파이썬 float이고 열 캔들도 파이썬 float을 내므로, 대조군도 맞춘다."""
    return [Candle(*(float(getattr(c, k)) for k in ("open", "high", "low", "close", "volume")),
                   c.start_time, c.end_time) for c in synthetic_candles(n, seed)]


def check_columnar_offline():
    check_columnar_roundtrip()
    check_cache_keys()
    eth = _pure_synthetic(20_000, seed=11)
    btc = _pure_synthetic(20_000, seed=12)[300:]
    btc = btc[:8000] + btc[9000:]
    check_columnar_and_cache_parity({SYM: eth}, "합성 단일 심볼")
    check_columnar_and_cache_parity({SYM: eth, SYM2: btc}, "합성 ragged 2심볼")
    check_sweep_parity({SYM: eth}, "합성")


def check_columnar_real():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 2, 1, tzinfo=timezone.utc)
    eth = _load(SYM, start, end)
    btc = _load(SYM2, start, end)[500:]
    check_columnar_and_cache_parity({SYM: eth}, "실제 캔들")
    check_columnar_and_cache_parity({SYM: eth, SYM2: btc[:5000] + btc[5800:]},
                                    "실제 캔들 ragged 2심볼")
    check_sweep_parity({SYM: eth}, "실제 캔들")


# ====================================================


def main():
    setup_logging(default="WARNING")
    check_indicator_exactness()
    check_sink_playback()
    check_degenerate_inputs()
    check_symbol_rules()
    check_columnar_offline()
    if "--offline" not in sys.argv:
        check_strategy_parity()
        check_multi_symbol_ragged()
        check_columnar_real()

    if _failures:
        print(f"BACKTEST CHECK: FAILED ({len(_failures)}건) — {_failures}")
        sys.exit(1)
    print("BACKTEST CHECK: ALL OK")


if __name__ == "__main__":
    main()
