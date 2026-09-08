"""Live path check: LiveCandleProducer / LiveExecutor / 드라이런 == 백테스트.

라이브 경로는 순수 백테스트 검증이 닿지 않는 곳이다 — 소켓과 거래소가
필요하기 때문이다. 여기서는 둘 다 가짜로 세워 그 경로들을 오프라인으로 태운다:

1. **캔들 공급자** — 연속/중복/구멍 백필/상한 초과/경계 불일치/에러 프레임 (가짜 소켓),
   그리고 **지표 워밍업 이어붙이기** — 워밍업 소스와 실시간 소스가 별개 객체라, 어디까지
   먹였는지가 ``resume_after``로 넘어가야 그 사이 구멍이 백필된다
2. **라이브 실행기** — 계좌 갱신, 부분 체결 합치기, 미체결 장부 동기화, 수수료 자산 분리
   (가짜 주문 클라이언트 + 손으로 만든 유저 데이터 메시지)
3. **드라이런 == 백테스트** — 같은 캔들을 라이브의 이벤트 모양(심볼별 단일 키, 비동기)으로
   흘려보내고 백테스트 결과와 체결을 대조한다. 드라이런이 존재하는 이유가 이 대조이고,
   이제 둘은 같은 엔진과 같은 ``SimulatedExecutor``를 쓰므로 **완전히** 같아야 한다.

단일 심볼로 대조하는 이유: 백테스트는 같은 시각에 마감한 심볼들을 하나의 이벤트로 병합하지만
라이브는 심볼별로 따로 처리한다. 심볼이 하나면 두 이벤트 스트림이 동일해지므로, 그 차이를
빼고 순수하게 체결 규칙만 대조할 수 있다.

    uv run python core/checks/live_check.py            # 전부
    uv run python core/checks/live_check.py --offline  # 캔들 캐시가 필요 없는 1, 2 만
"""
import asyncio
import math
import sys
from concurrent.futures import Future
from datetime import datetime, timezone
from typing import Dict, List

from core.backtest.in_memory_candle_producer import InMemoryCandleProducer
from core.backtest.recorder import BacktestRecorder
from core.backtest.simulated_executor import SimulatedExecutor
from core.domain.action import Action, ActionType
from core.domain.candle import Candle
from core.domain.status import Status
from core.engine.candle_producer import CandleProducer, Event
from core.engine.engine import TradingEngine
from core.fetcher.binance.vision_fetcher import BinanceVisionFetcher
from core.live import candle_producer as lcp
from core.live.binance_order_client import OrderResult
from core.live.executor import LiveExecutor, resolve_margin_asset
from core.logging_config import setup_logging
from core.streamer.base_streamer import BaseStreamer
from core.streamer.strategies.keltner_stop_streamer import KeltnerStopStreamer
from core.streamer.strategies.keltner_streamer import KeltnerStreamer
from core.streamer.strategies.mean_reversion_zscore import MeanReversionZScoreStreamer
from core.utils import trunc_by_sign

MIN = 60_000
SYM, SYM2 = "ETHUSDT", "BTCUSDT"
T0 = 1_700_000_000_000 - (1_700_000_000_000 % MIN)
INIT_MARGIN = 100_000.0

_failures = []


def check(label, cond, detail=""):
    if not cond:
        _failures.append(label)
    print(f"[{label}] {'OK' if cond else 'FAIL'}{(' — ' + detail) if detail else ''}")


# ==================================================== 드라이런 대조용 픽스처
# (예전 backtest_fast_check.py에 있던 토이 전략 + Report 비교 헬퍼. 지금은 드라이런과
#  백테스트 Report가 완전히 같은지 확인하는 데만 쓴다.)


class LimitLadderStreamer(BaseStreamer):
    """토이 지정가 전략 (검사 전용) — 미체결 주문이 여러 봉에 걸쳐 사는 경로를 만든다.

    플랫이면 종가보다 ``spread`` 만큼 아래에 지정가 매수를 걸고, 포지션이 있으면 진입가보다
    ``spread`` 위에 지정가 매도(익절)를 건다. 주문이 ``ttl`` 캔들 안에 안 채워지면 만료된다.
    가끔 전량 취소를 섞어 CANCEL 경로도 태운다.

    지정가는 ``candle.close``에서만 유도되므로 (지표를 거치지 않는다) 백테스트와 드라이런의
    체결가가 비트 단위로 같다 — 구조적 버그가 float 반올림에 묻히지 않는다.
    """

    def __init__(self, symbols: List[str], spread: float = 0.002, ttl: int = 30):
        super().__init__(symbols, {s: {} for s in symbols})
        self.spread = spread
        self.ttl = ttl
        self._bar = 0

    def decide_action(self, candles: Dict[str, Candle], status: Status):
        self._bar += 1  # 이벤트당 1회 (검사 데이터는 단일 심볼이라 봉 카운트와 같다)
        actions = []
        for symbol in self.symbols:
            candle = candles.get(symbol)
            if candle is not None:
                actions.extend(self._decide_symbol(symbol, candle, status))
        return actions

    def _decide_symbol(self, symbol, candle: Candle, status: Status):
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

    ``reduce_only`` clamp와 갭 관통 체결(트리거보다 아래에서 봉이 시작하면 시가 체결)을
    태운다.
    """

    def _decide_symbol(self, symbol, candle: Candle, status: Status):
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
        # 롱만 잡으므로 손절은 항상 진입가 아래 (trigger_above=False).
        return [Action(symbol, -position * 3, order_type=ActionType.STOP_MARKET,
                       trigger_price=avg * (1 - self.spread), trigger_above=False,
                       reduce_only=True, client_id="stop")]


def compare_reports(label, ref_report, fast_report) -> bool:
    """두 Report(백테스트 vs 드라이런)가 체결·자본곡선·벤치마크까지 같은지 확인한다."""
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
        # 허용오차는 **상대**가 본질이다. 지표 반올림 차이가 체결가와 avg_price를 통해 자본에
        # 누적되므로, 오차는 계좌 크기에 비례해서 커진다. 고정 1e-6 절대값만 쓰면 100배로
        # 불어난 계좌에서 순수 반올림이 실패로 잡힌다 — 구조적 어긋남은 자본의 유의미한
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
    # 잘라냈는지까지 확인한다.
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


# ============================================================ 1. 캔들 공급자


def kline_msg(start_ms, closed=True, symbol=SYM, close=100.0):
    return {"stream": f"{symbol.lower()}_perpetual@continuousKline_1m",
            "data": {"ps": symbol, "k": {"t": start_ms, "o": "100", "h": "101",
                                         "l": "99", "c": str(close), "v": "10",
                                         "x": closed}}}


class FakeKlineSocket:
    def __init__(self, messages):
        self._messages = list(messages)

    async def recv(self):
        if not self._messages:
            # 메시지가 떨어지면 소켓이 죽은 것처럼 군다 (치명적 종료 경로)
            raise ConnectionError("no more messages")
        return self._messages.pop(0)

    async def close(self):
        pass


def make_producer(messages, backfill=None):
    errors = []

    async def on_error(e):
        errors.append(e)

    p = lcp.LiveCandleProducer(socket_manager=None, symbols=[SYM], interval="1m",
                               on_error=on_error)
    p.socket = FakeKlineSocket(messages)
    p._running = True
    if backfill is not None:
        async def _fetch(symbol, start_ms, end_ms):
            return backfill(symbol, start_ms, end_ms)
        p._fetch_range = _fetch
    return p, errors


def synthetic_candles(start_ms, n):
    return [Candle(open=100, high=101, low=99, close=100, volume=1,
                   start_time=start_ms + i * MIN, end_time=start_ms + (i + 1) * MIN)
            for i in range(n)]


async def check_candle_producer():
    async def collect(producer):
        return [ev async for ev in producer]

    p, errs = make_producer([kline_msg(T0), kline_msg(T0 + MIN), kline_msg(T0 + 2 * MIN)])
    evs = await collect(p)
    check("producer: 연속 3캔들",
          [e.time for e in evs] == [T0 + MIN, T0 + 2 * MIN, T0 + 3 * MIN],
          f"{[e.time for e in evs]}")
    check("producer: 이벤트는 단일 심볼", all(list(e.candles) == [SYM] for e in evs))
    check("producer: 실시간 캔들은 decide=True", all(e.decide for e in evs))
    c = evs[0].candles[SYM]
    # 웹소켓의 T(closeTime)는 경계 - 1ms 지만 페처는 경계를 쓴다. 그대로 두면 백필 캔들과
    # 라이브 캔들의 타임스탬프가 1ms 어긋나고 백테스트 시계열과도 짝이 맞지 않는다.
    check("producer: end_time 인터벌 경계 정규화",
          c.start_time == T0 and c.end_time == T0 + MIN)

    p, errs = make_producer([kline_msg(T0), kline_msg(T0), kline_msg(T0 + MIN)])
    evs = await collect(p)
    check("producer: 중복 캔들 스킵", len(evs) == 2 and not errs,
          f"{len(evs)}개, errors={len(errs)}")

    fetched = {}

    def backfill(symbol, start_ms, end_ms):
        fetched["range"] = (start_ms, end_ms)
        return synthetic_candles(start_ms, (end_ms - start_ms) // MIN)

    p, errs = make_producer([kline_msg(T0), kline_msg(T0 + 3 * MIN)], backfill=backfill)
    evs = await collect(p)
    check("producer: 구멍 백필이 먼저 나온다",
          [e.time for e in evs] == [T0 + MIN, T0 + 2 * MIN, T0 + 3 * MIN, T0 + 4 * MIN]
          and fetched.get("range") == (T0 + MIN, T0 + 3 * MIN),
          f"{[e.time for e in evs]} range={fetched.get('range')}")
    # 백필 캔들은 이미 지나간 봉이다 — 지표와 실행기까지만 가고 decide_action에는 닿으면
    # 안 된다. 여기가 True로 새면 몇 분 전 봉을 보고 지금 가격에 시장가가 나간다.
    check("producer: 백필 캔들만 decide=False",
          [e.decide for e in evs] == [True, False, False, True],
          f"{[e.decide for e in evs]}")

    p, _ = make_producer([kline_msg(T0),
                          kline_msg(T0 + (lcp.MAX_BACKFILL_CANDLES + 5) * MIN)],
                         backfill=backfill)
    evs = await collect(p)
    check("producer: 백필 상한 초과 → 치명적", len(evs) == 1 and p.fatal_reason is not None)

    p, _ = make_producer([kline_msg(T0), kline_msg(T0 + MIN + 137)])
    evs = await collect(p)
    check("producer: 봉 경계 불일치 → 치명적", len(evs) == 1 and p.fatal_reason is not None)

    def boom(symbol, start_ms, end_ms):
        raise RuntimeError("REST 실패")

    p, _ = make_producer([kline_msg(T0), kline_msg(T0 + 3 * MIN)], backfill=boom)
    evs = await collect(p)
    # 지표가 이미 갈라졌다. 이 상태로 계속 매매하면 전략이 백테스트와 다른 것을 본다.
    check("producer: 백필 실패 → 치명적", len(evs) == 1 and p.fatal_reason is not None)

    p, errs = make_producer([{"e": "error", "m": "boom"}, kline_msg(T0)])
    evs = await collect(p)
    check("producer: 에러 프레임은 on_error로", len(evs) == 1 and len(errs) == 1)

    p, errs = make_producer([kline_msg(T0, closed=False),
                             kline_msg(T0, symbol="DOGEUSDT"), kline_msg(T0)])
    evs = await collect(p)
    check("producer: 미마감/모르는 심볼 무시", len(evs) == 1 and not errs)

    p, _ = make_producer([kline_msg(T0), kline_msg(T0 + MIN), kline_msg(T0 + 2 * MIN)])
    got = []
    async for ev in p:
        got.append(ev)
        p.request_stop()
    check("producer: request_stop", len(got) == 1, f"{len(got)}개")


async def check_warmup():
    """지표 워밍업: 별개 소스로 먹이고, 실시간 소스에 이어붙이는 지점까지 넘어가는가.

    라이브 기동은 "과거 구간을 REST로 받아 지표에 먹인 뒤 소켓을 연다"인데, 그 두 소스는
    서로 다른 객체다. 마지막으로 먹인 지점이 실시간 소스로 넘어가지 않으면 그 사이 마감한
    봉이 조용히 사라져 모든 롤링 윈도우 지표가 백테스트와 영구히 갈라진다.
    """
    window = 5

    def assemble():
        streamer = KeltnerStreamer(symbols=[SYM], window=window)
        p, _ = make_producer([kline_msg(T0 + (window + 1) * MIN)], backfill=backfill)
        engine = TradingEngine(streamer, p, SimulatedExecutor(INIT_MARGIN))
        return streamer, p, engine

    fetched = {}

    def backfill(symbol, start_ms, end_ms):
        fetched["range"] = (start_ms, end_ms)
        return synthetic_candles(start_ms, (end_ms - start_ms) // MIN)

    streamer, p, engine = assemble()
    warmup = InMemoryCandleProducer({SYM: synthetic_candles(T0, window)}, progress=False)
    await engine.warmup_from(warmup)
    check("warmup: 지표가 데워진다",
          streamer.indicators[SYM]["MA"].get_latest() is not None)
    # 마지막으로 먹인 캔들의 start_time — end_time이 아니다 (연속성 판정 기준이 start다).
    check("warmup: 연속성 기준점이 실시간 공급자로 넘어간다",
          p._last_candle_start[SYM] == T0 + (window - 1) * MIN,
          f"{p._last_candle_start[SYM]}")

    # 워밍업은 T0+4분까지 먹었고 첫 실시간 캔들은 T0+6분 — 그 사이 T0+5분이 백필돼야 한다.
    evs = [ev async for ev in p]
    check("warmup: 워밍업과 첫 실시간 캔들 사이 구멍이 백필된다",
          fetched.get("range") == (T0 + window * MIN, T0 + (window + 1) * MIN)
          and [e.time for e in evs] == [T0 + (window + 1) * MIN, T0 + (window + 2) * MIN]
          and [e.decide for e in evs] == [False, True],
          f"range={fetched.get('range')} {[(e.time, e.decide) for e in evs]}")

    # 조용히 덜 데워진 지표로 매매하느니 기동에 실패하는 편이 낫다.
    _, _, engine = assemble()
    short = InMemoryCandleProducer({SYM: synthetic_candles(T0, window - 1)}, progress=False)
    try:
        await engine.warmup_from(short)
        raised = False
    except ValueError:
        raised = True
    check("warmup: 캔들이 모자라면 기동 실패", raised)


# ============================================================ 2. 라이브 실행기


class FakeOrderClient:
    def __init__(self, success=True):
        self.calls = []
        self.success = success

    def execute_action(self, action, client_order_id=None):
        self.calls.append(action)
        f = Future()
        f.set_result(OrderResult(success=self.success, order_id="1",
                                 error=None if self.success else "rejected"))
        return f


class FakeAsyncClient:
    """``LiveExecutor.create()``가 쓰는 엔드포인트에만 답하는 가짜 AsyncClient.

    ``marginBalance``를 일부러 ``walletBalance``와 다르게 실어 둔다 — 그쪽을 쓰면 미실현
    손익이 두 번 세어지므로, 어느 필드를 골랐는지가 검증 대상이다.
    """

    def __init__(self, margin=10_000.0, positions=None, open_orders=None, error=None,
                 orders_boom=False, taker_rate="0.0004"):
        self.margin = margin
        self.positions = positions or {}
        self.open_orders = open_orders or []
        self.error = error
        self.orders_boom = orders_boom
        self.taker_rate = taker_rate

    async def futures_account(self):
        if self.error:
            return {"error": self.error}
        return {
            "assets": [{"asset": "USDT", "walletBalance": str(self.margin),
                        "marginBalance": str(self.margin + 777.0)}],
            "positions": [{"symbol": s, "positionAmt": str(pa), "entryPrice": str(ep),
                           "unrealizedProfit": str(up)}
                          for s, (pa, ep, up) in self.positions.items()],
        }

    async def futures_get_open_orders(self):
        if self.orders_boom:
            raise RuntimeError("REST 실패")
        return self.open_orders

    async def futures_commission_rate(self, symbol=None):
        return {"symbol": symbol, "makerCommissionRate": "0.0002",
                "takerCommissionRate": self.taker_rate}


async def make_live_executor(margin=10_000.0, client=None):
    trades, errors, meta = [], [], []

    async def on_error(e):
        errors.append(e)

    # 생성 경로는 create() 하나뿐이다 — 계좌가 적재되지 않은 실행기는 만들 수 없다.
    ex = await LiveExecutor.create(
        [SYM, SYM2], order_client=FakeOrderClient(),
        client=client or FakeAsyncClient(margin=margin),
        on_trade=trades.append, on_error=on_error,
        on_metadata=lambda k, v: meta.append((k, v)))
    return ex, trades, errors, meta


def acct_msg(margin=None, positions=None, m="ORDER"):
    a = {"m": m, "B": [], "P": []}
    if margin is not None:
        a["B"] = [{"a": "USDT", "wb": str(margin)}]
    for sym, (pa, ep, up) in (positions or {}).items():
        a["P"].append({"s": sym, "pa": str(pa), "ep": str(ep), "up": str(up)})
    return {"e": "ACCOUNT_UPDATE", "a": a}


def order_msg(symbol=SYM, status="FILLED", x="TRADE", side="BUY", z=1.0, ap=100.0,
              rp=0.0, n=0.04, N="USDT", c="st-1", i=7, T=1000, otype="MARKET", q=1.0,
              sp=None, R=False):
    o = {"s": symbol, "X": status, "x": x, "S": side, "z": str(z), "ap": str(ap),
         "rp": str(rp), "n": str(n), "N": N, "c": c, "i": i, "T": T, "o": otype,
         "q": str(q), "R": R}
    if sp is not None:
        o["sp"] = str(sp)
    return {"e": "ORDER_TRADE_UPDATE", "o": o}


async def check_live_executor():
    check("executor: 정산 자산 해석",
          resolve_margin_asset(["ETHUSDT", "1000PEPEUSDT"]) == "USDT")
    try:
        resolve_margin_asset(["ETHUSDT", "ETHBTC"])
        check("executor: 혼합 정산 자산 거부", False)
    except ValueError:
        check("executor: 혼합 정산 자산 거부", True)

    # 생성이 곧 계좌 적재다 — 수화되지 않은 Status를 든 실행기는 존재할 수 없어야 한다.
    # walletBalance를 골라야 한다: marginBalance는 미실현을 이미 포함하고 total_margin()이
    # 그걸 또 더하므로, 잘못 고르면 자본이 조용히 부풀어 사이징 전체가 어긋난다.
    ex, _, _, _ = await make_live_executor(client=FakeAsyncClient(
        margin=7_000.0, positions={SYM: (2.0, 100.0, 10.0)},
        open_orders=[{"symbol": SYM, "type": "STOP_MARKET", "side": "SELL", "origQty": "2",
                      "stopPrice": "90", "reduceOnly": True, "clientOrderId": "stop-r",
                      "orderId": 11, "time": 500}]))
    book = ex.status.open_orders_for(SYM)
    check("executor: 생성 시점에 계좌/장부가 이미 채워져 있다",
          ex.status.margin == 7_000.0 and ex.status.position_for(SYM).position == 2.0
          and ex.status.position_for(SYM).avg_price == 100.0 and len(book) == 1
          and book[0].trigger_price == 90.0 and book[0].reduce_only,
          f"margin={ex.status.margin} book={book}")

    # 미체결 조회 실패는 치명적이지 않다 — 장부가 비어 보일 뿐이고 ORDER_TRADE_UPDATE가 채운다.
    ex, _, _, _ = await make_live_executor(client=FakeAsyncClient(margin=3_000.0,
                                                                 orders_boom=True))
    check("executor: 미체결 조회 실패는 치명적이지 않다",
          ex.status.margin == 3_000.0 and ex.status.total_open_orders() == 0)

    # 반대로 계좌 조회 실패는 치명적이다 — 잔고를 모르는 채로 사이징하면 안 된다.
    try:
        await make_live_executor(client=FakeAsyncClient(error="permission denied"))
        check("executor: 계좌 조회 실패는 치명적", False)
    except RuntimeError:
        check("executor: 계좌 조회 실패는 치명적", True)

    # ACCOUNT_UPDATE는 **변경된 항목만** 싣는다. 없는 항목을 0으로 덮으면 마진이 0이 되어
    # 모든 사이징이 붕괴하거나, 포지션이 0으로 보여 재진입해 실제 포지션이 2배가 된다.
    ex, _, _, _ = await make_live_executor()
    await ex.on_user_data(acct_msg(margin=5000.0, positions={SYM: (2.0, 100.0, 10.0)}))
    check("executor: ACCOUNT_UPDATE 반영",
          ex.status.margin == 5000.0 and ex.status.position_for(SYM).position == 2.0
          and ex.status.position_for(SYM).unrealised_pnl == 10.0)
    await ex.on_user_data(acct_msg())
    check("executor: 빈 ACCOUNT_UPDATE는 무해",
          ex.status.margin == 5000.0 and ex.status.position_for(SYM).position == 2.0)
    await ex.on_user_data(acct_msg(margin=6000.0, positions={SYM: (0.0, 0.0, 0.0)},
                                   m="FUNDING_FEE"))
    check("executor: FUNDING_FEE는 포지션을 건드리지 않는다",
          ex.status.margin == 6000.0 and ex.status.position_for(SYM).position == 2.0)
    await ex.on_user_data(acct_msg(positions={SYM: (1.0, 1.0, 1.0), SYM2: (3.0, 2.0, 2.0),
                                              "XRPUSDT": (9.0, 9.0, 9.0)}))
    check("executor: 여러 심볼 동시 갱신 + 모르는 심볼 무시",
          ex.status.position_for(SYM).position == 1.0
          and ex.status.position_for(SYM2).position == 3.0
          and "XRPUSDT" not in ex.status.positions)

    # 거래소는 한 주문을 여러 번에 나눠 채울 수 있는데, 부분 체결마다 Trade를 만들면
    # "액션 하나 = 체결 하나"인 백테스트와 모양이 달라진다.
    ex, trades, _, _ = await make_live_executor()
    ex.submit(Action(SYM, 2.0), event_time=5000)
    cid = ex._orders.calls[0].client_id
    await ex.on_user_data(order_msg(status="PARTIALLY_FILLED", z=1.0, ap=100.0, rp=1.0,
                                    n=0.04, c=cid))
    check("executor: 부분 체결은 아직 Trade 아님", not trades)
    await ex.on_user_data(order_msg(status="FILLED", z=2.0, ap=101.0, rp=3.0, n=0.05, c=cid))
    check("executor: 종결 시 하나의 Trade로 합침",
          len(trades) == 1 and trades[0].quantity == 2.0 and trades[0].price == 101.0
          and abs(trades[0].wnl - 4.0) < 1e-9 and abs(trades[0].fee - 0.09) < 1e-9,
          f"{trades[0] if trades else None}")
    # T는 캔들 경계보다 수백 ms 뒤라, 집계 뷰에서 원인이 된 캔들과 다른 버킷에 떨어진다.
    check("executor: 시장가 timestamp = 결정 캔들 마감 시각",
          trades[0].timestamp == 5000 and trades[0].submitted_at == 5000)

    ex, trades, _, _ = await make_live_executor()
    ex.submit(Action(SYM, 1.0), event_time=1)
    cid = ex._orders.calls[0].client_id
    await ex.on_user_data(order_msg(status="CANCELED", x="CANCELED", z=0.0, c=cid))
    check("executor: 미체결 취소는 Trade 없이 pending 정리",
          not trades and not ex._pending_decision)

    ex, trades, _, _ = await make_live_executor()
    await ex.on_user_data(order_msg(status="NEW", x="NEW", z=0.0, otype="STOP_MARKET",
                                    sp=95.0, side="SELL", c="stop-1"))
    book = ex.status.open_orders_for(SYM)
    check("executor: NEW로 장부 등록 + 방향/트리거 해석",
          len(book) == 1 and book[0].order_type is ActionType.STOP_MARKET
          and book[0].quantity == -1.0 and book[0].trigger_price == 95.0
          and book[0].trigger_above is False, f"{book}")
    await ex.on_user_data(order_msg(status="FILLED", z=1.0, ap=95.0, side="SELL", c="stop-1"))
    check("executor: 종결로 장부 해제", ex.status.total_open_orders() == 0)
    # 미체결 주문은 결정이 몇 봉 전이므로 결정 캔들에 버킷하면 오히려 틀리다.
    check("executor: 미체결 체결 timestamp = 실제 체결 시각",
          len(trades) == 1 and trades[0].timestamp == 1000)

    # 청산/ADL/앱에서 낸 수동 주문 — 실제 자본을 움직이므로 기록은 해야 한다.
    ex, trades, _, _ = await make_live_executor()
    await ex.on_user_data(order_msg(status="FILLED", z=1.0, ap=50.0, c="unknown-id"))
    check("executor: 짝지어지지 않은 체결도 기록", len(trades) == 1 and trades[0].price == 50.0)

    # BNB 수수료 할인을 켜면 n이 BNB 단위로 온다. 마진 자산 손익에 더하면 wnl - fee 가 오염된다.
    ex, trades, _, meta = await make_live_executor()
    ex.submit(Action(SYM, 1.0), event_time=1)
    cid = ex._orders.calls[0].client_id
    await ex.on_user_data(order_msg(status="FILLED", z=1.0, n=0.5, N="BNB", c=cid))
    check("executor: 다른 자산 수수료 분리",
          trades[0].fee == 0.0 and meta == [("fee_asset_mismatch", "BNB")])

    ex, _, _, _ = await make_live_executor()
    await ex.on_user_data(order_msg(status="NEW", x="NEW", z=0.0, otype="STOP_MARKET",
                                    sp=95.0, side="SELL", c="stop-1"))
    ex.submit(Action.cancel(SYM, "stop-1"), event_time=1)
    check("executor: CANCEL은 장부를 비우고 거래소로도 나간다",
          ex.status.total_open_orders() == 0
          and ex._orders.calls[-1].order_type is ActionType.CANCEL)

    # 라이브는 미체결 주문을 캔들로 시뮬레이션하지 않는다 — begin_event가 트리거를 넘긴
    # 캔들을 받아도 장부는 그대로고 Trade도 생기지 않는다 (거래소가 체결하고 유저 데이터로 온다).
    ex, trades, _, _ = await make_live_executor()
    await ex.on_user_data(order_msg(status="NEW", x="NEW", z=0.0, otype="STOP_MARKET",
                                    sp=95.0, side="SELL", c="stop-sim"))
    ex.begin_event(2, {SYM: Candle(1, 1, 1, 1, 1, 0, MIN)})
    check("executor: 미체결 주문을 시뮬레이션하지 않는다",
          not trades and ex.status.total_open_orders() == 1)

    ex, _, _, _ = await make_live_executor()
    ex.submit(Action(SYM, 1.0), event_time=1)
    ex.submit(Action(SYM, -1.0, order_type=ActionType.STOP_MARKET,
                     trigger_price=90.0, trigger_above=False, client_id="stop-x"), event_time=1)
    ex.begin_event(2, {SYM: Candle(1, 1, 1, 1, 1, 0, MIN)})
    left = list(ex._pending_decision.values())
    # 미체결 주문은 몇 봉 뒤 체결이 정상이므로 캔들 경계에서 버리면 안 된다.
    check("executor: 캔들 경계에서 시장가 결정만 폐기",
          len(left) == 1 and left[0].resting is True)

    ex, _, errors, _ = await make_live_executor()
    ex._orders.success = False
    # 실행기는 재시도 소진 후 예외 대신 success=False를 반환한다 — 확인하지 않으면 영구 거부된
    # 주문이 아무 흔적 없이 지나가고 전략이 거래소와 어긋난다. submit은 결과-대기를 detached
    # 태스크로 띄우므로 drain_pending_orders로 끝날 때까지 기다린 뒤 확인한다.
    ex.submit(Action(SYM, 1.0), event_time=1)
    await ex.drain_pending_orders()
    check("executor: 주문 실패는 on_error로", len(errors) == 1, f"{errors}")


# ============================================================ 3. 드라이런 == 백테스트


class ReplayProducer(CandleProducer):
    """라이브처럼 심볼별 단일 키 이벤트를 비동기로 내주는 소스 (캔들은 메모리에서)."""

    def __init__(self, candles_by_symbol, interval_ms: int):
        super().__init__(interval_ms)
        self._events = sorted(
            ((c.end_time, sym, c) for sym, cs in candles_by_symbol.items() for c in cs),
            key=lambda t: (t[0], t[1]))

    async def __aiter__(self):
        for end_time, symbol, candle in self._events:
            yield Event(end_time, {symbol: candle})


def _assemble(streamer, producer, slippage_ratio, log_label=""):
    """부품을 조립해 엔진을 만든다. 백테스트와 드라이런의 **차이는 공급자뿐**이다.

    ``on_trade``는 엔진 생성자가 레코더로 이어 준다.
    """
    executor = SimulatedExecutor(
        INIT_MARGIN, slippage_ratio=(slippage_ratio or 0.0), log_label=log_label)
    recorder = BacktestRecorder(streamer, executor.status, interval_ms=producer.interval_ms)
    return TradingEngine(streamer, producer, executor, recorder)


async def run_dry(streamer, candles_by_symbol, interval_ms, slippage_ratio=None):
    """``BinanceTrader``의 드라이런과 **같은 부품 구성**으로 돌린다."""
    producer = ReplayProducer(candles_by_symbol, interval_ms)
    return await _assemble(streamer, producer, slippage_ratio, "dry-run").run_async()


async def run_bt(streamer, candles_by_symbol, slippage_ratio=None):
    """드라이런과 같은 조립, 공급자만 백테스트의 병합 타임라인이다."""
    producer = InMemoryCandleProducer(candles_by_symbol, progress=False)
    return await _assemble(streamer, producer, slippage_ratio).run_async()


async def check_dry_run_parity():
    interval = "1m"
    candles = BinanceVisionFetcher(compress=False).get_candles_with_cache(
        SYM, datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 2, 1, tzinfo=timezone.utc), interval)
    print(f"Loaded {len(candles)} candles for dry-run parity")
    by_symbol = {SYM: candles}
    kp = dict(window=20 * 60, m_entry=2.0, m_exit=0.0, max_loss=0.02)

    cases = [
        ("dry-run: Keltner (시장가)", lambda: KeltnerStreamer(symbols=[SYM], **kp), None),
        ("dry-run: MeanReversionZScore (상태 있는 전략)",
         lambda: MeanReversionZScoreStreamer(symbol=SYM, window=60, entry_z=2.0,
                                             timeout_candles=60, max_loss=0.02), None),
        ("dry-run: KeltnerStop (조건부 주문)",
         lambda: KeltnerStopStreamer(symbols=[SYM], window=72 * 60, m_entry=4.0,
                                     m_exit=3.0, max_loss=0.005), None),
        ("dry-run: LimitLadder (지정가 + 취소)", lambda: LimitLadderStreamer([SYM]), None),
        ("dry-run: StopLadder (reduce_only + 슬리피지)",
         lambda: StopLadderStreamer([SYM]), 0.0005),
    ]

    for label, make_streamer, slippage in cases:
        bt = await run_bt(make_streamer(), by_symbol, slippage)
        dry = await run_dry(make_streamer(), by_symbol, MIN, slippage)
        check(label, compare_reports(label, bt, dry),
              f"trades={len(bt.trades)} final={bt.status.total_margin():.2f}")


async def main():
    # 판정 결과를 print로 읽는 게 본론이라 기본 레벨을 WARNING으로 둔다.
    setup_logging(default="WARNING")
    await check_candle_producer()
    await check_warmup()
    await check_live_executor()
    if "--offline" not in sys.argv:
        await check_dry_run_parity()

    if _failures:
        print(f"LIVE CHECK: FAILED ({len(_failures)}건) — {_failures}")
        sys.exit(1)
    print("LIVE CHECK: ALL OK")


if __name__ == "__main__":
    asyncio.run(main())
