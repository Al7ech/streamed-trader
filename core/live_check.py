"""Live path check: LiveCandleProducer / LiveExecutor / 드라이런 == 백테스트.

라이브 경로는 ``backtest_fast_check.py``의 패리티 검증이 닿지 않는 곳이다 — 소켓과 거래소가
필요하기 때문이다. 여기서는 둘 다 가짜로 세워 그 경로들을 오프라인으로 태운다:

1. **캔들 공급자** — 연속/중복/구멍 백필/상한 초과/경계 불일치/에러 프레임 (가짜 소켓)
2. **라이브 실행기** — 계좌 갱신, 부분 체결 합치기, 미체결 장부 동기화, 수수료 자산 분리
   (가짜 주문 클라이언트 + 손으로 만든 유저 데이터 메시지)
3. **드라이런 == 백테스트** — 같은 캔들을 라이브의 이벤트 모양(심볼별 단일 키, 비동기)으로
   흘려보내고 백테스트 결과와 체결을 대조한다. 드라이런이 존재하는 이유가 이 대조이고,
   이제 둘은 같은 엔진과 같은 ``SimulatedExecutor``를 쓰므로 **완전히** 같아야 한다.

단일 심볼로 대조하는 이유: 백테스트는 같은 시각에 마감한 심볼들을 하나의 이벤트로 병합하지만
라이브는 심볼별로 따로 처리한다. 심볼이 하나면 두 이벤트 스트림이 동일해지므로, 그 차이를
빼고 순수하게 체결 규칙만 대조할 수 있다.

    uv run python core/live_check.py            # 전부
    uv run python core/live_check.py --offline  # 캔들 캐시가 필요 없는 1, 2 만
"""
import asyncio
import sys
from concurrent.futures import Future
from datetime import datetime, timezone

from core.backtest_fast_check import LimitLadderStreamer, StopLadderStreamer, compare_reports
from core.binance_candle_fetcher.vision_fetcher import BinanceVisionFetcher
from core.engine.backtest import run_backtest
from core.engine.candle_producer import CandleProducer
from core.engine.engine import TradingEngine
from core.engine.executor import (
    SimulatedExecutor, resolve_fee_ratio, resolve_slippage_ratio)
from core.engine.recorder import BacktestRecorder
from core.engine.status import Status
from core.logging_config import setup_logging
from core.streamer.action import Action, ActionType
from core.streamer.candle import Candle
from core.streamer.keltner_stop_streamer import KeltnerStopStreamer
from core.streamer.keltner_streamer import KeltnerStreamer
from core.streamer.mean_reversion_zscore import MeanReversionZScoreStreamer
from core.trader import live_candle_producer as lcp
from core.trader.BinanceExecutor import OrderResult
from core.trader.live_executor import LiveExecutor, resolve_margin_asset

MIN = 60_000
SYM, SYM2 = "ETHUSDT", "BTCUSDT"
T0 = 1_700_000_000_000 - (1_700_000_000_000 % MIN)
INIT_MARGIN = 100_000.0

_failures = []


def check(label, cond, detail=""):
    if not cond:
        _failures.append(label)
    print(f"[{label}] {'OK' if cond else 'FAIL'}{(' — ' + detail) if detail else ''}")


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
          [t for t, _ in evs] == [T0 + MIN, T0 + 2 * MIN, T0 + 3 * MIN],
          f"{[t for t, _ in evs]}")
    check("producer: 이벤트는 단일 심볼", all(list(c) == [SYM] for _, c in evs))
    c = evs[0][1][SYM]
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
          [t for t, _ in evs] == [T0 + MIN, T0 + 2 * MIN, T0 + 3 * MIN, T0 + 4 * MIN]
          and fetched.get("range") == (T0 + MIN, T0 + 3 * MIN),
          f"{[t for t, _ in evs]} range={fetched.get('range')}")

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


# ============================================================ 2. 라이브 실행기


class FakeOrderClient:
    def __init__(self, success=True):
        self.calls = []
        self.success = success

    def execute_action(self, action, client_order_id=None, reference_price=None):
        self.calls.append((action, reference_price))
        f = Future()
        f.set_result(OrderResult(success=self.success, order_id="1",
                                 error=None if self.success else "rejected"))
        return f


def make_live_executor(margin=10_000.0):
    trades, errors, meta = [], [], []

    async def on_error(e):
        errors.append(e)

    ex = LiveExecutor(FakeOrderClient(), Status(margin=margin), [SYM, SYM2], "USDT",
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

    # ACCOUNT_UPDATE는 **변경된 항목만** 싣는다. 없는 항목을 0으로 덮으면 마진이 0이 되어
    # 모든 사이징이 붕괴하거나, 포지션이 0으로 보여 재진입해 실제 포지션이 2배가 된다.
    ex, _, _, _ = make_live_executor()
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
    ex, trades, _, _ = make_live_executor()
    ex.status.last_close[SYM] = 100.0
    ex.submit(Action(SYM, 2.0), event_time=5000)
    cid = ex._orders.calls[0][0].client_id
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

    ex, trades, _, _ = make_live_executor()
    ex.status.last_close[SYM] = 100.0
    ex.submit(Action(SYM, 1.0), event_time=1)
    cid = ex._orders.calls[0][0].client_id
    await ex.on_user_data(order_msg(status="CANCELED", x="CANCELED", z=0.0, c=cid))
    check("executor: 미체결 취소는 Trade 없이 pending 정리",
          not trades and not ex._pending_decision)

    ex, trades, _, _ = make_live_executor()
    ex.status.last_close[SYM] = 100.0
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
    ex, trades, _, _ = make_live_executor()
    await ex.on_user_data(order_msg(status="FILLED", z=1.0, ap=50.0, c="unknown-id"))
    check("executor: 짝지어지지 않은 체결도 기록", len(trades) == 1 and trades[0].price == 50.0)

    # BNB 수수료 할인을 켜면 n이 BNB 단위로 온다. 마진 자산 손익에 더하면 wnl - fee 가 오염된다.
    ex, trades, _, meta = make_live_executor()
    ex.status.last_close[SYM] = 100.0
    ex.submit(Action(SYM, 1.0), event_time=1)
    cid = ex._orders.calls[0][0].client_id
    await ex.on_user_data(order_msg(status="FILLED", z=1.0, n=0.5, N="BNB", c=cid))
    check("executor: 다른 자산 수수료 분리",
          trades[0].fee == 0.0 and meta == [("fee_asset_mismatch", "BNB")])

    ex, _, _, _ = make_live_executor()
    ex.status.last_close[SYM] = 100.0
    await ex.on_user_data(order_msg(status="NEW", x="NEW", z=0.0, otype="STOP_MARKET",
                                    sp=95.0, side="SELL", c="stop-1"))
    dispatch = ex.submit(Action.cancel(SYM, "stop-1"), event_time=1)
    check("executor: CANCEL은 장부를 비우고 거래소로도 나간다",
          ex.status.total_open_orders() == 0 and dispatch is not None
          and ex._orders.calls[-1][0].order_type is ActionType.CANCEL)

    ex, trades, _, _ = make_live_executor()
    ex.match_resting(SYM, Candle(1, 1, 1, 1, 1, 0, MIN), MIN)
    check("executor: 미체결 주문을 시뮬레이션하지 않는다", not trades)
    check("executor: 강제청산은 거래소 몫", ex.force_liquidation(-100.0) is False)

    ex, _, _, _ = make_live_executor()
    ex.status.last_close[SYM] = 100.0
    ex.submit(Action(SYM, 1.0), event_time=1)
    ex.submit(Action(SYM, -1.0, order_type=ActionType.STOP_MARKET,
                     trigger_price=90.0, client_id="stop-x"), event_time=1)
    ex.begin_event(2, {SYM: Candle(1, 1, 1, 1, 1, 0, MIN)})
    left = list(ex._pending_decision.values())
    # 미체결 주문은 몇 봉 뒤 체결이 정상이므로 캔들 경계에서 버리면 안 된다.
    check("executor: 캔들 경계에서 시장가 결정만 폐기",
          len(left) == 1 and left[0].resting is True)

    ex, _, errors, _ = make_live_executor()
    ex._orders.success = False
    ex.status.last_close[SYM] = 100.0
    # 실행기는 재시도 소진 후 예외 대신 success=False를 반환한다 — 확인하지 않으면 영구 거부된
    # 주문이 아무 흔적 없이 지나가고 전략이 거래소와 어긋난다.
    await ex.submit(Action(SYM, 1.0), event_time=1).wait(1.0)
    check("executor: 주문 실패는 on_error로", len(errors) == 1, f"{errors}")

    ex, _, _, _ = make_live_executor()
    ex.status.last_close[SYM] = 100.0
    ex.status.last_close[SYM2] = 200.0
    ex.submit(Action(SYM2, 1.0), event_time=1)
    check("executor: reference_price = 액션 대상 심볼의 종가",
          ex._orders.calls[-1][1] == 200.0)


# ============================================================ 3. 드라이런 == 백테스트


class ReplayProducer(CandleProducer):
    """라이브처럼 심볼별 단일 키 이벤트를 비동기로 내주는 소스 (캔들은 메모리에서)."""

    def __init__(self, candles_by_symbol, interval_ms: int):
        self.interval_ms = interval_ms
        self._events = sorted(
            ((c.end_time, sym, c) for sym, cs in candles_by_symbol.items() for c in cs),
            key=lambda t: (t[0], t[1]))

    async def __aiter__(self):
        for end_time, symbol, candle in self._events:
            yield end_time, {symbol: candle}


async def run_dry(streamer, candles_by_symbol, interval_ms, slippage_ratio=None):
    """``BinanceTrader``의 드라이런과 **같은 부품 구성**으로 돌린다."""
    status = Status(margin=INIT_MARGIN)
    recorder = BacktestRecorder(streamer, status)
    executor = SimulatedExecutor(
        status, resolve_fee_ratio(streamer),
        resolve_slippage_ratio(streamer, slippage_ratio),
        on_trade=recorder.record_trade, log_label="dry-run")
    engine = TradingEngine(streamer, executor, recorder)
    await engine.run_async(ReplayProducer(candles_by_symbol, interval_ms))
    return recorder.build_report(INIT_MARGIN)


async def check_dry_run_parity():
    interval = "1m"
    candles = BinanceVisionFetcher(compress=False).get_candles_with_cache(
        SYM, datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 2, 1, tzinfo=timezone.utc), interval)
    print(f"Loaded {len(candles)} candles for dry-run parity")
    by_symbol = {SYM: candles}
    kp = dict(window=20 * 60, m_entry=2.0, m_exit=0.0, max_loss=0.02, fee_ratio=0.0004)

    cases = [
        ("dry-run: Keltner (시장가)", lambda: KeltnerStreamer(symbols=[SYM], **kp), None),
        ("dry-run: MeanReversionZScore (상태 있는 전략)",
         lambda: MeanReversionZScoreStreamer(symbol=SYM, window=60, entry_z=2.0,
                                             timeout_candles=60, max_loss=0.02,
                                             fee_ratio=0.0004), None),
        ("dry-run: KeltnerStop (조건부 주문)",
         lambda: KeltnerStopStreamer(symbols=[SYM], window=72 * 60, m_entry=4.0,
                                     m_exit=3.0, max_loss=0.005, fee_ratio=0.0004), None),
        ("dry-run: LimitLadder (지정가 + 취소)", lambda: LimitLadderStreamer([SYM]), None),
        ("dry-run: StopLadder (reduce_only + 슬리피지)",
         lambda: StopLadderStreamer([SYM]), 0.0005),
    ]

    for label, make_streamer, slippage in cases:
        bt = run_backtest(make_streamer(), by_symbol, vectorized=False, progress=False,
                          init_margin=INIT_MARGIN,
                          **({"slippage_ratio": slippage} if slippage is not None else {}))
        dry = await run_dry(make_streamer(), by_symbol, MIN, slippage)
        check(label, compare_reports(label, bt, dry),
              f"trades={len(bt.trades)} final={bt.status.total_margin():.2f}")


async def main():
    # 판정 결과를 print로 읽는 게 본론이라 기본 레벨을 WARNING으로 둔다 (backtest_fast_check와 동일).
    setup_logging(default="WARNING")
    await check_candle_producer()
    await check_live_executor()
    if "--offline" not in sys.argv:
        await check_dry_run_parity()

    if _failures:
        print(f"LIVE CHECK: FAILED ({len(_failures)}건) — {_failures}")
        sys.exit(1)
    print("LIVE CHECK: ALL OK")


if __name__ == "__main__":
    asyncio.run(main())
