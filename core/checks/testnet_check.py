"""Testnet check: LiveExecutor + BinanceOrderClient를 **실제 거래소(바이낸스 USD-M 테스트넷)**에 대고 돌린다.

``live_check.py`` 2절은 주문 클라이언트와 AsyncClient를 둘 다 가짜로 세우고 유저 데이터
메시지를 손으로 만들어 넣는다. 그래서 다음은 거기서 확인되지 않는다 — 여기서 확인한다:

- 거래소가 우리가 보내는 주문 파라미터(타입/timeInForce/reduceOnly/newClientOrderId)를 받는가
- 실제 ``ORDER_TRADE_UPDATE``/``ACCOUNT_UPDATE`` 페이로드가 파서의 가정과 맞는가
- ``create()``의 수화(지갑/커미션/미체결 주문/심볼 규칙)가 실제 응답에서 맞게 나오는가
- 주문 실패가 재시도 소진 뒤 ``on_error``로 흘러가는가

**테스트넷에 실제 주문을 낸다.** 그래서 ``TESTNET_API_KEY``/``TESTNET_API_SECRET``만 읽고
(``.env``의 ``API_KEY``는 절대 쓰지 않는다), 클라이언트가 만드는 URI가 테스트넷인지 확인한 뒤에야
주문을 낸다. 시작과 끝에 ``SYM``의 미체결 주문을 전부 취소하고 포지션을 청산한다.

    uv run python core/checks/testnet_check.py

키는 https://testnet.binancefuture.com 에서 발급한다. 계좌는 one-way 모드여야 한다.
"""
import asyncio
import math
import os
import sys
import time
from typing import Callable, Dict, List, Optional

from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv

from core.account.trade import Trade
from core.candle.candle import Candle
from core.executor.live import LiveExecutor, order_from_exchange
from core.fetcher.binance.exchange_info import diff_rules, parse_exchange_info
from core.logging_config import setup_logging
from core.order.action import Action, ActionType
from core.order.symbol_rules import DEFAULT_RULES
from core.producer.reliable_websocket import ReliableWebsocket

SYM = "ETHUSDT"          # 하드코딩 규칙이 테스트넷과 일치하는 심볼 (2026-10 확인)
DRIFT_SYM = "BTCUSDT"    # 테스트넷 stepSize가 하드코딩과 다른 심볼 — create() 거부 확인용
NOTIONAL = 30.0          # 진입 명목가치 (USDT). 최소 명목가치 20을 넉넉히 넘긴다.
WAIT_S = 15.0
MIN = 60_000

_failures = []


def check(label, cond, detail=""):
    if not cond:
        _failures.append(label)
    print(f"[{label}] {'OK' if cond else 'FAIL'}{(' — ' + detail) if detail else ''}")


async def until(pred: Callable[[], bool], timeout: float = WAIT_S) -> bool:
    """비동기로 도착하는 체결/계좌 갱신을 기다린다."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.1)
    return pred()


# ============================================================ 거래소 헬퍼 (실행기를 거치지 않는 직접 조회)


def _is_testnet(client) -> bool:
    # FUTURES_URL 속성은 testnet=True여도 메인넷 값 그대로다 — 실제로 만들어지는 URI를 본다.
    return bool(client.testnet) and client._create_futures_api_uri("order").startswith(
        client.FUTURES_TESTNET_URL)


async def mark_price(client: AsyncClient) -> float:
    return float((await client.futures_mark_price(symbol=SYM))["markPrice"])


async def exchange_position(client: AsyncClient) -> float:
    rows = await client.futures_position_information(symbol=SYM)
    return sum(float(r.get("positionAmt", 0.0)) for r in rows)


async def open_orders(client: AsyncClient) -> Dict[str, List[Dict]]:
    """거래소의 미체결 주문 — 일반 장부와 algo(조건부) 장부를 **둘 다** 본다.

    python-binance 1.0.37은 조건부 주문(STOP_MARKET/TAKE_PROFIT_MARKET)을 2025-12-09 이후
    algo 엔드포인트로 보낸다. 그쪽은 ``futures_get_open_orders()``에 보이지 않는다.
    """
    regular = await client.futures_get_open_orders(symbol=SYM)
    try:
        algo = await client.futures_get_open_orders(symbol=SYM, conditional=True)
        if isinstance(algo, dict):  # 응답 래핑 형태가 바뀌어도 리스트로
            algo = algo.get("orders") or algo.get("rows") or []
    except Exception as e:
        print(f"    (algo 미체결 조회 실패: {e})")
        algo = []
    return {"regular": list(regular), "algo": list(algo)}


def find_order(books: Dict[str, List[Dict]], client_id: str) -> Optional[tuple]:
    """client id로 주문을 찾아 (장부 이름, 페이로드)를 돌려준다."""
    for name, rows in books.items():
        for o in rows:
            if client_id in (o.get("clientOrderId"), o.get("clientAlgoId")):
                return name, o
    return None


def describe(books: Dict[str, List[Dict]]) -> str:
    return "; ".join(
        f"{name}: " + ", ".join(
            f"{o.get('type') or o.get('orderType')}/{o.get('side')}"
            f" id={o.get('clientOrderId') or o.get('clientAlgoId')}"
            for o in rows)
        for name, rows in books.items() if rows) or "없음"


async def cleanup(client: AsyncClient) -> None:
    """SYM의 미체결 주문(일반 + algo)을 전부 취소하고 포지션을 청산한다. 멱등이다."""
    for kw in ({}, {"conditional": True}):
        try:
            await client.futures_cancel_all_open_orders(symbol=SYM, **kw)
        except Exception as e:
            print(f"    (cleanup: 취소 실패 {kw}: {e})")
    pos = await exchange_position(client)
    if pos != 0:
        await client.futures_create_order(symbol=SYM, side="SELL" if pos > 0 else "BUY",
                                          type="MARKET", quantity=abs(pos), reduceOnly=True)
        print(f"    (cleanup: 남은 포지션 {pos} 청산)")


# ============================================================ 유저 데이터 펌프 (BinanceTrader._listen_user_websocket과 같은 모양)


class UserStream:
    def __init__(self, client: AsyncClient, executor: LiveExecutor):
        self.socket = ReliableWebsocket(BinanceSocketManager(client).futures_user_socket())
        self.executor = executor
        self.events: List[Dict] = []
        self.error_frames: List[Dict] = []
        self.handler_errors: List[Exception] = []
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        await self.socket.connect()
        self._task = asyncio.create_task(self._pump())

    async def _pump(self):
        while True:
            data = await self.socket.recv()
            if isinstance(data, dict) and data.get("e") == "error":
                self.error_frames.append(data)
                continue
            self.events.append(data)
            try:
                await self.executor.on_user_data(data)
            except Exception as e:
                self.handler_errors.append(e)

    def event_types(self) -> List[str]:
        return [e.get("e") for e in self.events if isinstance(e, dict)]

    async def close(self):
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        await self.socket.close()


# ============================================================ 본문


class Run:
    """실행기 + 유저 스트림 + 싱크들. 각 절이 공유한다."""

    def __init__(self, client: AsyncClient, executor: LiveExecutor, stream: UserStream,
                 trades: List[Trade], errors: List[Exception], meta: List[tuple]):
        self.client, self.ex, self.stream = client, executor, stream
        self.trades, self.errors, self.meta = trades, errors, meta
        self.rules = DEFAULT_RULES[SYM]

    async def new_event(self) -> tuple:
        """현재 마크 가격으로 합성 캔들 하나를 begin_event에 넣는다 (시장가 최소 명목가치 기준가).
        반환: (event_time, mark)."""
        mark = await mark_price(self.client)
        now = int(time.time() * 1000)
        t = now - now % MIN
        self.ex.begin_event(t, {SYM: Candle(mark, mark, mark, mark, 0.0, t - MIN, t)})
        return t, mark

    def position(self) -> float:
        return self.ex.status.position_for(SYM).position


async def section_hydration(client: AsyncClient, key: str, secret: str) -> None:
    print("\n=== 1. create() 수화 ===")
    account = await client.futures_account()
    wallet = next((float(a["walletBalance"]) for a in account["assets"]
                   if a["asset"] == "USDT"), None)
    taker = float((await client.futures_commission_rate(symbol=SYM))["takerCommissionRate"])

    ex = await LiveExecutor.create([SYM], client=client, api_key=key, api_secret=secret,
                                   testnet=True)
    try:
        check("hydration: 주문 클라이언트도 테스트넷", _is_testnet(ex._orders.client))
        check("hydration: margin = USDT walletBalance",
              wallet is not None and math.isclose(ex.status.margin, wallet, abs_tol=1e-6),
              f"status={ex.status.margin} wallet={wallet}")
        check("hydration: fee_ratio = taker 커미션", ex.status.fee_ratio == taker,
              f"{ex.status.fee_ratio} vs {taker}")
        check("hydration: 정리 후 포지션 0, 장부 비어 있음",
              ex.status.position_for(SYM).position == 0 and ex.status.total_open_orders() == 0,
              f"{ex.status}")
    finally:
        await ex.close()

    # 심볼 규칙 검증: 거래소 값과 하드코딩이 다르면 create()가 거부해야 한다. 기대값은 지금
    # 거래소에서 계산하므로 테스트넷이 규칙을 바꿔도 이 검사가 낡지 않는다.
    drift = diff_rules(DEFAULT_RULES, parse_exchange_info(await client.futures_exchange_info()),
                       [DRIFT_SYM])
    try:
        bad = await LiveExecutor.create([DRIFT_SYM], client=client, api_key=key,
                                        api_secret=secret, testnet=True)
        await bad.close()
        check(f"hydration: {DRIFT_SYM} 규칙 판정이 거래소 diff와 일치", not drift, f"diff={drift}")
    except RuntimeError as e:
        check(f"hydration: {DRIFT_SYM} 규칙 판정이 거래소 diff와 일치", bool(drift), str(e))


async def section_market_entry(r: Run) -> float:
    print("\n=== 2. 시장가 진입 ===")
    t, mark = await r.new_event()
    q = r.rules.floor_qty(NOTIONAL / mark)
    n0 = len(r.trades)
    r.ex.submit(Action(SYM, q), event_time=t)
    await r.ex.drain_pending_orders()

    got = await until(lambda: len(r.trades) > n0)
    check("market: Trade 도착", got, f"user events={r.stream.event_types()}")
    if got:
        tr = r.trades[n0]
        check("market: Trade 내용",
              tr.symbol == SYM and math.isclose(tr.quantity, q) and tr.price > 0
              and tr.pre_position == 0 and tr.order_type == ActionType.MARKET.value
              and tr.timestamp == t and tr.submitted_at == t, f"{tr}")
    check("market: ACCOUNT_UPDATE로 포지션 반영",
          await until(lambda: math.isclose(r.position(), q)), f"position={r.position()}")
    check("market: 거래소 포지션과 일치",
          math.isclose(await exchange_position(r.client), r.position()))
    check("market: 오류 없음", not r.errors, f"{r.errors}")
    return q


async def section_marketable_limit(r: Run, q: float) -> None:
    print("\n=== 3. 즉시 체결되는 지정가 (추가 진입) ===")
    t, mark = await r.new_event()
    n0 = len(r.trades)
    r.ex.submit(Action(SYM, q, order_type=ActionType.LIMIT,
                       price=r.rules.round_price(mark * 1.01), client_id="tn-limit-fill"),
                event_time=t)
    await r.ex.drain_pending_orders()

    got = await until(lambda: len(r.trades) > n0)
    check("limit fill: Trade 도착", got, f"errors={r.errors}")
    if got:
        tr = r.trades[n0]
        # 미체결 주문의 timestamp는 실제 체결 시각(T), 결정 시각은 submitted_at이다.
        check("limit fill: order_type=LIMIT, timestamp=체결 시각, submitted_at=결정 시각",
              tr.order_type == ActionType.LIMIT.value and tr.timestamp != t
              and tr.timestamp > t and tr.submitted_at == t and math.isclose(tr.quantity, q),
              f"{tr}")
    check("limit fill: 포지션 2q", await until(lambda: math.isclose(r.position(), 2 * q)),
          f"position={r.position()}")
    check("limit fill: 장부에 남지 않음", r.ex.status.total_open_orders() == 0,
          f"{r.ex.status.open_orders}")


async def section_resting(r: Run, q: float, key: str, secret: str) -> None:
    print("\n=== 4-5. 조건부 주문 (손절 STOP_MARKET / 익절 TAKE_PROFIT_MARKET) ===")
    t, mark = await r.new_event()
    stop = Action(SYM, -2 * q, order_type=ActionType.STOP_MARKET,
                  trigger_price=r.rules.round_price(mark * 0.8), trigger_above=False,
                  reduce_only=True, client_id="tn-stop")
    tp = Action(SYM, -2 * q, order_type=ActionType.STOP_MARKET,
                trigger_price=r.rules.round_price(mark * 1.2), trigger_above=True,
                reduce_only=True, client_id="tn-tp")
    e0 = len(r.errors)
    r.ex.submit(stop, event_time=t)
    r.ex.submit(tp, event_time=t)
    await r.ex.drain_pending_orders()
    check("conditional: 주문 접수 (on_error 없음)", len(r.errors) == e0,
          f"{r.errors[e0:]}")

    await asyncio.sleep(2.0)  # 유저 스트림 NEW 이벤트가 올 시간
    books = await open_orders(r.client)
    print(f"    거래소 미체결: {describe(books)}")
    print(f"    유저 스트림 이벤트: {r.stream.event_types()[-6:]}")

    for action, exchange_type in ((stop, "STOP_MARKET"), (tp, "TAKE_PROFIT_MARKET")):
        cid = action.client_id
        found = find_order(books, cid)
        # 전략의 client_id가 거래소에 남아야 나중에 그 id로 취소하고 체결을 결정과 짝지을 수 있다.
        check(f"conditional[{cid}]: 거래소에 전략의 client_id로 걸려 있다", found is not None,
              f"거래소 미체결: {describe(books)}")
        if found:
            where, payload = found
            otype = payload.get("type") or payload.get("orderType")
            check(f"conditional[{cid}]: 거래소 타입 {exchange_type}", otype == exchange_type,
                  f"{where}: {payload}")
            parsed = order_from_exchange(payload)
            check(f"conditional[{cid}]: order_from_exchange 왕복",
                  parsed is not None and parsed.order_type is ActionType.STOP_MARKET
                  and math.isclose(parsed.quantity, action.quantity)
                  and parsed.trigger_price == action.trigger_price
                  and parsed.trigger_above == action.trigger_above and parsed.reduce_only,
                  f"{where}: {parsed}")
        local = [o for o in r.ex.status.open_orders_for(SYM) if o.client_id == cid]
        check(f"conditional[{cid}]: 유저 스트림 NEW로 로컬 장부 등록",
              len(local) == 1 and local[0].trigger_price == action.trigger_price
              and local[0].trigger_above == action.trigger_above,
              f"local={r.ex.status.open_orders_for(SYM)}")

    # 재기동 시 수화: 거래소에 걸려 있는 손절을 새 실행기가 읽어야 중복 손절을 안 건다.
    fresh = await LiveExecutor.create([SYM], client=r.client, api_key=key, api_secret=secret,
                                      testnet=True)
    try:
        ids = {o.client_id for o in fresh.status.open_orders_for(SYM)}
        check("conditional: 재기동 수화가 조건부 주문을 읽는다", {"tn-stop", "tn-tp"} <= ids,
              f"hydrated ids={ids}")
    finally:
        await fresh.close()


async def section_limit_cancel(r: Run, q: float) -> None:
    print("\n=== 6. 걸린 지정가 + id로 취소 ===")
    t, mark = await r.new_event()
    # PERCENT_PRICE(0.95)를 넘지 않는 선에서 멀리 둔다.
    r.ex.submit(Action(SYM, q, order_type=ActionType.LIMIT,
                       price=r.rules.round_price(mark * 0.96), client_id="tn-limit"),
                event_time=t)
    await r.ex.drain_pending_orders()
    registered = await until(lambda: any(o.client_id == "tn-limit"
                                         for o in r.ex.status.open_orders_for(SYM)))
    check("limit: NEW로 로컬 장부 등록", registered, f"{r.ex.status.open_orders_for(SYM)}")
    books = await open_orders(r.client)
    found = find_order(books, "tn-limit")
    check("limit: 거래소 일반 장부에 걸려 있다",
          found is not None and found[0] == "regular"
          and (found[1].get("timeInForce") == "GTC"), f"{found}")

    n0 = len(r.trades)
    r.ex.submit(Action.cancel(SYM, "tn-limit"), event_time=t)
    check("limit cancel: 로컬 장부는 즉시 비워진다",
          not any(o.client_id == "tn-limit" for o in r.ex.status.open_orders_for(SYM)))
    await r.ex.drain_pending_orders()
    await asyncio.sleep(1.5)
    check("limit cancel: 거래소에서도 사라졌다",
          find_order(await open_orders(r.client), "tn-limit") is None)
    check("limit cancel: Trade 없음, pending 정리",
          len(r.trades) == n0 and (SYM, "tn-limit") not in r.ex._pending_decision,
          f"pending={list(r.ex._pending_decision)}")


async def section_cancel_conditional(r: Run) -> None:
    print("\n=== 7. 조건부 주문 취소 (id 하나 → 전부) ===")
    t, _ = await r.new_event()
    e0 = len(r.errors)
    r.ex.submit(Action.cancel(SYM, "tn-stop"), event_time=t)
    await r.ex.drain_pending_orders()
    await asyncio.sleep(1.5)
    books = await open_orders(r.client)
    check("cancel id: 손절이 거래소에서 사라졌다 (on_error 없음)",
          find_order(books, "tn-stop") is None and len(r.errors) == e0,
          f"거래소: {describe(books)} errors={r.errors[e0:]}")

    e0 = len(r.errors)
    r.ex.submit(Action.cancel(SYM), event_time=t)
    await r.ex.drain_pending_orders()
    await asyncio.sleep(1.5)
    books = await open_orders(r.client)
    check("cancel all: 거래소 장부(일반+algo)가 비었다",
          not books["regular"] and not books["algo"] and len(r.errors) == e0,
          f"거래소: {describe(books)} errors={r.errors[e0:]}")
    check("cancel all: 로컬 장부가 비었다", r.ex.status.total_open_orders() == 0,
          f"{r.ex.status.open_orders}")


async def section_market_close(r: Run, q: float) -> None:
    print("\n=== 8. 시장가 청산 (순수 청산 → reduceOnly) ===")
    # 앞 절이 실패했어도 실제 포지션을 기준으로 닫는다.
    pos = r.position()
    t, _ = await r.new_event()
    n0 = len(r.trades)
    close = Action(SYM, -pos)
    r.ex.submit(close, event_time=t)
    await r.ex.drain_pending_orders()
    got = await until(lambda: len(r.trades) > n0)
    check("close: Trade 도착", got, f"errors={r.errors}")
    if got:
        tr = r.trades[n0]
        check("close: pre_position = 청산 전 포지션, wnl 유한",
              math.isclose(tr.pre_position, pos) and math.isclose(tr.quantity, -pos)
              and math.isfinite(tr.wnl), f"{tr}")
        check("close: 수수료가 마진 자산으로 잡혔다 (또는 불일치 기록)",
              tr.fee > 0 or ("fee_asset_mismatch", "BNB") in r.meta, f"fee={tr.fee} meta={r.meta}")
    check("close: 전략의 Action은 그대로 (reduce_only 사본만 나감)", not close.reduce_only)
    check("close: ACCOUNT_UPDATE로 flat", await until(lambda: r.position() == 0),
          f"position={r.position()}")
    check("close: 거래소도 flat", await exchange_position(r.client) == 0)


async def section_triggered_stop(r: Run, q: float) -> None:
    print("\n=== 8b. 발동되는 조건부 주문 (algo → 시장가 체결이 결정과 짝지어지는가) ===")
    # 진입 후 마크 가격 ±0.02%에 손절/익절을 둘 다 건다 — 테스트넷 가격은 몇 초~몇 분이면
    # 그만큼 움직인다. 먼저 발동한 쪽의 체결을 검사하고 남은 쪽은 취소한다.
    t, _ = await r.new_event()
    r.ex.submit(Action(SYM, q), event_time=t)
    await r.ex.drain_pending_orders()
    if not await until(lambda: math.isclose(r.position(), q)):
        check("trigger: 진입", False, f"position={r.position()} errors={r.errors}")
        return

    t, mark = await r.new_event()
    n0, e0 = len(r.trades), len(r.errors)
    for cid, mult, above in (("tn-near-sl", 0.9998, False), ("tn-near-tp", 1.0002, True)):
        r.ex.submit(Action(SYM, -q, order_type=ActionType.STOP_MARKET,
                           trigger_price=r.rules.round_price(mark * mult), trigger_above=above,
                           reduce_only=True, client_id=cid), event_time=t)
    await r.ex.drain_pending_orders()
    check("trigger: 두 주문 접수", len(r.errors) == e0, f"{r.errors[e0:]}")

    fired = await until(lambda: len(r.trades) > n0, timeout=300)
    if not fired:
        print("    [trigger] SKIP — 300초 안에 가격이 ±0.02% 움직이지 않았다")
    else:
        tr = r.trades[n0]
        check("trigger: 체결이 결정과 짝지어졌다 (order_type/pre_position/submitted_at)",
              tr.order_type == ActionType.STOP_MARKET.value and math.isclose(tr.quantity, -q)
              and math.isclose(tr.pre_position, q) and tr.submitted_at == t
              and tr.timestamp > t, f"{tr}")
        check("trigger: flat", await until(lambda: r.position() == 0),
              f"position={r.position()}")
        await asyncio.sleep(1.5)  # TRIGGERED/FINISHED ALGO_UPDATE가 올 시간
        left = {o.client_id for o in r.ex.status.open_orders_for(SYM)}
        check("trigger: 발동된 주문은 장부에서 빠지고 반대쪽만 남는다", len(left) == 1,
              f"local={left}")

    r.ex.submit(Action.cancel(SYM), event_time=t)
    await r.ex.drain_pending_orders()
    await asyncio.sleep(1.5)
    books = await open_orders(r.client)
    check("trigger: 남은 주문 정리 (거래소 + 로컬)",
          not books["regular"] and not books["algo"] and r.ex.status.total_open_orders() == 0,
          f"거래소: {describe(books)} local={r.ex.status.open_orders}")
    if not fired:  # 발동이 없었으면 진입 포지션을 닫는다
        t, _ = await r.new_event()
        r.ex.submit(Action(SYM, -r.position()), event_time=t)
        await r.ex.drain_pending_orders()
        await until(lambda: r.position() == 0)


async def section_rejection(r: Run, q: float) -> None:
    print("\n=== 9. 거래소 거부 → on_error ===")
    # flat에서 reduceOnly 시장가: 로컬 규칙 검사는 통과(reduce_only는 명목가치 면제)하지만
    # 거래소가 거부한다(-2022). 재시도 소진 뒤 success=False가 on_error까지 와야 한다.
    t, _ = await r.new_event()
    n0, e0 = len(r.trades), len(r.errors)
    r.ex.submit(Action(SYM, -q, reduce_only=True), event_time=t)
    await r.ex.drain_pending_orders()
    new_errors = r.errors[e0:]
    check("reject: on_error로 한 번 흘러간다",
          len(new_errors) == 1 and "order failed" in str(new_errors[0]), f"{new_errors}")
    await asyncio.sleep(1.0)
    check("reject: Trade 없음", len(r.trades) == n0)


async def main():
    load_dotenv()
    setup_logging(default="WARNING")
    key, secret = os.getenv("TESTNET_API_KEY"), os.getenv("TESTNET_API_SECRET")
    if not key or not secret or key == "KEY":
        print("TESTNET_API_KEY / TESTNET_API_SECRET가 필요하다 (https://testnet.binancefuture.com).")
        sys.exit(2)

    client = await AsyncClient.create(api_key=key, api_secret=secret, testnet=True)
    if not _is_testnet(client):
        await client.close_connection()
        print("클라이언트가 테스트넷 URI를 만들지 않는다 — 중단한다.")
        sys.exit(2)

    ex, stream = None, None
    try:
        mode = await client.futures_get_position_mode()
        if mode.get("dualSidePosition"):
            print("계좌가 hedge 모드다 — 주문 클라이언트는 positionSide를 보내지 않으므로 "
                  "one-way 모드로 바꾼 뒤 다시 돌려라.")
            sys.exit(2)

        await cleanup(client)
        await section_hydration(client, key, secret)

        trades: List[Trade] = []
        errors: List[Exception] = []
        meta: List[tuple] = []

        async def on_error(e):
            errors.append(e)

        ex = await LiveExecutor.create([SYM], client=client, api_key=key, api_secret=secret,
                                       testnet=True, on_trade=trades.append, on_error=on_error,
                                       on_metadata=lambda k, v: meta.append((k, v)))
        stream = UserStream(client, ex)
        await stream.start()
        r = Run(client, ex, stream, trades, errors, meta)

        q = await section_market_entry(r)
        await section_marketable_limit(r, q)
        await section_resting(r, q, key, secret)
        await section_limit_cancel(r, q)
        await section_cancel_conditional(r)
        await section_market_close(r, q)
        await section_triggered_stop(r, q)
        await section_rejection(r, q)

        check("stream: 에러 프레임/핸들러 예외 없음",
              not stream.error_frames and not stream.handler_errors,
              f"frames={stream.error_frames} handler={stream.handler_errors}")
    finally:
        if stream:
            await stream.close()
        if ex:
            await ex.close()
        try:
            await cleanup(client)
            books = await open_orders(client)
            pos = await exchange_position(client)
            print(f"\n정리 후: position={pos}, 미체결={describe(books)}")
        finally:
            await client.close_connection()

    print()
    if _failures:
        print(f"FAILED ({len(_failures)}): {_failures}")
        sys.exit(1)
    print("ALL OK")


if __name__ == "__main__":
    asyncio.run(main())
