"""라이브 캔들 공급자.

바이낸스 선물 kline 웹소켓에서 마감 캔들을 받아 :class:`~core.engine.engine.TradingEngine`이
소비할 이벤트로 내준다. 소켓 연결/수신과 웹소켓 봉투 파싱, 마감 시각 정규화만 한다.

**연속성 판정은 하지 않는다.** 중복 캔들 감지, 구멍 백필, 워밍업↔라이브 이어붙이기는 모두
엔진 몫이다 (:meth:`~core.engine.engine.TradingEngine` 참고) — 엔진이 심볼별 연속성 앵커를
들고, 스트림이 앞으로 건너뛰면 그 구간을 ``history`` 조회로 받아 결정 없이 재생한다.

**멈춘 스트림은 여기서 잡는다.** 엔진의 구멍 판정은 다음 캔들이 *도착해야* 일어나므로, 캔들이
아예 안 오면 아무도 모른다 — 연결은 살아 있는데 거래소가 푸시를 멈추거나, 멀티플렉스 중 한
심볼만 끊기면 python-binance는 그걸 끊김으로 보지 않는다(수신 타임아웃에서 그냥 다시 기다린다).
그래서 심볼별로 마지막 마감 캔들 이후 ``stall_timeout_s``가 지나면 치명적으로 끊는다.
재기동이 워밍업으로 지표를 다시 세운다.

라이브는 심볼을 병합하지 않는다. 각 심볼의 캔들이 도착하는 즉시 **키 하나짜리 이벤트**로
내주므로, 엔진의 ``streamer.symbols`` 순회에서 나머지 심볼은 캔들이 없어 자연히 걸러진다.
"""

import asyncio
import logging
import time
from typing import AsyncIterator, Awaitable, Callable, Dict, List, Optional

from binance.enums import ContractType

from core.candle.candle import Candle
from core.producer.base import CandleProducer, Event
from core.producer.reliable_websocket import ReliableWebsocket
from core.utils import interval_to_minutes

#: 멈춘 스트림 판정의 기본 여유(초). 한 인터벌에 이만큼 더 기다려도 마감 캔들이 없으면 멈춘
#: 것으로 본다. python-binance의 내부 재연결(최대 5회, 백오프)이 끝날 시간을 준다.
DEFAULT_STALL_GRACE_S = 60.0


class LiveCandleProducer(CandleProducer):
    """바이낸스 kline 웹소켓에서 마감 캔들을 내주는 비동기 소스.

    :param socket_manager: 이미 만들어진 ``BinanceSocketManager``.
    :param on_error: 치명적이지 않은 오류(에러 프레임, 파싱 실패)를 흘려보낼 곳.
    :param stall_timeout_s: 어떤 심볼이든 마지막 마감 캔들(첫 캔들이면 수신 시작) 뒤로 이만큼
        마감 캔들이 없으면 스트림을 치명적으로 끊는다. None이면 한 인터벌 +
        :data:`DEFAULT_STALL_GRACE_S`.
    """

    def __init__(self, socket_manager, symbols: List[str], interval: str,
                 on_error: Callable[[Exception], Awaitable[None]],
                 stall_timeout_s: Optional[float] = None):
        self.logger = logging.getLogger(__name__)
        self._socket_manager = socket_manager
        self.symbols = [s.upper() for s in symbols]
        self._symbol_set = set(self.symbols)
        self.interval = interval
        # 라이브의 간격 출처는 config다 — 첫 캔들 전에 경계 정규화에 이미 필요하다.
        super().__init__(interval_to_minutes(interval) * 60_000)
        self._on_error = on_error
        self.stall_timeout_s = (stall_timeout_s if stall_timeout_s is not None
                                else self.interval_ms / 1000 + DEFAULT_STALL_GRACE_S)
        #: 심볼별 마지막 마감 캔들 수신 시각 (``time.monotonic``). 벽시계가 아니라서 로컬
        #: 시계 보정·밀림에 흔들리지 않는다. :meth:`__aiter__`가 수신을 시작할 때 채운다.
        self._last_close_at: Dict[str, float] = {}
        self.socket: Optional[ReliableWebsocket] = None
        self._running = False

    # ------------------------------------------------------------- 수명주기

    async def connect(self) -> None:
        """멀티플렉스 kline 소켓을 연다.

        ``kline_futures_socket`` 자체가 내부적으로 continuousKline 스트림
        (``<symbol>_<contract_type>@continuousKline_<interval>``)을 구독한다 — 여러 심볼을
        같은 이름 규칙으로 만들어 ``futures_multiplex_socket``으로 묶는다.
        """
        streams = [f"{sym.lower()}_{ContractType.PERPETUAL.value}@continuousKline_{self.interval}"
                   for sym in self.symbols]
        self.socket = ReliableWebsocket(
            self._socket_manager.futures_multiplex_socket(streams=streams))
        # noinspection PyProtectedMember
        self.logger.info(f"connecting to kline: {self.socket._url}{self.socket._path} "
                         f"({self.socket.id()}, stall_timeout={self.stall_timeout_s:g}s)")
        await self.socket.connect()
        self._running = True

    def request_stop(self) -> None:
        """다음 수신 후 스트림을 정상 종료한다."""
        self._running = False

    async def close(self) -> None:
        self._running = False
        if self.socket:
            await self.socket.close()
            self.socket = None

    # ------------------------------------------------------------- 캔들 공급

    async def __aiter__(self) -> AsyncIterator[Event]:
        """마감 캔들을 이벤트로 내준다. 스트림이 끝나면(정상/치명적/멈춤) 반복이 끝난다."""
        # 감시는 수신을 시작하는 순간부터다 — 첫 마감 캔들도 기한 안에 와야 한다.
        started = time.monotonic()
        self._last_close_at = {symbol: started for symbol in self.symbols}
        while self._running:
            # 가장 오래 조용한 심볼의 기한까지만 기다린다. 다른 심볼의 메시지가 계속 와서
            # recv가 매번 돌아와도, 매 바퀴 여기서 기한을 다시 보므로 한 심볼의 멈춤도 잡힌다.
            wait = min(self._last_close_at.values()) + self.stall_timeout_s - time.monotonic()
            if wait <= 0:
                self._fatal(self._stall_reason())
                break
            try:
                data = await asyncio.wait_for(self.socket.recv(), timeout=wait)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                if not self._running:
                    break
                self._fatal(f"kline 메시지를 받지 못했다: {e}")
                break
            if not self._running:
                break

            try:
                async for event in self._events_from(data):
                    yielded_at = time.monotonic()
                    yield event
                    # 소비자(엔진)가 이 이벤트를 처리하는 동안은 소켓을 읽지 않는다. 백필 REST로
                    # 수십 초가 걸려도 스트림이 멈춘 게 아니므로, 그 시간만큼 모든 기한을 민다.
                    paused = time.monotonic() - yielded_at
                    for symbol in self._last_close_at:
                        self._last_close_at[symbol] += paused
                    if not self._running:
                        return
            except Exception as e:
                # 파싱 중의 예상 못 한 오류. 한 메시지를 버리고 계속한다.
                self.logger.exception("Error processing kline message: %s", e)
                await self._on_error(e)

    async def _events_from(self, data: dict) -> AsyncIterator[Event]:
        """메시지 하나에서 나올 이벤트 (마감 캔들이면 하나, 아니면 없음)."""
        if await self._handle_stream_error_frame(data):
            return

        payload = data.get("data") or {}
        symbol = (payload.get("ps") or "").upper()
        if symbol not in self._symbol_set:
            self.logger.warning(f"알 수 없는 심볼의 kline 메시지를 버린다: {payload.get('ps')!r}")
            return

        kline = payload.get("k", {})
        if not kline.get("x", False):  # x = is_closed
            return

        start_time = int(kline["t"])
        # 마감 시각은 **인터벌 경계**로 정규화한다. 웹소켓의 T(closeTime)는 경계 - 1ms 인데
        # BinanceCandleFetcher는 경계(k[6] + 1)를 쓴다. 그대로 두면 백필한 캔들과 라이브 캔들의
        # 타임스탬프가 1ms 어긋나고, 백테스트 시계열과도 짝이 맞지 않는다.
        # taker_buy_volume(V)·trade_count(n)도 채운다 — REST/Vision 캔들은 채우므로, 빠뜨리면
        # 워밍업·백필 봉에는 값이 있고 실시간 봉에만 None이라 그 필드를 읽는 지표가
        # (TakerImbalanceIndicator) 실시간 봉이 창에 들어오는 순간부터 영원히 None이 된다.
        candle = Candle(
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            start_time=start_time,
            end_time=start_time + self.interval_ms,
            taker_buy_volume=float(kline["V"]) if "V" in kline else None,
            trade_count=int(kline["n"]) if "n" in kline else None,
        )
        self._last_close_at[symbol] = time.monotonic()
        yield Event(candle.end_time, {symbol: candle})

    # ------------------------------------------------------------- 내부

    async def _handle_stream_error_frame(self, data: dict) -> bool:
        """웹소켓 에러 프레임이면 에러 경로로 넘기고 True.

        python-binance는 끊김/재연결 실패를 예외가 아니라 ``{"e": "error", ...}`` **메시지**로
        큐에 올린다. 멀티플렉스 봉투에 싸이지 않고 그대로 얹히므로 봉투를 풀기 **전에**
        검사해야 한다.
        """
        if not isinstance(data, dict) or data.get("e") != "error":
            return False
        message = data.get("m") or data.get("type") or str(data)
        await self._on_error(RuntimeError(f"kline stream error: {message}"))
        return True

    def _stall_reason(self) -> str:
        now = time.monotonic()
        stalled = sorted(s for s, t in self._last_close_at.items()
                         if now - t >= self.stall_timeout_s)
        return (f"마감 캔들이 {self.stall_timeout_s:g}초 넘게 오지 않는다 (symbols={stalled}) "
                f"— 스트림이 멈췄다고 보고 재기동으로 복구한다")

    def _fatal(self, reason: str) -> None:
        self.fatal_reason = reason
        self._running = False
        self.logger.fatal(f"Stopping trader. {reason}")
