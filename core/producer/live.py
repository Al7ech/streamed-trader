"""라이브 캔들 공급자.

바이낸스 선물 kline 웹소켓에서 마감 캔들을 받아 :class:`~core.engine.engine.TradingEngine`이
소비할 이벤트로 내준다. 소켓 연결/수신과 웹소켓 봉투 파싱, 마감 시각 정규화만 한다.

**연속성 판정은 하지 않는다.** 중복 캔들 감지, 구멍 백필, 워밍업↔라이브 이어붙이기는 모두
엔진 몫이다 (:meth:`~core.engine.engine.TradingEngine` 참고) — 엔진이 심볼별 연속성 앵커를
들고, 스트림이 앞으로 건너뛰면 그 구간용 공급자를 즉석에서 만들어 지표만 채운다.

라이브는 심볼을 병합하지 않는다. 각 심볼의 캔들이 도착하는 즉시 **키 하나짜리 이벤트**로
내주므로, 엔진의 ``streamer.symbols`` 순회에서 나머지 심볼은 캔들이 없어 자연히 걸러진다.
"""

import logging
from typing import AsyncIterator, Awaitable, Callable, List, Optional

from binance.enums import ContractType

from core.candle.candle import Candle
from core.producer.base import CandleProducer, Event
from core.producer.reliable_websocket import ReliableWebsocket
from core.utils import interval_to_minutes


class LiveCandleProducer(CandleProducer):
    """바이낸스 kline 웹소켓에서 마감 캔들을 내주는 비동기 소스.

    :param socket_manager: 이미 만들어진 ``BinanceSocketManager``.
    :param on_error: 치명적이지 않은 오류(에러 프레임, 파싱 실패)를 흘려보낼 곳.
    """

    def __init__(self, socket_manager, symbols: List[str], interval: str,
                 on_error: Callable[[Exception], Awaitable[None]]):
        self.logger = logging.getLogger(__name__)
        self._socket_manager = socket_manager
        self.symbols = [s.upper() for s in symbols]
        self._symbol_set = set(self.symbols)
        self.interval = interval
        # 라이브의 간격 출처는 config다 — 첫 캔들 전에 경계 정규화에 이미 필요하다.
        super().__init__(interval_to_minutes(interval) * 60_000)
        self._on_error = on_error
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
                         f"({self.socket.id()})")
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
        """마감 캔들을 이벤트로 내준다. 스트림이 끝나면(정상/치명적) 반복이 끝난다."""
        while self._running:
            try:
                data = await self.socket.recv()
            except Exception as e:
                if not self._running:
                    break
                self._fatal(f"kline 메시지를 받지 못했다: {e}")
                break
            if not self._running:
                break

            try:
                async for event in self._events_from(data):
                    yield event
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
        candle = Candle(
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            start_time=start_time,
            end_time=start_time + self.interval_ms,
        )
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

    def _fatal(self, reason: str) -> None:
        self.fatal_reason = reason
        self._running = False
        self.logger.fatal(f"Stopping trader. {reason}")
