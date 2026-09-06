"""라이브 캔들 공급자.

바이낸스 선물 kline 웹소켓에서 마감 캔들을 받아 :class:`~core.engine.engine.TradingEngine`이
소비할 이벤트로 내준다. 캔들 소스에 관한 모든 것 — 소켓 연결/수신, 심볼별 연속성 판정,
구멍 백필, 지표 워밍업용 과거 캔들 — 이 여기 모여 있다.

라이브는 심볼을 병합하지 않는다. 각 심볼의 캔들이 도착하는 즉시 **키 하나짜리 이벤트**로
내주므로, 엔진의 ``streamer.symbols`` 순회에서 나머지 심볼은 캔들이 없어 자연히 걸러진다.

**백필이 스트림 순서로 표현된다**: 구멍을 발견하면 빠진 캔들을 먼저 yield하고 그다음에 방금
받은 캔들을 yield한다. 예전에는 처리 경로를 재귀적으로 재진입해야 했던 것("라이브 캔들과
백필 캔들이 같은 경로를 타야 한다")이, 소스가 순서를 책임지는 것으로 바뀌었다.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple

from binance.enums import ContractType

from core.domain.candle import Candle
from core.engine.candle_producer import CandleProducer
from core.fetcher.binance.rest_fetcher import BinanceCandleFetcher
from core.live.reliable_websocket import ReliableWebsocket
from core.utils import interval_to_minutes, ms_timestamp_to_datetime

#: 한 번에 백필할 수 있는 캔들 수의 상한. 이걸 넘으면 재기동으로 복구하는 편이 안전하다 —
#: 프로세스가 오래 죽어 있었다는 뜻이고, 재기동은 지표를 프리피드로 다시 세우고 포지션은
#: 거래소에서 다시 읽는다.
MAX_BACKFILL_CANDLES = 60


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
        # 라이브의 간격 출처는 config다 — 첫 캔들 전에 경계 정규화·갭 감지에 이미 필요하다.
        super().__init__(interval_to_minutes(interval) * 60_000)
        self._on_error = on_error
        #: 심볼별 마지막으로 내준 캔들의 시작 시각 — 중복/구멍 판정의 기준점. None이면 아직
        #: 기준이 없다 (프리피드 전, 또는 프리피드할 지표가 하나도 없는 심볼).
        self._last_candle_start: Dict[str, Optional[int]] = {s: None for s in self.symbols}
        self.socket: Optional[ReliableWebsocket] = None
        self._running = False
        #: 치명적 사유로 스트림을 끊었다면 그 이유. 소비자가 트레이더를 멈추는 근거가 된다.
        self.fatal_reason: Optional[str] = None

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

    async def __aiter__(self) -> AsyncIterator[Tuple[int, Dict[str, Candle]]]:
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
            except _FatalStream:
                break
            except _SkipCandle:
                continue  # 중복 수신 — 이미 경고를 남겼다
            except Exception as e:
                # 파싱/백필 중의 예상 못 한 오류. 한 메시지를 버리고 계속한다.
                self.logger.exception("Error processing kline message: %s", e)
                await self._on_error(e)

    async def _events_from(self, data: dict) -> AsyncIterator[Tuple[int, Dict[str, Candle]]]:
        """메시지 하나에서 나올 이벤트들 (백필 캔들이 있으면 그것들이 먼저)."""
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

        for missed in await self._missing_before(symbol, candle):
            self._last_candle_start[symbol] = missed.start_time
            yield missed.end_time, {symbol: missed}

        self._last_candle_start[symbol] = candle.start_time
        yield candle.end_time, {symbol: candle}

    async def _missing_before(self, symbol: str, candle: Candle) -> List[Candle]:
        """이 캔들 앞에 빠진 캔들들. 이 캔들 자체를 버려야 하면 :class:`_FatalStream`이나
        :class:`_SkipCandle`을 올린다.

        python-binance는 끊김을 스스로 재연결하고(``ReconnectingWebsocket._run_reconnect``)
        그 사이 메시지를 버린다. 게다가 그 사실은 예외가 아니라 ``{"e": "error"}`` 메시지로만
        알려지므로, 그냥 두면 두 가지가 조용히 일어난다: 마감 캔들이 통째로 빠져 지표가
        백테스트와 영구히 갈라지거나(청산 조건이 걸린 캔들을 놓치면 포지션이 그대로 남는다),
        재연결 직후 같은 마감 캔들이 다시 와서 ``decide_action``이 두 번 불리고 **주문이 두 번**
        나간다.
        """
        last = self._last_candle_start[symbol]
        if last is None:
            return []

        if candle.start_time <= last:
            self.logger.warning(
                f"이미 처리한 캔들을 버린다 (symbol={symbol}): "
                f"start={ms_timestamp_to_datetime(candle.start_time)} <= "
                f"마지막 처리 {ms_timestamp_to_datetime(last)}")
            raise _SkipCandle

        elapsed = candle.start_time - last
        if elapsed % self.interval_ms != 0:
            self._fatal(
                f"캔들 경계가 인터벌과 맞지 않는다 (symbol={symbol}): "
                f"{elapsed}ms 는 {self.interval_ms}ms 의 배수가 아니다 "
                f"({ms_timestamp_to_datetime(last)} -> "
                f"{ms_timestamp_to_datetime(candle.start_time)})")
            raise _FatalStream

        missing = elapsed // self.interval_ms - 1
        if missing == 0:
            return []

        if missing > MAX_BACKFILL_CANDLES:
            self._fatal(
                f"캔들 {missing}개가 비었다 (symbol={symbol}) — 백필 상한 "
                f"{MAX_BACKFILL_CANDLES}개를 넘어 재기동으로 복구한다")
            raise _FatalStream

        gap_start = last + self.interval_ms
        self.logger.warning(
            f"캔들 {missing}개가 비었다 (symbol={symbol}) — 백필해서 재생한다: "
            f"{ms_timestamp_to_datetime(gap_start)} ~ "
            f"{ms_timestamp_to_datetime(candle.start_time)}")
        try:
            return await self._fetch_range(symbol, gap_start, candle.start_time)
        except Exception as e:
            # 지표는 이미 갈라졌다. 이 상태로 계속 매매하면 전략이 백테스트와 다른 것을 본다.
            self._fatal(f"빠진 캔들을 백필하지 못했다 (symbol={symbol}): {e}")
            raise _FatalStream

    async def _fetch_range(self, symbol: str, start_ms: int, end_ms: int) -> List[Candle]:
        """``[start_ms, end_ms)`` 구간의 마감 캔들을 REST로 가져온다.

        ``get_candles``의 구간 규약이 반열린 구간이라 정확히 빠진 캔들만 돌아온다 —
        :meth:`warmup_candles`가 쓰는 규약과 같다. 개수와 정렬을 검증해서, 어긋난 캔들이
        조용히 지표에 먹히는 일이 없게 한다.
        """
        # 동기 HTTP + tqdm이라 스레드로 뺀다 — 이벤트 루프를 막으면 그동안 유저 데이터
        # 스트림을 읽지 못한다.
        candles = await asyncio.to_thread(
            BinanceCandleFetcher().get_candles,
            symbol=symbol, interval=self.interval,
            start_date=ms_timestamp_to_datetime(start_ms),
            end_date=ms_timestamp_to_datetime(end_ms))

        expected = (end_ms - start_ms) // self.interval_ms
        if len(candles) != expected:
            raise ValueError(f"백필 캔들 개수가 맞지 않는다: {len(candles)} != {expected}")
        for i, c in enumerate(candles):
            want = start_ms + i * self.interval_ms
            if c.start_time != want:
                raise ValueError(
                    f"백필 캔들 {i}의 시작 시각이 어긋난다: "
                    f"{ms_timestamp_to_datetime(c.start_time)} != "
                    f"{ms_timestamp_to_datetime(want)}")
        return candles

    # ------------------------------------------------------------- 워밍업

    async def warmup_candles(self, windows: Dict[str, int]) -> Dict[str, List[Candle]]:
        """지표 워밍업용 과거 캔들을 심볼별로 가져온다.

        심볼마다 독립적으로(순차) 페치하지만, 봉 경계로 내림한 ``end_time``은 전 심볼이
        공유한다 — 심볼마다 ``datetime.now()``에서 다시 계산하면, 순차 페치가 실제로 걸리는
        시간만큼 뒤 심볼의 창이 앞 심볼보다 늦은 경계로 밀려 서로 어긋난다.

        가져온 마지막 캔들이 그 심볼의 연속성 기준점이 된다 — 프리피드와 첫 라이브 캔들
        사이에 생긴 구멍도 백필로 메워진다 (프리피드는 레이트리밋 대기까지 포함해 수십 초가
        걸릴 수 있어 실제로 벌어지는 일이다).

        :param windows: 심볼별로 필요한 캔들 수 (보통 그 심볼 지표들의 최대 window).
        """
        interval_minutes = interval_to_minutes(self.interval)

        current_sec = datetime.now().second
        if 55 <= current_sec:
            sleep_sec = 61 - current_sec
            self.logger.info(f"skipping to next minute ({sleep_sec} seconds)")
            await asyncio.sleep(sleep_sec)

        # **인터벌 경계**로 내림한다. 분 단위로만 자르면 1h 인터벌을 13:37에 기동할 때
        # start_time이 :37이 되고 Binance가 주는 정시 정렬 캔들과 어긋나 아래 검증이 무조건
        # 실패한다 — 사실상 1m 외에는 라이브 기동이 불가능해진다.
        interval_delta = timedelta(minutes=interval_minutes)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        end_time = epoch + (now - epoch) // interval_delta * interval_delta

        fetcher = BinanceCandleFetcher()
        out: Dict[str, List[Candle]] = {}
        for symbol in self.symbols:
            max_window = windows.get(symbol, 0)
            if max_window == 0:
                self.logger.info(f"No indicators found for {symbol}, skipping pre-feeding")
                continue

            self.logger.info(f"[{symbol}] Maximum indicator window size: {max_window}")
            start_time = end_time - timedelta(minutes=max_window * interval_minutes)
            self.logger.info(f"[{symbol}] Fetching historical data from {start_time} to {end_time}")

            candles = await asyncio.to_thread(
                fetcher.get_candles, symbol=symbol, interval=self.interval,
                start_date=start_time, end_date=end_time)

            actual_start = ms_timestamp_to_datetime(candles[0].start_time) if candles else None
            actual_end = ms_timestamp_to_datetime(candles[-1].end_time) if candles else None
            if max_window != len(candles) or actual_start != start_time or actual_end != end_time:
                raise ValueError(
                    f"historical kline assert error for {symbol}: "
                    f"count {len(candles)} != {max_window}, "
                    f"start {actual_start} != {start_time}, "
                    f"end {actual_end} != {end_time}")

            out[symbol] = candles
            self._last_candle_start[symbol] = candles[-1].start_time
            self.logger.info(
                f"[{symbol}] Pre-fed indicators with {len(candles)} historical candles: "
                f"{ms_timestamp_to_datetime(candles[0].start_time)} ~ "
                f"{ms_timestamp_to_datetime(candles[-1].end_time)}")
        return out

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


class _FatalStream(Exception):
    """스트림을 끊어야 하는 상황. :meth:`LiveCandleProducer.__aiter__`가 잡아 반복을 끝낸다."""


class _SkipCandle(Exception):
    """이 캔들만 버리고 계속한다 (중복 수신)."""
