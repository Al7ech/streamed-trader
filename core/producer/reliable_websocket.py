"""python-binance ``ReconnectingWebsocket``을 감싸 끊긴 소켓을 프로세스 안에서 되살린다.

python-binance(1.0.37 기준)는 끊김을 **예외로 던지지 않는다.** read loop가 잡아서
``{"e": "error", "type": <예외 클래스 이름>, "m": <메시지>}`` 프레임을 큐에 넣고, ``recv()``는
그걸 정상 메시지처럼 돌려준다. 프레임은 두 부류다.

- **라이브러리가 재연결 중** (:data:`_LIBRARY_RECONNECTS`): read loop는 계속 돌고 백오프 뒤
  스스로 ``connect()``를 다시 한다(최대 ``MAX_RECONNECTS``회). 여기서 또 재연결하면 이중
  재연결이므로 **손대지 않는다.** 끝내 실패하면 아래 부류의 프레임이나 ``ReadLoopClosed``로
  이어져 거기서 잡힌다.
- **read loop가 끝났다** (그 밖의 전부 — ``BinanceWebsocketUnableToConnect``,
  ``BinanceWebsocketQueueOverflow``, 처리 안 된 예외): 아무도 다시 연결하지 않으므로 우리가
  ``close()`` → ``connect()``로 새로 연다. 다음 ``recv()``는 ``ReadLoopClosed``를 던지는데
  그것도 같은 경로로 간다.

``CancelledError`` 프레임은 read loop 태스크가 취소된 것(종료 중)이라 재연결 사유가 아니다.
정말 죽은 거라면 다음 ``recv()``의 ``ReadLoopClosed``가 잡는다.

판정은 비공개 속성(``_handle_read_loop``)이 아니라 프레임의 ``type`` 이름으로 한다. 그래서
위 분류는 python-binance 1.0.37 ``ReconnectingWebsocket._read_loop``의 except 튜플을 그대로
옮긴 것이고, **라이브러리를 올리면 다시 확인해야 한다.** 모르는 이름은 "끝났다"로 본다 —
라이브러리의 catch-all 분기도 루프를 끝낸다. 분류가 틀려도 ``ReadLoopClosed``가 최후 그물이다.
"""

import asyncio
import logging
import time
from typing import Optional

from binance import ReconnectingWebsocket

#: read loop를 끝내지 않고 라이브러리가 스스로 재연결하는 예외 (python-binance 1.0.37).
_LIBRARY_RECONNECTS = frozenset({
    "IncompleteReadError", "gaierror", "ConnectionClosedError", "ConnectionClosedOK",
    "BinanceWebsocketClosed",
})
#: read loop 태스크 취소 — 종료 중이지 끊김이 아니다.
_SHUTDOWN = frozenset({"CancelledError"})


class WebsocketReconnectFailed(RuntimeError):
    """재연결을 정해진 횟수만큼 시도하고 포기했다 — 호출자는 치명적으로 다룬다."""


def _error_frame_type(data) -> Optional[str]:
    if isinstance(data, dict) and data.get("e") == "error":
        return str(data.get("type") or "")
    return None


class ReliableWebsocket:
    """:param max_attempts: 정상 메시지 없이 연달아 시도할 재연결 횟수 상한. 넘기면
        :class:`WebsocketReconnectFailed` — 프로세스 재기동이라는 최후 수단으로 넘긴다.
    :param backoff_s: 두 번째 시도 전 대기(초). 시도마다 두 배, ``backoff_max_s``에서 멈춘다.
        첫 시도는 바로 한다.
    :param stable_after_s: 마지막 재연결 성공 뒤 이만큼 지났으면 연속 시도 횟수를 0으로 되돌린다.
        보통은 정상 메시지가 되돌리지만, 유저 데이터 소켓은 몇 시간씩 조용할 수 있다.
    """

    def __init__(self, delegate: ReconnectingWebsocket, *, max_attempts: int = 5,
                 backoff_s: float = 1.0, backoff_max_s: float = 16.0,
                 stable_after_s: float = 60.0):
        self.delegate = delegate
        self.max_attempts = max_attempts
        self.backoff_s = backoff_s
        self.backoff_max_s = backoff_max_s
        self.stable_after_s = stable_after_s

        #: 이 소켓이 재연결한 횟수. 로그에 같이 실어 연결 안정성을 눈으로 볼 수 있게 한다 —
        #: 재연결이 하루 한 번인지 분당 한 번인지는 운영상 전혀 다른 이야기인데, 예전에는
        #: 실패만 찍고 성공은 안 찍어서 로그에서 셀 수가 없었다.
        self.reconnects = 0
        #: 라이브러리가 내부에서 재연결에 성공한 횟수. 재연결 중 프레임 뒤에 정상 메시지가
        #: 다시 오면 성공으로 센다 (라이브러리는 성공을 debug로만 남긴다).
        self.library_reconnects = 0

        self._closing = False
        self._reconnect_due: Optional[str] = None
        self._library_recovering = False
        #: 정상 메시지 없이 연달아 한 재연결 시도 수.
        self._streak = 0
        self._last_reconnect_ok: Optional[float] = None

        # Logging
        self.logger = logging.getLogger(__name__)

    @property
    def recoveries(self) -> int:
        """끊김에서 되살아난 총 횟수 (우리 재연결 + 라이브러리 재연결)."""
        return self.reconnects + self.library_reconnects

    async def recv(self):
        """다음 메시지. 에러 프레임도 **그대로 돌려준다** — 호출자의 에러 경로(``on_error``,
        ``add_error_callback``)가 알림 지점이라, 여기서 삼키면 끊김이 운영자에게 안 보인다.
        여기서는 프레임을 보고 재연결이 필요한지만 판단한다.
        """
        while True:
            if self._reconnect_due is not None:
                reason, self._reconnect_due = self._reconnect_due, None
                await self._reconnect(reason)
            try:
                data = await self.delegate.recv()
            except Exception as e:
                if self._closing:
                    raise
                self._reconnect_due = repr(e)
                continue

            kind = _error_frame_type(data)
            if kind is None:
                self._on_message()
            elif kind in _LIBRARY_RECONNECTS:
                if not self._library_recovering:
                    self._library_recovering = True
                    self.logger.warning("python-binance가 %s를 재연결 중: %s (%s)",
                                        self.id(), kind, data.get("m"))
            elif kind not in _SHUTDOWN and not self._closing:
                # 프레임은 먼저 호출자에게 넘기고, 재연결은 다음 recv() 처음에 한다.
                self._reconnect_due = f"{kind}: {data.get('m')}"
            return data

    def _on_message(self) -> None:
        self._streak = 0
        if self._library_recovering:
            self._library_recovering = False
            self.library_reconnects += 1
            self.logger.info("reconnected %s by python-binance (라이브러리 누적 %d회)",
                             self.id(), self.library_reconnects)

    async def _reconnect(self, reason: str) -> None:
        # 우리가 다시 여는 순간 라이브러리 쪽 회복 에피소드는 끝난 것이다 (이중 집계 방지).
        self._library_recovering = False
        if (self._last_reconnect_ok is not None
                and time.monotonic() - self._last_reconnect_ok > self.stable_after_s):
            self._streak = 0
        while self._streak < self.max_attempts:
            if self._closing:
                raise WebsocketReconnectFailed(f"{self.id()} 닫는 중이라 재연결하지 않는다")
            self._streak += 1
            if self._streak > 1:
                await asyncio.sleep(min(self.backoff_s * 2 ** (self._streak - 2),
                                        self.backoff_max_s))
            self.logger.warning("reconnecting %s (시도 %d/%d): %s", self.id(), self._streak,
                                self.max_attempts, reason)
            try:
                await self.delegate.close()
            except Exception as e:
                self.logger.warning("closing %s before reconnect failed: %r", self.id(), e)
            try:
                await self.delegate.connect()
            except Exception as e:
                reason = f"connect 실패: {e!r}"
                continue
            self.reconnects += 1
            self._last_reconnect_ok = time.monotonic()
            self.logger.info("reconnected %s (누적 %d회)", self.id(), self.reconnects)
            return
        raise WebsocketReconnectFailed(
            f"{self.id()} 재연결을 {self.max_attempts}회 연달아 시도하고 포기했다: "
            f"{reason}")

    async def close(self):
        """정상 종료. 이후의 ``ReadLoopClosed``/``CancelledError`` 프레임은 재연결 사유가 아니다."""
        self._closing = True
        await self.delegate.close()

    def id(self) -> str:
        return f"{id(self.delegate) & 0xFFFFFF:06x}"

    def __getattr__(self, name):
        return getattr(self.delegate, name)
