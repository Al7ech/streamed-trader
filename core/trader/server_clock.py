"""거래소 서버 시각을 로컬 monotonic 시계로 이어 재는 시계.

라이브가 "지금"을 거래소 기준으로 알아야 하는 곳이 두 군데다:

- **워밍업 구간의 끝** — 로컬 시계가 거래소보다 앞서 있으면 아직 진행 중인 봉이 구간에
  들어와 마감봉처럼 지표에 먹힌다 (:meth:`~core.engine.engine.TradingEngine.warmup`).
- **결정 기한** — 봉이 경계 뒤 몇 초 만에 처리됐는지를 잰다. 기한이 몇 초 단위라, 로컬
  시계가 몇 초 어긋나면 판정이 통째로 틀린다 (실제로 WSL2에서 2.7초 뒤처진 것을 쟀다).

매번 서버에 물을 수는 없다 — 결정 기한은 봉마다, 동기 코드(``process_event`` 직전) 안에서
읽는다. 그래서 한 번 물어본 서버 시각에 로컬 ``time.monotonic()``의 경과를 더한다.
monotonic은 벽시계 보정에 흔들리지 않지만, 호스트 절전 동안 멈출 수 있고 조금씩 흐를 수도
있어서 :meth:`ServerClock.resync_forever`가 주기적으로 다시 맞춘다.
"""

import asyncio
import logging
import time
from typing import Optional

#: 로컬 시계와 거래소 서버 시각의 어긋남이 이보다 크면 경고한다.
CLOCK_SKEW_WARN_MS = 1000

#: :meth:`ServerClock.resync_forever`의 기본 재동기 주기(초).
DEFAULT_RESYNC_EVERY_S = 300.0


class ServerClock:
    """거래소 서버 시각(ms epoch)을 돌려주는 시계.

    :param client: ``futures_time()``을 가진 python-binance ``AsyncClient`` (인증 불필요).
    """

    def __init__(self, client):
        self._client = client
        self.logger = logging.getLogger(__name__)
        #: 마지막 동기 시점의 서버 시각과, 그 시각에 대응하는 로컬 monotonic 초.
        self._server_ms: Optional[float] = None
        self._mono: Optional[float] = None
        self._warned = False

    async def sync(self) -> int:
        """서버에 시각을 물어 기준점을 다시 잡고, 그 순간의 서버 시각을 돌려준다.

        응답의 ``serverTime``은 왕복 중간쯤의 시각이므로 기준 monotonic도 요청 전후의 중간으로
        잡는다. 로컬 벽시계와의 어긋남은 처음 한 번 경고한다 (이후엔 DEBUG) — 어긋남이 계속되는
        머신에서 재동기마다 경고가 쌓이지 않게.
        """
        t0 = time.monotonic()
        server_ms = int((await self._client.futures_time())["serverTime"])
        t1 = time.monotonic()
        self._server_ms, self._mono = server_ms, (t0 + t1) / 2

        skew_ms = round(time.time() * 1000) - server_ms
        if abs(skew_ms) > CLOCK_SKEW_WARN_MS and not self._warned:
            self._warned = True
            self.logger.warning("로컬 시계가 거래소 서버와 %+dms 어긋나 있다 — 워밍업 구간과 결정 "
                                "기한은 서버 시각 기준으로 잰다", skew_ms)
        else:
            self.logger.debug("server clock synced: skew=%+dms rtt=%.0fms",
                              skew_ms, (t1 - t0) * 1000)
        return server_ms

    def now_ms(self) -> int:
        """지금의 서버 시각 추정. 한 번도 동기하지 않았으면 로컬 벽시계로 대신한다."""
        if self._server_ms is None:
            return round(time.time() * 1000)
        return round(self._server_ms + (time.monotonic() - self._mono) * 1000)

    async def resync_forever(self, every_s: float = DEFAULT_RESYNC_EVERY_S) -> None:
        """``every_s``마다 다시 맞춘다. 실패하면 경고만 남기고 이전 기준점을 계속 쓴다."""
        while True:
            await asyncio.sleep(every_s)
            try:
                await self.sync()
            except Exception as e:
                self.logger.warning("서버 시각 재동기 실패 — 이전 기준점을 계속 쓴다: %s", e)
