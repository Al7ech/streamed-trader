import logging

from binance import ReconnectingWebsocket


class ReliableWebsocket:
    def __init__(self, delegate: ReconnectingWebsocket):
        self.delegate = delegate

        #: 이 소켓이 재연결한 횟수. 로그에 같이 실어 연결 안정성을 눈으로 볼 수 있게 한다 —
        #: 재연결이 하루 한 번인지 분당 한 번인지는 운영상 전혀 다른 이야기인데, 예전에는
        #: 실패만 찍고 성공은 안 찍어서 로그에서 셀 수가 없었다.
        self.reconnects = 0

        # Logging
        self.logger = logging.getLogger(__name__)

    async def recv(self):
        try:
            return await self.delegate.recv()
        except Exception as e:
            self.logger.warning(f"reconnecting {self.id()}: {e!r}")
            await self.delegate.close()
            await self.delegate.connect()
            self.reconnects += 1
            self.logger.info("reconnected %s (누적 %d회)", self.id(), self.reconnects)
            return await self.delegate.recv()

    def id(self) -> str:
        return f"{id(self.delegate) & 0xFFFFFF:06x}"

    def __getattr__(self, name):
        return getattr(self.delegate, name)
