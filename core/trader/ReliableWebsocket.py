import logging

from binance import ReconnectingWebsocket


class ReliableWebsocket:
    def __init__(self, delegate: ReconnectingWebsocket):
        self.delegate = delegate

        # Logging
        self.logger = logging.getLogger(__name__)

    async def recv(self):
        try:
            return await self.delegate.recv()
        except Exception as e:
            self.logger.warning(f"reconnecting {self.id()}: {e!r}")
            await self.delegate.close()
            await self.delegate.connect()
            return await self.delegate.recv()

    def id(self) -> str:
        return f"{id(self.delegate) & 0xFFFFFF:06x}"

    def __getattr__(self, name):
        return getattr(self.delegate, name)
