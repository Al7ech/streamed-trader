import logging

from binance import ReconnectingWebsocket


class ReliableWebsocket:
    def __init__(self, delegate: ReconnectingWebsocket):
        self.delegate = delegate

        # Logging
        self.logger = logging.getLogger(__name__)

    async def recv(self):
        try:
            await self.delegate.recv()
        except Exception as e:
            logging.info(f"reconnecting {self.id()}")
            await self.delegate.close()
            await self.delegate.connect()
            return self.delegate.recv()

    def id(self) -> str:
        return f"{id(self.delegate) & 0xFFFFFF:06x}"

    def __getattr__(self, name):
        return getattr(self.delegate, name)
