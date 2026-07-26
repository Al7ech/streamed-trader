from abc import ABC, abstractmethod
from typing import Dict

from core.backtest.status import Status
from core.streamer.action import Action
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import BaseIndicator


class BaseStreamer(ABC):
    """
    Base class for all streamer implementations.
    
    This abstract base class defines the interface that all streamers must implement.
    It provides common functionality and enforces a consistent API across different
    streaming implementations.
    """

    def __init__(self, indicators: Dict[str, BaseIndicator]):
        """
        Initialize the BaseStreamer.
        """
        self.indicators = indicators

    def update_candle(self, candle: Candle, status: Status) -> Action:
        return self.decide_action(candle, status)

    @abstractmethod
    def decide_action(self, candle: Candle, status: Status) -> Action:
        """
        Update된 candle과 indicator들을 바탕으로 거래 Action을 결정한다.
        """
        pass
