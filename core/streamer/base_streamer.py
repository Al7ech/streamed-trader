from abc import ABC, abstractmethod
from typing import Dict, List

from core.engine.status import Status
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

    def __init__(self, symbols: List[str], indicators: Dict[str, Dict[str, BaseIndicator]]):
        """
        :param symbols: 이 스트리머가 다루는 심볼 전체.
        :param indicators: 심볼별 지표 딕셔너리 — ``{symbol: {indicator_name: BaseIndicator}}``.
            지표 인스턴스는 각자 자기 시계열 상태를 들고 있으므로 (symbol, name) 쌍마다
            별도 인스턴스여야 한다 — 두 심볼이 같은 인스턴스를 공유하면 안 된다.
        """
        self.symbols = symbols
        self.indicators = indicators

    @abstractmethod
    def decide_action(self, symbol: str, candle: Candle, status: Status) -> List[Action]:
        """
        Update된 candle과 indicator들을 바탕으로 거래 Action들을 결정한다.

        :param symbol: 이번에 마감된 candle의 심볼. ``self.indicators``는 이 스트리머가
            다루는 **모든** 심볼의 지표를 담고 있으므로, 구현은 ``symbol`` 외의 다른 심볼의
            지표도 읽을 수 있고, 다른 심볼을 대상으로 하는 Action도 반환할 수 있다.
        :return: Action 리스트. 비어 있으면 아무 것도 하지 않는다.
        """
        pass
