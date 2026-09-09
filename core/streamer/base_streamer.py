from abc import ABC, abstractmethod
from typing import Dict, List

from core.order.action import Action
from core.candle.candle import Candle
from core.account.status import Status
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

    def warmup_windows(self) -> Dict[str, int]:
        """심볼별로 워밍업에 필요한 캔들 수 = 그 심볼 지표들의 최대 window.

        :meth:`~core.engine.engine.TradingEngine.warmup`이 받아올 구간 길이를 정할 때
        (전 심볼 공통 ``max(window)``), 그리고 다 먹인 뒤 "충분히 데워졌는가"를 심볼별로
        판정할 때 쓴다.
        """
        return {symbol: max((ind.window for ind in self.indicators.get(symbol, {}).values()),
                            default=0)
                for symbol in self.symbols}

    @abstractmethod
    def decide_action(self, candles: Dict[str, Candle], status: Status) -> List[Action]:
        """
        Update된 candle들과 indicator들을 바탕으로 거래 Action들을 결정한다.

        이벤트당 **한 번** 호출된다 — 호출 시점에 이 스트리머가 다루는 **모든** 심볼의
        지표가 이번 이벤트 캔들까지 이미 갱신돼 있으므로, 크로스심볼 결정이 자연스럽다.

        :param candles: 이번 이벤트에 캔들이 마감한 심볼 → 그 캔들. 라이브는 항상 원소
            하나, 백테스트 병합 이벤트는 하나 이상. ``self.indicators``는 여기 없는 심볼의
            지표도 담고 있으므로, 구현은 임의의 심볼 지표를 읽고 임의의 심볼을 겨냥한
            Action(각 ``Action``이 자기 ``.symbol``을 든다)을 반환할 수 있다.
        :return: Action 리스트. 비어 있으면 아무 것도 하지 않는다.
        """
        pass
