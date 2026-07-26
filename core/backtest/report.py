from typing import List, Optional, Tuple

from core.backtest.status import Status
from core.backtest.trade import Trade


class Report:
    def __init__(self, trades: List[Trade], max_leverage: float, status: Status,
                 equity_curve: Optional[List[Tuple[int, float]]] = None):
        self.trades = trades
        self.max_leverage = max_leverage
        self.status = status
        self.equity_curve = equity_curve or []
