from typing import List, Optional, Tuple

from core.backtest.status import Status
from core.backtest.trade import Trade


class Report:
    def __init__(self, trades: List[Trade], max_leverage: float, status: Status,
                 equity_curve: Optional[List[Tuple[int, float]]] = None,
                 benchmark_curve: Optional[List[Tuple[int, float]]] = None):
        self.trades = trades
        self.max_leverage = max_leverage
        self.status = status
        self.equity_curve = equity_curve or []
        # 기초자산 단순 보유(buy & hold) 곡선 — equity_curve 와 같은 길이/타임스탬프.
        # 전략 성과가 아니라 비교 기준선이라 metrics.build_buy_and_hold_curve 로 따로 만든다.
        self.benchmark_curve = benchmark_curve or []
