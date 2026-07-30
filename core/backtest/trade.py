from dataclasses import dataclass

from core.backtest.status import Status


@dataclass
class Trade:
    timestamp: int
    quantity: float
    price: float
    wnl: float  ## (win & lose) = 수수료 **차감 전** 실현손익. 순손익은 wnl - fee.
    fee: float
    status: Status  ## deep-copied *pre-trade* status snapshot
    leverage: float = 0.0  ## *post-trade* leverage
