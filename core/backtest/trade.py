from dataclasses import dataclass

from core.backtest.status import Status


@dataclass
class Trade:
    timestamp: int
    quantity: float
    price: float
    wnl: float  ## (profit & loss) = (win & lose) + fee
    fee: float
    status: Status  ## deep-copied *pre-trade* status snapshot
    leverage: float = 0.0  ## *post-trade* leverage
