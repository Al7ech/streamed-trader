from dataclasses import dataclass
from typing import Optional

from core.backtest.status import Status


@dataclass
class Trade:
    timestamp: int
    symbol: str
    quantity: float
    price: float
    wnl: float  ## (win & lose) = 수수료 **차감 전** 실현손익. 순손익은 wnl - fee.
    fee: float
    status: Status  ## deep-copied *pre-trade* status snapshot
    leverage: float = 0.0  ## *post-trade* leverage
    order_type: str = "MARKET"  ## 이 체결을 낳은 주문 종류 (ActionType의 값)
    ## 주문이 제출된 시각(ms). MARKET은 timestamp와 같고, 지정가/조건부 주문은 몇 봉 전이다 —
    ## 결정 시점과 체결 시점이 갈라지는 것을 timestamp 하나로는 표현할 수 없어 따로 둔다.
    submitted_at: Optional[int] = None
