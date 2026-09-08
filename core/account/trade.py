from dataclasses import dataclass
from typing import Optional


@dataclass
class Trade:
    timestamp: int
    symbol: str
    quantity: float
    price: float
    wnl: float  ## (win & lose) = 수수료 **차감 전** 실현손익. 순손익은 wnl - fee.
    fee: float
    ## 거래 **직전** 스냅샷에서 뽑은 두 스칼라. 예전엔 Status 전체를 deep copy 했지만
    ## 소비처가 읽는 건 이 둘뿐이다 (result_writer._win_lose_counts 의 포지션 부호,
    ## run JSON 의 trades[].margin -> 프론트의 손익 % 표시).
    pre_position: float  ## 이 체결 심볼의 거래 전 signed 포지션
    pre_margin: float    ## 거래 전 계좌 전체 자본 (status.total_margin())
    leverage: float = 0.0  ## *post-trade* leverage
    order_type: str = "MARKET"  ## 이 체결을 낳은 주문 종류 (ActionType의 값)
    ## 주문이 제출된 시각(ms). MARKET은 timestamp와 같고, 지정가/조건부 주문은 몇 봉 전이다 —
    ## 결정 시점과 체결 시점이 갈라지는 것을 timestamp 하나로는 표현할 수 없어 따로 둔다.
    submitted_at: Optional[int] = None
