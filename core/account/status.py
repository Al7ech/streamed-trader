from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:  # 런타임 임포트는 순환을 만든다 (order_book이 Status를 쓴다)
    from core.order.order_book import OpenOrder

#: 계좌 수수료율의 **유일한 기본값 정의**. 실제로는 거래소가 VIP/커미션 티어로 정하는 계좌
#: 속성이라 ``Status``에 얹혀 있고, 실행기는 값(또는 ``None``)을 넘겨주기만 한다 —
#: ``SimulatedExecutor``는 생성자 인자로, ``LiveExecutor``는 ``futures_commission_rate``로.
#: 넘어온 값이 ``None``이면 아래 ``Status.__init__``이 이 상수를 박는다 (기본값이 실제로
#: 적용되는 곳은 거기 한 줄뿐이다). 전략은 사이징할 때 ``status.fee_ratio``를 읽는다.
DEFAULT_FEE_RATIO = 0.0004


@dataclass
class PositionState:
    """한 심볼의 포지션 상태. ``Status.margin``은 전 심볼이 공유하는 증거금 풀이라 여기 없다."""
    avg_price: float = 0.0
    position: float = 0.0
    unrealised_pnl: float = 0.0


class Status:
    def __init__(self,
                 margin: float = 0.0,
                 positions: Optional[Dict[str, PositionState]] = None,
                 open_orders: Optional[Dict[str, List["OpenOrder"]]] = None,
                 fee_ratio: Optional[float] = None):
        self.margin = margin
        self.positions: Dict[str, PositionState] = positions if positions is not None else {}
        #: 명목가치에 곱할 수수료율. 계좌 속성이라 여기 둔다 — 실행기가 값(또는 None)을 넘기고
        #: (백테스트: 생성자 인자, 라이브: 거래소 커미션 티어), 전략은 사이징 시 이 값을 읽는다.
        #: None이면 여기서 ``DEFAULT_FEE_RATIO``를 박는다 — 기본값이 적용되는 유일한 지점.
        self.fee_ratio = DEFAULT_FEE_RATIO if fee_ratio is None else fee_ratio
        #: 심볼별 미체결(resting) 주문. 거래소에서 미체결 주문은 실제로 계좌 상태의 일부이고,
        #: Status는 백테스터와 라이브 트레이더가 공유하는 단일 계좌 상태 표현이므로 여기 둔다.
        #: 덕분에 decide_action(candles, status) 시그니처를 바꾸지 않고도 전략이 자기
        #: 미체결 주문을 읽고 취소할 수 있다. 리스트 순서 = 제출 순서.
        self.open_orders: Dict[str, List["OpenOrder"]] = \
            open_orders if open_orders is not None else {}

    def position_for(self, symbol: str) -> PositionState:
        """해당 심볼의 PositionState. 처음 보는 심볼이면 flat 상태로 만들어 등록한다.
        """
        p = self.positions.get(symbol)
        if p is None:
            p = self.positions[symbol] = PositionState()
        return p

    def open_orders_for(self, symbol: str) -> List["OpenOrder"]:
        """해당 심볼의 미체결 주문 리스트. 처음 보는 심볼이면 빈 리스트를 만들어 등록한다.

        반환된 리스트는 살아 있는 참조다 — 전략은 **읽기만** 해야 하고, 취소는
        ``Action.cancel(symbol, client_id)``로 해야 세 엔진이 같은 의미를 갖는다.

        ``position_for``와 같은 이유로 get-후-삽입을 쓴다.
        """
        b = self.open_orders.get(symbol)
        if b is None:
            b = self.open_orders[symbol] = []
        return b

    def total_open_orders(self) -> int:
        return sum(len(v) for v in self.open_orders.values())

    def total_margin(self) -> float:
        return self.margin + sum(p.unrealised_pnl for p in self.positions.values())

    def update_unrealised_pnl(self, symbol: str, price: float) -> float:
        p = self.position_for(symbol)
        p.unrealised_pnl = p.position * (price - p.avg_price)
        return p.unrealised_pnl

    def leverage(self) -> float:
        """실효 레버리지 = 전 심볼 명목가치(평단 기준) 합 / 시가평가 자본.

        ``total_margin()``과 같은 부류의 **매 호출 계산되는 파생값**이다 — 저장하지 않는다.
        저장 필드로 두면 체결 회계·시가평가가 positions·margin을 바꾼 뒤 누가 갱신을 부르기
        전까지 낡은 값이 남는데, positions·margin만으로 언제든 다시 구할 수 있으므로 캐시할
        이유가 없다. 체결 순간에 얼려야 하는 시점 값은 ``Trade.leverage``가 따로 들고 있다.

        분모는 margin이 아니라 total_margin()이다. margin만 쓰면 미실현손익이 빠져서, 수수료로
        margin이 음수가 된 순간 비율이 음수가 되고 그게 max(0.0, ...)에 눌려 **파산이 레버리지
        0으로 보고되는** 문제가 있었다. 자본을 분모로 두면 그 퇴화가 원천 제거된다 — 강제청산이
        자본 0 이하에서 걸리므로 포지션이 열려 있는 한 분모는 양수다.

        마진이 공유 풀이므로 레버리지는 계좌 전체 기준이다: 각 심볼의 명목가치를 더해 하나의
        분자로 쓴다.
        """
        equity = self.total_margin()
        if equity <= 0.0:
            return 0.0
        notional = sum(p.avg_price * abs(p.position) for p in self.positions.values())
        return notional / equity

    def __repr__(self):
        positions = ", ".join(f"{sym}: [avg_price: {p.avg_price}, position: {p.position}, "
                              f"unrealised_pnl: {p.unrealised_pnl}]"
                              for sym, p in self.positions.items())
        resting = self.total_open_orders()
        orders = f", open_orders: {resting}" if resting else ""
        return (f"[margin: {self.margin}, leverage: {self.leverage()}, "
                f"positions: {{{positions}}}{orders}]")
