from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:  # 런타임 임포트는 순환을 만든다 (order_book이 Status를 쓴다)
    from core.engine.order_book import OpenOrder


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
                 leverage: float = 0.0,
                 last_close: Optional[Dict[str, float]] = None,
                 open_orders: Optional[Dict[str, List["OpenOrder"]]] = None):
        self.margin = margin
        self.positions: Dict[str, PositionState] = positions if positions is not None else {}
        self.leverage = leverage
        #: 심볼별 최근 알려진 종가. 엔진이 매 이벤트마다 갱신한다 — 트리거 심볼이 아닌
        #: 다른 심볼을 대상으로 하는 Action의 체결가/시가평가에 쓰인다.
        self.last_close: Dict[str, float] = last_close if last_close is not None else {}
        #: 심볼별 미체결(resting) 주문. 거래소에서 미체결 주문은 실제로 계좌 상태의 일부이고,
        #: Status는 백테스터와 라이브 트레이더가 공유하는 단일 계좌 상태 표현이므로 여기 둔다.
        #: 덕분에 decide_action(symbol, candle, status) 시그니처를 바꾸지 않고도 전략이 자기
        #: 미체결 주문을 읽고 취소할 수 있다. 리스트 순서 = 제출 순서.
        self.open_orders: Dict[str, List["OpenOrder"]] = \
            open_orders if open_orders is not None else {}

    def position_for(self, symbol: str) -> PositionState:
        """해당 심볼의 PositionState. 처음 보는 심볼이면 flat 상태로 만들어 등록한다."""
        return self.positions.setdefault(symbol, PositionState())

    def open_orders_for(self, symbol: str) -> List["OpenOrder"]:
        """해당 심볼의 미체결 주문 리스트. 처음 보는 심볼이면 빈 리스트를 만들어 등록한다.

        반환된 리스트는 살아 있는 참조다 — 전략은 **읽기만** 해야 하고, 취소는
        ``Action.cancel(symbol, client_id)``로 해야 세 엔진이 같은 의미를 갖는다.
        """
        return self.open_orders.setdefault(symbol, [])

    def total_open_orders(self) -> int:
        return sum(len(v) for v in self.open_orders.values())

    def total_margin(self) -> float:
        return self.margin + sum(p.unrealised_pnl for p in self.positions.values())

    def update_unrealised_pnl(self, symbol: str, price: float) -> float:
        p = self.position_for(symbol)
        p.unrealised_pnl = p.position * (price - p.avg_price)
        return p.unrealised_pnl

    def update_leverage(self) -> float:
        """실효 레버리지 = 전 심볼 명목가치(평단 기준) 합 / 시가평가 자본.

        분모는 margin이 아니라 total_margin()이다. margin만 쓰면 미실현손익이 빠져서, 수수료로
        margin이 음수가 된 순간 비율이 음수가 되고 그게 max(0.0, ...)에 눌려 **파산이 레버리지
        0으로 보고되는** 문제가 있었다. 자본을 분모로 두면 그 퇴화가 원천 제거된다 — 강제청산이
        자본 0 이하에서 걸리므로 포지션이 열려 있는 한 분모는 양수다.

        마진이 공유 풀이므로 레버리지는 계좌 전체 기준이다: 각 심볼의 명목가치를 더해 하나의
        분자로 쓴다.
        """
        equity = self.total_margin()
        if equity <= 0.0:
            self.leverage = 0.0
        else:
            notional = sum(p.avg_price * abs(p.position) for p in self.positions.values())
            self.leverage = notional / equity
        return self.leverage

    def apply_fill(self, symbol: str, quantity: float, price: float,
                    fee_ratio: float) -> Tuple[float, float]:
        """체결 하나를 이 Status에 반영한다. 반환값은 (wnl, fee).

        백테스터(``SingleThreadedBacktester._trade``)와 라이브 트레이더의 dry-run 경로가
        **같은** 회계를 쓰도록 여기 한 곳에만 둔다. 예전에는 dry-run이 자체 근사식을 써서
        부분 청산에서 avg_price를 현재가로 덮어쓰고 margin을 아예 갱신하지 않았다.

        ``wnl``은 **수수료 차감 전** 실현손익이고 ``fee``는 별도로 반환한다. margin에는 둘 다
        반영된다(실현손익 가산 후 수수료 차감). margin은 전 심볼이 공유하는 증거금 풀이라
        어느 심볼의 체결이든 같은 ``self.margin``을 갱신한다 — 심볼별로 분리되는 것은
        position/avg_price/unrealised_pnl 뿐이다.

        :param symbol: 체결이 일어난 심볼.
        :param quantity: 현재 포지션에 더할 부호 있는 수량 (양수=매수, 음수=매도)
        :param price: 체결가
        :param fee_ratio: 명목가치에 곱할 수수료율
        """
        p = self.position_for(symbol)
        qty = quantity
        wnl = 0.0
        fee = price * abs(qty) * fee_ratio

        # 신규 진입 (롱/숏 방향 동일하게 처리)
        if p.position == 0.0:
            p.avg_price = price
            p.position = qty

        # 같은 방향 추가 진입 (롱/숏)
        elif (p.position > 0 and qty > 0) or (p.position < 0 and qty < 0):
            total_cost = abs(p.avg_price * p.position) + abs(price * qty)
            total_pos = p.position + qty
            p.avg_price = total_cost / abs(total_pos)
            p.position = total_pos
            # margin, unrealised_pnl는 변동 없음

        # 반대 방향 청산(부분/전부)
        else:
            if abs(qty) > abs(p.position):
                # 방향 전환: 기존 포지션 청산 후 신규 진입
                open_qty = qty + p.position

                wnl = p.unrealised_pnl
                # PNL/margin 계산 (전부 청산)
                p.avg_price = price
                self.margin += p.unrealised_pnl
                p.unrealised_pnl = 0.0
                p.position = open_qty
            else:
                # 부분 청산 (수량이 정확히 같으면 비율이 1이라 전량 청산이 된다)
                closed_qty = qty
                realised_pnl = p.unrealised_pnl * (-closed_qty / p.position)

                wnl = realised_pnl
                # avg_price는 변동 없음
                self.margin += realised_pnl
                p.unrealised_pnl -= realised_pnl
                p.position += closed_qty

        if p.position == 0.0:
            p.avg_price = 0.0

        self.margin -= fee

        return wnl, fee

    def __repr__(self):
        positions = ", ".join(f"{sym}: [avg_price: {p.avg_price}, position: {p.position}, "
                              f"unrealised_pnl: {p.unrealised_pnl}]"
                              for sym, p in self.positions.items())
        resting = self.total_open_orders()
        orders = f", open_orders: {resting}" if resting else ""
        return (f"[margin: {self.margin}, leverage: {self.leverage}, "
                f"positions: {{{positions}}}{orders}]")
