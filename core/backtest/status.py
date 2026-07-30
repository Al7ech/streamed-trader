from typing import Tuple


class Status:
    def __init__(self,
                 avg_price: float = 0.0,
                 unrealised_pnl: float = 0.0,
                 margin: float = 0.0,
                 position: float = 0.0,
                 leverage: float = 0.0):
        self.avg_price = avg_price
        self.margin = margin
        self.unrealised_pnl = unrealised_pnl

        self.position = position
        self.leverage = leverage

    def total_margin(self):
        return self.margin + self.unrealised_pnl

    def update_unrealised_pnl(self, price: float) -> float:
        self.unrealised_pnl = self.position * (price - self.avg_price)
        return self.unrealised_pnl

    def update_leverage(self) -> float:
        """실효 레버리지 = 명목가치(평단 기준) / 시가평가 자본.

        분모는 margin이 아니라 total_margin()이다. margin만 쓰면 미실현손익이 빠져서, 수수료로
        margin이 음수가 된 순간 비율이 음수가 되고 그게 max(0.0, ...)에 눌려 **파산이 레버리지
        0으로 보고되는** 문제가 있었다. 자본을 분모로 두면 그 퇴화가 원천 제거된다 — 강제청산이
        자본 0 이하에서 걸리므로 포지션이 열려 있는 한 분모는 양수다.

        값이 달라지는 것은 같은 방향 증량 체결뿐이다: 이 함수는 체결 직후에만 불리는데,
        flat에서의 신규 진입은 미실현이 0이라 margin == total_margin()이고 전량 청산은
        position이 0이다. 손익 계산에는 쓰이지 않는 보고 전용 값이다.
        """
        equity = self.total_margin()
        if equity <= 0.0:
            self.leverage = 0.0
        else:
            self.leverage = self.avg_price * abs(self.position) / equity
        return self.leverage

    def apply_fill(self, quantity: float, price: float, fee_ratio: float) -> Tuple[float, float]:
        """체결 하나를 이 Status에 반영한다. 반환값은 (wnl, fee).

        백테스터(``SingleThreadedBacktester._trade``)와 라이브 트레이더의 dry-run 경로가
        **같은** 회계를 쓰도록 여기 한 곳에만 둔다. 예전에는 dry-run이 자체 근사식을 써서
        부분 청산에서 avg_price를 현재가로 덮어쓰고 margin을 아예 갱신하지 않았다.

        ``wnl``은 **수수료 차감 전** 실현손익이고 ``fee``는 별도로 반환한다. margin에는 둘 다
        반영된다(실현손익 가산 후 수수료 차감).

        :param quantity: 현재 포지션에 더할 부호 있는 수량 (양수=매수, 음수=매도)
        :param price: 체결가
        :param fee_ratio: 명목가치에 곱할 수수료율
        """
        qty = quantity
        wnl = 0.0
        fee = price * abs(qty) * fee_ratio

        # 신규 진입 (롱/숏 방향 동일하게 처리)
        if self.position == 0.0:
            self.avg_price = price
            self.position = qty

        # 같은 방향 추가 진입 (롱/숏)
        elif (self.position > 0 and qty > 0) or (self.position < 0 and qty < 0):
            total_cost = abs(self.avg_price * self.position) + abs(price * qty)
            total_pos = self.position + qty
            self.avg_price = total_cost / abs(total_pos)
            self.position = total_pos
            # margin, unrealised_pnl는 변동 없음

        # 반대 방향 청산(부분/전부)
        else:
            if abs(qty) > abs(self.position):
                # 방향 전환: 기존 포지션 청산 후 신규 진입
                open_qty = qty + self.position

                wnl = self.unrealised_pnl
                # PNL/margin 계산 (전부 청산)
                self.avg_price = price
                self.margin += self.unrealised_pnl
                self.unrealised_pnl = 0.0
                self.position = open_qty
            else:
                # 부분 청산 (수량이 정확히 같으면 비율이 1이라 전량 청산이 된다)
                closed_qty = qty
                realised_pnl = self.unrealised_pnl * (-closed_qty / self.position)

                wnl = realised_pnl
                # avg_price는 변동 없음
                self.margin += realised_pnl
                self.unrealised_pnl -= realised_pnl
                self.position += closed_qty

        if self.position == 0.0:
            self.avg_price = 0.0

        self.margin -= fee

        return wnl, fee

    def __repr__(self):
        return f"[margin: {self.margin}, avg_price: {self.avg_price}, unrealised_pnl: {self.unrealised_pnl}, position: {self.position}, leverage: {self.leverage}]"
