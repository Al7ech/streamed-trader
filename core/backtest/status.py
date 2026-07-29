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

    def __repr__(self):
        return f"[margin: {self.margin}, avg_price: {self.avg_price}, unrealised_pnl: {self.unrealised_pnl}, position: {self.position}, leverage: {self.leverage}]"
