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
        if self.margin == 0.0:
            self.leverage = 0.0
        else:
            self.leverage = max(0.0, self.avg_price * abs(self.position) / self.margin)
        return self.leverage

    def __repr__(self):
        return f"[margin: {self.margin}, avg_price: {self.avg_price}, unrealised_pnl: {self.unrealised_pnl}, position: {self.position}, leverage: {self.leverage}]"
