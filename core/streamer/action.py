class Action:
    def __init__(self, symbol: str, quantity: float):
        self.symbol = symbol
        self.quantity = quantity

    def __repr__(self):
        return f"[symbol: {self.symbol}, quantity: {self.quantity}]"
