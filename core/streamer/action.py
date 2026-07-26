class Action:
    def __init__(self, quantity: float):
        self.quantity = quantity

    def __repr__(self):
        return f"[quantity: {self.quantity}]"
