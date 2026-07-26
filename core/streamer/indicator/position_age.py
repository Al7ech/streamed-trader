from collections import deque
from typing import Optional

from core.backtest.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import BaseIndicator


class PositionAgeIndicator(BaseIndicator):
    """포지션을 보유한 연속 캔들 수를 세는 status 기반 지표.

    update()는 pre-trade status를 받으므로 진입 캔들 자체는 age 0으로 기록되고, 이후
    캔들부터 1씩 증가한다. 즉 decide_action 시점의 get_latest()는 "직전 캔들 기준,
    진입 캔들을 제외하고 몇 캔들째 보유 중인지"를 돌려준다 (flat이면 0).

    status가 벡터화 불가능한 피드백이므로 plain BaseIndicator(loop 경로)로만 동작한다.
    라이브 트레이더의 prefeed는 status=None을 넘기므로 age는 0에서 시작한다 — 포지션을
    든 채 재시작하면 보유 시간이 0부터 다시 세어진다는 한계가 있다 (보수적: 시간 기반
    청산 조정이 실제보다 늦게 발동).
    """

    scale_group = "age"

    def __init__(self):
        super().__init__(1)
        self.values = deque(maxlen=2)
        self._age = 0

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        if status is not None and status.position != 0.0:
            self._age += 1
        else:
            self._age = 0
        self.values.append(self._age)

    def get_index(self, idx: int) -> Optional[float]:
        try:
            return self.values[idx]
        except IndexError:
            return None
