from typing import Optional

from core.domain.candle import Candle
from core.domain.status import Status
from core.streamer.indicator.base_indicator import NumericIndicator


class PositionAgeIndicator(NumericIndicator):
    """포지션을 보유한 연속 캔들 수를 세는 status 기반 지표.

    update()는 decide_action보다 먼저, pre-trade status로 불린다. 진입 캔들 자체는 그 시점에
    아직 체결 전이라 age 0으로 기록되고, 이후 캔들부터 1씩 증가한다 — 체결은 같은 캔들의
    decide_action 이후에 일어나므로, 진입 캔들의 update()는 여전히 flat인 status를 본다.
    즉 decide_action 시점의 get_latest()는 "이번 캔들까지 몇 캔들째 보유 중인지"를 돌려준다
    (flat이면 0).

    status가 벡터화 불가능한 피드백이므로 plain NumericIndicator(loop 경로)로만 동작한다.
    라이브 트레이더의 prefeed는 status=None을 넘기므로 age는 0에서 시작한다 — 포지션을
    든 채 재시작하면 보유 시간이 0부터 다시 세어진다는 한계가 있다 (보수적: 시간 기반
    청산 조정이 실제보다 늦게 발동).
    """

    scale_group = "age"

    def __init__(self, symbol: str):
        super().__init__(history_size=2)
        self.window = 1
        self._symbol = symbol
        self._age = 0

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        if status is not None and status.position_for(self._symbol).position != 0.0:
            self._age += 1
        else:
            self._age = 0
        self._deque.append(self._age)
