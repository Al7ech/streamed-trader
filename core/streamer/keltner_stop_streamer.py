import logging
from typing import List

from core.engine.status import Status
from core.streamer.action import Action, ActionType
from core.streamer.candle import Candle
from core.streamer.keltner_streamer import KeltnerStreamer
from core.utils import trunc_by_sign


class KeltnerStopStreamer(KeltnerStreamer):
    """Keltner 채널 돌파 + **진짜 손절 주문**.

    ``KeltnerStreamer``와 진입 규칙은 같지만, 청산을 손으로 흉내내지 않는다. 원본은
    ``candle.low < ma - m_exit*atr`` 를 확인한 뒤 **그 봉의 종가로** 청산하는데, 손절가를
    뚫고 종가까지 더 밀린 봉에서는 실제보다 나쁜 가격에, 되돌아온 봉에서는 실제보다 좋은
    가격에 체결된 것으로 계산된다. 여기서는 진입과 동시에 ``reduce_only`` 조건부 시장가를
    채널 이탈 지점에 걸어두므로, 체결가가 실제 트리거 가격이 된다.

    채널은 매 봉 움직이므로 포지션을 들고 있는 동안 손절을 **취소 후 재등록**해 따라 올린다
    (트레일링). 취소 액션이 재등록보다 먼저 처리되어야 같은 ``client_id``가 겹치지 않는데, 이 순서는
    **백테스트/드라이런에서만** 보장된다 (``SimulatedExecutor``가 동기라 리스트 순서대로 처리한다).
    라이브에서는 두 주문이 스레드풀에서 경쟁하므로 한 봉 동안 손절이 stale이거나 잠깐 사라질 수
    있다 — 라이브 트레일링 스탑은 봉마다 유니크한 ``client_id`` + 직전 id를 지정 취소하는 방식을
    써야 한다 (이 예제는 픽스처라 그대로 둔다).

    사이징이 원본과 다른 점: 손절 거리를 종가가 아니라 **실제 손절가**에서 재므로, 걸어둔
    주문과 의도한 손실폭이 정확히 맞는다.
    """

    #: 손절 주문의 client_id. 심볼별 장부 안에서만 유일하면 되므로 상수로 충분하다.
    STOP_ID = "keltner-stop"

    def __init__(self, *args, slippage_ratio: float = 0.0, **kwargs):
        """:param slippage_ratio: 조건부 시장가 체결에 얹을 슬리피지. 백테스터가
            ``fee_ratio``와 같은 규칙으로 이 값을 읽어간다."""
        super().__init__(*args, **kwargs)
        self.slippage_ratio = slippage_ratio
        self.logger = logging.getLogger(__name__)

    def _stop(self, symbol: str, quantity: float, level: float) -> Action:
        return Action(symbol, quantity,
                      order_type=ActionType.STOP_MARKET,
                      trigger_price=level,
                      reduce_only=True,
                      client_id=self.STOP_ID)

    def decide_action(self, symbol: str, candle: Candle, status: Status) -> List[Action]:
        ind = self.indicators[symbol]
        ma = ind["MA"].get_latest()
        atr = ind["ATR"].get_latest()
        if ma is None or atr is None or atr <= 0:
            return []

        price = candle.close
        position = status.position_for(symbol).position

        if position != 0:
            # 채널을 따라 손절을 옮겨 단다. 이미 체결됐다면 장부에 없으므로 취소는 무해하다.
            level = ma - self.m_exit * atr if position > 0 else ma + self.m_exit * atr
            return [Action.cancel(symbol, self.STOP_ID),
                    self._stop(symbol, -position, level)]

        upper = ma + self.m_entry * atr
        lower = ma - self.m_entry * atr
        long_stop = ma - self.m_exit * atr
        short_stop = ma + self.m_exit * atr

        if upper <= price:
            # 손절 거리는 진입가에서 실제 손절가까지. 원본은 (m_entry+m_exit)*atr로 근사했다.
            dist = price - long_stop
            if dist <= 0:
                return []
            lev = min(6.0, self.max_loss * price / dist)
            qty = trunc_by_sign(status.total_margin() / (price * (1 / lev + self.fee_ratio)), 3)
            if qty == 0:
                return []
            return [Action(symbol, qty), self._stop(symbol, -qty, long_stop)]

        if price <= lower:
            dist = short_stop - price
            if dist <= 0:
                return []
            lev = min(6.0, self.max_loss * price / dist)
            qty = trunc_by_sign(-status.total_margin() / (price * (1 / lev + self.fee_ratio)), 3)
            if qty == 0:
                return []
            return [Action(symbol, qty), self._stop(symbol, -qty, short_stop)]

        return []
