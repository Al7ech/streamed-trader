"""캔들로 체결을 판정하는 가상 실행기 — 백테스트와 드라이런이 **같은 클래스**를 쓴다.

드라이런은 백테스트와 대조하기 위해 존재한다. 그래서 체결 규칙·회계·미체결 장부가 한 벌이어야
하고, 여기가 그 한 벌이다 (:mod:`core.checks.live_check`가 "드라이런 == 백테스트"를 실제로
검증한다). 체결 판정 규칙 자체는 :mod:`core.domain.order_book`에 있다.

라이브 패키지(:mod:`core.live.trader`)가 드라이런 모드에서 이 클래스를 import한다 — 계층상
거꾸로 보이지만, "드라이런은 백테스트 코드를 그대로 돌린다"는 불변식을 import 한 줄로 드러내는
것이 의도다.
"""

import copy
from typing import Callable, Dict, Optional

from core.domain import order_book
from core.domain.action import Action, ActionType
from core.domain.candle import Candle
from core.domain.status import DEFAULT_FEE_RATIO, Status
from core.domain.trade import Trade
from core.engine.executor import Executor


class SimulatedExecutor(Executor):
    """캔들로 체결을 판정하는 가상 실행기. 백테스트와 드라이런이 공유한다.

    :param init_margin: 초기 증거금. 계좌 ``Status``는 여기서 만들어지고 이 실행기가 소유한다 —
        호출자는 ``executor.status``로 읽는다 (레코더에 넘길 때도 그 참조를 쓴다).
    :param fee_ratio: 명목가치에 곱할 수수료율. 이 값이 ``Status.fee_ratio``에 실려 회계
        (``apply_fill``)와 전략 사이징(``status.fee_ratio``)이 같은 값을 본다.
    :param slippage_ratio: 조건부 시장가(STOP_MARKET) 체결에 불리하게 얹을 비율. 순전히
        백테스트 모델링 값이라 ``Status``에 얹지 않고 여기서만 들고 있다 — 전략은 읽지 않는다.
    :param log_label: 체결 로그에 붙일 접두사. 드라이런은 ``"dry-run"``을 넘겨 실제 돈이 걸린
        체결과 구분되게 한다 (백테스트는 접두사가 없다).
    """

    def __init__(self, init_margin: float, fee_ratio: float = DEFAULT_FEE_RATIO,
                 slippage_ratio: float = 0.0,
                 on_trade: Optional[Callable[[Trade], None]] = None, log_label: str = ""):
        super().__init__(Status(margin=init_margin, fee_ratio=fee_ratio), on_trade)
        self.slippage_ratio = slippage_ratio
        #: 심볼별 최근 알려진 종가. :meth:`begin_event`가 이벤트 캔들로 갱신하고 이벤트를
        #: 넘어 유지된다 — 캔들이 없는 이벤트에서도 심볼이 직전 값을 들고 있다. 계좌 상태가
        #: 아니라 시세 캐시다. 시가평가와 MARKET 체결가(:meth:`submit`)가 읽는다.
        self.last_close: Dict[str, float] = {}
        #: 미체결 주문 제출 순서. 같은 봉 안의 체결 순서를 결정적으로 만든다.
        self._order_seq = 0
        self._prefix = f"[{log_label}] " if log_label else ""

    def begin_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        """이벤트 경계: 최근 종가 갱신 → 미체결 주문 매칭 → 열린 포지션 시가평가.

        **첫 루프** — 이벤트에 캔들이 있는 심볼마다: 최근 종가 캐시를 갱신하고, 곧바로 그
        심볼의 미체결 주문을 이 캔들로 매칭한다. ``match_symbol``이 제너레이터라 체결은 한
        건씩 즉시 반영되고, 같은 봉의 뒤쪽 주문은 갱신된 포지션을 본다 (``reduce_only`` clamp가
        올바르려면 필요하다). 만료 처리는 매칭 **뒤에** 온다 — ``expire_after_candles=N``인
        주문이 N번째 캔들에서도 체결 기회를 갖도록.

        **둘째 루프** — 열린 포지션 **전부**(이번 이벤트에 캔들이 없는 심볼 포함)를 최근
        종가로 (재)시가평가한다. 이것이 자본곡선 값이 된다 — 레코더는 단계 6에서
        ``status.total_margin()``을 읽는다. 이 루프는 첫 루프의 체결이 트리거/지정가 기준으로
        남긴 ``unrealised_pnl``을 봉 종가 기준으로 되돌리는 역할까지 겸한다 (``_fill``은
        체결가로 시가평가하므로). ``st.positions.items()``로 이미 ``pos_state``를 쥐고 있어
        ``st.update_unrealised_pnl``의 심볼 재조회를 피하고 공식을 인라인한다 — 이벤트·심볼당
        불리므로 조회 한 번이 수백만 회 쌓인다.

        미체결 매칭이 지표 갱신·``decide_action`` **앞**에 있는 이유: 실제 거래소에서는
        미체결 주문이 봉이 닫히기 전에 체결되므로, 뒤에 두면 전략이 이미 손절된 포지션을 아직
        들고 있다고 착각한 채 결정하게 된다. 라이브는 거래소가 장부를 소유하므로 이 매칭이
        없다 (:meth:`~core.engine.executor.Executor.begin_event` 기본 no-op 위의 라이브 override).
        """
        st = self.status
        for symbol, candle in candles.items():
            self.last_close[symbol] = candle.close
            for order, price, quantity in order_book.match_symbol(
                    st, symbol, candle, self.slippage_ratio):
                self._fill(symbol, quantity, price, event_time,
                           order.order_type.value, order.created_at)
            for order in order_book.tick_expiry(st, symbol):
                self.logger.info("%s미체결 주문 만료: %s", self._prefix, order)
        for symbol, pos_state in st.positions.items():
            if pos_state.position != 0.0:
                price = self.last_close.get(symbol)
                if price is not None:
                    pos_state.unrealised_pnl = pos_state.position * (price - pos_state.avg_price)

    def force_liquidation(self) -> bool:
        """시가평가 자본이 0 이하로 떨어지면 파산이다. 증거금이 공유 풀이므로 한 심볼이 자본을
        다 태우면 나머지 심볼도 전부 청산된다 (flatten 액션은 엔진이 만든다).

        장부를 여기서 비우지 않으면 파산 후에도 손절 주문이 남아, flat이 된 계좌에 나중에 유령
        포지션을 여는 체결이 생긴다.
        """
        if self.status.total_margin() > 0.0:
            return False
        cancelled = order_book.cancel_all(self.status)
        if cancelled:
            self.logger.warning("강제청산: 미체결 주문 %d건을 취소한다", len(cancelled))
        return True

    def submit(self, action: Action, event_time: int) -> None:
        """가상 실행기는 즉시 체결하거나 장부에 올린다 — 반환값이 없다.

        장부 조작(취소/등록)이 수량 검사보다 **먼저**다: CANCEL은 quantity가 0이라 뒤에 두면
        조용히 사라진다.
        """
        if action.order_type is ActionType.CANCEL:
            cancelled = order_book.cancel_orders(self.status, action.symbol, action.client_id)
            if cancelled:
                self.logger.debug("%s주문 취소: symbol=%s client_id=%s (%d건)", self._prefix,
                                  action.symbol, action.client_id or "*", len(cancelled))
            return None

        if action.is_resting:
            self._order_seq += 1
            if order_book.register_order(self.status, action, event_time, self._order_seq):
                self.logger.debug("%s미체결 주문 등록: %s", self._prefix, action)
            return None

        if action.quantity == 0:
            return None

        # 체결가는 액션의 **대상 심볼**의 마지막 알려진 종가다 — 트리거 심볼의 종가가 아니다.
        # 교차 심볼 액션이면 둘이 다르다.
        price = self.last_close.get(action.symbol)
        if price is None:
            self.logger.warning("%s가격을 알 수 없는 심볼 %s 에 대한 액션을 건너뛴다: %s",
                                self._prefix, action.symbol, action)
            return None

        self._fill(action.symbol, action.quantity, price, event_time,
                   ActionType.MARKET.value, event_time)
        return None

    def _fill(self, symbol: str, quantity: float, price: float, event_time: int,
              order_type: str, submitted_at: int) -> Trade:
        """체결 하나를 반영하고 ``on_trade``로 흘려보낸다.

        거래 전 스냅샷은 ``apply_fill`` **전에** 떠야 한다 — ``Trade.status``의 계약이고,
        승패 분류(``result_writer._win_lose_counts``)가 그 시점의 포지션 부호를 본다.

        ``apply_fill``의 청산 손익은 ``unrealised_pnl``을 안분해서 구하므로, 그 값이 **체결가
        기준**이어야 실현손익이 맞는다. 시장가는 체결가가 곧 이벤트 종가라 직전 시가평가가 이미
        그 값이지만, 지정가/조건부는 봉 중간 가격에 체결되므로 여기서 다시 매긴다 (시장가에는
        같은 값을 다시 계산하는 무해한 no-op이다).
        """
        self.status.update_unrealised_pnl(symbol, price)
        prev_status = copy.deepcopy(self.status)
        wnl, fee = self.status.apply_fill(symbol, quantity, price)
        leverage = self.status.update_leverage()
        trade = Trade(
            timestamp=event_time,
            symbol=symbol,
            quantity=quantity,
            price=price,
            wnl=wnl,
            fee=fee,
            status=prev_status,
            leverage=leverage,
            order_type=order_type,
            submitted_at=submitted_at,
        )
        if self._prefix:
            # 백테스트는 체결이 수만 건이라 로그를 남기지 않는다. 드라이런만 남긴다 —
            # 라이브 체결 로그와 나란히 읽히는 것이 이 로그의 용도다.
            self.logger.info("%s%s filled symbol=%s qty=%s @ %s wnl=%.4f fee=%.4f -> %s",
                             self._prefix, order_type, symbol, quantity, price, wnl, fee,
                             self.status)
        self.on_trade(trade)
        return trade
