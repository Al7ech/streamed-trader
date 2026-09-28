"""심볼별 거래 규칙 — 수량 단위(stepSize), 최소 수량, 최소 명목가치, 가격 단위(tickSize).

책임 분담이 이 모듈의 요점이다:

* **양자화는 전략 몫이다.** 전략은 ``status.rules_for(symbol)``로 규칙을 읽어
  :meth:`SymbolRules.floor_qty` / :meth:`SymbolRules.round_price`로 주문 수량·가격을 맞춘다.
  실행기가 대신 고쳐주면 "내가 낸 주문이 그대로 나간다"는 전제가 깨지고, 양자화를 빠뜨린
  전략이 조용히 숨는다.
* **실행기는 검증만 한다.** :meth:`SymbolRules.check`가 위반 사유를 돌려주면 실행기는 그
  주문을 거부한다 — 거래소가 하는 일과 같다. 가상 실행기와 라이브 실행기가 **이 함수 하나**를
  쓰므로, 백테스트에서 통과한 주문은 라이브에서도 통과하고 백테스트에서 거부된 주문은 라이브에
  나가지 않는다.
* **체결 후 포지션 정규화(:meth:`SymbolRules.normalize_qty`)는 회계 몫이다.** 전략이 0.1,
  0.1, -0.3을 전부 정확히 양자화해 내도 float 덧셈은 -0.09999999999999998을 만든다. 그래서
  ``apply_fill``이 결과 포지션을 단위 자릿수로 반올림한다. ``round``는 가장 가까운 float를
  돌려주므로 십진수로 같은 값은 항상 같은 float가 되고, ``position == 0``·``-position`` 청산이
  정확해진다.

값은 ``core.order.symbol_rules_data``에 하드코딩돼 있다 (Binance USD-M ``exchangeInfo``에서
생성: ``uv run python -m core.fetcher.binance.exchange_info``). 규칙 이력은 거래소가 주지
않으므로 과거 구간 백테스트에도 오늘의 규칙이 적용된다.
"""

import math
from collections.abc import Mapping
from decimal import Decimal
from typing import Dict, Iterator, Optional, Tuple

from core.order.action import Action, ActionType

_CANCEL, _MARKET, _LIMIT = ActionType.CANCEL, ActionType.MARKET, ActionType.LIMIT

#: 절삭 전 반올림 자릿수(단위 개수 기준). ``4.35 * 100 == 434.99999999999994`` 같은
#: 곱셈 오차가 한 단위를 통째로 깎지 않도록, 내림 전에 이만큼만 반올림한다.
_FLOOR_GUARD_DIGITS = 6


def _decompose(unit: str) -> Tuple[int, int]:
    """십진 문자열 단위를 ``m × 10^-nd``로 분해한다. ``"0.001"`` → ``(3, 1)``, ``"0.0005"`` → ``(4, 5)``.

    ``nd``는 음수일 수 있다 (``"10"`` → ``(-1, 1)``).
    """
    d = Decimal(unit).normalize()
    if d <= 0:
        raise ValueError(f"단위는 양수여야 한다: {unit!r}")
    sign, digits, exponent = d.as_tuple()
    m = int("".join(map(str, digits)))
    return -exponent, m


class _Unit:
    """한 단위(step 또는 tick)의 계산. 매 호출 Decimal을 쓰지 않도록 분해 결과를 들고 있다."""

    __slots__ = ("text", "nd", "m", "_scale", "_inv")

    def __init__(self, text: str):
        self.text = text
        self.nd, self.m = _decompose(text)
        # 정수 k를 10**nd로 **나누면** 결과가 k·10^-nd 리터럴과 같은 float다 (IEEE 나눗셈은
        # 정확 반올림). nd가 음수면 곱한다.
        self._scale = 10 ** self.nd if self.nd >= 0 else None
        self._inv = 10 ** (-self.nd) if self.nd < 0 else None

    def _to_units(self, x: float) -> float:
        return x * self._scale if self._scale is not None else x / self._inv

    def _from_units(self, k: int) -> float:
        return k / self._scale if self._scale is not None else float(k * self._inv)

    def floor(self, x: float) -> float:
        """0 방향으로 단위 배수에 맞춘다."""
        k = math.floor(round(self._to_units(abs(x)), _FLOOR_GUARD_DIGITS))
        k -= k % self.m
        return math.copysign(self._from_units(k), x) if k else 0.0

    def nearest(self, x: float) -> float:
        """가장 가까운 단위 배수."""
        scale = self._scale
        if self.m == 1 and scale is not None:
            # 10의 거듭제곱 단위(현재 Binance 전 심볼)는 정수 반올림 한 번이면 된다. 매 봉 가격을
            # 다시 거는 전략의 핫패스라 산술과 호출을 아낀다 (``round(x, nd)``는 내부에서 십진
            # 문자열 변환을 거쳐 이보다 몇 배 느리다).
            k = round(x * scale)
            return k / scale if k else 0.0
        else:
            k = round(round(self._to_units(x), _FLOOR_GUARD_DIGITS) / self.m) * self.m
        return self._from_units(k) if k else 0.0

    def is_multiple(self, x: float) -> bool:
        """``x``가 정확히 이 단위의 배수를 나타내는 float인가.

        가장 가까운 격자점 ``k``를 구해 그 **정규 float**(``k``를 정확 반올림 나눗셈으로 되돌린
        값)와 비트 단위로 같은지 본다. ``0.30000000000000004``처럼 격자에서 부동소수점 잔여만큼
        벗어난 값은 여기서 걸린다 — ``round(x, nd) == x``와 같은 판정이지만 그보다 몇 배 빠르다
        (주문 검증마다 불린다).
        """
        scale = self._scale
        if scale is not None:  # 핫패스: 헬퍼 호출 없이 인라인
            k = round(x * scale)
            return k / scale == x and (self.m == 1 or k % self.m == 0)
        k = round(x / self._inv)
        return float(k * self._inv) == x and k % self.m == 0


def _fast_scale(unit: _Unit) -> Optional[int]:
    return unit._scale if unit.m == 1 else None


class SymbolRules:
    """한 심볼의 거래 규칙. 값은 거래소가 준 십진 문자열 그대로 받는다.

    :param step: 지정가·조건부 주문의 수량 단위 (``LOT_SIZE.stepSize``).
    :param min_qty: 지정가·조건부 주문의 최소 수량 (``LOT_SIZE.minQty``).
    :param market_step: 시장가 주문의 수량 단위 (``MARKET_LOT_SIZE.stepSize``).
    :param market_min_qty: 시장가 주문의 최소 수량 (``MARKET_LOT_SIZE.minQty``).
    :param min_notional: 최소 명목가치 (``MIN_NOTIONAL.notional``). 노출을 늘리는 주문에만
        적용된다 — ``reduce_only``이거나 순수 청산인 주문은 면제 (거래소도 reduce-only는 면제한다).
    :param tick: 가격 단위 (``PRICE_FILTER.tickSize``).
    """

    __slots__ = ("_raw", "_step", "_min_qty", "_market_step", "_market_min_qty", "min_notional",
                 "_tick", "_position_nd", "_step_fast", "_market_step_fast", "_tick_fast")

    def __init__(self, step: str, min_qty: str, market_step: str, market_min_qty: str,
                 min_notional: str, tick: str):
        self._raw = (step, min_qty, market_step, market_min_qty, min_notional, tick)
        self._step = _Unit(step)
        self._min_qty = float(min_qty)
        self._market_step = _Unit(market_step)
        self._market_min_qty = float(market_min_qty)
        self.min_notional = float(min_notional)
        self._tick = _Unit(tick)
        # 포지션은 두 수량 단위 중 더 가는 쪽 격자에 있다 — 그 자릿수로 정규화한다.
        self._position_nd = max(self._step.nd, self._market_step.nd)
        # check()의 핫패스용: 10^-nd 단위(nd >= 0)면 그 배율, 아니면 None (일반 경로로 간다).
        self._step_fast = _fast_scale(self._step)
        self._market_step_fast = _fast_scale(self._market_step)
        self._tick_fast = _fast_scale(self._tick)

    def as_tuple(self) -> Tuple[str, ...]:
        """받은 십진 문자열 그대로 — 생성 파일(``symbol_rules_data``)의 값과 같은 모양."""
        return self._raw

    @property
    def step(self) -> float:
        return float(self._step.text)

    @property
    def tick(self) -> float:
        return float(self._tick.text)

    # ------------------------------------------------------------- 전략용

    def floor_qty(self, quantity: float, market: bool = True) -> float:
        """수량을 0 방향으로 단위 배수에 맞춘다 (부호 유지). 최소 수량 검사는 하지 않는다 —
        :meth:`meets_min`으로 따로 확인한다."""
        return (self._market_step if market else self._step).floor(quantity)

    def round_price(self, price: float) -> float:
        """가격을 가장 가까운 tick 배수로 맞춘다. 지정가·트리거 가격에 쓴다."""
        return self._tick.nearest(price)

    def meets_min(self, quantity: float, price: float, market: bool = True) -> bool:
        """노출을 늘리는 주문으로서 최소 수량·최소 명목가치를 만족하는가."""
        min_qty = self._market_min_qty if market else self._min_qty
        return abs(quantity) >= min_qty and abs(quantity) * price >= self.min_notional

    # ------------------------------------------------------------- 실행기용

    def normalize_qty(self, quantity: float) -> float:
        """체결 회계가 만든 포지션을 수량 격자 위의 float로 되돌린다 (부동소수점 잔여 제거)."""
        q = round(quantity, self._position_nd)
        return q if q else 0.0  # -0.0을 0.0으로

    def check(self, action: Action, position: float,
              ref_price: Optional[float]) -> Optional[str]:
        """주문이 규칙을 지키는가. 위반이면 사유 문자열, 통과면 ``None``.

        :param position: 이 심볼의 현재 signed 포지션 (순수 청산 판정용).
        :param ref_price: 명목가치 기준가. 시장가는 최근 종가, 지정가는 ``price``,
            조건부는 ``trigger_price``를 호출자가 넘긴다. ``None``이면 명목가치 검사를 건너뛴다.
        """
        # 매 봉 주문을 다시 거는 전략에서는 봉마다 불린다 — 10의 거듭제곱 단위(현재 Binance 전
        # 심볼)는 _Unit 메서드를 거치지 않고 여기서 바로 판정한다 (_Unit.is_multiple과 같은 식).
        order_type = action.order_type
        if order_type is _CANCEL:
            return None
        q = action.quantity
        if order_type is _MARKET:
            unit, fast, min_qty = self._market_step, self._market_step_fast, self._market_min_qty
        else:
            unit, fast, min_qty = self._step, self._step_fast, self._min_qty
            price = action.price if order_type is _LIMIT else action.trigger_price
            tf = self._tick_fast
            if not (round(price * tf) / tf == price if tf is not None
                    else self._tick.is_multiple(price)):
                kind = "지정가" if order_type is _LIMIT else "트리거"
                return f"{kind} {price!r}가 tick {self._tick.text}의 배수가 아니다"
        if not (round(q * fast) / fast == q if fast is not None else unit.is_multiple(q)):
            return f"수량 {q!r}이 단위 {unit.text}의 배수가 아니다"
        aq = abs(q)
        if aq < min_qty:
            return f"수량 {q!r}이 최소 수량 {min_qty!r} 미만이다"
        if ref_price is not None and not action.reduce_only and not is_pure_reduction(q, position):
            notional = aq * ref_price
            if notional < self.min_notional:
                return f"명목가치 {notional:.4f}가 최소 {self.min_notional!r} 미만이다"
        return None

    def __eq__(self, other):
        # 문자열이 아니라 값으로 비교한다 — "0.001"과 "0.00100000"은 같은 규칙이다.
        return (isinstance(other, SymbolRules)
                and all(Decimal(a) == Decimal(b) for a, b in zip(self._raw, other._raw)))

    __hash__ = None

    def __repr__(self):
        return (f"SymbolRules(step={self._step.text}, min_qty={self._min_qty}, "
                f"market_step={self._market_step.text}, market_min_qty={self._market_min_qty}, "
                f"min_notional={self.min_notional}, tick={self._tick.text})")


def is_pure_reduction(quantity: float, position: float) -> bool:
    """포지션을 줄이기만 하는 수량인가 (반대 부호이고 포지션 크기 이하 — 반전 없음)."""
    return position != 0.0 and (quantity > 0) != (position > 0) and abs(quantity) <= abs(position)


class _LazyRules(Mapping):
    """생성 파일의 문자열 튜플을 처음 조회할 때 :class:`SymbolRules`로 만들어 캐시한다.

    수백 개 심볼 전부를 import 시점에 만들 이유가 없다 — 한 런이 쓰는 건 몇 개다.
    """

    def __init__(self, raw: Dict[str, Tuple[str, ...]]):
        self._raw = raw
        self._built: Dict[str, SymbolRules] = {}

    def __getitem__(self, symbol: str) -> SymbolRules:
        rules = self._built.get(symbol)
        if rules is None:
            rules = self._built[symbol] = SymbolRules(*self._raw[symbol])
        return rules

    def __contains__(self, symbol) -> bool:
        return symbol in self._raw

    def __iter__(self) -> Iterator[str]:
        return iter(self._raw)

    def __len__(self) -> int:
        return len(self._raw)


def _load_default() -> Mapping:
    from core.order.symbol_rules_data import RULES
    return _LazyRules(RULES)


#: 하드코딩된 Binance USD-M 전 심볼 규칙. ``Status``가 ``symbol_rules``를 받지 않으면 이걸 쓴다.
DEFAULT_RULES: Mapping = _load_default()
