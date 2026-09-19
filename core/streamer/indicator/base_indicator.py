from abc import ABC, abstractmethod
from collections import deque
from typing import Callable, Optional

import numpy as np

from core.candle.candle import Candle
from core.account.status import Status


class BaseIndicator(ABC):
    """
    Base contract for every indicator: feed it a candle via ``update``, read a past value back
    via ``read(idx)``. How (or whether) a value gets stored is entirely up to the subclass —
    see ``NumericIndicator`` for the standard single-deque implementation, and
    ``SupertrendIndicator``/``PivotTrendlineIndicator`` for subclasses with their own storage.
    """

    #: Chart grouping for the frontend (visualise/): indicators sharing a group are plotted
    #: together on one pane/price scale. "price" (the default) means the value is a price level
    #: and overlays the candlestick pane; any other value gets its own pane.
    scale_group: str = "price"

    @abstractmethod
    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        """
        Update the indicator with a new candle.

        The engine always calls this — for **every** symbol in the event — **before**
        ``decide_action`` runs for that event, so ``get_latest()`` includes the candle being
        decided on and ``read(-2)`` is the previous one. That candle has already closed by then, so this is a semantics choice, not
        look-ahead — but it does mean breakout/extremum comparisons must read ``read(-2)``
        instead of ``get_latest()``: a Donchian max channel that included the current bar would
        satisfy ``channel_max >= candle.high >= candle.close``, so ``close > channel_max`` could
        never fire. Level-style indicators (MA, ATR, rolling std) read fine at ``get_latest()``.

        ``status`` is the pre-trade account snapshot — the same one ``decide_action`` sees for
        this event (on an entry candle, ``status.position_for(symbol).position`` is still 0).
        During the live trader's indicator prefeed the historical status is unknown and ``None``
        is passed, so status-aware indicators must treat ``None`` as warm-up.
        """
        pass

    @abstractmethod
    def read(self, idx: int) -> Optional[float]:
        """
        Return the indicator value at the given index (e.g., -1 for latest, -2 for previous, etc.)
        """
        pass

    def get_latest(self) -> Optional[float]:
        """
        Return the latest indicator value (same as read(-1)).
        """
        return self.read(-1)

    # An indicator may additionally opt into the backtest engine's vectorized path by inheriting
    # `VectorizableNumericIndicator` (below `NumericIndicator` in this file) instead of plain
    # `NumericIndicator`. `core/engine/backtest.py` detects that by `isinstance(indicator,
    # VectorizableIndicator)` — see `VectorizableIndicator`'s docstring for the `compute`
    # contract (bit-identity requirement, why an indicator that reads `status` can't vectorize)
    # and `VectorizableNumericIndicator`'s for `sink`.


class NumericIndicator(BaseIndicator):
    """
    ``update()``가 계산한 스칼라 값의 이력을 경계 있는 deque에 쌓고 ``read(idx)``로 되읽는
    표준 구현체. 대부분의 지표는 이 클래스를 상속해 ``update()``만 오버라이드하면 된다
    (계산한 값을 ``self._deque.append(...)``에 쌓는다).

    ``update()``는 여전히 추상 메서드다 — 값을 어떻게 계산하는지는 지표마다 다르므로 여기서
    채울 수 없다. 시계열을 두 개 이상 갖는 지표(``SupertrendIndicator``)도 이 클래스를
    상속한다 — 주 출력(예: 라인 값)은 상속받은 ``self._deque``/``read()``를 그대로 쓰고,
    나머지 출력은 자기 소유 deque와 ``self._read_series(...)`` 재사용으로 추가한다
    (``MinDonchianIndicator``가 상속받은 출력 deque 외에 자기만의 작업용 deque
    ``min_deque``를 따로 갖는 것과 같은 모양).
    """

    #: How many past output values stay readable through ``read``.
    #:
    #: Output series are bounded deques. Without a bound they grow for as long as the process
    #: runs — the live trader would leak one value per candle forever, and the reference
    #: backtester holds a value per candle per indicator over the whole history.
    #:
    #: Reads deeper than this are a programming error and raise ``IndexError`` rather than
    #: silently returning ``None``: a silent ``None`` would let a read past the retained history
    #: pass unnoticed (and would diverge from any array-backed fast path that mirrors this bound
    #: if the vectorized backtest path is reintroduced). Reads that are merely still in warm-up
    #: keep returning ``None``.
    #:
    #: The default is far deeper than anything shipped reads (the deepest is
    #: ``MomentumTimeExitStreamer``'s ``mom_lookback``). Pass ``history_size`` to the
    #: constructor when a strategy parameter can drive the read depth past it.
    history_size: int = 8192

    def __init__(self, history_size: Optional[int] = None):
        if history_size is not None:
            self.history_size = history_size
        self._deque = deque(maxlen=self.history_size)

    def read(self, idx: int) -> Optional[float]:
        return self._read_series(self._deque, idx, self.history_size)

    def get_latest(self) -> Optional[float]:
        """``read(-1)``과 같은 값을 프레임 하나로 돌려준다.

        레코더가 이벤트마다 지표마다 부르는 경로라 ``get_latest → read → _read_series`` 세
        프레임이 1m 6.5년 백테스트에서만 초 단위로 쌓였다. ``-1``은 음수이고 보관 이력 안이라
        인덱스 검사는 필요 없고, 워밍업 None / NaN → None 규약은 :meth:`_read_series`와 같다.
        """
        d = self._deque
        if not d:
            return None
        v = d[-1]
        return None if v is not None and v != v else v

    def _read_series(self, values, idx: int, history_size: int) -> Optional[float]:
        """출력 시계열 조회의 공통 규약. 워밍업은 None, 이력 경계 밖은 IndexError.

        ``self._deque`` 하나뿐 아니라, 시계열을 추가로 갖는 서브클래스(예:
        ``SupertrendIndicator``의 방향 값)도 그 deque와 history_size를 넘겨 그대로
        재사용한다 — ``history_size``를 ``self``에서 읽지 않고 인자로 받는 이유다.

        인덱스는 **음수만** 받는다. 음수가 아니면 deque 조회는 ``values[0]`` = 가장 오래된
        보관값을 조용히 돌려줘 "최신"을 뜻하는 ``-1``과 정반대가 된다.

        NaN은 ``None``으로 정규화한다. ``VectorizableNumericIndicator.compute()``를 정의한
        지표는 값이 없는 구간을 NaN으로 표현하는데, 루프 경로가 raw NaN을 그대로 내보내면
        ``x is None`` / ``x <= 0`` 류의 가드가 NaN을 전부 통과시켜 조용히 전략 로직을
        오염시킨다.
        """
        if idx >= 0:
            raise IndexError(
                f"{type(self).__name__}.read({idx}): 인덱스는 음수여야 한다 "
                f"(-1 = 최신, -2 = 직전).")
        if idx < -history_size:
            raise IndexError(
                f"{type(self).__name__}.read({idx}): 보관 이력 {history_size}개를 "
                f"넘는 조회다. 생성자에 history_size를 키워 넘겨라.")
        try:
            v = values[idx]
        except IndexError:
            return None
        return None if v is not None and v != v else v


class VectorizableIndicator(ABC):
    """"이 지표는 전 구간을 한 번에 계산할 수 있다"는 것만 표시하는 얇은 능력 마커.

    ``NumericIndicator``와는 독립적이다 — 저장 방식(deque든 다른 무엇이든)에 대해 아무 것도
    가정하지 않는다. ``BacktestEngine``이 벡터화 대상을 판정할 때 ``isinstance(indicator,
    VectorizableIndicator)`` 하나로 체크하는 게 이 클래스가 존재하는 이유다 — "무엇으로
    만들어졌는가"가 아니라 "전 구간을 미리 계산해 낼 수 있는가"를 직접 묻는다.

    실제로 쓰는 지표는 이 클래스 단독이 아니라 ``VectorizableNumericIndicator``(아래, 이
    클래스와 ``NumericIndicator``를 다중상속)를 통해 결합된다 — 값을 계산하는 것
    (``compute``)과 그 값을 어디에 꽂는지(``sink``)는 서로 다른 관심사라 나눴다: 계산은
    저장 방식과 무관하지만, 주입은 저장 방식(``NumericIndicator``의 ``self._deque``)을
    알아야 한다.
    """

    @abstractmethod
    def compute(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        """심볼의 OHLCV 전 구간에서 이 지표의 전체 시계열을 한 번에 계산한다.

        엘리먼트 i는 캔들 [0..i]까지로 ``update()``를 부른 뒤의 ``get_latest()``와 **비트
        단위로 같아야 한다** — 워밍업 구간은 NaN, dtype은 ``float64``. 단순히 근접해서는
        안 된다: ``update()``가 도는 증분 합산과 다른 순서로 더하는 롤링 헬퍼는 ~1e-14
        상대오차로 갈리는데, 수백만 캔들에 걸쳐 이게 임계값 비교를 뒤집고 — 트리거가
        지표값 자체인 조건부 주문에서는 곧바로 ``Trade.price``로 번진다.
        ``core/streamer/indicator/vector_ops.py``가 각 루프 재귀식을 그대로 펼치는 헬퍼를
        갖고 있고(``np.cumsum``이 엄격한 순차 폴드라 파이썬 루프와 반올림까지 같다),
        ``core/checks/backtest_check.py``가 지표마다 이 동등성을 검사한다.

        ``status``를 받지 않는다 — 계좌 상태는 전략 자신의 거래에 좌우되는 피드백 루프라
        미리 계산할 수 없다. ``status``를 읽는 지표는 이 클래스를 상속하지 않고 루프
        전용으로 남아야 한다.
        """
        pass


class VectorizableNumericIndicator(NumericIndicator, VectorizableIndicator):
    """``NumericIndicator``(저장) + ``VectorizableIndicator``(계산 가능 표시)를 잇는 결합체.

    루프 경로(``update``/``read``/``get_latest``)는 ``NumericIndicator``에서, "전 구간을
    미리 계산할 수 있다"는 계약(``compute``)은 ``VectorizableIndicator``에서 상속받는다.
    이 클래스가 추가하는 건 그 둘을 실제로 잇는 ``sink()`` 하나뿐이다.

    ``sink() -> Callable[[Optional[float]], None]``: 미리 계산한 값을 ``update()`` 대신
    얹을 자리. ``self._deque.append``를 그대로 리턴한다 — 별도 저장소를 두지 않고 루프
    경로와 **같은 deque**를 공유하는 게 핵심이다. 이벤트마다 지표마다 불리는 자리라(1m
    6.5년 백테스트에서 지표당 345만 번) 매번 새 콜러블을 만드는 대신 **한 번만 묶어 두고
    재사용**하도록 백테스트 엔진이 호출한다 — 파이썬 프레임 하나를 아낀다. 출력 deque에
    그대로 얹으므로 ``read``/``get_latest``의 규약(음수 인덱스, 보관 이력 ``IndexError``,
    NaN→None)이 루프 경로와 **같은 코드**로 유지된다. 지표 객체를 다른 것으로 갈아끼우지
    않는 것도 요점이다: 샤드 라이터가 생성 시점에 묶어 둔 ``get_latest``와 전략이 들고
    있는 참조가 그대로 이 객체를 가리켜야 한다.

    배열+커서로 값을 따로 들고 ``read()``가 그걸 직접 인덱싱하게 만드는 대안도 검토했지만
    기각했다: numpy 스칼라를 읽을 때마다 박싱 비용이 붙어(실측 deque 대비 +14~150%, 재사용
    경로에 따라) ``sink``가 아끼려는 것보다 더 크게 손해를 본다 — deque 하나 공유가
    실측으로도 더 빠르다.

    캔들 하나에 값 하나를 얹는다 — 워밍업 구간의 NaN도 포함이다. 루프 경로는 그 구간에
    아무것도 얹지 않거나(``MovingAverage``) ``None``을 얹어(돈치안) deque 길이가 다를 수
    있지만, ``read``의 답은 같다: 두 deque가 **끝에서 정렬**돼 있고 NaN도 짧은 deque의
    빈자리도 똑같이 ``None``으로 읽히며, 보관 이력 경계도 같은 자리에서 걸린다.

    지표 인스턴스는 (심볼, 이름) 쌍마다 하나여야 한다. 두 심볼이 한 인스턴스를 공유하면
    여기로도 값이 두 번 들어온다 — 루프 경로에서 ``update``가 두 번 불리는 것과 같은 기존
    제약이다.
    """

    def sink(self) -> Callable[[Optional[float]], None]:
        return self._deque.append
