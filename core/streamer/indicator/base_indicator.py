from abc import ABC, abstractmethod
from collections import deque
from typing import Callable, Optional

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

    # An indicator may additionally define `precompute_series(open, high, low, close, volume)
    # -> np.ndarray` to compute its whole series from candle arrays in one shot. The backtest
    # engine (`core/engine/backtest.py`) detects it by attribute presence
    # (`getattr(indicator, "precompute_series", None)`) and uses it instead of looping
    # `update()`. There is deliberately no default implementation here — only subclasses that
    # define it opt into vectorization. Element i of the returned array must equal
    # `get_latest()` after `update()` has been called with candles [0..i] — warm-up positions
    # are NaN (the loop-based indicators return None there), and the dtype must be float64.
    #
    # **It must be bit-identical to the loop**, not merely close. A rolling helper that sums in
    # a different order than `update()`'s incremental sum diverges by ~1e-14 relative, which over
    # millions of candles flips threshold comparisons and — for a conditional order whose trigger
    # *is* an indicator value — lands straight in `Trade.price`. `core/streamer/indicator/
    # vector_ops.py` holds helpers that unfold each loop recurrence exactly (`np.cumsum` is a
    # strict sequential fold, so it rounds like a Python loop); `core/checks/backtest_check.py`
    # asserts the equality per indicator. That is what lets a dry run and a backtest of the same
    # candles stay bit-identical.
    #
    # `precompute_series` receives no `status`: the account state depends on the trades the
    # strategy makes, which is a feedback loop that can't be precomputed. An indicator that reads
    # `status` must therefore stay loop-only (no `precompute_series`).
    #
    # Its counterpart is `NumericIndicator.precomputed_sink()` below — where the precomputed
    # values go in place of `update()`. Both are needed for an indicator to be vectorized.


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

    def precomputed_sink(self) -> Callable[[Optional[float]], None]:
        """미리 계산한 출력값을 ``update()`` 대신 얹을 자리 — 벡터화 백테스트 전용.

        ``precompute_series``의 **짝**이다 (위 규약 참고). 이벤트마다 지표마다 불리는 자리라
        매번 호출되는 메서드가 아니라 **한 번만 묶어 두고 쓰는** 호출 가능 객체를 돌려준다 —
        1m 6.5년 백테스트에서 지표당 345만 번이라 파이썬 프레임 하나가 초 단위로 쌓인다.

        출력 deque에 그대로 얹으므로 ``read``/``get_latest``의 규약(음수 인덱스, 보관 이력
        ``IndexError``, NaN→None)이 루프 경로와 **같은 코드**로 유지된다. 지표 객체를 다른
        것으로 갈아끼우지 않는 것도 요점이다: 샤드 라이터가 생성 시점에 묶어 둔
        ``get_latest``와 전략이 들고 있는 참조가 그대로 이 객체를 가리켜야 한다.

        캔들 하나에 값 하나를 얹는다 — 워밍업 구간의 NaN도 포함이다. 루프 경로는 그 구간에
        아무것도 얹지 않거나(``MovingAverage``) ``None``을 얹어(돈치안) deque 길이가 다를 수
        있지만, ``read``의 답은 같다: 두 deque가 **끝에서 정렬**돼 있고 NaN도 짧은 deque의
        빈자리도 똑같이 ``None``으로 읽히며, 보관 이력 경계도 같은 자리에서 걸린다.

        지표 인스턴스는 (심볼, 이름) 쌍마다 하나여야 한다. 두 심볼이 한 인스턴스를 공유하면
        여기로도 값이 두 번 들어온다 — 루프 경로에서 ``update``가 두 번 불리는 것과 같은
        기존 제약이다.
        """
        return self._deque.append

    def _read_series(self, values, idx: int, history_size: int) -> Optional[float]:
        """출력 시계열 조회의 공통 규약. 워밍업은 None, 이력 경계 밖은 IndexError.

        ``self._deque`` 하나뿐 아니라, 시계열을 추가로 갖는 서브클래스(예:
        ``SupertrendIndicator``의 방향 값)도 그 deque와 history_size를 넘겨 그대로
        재사용한다 — ``history_size``를 ``self``에서 읽지 않고 인자로 받는 이유다.

        인덱스는 **음수만** 받는다. 음수가 아니면 deque 조회는 ``values[0]`` = 가장 오래된
        보관값을 조용히 돌려줘 "최신"을 뜻하는 ``-1``과 정반대가 된다 (벡터화 백테스트 경로가
        재도입되면 그쪽 array 조회는 아직 반영되지 않은 캔들 = 룩어헤드를 주므로 더 나쁘다).

        NaN은 ``None``으로 정규화한다. ``precompute_series``를 정의한 지표는 값이 없는 구간을
        NaN으로 표현하는데, 루프 경로가 raw NaN을 그대로 내보내면 ``x is None`` / ``x <= 0``
        류의 가드가 NaN을 전부 통과시켜 조용히 전략 로직을 오염시킨다.
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
