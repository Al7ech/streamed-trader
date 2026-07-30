from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

import numpy as np

from core.backtest.status import Status
from core.streamer.candle import Candle


class BaseIndicator(ABC):
    """
    Base class for rolling indicators (loop-updated, one candle at a time).
    """

    #: Chart grouping for the frontend (visualise/): indicators sharing a group are plotted
    #: together on one pane/price scale. "price" (the default) means the value is a price level
    #: and overlays the candlestick pane; any other value gets its own pane.
    scale_group: str = "price"

    #: When to ingest the candle a decision is being made on.
    #:
    #: False (default) — the engine calls ``decide_action`` first and updates this indicator
    #: afterwards, so ``get_latest()`` is the value *through the previous candle* and
    #: ``get_index(-2)`` the one before that.
    #:
    #: True — the engine feeds the current candle *before* ``decide_action``, so ``get_latest()``
    #: includes the candle being decided on and ``get_index(-2)`` is the previous one. That candle
    #: has already closed by then, so this is a semantics choice, not look-ahead.
    #:
    #: Turning it on shifts every read by one index. Breakout comparisons in particular must move
    #: to ``get_index(-2)``: a Donchian max channel that includes the current bar satisfies
    #: ``channel_max >= candle.high >= candle.close``, so ``close > channel_max`` could never fire.
    #: Level-style indicators (MA, ATR, rolling std) read fine at ``get_latest()`` either way.
    #:
    #: Settable per subclass or per instance. Every indicator shipped here leaves it False.
    updates_before_decide: bool = False

    #: How many past output values stay readable through ``get_index``.
    #:
    #: Output series are bounded deques. Without a bound they grow for as long as the process
    #: runs — the live trader would leak one value per candle forever, and the reference
    #: backtester holds a value per candle per indicator over the whole history.
    #:
    #: Reads deeper than this are a programming error and raise ``IndexError`` rather than
    #: silently returning ``None``: a silent ``None`` would make the loop path and
    #: ``FastBacktester``'s ``ArrayIndicator`` (which mirrors this bound) disagree. Reads that
    #: are merely still in warm-up keep returning ``None``.
    #:
    #: The default is far deeper than anything shipped reads (the deepest is
    #: ``MomentumTimeExitStreamer``'s ``mom_lookback``). Pass ``history_size`` to the
    #: constructor when a strategy parameter can drive the read depth past it.
    history_size: int = 8192

    def __init__(self, window: int, history_size: Optional[int] = None):
        self.window = window
        if history_size is not None:
            self.history_size = history_size

    def _new_history(self) -> deque:
        """출력 시계열용 경계 있는 deque. 서브클래스는 ``super().__init__`` 뒤에 이걸로 만든다."""
        return deque(maxlen=self.history_size)

    def _read(self, values, idx: int) -> Optional[float]:
        """출력 시계열 조회의 공통 규약. 워밍업은 None, 이력 경계 밖은 IndexError.

        인덱스는 **음수만** 받는다. 음수가 아니면 루프 경로는 ``values[0]`` = 가장 오래된
        보관값을, ``FastBacktester``의 ``ArrayIndicator``는 ``seq[cursor]`` = 아직 반영되지
        않은 캔들의 값(= 룩어헤드)을 주므로 둘이 아예 다른 뜻이 된다.

        NaN은 ``None``으로 정규화한다. ``precompute_series``는 값이 없는 구간을 NaN으로
        표현하고 ``ArrayIndicator``가 그걸 ``None``으로 바꾸는데, 루프 경로가 raw NaN을
        내보내면 두 경로가 갈린다 — 게다가 ``x is None`` / ``x <= 0`` 류의 가드가 NaN을
        전부 통과시켜 조용히 전략 로직을 오염시킨다.
        """
        if idx >= 0:
            raise IndexError(
                f"{type(self).__name__}.get_index({idx}): 인덱스는 음수여야 한다 "
                f"(-1 = 최신, -2 = 직전).")
        if idx < -self.history_size:
            raise IndexError(
                f"{type(self).__name__}.get_index({idx}): 보관 이력 {self.history_size}개를 "
                f"넘는 조회다. 생성자에 history_size를 키워 넘겨라.")
        try:
            v = values[idx]
        except IndexError:
            return None
        return None if v is not None and v != v else v

    @abstractmethod
    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        """
        Update the indicator with a new candle.

        ``status`` is the pre-trade account snapshot — the same one ``decide_action`` saw for
        this candle (on an entry candle, ``status.position`` is still 0). During the live
        trader's indicator prefeed the historical status is unknown and ``None`` is passed,
        so status-aware indicators must treat ``None`` as warm-up.
        """
        pass

    @abstractmethod
    def get_index(self, idx: int) -> Optional[float]:
        """
        Return the indicator value at the given index (e.g., -1 for latest, -2 for previous, etc.)
        """
        pass

    def get_latest(self) -> Optional[float]:
        """
        Return the latest indicator value (same as get_index(-1)).
        """
        return self.get_index(-1)


class VectorizedIndicator(BaseIndicator):
    """
    Indicator that can additionally precompute its whole series from candle arrays at once.

    Subclasses must still implement `update()` so they keep working in the live trader and the
    reference backtester; `precompute_series` is the fast path used by FastBacktester.

    `precompute_series` receives no `status`: the account state depends on the trades the
    strategy makes, which is a feedback loop that can't be precomputed. An indicator that
    reads `status` must therefore be a plain `BaseIndicator` (loop path) instead.
    """

    @abstractmethod
    def precompute_series(self, open: np.ndarray, high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        """
        Compute the full indicator series for the given candle arrays in one shot.

        Element i of the returned array must equal `get_latest()` after `update()` has been
        called with candles [0..i] — warm-up positions are NaN (the loop-based indicators
        return None there).
        """
        pass
