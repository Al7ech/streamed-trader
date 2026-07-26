from abc import ABC, abstractmethod
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

    def __init__(self, window: int):
        self.window = window

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
