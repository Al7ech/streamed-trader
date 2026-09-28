"""캔들 시계열을 numpy 열로 들고, 순회할 때만 :class:`Candle`을 만들어 내주는 시퀀스.

``List[Candle]``은 1m 6.5년(345만 개)에 심볼당 ~1.8GB다 — 객체 헤더, ``__dict__``, 필드마다
박싱된 float/int. 게다가 fork한 자식이 그 리스트를 순회만 해도 객체마다 refcount가 바뀌어
COW 페이지가 복사되므로, 부모가 한 번 올린 캔들을 워커 여럿이 **나눠 쓸 수 없다**.
열로 들면 같은 데이터가 ~200MB이고 numpy 버퍼는 읽어도 refcount를 건드리지 않아 fork 뒤에도
공유된다. 파라미터 스윕(:mod:`core.sweep`)이 이것 때문에 존재한다.

:class:`~core.engine.backtest.BacktestEngine`은 ``List[Candle]`` 자리에 이것을 그대로 받는다
(``Sequence[Candle]`` 규약: ``len``, 인덱싱, 순회). 순회는 블록 단위 ``tolist()`` 뒤
``Candle``을 만드는 것이라 345만 개에 ~1.4초가 든다. 대신 엔진은 :meth:`ColumnarCandles.ohlcv`
로 선계산용 배열을 복사 없이 받아 ``np.fromiter`` 추출(~0.7초)을 건너뛴다.

**값은 원본과 비트 단위로 같다**: float64/int64 배열의 ``tolist()``는 원래의 파이썬
float/int를 그대로 돌려준다. ``taker_buy_volume``/``trade_count``의 ``None``은 마스크로 들고
있다가 그 자리에 ``None``으로 되돌린다 (NaN으로 뭉개지 않는다).
"""

from itertools import repeat
from typing import Iterator, List, Optional, Sequence, Tuple, Union, overload

import numpy as np

from core.candle.candle import Candle

#: 순회 시 한 번에 파이썬 객체로 바꾸는 캔들 수. ``core.engine.backtest._TOLIST_CHUNK``와 같은
#: 이유(통째 ``tolist``와 같은 속도에 메모리는 블록만큼)로 같은 값이다.
_CHUNK = 1 << 16

_BASE_FIELDS = ("open", "high", "low", "close", "volume", "start_time", "end_time")


class ColumnarCandles(Sequence[Candle]):
    """열 배열로 든 캔들 시계열. 만든 뒤에는 읽기 전용이다 (배열의 쓰기 플래그를 끈다).

    :param taker_buy_volume: 없으면 ``None`` (모든 캔들이 ``None``).
    :param taker_buy_mask: 캔들마다 ``taker_buy_volume``이 있는지. ``None``이면 전부 있다.
    :param trade_count: / :param trade_count_mask: 위와 같다.

    보통은 :meth:`from_candles`로 만든다.
    """

    def __init__(self, open: np.ndarray, high: np.ndarray, low: np.ndarray, close: np.ndarray,
                 volume: np.ndarray, start_time: np.ndarray, end_time: np.ndarray,
                 taker_buy_volume: Optional[np.ndarray] = None,
                 taker_buy_mask: Optional[np.ndarray] = None,
                 trade_count: Optional[np.ndarray] = None,
                 trade_count_mask: Optional[np.ndarray] = None):
        n = len(close)
        self.open = _frozen(open, np.float64, n, "open")
        self.high = _frozen(high, np.float64, n, "high")
        self.low = _frozen(low, np.float64, n, "low")
        self.close = _frozen(close, np.float64, n, "close")
        self.volume = _frozen(volume, np.float64, n, "volume")
        self.start_time = _frozen(start_time, np.int64, n, "start_time")
        self.end_time = _frozen(end_time, np.int64, n, "end_time")
        self.taker_buy_volume = _frozen_opt(taker_buy_volume, np.float64, n, "taker_buy_volume")
        self.taker_buy_mask = _frozen_opt(taker_buy_mask, np.bool_, n, "taker_buy_mask")
        self.trade_count = _frozen_opt(trade_count, np.int64, n, "trade_count")
        self.trade_count_mask = _frozen_opt(trade_count_mask, np.bool_, n, "trade_count_mask")

    @classmethod
    def from_candles(cls, candles: Sequence[Candle]) -> "ColumnarCandles":
        """``Candle`` 시퀀스를 열로 옮긴다. 원본 리스트는 호출자가 버려야 메모리가 준다."""
        n = len(candles)
        base = [np.fromiter((getattr(c, k) for c in candles),
                            np.int64 if k.endswith("_time") else np.float64, n)
                for k in _BASE_FIELDS]
        tbv, tbv_mask = _optional_column([c.taker_buy_volume for c in candles], np.float64)
        tc, tc_mask = _optional_column([c.trade_count for c in candles], np.int64)
        return cls(*base, taker_buy_volume=tbv, taker_buy_mask=tbv_mask,
                   trade_count=tc, trade_count_mask=tc_mask)

    # ------------------------------------------------------------------ 엔진이 쓰는 것

    def ohlcv(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """``VectorizableIndicator.compute()``에 넘길 (open, high, low, close, volume). 복사하지
        않는다 — 배열이 읽기 전용이라 지표가 실수로 고쳐 쓰면 여기서 예외가 난다."""
        return self.open, self.high, self.low, self.close, self.volume

    @property
    def nbytes(self) -> int:
        return sum(a.nbytes for a in self._arrays() if a is not None)

    # ------------------------------------------------------------------ Sequence 규약

    def __len__(self) -> int:
        return len(self.close)

    @overload
    def __getitem__(self, idx: int) -> Candle: ...

    @overload
    def __getitem__(self, idx: slice) -> "ColumnarCandles": ...

    def __getitem__(self, idx: Union[int, slice]):
        if isinstance(idx, slice):
            return ColumnarCandles(*(None if a is None else a[idx] for a in self._arrays()))
        n = len(self)
        i = idx + n if idx < 0 else idx
        if not 0 <= i < n:
            raise IndexError(f"ColumnarCandles index {idx} out of range (len={n})")
        return next(self._iter_range(i, i + 1))

    def __iter__(self) -> Iterator[Candle]:
        n = len(self)
        for start in range(0, n, _CHUNK):
            yield from self._iter_range(start, min(start + _CHUNK, n))

    def __repr__(self) -> str:
        if not len(self):
            return "ColumnarCandles(0)"
        return f"ColumnarCandles({len(self)}: {self[0]} ... {self[-1]})"

    # ------------------------------------------------------------------ 내부

    def _arrays(self) -> List[Optional[np.ndarray]]:
        """생성자 인자 순서 그대로."""
        return [self.open, self.high, self.low, self.close, self.volume, self.start_time,
                self.end_time, self.taker_buy_volume, self.taker_buy_mask, self.trade_count,
                self.trade_count_mask]

    def _iter_range(self, a: int, b: int) -> Iterator[Candle]:
        cols = [arr[a:b].tolist() for arr in (self.open, self.high, self.low, self.close,
                                              self.volume, self.start_time, self.end_time)]
        tbv = _optional_values(self.taker_buy_volume, self.taker_buy_mask, a, b)
        tc = _optional_values(self.trade_count, self.trade_count_mask, a, b)
        return map(Candle, *cols, tbv, tc)


def _frozen(arr, dtype, n: int, name: str) -> np.ndarray:
    out = np.asarray(arr, dtype=dtype)
    if out.ndim != 1 or len(out) != n:
        raise ValueError(f"ColumnarCandles.{name}: 길이 {len(out)} != {n}")
    # 호출자 배열의 플래그를 건드리지 않도록 읽기 전용 뷰를 따로 만든다.
    out = out.view()
    out.flags.writeable = False
    return out


def _frozen_opt(arr, dtype, n: int, name: str) -> Optional[np.ndarray]:
    return None if arr is None else _frozen(arr, dtype, n, name)


def _optional_column(values: List, dtype) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
    """``None``이 섞일 수 있는 필드를 (값 배열, 존재 마스크)로. 전부 None이면 (None, None),
    전부 있으면 마스크는 None."""
    present = np.fromiter((v is not None for v in values), np.bool_, len(values))
    if not present.any():
        return None, None
    col = np.fromiter((0 if v is None else v for v in values), dtype, len(values))
    return col, (None if present.all() else present)


def _optional_values(col: Optional[np.ndarray], mask: Optional[np.ndarray], a: int, b: int):
    if col is None:
        return repeat(None, b - a)
    vals = col[a:b].tolist()
    if mask is not None:
        for i in np.flatnonzero(~mask[a:b]).tolist():
            vals[i] = None
    return vals
