from typing import Optional

from core.backtest.status import Status
from core.streamer.candle import Candle
from core.streamer.indicator.base_indicator import BaseIndicator


class PivotTrendlineIndicator(BaseIndicator):
    """
    프랙탈 피벗 2개를 이은 사선 추세선.

    - 피벗 고점(mode="high"): x[c] > max(x[c-k..c-1]) AND x[c] >= max(x[c+1..c+k]) —
      c+k 봉을 ingest한 시점에서야 확정된다 (look-ahead 없음). 저점은 대칭.
    - `get_latest()`는 방금 ingest한 봉에서의 선값(가격, float) — 피벗이 2개 미만이면 None.
      봉당 기울기와 피벗페어 ID(새 피벗 확정마다 증가, 같은 ID = 같은 선)는 `slope`/`pair_id`
      속성으로 노출한다. 시리즈 저장이 float 컬럼을 기대하므로 값은 선값만 반환한다.

    경로 의존(확정 지연 + 최근 2개 선택)이라 plain BaseIndicator로 둔다
    (FastBacktester가 루프 경로로 처리).
    """

    scale_group = "price"

    def __init__(self, window: int, mode: str = "high"):
        super().__init__(window)
        assert mode in ("high", "low")
        self.mode = mode
        self._vals = []                     # 봉별 high(또는 low) 전체 이력
        self._p1 = self._p2 = -1            # 최근 피벗 2개의 봉 인덱스
        self._y1 = self._y2 = 0.0
        self._latest: Optional[float] = None
        self.slope: Optional[float] = None
        self.pair_id: int = 0

    def update(self, candle: Candle, status: Optional[Status] = None) -> None:
        k = self.window
        vals = self._vals
        vals.append(candle.high if self.mode == "high" else candle.low)
        i = len(vals) - 1

        if i >= 2 * k:
            c = i - k                       # 이번 봉에서 확정 여부가 판가름나는 중심 봉
            x = vals[c]
            if self.mode == "high":
                if x > max(vals[c - k:c]) and x >= max(vals[c + 1:i + 1]):
                    self._push_pivot(c, x)
            else:
                if x < min(vals[c - k:c]) and x <= min(vals[c + 1:i + 1]):
                    self._push_pivot(c, x)

        if self.pair_id >= 2:
            self.slope = (self._y2 - self._y1) / (self._p2 - self._p1)
            self._latest = self._y2 + self.slope * (i - self._p2)

    def _push_pivot(self, idx: int, val: float) -> None:
        self._p1, self._y1 = self._p2, self._y2
        self._p2, self._y2 = idx, val
        self.pair_id += 1

    def get_index(self, idx: int) -> Optional[float]:
        # 경신 빈도가 낮아 과거 이력은 유지하지 않는다 — 최신값만 지원.
        assert idx == -1, "PivotTrendlineIndicator supports only get_index(-1)"
        return self._latest
