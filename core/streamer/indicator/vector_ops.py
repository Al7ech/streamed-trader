"""지표의 ``precompute_series``가 쓰는 롤링 축약 — **루프 경로와 비트 단위로 같다**.

여기 있는 함수들의 존재 이유는 속도가 아니라 **정확성**이다. pandas의 ``rolling().mean()``은
빠르지만 루프 지표의 증분 합산과 **더하는 순서가 달라서**, 부동소수점 반올림이 갈린다
(실측: 50만 캔들 중 47.5만 개가 상대오차 최대 5.6e-15로 불일치). 그 차이는 3백만 캔들 위에서
임계값 비교를 몇 번 뒤집고, 지표값이 곧 트리거 가격인 조건부 주문에서는 ``Trade.price``에
그대로 들어간다. 그래서 예전 벡터화 대조 하네스는 상대 허용오차로 비교할 수밖에 없었다.

해법은 고정소수점이 아니라 **루프의 재귀식을 그대로 좌측 폴드로 펼치는 것**이다.
``np.cumsum``은 엄격한 순차 폴드(``c[i] = c[i-1] + x[i]``)라 파이썬 루프와 반올림까지 같다.
그 성질 위에서 각 함수는 자기 짝이 되는 ``update()``가 실제로 수행하는 연산 열을 재현한다 —
그래서 벡터화 경로와 루프 경로가 **같은 값**을 내고, 드라이런 == 백테스트 대조가 허용오차가
아니라 비트 동일로 성립한다. 그 계약은 ``core/checks/backtest_check.py`` 1절이 지표마다
확인한다.

정확도 자체를 올리려는 것이 아니라는 점이 중요하다. 루프 경로가 기준이므로, 루프가 가진
누적 반올림(상대 ~1.5e-14)을 **그대로** 재현하는 것이 옳다.
"""

import numpy as np


def rolling_mean_exact(x: np.ndarray, window: int) -> np.ndarray:
    """``s = (s - old) + new`` 증분 합산의 롤링 평균. 루프와 비트 단위로 같다.

    ``MovingAverage``/``ATRIndicator``/``VolumeMovingAverage``의 ``update()``는 창이 다 차면
    가장 오래된 값을 빼고 새 값을 더한다. 이 두 연산을 ``(-old, +new)`` 순서로 번갈아 놓은
    길이 ``2n`` 배열의 ``cumsum``이 바로 그 연산 열이다 — 홀수 자리만 뽑으면 각 캔들 시점의
    ``_sum``이 된다. 워밍업 구간에는 뺄 것이 없으므로 0.0을 놓는다 (``s + 0.0 == s``, 정확).

    :param x: 창에 넣을 값 (종가, TR, 거래량 등).
    :param window: 창 길이. 이보다 짧은 앞부분은 NaN이다 (``precompute_series`` 규약).
    """
    n = len(x)
    out = np.full(n, np.nan, dtype=np.float64)
    if n == 0 or window <= 0 or n < window:
        return out
    y = np.empty(2 * n, dtype=np.float64)
    y[1::2] = x
    y[0::2] = 0.0
    if n > window:
        y[2 * window::2] = -x[:n - window]
    sums = np.cumsum(y)[1::2]
    out[window - 1:] = sums[window - 1:] / window
    return out


def rolling_std_exact(x: np.ndarray, window: int, chunk: int = 100_000) -> np.ndarray:
    """``np.std(values, ddof=1)`` 롤링 표본표준편차. 루프와 비트 단위로 같다.

    ``RollingStd``/``VolumeRollingStd``의 ``update()``는 증분이 아니라 **창을 매번 통째로 다시
    잰다**(``np.std(self.values, ddof=1)``). 그래서 여기서도 창마다 같은 축약을 하고, 다만
    파이썬 루프 대신 슬라이딩 윈도우 뷰 위에서 한 번에 돌린다.

    청크로 끊는 것은 메모리 때문이다 — 축약이 만드는 임시 배열이 ``window * chunk``로 묶인다.
    끊는 위치는 결과에 영향을 주지 않는다 (창마다 독립적인 축약이다).
    """
    n = len(x)
    out = np.full(n, np.nan, dtype=np.float64)
    if n == 0 or window <= 0 or n < window:
        return out
    last = n - window + 1  # 창의 시작 인덱스 개수
    for start in range(0, last, chunk):
        end = min(start + chunk, last)
        view = np.lib.stride_tricks.sliding_window_view(x[start:end + window - 1], window)
        out[start + window - 1:end + window - 1] = view.std(axis=1, ddof=1)
    return out


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    """True Range 배열. 첫 캔들은 직전 종가가 없어 ``high - low``다 (루프와 같다)."""
    n = len(close)
    if n == 0:
        return np.empty(0, dtype=np.float64)
    prev_close = np.empty(n, dtype=np.float64)
    prev_close[0] = np.nan
    prev_close[1:] = close[:-1]
    tr = np.maximum(high - low,
                    np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    tr[0] = high[0] - low[0]
    return tr
