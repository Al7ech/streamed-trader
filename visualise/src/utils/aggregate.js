// 타임프레임 다운스케일링(1m → 5m/1h/1d) 유틸.
//
// 로드된 데이터는 1분봉이라고 가정한다. 여기 함수들은 이미 병합된 시계열(SeriesLoader 가
// shard 를 시간순으로 이어붙인 결과)을 받아 버킷 단위로 집계한다. 버킷 경계
// (floor(time/iv)*iv)는 월별 shard 안에서 깔끔히 나눠떨어지므로(5m/1h/1d 모두 하루/월을
// 균등 분할) shard 경계를 넘어 잘못 집계될 일이 없다.

export const TIMEFRAMES = [
  {label: '1m', seconds: 60},
  {label: '5m', seconds: 300},
  {label: '1h', seconds: 3600},
  {label: '1d', seconds: 86400},
];

/**
 * coarse 타임프레임(1h 이상) 여부. coarse 도 사용자가 보는/클릭한 구간의 shard 만
 * lazy 로 집계 로드하되, 집계본이 작아서(월당 1d≈31개, 1h≈744개) evict 없이 영구 캐시한다.
 */
export const isCoarse = (intervalSec) => intervalSec >= 3600;

/** ms 타임스탬프 → 초 (lightweight-charts 의 time 단위). */
export const toSeconds = (ms) => Math.floor(ms / 1000);

/** time(초) 을 intervalSec 버킷 시작 시각으로 스냅한다. */
export const bucketTime = (timeSec, intervalSec) =>
  Math.floor(timeSec / intervalSec) * intervalSec;

/**
 * 1분봉 배열을 intervalSec 타임프레임으로 집계한다.
 * open=버킷 첫 봉, close=버킷 마지막 봉, high=최대, low=최소.
 * @param candles 시간 오름차순 정렬된 {time, open, high, low, close} 배열
 */
export function aggregateCandles(candles, intervalSec) {
  if (intervalSec <= 60 || candles.length === 0) return candles;

  const out = [];
  let b = null;
  for (const c of candles) {
    const bt = bucketTime(c.time, intervalSec);
    if (!b || b.time !== bt) {
      if (b) out.push(b);
      b = {time: bt, open: c.open, high: c.high, low: c.low, close: c.close};
    } else {
      if (c.high > b.high) b.high = c.high;
      if (c.low < b.low) b.low = c.low;
      b.close = c.close;
    }
  }
  if (b) out.push(b);
  return out;
}

/**
 * 인디케이터 라인 포인트를 intervalSec 타임프레임으로 집계한다.
 * 지표는 step-line 값이므로 버킷 내 마지막 값을 사용한다.
 * @param points 시간 오름차순 정렬된 {time, value} 배열
 */
export function aggregateLine(points, intervalSec) {
  if (intervalSec <= 60 || points.length === 0) return points;

  const out = [];
  let bt = null;
  for (const p of points) {
    const k = bucketTime(p.time, intervalSec);
    if (k !== bt) {
      out.push({time: k, value: p.value});
      bt = k;
    } else {
      out[out.length - 1].value = p.value;
    }
  }
  return out;
}

/**
 * aggregateCandles 와 동일한 결과를, shard 의 원본 컬럼형 배열(time/open/high/low/close,
 * ms 단위)에서 직접 만든다. seriesLoader.parseShard 가 만드는 캔들당 객체({time,open,...})를
 * 거치지 않으므로 — coarse 집계에만 필요한 44,640행짜리 중간 객체 배열(shard당 수십MB)을
 * 아예 만들지 않는다. parseShard 의 유효성 필터(o/h/l/c 모두 > 0)와 동일하게 적용한다.
 * @param timeMs 시간 오름차순 정렬된 ms 타임스탬프 배열
 */
export function aggregateRawCandles(timeMs, open, high, low, close, intervalSec) {
  const out = [];
  let b = null;
  for (let i = 0; i < timeMs.length; i++) {
    const o = open[i], h = high[i], l = low[i], c = close[i];
    if (!(o > 0 && h > 0 && l > 0 && c > 0)) continue;
    const bt = bucketTime(toSeconds(timeMs[i]), intervalSec);
    if (!b || b.time !== bt) {
      if (b) out.push(b);
      b = {time: bt, open: o, high: h, low: l, close: c};
    } else {
      if (h > b.high) b.high = h;
      if (l < b.low) b.low = l;
      b.close = c;
    }
  }
  if (b) out.push(b);
  return out;
}

/**
 * aggregateLine 과 동일한 결과를 shard 의 원본 컬럼형 배열(time ms, 인디케이터 값)에서
 * 직접 만든다. parseShard 와 동일한 유효성 필터(null/0/NaN 제외)를 적용한다.
 * @param timeMs 시간 오름차순 정렬된 ms 타임스탬프 배열
 * @param arr timeMs 와 같은 길이의 인디케이터 값 배열
 */
export function aggregateRawLine(timeMs, arr, intervalSec) {
  const out = [];
  let bt = null;
  for (let i = 0; i < timeMs.length; i++) {
    const v = arr[i];
    if (v === null || v === 0 || Number.isNaN(v)) continue;
    const k = bucketTime(toSeconds(timeMs[i]), intervalSec);
    if (k !== bt) {
      out.push({time: k, value: v});
      bt = k;
    } else {
      out[out.length - 1].value = v;
    }
  }
  return out;
}
