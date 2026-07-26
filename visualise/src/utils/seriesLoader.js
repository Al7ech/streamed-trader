// 뷰포트 lazy 시계열 로더 — run 의 월별 series shard 를 필요한 만큼만 로드한다.
//
// fine(1m~30m 표시, 원본은 1m): 보이는 구간에 겹치는 shard 만 fetch/parse 해서 유지하고,
// 뷰 밖으로 벗어난 shard 는 evict 한다. → "실제로 표시되는 데이터만 FE 메모리에".
// coarse(1h/1d): 같은 방식으로 본/클릭한 구간의 shard 만 lazy 로드하되, 집계본이 작아서
// (월당 1d≈31개, 1h≈744개) evict 하지 않고 영구 캐시한다 — 재방문은 즉시다.

import {
  aggregateCandles,
  aggregateLine,
  aggregateRawCandles,
  aggregateRawLine,
  isCoarse,
  toSeconds,
} from './aggregate';
import {fetchFileText} from './fileAccess';

const parseShard = (sd) => {
  const time = sd.time || [];
  const n = time.length;

  const candles = [];
  if (sd.ohlc) {
    const {open, high, low, close} = sd.ohlc;
    for (let i = 0; i < n; i++) {
      const o = open[i], h = high[i], l = low[i], c = close[i];
      if (o > 0 && h > 0 && l > 0 && c > 0) {
        candles.push({time: toSeconds(time[i]), open: o, high: h, low: l, close: c});
      }
    }
  }

  const indicators = {};
  for (const [name, arr] of Object.entries(sd.indicators || {})) {
    const pts = [];
    for (let i = 0; i < n; i++) {
      const v = arr[i];
      if (v !== null && v !== 0 && !Number.isNaN(v)) {
        pts.push({time: toSeconds(time[i]), value: v});
      }
    }
    indicators[name] = pts;
  }

  return {start: sd.start, end: sd.end, candles, indicators};
};

/**
 * 한 run 의 시계열 shard 들을 뷰포트에 맞춰 lazy 로드/eviction 하는 매니저.
 * ensure* 는 { candles, indicators } 병합 결과를 반환하며, 로드셋이 바뀌지 않았으면 null 을 반환한다.
 * fine 타임프레임은 1m 원본을 그대로 반환하고(표시 집계는 차트 몫), coarse(1h/1d)는
 * intervalSec 집계본을 반환한다.
 */
export class SeriesLoader {
  constructor(series) {
    this.shards = [...(series.shards || [])].sort((a, b) => a.start - b.start);
    this.columns = series.columns || [];
    this.columnGroups = series.column_groups || {};
    this.hasOhlc = !!series.has_ohlc;
    this.cache = new Map();       // file -> parsed 1m shard (fine 경로, 뷰 밖 evict)
    this.pending = new Map();     // file -> Promise (fine fetch 중복 방지)
    this.coarseCache = new Map(); // file -> Map<intervalSec, {candles, indicators}> — evict 안 함
    this._mergedKey = null;       // 마지막으로 반환한 병합 결과의 시그니처 (fine/coarse 공용)
  }

  get isEmpty() {
    return this.shards.length === 0;
  }

  fullTimeRangeMs() {
    if (this.isEmpty) return null;
    return {from: this.shards[0].start, to: this.shards[this.shards.length - 1].end};
  }

  /**
   * 표시 타임프레임 intervalSec 기준으로 [fromMs, toMs] 구간을 보이게 하는 데 필요한
   * shard 를 확보(±pad)하고 병합 결과를 반환한다. 로드셋이 안 바뀌었으면 null —
   * 이미 로드된 구간 안에서의 패닝 시 매 debounce tick 마다 재병합·재-setData 하는 것을 막는다.
   */
  async ensureRange(intervalSec, fromMs, toMs, pad = 1) {
    if (this.isEmpty) return null;
    const indices = this._targetIndices(fromMs, toMs, pad);
    return isCoarse(intervalSec)
      ? this._ensureCoarse(intervalSec, indices)
      : this._ensureFine(indices);
  }

  /** 최신 spanBars 봉(차트 기본 뷰 폭)을 채우는 구간 로드 — 초기 뷰/타임프레임 전환용. */
  async ensureLatest(intervalSec, spanBars = 200) {
    const range = this.fullTimeRangeMs();
    if (!range) return null;
    return this.ensureRange(intervalSec, range.to - spanBars * intervalSec * 1000, range.to, 0);
  }

  /** 요청 구간과 겹치는 shard 인덱스(±pad). 구간이 데이터 밖이면 가장 가까운 끝 shard 1개. */
  _targetIndices(fromMs, toMs, pad) {
    const idx = [];
    this.shards.forEach((s, i) => {
      if (s.start <= toMs && s.end >= fromMs) idx.push(i);
    });
    if (idx.length === 0) {
      return [toMs < this.shards[0].start ? 0 : this.shards.length - 1];
    }
    const lo = Math.max(0, idx[0] - pad);
    const hi = Math.min(this.shards.length - 1, idx[idx.length - 1] + pad);
    const out = [];
    for (let i = lo; i <= hi; i++) out.push(i);
    return out;
  }

  /** {candles, indicators} 조각들을 시간순으로 이어붙인다 (fine shard / coarse 집계본 공용). */
  _concatParts(parts) {
    const candles = [];
    const indicators = {};
    this.columns.forEach(c => { indicators[c] = []; });

    for (const part of parts) {
      if (!part) continue;
      for (const c of part.candles) candles.push(c);
      for (const [name, pts] of Object.entries(part.indicators)) {
        if (!indicators[name]) indicators[name] = [];
        for (const p of pts) indicators[name].push(p);
      }
    }
    return {candles, indicators};
  }

  /** 병합 시그니처가 마지막 반환 이후 그대로면 null, 바뀌었으면 새로 병합해 반환. */
  _mergeIfChanged(key, parts) {
    if (key === this._mergedKey) return null;
    this._mergedKey = key;
    return this._concatParts(parts);
  }

  /* ---------------- fine: 1m 원본, 뷰포트 로드 + evict ---------------- */

  async _ensureFine(indices) {
    await Promise.all(indices.map(i => this._loadShard(this.shards[i].file)));

    // 뷰 밖 shard evict
    const keep = new Set(indices.map(i => this.shards[i].file));
    for (const file of Array.from(this.cache.keys())) {
      if (!keep.has(file)) this.cache.delete(file);
    }

    return this._mergeIfChanged(
      `f:${indices.join(',')}`,
      indices.map(i => this.cache.get(this.shards[i].file)),
    );
  }

  async _loadShard(file) {
    if (this.cache.has(file)) return this.cache.get(file);
    if (this.pending.has(file)) return this.pending.get(file);
    const p = (async () => {
      const text = await fetchFileText(file);
      const parsed = parseShard(JSON.parse(text));
      this.cache.set(file, parsed);
      this.pending.delete(file);
      return parsed;
    })();
    this.pending.set(file, p);
    return p;
  }

  /* ---------------- coarse: intervalSec 집계본, 영구 캐시 ---------------- */

  /**
   * 버킷 경계(1h/1d)가 월 shard 경계와 정확히 나눠떨어지므로 shard별 집계 후 이어붙인
   * 결과는 전체를 한 번에 집계한 것과 동일하다 (aggregate.js 참고). 병합은 항상
   * coarseCache 전체에서 다시 하므로, 동시에 진행된 호출들이 서로 먼저 로드한 shard 를
   * 모르는 채로 낡은 결과로 덮어써버리는 일이 없다 (준비 안 된 shard 는 gap 으로 남는다).
   */
  async _ensureCoarse(intervalSec, indices) {
    await Promise.all(indices.map(i => this._ensureCoarseShard(this.shards[i].file, intervalSec)));

    const ready = this.shards
      .map(s => (this.coarseCache.get(s.file)?.has(intervalSec) ? '1' : '0'))
      .join('');
    return this._mergeIfChanged(
      `c${intervalSec}:${ready}`,
      this.shards.map(s => this.coarseCache.get(s.file)?.get(intervalSec)),
    );
  }

  /**
   * 파일 하나의 intervalSec 집계본을 준비한다(캐시돼 있으면 즉시 반환).
   *
   * 이미 뷰포트 로드셋(this.cache)에 캔들/지표가 객체 배열로 파싱돼 있으면 그걸 그대로
   * aggregateCandles/aggregateLine 으로 집계한다. 그렇지 않으면 _loadShard(전체를 캔들당
   * 객체로 만드는 parseShard)를 거치지 않고, JSON 텍스트를 직접 fetch/parse 해서 원본
   * 컬럼형 배열에서 곧장 집계한다(aggregateRawCandles/aggregateRawLine) — shard당
   * 44,640행짜리 중간 객체 배열(수십MB)을 만들지 않고 버킷 결과(수십~수백 개)만 남긴다.
   */
  async _ensureCoarseShard(file, intervalSec) {
    let byInterval = this.coarseCache.get(file);
    if (!byInterval) {
      byInterval = new Map();
      this.coarseCache.set(file, byInterval);
    }

    if (!byInterval.has(intervalSec)) {
      if (this.cache.has(file)) {
        const shard = this.cache.get(file);
        const indicators = {};
        for (const [name, pts] of Object.entries(shard.indicators)) {
          indicators[name] = aggregateLine(pts, intervalSec);
        }
        byInterval.set(intervalSec, {
          candles: aggregateCandles(shard.candles, intervalSec),
          indicators,
        });
      } else {
        const text = await fetchFileText(file);
        const raw = JSON.parse(text);
        const timeMs = raw.time || [];

        const indicators = {};
        for (const [name, arr] of Object.entries(raw.indicators || {})) {
          indicators[name] = aggregateRawLine(timeMs, arr, intervalSec);
        }

        let candles = [];
        if (raw.ohlc) {
          const {open, high, low, close} = raw.ohlc;
          candles = aggregateRawCandles(timeMs, open, high, low, close, intervalSec);
        }

        byInterval.set(intervalSec, {candles, indicators});
      }
    }

    return byInterval.get(intervalSec);
  }
}
