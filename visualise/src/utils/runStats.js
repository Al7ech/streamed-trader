// run 전체 성과 KPI — Trades 탭 상단 타일용.
// 입력은 runLoader 형태 그대로: equity [{time(초), value}], trades [{time(초), quantity, wnl(순손익), fee, ...}].
// trades 는 백테스터 기록 순서(시간 오름차순)가 보존된다고 가정한다 (runLoader.formatTrades 는 순서를 바꾸지 않는다).

import {rawWnl} from './runLoader';

const EPS = 1e-9; // tradeSegments.js 와 같은 포지션 flat 판정 기준
const YEAR_SEC = 365 * 24 * 3600;

// summary.max_drawdown 은 신형이면 {max_drawdown, peak/trough...} 객체, 구형이면 없음 —
// 비율 하나만 필요한 소비자(KPI Calmar, equity 차트 범례)용 공통 언랩.
export const getMddRatio = (summary) => {
  const mdd = summary && summary.max_drawdown;
  return mdd && typeof mdd === 'object' ? mdd.max_drawdown : undefined;
};

/**
 * run 수준 KPI 를 계산한다.
 * - cagr: equity 양끝값 기준 연율 성장률. equity 없거나 기간/시작값이 무효면 null.
 * - profitFactor/payoff: 헤더 승률과 동일한 raw wnl(= wnl + fee, 0 제외) 기준.
 *   PF = Σ양수/|Σ음수|, payoff = 평균 양수/|평균 음수|. 분모 0 이면 null.
 * - expectancy: 수수료 포함 실손익 기준 — Σ(net wnl 전체 체결) / 청산 체결 수(raw≠0).
 * - exposure/avgHoldSec: 체결 리플레이로 flat→flat 에피소드를 복원해
 *   포지션 보유 시간 비율과 에피소드 평균 보유 시간을 구한다. 미청산 포지션은 구간 끝까지 보유로 간주.
 */
export const computeRunStats = (equity, trades) => {
  const stats = {
    cagr: null,
    profitFactor: null,
    payoff: null,
    expectancy: null,
    exposure: null,
    avgHoldSec: null,
    closedCount: 0,
  };

  if (Array.isArray(equity) && equity.length >= 2) {
    const first = equity[0];
    const last = equity[equity.length - 1];
    const span = last.time - first.time;
    if (span > 0 && first.value > 0 && last.value > 0) {
      stats.cagr = Math.pow(last.value / first.value, YEAR_SEC / span) - 1;
    }
  }

  const fills = trades || [];
  if (fills.length === 0) return stats;

  let grossWin = 0;
  let grossLoss = 0; // 양수 누적
  let winCount = 0;
  let lossCount = 0;
  let netTotal = 0;

  for (const t of fills) {
    const raw = rawWnl(t);
    netTotal += t.wnl;
    if (raw > 0) {
      grossWin += raw;
      winCount += 1;
    } else if (raw < 0) {
      grossLoss += -raw;
      lossCount += 1;
    }
  }

  stats.closedCount = winCount + lossCount;
  if (grossLoss > 0) {
    stats.profitFactor = grossWin / grossLoss;
    if (winCount > 0 && lossCount > 0) {
      stats.payoff = (grossWin / winCount) / (grossLoss / lossCount);
    }
  }
  if (stats.closedCount > 0) stats.expectancy = netTotal / stats.closedCount;

  // 포지션 보유 에피소드(flat→flat) 리플레이 — tradeSegments.js 와 같은 수량 누적/EPS 규칙
  const rangeStart = Array.isArray(equity) && equity.length > 0 ? equity[0].time : fills[0].time;
  const rangeEnd = Array.isArray(equity) && equity.length > 0
    ? equity[equity.length - 1].time
    : fills[fills.length - 1].time;

  let position = 0;
  let episodeStart = null;
  let inMarketSec = 0;
  let episodeCount = 0;

  for (const t of fills) {
    if (!t.quantity) continue;
    const wasFlat = Math.abs(position) < EPS;
    position += t.quantity;
    const isFlat = Math.abs(position) < EPS;
    if (wasFlat && !isFlat) {
      episodeStart = t.time;
    } else if (!wasFlat && isFlat && episodeStart !== null) {
      inMarketSec += t.time - episodeStart;
      episodeCount += 1;
      episodeStart = null;
    }
  }
  if (episodeStart !== null && rangeEnd > episodeStart) {
    // run 끝까지 미청산 — 구간 끝까지 보유한 것으로 계산
    inMarketSec += rangeEnd - episodeStart;
    episodeCount += 1;
  }

  const totalSpan = rangeEnd - rangeStart;
  if (totalSpan > 0) stats.exposure = inMarketSec / totalSpan;
  if (episodeCount > 0) stats.avgHoldSec = inMarketSec / episodeCount;

  return stats;
};

/**
 * underwater(drawdown) 곡선: 러닝 피크 대비 v/peak − 1 (0 ~ 음수 비율).
 * 다운샘플 equity 기반 근사 — 포인트 사이의 순간 낙폭은 놓칠 수 있다.
 */
export const computeDrawdownSeries = (equity) => {
  if (!Array.isArray(equity) || equity.length === 0) return [];
  let peak = -Infinity;
  return equity.map((p) => {
    if (p.value > peak) peak = p.value;
    return {time: p.time, value: peak > 0 ? p.value / peak - 1 : 0};
  });
};

/**
 * 기초자산(buy & hold) 대비 누적 초과 성과: equity/baseAsset − 1.
 * 0 위면 그냥 들고 있는 것보다 앞서는 중, 아래면 뒤처지는 중이다.
 *
 * 두 곡선은 백엔드에서 같은 stride 로 다운샘플되어 타임스탬프가 인덱스별로 일치한다 —
 * 그래도 길이/시각을 확인하고 어긋나면 보간 없이 [] 를 돌려 조용히 비활성화한다.
 */
export const computeRelativeStrengthSeries = (equity, baseAsset) => {
  if (!Array.isArray(equity) || !Array.isArray(baseAsset)) return [];
  if (equity.length === 0 || equity.length !== baseAsset.length) return [];
  const out = [];
  for (let i = 0; i < equity.length; i += 1) {
    const base = baseAsset[i];
    if (base.time !== equity[i].time || !(base.value > 0)) return [];
    out.push({time: equity[i].time, value: equity[i].value / base.value - 1});
  }
  return out;
};
