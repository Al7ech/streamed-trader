// 월별 성과 집계 — Trades 탭의 월별 성과 차트용.
// 입력은 runLoader 가 만든 형태 그대로: equity 는 다운샘플 [{time(초), value}],
// trades 는 formatTrades 결과([{time(초), wnl(순손익), fee, ...}]). 월 경계는 프로젝트 규약대로 UTC.

import {rawWnl} from './runLoader';

// epoch(초) ↔ 'YYYY-MM' 변환 쌍 — UTC 월 경계 규약의 단일 출처
export const monthKey = (sec) => new Date(sec * 1000).toISOString().slice(0, 7);
export const monthStartSec = (month) => {
  const [y, m] = month.split('-').map(Number);
  return Date.UTC(y, m - 1, 1) / 1000;
};

/**
 * 월별 수익률/승률/MDD 를 계산한다.
 *
 * - 수익률: 해당 월 마지막 equity / 직전 월 마지막 equity − 1 (첫 달은 첫 포인트 기준).
 * - MDD: 월내 equity 포인트의 peak 대비 최대 낙폭. equity 가 다운샘플이라 근사치다
 *   (정확한 per-candle 곡선은 lazy shard 에만 있다).
 * - 승률: 헤더 summary(build_summary)와 동일하게 수수료 미반영 raw wnl(> 0 승, < 0 패,
 *   0 은 제외) 기준. 프론트의 trade.wnl 은 순손익이므로 fee 를 되돌려 raw 를 복원한다.
 *
 * @returns {Array<{month, returnPct, maxDD, wins, losses, winRate}>} 월 오름차순.
 *   equity 없는 구 run 이면 returnPct/maxDD 는 null.
 */
export const computeMonthlyStats = (equity, trades) => {
  const rows = new Map();
  const get = (month) => {
    if (!rows.has(month)) {
      rows.set(month, {month, returnPct: null, maxDD: null, wins: 0, losses: 0, winRate: null});
    }
    return rows.get(month);
  };

  if (Array.isArray(equity) && equity.length > 0) {
    let month = null;
    let base = null; // 이번 달 수익률의 기준값 — 직전 월 마지막 equity (첫 달은 첫 포인트)
    let peak = -Infinity;
    let maxDD = 0;
    let last = null;

    const flush = () => {
      if (month === null) return;
      const r = get(month);
      r.returnPct = base ? last / base - 1 : null;
      r.maxDD = maxDD;
    };

    for (const p of equity) {
      const m = monthKey(p.time);
      if (m !== month) {
        flush();
        base = last ?? p.value;
        month = m;
        peak = -Infinity;
        maxDD = 0;
      }
      if (p.value > peak) {
        peak = p.value;
      } else if (peak > 0) {
        maxDD = Math.max(maxDD, 1 - p.value / peak);
      }
      last = p.value;
    }
    flush();
  }

  for (const t of trades || []) {
    const raw = rawWnl(t);
    if (raw === 0) continue;
    const r = get(monthKey(t.time));
    if (raw > 0) r.wins += 1;
    else r.losses += 1;
  }

  const out = [...rows.values()].sort((a, b) => a.month.localeCompare(b.month));
  for (const r of out) {
    const total = r.wins + r.losses;
    r.winRate = total ? r.wins / total : null;
  }
  return out;
};
