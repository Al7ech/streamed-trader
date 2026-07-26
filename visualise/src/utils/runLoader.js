// run JSON 로더 — 백테스터가 쓰는 결과 구조:
//   backtest/<run>.json                 : metadata + summary + trades + series 인덱스 (작음)
//   backtest/<run>.<YYYY-MM>.series.json : 월별 컬럼형 OHLC/지표 shard (무거움 — seriesLoader 가 lazy 로드)
// 여기서는 가벼운 run JSON 파싱과 run 파일 목록 필터링만 담당한다.

import {toSeconds} from './aggregate';
import {deleteFiles, fetchFileText} from './fileAccess';

/**
 * run 파일(사이드바에 나열)만 골라낸다: backtest/ 아래의 .json 중 .series.json 이 아닌 것.
 */
export const filterRunFiles = (files) => {
  return files
    .filter(file => {
      const pathLower = (file.path || file.name).toLowerCase();
      const nameLower = file.name.toLowerCase();
      return pathLower.includes('backtest/') && !nameLower.endsWith('.series.json');
    })
    .sort((a, b) => b.name.localeCompare(a.name)); // 최근 run 이 위로
};

// run JSON 의 다운샘플된 곡선 블록({time: [ms...], value: [...]}) → {time(초), value} 포인트 배열.
// equity(전략 자산) 와 benchmark(기초자산 buy & hold) 둘 다 같은 형태로 실려 온다 — 전체 구간이
// 항상 필요해서 lazy shard 가 아닌 run JSON 에 넣는다. 백엔드가 같은 stride 로 다운샘플하므로
// 두 배열의 타임스탬프는 인덱스별로 일치한다. 구 run(블록 없음)은 null.
const formatCurve = (curve) => {
  if (!curve || !Array.isArray(curve.time) || curve.time.length === 0) return null;
  return curve.time.map((ms, i) => ({time: toSeconds(ms), value: curve.value[i]}));
};

// 프론트 trade(순손익 wnl)에서 백엔드 build_summary 가 쓰는 수수료 미반영 raw wnl 을 복원한다.
// 승/패 분류(헤더 승률·월별 승률·PF/Payoff)는 반드시 이 값 기준 — 순손익(wnl) 기준으로 세면
// 수수료만 낸 진입 체결이 패로 잡혀 헤더 summary 와 어긋난다.
export const rawWnl = (trade) => trade.wnl + trade.fee;

const formatTrades = (trades) => trades.map((t) => {
  const netWnl = t.wnl - t.fee; // 실제 margin 변화량과 일치하는 순손익(수수료 반영)
  return {
    time: toSeconds(t.timestamp),
    quantity: t.quantity,
    price: t.price,
    wnl: netWnl,
    fee: t.fee,
    margin: t.margin,
    wnl_percent: t.margin ? netWnl / t.margin : 0,
    leverage: t.leverage,
  };
});

/**
 * run JSON 을 읽어 파싱한다.
 * @returns {Promise<{metadata, summary, equity, baseAsset, trades, series}>}
 */
export const loadRun = async (fileName) => {
  const text = await fetchFileText(fileName);
  const doc = JSON.parse(text);
  return {
    metadata: doc.metadata || {},
    summary: doc.summary || {},
    equity: formatCurve(doc.equity),
    baseAsset: formatCurve(doc.benchmark), // schema v2 부터 — 구 run 은 null
    trades: formatTrades(doc.trades || []),
    series: doc.series || {columns: [], has_ohlc: false, shards: []},
  };
};

/**
 * run 삭제: run JSON에 실려 있는 series.shards 로 이 run에 속한 모든 shard 파일을 알아낸 뒤,
 * shard → run JSON 순서로 지운다 (중간에 중단돼도 run JSON이 남아 목록에 계속 보이고 재시도로
 * 마무리할 수 있게 하기 위함 — 반대 순서면 run JSON이 먼저 없어져 목록에서 사라지지만 shard가
 * 고아로 남아 영구히 정리할 UI 경로가 없어진다).
 */
export const deleteRun = async (fileName) => {
  let shardFiles = [];
  try {
    const text = await fetchFileText(fileName);
    const doc = JSON.parse(text);
    shardFiles = Array.isArray(doc?.series?.shards)
      ? doc.series.shards.map(s => s.file).filter(Boolean)
      : [];
  } catch (error) {
    console.warn(`Could not read shard list for ${fileName}, deleting run file only:`, error);
  }
  return deleteFiles([...shardFiles, fileName]);
};
