// 거래 내역(체결 리스트)을 리플레이해 "포지션 구간"(진입 평단 → 청산가)을 만든다.
// 백테스터 SingleThreadedBacktester._trade 와 같은 평균단가 규칙을 따른다:
// 같은 방향 추가(피라미딩)는 수량 가중 평균으로 평단 갱신, 부분 청산은 평단 유지.

const EPS = 1e-9;

/**
 * 포지션을 줄이는 체결(부분/전체 청산·플립)마다 구간 하나를 방출한다.
 * @param trades 시간 오름차순 정렬 가능한 {time, quantity, price, wnl} 배열
 * @returns {Array<{t0, p0, t1, p1, wnl}>} t0/p0 = 진입 시각/평단, t1/p1 = 청산 시각/체결가
 */
export function buildPositionSegments(trades) {
  const sorted = [...trades].sort((a, b) => a.time - b.time);
  const segments = [];

  let position = 0;
  let avgPrice = 0;
  let startTime = 0;

  for (const t of sorted) {
    const q = t.quantity;
    if (!q) continue;

    if (Math.abs(position) < EPS) {
      // 신규 진입
      position = q;
      avgPrice = t.price;
      startTime = t.time;
    } else if ((position > 0) === (q > 0)) {
      // 피라미딩 — 평단만 갱신, 구간 시작은 유지
      avgPrice = (avgPrice * Math.abs(position) + t.price * Math.abs(q))
        / (Math.abs(position) + Math.abs(q));
      position += q;
    } else {
      // 청산(부분/전체/플립) — 지금까지의 포지션 구간을 방출
      segments.push({t0: startTime, p0: avgPrice, t1: t.time, p1: t.price, wnl: t.wnl});

      const next = position + q;
      if (Math.abs(next) < EPS) {
        position = 0;
        avgPrice = 0;
      } else if ((next > 0) === (position > 0)) {
        position = next; // 부분 청산 — 평단·시작 시각 유지
      } else {
        // 플립 — 같은 체결에서 반대 방향 신규 진입
        position = next;
        avgPrice = t.price;
        startTime = t.time;
      }
    }
  }

  return segments;
}
