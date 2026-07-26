// 여러 lightweight-charts 인스턴스의 보이는 시간 구간을 상호 동기화한다.
// (Trades 탭의 equity 차트 ↔ 월별 성과 차트처럼 데이터 밀도가 다른 차트들은
//  논리 인덱스가 아닌 "시간 구간" 기준으로 맞춰야 한다.)
//
// 전파는 "마우스가 올라가 있는 차트"에서만 일어난다. 밀도가 다른 차트끼리는
// setVisibleRange 가 각자 스냅/보정한 구간을 비동기로 다시 방출하기 때문에,
// 양방향으로 무조건 전파하면 서로 보정값을 되밀어내며 줌/팬이 즉시 되돌려진다 —
// 제스처를 주도하는 차트 하나만 소스로 삼으면 루프 자체가 생기지 않는다.

export const createTimeRangeSync = () => {
  const charts = new Set();
  let active = null; // 현재 제스처(휠/드래그)를 주도하는 차트 — 포인터가 올라간 쪽

  return {
    /**
     * 차트를 동기화 그룹에 등록한다.
     * @param chart lightweight-charts 인스턴스
     * @param container 포인터 진입/이탈을 감지할 차트 컨테이너 요소
     * @returns 해제 함수 (차트 remove 전에 호출)
     */
    register(chart, container) {
      const onEnter = () => {
        active = chart;
      };
      const onLeave = () => {
        if (active === chart) active = null;
      };
      container.addEventListener('mouseenter', onEnter);
      container.addEventListener('mouseleave', onLeave);

      const handler = (range) => {
        if (!range || active !== chart) return; // 동기화로 인한 수동 변경은 재전파하지 않는다
        for (const other of charts) {
          if (other === chart) continue;
          try {
            other.timeScale().setVisibleRange(range);
          } catch (error) {
            // 아직 데이터가 없는 차트 등 — 다음 변경 때 다시 맞춰진다
          }
        }
      };
      charts.add(chart);
      chart.timeScale().subscribeVisibleTimeRangeChange(handler);

      return () => {
        charts.delete(chart);
        if (active === chart) active = null;
        container.removeEventListener('mouseenter', onEnter);
        container.removeEventListener('mouseleave', onLeave);
        try {
          chart.timeScale().unsubscribeVisibleTimeRangeChange(handler);
        } catch (error) {
          // 차트가 이미 제거된 경우 무시
        }
      };
    },
  };
};
