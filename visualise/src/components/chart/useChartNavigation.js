import {useCallback, useMemo, useRef, useState} from 'react';
import debounce from 'lodash.debounce';

/**
 * lazy 로드와 맞물린 차트 내비게이션 훅 — 거래 클릭/타임라인 점프·드래그로 특정 시점에
 * 이동하는 로직과, 대상 구간이 아직 로드 전일 때의 "요청해두고 도착하면 이동" 기제를 담당한다.
 *
 * setVisibleRange 는 시간 기반이라 대상 구간 근처에 이미 로드된 bar 가 있어야 동작한다 —
 * 그 구간에 bar 가 하나도 없으면(아직 fetch 안 된 shard) 조용히 무시되고 뷰가 전혀 움직이지
 * 않는다(lightweight-charts 로 직접 확인한 동작). 그래서 대상이 candleData 범위 안이면
 * 바로 이동(fast path)하고, 범위 밖이면 pendingFocus/pendingPan 에 적어두고
 * onVisibleRangeChange 로 로드를 요청한 뒤, 데이터가 도착했을 때(resolvePending) 재시도한다.
 *
 * @param chartRef lightweight-charts 인스턴스 ref
 * @param candleData 로드된 1m(또는 coarse 집계) 캔들 — 로드 여부 판정 기준
 * @param displayCandles 표시 타임프레임으로 집계된 캔들 — 이동 대상 인덱스 계산 기준
 * @param defaultCandles 기본 뷰 폭(봉 개수)
 * @param onVisibleRangeChange (fromSec, toSec) 로드 요청 콜백
 */
export const useChartNavigation = ({chartRef, candleData, displayCandles, defaultCandles, onVisibleRangeChange}) => {
  const pendingFocusRef = useRef(null); // 아직 로드 안 된 구간의 거래로 점프 요청 (초 단위 time)
  const pendingPanRef = useRef(null); // 아직 로드 안 된 구간으로 pan 요청 ({from, to}) — 도착하면 setVisibleRange 재시도
  const onRangeChangeRef = useRef(onVisibleRangeChange); // 최신 콜백 참조 (stale 방지)
  onRangeChangeRef.current = onVisibleRangeChange;
  const [selectedTradeTime, setSelectedTradeTime] = useState(null); // 선택된 거래 시간

  // 차트를 특정 시간으로 이동하는 함수
  const moveChartToTime = useCallback((targetTime) => {
    if (!chartRef.current || !displayCandles.length) return;

    try {
      // 타겟 시간과 가장 가까운 캔들 인덱스 찾기 (집계본 기준)
      let closestIndex = 0;
      let minTimeDiff = Math.abs(displayCandles[0].time - targetTime);

      for (let i = 1; i < displayCandles.length; i++) {
        const timeDiff = Math.abs(displayCandles[i].time - targetTime);
        if (timeDiff < minTimeDiff) {
          minTimeDiff = timeDiff;
          closestIndex = i;
        }
      }

      // 해당 인덱스를 중심으로 차트 범위 설정
      const rangeSize = Math.min(defaultCandles, displayCandles.length);
      const fromIndex = Math.max(0, closestIndex - Math.floor(rangeSize / 2));
      const toIndex = Math.min(displayCandles.length, fromIndex + rangeSize);

      chartRef.current.timeScale().setVisibleLogicalRange({
        from: fromIndex,
        to: toIndex
      });

      setSelectedTradeTime(targetTime);
    } catch (error) {
      console.warn('Error moving chart to time:', error);
    }
  }, [chartRef, displayCandles, defaultCandles]);

  // 거래 클릭: 로드된 구간이면 바로 이동, 아니면 해당 구간 shard 를 lazy 로드 후 이동(pendingFocus)
  const handleTradeClick = useCallback((targetTime) => {
    setSelectedTradeTime(targetTime);
    const loaded = candleData.length > 0
      && targetTime >= candleData[0].time
      && targetTime <= candleData[candleData.length - 1].time;
    if (loaded) {
      moveChartToTime(targetTime);
      return;
    }
    // 아직 로드되지 않은 구간 → 하루 폭으로 로드를 요청하고, 도착하면 이동
    pendingFocusRef.current = targetTime;
    const cb = onRangeChangeRef.current;
    if (cb) cb(targetTime - 12 * 60 * 60, targetTime + 12 * 60 * 60);
  }, [candleData, moveChartToTime]);

  // 타임라인 하이라이트 박스 드래그는 mousemove 마다 panTo 를 호출하므로, 로드 안 된 구간으로
  // 끌고 들어갈 때 매 tick 마다 ensureRange 를 쏘지 않도록 디바운스한다.
  // (이미 로드된 구간 안에서의 pan/드래그는 panTo 의 fast path 를 타므로 이 디바운스와 무관 — 그대로 즉시 반응한다.)
  const triggerPendingLoad = useMemo(
    () => debounce((from, to) => {
      const cb = onRangeChangeRef.current;
      if (cb) cb(from, to);
    }, 150),
    []
  );

  // 타임라인 셀렉터(클릭/드래그)·First 버튼 → 차트 뷰 패닝
  const panTo = useCallback((fromSec, toSec) => {
    if (!chartRef.current) return;
    pendingFocusRef.current = null;

    const loaded = candleData.length > 0
      && fromSec >= candleData[0].time
      && toSec <= candleData[candleData.length - 1].time;
    if (loaded) {
      pendingPanRef.current = null;
      try {
        chartRef.current.timeScale().setVisibleRange({from: fromSec, to: toSec});
      } catch (error) {
        console.warn('Error panning chart from timeline selector:', error);
      }
      return;
    }

    pendingPanRef.current = {from: fromSec, to: toSec};
    triggerPendingLoad(fromSec, toSec);
  }, [chartRef, candleData, triggerPendingLoad]);

  /** run/타임프레임 전환 시 미해결 점프 요청 폐기. */
  const clearPending = useCallback(() => {
    pendingFocusRef.current = null;
    pendingPanRef.current = null;
  }, []);

  /**
   * 새 데이터가 차트에 setData 된 직후 호출 — 로드 전 구간으로의 점프 요청이 이제
   * 충족됐으면 그때 이동을 재시도한다. pendingPan 은 대상 구간과 일부만 겹쳐도 이동한다
   * (전부 로드될 필요는 없다 — 나머지는 visible-range-change 구독의 lazy load 가 이어서 채운다).
   */
  const resolvePending = useCallback(() => {
    if (!chartRef.current || displayCandles.length === 0) return;
    const first = displayCandles[0].time;
    const last = displayCandles[displayCandles.length - 1].time;

    if (pendingFocusRef.current != null) {
      // 거래 클릭으로 요청한 구간이 이제 로드되었으면 그 시점으로 이동
      const t = pendingFocusRef.current;
      if (first <= t && t <= last) {
        pendingFocusRef.current = null;
        moveChartToTime(t);
      }
    } else if (pendingPanRef.current != null) {
      // panTo 의 fast path 가 실패했던 이유(대상 구간에 bar 가 없어 시간 기반
      // setVisibleRange 가 무시됨)가 이제 해소됐으니 다시 시도한다.
      const {from, to} = pendingPanRef.current;
      if (first <= to && from <= last) {
        pendingPanRef.current = null;
        try {
          chartRef.current.timeScale().setVisibleRange({from, to});
        } catch (error) {
          console.warn('Error panning chart once pending data arrived:', error);
        }
      }
    }
  }, [chartRef, displayCandles, moveChartToTime]);

  return {
    selectedTradeTime,
    setSelectedTradeTime,
    moveChartToTime,
    panTo,
    handleTradeClick,
    resolvePending,
    clearPending,
  };
};
