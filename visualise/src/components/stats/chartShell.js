// Trades 탭 차트(EquityChart/MonthlyStatsChart) 공통 셸.
// createChart 기본 옵션과, keepMounted 숨김 탭에서 크기 0 으로 마운트됐다가 가시화될 때
// 리사이즈 + 1회 fitContent 하는 수명 관리를 한 곳에 둔다 — 두 컴포넌트는 시리즈 구성만 다르다.

import {createChart} from 'lightweight-charts';
import {CHART_COLORS} from '../../theme';

// 비율(0~1) 값을 % 로 표시하는 공통 priceFormat
export const PCT_FORMAT = {
  type: 'custom',
  formatter: (v) => `${(v * 100).toFixed(1)}%`,
  minMove: 0.0001,
};

/**
 * 공통 옵션으로 차트를 만든다. 백테스트 구간이 데이터의 전부이므로
 * 시작 이전/종료 이후로는 줌/팬이 못 나가게 양끝을 고정한다.
 */
export const createStatsChart = (container, {timeFormatter, scaleMargins}) =>
  createChart(container, {
    layout: {
      background: {color: CHART_COLORS.background},
      textColor: CHART_COLORS.text,
    },
    grid: {
      vertLines: {color: CHART_COLORS.grid},
      horzLines: {color: CHART_COLORS.grid},
    },
    crosshair: {mode: 1},
    rightPriceScale: {borderColor: CHART_COLORS.scaleBorder, scaleMargins},
    timeScale: {
      borderColor: CHART_COLORS.scaleBorder,
      timeVisible: false,
      fixLeftEdge: true,
      fixRightEdge: true,
      lockVisibleTimeRangeOnResize: true,
    },
    localization: {timeFormatter},
  });

/**
 * 셸 수명 관리: ResizeObserver 로 컨테이너 크기를 따라가고(숨김 탭의 0→실제 크기 전환 포함),
 * 최초 가시화 시 1회 fitContent 와 onFirstSize 훅(예: pane 비율 조정)을 수행하며,
 * timeSync 그룹에 등록한다. 반환된 cleanup 을 chart.remove() 전에 호출할 것.
 * @param fitState {{fitted: boolean}} — refitChart 와 공유하는 fit 상태
 */
export const installChartShell = (chart, container, {timeSync, fitState, onFirstSize}) => {
  const unregisterSync = timeSync ? timeSync.register(chart, container) : null;
  let sized = false;
  const observer = new ResizeObserver((entries) => {
    const {width, height} = entries[0].contentRect;
    if (width <= 0 || height <= 0) return;
    chart.applyOptions({width, height});
    if (!sized) {
      sized = true;
      if (onFirstSize) onFirstSize(chart);
    }
    if (!fitState.fitted) {
      fitState.fitted = true;
      chart.timeScale().fitContent();
    }
  });
  observer.observe(container);

  return () => {
    observer.disconnect();
    if (unregisterSync) unregisterSync();
  };
};

/**
 * 데이터 교체 시(run 전환) 뷰 재조정: 이미 보이는 상태면 즉시 다시 fit,
 * 숨김 상태면 fitted 플래그만 풀어 installChartShell 의 observer 가 가시화 때 수행하게 한다.
 */
export const refitChart = (chart, container, fitState, hasData) => {
  fitState.fitted = false;
  if (container && container.clientWidth > 0 && hasData) {
    fitState.fitted = true;
    chart.timeScale().fitContent();
  }
};
