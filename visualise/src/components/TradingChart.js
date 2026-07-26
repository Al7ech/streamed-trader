import React, {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {CandlestickSeries, createChart, createSeriesMarkers, createTextWatermark, LineSeries} from 'lightweight-charts';
import {Button, Checkbox, Group, Paper, SegmentedControl, Stack, Switch, Text} from '@mantine/core';
import {IconArrowsMaximize, IconChevronsLeft, IconChevronsRight} from '@tabler/icons-react';
import debounce from 'lodash.debounce';
import TradeTable from './TradeTable';
import {aggregateCandles, aggregateLine, bucketTime, TIMEFRAMES} from '../utils/aggregate';
import {buildPositionSegments} from '../utils/tradeSegments';
import {TradeSegmentsPrimitive} from './chart/TradeSegmentsPrimitive';
import TimelineSelector from './chart/TimelineSelector';
import {useChartNavigation} from './chart/useChartNavigation';
import {CANDLE_COLORS, CHART_COLORS, INDICATOR_COLORS, TRADE_COLORS} from '../theme';
import './TradingChart.css';

const TradingChart = ({
                        candleData,
                        indicatorData,
                        indicatorGroups,
                        tradeData,
                        equityData,
                        selectedBacktestFile,
                        fullRange,
                        onVisibleRangeChange,
                        onTimeframeChange
                      }) => {
  const chartContainerRef = useRef(null);
  const chartRef = useRef(null);
  const candleRef = useRef(null);
  const markersRef = useRef(null); // 트레이드 마커 primitive
  const segmentsRef = useRef(null); // 포지션 구간(진입→청산) 연결선 primitive
  const seriesRefsRef = useRef({}); // 인디케이터 시리즈 참조 저장
  const paneLabelsRef = useRef([]); // pane 좌상단 그룹명 watermark 플러그인 참조
  const lastRunRef = useRef(null); // 마지막으로 초기 뷰를 맞춘 run (fit-once 판정)
  const lastTimeframeRef = useRef(60); // 마지막으로 뷰를 맞춘 타임프레임 (변경 시 재-fit)
  const onRangeChangeRef = useRef(onVisibleRangeChange); // 최신 콜백 참조 (stale 방지)
  onRangeChangeRef.current = onVisibleRangeChange;
  const onTimeframeChangeRef = useRef(onTimeframeChange); // 최신 콜백 참조 (stale 방지)
  onTimeframeChangeRef.current = onTimeframeChange;
  const [chartSize, setChartSize] = useState({width: 0, height: 0});
  const [visibleIndicators, setVisibleIndicators] = useState({}); // 인디케이터 가시성 상태
  const visibleIndicatorsRef = useRef(visibleIndicators); // 시리즈 재생성 시 최신 가시성 참조 (stale 방지)
  visibleIndicatorsRef.current = visibleIndicators;
  const [allIndicatorsVisible, setAllIndicatorsVisible] = useState(true); // 전체 인디케이터 가시성 상태
  const [timeframe, setTimeframe] = useState(60); // 표시 타임프레임(초). 원본 데이터는 1m 가정.
  const [panelCollapsed, setPanelCollapsed] = useState(false); // 우측 거래 패널 접힘 여부
  const [panelWidth, setPanelWidth] = useState(360); // 우측 거래 패널 폭(px). 드래그 핸들로 조절.
  const rootRef = useRef(null); // 전체 레이아웃 — 드래그 시 패널 최대 폭 클램프 기준
  const [visibleTimeRange, setVisibleTimeRange] = useState(null); // 현재 차트에 보이는 시간 구간(초) — 타임라인 셀렉터용

  const defaultCandles = 200;

  // 현재 탭 JS 힙 사용량(MB) — Chromium 전용 performance.memory, 미지원 브라우저에선 표시 안 함
  const [memoryMB, setMemoryMB] = useState(null);
  useEffect(() => {
    if (!performance.memory) return;
    const read = () => setMemoryMB(Math.round(performance.memory.usedJSHeapSize / 1048576));
    read();
    const id = setInterval(read, 2000);
    return () => clearInterval(id);
  }, []);

  // 타임프레임 변경(초기 포함)을 부모에 알린다 — 부모가 로딩 모드(뷰포트 lazy/전체 coarse)를 전환
  useEffect(() => {
    const cb = onTimeframeChangeRef.current;
    if (cb) cb(timeframe);
  }, [timeframe]);

  // 로드된 1분봉/지표를 선택 타임프레임으로 집계한 "표시용" 데이터.
  // 차트에 그리는 것/논리 인덱스 계산은 모두 이 집계본을 기준으로 한다.
  const displayCandles = useMemo(
    () => aggregateCandles(candleData, timeframe),
    [candleData, timeframe]
  );
  const displayIndicators = useMemo(() => {
    if (timeframe <= 60) return indicatorData;
    const out = {};
    for (const [name, pts] of Object.entries(indicatorData || {})) {
      out[name] = Array.isArray(pts) ? aggregateLine(pts, timeframe) : pts;
    }
    return out;
  }, [indicatorData, timeframe]);

  // lazy 로드와 맞물린 내비게이션(거래 클릭/타임라인 점프·드래그, 로드 전 구간 재시도)
  const {
    selectedTradeTime,
    setSelectedTradeTime,
    panTo,
    handleTradeClick,
    resolvePending,
    clearPending,
  } = useChartNavigation({chartRef, candleData, displayCandles, defaultCandles, onVisibleRangeChange});

  // 차트 크기 조정을 위한 디바운스된 함수
  const debouncedResize = useMemo(
    () => debounce(() => {
      if (chartContainerRef.current) {
        const rect = chartContainerRef.current.getBoundingClientRect();
        setChartSize({width: rect.width, height: rect.height});
      }
    }, 150),
    []
  );

  // 차트 컨테이너 크기 추적 — ResizeObserver 가 창 리사이즈·사이드바/패널 전환·탭 숨김→표시 등
  // "왜 바뀌었든" 컨테이너 박스 변화를 전부 잡는다 (원인별 setTimeout/전역 이벤트 불필요).
  // 숨김 상태(크기 0)는 chartSize 적용 effect 의 width > 0 가드가 걸러낸다.
  useEffect(() => {
    const el = chartContainerRef.current;
    if (!el) return;
    const observer = new ResizeObserver(() => debouncedResize());
    observer.observe(el);
    debouncedResize(); // 초기 크기 설정

    return () => {
      observer.disconnect();
      debouncedResize.cancel();
    };
  }, [debouncedResize]);

  // TradeTable(React.memo) 이 prop 동등성으로 리렌더를 건너뛸 수 있도록 안정적인 참조로 고정
  const toggleTradePanel = useCallback(() => {
    setPanelCollapsed((v) => !v);
  }, []);

  // 차트/패널 분할 핸들 드래그 — 왼쪽으로 끌면 패널이 커지고 차트가 줄어든다
  const handleSplitMouseDown = useCallback((e) => {
    e.preventDefault();
    const startX = e.clientX;
    const startWidth = panelWidth;
    const containerWidth = rootRef.current
      ? rootRef.current.getBoundingClientRect().width
      : window.innerWidth;
    const maxWidth = containerWidth * 0.5;

    const onMouseMove = (ev) => {
      const next = Math.min(maxWidth, Math.max(280, startWidth + (startX - ev.clientX)));
      setPanelWidth(next);
    };
    const onMouseUp = () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
      document.body.style.userSelect = '';
      document.body.style.cursor = '';
    };

    document.body.style.userSelect = 'none';
    document.body.style.cursor = 'col-resize';
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  }, [panelWidth]);

  // 차트 초기화 - DOM 마운트 후 실행
  useEffect(() => {
    const initializeChart = () => {
      if (!chartContainerRef.current) {
        setTimeout(initializeChart, 100);
        return;
      }

      // 초기 크기는 임시값 — 실제 크기는 아래 chartSize effect 가 applyOptions 로 적용
      chartRef.current = createChart(chartContainerRef.current, {
        width: 800,
        height: 350,
        layout: {
          background: {color: CHART_COLORS.background},
          textColor: CHART_COLORS.text,
        },
        grid: {
          vertLines: {color: CHART_COLORS.grid},
          horzLines: {color: CHART_COLORS.grid},
        },
        crosshair: {
          mode: 1, // Normal crosshair mode
        },
        rightPriceScale: {
          borderColor: CHART_COLORS.scaleBorder,
          scaleMargins: {
            top: 0.1,
            bottom: 0.1,
          },
        },
        timeScale: {
          borderColor: CHART_COLORS.scaleBorder,
          timeVisible: true,
          secondsVisible: false,
          rightOffset: 12,
          barSpacing: 3,
          fixLeftEdge: false,
          lockVisibleTimeRangeOnResize: true,
          rightBarStaysOnScroll: true,
          shiftVisibleRangeOnNewBar: true,
        },
        localization: {
          timeFormatter: (timestamp) => {
            const date = new Date(timestamp * 1000);
            const year = date.getUTCFullYear();
            const month = String(date.getUTCMonth() + 1).padStart(2, '0');
            const day = String(date.getUTCDate()).padStart(2, '0');
            const hours = String(date.getUTCHours()).padStart(2, '0');
            const minutes = String(date.getUTCMinutes()).padStart(2, '0');
            return `${year}-${month}-${day} ${hours}:${minutes}`;
          },
        },
        handleScroll: {
          mouseWheel: true,
          pressedMouseMove: true,
          horzTouchDrag: true,
          vertTouchDrag: true,
        },
        handleScale: {
          axisPressedMouseMove: true,
          mouseWheel: true,
          pinch: true,
        },
      });

    };

    initializeChart();
    candleRef.current = chartRef.current.addSeries(CandlestickSeries, {
      lastValueVisible: false,
      priceLineVisible: false,
      // muted 톤 — 마커/포지션 연결선(TRADE_COLORS)이 캔들 위로 도드라지게
      upColor: CANDLE_COLORS.up,
      downColor: CANDLE_COLORS.down,
      wickUpColor: CANDLE_COLORS.up,
      wickDownColor: CANDLE_COLORS.down,
      borderVisible: false,
    });

    // 포지션 구간 연결선 + 선택 하이라이트를 그리는 primitive
    segmentsRef.current = new TradeSegmentsPrimitive();
    candleRef.current.attachPrimitive(segmentsRef.current);

    // 마커 클릭 → 해당 거래 선택 (마커에 준 id 가 hoveredObjectId 로 넘어온다)
    chartRef.current.subscribeClick((param) => {
      if (param.hoveredObjectId != null) {
        setSelectedTradeTime(Number(param.hoveredObjectId));
      }
    });
    // 마커 위에 올라가면 커서를 pointer 로 — 클릭 가능함을 표시
    chartRef.current.subscribeCrosshairMove((param) => {
      if (chartContainerRef.current) {
        chartContainerRef.current.style.cursor = param.hoveredObjectId != null ? 'pointer' : '';
      }
    });

    return () => {
      if (chartRef.current) {
        try {
          chartRef.current.remove();
        } catch (error) {
          console.warn('Error removing chart on cleanup:', error);
        }
        chartRef.current = null;
      }
    };
    // setSelectedTradeTime 은 useState setter 라 참조가 안정적 — 사실상 한 번만 실행
  }, [setSelectedTradeTime]);

  // 차트 크기 업데이트
  useEffect(() => {
    if (chartRef.current && chartSize.width > 0) {
      try {
        chartRef.current.applyOptions({
          width: chartSize.width,
          height: Math.max(chartSize.height, 300),
        });
      } catch (error) {
        console.warn('Error updating chart size:', error);
      }
    }
  }, [chartSize]);

  // 인디케이터 가시성 초기화
  useEffect(() => {
    if (indicatorData && typeof indicatorData === 'object') {
      setVisibleIndicators(prev => {
        const next = {};
        Object.keys(indicatorData).forEach(columnName => {
          // 기존 상태가 있으면 유지, 없으면 true로 초기화
          next[columnName] = prev[columnName] !== undefined ? prev[columnName] : true;
        });
        return next;
      });
    }
  }, [indicatorData]);

  // 인디케이터 가시성 토글 — state 만 갱신하면 시리즈 생성 effect 가 켜진 컬럼만 다시 만든다
  // (꺼진 컬럼은 시리즈 자체가 제거되고, 그룹의 마지막 시리즈가 빠지면 lightweight-charts 가
  //  빈 pane 을 자동 제거해 캔들 영역이 다시 넓어진다)
  const toggleIndicatorVisibility = useCallback((columnName) => {
    setVisibleIndicators(prev => ({
      ...prev,
      [columnName]: !prev[columnName]
    }));
  }, []);

  // 전체 인디케이터 가시성 토글 함수
  const toggleAllIndicators = useCallback(() => {
    const newVisibility = !allIndicatorsVisible;
    setAllIndicatorsVisible(newVisibility);

    const updatedVisibility = {};
    Object.keys(indicatorData).forEach(columnName => {
      updatedVisibility[columnName] = newVisibility;
    });
    setVisibleIndicators(updatedVisibility);
  }, [allIndicatorsVisible, indicatorData]);

  // 캔들 데이터 업데이트 (lazy 로드로 candleData 가 자주 갱신되므로,
  // 초기 뷰 조정은 run 이 바뀔 때 딱 한 번만 한다. 이후 병합 갱신은 setData 만 → 사용자의 뷰 유지)
  useEffect(() => {
    if (!chartRef.current || !candleRef.current) return;
    if (displayCandles.length === 0) return;

    const chart = chartRef.current;
    candleRef.current.setData(displayCandles);

    // run 이 바뀌었거나 타임프레임이 바뀌면(봉 개수가 크게 달라짐) 뷰를 다시 맞춘다.
    if (lastRunRef.current !== selectedBacktestFile || lastTimeframeRef.current !== timeframe) {
      lastRunRef.current = selectedBacktestFile;
      lastTimeframeRef.current = timeframe;
      // 최근 shard 를 먼저 로드하므로 가장 최근 캔들이 보이도록 우측을 맞춘다
      clearPending();
      chart.timeScale().fitContent();
      chart.timeScale().setVisibleLogicalRange({
        from: Math.max(0, displayCandles.length - defaultCandles),
        to: displayCandles.length,
      });
    } else {
      // 로드 전 구간으로의 점프 요청(거래 클릭/타임라인 pan)이 있었으면, 방금 도착한
      // 데이터로 충족됐는지 확인하고 이동을 재시도한다.
      resolvePending();
    }
  }, [displayCandles, selectedBacktestFile, timeframe, clearPending, resolvePending]);

  // 보이는 시간 구간 변화 → App 에 알려 필요한 shard 를 lazy 로드 + 타임라인 셀렉터 하이라이트 갱신
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    const notify = debounce((range) => {
      if (!range) return;
      const cb = onRangeChangeRef.current;
      if (cb) cb(range.from, range.to);
    }, 200);

    const handleVisibleTimeRangeChange = (range) => {
      setVisibleTimeRange(range);
      notify(range);
    };

    const timeScale = chart.timeScale();
    timeScale.subscribeVisibleTimeRangeChange(handleVisibleTimeRangeChange);

    return () => {
      try {
        timeScale.unsubscribeVisibleTimeRangeChange(handleVisibleTimeRangeChange);
      } catch (e) {
        // 차트가 이미 제거된 경우 무시
      }
      notify.cancel();
    };
  }, []);

  // 토글 상태가 바뀌면 시리즈 생성 effect 를 다시 돌리기 위한 안정 키 —
  // 켜져 있는 컬럼 집합이 실제로 바뀔 때만 값이 달라진다.
  const enabledKey = useMemo(
    () => Object.keys(displayIndicators || {})
      .filter((c) => visibleIndicators[c] !== false)
      .join('|'),
    [displayIndicators, visibleIndicators]
  );

  // 인디케이터 데이터 업데이트 — 켜져 있는 컬럼만 시리즈를 만든다. 꺼진 컬럼은 시리즈를
  // 아예 만들지 않으므로, 그룹의 지표가 모두 꺼지면 removeSeries 시점에 빈 pane 이
  // 자동 제거돼(cleanupIfPaneIsEmpty) 캔들 pane 이 그 공간을 되찾는다.
  useEffect(() => {
    if (!chartRef.current) return;

    const chart = chartRef.current;

    // 기존 pane 라벨 제거 — removeSeries 로 pane 이 사라지기 전에 떼어야 한다
    paneLabelsRef.current.forEach((label) => {
      try {
        label.detach();
      } catch (error) {
        console.warn('Error detaching pane label:', error);
      }
    });
    paneLabelsRef.current = [];

    // 기존 인디케이터 시리즈 제거
    Object.values(seriesRefsRef.current).forEach(series => {
      chart.removeSeries(series);
    });
    seriesRefsRef.current = {};

    const columnNames = Object.keys(displayIndicators || {});
    const enabledColumns = columnNames.filter(
      (c) => visibleIndicatorsRef.current[c] !== false
    );

    if (enabledColumns.length > 0) {
      // price 가 아닌 그룹(ATR, AGE, balance 등)은 첫 등장 순서대로 별도 pane(1, 2, ...)에
      // 배정 — pane 마다 'right' 스케일이 독립적이므로, 같은 그룹(=같은 pane)의 컬럼끼리는
      // priceScaleId 를 따로 지정하지 않아도 그 pane 의 기본 'right' 축을 공유한다.
      // (커스텀 priceScaleId 는 축 없는 오버레이 스케일이 되어 단위/눈금이 표시되지 않는다.)
      const groupPanes = {};
      let nextPaneIndex = 1;
      for (const columnName of enabledColumns) {
        const group = (indicatorGroups && indicatorGroups[columnName]) || 'price';
        if (group !== 'price' && groupPanes[group] === undefined) {
          groupPanes[group] = nextPaneIndex++;
        }
      }

      for (const columnName of enabledColumns) {
        const seriesData = displayIndicators[columnName];
        if (Array.isArray(seriesData) && seriesData.length > 0) {
          try {
            // 색은 전체 컬럼 목록 기준 인덱스로 고정 — 앞 컬럼이 꺼져도 색이 밀리지 않고
            // 범례 체크박스(전체 목록 순서로 색 배정)와 항상 일치한다.
            const color = INDICATOR_COLORS[columnNames.indexOf(columnName) % INDICATOR_COLORS.length];
            const group = (indicatorGroups && indicatorGroups[columnName]) || 'price';
            const isOverlay = group === 'price';

            const seriesOptions = {
              color: color,
              lineWidth: 1,
              lineStyle: 0, // Solid line
              lineType: 1, // Step line (0: Simple, 1: WithSteps, 2: Curved)
              crosshairMarkerRadius: 1,
              lastValueVisible: false,
              priceLineVisible: false,
            };

            const lineSeries = isOverlay
              ? chart.addSeries(LineSeries, seriesOptions)
              : chart.addSeries(LineSeries, seriesOptions, groupPanes[group]);

            lineSeries.setData(seriesData);

            // 시리즈 참조 저장
            seriesRefsRef.current[columnName] = lineSeries;
          } catch (error) {
            console.error(`Error adding indicator series ${columnName}:`, error);
          }
        }
      }

      // 각 인디케이터 pane 좌상단에 그룹 이름을 표시한다. 캔들 pane(0)은 가격 차트임이
      // 자명하므로 라벨을 붙이지 않는다.
      for (const [group, paneIndex] of Object.entries(groupPanes)) {
        try {
          const label = createTextWatermark(chart.panes()[paneIndex], {
            horzAlign: 'left',
            vertAlign: 'top',
            lines: [{text: group, color: 'rgba(193, 194, 197, 0.5)', fontSize: 12}],
          });
          paneLabelsRef.current.push(label);
        } catch (error) {
          console.warn(`Error adding pane label for group ${group}:`, error);
        }
      }

      // 캔들 pane(0)이 항상 최소 80% 를 차지하도록 보장한다. pane.setHeight() 는 내부적으로
      // stretch factor(비율)를 갱신하며 그 차이를 "나머지 pane 들"에 고르게 재분배하므로,
      // 이 한 번의 호출만으로 인디케이터 pane 이 몇 개든 나머지 20% 를 나눠 갖게 된다 — 이후
      // 창 크기 조절에도 stretch factor(비율) 기반이라 80/20 비율이 그대로 유지된다.
      // pane 이 1개(비-price 지표 없음)뿐이면 라이브러리가 이 호출을 그냥 무시한다.
      if (Object.keys(groupPanes).length > 0) {
        try {
          const panes = chart.panes();
          const totalHeight = panes.reduce((sum, p) => sum + p.getHeight(), 0);
          if (totalHeight > 0) {
            panes[0].setHeight(totalHeight * 0.8);
          }
        } catch (error) {
          console.warn('Error sizing candle pane:', error);
        }
      }
    }
    // enabledKey: 토글로 켜진 컬럼 집합이 바뀔 때 재생성 (값 자체는 여기서 안 씀)
  }, [displayIndicators, indicatorGroups, enabledKey]);

  // 거래 데이터 업데이트
  // lightweight-charts 는 시리즈에 없는 time 의 마커에 대해 경고를 내므로,
  // 현재 로드된 캔들의 시간 범위 안에 드는 마커만 표시한다. (lazy 로드로 candleData 가 바뀌면 재계산)
  useEffect(() => {
    if (!chartRef.current || !candleRef.current) return;
    if (!markersRef.current) {
      markersRef.current = createSeriesMarkers(candleRef.current, []);
    }

    if (!tradeData || tradeData.length === 0 || displayCandles.length === 0) {
      markersRef.current.setMarkers([]);
      return;
    }

    // 마커는 시리즈에 존재하는 캔들 time 에만 붙어야 하므로, 거래 시각을 현재
    // 타임프레임 버킷으로 스냅한 뒤, 로드된 집계 캔들 범위 안에 드는 것만 표시한다.
    // 상세 수치(수량/가격)는 우측 Trade History 가 담당하므로 텍스트는 붙이지 않는다.
    const minTime = displayCandles[0].time;
    const maxTime = displayCandles[displayCandles.length - 1].time;

    const tradeMarkers = tradeData
      .map(trade => ({trade, time: bucketTime(trade.time, timeframe)}))
      .filter(({time}) => time >= minTime && time <= maxTime)
      .map(({trade, time}) => {
        const isBuy = trade.quantity > 0; // quantity가 양수면 Buy, 음수면 Sell
        const isSelected = trade.time === selectedTradeTime;
        return {
          id: String(trade.time), // 클릭 시 hoveredObjectId 로 돌려받아 거래를 식별
          time,
          position: isBuy ? 'belowBar' : 'aboveBar',
          color: isSelected
            ? TRADE_COLORS.selected
            : (isBuy ? TRADE_COLORS.buy : TRADE_COLORS.sell),
          shape: isBuy ? 'arrowUp' : 'arrowDown',
          size: isSelected ? 3 : 2,
        };
      });

    try {
      markersRef.current.setMarkers(tradeMarkers);
    } catch (error) {
      console.warn('Error setting trade markers:', error);
    }
  }, [tradeData, displayCandles, timeframe, selectedTradeTime]);

  // 포지션 구간(진입 평단→청산가) — 전체 거래 기준으로 한 번 계산해 두고,
  // 표시 데이터/타임프레임/선택이 바뀔 때마다 primitive 에 넘겨 다시 그린다.
  const positionSegments = useMemo(
    () => buildPositionSegments(tradeData || []),
    [tradeData]
  );
  useEffect(() => {
    if (!segmentsRef.current) return;
    segmentsRef.current.setData({
      segments: positionSegments,
      timeframe,
      selectedTime: selectedTradeTime,
      candles: displayCandles,
    });
  }, [positionSegments, timeframe, selectedTradeTime, displayCandles]);

  return (
    <div
      ref={rootRef}
      style={{display: 'flex', height: '100%', padding: 'var(--mantine-spacing-sm)', overflow: 'hidden'}}
    >
      <Stack gap="sm" style={{flex: 1, minWidth: 0}}>
      {/* 툴바: 데이터 요약 + 뷰 컨트롤 */}
      <Group justify="space-between" wrap="nowrap">
        <Group gap="md" wrap="nowrap" style={{overflow: 'hidden'}}>
          <Text size="xs" c="dimmed" truncate>
            Candles <Text span size="xs" fw={600} c="bright">{displayCandles.length.toLocaleString()}</Text>
            {tradeData && tradeData.length > 0 && (
              <>
                {' · '}Trades <Text span size="xs" fw={600} c="bright">{tradeData.length.toLocaleString()}</Text>
              </>
            )}
            {memoryMB !== null && (
              <>
                {' · '}Mem <Text span size="xs" fw={600} c="bright">{memoryMB.toLocaleString()} MB</Text>
              </>
            )}
          </Text>
        </Group>

        <Group gap={4} wrap="nowrap">
          <Button
            variant="default"
            size="compact-xs"
            leftSection={<IconChevronsLeft size={13}/>}
            onClick={() => {
              // run 의 실제 시작 시점으로 — 그 구간이 아직 로드 전이면 panTo 가
              // pendingPan 경로로 lazy 로드를 요청하고 도착 시 이동한다.
              if (fullRange) {
                panTo(fullRange.from, fullRange.from + defaultCandles * timeframe);
                return;
              }
              try {
                if (chartRef.current) {
                  chartRef.current.timeScale().setVisibleLogicalRange({
                    from: 0,
                    to: defaultCandles
                  });
                }
              } catch (error) {
                console.warn('Error scrolling to first:', error);
              }
            }}
          >
            First
          </Button>
          <Button
            variant="default"
            size="compact-xs"
            leftSection={<IconArrowsMaximize size={13}/>}
            onClick={() => {
              try {
                if (chartRef.current) {
                  chartRef.current.timeScale().fitContent();
                }
              } catch (error) {
                console.warn('Error fitting content:', error);
              }
            }}
          >
            Fit
          </Button>
          <Button
            variant="default"
            size="compact-xs"
            rightSection={<IconChevronsRight size={13}/>}
            onClick={() => {
              try {
                if (chartRef.current) {
                  chartRef.current.timeScale().setVisibleLogicalRange({
                    from: displayCandles.length - defaultCandles,
                    to: displayCandles.length
                  });
                }
              } catch (error) {
                console.warn('Error scrolling to latest:', error);
              }
            }}
          >
            Latest
          </Button>
        </Group>
      </Group>

      {/* 전체 백테스트 구간 대비 현재 뷰 — 클릭 이동/드래그 패닝, 매수·매도 세로선 */}
      <TimelineSelector
        fullRange={fullRange}
        visibleRange={visibleTimeRange}
        trades={tradeData}
        equity={equityData}
        onPan={panTo}
        timeframe={timeframe}
      />

      {/* 인디케이터 범례 (클릭으로 시리즈 표시/숨김) */}
      {indicatorData && Object.keys(indicatorData).length > 0 && (
        <Group gap="sm" wrap="wrap">
          <Switch
            size="xs"
            label="All Indicators"
            checked={allIndicatorsVisible}
            onChange={toggleAllIndicators}
          />
          {Object.keys(indicatorData).map((columnName, index) => {
            const color = INDICATOR_COLORS[index % INDICATOR_COLORS.length];
            const isVisible = visibleIndicators[columnName] !== undefined
              ? visibleIndicators[columnName]
              : true;

            return (
              <Checkbox
                key={columnName}
                size="xs"
                color={color}
                checked={isVisible}
                onChange={() => toggleIndicatorVisibility(columnName)}
                label={
                  <Text size="xs" fw={500} style={{color: isVisible ? color : 'var(--mantine-color-dimmed)'}}>
                    {columnName}
                  </Text>
                }
              />
            );
          })}
        </Group>
      )}

      {/* 차트 */}
      <Paper
        withBorder
        radius="md"
        pos="relative"
        style={{flex: 1, minHeight: 0, overflow: 'hidden', background: CHART_COLORS.background}}
      >
        {/* 타임프레임 선택 (TradingView 처럼 차트 좌상단에 간단히) */}
        <SegmentedControl
          className="timeframe-overlay"
          size="xs"
          withItemsBorders={false}
          value={String(timeframe)}
          onChange={(value) => setTimeframe(Number(value))}
          data={TIMEFRAMES.map((tf) => ({label: tf.label, value: String(tf.seconds)}))}
        />

        <div ref={chartContainerRef} className="chart-container"/>
      </Paper>

      </Stack>

      {/* 우측 거래 패널 (폭은 드래그 핸들로 조절) */}
      {tradeData && tradeData.length > 0 && (
        <>
          {!panelCollapsed && (
            <div
              className="split-handle split-handle--vertical"
              role="separator"
              aria-orientation="vertical"
              onMouseDown={handleSplitMouseDown}
            />
          )}
          <TradeTable
            data={tradeData}
            width={panelWidth}
            collapsed={panelCollapsed}
            onToggleCollapsed={toggleTradePanel}
            onTradeClick={handleTradeClick}
            selectedTradeTime={selectedTradeTime}
          />
        </>
      )}
    </div>
  );
};

export default TradingChart;
