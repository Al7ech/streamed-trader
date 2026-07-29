import React, {useEffect, useMemo, useRef, useState} from 'react';
import {
  AreaSeries,
  BaselineSeries,
  createSeriesMarkers,
  createTextWatermark,
  LineSeries,
  PriceScaleMode,
} from 'lightweight-charts';
import {Group, Paper, SegmentedControl, Text} from '@mantine/core';
import {BASE_ASSET_COLOR, CHART_COLORS, EQUITY_COLOR, TRADE_COLORS} from '../../theme';
import {fmtPct} from '../../utils/format';
import {computeDrawdownSeries} from '../../utils/runStats';
import {createStatsChart, installChartShell, PCT_FORMAT, refitChart} from './chartShell';
import '../TradingChart.css';

const BENCHMARK_COLOR = TRADE_COLORS.flat; // 기준선 — 매수/매도/손익 색과 겹치지 않는 무채색

// 시작→끝 자산을 잇는 상수 성장률(기하) 기준선: v(t) = v0 · (vN/v0)^((t−t0)/(tN−t0)).
// 로그 스케일에선 직선, 리니어에선 지수 곡선이 되어, 실제 equity 가 기준선보다 위면
// 평균보다 빨리(더 많이), 아래면 느리게(덜) 번 구간임을 바로 읽을 수 있다.
const buildBenchmark = (equity) => {
  if (!equity || equity.length < 2) return [];
  const first = equity[0];
  const last = equity[equity.length - 1];
  const span = last.time - first.time;
  // 기하 성장은 양수 자산에서만 정의된다 (파산/음수 margin run 은 기준선 생략)
  if (span <= 0 || first.value <= 0 || last.value <= 0) return [];
  const growth = last.value / first.value;
  return equity.map((p) => ({
    time: p.time,
    value: first.value * Math.pow(growth, (p.time - first.time) / span),
  }));
};

/**
 * 백테스트 전체 구간의 자산(equity) 변화 차트. run JSON 의 다운샘플 equity 를 그대로 그린다.
 * 장기 복리 run 은 초반 변화가 눌려 보이므로 우상단에 Linear/Log 스케일 토글을 둔다.
 * 하단 pane 에 러닝피크 대비 underwater(drawdown) 곡선을 함께 그리고,
 * 최대 낙폭 지점(최하점)은 그 곡선 위에 점 마커로 표시한다.
 * timeSync(chartSync.js)가 넘어오면 아래 월별 성과 차트와 시간축을 동기화한다.
 *
 * baseAssetData(기초자산 buy & hold, schema v2+)가 있으면 equity 와 같은 가격축에 겹쳐 그리고
 * (둘 다 init_margin 에서 시작하는 같은 단위라 정규화가 필요 없다) 맨 아래 pane 에
 * 상대강도(equity ÷ 기초자산 − 1) 곡선을 추가한다. 없으면(구 run) 둘 다 만들지 않는다.
 */
const EquityChart = ({equityData, baseAssetData, relativeData, mddRatio, timeSync}) => {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const benchmarkRef = useRef(null); // 상수 성장률 기준선 시리즈
  const baseAssetRef = useRef(null); // 기초자산 buy & hold 시리즈 (구 run 에서는 null)
  const drawdownRef = useRef(null); // 하단 pane 의 underwater 시리즈
  const relativeRef = useRef(null); // 최하단 pane 의 상대강도 시리즈 (구 run 에서는 null)
  const troughMarkersRef = useRef(null); // drawdown 최하점 마커 plugin
  const fitStateRef = useRef({fitted: false}); // chartShell 과 공유하는 fit-once 상태
  const [scaleMode, setScaleMode] = useState('linear');

  const benchmarkData = useMemo(() => buildBenchmark(equityData), [equityData]);
  const drawdownData = useMemo(() => computeDrawdownSeries(equityData), [equityData]);
  // 시리즈/pane 구성 자체가 바뀌므로 마운트 이펙트의 의존성으로 쓴다 (run 전환 시 어차피 재생성)
  const hasBaseAsset = Boolean(baseAssetData && baseAssetData.length > 0);

  // drawdown 곡선의 최하점(= max drawdown 시점). 마커는 시리즈에 존재하는 time 에만 붙일 수
  // 있으므로 summary 의 trough_timestamp 대신 다운샘플 곡선 자체의 최소값 포인트를 쓴다.
  const trough = useMemo(() => {
    let min = null;
    for (const p of drawdownData) {
      if (p.value < 0 && (min === null || p.value < min.value)) min = p;
    }
    return min;
  }, [drawdownData]);

  // 범례에 보여줄 MDD 비율 — summary 값(per-candle 정확치)이 있으면 그걸 우선
  const legendMdd = mddRatio !== undefined && mddRatio > 0
    ? mddRatio
    : (trough ? -trough.value : null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const chart = createStatsChart(container, {
      // TradingChart 와 같은 UTC 규약 (equity 는 다운샘플이라 날짜 단위면 충분)
      timeFormatter: (timestamp) => new Date(timestamp * 1000).toISOString().slice(0, 10),
      scaleMargins: {top: 0.1, bottom: 0.1},
    });
    const series = chart.addSeries(AreaSeries, {
      lineColor: EQUITY_COLOR,
      topColor: 'rgba(77, 171, 247, 0.25)', // EQUITY_COLOR 의 반투명 채움
      bottomColor: 'rgba(77, 171, 247, 0.0)',
      lineWidth: 2,
      lastValueVisible: false,
      priceLineVisible: false,
    });
    const benchmark = chart.addSeries(LineSeries, {
      color: BENCHMARK_COLOR,
      lineWidth: 1,
      lineStyle: 2, // Dashed — 실제 equity 와 확실히 구분
      lastValueVisible: false,
      priceLineVisible: false,
      crosshairMarkerVisible: false,
    });
    // 기초자산 보유 곡선 — equity 와 같은 단위/같은 pane 0 right 스케일이라
    // Linear/Log 토글(아래)도 자동으로 함께 적용된다.
    const baseAsset = hasBaseAsset ? chart.addSeries(LineSeries, {
      color: BASE_ASSET_COLOR,
      lineWidth: 2,
      lastValueVisible: false,
      priceLineVisible: false,
    }) : null;
    // 하단 pane(1): underwater 곡선. BaselineSeries 로 0 기준선과 곡선 사이(잠긴 깊이)를 채운다.
    const drawdown = chart.addSeries(BaselineSeries, {
      baseValue: {type: 'price', price: 0},
      bottomLineColor: TRADE_COLORS.loss,
      bottomFillColor1: 'rgba(255, 77, 103, 0.05)',
      bottomFillColor2: 'rgba(255, 77, 103, 0.30)',
      topLineColor: TRADE_COLORS.loss,
      topFillColor1: 'rgba(0, 0, 0, 0)',
      topFillColor2: 'rgba(0, 0, 0, 0)',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
      priceFormat: PCT_FORMAT,
    }, 1);
    // 최하단 pane(2): 기초자산 대비 초과 성과. 0 위(초록)면 그냥 들고 있는 것보다 앞선 구간.
    const relative = hasBaseAsset ? chart.addSeries(BaselineSeries, {
      baseValue: {type: 'price', price: 0},
      topLineColor: TRADE_COLORS.profit,
      topFillColor1: 'rgba(0, 230, 118, 0.30)',
      topFillColor2: 'rgba(0, 230, 118, 0.05)',
      bottomLineColor: TRADE_COLORS.loss,
      bottomFillColor1: 'rgba(255, 77, 103, 0.05)',
      bottomFillColor2: 'rgba(255, 77, 103, 0.30)',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
      priceFormat: PCT_FORMAT,
    }, 2) : null;
    if (relative) {
      // pane 라벨 — MonthlyStatsChart 와 같은 워터마크 패턴
      createTextWatermark(chart.panes()[2], {
        horzAlign: 'left',
        vertAlign: 'top',
        lines: [{text: '기초자산 대비', color: 'rgba(193, 194, 197, 0.5)', fontSize: 12}],
      });
    }

    chartRef.current = chart;
    seriesRef.current = series;
    benchmarkRef.current = benchmark;
    baseAssetRef.current = baseAsset;
    drawdownRef.current = drawdown;
    relativeRef.current = relative;
    troughMarkersRef.current = createSeriesMarkers(drawdown, []);

    const cleanupShell = installChartShell(chart, container, {
      timeSync,
      fitState: fitStateRef.current,
      // 실제 크기가 잡힌 뒤 한 번만 equity pane 비율을 조정한다 — 하단 보조 pane 이 1개면 85%,
      // 상대강도 pane 까지 2개면 70% 를 주고 나머지를 반씩 나눈다.
      // (setHeight 는 stretch factor 기반이라 이후 리사이즈에도 비율이 유지된다 — TradingChart 참고)
      onFirstSize: () => {
        const panes = chart.panes();
        const totalHeight = panes.reduce((sum, p) => sum + p.getHeight(), 0);
        if (panes.length < 2 || totalHeight <= 0) return;
        if (panes.length > 2) {
          panes[0].setHeight(totalHeight * 0.70);
          panes[1].setHeight(totalHeight * 0.15);
        } else {
          panes[0].setHeight(totalHeight * 0.85);
        }
      },
    });

    return () => {
      cleanupShell();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      benchmarkRef.current = null;
      baseAssetRef.current = null;
      drawdownRef.current = null;
      relativeRef.current = null;
      troughMarkersRef.current = null;
    };
  }, [timeSync, hasBaseAsset]);

  useEffect(() => {
    if (!seriesRef.current) return;
    seriesRef.current.setData(equityData || []);
    benchmarkRef.current.setData(benchmarkData);
    if (baseAssetRef.current) baseAssetRef.current.setData(baseAssetData || []);
    drawdownRef.current.setData(drawdownData);
    if (relativeRef.current) relativeRef.current.setData(relativeData || []);
    troughMarkersRef.current.setMarkers(trough ? [{
      time: trough.time,
      position: 'inBar', // 데이터 포인트 값 위치에 그대로 찍는다
      color: TRADE_COLORS.loss,
      shape: 'circle',
      size: 1,
    }] : []);
    refitChart(chartRef.current, containerRef.current, fitStateRef.current, (equityData || []).length > 0);
  }, [equityData, baseAssetData, relativeData, benchmarkData, drawdownData, trough]);

  useEffect(() => {
    if (!seriesRef.current) return;
    // equity pane 의 스케일에만 적용 — 하단 drawdown pane 은 항상 linear 유지
    seriesRef.current.priceScale().applyOptions({
      mode: scaleMode === 'log' ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
    });
  }, [scaleMode]);

  return (
    <Paper
      withBorder
      radius="md"
      pos="relative"
      h="100%"
      style={{overflow: 'hidden', background: CHART_COLORS.background}}
    >
      <SegmentedControl
        className="timeframe-overlay"
        size="xs"
        withItemsBorders={false}
        value={scaleMode}
        onChange={setScaleMode}
        data={[{label: 'Linear', value: 'linear'}, {label: 'Log', value: 'log'}]}
      />
      {/* 시리즈가 2개 이상(equity + 기초자산 + 기준선 + drawdown)이라 범례 필수 —
          우측 가격축을 피해 우상단에 겹쳐 둔다 */}
      {benchmarkData.length > 0 && (
        <Group gap="md" wrap="nowrap" pos="absolute" top={10} right={72} style={{zIndex: 5}}>
          <Group gap={6} wrap="nowrap">
            <span style={{width: 14, height: 0, borderTop: `2px solid ${EQUITY_COLOR}`}}/>
            <Text size="xs" c="dimmed">Equity</Text>
          </Group>
          {hasBaseAsset && (
            <Group gap={6} wrap="nowrap">
              <span style={{width: 14, height: 0, borderTop: `2px solid ${BASE_ASSET_COLOR}`}}/>
              <Text size="xs" c="dimmed">기초자산 보유</Text>
            </Group>
          )}
          <Group gap={6} wrap="nowrap">
            <span style={{width: 14, height: 0, borderTop: `2px dashed ${BENCHMARK_COLOR}`}}/>
            <Text size="xs" c="dimmed">평균 성장 기준선</Text>
          </Group>
          <Group gap={6} wrap="nowrap">
            <span style={{width: 14, height: 0, borderTop: `2px solid ${TRADE_COLORS.loss}`}}/>
            <Text size="xs" c="dimmed">Drawdown</Text>
          </Group>
          {legendMdd !== null && trough && (
            <Group gap={6} wrap="nowrap">
              <span
                style={{
                  width: 8,
                  height: 8,
                  borderRadius: '50%',
                  background: TRADE_COLORS.loss,
                }}
              />
              <Text size="xs" c="dimmed">Max DD {fmtPct(legendMdd)}</Text>
            </Group>
          )}
        </Group>
      )}
      {(!equityData || equityData.length === 0) && (
        <Text
          size="sm"
          c="dimmed"
          pos="absolute"
          top="50%"
          left="50%"
          style={{transform: 'translate(-50%, -50%)', zIndex: 1}}
        >
          이 run 에는 equity 데이터가 없습니다 (구 버전 결과 파일)
        </Text>
      )}
      <div ref={containerRef} className="chart-container"/>
    </Paper>
  );
};

export default EquityChart;
