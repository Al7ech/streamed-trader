import React, {useEffect, useMemo, useRef} from 'react';
import {createTextWatermark, HistogramSeries, LineSeries} from 'lightweight-charts';
import {Paper, Text} from '@mantine/core';
import {BarValueLabelsPrimitive} from './BarValueLabelsPrimitive';
import {CHART_COLORS, EQUITY_COLOR, INDICATOR_COLORS, TRADE_COLORS} from '../../theme';
import {fmtSignedPct} from '../../utils/format';
import {monthKey, monthStartSec} from '../../utils/monthlyStats';
import {createStatsChart, installChartShell, PCT_FORMAT, refitChart} from './chartShell';
import '../TradingChart.css';

const WIN_RATE_COLOR = EQUITY_COLOR; // 승률 바 — 손익 색(초록/빨강)과 구분되는 자산 계열 파랑
const TRADE_COUNT_COLOR = INDICATOR_COLORS[1]; // 거래수 라인 — 팔레트의 노랑, 파란 바와 구분

/**
 * 월별 성과 바 차트 — computeMonthlyStats 결과를 3개 pane 으로 그린다:
 * Return(부호별 색) / Win Rate(50% 기준선 + 거래수 라인) / Max DD(음수, 아래로 자라는 바).
 * 각 바에는 값 라벨(BarValueLabelsPrimitive — 좁아지면 자동 솎아냄)을 붙이고,
 * 차트 셸(옵션/숨김 탭 fit/시간축 동기화)은 chartShell.js 를 EquityChart 와 공유한다.
 */
const MonthlyStatsChart = ({monthly, timeSync}) => {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRefs = useRef(null); // {returns, winRates, tradeCounts, mdds} 시리즈
  const labelPrimitives = useRef(null); // {returns, winRates, mdds} 값 라벨 primitive
  const fitStateRef = useRef({fitted: false}); // chartShell 과 공유하는 fit-once 상태

  const seriesData = useMemo(() => {
    const returns = [];
    const winRates = [];
    const tradeCounts = [];
    const mdds = [];
    for (const row of monthly || []) {
      const time = monthStartSec(row.month);
      // 거래수(청산 수) = win rate 의 분모. 청산이 없는 달도 0으로 그려 라인이 끊기지 않게 한다.
      tradeCounts.push({time, value: row.wins + row.losses});
      if (row.returnPct !== null) {
        returns.push({
          time,
          value: row.returnPct,
          color: row.returnPct >= 0 ? TRADE_COLORS.profit : TRADE_COLORS.loss,
          label: fmtSignedPct(row.returnPct),
        });
      }
      if (row.winRate !== null) {
        winRates.push({
          time,
          value: row.winRate,
          color: WIN_RATE_COLOR,
          label: `${Math.round(row.winRate * 100)}%`,
        });
      }
      if (row.maxDD !== null) {
        mdds.push({
          time,
          value: -row.maxDD,
          color: TRADE_COLORS.loss,
          label: `${(-row.maxDD * 100).toFixed(1)}%`,
        });
      }
    }
    return {returns, winRates, tradeCounts, mdds};
  }, [monthly]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const chart = createStatsChart(container, {
      // 월 단위 데이터 — 크로스헤어 라벨은 YYYY-MM 까지만 (utils/monthlyStats 와 같은 UTC 규약)
      timeFormatter: monthKey,
      // 바 위/아래 값 라벨이 pane 경계에 잘리지 않도록 여백을 넉넉히
      scaleMargins: {top: 0.2, bottom: 0.15},
    });

    const barDefaults = {
      priceFormat: PCT_FORMAT,
      lastValueVisible: false,
      priceLineVisible: false,
    };
    const returns = chart.addSeries(HistogramSeries, {...barDefaults}, 0);
    const winRate = chart.addSeries(HistogramSeries, {...barDefaults, color: WIN_RATE_COLOR}, 1);
    const mdd = chart.addSeries(HistogramSeries, {...barDefaults, color: TRADE_COLORS.loss}, 2);

    // Win Rate pane 오버레이: 월별 거래수(청산 수) 라인 — 승률의 표본 크기를 같은 자리에서 읽게.
    // %축과 단위가 달라 축 없는 오버레이 스케일('trades')을 쓴다.
    const tradeCount = chart.addSeries(LineSeries, {
      priceScaleId: 'trades',
      color: TRADE_COUNT_COLOR,
      lineWidth: 2,
      priceFormat: {type: 'custom', formatter: (v) => `${Math.round(v)}`, minMove: 1},
      lastValueVisible: false,
      priceLineVisible: false,
      crosshairMarkerRadius: 3,
    }, 1);
    // 바(0~100% 영역)와 겹침을 줄이도록 라인을 pane 중앙대에 배치
    tradeCount.priceScale().applyOptions({scaleMargins: {top: 0.25, bottom: 0.25}});

    // 바 값 라벨 primitive — 바 폭이 좁으면 알아서 솎아 그린다
    labelPrimitives.current = {
      returns: new BarValueLabelsPrimitive(),
      winRates: new BarValueLabelsPrimitive(),
      mdds: new BarValueLabelsPrimitive(),
    };
    returns.attachPrimitive(labelPrimitives.current.returns);
    winRate.attachPrimitive(labelPrimitives.current.winRates);
    mdd.attachPrimitive(labelPrimitives.current.mdds);

    // 승률 50% 기준선 — 엣지 유무를 바로 읽을 수 있게
    winRate.createPriceLine({
      price: 0.5,
      color: TRADE_COLORS.flat,
      lineWidth: 1,
      lineStyle: 2, // Dashed
      axisLabelVisible: false,
    });

    // 각 pane 좌상단에 지표명 (TradingChart 의 pane 라벨 패턴).
    // Win Rate pane 은 시리즈가 2개라 'Trades' 라인의 정체를 색으로 함께 표기한다.
    const winRateLabelLine = {text: 'Win Rate', color: 'rgba(193, 194, 197, 0.5)', fontSize: 12};
    const tradesLabelLine = {text: 'Trades', color: 'rgba(255, 212, 59, 0.6)', fontSize: 11};
    createTextWatermark(chart.panes()[0], {
      horzAlign: 'left',
      vertAlign: 'top',
      lines: [{text: 'Return', color: 'rgba(193, 194, 197, 0.5)', fontSize: 12}],
    });
    const winRatePaneLabel = createTextWatermark(chart.panes()[1], {
      horzAlign: 'left',
      vertAlign: 'top',
      lines: [winRateLabelLine, tradesLabelLine],
    });
    createTextWatermark(chart.panes()[2], {
      horzAlign: 'left',
      vertAlign: 'top',
      lines: [{text: 'Max DD', color: 'rgba(193, 194, 197, 0.5)', fontSize: 12}],
    });

    // hover 시 거래수 표시 — 'trades' 오버레이 스케일은 축이 없어 크로스헤어 라벨이 안 뜨므로,
    // Win Rate pane 의 'Trades' 워터마크 라인을 hover 한 달의 수치로 갱신한다.
    chart.subscribeCrosshairMove((param) => {
      const point = param.seriesData && param.seriesData.get(tradeCount);
      const text = point ? `Trades: ${Math.round(point.value)}` : 'Trades';
      if (text === tradesLabelLine.text) return; // 변화 없으면 재적용 생략
      tradesLabelLine.text = text;
      winRatePaneLabel.applyOptions({lines: [winRateLabelLine, tradesLabelLine]});
    });

    chartRef.current = chart;
    seriesRefs.current = {returns, winRates: winRate, tradeCounts: tradeCount, mdds: mdd};

    const cleanupShell = installChartShell(chart, container, {
      timeSync,
      fitState: fitStateRef.current,
    });

    return () => {
      cleanupShell();
      chart.remove();
      chartRef.current = null;
      seriesRefs.current = null;
      labelPrimitives.current = null;
    };
  }, [timeSync]);

  useEffect(() => {
    const series = seriesRefs.current;
    if (!series) return;
    series.returns.setData(seriesData.returns);
    series.winRates.setData(seriesData.winRates);
    series.tradeCounts.setData(seriesData.tradeCounts);
    series.mdds.setData(seriesData.mdds);
    const labels = labelPrimitives.current;
    labels.returns.setData({points: seriesData.returns});
    labels.winRates.setData({points: seriesData.winRates});
    labels.mdds.setData({points: seriesData.mdds});
    refitChart(chartRef.current, containerRef.current, fitStateRef.current, seriesData.tradeCounts.length > 0);
  }, [seriesData]);

  return (
    <Paper
      withBorder
      radius="md"
      pos="relative"
      h="100%"
      style={{overflow: 'hidden', background: CHART_COLORS.background}}
    >
      {(!monthly || monthly.length === 0) && (
        <Text
          size="sm"
          c="dimmed"
          pos="absolute"
          top="50%"
          left="50%"
          style={{transform: 'translate(-50%, -50%)', zIndex: 1}}
        >
          표시할 거래/자산 데이터가 없습니다
        </Text>
      )}
      <div ref={containerRef} className="chart-container" style={{minHeight: 220}}/>
    </Paper>
  );
};

export default MonthlyStatsChart;
