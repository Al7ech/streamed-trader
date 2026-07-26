import React, {useMemo} from 'react';
import {Group, Paper, Stack, Text} from '@mantine/core';
import EquityChart from './EquityChart';
import MonthlyStatsChart from './MonthlyStatsChart';
import {createTimeRangeSync} from '../../utils/chartSync';
import {computeMonthlyStats} from '../../utils/monthlyStats';
import {computeRunStats, getMddRatio} from '../../utils/runStats';
import {fmtDuration, fmtNum, fmtPct, fmtSignedPct} from '../../utils/format';
import {TRADE_COLORS} from '../../theme';

// 상단 KPI 타일 — InfoPanel 의 label/value 스타일을 타일 형태로
const Kpi = ({label, value, color}) => (
  <Paper withBorder radius="md" px="md" py={6} style={{flex: '1 1 0', minWidth: 108}}>
    <Text size="xs" c="dimmed" style={{whiteSpace: 'nowrap'}}>{label}</Text>
    <Text size="sm" fw={700} style={{whiteSpace: 'nowrap', ...(color ? {color} : undefined)}}>
      {value}
    </Text>
  </Paper>
);

const signedColor = (v) => (v === null || v === undefined || v === 0
  ? undefined
  : (v > 0 ? TRADE_COLORS.profit : TRADE_COLORS.loss));

/**
 * Trades 탭 본문: KPI 타일 → 전체 구간 equity 차트 → 월별 성과 바 차트(Return/Win Rate/Max DD).
 * 월별 지표 산출 규칙은 utils/monthlyStats.js 참고 (승률 기준은 헤더 summary 와 동일).
 */
const StatsTab = ({equityData, tradeData, summary}) => {
  const monthly = useMemo(
    () => computeMonthlyStats(equityData, tradeData),
    [equityData, tradeData]
  );

  const totalMdd = getMddRatio(summary);

  const runStats = useMemo(
    () => computeRunStats(equityData, tradeData),
    [equityData, tradeData]
  );
  const calmar = runStats.cagr !== null && totalMdd > 0 ? runStats.cagr / totalMdd : null;

  // equity 차트 ↔ 월별 성과 차트 시간축 동기화 그룹 (컴포넌트 수명 동안 하나)
  const timeSync = useMemo(() => createTimeRangeSync(), []);

  return (
    <Stack gap="sm" p="sm" h="100%" style={{overflow: 'hidden'}}>
      {/* run 전체 KPI — 산출 규칙은 utils/runStats.js (PF/Payoff 는 헤더 승률과 같은 raw wnl 기준,
          기대손익은 수수료 포함 순손익을 청산 체결 수로 나눈 값) */}
      <Group gap="sm" wrap="nowrap" style={{overflowX: 'auto', flexShrink: 0}}>
        <Kpi
          label="CAGR"
          value={fmtSignedPct(runStats.cagr)}
          color={signedColor(runStats.cagr)}
        />
        <Kpi label="Calmar" value={calmar === null ? '-' : fmtNum(calmar)}/>
        <Kpi label="Profit Factor" value={runStats.profitFactor === null ? '-' : fmtNum(runStats.profitFactor)}/>
        <Kpi label="Payoff" value={runStats.payoff === null ? '-' : fmtNum(runStats.payoff)}/>
        <Kpi
          label="기대손익/청산"
          value={runStats.expectancy === null
            ? '-'
            : `${runStats.expectancy >= 0 ? '+' : '-'}$${Math.abs(runStats.expectancy).toFixed(2)}`}
          color={signedColor(runStats.expectancy)}
        />
        <Kpi label="Exposure" value={runStats.exposure === null ? '-' : fmtPct(runStats.exposure)}/>
        <Kpi label="평균 보유" value={fmtDuration(runStats.avgHoldSec)}/>
      </Group>

      <div style={{flex: '0 0 45%', minHeight: 260}}>
        <EquityChart equityData={equityData} mddRatio={totalMdd} timeSync={timeSync}/>
      </div>

      <div style={{flex: 1, minHeight: 220}}>
        <MonthlyStatsChart monthly={monthly} timeSync={timeSync}/>
      </div>
    </Stack>
  );
};

export default StatsTab;
