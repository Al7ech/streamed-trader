import React from 'react';
import {Badge, Group, Text, Tooltip} from '@mantine/core';
import {fmtDate, fmtNum, fmtPct, paramsString} from '../utils/format';
import {TRADE_COLORS} from '../theme';

const Metric = ({label, value, color}) => (
  <Group gap={4} wrap="nowrap">
    <Text size="xs" c="dimmed">{label}</Text>
    <Text size="sm" fw={600} style={color ? {color} : undefined}>{value}</Text>
  </Group>
);

/**
 * 헤더용 run 요약: 메타(streamer/symbol/기간/params) badge + 핵심 지표.
 */
const InfoPanel = ({metadata, summary}) => {
  if (!metadata || Object.keys(metadata).length === 0) return null;

  const md = metadata;
  const s = summary || {};
  const profit = s.profit_pct;
  const mdd = s.max_drawdown && typeof s.max_drawdown === 'object'
    ? s.max_drawdown.max_drawdown
    : undefined;

  return (
    <Group gap="lg" wrap="nowrap" style={{flex: 1, overflow: 'hidden'}}>
      <Group gap="xs" wrap="nowrap" style={{overflow: 'hidden'}}>
        {md.label && (
          <Tooltip label={md.label} multiline maw={420}>
            <Text size="sm" fw={600} style={{maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap'}}>
              {md.label}
            </Text>
          </Tooltip>
        )}
        <Badge variant="filled" radius="sm">{md.streamer || 'Streamer'}</Badge>
        <Badge variant="light" color="gray" radius="sm">{md.symbol} · {md.interval}</Badge>
        <Badge variant="light" color="gray" radius="sm">
          {fmtDate(md.start)} → {fmtDate(md.end)}
        </Badge>
        {md.candle_count !== undefined && (
          <Badge variant="light" color="gray" radius="sm">
            {Number(md.candle_count).toLocaleString()} candles
          </Badge>
        )}
        {md.params && (
          <Tooltip label={paramsString(md.params)} multiline maw={420}>
            <Badge variant="outline" color="gray" radius="sm" style={{maxWidth: 260}}>
              {paramsString(md.params)}
            </Badge>
          </Tooltip>
        )}
      </Group>

      <Group gap="md" wrap="nowrap" ml="auto">
        <Metric
          label="Profit"
          value={profit === undefined ? '-' : `${profit >= 0 ? '+' : ''}${fmtNum(profit)}%`}
          color={profit === undefined ? undefined : (profit >= 0 ? TRADE_COLORS.profit : TRADE_COLORS.loss)}
        />
        <Metric
          label="Win Rate"
          value={`${s.win_trades ?? '-'}/${s.total_trades ?? '-'} (${fmtPct(s.win_rate)})`}
        />
        <Metric label="Max Lev" value={`${fmtNum(s.max_leverage)}x`}/>
        <Metric label="Sharpe" value={fmtNum(s.sharpe)}/>
        <Metric
          label="Max DD"
          value={mdd === undefined ? '-' : fmtPct(mdd)}
          color={TRADE_COLORS.loss}
        />
      </Group>
    </Group>
  );
};

export default InfoPanel;
