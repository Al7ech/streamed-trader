import React, {useEffect, useMemo, useRef, useState} from 'react';
import {
  ActionIcon,
  Badge,
  Divider,
  Group,
  Paper,
  ScrollArea,
  Select,
  Text,
  Tooltip,
  UnstyledButton,
} from '@mantine/core';
import {
  IconChevronsLeft,
  IconChevronsRight,
  IconSortAscending,
  IconSortDescending,
} from '@tabler/icons-react';
import {TRADE_COLORS} from '../theme';

const SORT_OPTIONS = [
  {value: 'time', label: 'Time'},
  {value: 'quantity', label: 'Quantity'},
  {value: 'price', label: 'Price'},
  {value: 'wnl', label: 'W&L'},
  {value: 'leverage', label: 'Leverage'},
];

const formatTime = (timestamp) => {
  const date = new Date(timestamp * 1000);
  const pad = (n) => String(n).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} `
    + `${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())} UTC`;
};

// 차트 마커/연결선과 같은 색 체계(TRADE_COLORS)로 위험도를 표시한다
const leverageColor = (leverage) => {
  if (leverage <= 1) return TRADE_COLORS.buy;
  if (leverage <= 3) return TRADE_COLORS.selected;
  return TRADE_COLORS.sell;
};

/**
 * 우측 거래 내역 패널 (구 하단 테이블). 거래당 컴팩트 2줄 행으로 표시하고,
 * 행 클릭 → 차트 점프는 기존 그대로. 폭/접힘 상태는 차트 리사이즈와 엮여 있어
 * 부모(TradingChart)가 소유한다.
 */
const TradeTable = ({data, width = 360, collapsed, onToggleCollapsed, onTradeClick, selectedTradeTime}) => {
  const [sortConfig, setSortConfig] = useState({key: 'time', direction: 'desc'});
  const selectedRowRef = useRef(null); // 선택된 거래 행 — 차트 마커 클릭으로 선택돼도 보이게 스크롤

  useEffect(() => {
    if (selectedRowRef.current) {
      selectedRowRef.current.scrollIntoView({block: 'nearest'});
    }
  }, [selectedTradeTime]);

  const sortedData = useMemo(() => {
    return [...data].sort((a, b) => {
      const aValue = a[sortConfig.key];
      const bValue = b[sortConfig.key];
      if (aValue < bValue) return sortConfig.direction === 'asc' ? -1 : 1;
      if (aValue > bValue) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });
  }, [data, sortConfig]);

  if (collapsed) {
    return (
      <Paper
        withBorder
        radius="md"
        ml="sm"
        w={40}
        py="xs"
        style={{flexShrink: 0, display: 'flex', justifyContent: 'center', alignItems: 'flex-start'}}
      >
        <Tooltip label="Trade History 펼치기">
          <ActionIcon variant="subtle" color="gray" onClick={onToggleCollapsed} aria-label="Trade History 펼치기">
            <IconChevronsLeft size={16}/>
          </ActionIcon>
        </Tooltip>
      </Paper>
    );
  }

  return (
    <Paper
      withBorder
      radius="md"
      w={width}
      style={{flexShrink: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden'}}
    >
      <Group px="sm" py="xs" justify="space-between" wrap="nowrap" gap="xs">
        <Text size="sm" fw={600} truncate>Trade History ({data.length})</Text>
        <Group gap={4} wrap="nowrap">
          <Select
            size="xs"
            w={104}
            data={SORT_OPTIONS}
            value={sortConfig.key}
            onChange={(value) => value && setSortConfig((c) => ({...c, key: value}))}
            allowDeselect={false}
            comboboxProps={{width: 130, position: 'bottom-end'}}
          />
          <Tooltip label={sortConfig.direction === 'desc' ? '내림차순' : '오름차순'}>
            <ActionIcon
              variant="default"
              size="md"
              onClick={() => setSortConfig((c) => ({...c, direction: c.direction === 'desc' ? 'asc' : 'desc'}))}
              aria-label="정렬 방향 전환"
            >
              {sortConfig.direction === 'desc' ? <IconSortDescending size={14}/> : <IconSortAscending size={14}/>}
            </ActionIcon>
          </Tooltip>
          <Tooltip label="접기">
            <ActionIcon variant="subtle" color="gray" onClick={onToggleCollapsed} aria-label="Trade History 접기">
              <IconChevronsRight size={16}/>
            </ActionIcon>
          </Tooltip>
        </Group>
      </Group>
      <Divider/>

      <ScrollArea style={{flex: 1, minHeight: 0}} type="auto">
        {sortedData.map((trade, index) => {
          const isSelected = selectedTradeTime === trade.time;
          const isBuy = trade.quantity > 0;
          const wnlValue = trade.wnl;
          const wnlPercentValue = trade.wnl_percent * 100;
          const isProfit = wnlValue > 0;
          const wnlColor = wnlValue === 0
            ? TRADE_COLORS.flat
            : (isProfit ? TRADE_COLORS.profit : TRADE_COLORS.loss);

          return (
            <UnstyledButton
              key={index}
              ref={isSelected ? selectedRowRef : undefined}
              onClick={() => onTradeClick && onTradeClick(trade.time)}
              w="100%"
              px="sm"
              py={6}
              bg={isSelected ? 'var(--mantine-color-blue-light)' : undefined}
              style={{borderBottom: '1px solid var(--mantine-color-default-border)'}}
              title="클릭하여 차트에서 해당 거래 시간으로 이동"
            >
              <Group justify="space-between" wrap="nowrap" gap="xs">
                <Group gap={6} wrap="nowrap" style={{overflow: 'hidden'}}>
                  <Badge
                    variant="light"
                    color={isBuy ? TRADE_COLORS.buy : TRADE_COLORS.sell}
                    radius="sm"
                    size="sm"
                    style={{flexShrink: 0}}
                  >
                    {isBuy ? '+' : ''}{trade.quantity.toFixed(4)}
                  </Badge>
                  <Text size="xs" c="dimmed" truncate>@{trade.price.toFixed(2)}</Text>
                </Group>
                <Text size="xs" fw={600} c={wnlColor} style={{whiteSpace: 'nowrap'}}>
                  {isProfit ? '+' : ''}${wnlValue.toFixed(2)} ({isProfit ? '+' : ''}{wnlPercentValue.toFixed(2)}%)
                </Text>
              </Group>
              <Group justify="space-between" wrap="nowrap" gap="xs" mt={2}>
                <Text size="xs" c="dimmed">{formatTime(trade.time)}</Text>
                {trade.leverage !== 0.0 && (
                  <Text size="xs" fw={600} c={leverageColor(trade.leverage)}>
                    {trade.leverage.toFixed(1)}x
                  </Text>
                )}
              </Group>
            </UnstyledButton>
          );
        })}
      </ScrollArea>
    </Paper>
  );
};

export default React.memo(TradeTable);
