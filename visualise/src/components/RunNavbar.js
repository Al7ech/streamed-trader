import React from 'react';
import {AppShell, NavLink, ScrollArea, Text, Tooltip} from '@mantine/core';
import {IconArrowLeft, IconFileAnalytics} from '@tabler/icons-react';

/**
 * /run/:fileName 좌측 내비게이션: run 목록으로 돌아가기 + backtest run 파일 전환.
 * (구 FileExplorer(react-pro-sidebar) 대체 — 접힘/펼침은 AppShell navbar collapse가 담당)
 */
const RunNavbar = ({backtestFiles, selectedBacktestFile, onBacktestFileSelect, onBackToList}) => (
  <>
    <AppShell.Section>
      <NavLink
        label="목록으로"
        leftSection={<IconArrowLeft size={16}/>}
        onClick={onBackToList}
      />
    </AppShell.Section>

    <AppShell.Section grow component={ScrollArea}>
      <Text size="xs" c="dimmed" fw={600} tt="uppercase" px="sm" pt="sm" pb={4}>
        Backtest Runs
      </Text>
      {backtestFiles.length === 0 ? (
        <Text size="sm" c="dimmed" px="sm" py="xs">No files</Text>
      ) : (
        backtestFiles.map((file) => (
          <Tooltip key={file.name} label={file.name} position="right" openDelay={500}>
            <NavLink
              active={selectedBacktestFile === file.name}
              label={<Text size="sm" truncate="end">{file.name}</Text>}
              leftSection={<IconFileAnalytics size={16}/>}
              onClick={() => onBacktestFileSelect(file.name)}
            />
          </Tooltip>
        ))
      )}
    </AppShell.Section>
  </>
);

export default RunNavbar;
