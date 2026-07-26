import React, {useCallback, useEffect, useRef, useState} from 'react';
import {Link} from 'react-router-dom';
import {ActionIcon, Anchor, Center, Container, Loader, Paper, Stack, Table, Text, Title, Tooltip} from '@mantine/core';
import {IconTrash} from '@tabler/icons-react';
import {loadRun} from '../utils/runLoader';
import {fmtDate, fmtNum, fmtPct, paramsLines, paramsValues} from '../utils/format';

/**
 * root(`/`) 목록 페이지: backtestFiles 각각의 run JSON에서 metadata+summary만 읽어
 * (series shard는 로드하지 않음) PnL/MDD 등을 요약 테이블로 보여준다.
 */
const RunListPage = ({backtestFiles, onDeleteRun}) => {
  const [rows, setRows] = useState([]);
  const [loadingRows, setLoadingRows] = useState(true);
  const [deletingNames, setDeletingNames] = useState(new Set());
  const deletingRef = useRef(new Set());

  const handleDelete = useCallback(async (name) => {
    if (deletingRef.current.has(name)) return;
    if (!window.confirm(`"${name}" run 을 삭제하시겠습니까? 파일이 영구적으로 삭제되며 되돌릴 수 없습니다.`)) return;

    deletingRef.current.add(name);
    setDeletingNames(prev => new Set(prev).add(name));
    try {
      await onDeleteRun(name);
    } catch (error) {
      console.error(`Error deleting run ${name}:`, error);
      alert(`삭제 중 오류가 발생했습니다: ${error.message}`);
    } finally {
      deletingRef.current.delete(name);
      setDeletingNames(prev => {
        const next = new Set(prev);
        next.delete(name);
        return next;
      });
    }
  }, [onDeleteRun]);

  useEffect(() => {
    let cancelled = false;

    const loadAll = async () => {
      setLoadingRows(true);
      const results = await Promise.all(
        backtestFiles.map(async (file) => {
          try {
            const {metadata, summary} = await loadRun(file.name);
            return {file, metadata, summary, error: null};
          } catch (error) {
            console.error(`Error loading run ${file.name}:`, error);
            return {file, metadata: null, summary: null, error};
          }
        })
      );
      if (!cancelled) {
        setRows(results);
        setLoadingRows(false);
      }
    };

    if (backtestFiles.length > 0) {
      loadAll();
    } else {
      setRows([]);
      setLoadingRows(false);
    }

    return () => {
      cancelled = true;
    };
  }, [backtestFiles]);

  if (loadingRows) {
    return (
      <Center h="100vh">
        <Stack align="center" gap="sm">
          <Loader/>
          <Text size="sm" c="dimmed">run 목록을 불러오는 중...</Text>
        </Stack>
      </Center>
    );
  }

  return (
    <Container size="xl" py="xl">
      <Title order={2} mb="lg">Backtest Runs</Title>
      {rows.length === 0 ? (
        <Text c="dimmed">backtest/ 폴더에 run 파일이 없습니다.</Text>
      ) : (
        <Paper withBorder radius="md" style={{overflowX: 'auto'}}>
          <Table highlightOnHover verticalSpacing="sm" horizontalSpacing="md" fz="sm" style={{whiteSpace: 'nowrap'}}>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Label</Table.Th>
                <Table.Th>Symbol</Table.Th>
                <Table.Th>기간</Table.Th>
                <Table.Th>Profit</Table.Th>
                <Table.Th>Max DD</Table.Th>
                <Table.Th>Win Rate</Table.Th>
                <Table.Th>Sharpe</Table.Th>
                <Table.Th/>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {rows.map(({file, metadata, summary, error}) => {
                if (error || !metadata) {
                  return (
                    <Table.Tr key={file.name}>
                      <Table.Td colSpan={8}>
                        <Text size="sm" c="red.4">{file.name} — 로드 실패</Text>
                      </Table.Td>
                    </Table.Tr>
                  );
                }
                const profit = summary.profit_pct;
                const mdd = summary.max_drawdown && typeof summary.max_drawdown === 'object'
                  ? summary.max_drawdown.max_drawdown
                  : undefined;
                const params = paramsValues(metadata.params);
                return (
                  <Table.Tr key={file.name}>
                    <Table.Td>
                      <Anchor component={Link} to={`/run/${encodeURIComponent(file.name)}`} size="sm" fw={600}>
                        {metadata.label || metadata.streamer || file.name}
                      </Anchor>
                      {metadata.label && metadata.streamer && (
                        <Text size="xs" c="dimmed">{metadata.streamer}</Text>
                      )}
                      {params && (
                        <Tooltip
                          label={<span style={{whiteSpace: 'pre-line'}}>{paramsLines(metadata.params)}</span>}
                          multiline
                          maw={420}
                        >
                          <Text size="xs" c="dimmed" style={{maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis'}}>
                            {params}
                          </Text>
                        </Tooltip>
                      )}
                    </Table.Td>
                    <Table.Td>{metadata.symbol} · {metadata.interval}</Table.Td>
                    <Table.Td>{fmtDate(metadata.start)} → {fmtDate(metadata.end)}</Table.Td>
                    <Table.Td>
                      <Text size="sm" fw={600} c={profit >= 0 ? 'green.4' : 'red.4'}>
                        {profit === undefined ? '-' : `${profit >= 0 ? '+' : ''}${fmtNum(profit)}%`}
                      </Text>
                    </Table.Td>
                    <Table.Td>
                      <Text size="sm" fw={600} c="red.4">
                        {mdd === undefined ? '-' : fmtPct(mdd)}
                      </Text>
                    </Table.Td>
                    <Table.Td>
                      {summary.win_trades ?? '-'}/{summary.total_trades ?? '-'} ({fmtPct(summary.win_rate)})
                    </Table.Td>
                    <Table.Td>{fmtNum(summary.sharpe)}</Table.Td>
                    <Table.Td>
                      <ActionIcon
                        variant="subtle"
                        color="red"
                        size="sm"
                        aria-label={`${file.name} 삭제`}
                        loading={deletingNames.has(file.name)}
                        disabled={deletingNames.has(file.name)}
                        onClick={() => handleDelete(file.name)}
                      >
                        <IconTrash size={16}/>
                      </ActionIcon>
                    </Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </Paper>
      )}
    </Container>
  );
};

export default RunListPage;
