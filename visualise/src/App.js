import React from 'react';
import {Route, Routes} from 'react-router-dom';
import {Button, Center, Code, Loader, MantineProvider, Stack, Text, Title} from '@mantine/core';
import {IconFolderOpen} from '@tabler/icons-react';
import RunListPage from './components/RunListPage';
import RunDetailPage from './components/RunDetailPage';
import {useBacktestFiles} from './hooks/useBacktestFiles';
import {theme} from './theme';

function App() {
  const {loading, directorySelected, backtestFiles, selectDirectory} = useBacktestFiles();

  let content;
  if (loading) {
    content = (
      <Center h="100vh">
        <Stack align="center" gap="sm">
          <Loader/>
          <Text size="sm" c="dimmed">파일 목록을 불러오는 중...</Text>
        </Stack>
      </Center>
    );
  } else if (!directorySelected) {
    content = (
      <Center h="100vh">
        <Stack align="center" gap="md" maw={480} px="md">
          <Title order={2}>Backtest Visualizer</Title>
          <Button
            size="md"
            leftSection={<IconFolderOpen size={18}/>}
            onClick={selectDirectory}
            disabled={loading}
          >
            asset 폴더 선택
          </Button>
          <Text size="xs" c="dimmed" ta="center">
            백테스터가 결과를 쓰는 asset 폴더를 선택하세요 (기본값: 리포지토리 루트의 asset/).
            <br/>
            <Code>backtest/</Code> 폴더의 결과 JSON 이 자동으로 로드됩니다.
            <br/>
            <Text span fw={600} inherit>한 번만 선택하면 새로고침 후에도 자동으로 로드됩니다.</Text>
          </Text>
        </Stack>
      </Center>
    );
  } else {
    content = (
      <Routes>
        <Route path="/" element={<RunListPage backtestFiles={backtestFiles}/>}/>
        <Route path="/run/:fileName" element={<RunDetailPage backtestFiles={backtestFiles}/>}/>
      </Routes>
    );
  }

  return (
    <MantineProvider theme={theme} defaultColorScheme="dark">
      {content}
    </MantineProvider>
  );
}

export default App;
