import React, {useCallback, useEffect, useRef, useState} from 'react';
import {useNavigate, useParams} from 'react-router-dom';
import {ActionIcon, AppShell, Group, Tabs, Tooltip} from '@mantine/core';
import {IconLayoutSidebarLeftCollapse, IconLayoutSidebarLeftExpand} from '@tabler/icons-react';
import TradingChart from './TradingChart';
import RunNavbar from './RunNavbar';
import InfoPanel from './InfoPanel';
import StatsTab from './stats/StatsTab';
import {loadRun} from '../utils/runLoader';
import {SeriesLoader} from '../utils/seriesLoader';
import {isCoarse, toSeconds} from '../utils/aggregate';

/**
 * /run/:fileName 상세 페이지. AppShell 레이아웃:
 * Header(사이드바 토글 + run 메타/지표) / Navbar(run 목록) / Main(차트 + 거래 테이블).
 *
 * 데이터 오케스트레이션: loadRun 으로 run JSON 을 읽고, 시계열은 SeriesLoader 가
 * 타임프레임에 맞춰 lazy 로드한다 — 초기/타임프레임 전환 시 최신 스팬(showLatest),
 * 이후 차트 이동(클릭/드래그/패닝) 시 해당 구간(showRange)만 그때그때.
 */
const RunDetailPage = ({backtestFiles}) => {
  const {fileName: encodedFileName} = useParams();
  const fileName = decodeURIComponent(encodedFileName);
  const navigate = useNavigate();

  const [metadata, setMetadata] = useState(null);
  const [summary, setSummary] = useState(null);
  const [candleData, setCandleData] = useState([]);
  const [indicatorData, setIndicatorData] = useState({});
  const [indicatorGroups, setIndicatorGroups] = useState({});
  const [tradeData, setTradeData] = useState([]);
  const [equityData, setEquityData] = useState(null); // 전체 구간 다운샘플 equity — 타임라인 sparkline 용
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [fullRange, setFullRange] = useState(null); // 전체 백테스트 시간 구간(초) — 타임라인 셀렉터용
  const [activeTab, setActiveTab] = useState('visualise'); // 'visualise'(차트) | 'trades'(equity+월별 통계)
  // Trades 탭은 처음 열 때까지 마운트하지 않는다 — 안 열어본 run 에서 통계 계산·차트 2개 생성을
  // 아끼고, 한 번 연 뒤에는 keepMounted 로 상태를 유지한다.
  const [tradesVisited, setTradesVisited] = useState(false);

  const handleTabChange = useCallback((tab) => {
    setActiveTab(tab);
    if (tab === 'trades') setTradesVisited(true);
  }, []);

  const loaderRef = useRef(null);
  const timeframeRef = useRef(60); // TradingChart 의 현재 타임프레임(초)

  // 로더 응답을 현재 뷰에 적용해도 되는지 가드하고 적용한다. coarse 집계본은 요청 interval 과
  // 현재 타임프레임이 정확히 일치해야 하고, fine(1m 원본)은 현재도 fine 이기만 하면 된다
  // (1m→5m 등 fine 간 전환은 차트가 로드된 1m 을 클라이언트에서 재집계하므로 데이터가 그대로 유효).
  const applyIfCurrent = useCallback((loader, intervalSec, merged) => {
    if (!merged || loaderRef.current !== loader) return;
    const current = timeframeRef.current;
    if (isCoarse(intervalSec) ? current !== intervalSec : isCoarse(current)) return;
    setCandleData(merged.candles);
    setIndicatorData(merged.indicators);
  }, []);

  // 최신 스팬(차트 기본 뷰 ~200봉) 로드 — 초기 진입/타임프레임 모드 전환용.
  const showLatest = useCallback(async (intervalSec) => {
    const loader = loaderRef.current;
    if (!loader) return;
    try {
      applyIfCurrent(loader, intervalSec, await loader.ensureLatest(intervalSec));
    } catch (error) {
      console.warn('Error loading latest series:', error);
    }
  }, [applyIfCurrent]);

  // 특정 시간 구간 로드 — 타임라인 클릭/드래그·거래 클릭·패닝이 모두 이 경로를 탄다.
  const showRange = useCallback(async (intervalSec, fromMs, toMs) => {
    const loader = loaderRef.current;
    if (!loader) return;
    try {
      applyIfCurrent(loader, intervalSec, await loader.ensureRange(intervalSec, fromMs, toMs));
    } catch (error) {
      console.warn('Error lazy-loading series shard:', error);
    }
  }, [applyIfCurrent]);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const {metadata, summary, equity, trades, series} = await loadRun(fileName);
        if (cancelled) return;
        setMetadata(metadata);
        setSummary(summary);
        setEquityData(equity);
        setTradeData(trades);

        const loader = new SeriesLoader(series);
        loaderRef.current = loader;
        setIndicatorGroups(loader.columnGroups);
        const range = loader.fullTimeRangeMs();
        setFullRange(range ? {from: toSeconds(range.from), to: toSeconds(range.to)} : null);

        setCandleData([]);
        setIndicatorData({});
        await showLatest(timeframeRef.current);
      } catch (error) {
        console.error(`Error loading run ${fileName}:`, error);
        if (cancelled) return;
        setMetadata(null);
        setSummary(null);
        setEquityData(null);
        setTradeData([]);
        setCandleData([]);
        setIndicatorData({});
        setIndicatorGroups({});
        setFullRange(null);
      }
    };

    load();
    return () => {
      cancelled = true;
    };
  }, [fileName, showLatest]);

  const handleVisibleRangeChange = useCallback((fromSec, toSec) => {
    showRange(timeframeRef.current, fromSec * 1000, toSec * 1000);
  }, [showRange]);

  // 타임프레임 변경 → 로딩 모드가 바뀌는 경우(fine↔coarse, coarse 간 interval 변경)만
  // 최신 스팬을 새 interval 로 다시 로드한다. fine 간 전환은 로드된 1m 을 차트가 재집계.
  const handleTimeframeChange = useCallback((intervalSec) => {
    const prev = timeframeRef.current;
    timeframeRef.current = intervalSec;
    if (!loaderRef.current || intervalSec === prev) return;
    if (isCoarse(prev) || isCoarse(intervalSec)) showLatest(intervalSec);
  }, [showLatest]);

  const handleBacktestFileSelect = useCallback((selectedFileName) => {
    navigate(`/run/${encodeURIComponent(selectedFileName)}`);
  }, [navigate]);

  return (
    <AppShell
      header={{height: 56}}
      navbar={{
        width: 280,
        breakpoint: 'md',
        collapsed: {desktop: sidebarCollapsed, mobile: sidebarCollapsed},
      }}
      padding={0}
    >
      <AppShell.Header>
        <Group h="100%" px="md" gap="sm" wrap="nowrap">
          <Tooltip label={sidebarCollapsed ? '사이드바 펼치기' : '사이드바 접기'}>
            <ActionIcon
              variant="subtle"
              color="gray"
              size="lg"
              aria-label={sidebarCollapsed ? '사이드바 펼치기' : '사이드바 접기'}
              onClick={() => setSidebarCollapsed((v) => !v)}
            >
              {sidebarCollapsed
                ? <IconLayoutSidebarLeftExpand size={20}/>
                : <IconLayoutSidebarLeftCollapse size={20}/>}
            </ActionIcon>
          </Tooltip>
          <InfoPanel metadata={metadata} summary={summary}/>
        </Group>
      </AppShell.Header>

      <AppShell.Navbar>
        <RunNavbar
          backtestFiles={backtestFiles}
          selectedBacktestFile={fileName}
          onBacktestFileSelect={handleBacktestFileSelect}
          onBackToList={() => navigate('/')}
        />
      </AppShell.Navbar>

      <AppShell.Main h="100vh">
        {/* keepMounted(기본값): 탭을 오가도 TradingChart 를 언마운트하지 않아
            차트 뷰포트/선택 상태와 lazy 로드된 데이터가 유지된다 */}
        <Tabs
          value={activeTab}
          onChange={handleTabChange}
          style={{height: '100%', display: 'flex', flexDirection: 'column'}}
        >
          <Tabs.List px="sm">
            <Tabs.Tab value="visualise">Visualise</Tabs.Tab>
            <Tabs.Tab value="trades">Trades</Tabs.Tab>
          </Tabs.List>

          <Tabs.Panel value="visualise" style={{flex: 1, minHeight: 0}}>
            <TradingChart
              candleData={candleData}
              indicatorData={indicatorData}
              indicatorGroups={indicatorGroups}
              tradeData={tradeData}
              equityData={equityData}
              selectedBacktestFile={fileName}
              fullRange={fullRange}
              onVisibleRangeChange={handleVisibleRangeChange}
              onTimeframeChange={handleTimeframeChange}
            />
          </Tabs.Panel>

          <Tabs.Panel value="trades" style={{flex: 1, minHeight: 0}}>
            {tradesVisited && (
              <StatsTab equityData={equityData} tradeData={tradeData} summary={summary}/>
            )}
          </Tabs.Panel>
        </Tabs>
      </AppShell.Main>
    </AppShell>
  );
};

export default RunDetailPage;
