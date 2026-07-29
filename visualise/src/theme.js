import {createTheme} from '@mantine/core';

// 인디케이터 시리즈 색상 팔레트. 시리즈 등장 순서대로 순환 배정되며,
// 차트 시리즈와 범례 체크박스가 같은 인덱스로 이 배열을 공유한다.
// 매수/매도 마커·손익 연결선과 혼동되지 않도록 초록/빨강 계열은 제외한다.
// (순서 포함 dataviz validator 통과: 인접쌍 CVD ΔE ≥ 20, 대비 ≥ 3:1 on #1a1b1e)
export const INDICATOR_COLORS = [
  '#4dabf7', // 파랑
  '#ffd43b', // 노랑
  '#9775fa', // 보라
  '#22b8cf', // 시안
  '#e8b04b', // 앰버
  '#748ffc', // 인디고
  '#f783ac', // 핑크
  '#66d9e8', // 연시안
];

// 자산(equity/balance) 시각화 공통 색 — 타임라인 sparkline·Trades 탭 equity 곡선·승률 바가 공유.
// INDICATOR_COLORS[0] 과 같은 파랑 계열이지만, 지표 순환 팔레트와 별개로 "자산" 의미로 고정한다.
export const EQUITY_COLOR = '#4dabf7';

// 기초자산 단순 보유(buy & hold) 비교선 — equity(파랑)/평균 성장 기준선(무채색)/drawdown(빨강)과
// 한 pane 안에서 구분되어야 해서 검증된 팔레트의 노랑을 고정 배정한다.
export const BASE_ASSET_COLOR = INDICATOR_COLORS[1];

// 캔들 색 — 채도/명도를 낮춘 중간톤으로, 마커·연결선(TRADE_COLORS)이 앞에 서게 한다.
export const CANDLE_COLORS = {
  up: '#2c7a6f',
  down: '#a14a52',
};

// 거래 방향/손익 색의 단일 출처 — 차트 마커·포지션 연결선·거래 테이블·InfoPanel 이 공유한다.
// 무채색화된 캔들보다 확실히 밝고 선명해야 한다.
export const TRADE_COLORS = {
  buy: '#00e676',
  sell: '#ff4d67',
  profit: '#00e676',
  loss: '#ff4d67',
  flat: '#8b93a1',      // 손익 0인 포지션 구간선
  selected: '#FFD54F',  // 선택된 거래 마커/세로 하이라이트
};

// lightweight-charts 레이아웃 색 — Mantine dark 팔레트(dark.7 배경, dark.0 텍스트)와 동일 톤.
export const CHART_COLORS = {
  background: '#1a1b1e',
  text: '#c1c2c5',
  grid: 'rgba(255, 255, 255, 0.05)',
  scaleBorder: 'rgba(255, 255, 255, 0.15)',
};

export const theme = createTheme({
  primaryColor: 'blue',
  defaultRadius: 'md',
  fontFamily:
    "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', " +
    "'Cantarell', 'Fira Sans', 'Droid Sans', 'Helvetica Neue', sans-serif",
  fontFamilyMonospace: "source-code-pro, Menlo, Monaco, Consolas, 'Courier New', monospace",
});
