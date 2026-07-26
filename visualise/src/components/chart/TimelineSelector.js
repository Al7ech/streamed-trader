import React, {useCallback, useRef, useState} from 'react';
import {EQUITY_COLOR, TRADE_COLORS} from '../../theme';
import './TimelineSelector.css';

const MIN_BOX_WIDTH_PCT = 1.5; // 좁게 확대해도(1m 등) 하이라이트가 사라지지 않도록 하는 최소 폭
const TRACK_CLICK_CANDLES = 100; // 트랙 클릭(점프) 시 기본으로 보여줄 캔들 개수 — 직전 확대 폭을 물려받지 않는다

// TradeTable 의 formatTime 과 같은 UTC 규약 (YYYY-MM-DD HH:MM UTC)
const formatHoverTime = (sec) =>
  `${new Date(sec * 1000).toISOString().slice(0, 16).replace('T', ' ')} UTC`;

const clampRange = (fromSec, toSec, full) => {
  const width = toSec - fromSec;
  let from = fromSec;
  let to = toSec;
  if (from < full.from) {
    from = full.from;
    to = from + width;
  }
  if (to > full.to) {
    to = full.to;
    from = to - width;
  }
  return {from: Math.max(full.from, from), to: Math.min(full.to, to)};
};

// 전체 구간 balance(equity) sparkline — run JSON 의 다운샘플 equity 를 min–max 정규화해
// 트랙 배경(틱 뒤)에 그린다. 구 run(equity 없음)은 렌더하지 않는다.
// 패닝 중 바뀌는 visibleRange 와 무관하므로 React.memo 로 재계산을 막는다.
const TimelineSparkline = React.memo(({equity, fullRange}) => {
  if (!equity || equity.length < 2) return null;

  const fullWidth = Math.max(1, fullRange.to - fullRange.from);
  let min = Infinity;
  let max = -Infinity;
  for (const p of equity) {
    if (p.value < min) min = p.value;
    if (p.value > max) max = p.value;
  }
  const span = max - min || 1;

  const points = equity
    .map((p) => {
      const x = ((p.time - fullRange.from) / fullWidth) * 100;
      const y = 92 - ((p.value - min) / span) * 84; // 상하 8% 여백
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(' ');

  return (
    <svg
      className="timeline-selector__sparkline"
      viewBox="0 0 100 100"
      preserveAspectRatio="none"
    >
      {/* 자산 공통색(EQUITY_COLOR) — 매수/매도 틱(초록/빨강)과 구분된다 */}
      <polyline
        points={points}
        fill="none"
        stroke={EQUITY_COLOR}
        strokeWidth="1"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
});

// 매수/매도 틱은 trades/fullRange 에만 의존한다 — 패닝 중 바뀌는 visibleRange 와는 무관하므로
// React.memo 로 분리해 매 프레임 재생성(수백~수천 개 div)을 막는다.
const TimelineTicks = React.memo(({trades, fullRange}) => {
  const fullWidth = Math.max(1, fullRange.to - fullRange.from);
  const pct = (t) => Math.min(100, Math.max(0, ((t - fullRange.from) / fullWidth) * 100));

  return (
    <>
      {(trades || []).map((trade) => (
        <div
          key={trade.time}
          className="timeline-selector__tick"
          style={{
            left: `${pct(trade.time)}%`,
            background: trade.quantity > 0 ? TRADE_COLORS.buy : TRADE_COLORS.sell,
          }}
        />
      ))}
    </>
  );
});

/**
 * 전체 백테스트 구간 대비 현재 차트에 보이는 구간을 보여주는 미니맵 바.
 * 트랙 클릭 → 현재 폭을 유지한 채 그 지점으로 패닝. 하이라이트 박스 드래그 → 좌우 패닝.
 * lightweight-charts 프리미티브가 아닌 순수 DOM — 좌표계는 "전체 구간 대비 비율".
 */
const TimelineSelector = ({fullRange, visibleRange, trades, equity, onPan, timeframe = 60}) => {
  const trackRef = useRef(null);
  const [hover, setHover] = useState(null); // {leftPct, label} | null

  const timeAtClientX = useCallback((clientX) => {
    if (!trackRef.current || !fullRange) return null;
    const rect = trackRef.current.getBoundingClientRect();
    const frac = Math.min(1, Math.max(0, (clientX - rect.left) / rect.width));
    return fullRange.from + frac * (fullRange.to - fullRange.from);
  }, [fullRange]);

  // 트랙 클릭 = 점프. 클릭 직전의 확대 폭(예: 2개 캔들만 보일 만큼 확대된 상태)을 그대로
  // 물려받으면 이동 후에도 그만큼만 보이므로, 항상 TRACK_CLICK_CANDLES 폭으로 고정해 이동한다.
  // (하이라이트 박스 드래그는 반대로 현재 확대 폭을 유지해야 하므로 그쪽은 그대로 둔다.)
  const handleTrackClick = useCallback((e) => {
    if (!fullRange) return;
    const target = timeAtClientX(e.clientX);
    if (target == null) return;
    const fullWidth = fullRange.to - fullRange.from;
    const width = Math.min(fullWidth, TRACK_CLICK_CANDLES * timeframe);
    const {from, to} = clampRange(target - width / 2, target + width / 2, fullRange);
    onPan(from, to);
  }, [fullRange, timeframe, timeAtClientX, onPan]);

  const handleTrackMouseMove = useCallback((e) => {
    if (!trackRef.current) return;
    const time = timeAtClientX(e.clientX);
    if (time == null) return;
    const rect = trackRef.current.getBoundingClientRect();
    const leftPct = Math.min(100, Math.max(0, ((e.clientX - rect.left) / rect.width) * 100));
    setHover({leftPct, label: formatHoverTime(time)});
  }, [timeAtClientX]);

  const handleTrackMouseLeave = useCallback(() => setHover(null), []);

  const handleBoxMouseDown = useCallback((e) => {
    e.preventDefault();
    e.stopPropagation();
    if (!fullRange || !visibleRange || !trackRef.current) return;

    const rect = trackRef.current.getBoundingClientRect();
    const fullWidth = fullRange.to - fullRange.from;
    const width = visibleRange.to - visibleRange.from;
    const startX = e.clientX;
    const startFrom = visibleRange.from;

    const onMouseMove = (ev) => {
      const deltaSec = ((ev.clientX - startX) / rect.width) * fullWidth;
      const {from, to} = clampRange(startFrom + deltaSec, startFrom + deltaSec + width, fullRange);
      onPan(from, to);
    };
    const onMouseUp = () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
      document.body.style.userSelect = '';
      document.body.style.cursor = '';
    };

    document.body.style.userSelect = 'none';
    document.body.style.cursor = 'grabbing';
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  }, [fullRange, visibleRange, onPan]);

  if (!fullRange) return null;

  const fullWidth = Math.max(1, fullRange.to - fullRange.from);
  const pct = (t) => Math.min(100, Math.max(0, ((t - fullRange.from) / fullWidth) * 100));

  let boxLeft = 0;
  let boxWidth = 100;
  if (visibleRange) {
    boxLeft = pct(visibleRange.from);
    boxWidth = Math.max(MIN_BOX_WIDTH_PCT, pct(visibleRange.to) - boxLeft);
    if (boxLeft + boxWidth > 100) boxLeft = 100 - boxWidth;
  }

  return (
    // 라벨은 트랙(overflow: hidden) 밖 위쪽에 떠야 하므로 래퍼 기준으로 배치한다
    <div className="timeline-selector-wrap">
      <div
        className="timeline-selector"
        ref={trackRef}
        onMouseDown={handleTrackClick}
        onMouseMove={handleTrackMouseMove}
        onMouseLeave={handleTrackMouseLeave}
      >
        <TimelineSparkline equity={equity} fullRange={fullRange}/>
        <TimelineTicks trades={trades} fullRange={fullRange}/>
        {hover && (
          <div className="timeline-selector__hover-line" style={{left: `${hover.leftPct}%`}}/>
        )}
        <div
          className="timeline-selector__box"
          style={{left: `${boxLeft}%`, width: `${boxWidth}%`}}
          onMouseDown={handleBoxMouseDown}
        />
      </div>
      {hover && (
        <div
          className="timeline-selector__hover-label"
          // 가장자리에서 라벨(반폭 ≈ 라벨 폭/2)이 잘리지 않도록 clamp
          style={{left: `${Math.min(93, Math.max(7, hover.leftPct))}%`}}
        >
          {hover.label}
        </div>
      )}
    </div>
  );
};

export default TimelineSelector;
