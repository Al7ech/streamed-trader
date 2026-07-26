// lightweight-charts v5 series primitive — 포지션 구간(진입 평단→청산가) 점선과
// 선택 거래 세로 하이라이트를 캔들 시리즈 위에 캔버스로 일괄 렌더한다.
// 구간이 수백 개여도 LineSeries 를 구간마다 만들지 않고 한 primitive 가 다 그린다.

import {bucketTime} from '../../utils/aggregate';
import {CHART_COLORS, TRADE_COLORS} from '../../theme';
import {PaneViewPrimitive} from './PaneViewPrimitive';

const ENDPOINT_RADIUS = 4;

const segmentColor = (wnl) => {
  if (wnl > 0) return TRADE_COLORS.profit;
  if (wnl < 0) return TRADE_COLORS.loss;
  return TRADE_COLORS.flat;
};

class TradeSegmentsPaneRenderer {
  constructor(lines, selectedX) {
    this._lines = lines;
    this._selectedX = selectedX;
  }

  draw(target) {
    target.useMediaCoordinateSpace(({context: ctx, mediaSize}) => {
      // 선택 거래 세로 하이라이트 — 마커가 화면 밖 가격대에 있어도 위치를 인지할 수 있게
      if (this._selectedX != null && this._selectedX >= 0 && this._selectedX <= mediaSize.width) {
        ctx.save();
        ctx.strokeStyle = TRADE_COLORS.selected;
        ctx.globalAlpha = 0.35;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.moveTo(this._selectedX, 0);
        ctx.lineTo(this._selectedX, mediaSize.height);
        ctx.stroke();
        ctx.restore();
      }

      if (this._lines.length === 0) return;

      ctx.save();
      ctx.setLineDash([6, 4]);
      for (const line of this._lines) {
        // 완전히 화면 밖인 구간은 건너뛴다 (캔버스 클리핑에 맡겨도 되지만 그리기 비용 절약)
        if ((line.x0 < 0 && line.x1 < 0) || (line.x0 > mediaSize.width && line.x1 > mediaSize.width)) {
          continue;
        }
        // 배경색 halo 를 먼저 깔아 캔들과 겹쳐도 선이 분리돼 보이게 한다
        ctx.beginPath();
        ctx.moveTo(line.x0, line.y0);
        ctx.lineTo(line.x1, line.y1);
        ctx.strokeStyle = CHART_COLORS.background;
        ctx.lineWidth = 5;
        ctx.stroke();
        ctx.strokeStyle = line.color;
        ctx.lineWidth = 2;
        ctx.stroke();
      }

      // 양 끝점 — 진입/청산 지점을 점으로 강조 (배경색 링으로 캔들과 분리)
      ctx.setLineDash([]);
      ctx.lineWidth = 2;
      ctx.strokeStyle = CHART_COLORS.background;
      for (const line of this._lines) {
        ctx.fillStyle = line.color;
        for (const [x, y] of [[line.x0, line.y0], [line.x1, line.y1]]) {
          if (x < -ENDPOINT_RADIUS || x > mediaSize.width + ENDPOINT_RADIUS) continue;
          ctx.beginPath();
          ctx.arc(x, y, ENDPOINT_RADIUS, 0, Math.PI * 2);
          ctx.fill();
          ctx.stroke();
        }
      }
      ctx.restore();
    });
  }
}

class TradeSegmentsPaneView {
  constructor(source) {
    this._source = source;
    this._lines = [];
    this._selectedX = null;
  }

  update() {
    this._lines = [];
    this._selectedX = null;

    const {chart, series, data} = this._source;
    if (!chart || !series) return;
    const {segments, timeframe, selectedTime, candles} = data;
    if (!candles || candles.length === 0) return;

    const timeScale = chart.timeScale();

    // 거래 시각 → x 좌표. 시각을 타임프레임 버킷으로 스냅한 뒤 로드된 캔들에서 논리
    // 인덱스를 이진 탐색으로 찾는다. 로드 범위 밖(lazy 로드로 shard 가 없는 구간)이면
    // 가장자리 캔들 기준으로 논리 인덱스를 외삽해 기울기가 유지된 채 클리핑되게 한다.
    const xForTime = (t) => {
      const bt = bucketTime(t, timeframe);
      const last = candles.length - 1;
      let logical;
      if (bt <= candles[0].time) {
        logical = (bt - candles[0].time) / timeframe;
      } else if (bt >= candles[last].time) {
        logical = last + (bt - candles[last].time) / timeframe;
      } else {
        let lo = 0;
        let hi = last;
        while (lo < hi) {
          const mid = (lo + hi) >> 1;
          if (candles[mid].time < bt) lo = mid + 1;
          else hi = mid;
        }
        logical = lo; // 정확히 일치하거나, 로드 갭 안이면 다음 로드 캔들로 스냅
      }
      return timeScale.logicalToCoordinate(logical);
    };

    for (const seg of segments) {
      const x0 = xForTime(seg.t0);
      const x1 = xForTime(seg.t1);
      const y0 = series.priceToCoordinate(seg.p0);
      const y1 = series.priceToCoordinate(seg.p1);
      if (x0 == null || x1 == null || y0 == null || y1 == null) continue;
      this._lines.push({x0, y0, x1, y1, color: segmentColor(seg.wnl)});
    }

    if (selectedTime != null) {
      this._selectedX = xForTime(selectedTime);
    }
  }

  renderer() {
    return new TradeSegmentsPaneRenderer(this._lines, this._selectedX);
  }

  zOrder() {
    return 'top'; // 캔들 위에 그린다 — 'normal' 이면 캔들 몸통에 가려진다
  }
}

export class TradeSegmentsPrimitive extends PaneViewPrimitive {
  constructor() {
    super(
      {segments: [], timeframe: 60, selectedTime: null, candles: []},
      (host) => [new TradeSegmentsPaneView(host)]
    );
  }
}
