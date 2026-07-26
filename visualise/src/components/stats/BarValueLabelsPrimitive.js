// lightweight-charts v5 series primitive — 히스토그램 바 위/아래에 값 라벨을 그린다.
// 바 간격이 라벨 폭보다 좁아지면 n개마다 하나만 그려 겹침을 막는다(줌인하면 다시 촘촘해진다).

import {CHART_COLORS} from '../../theme';
import {PaneViewPrimitive} from '../chart/PaneViewPrimitive';

const FONT = '10px sans-serif';
const CHAR_WIDTH = 6; // 10px sans-serif 근사 폭 — measureText 없이 밀도 계산용
const LABEL_GAP = 6; // 라벨 사이 최소 여백(px)

class BarValueLabelsRenderer {
  constructor(items) {
    this._items = items;
  }

  draw(target) {
    target.useMediaCoordinateSpace(({context: ctx, mediaSize}) => {
      if (this._items.length === 0) return;
      ctx.save();
      ctx.font = FONT;
      ctx.textAlign = 'center';
      // 배경색 halo — 그리드/기준선과 겹쳐도 읽히게
      ctx.lineWidth = 3;
      ctx.strokeStyle = CHART_COLORS.background;
      for (const it of this._items) {
        if (it.x < -40 || it.x > mediaSize.width + 40) continue;
        ctx.fillStyle = it.color;
        ctx.strokeText(it.label, it.x, it.y);
        ctx.fillText(it.label, it.x, it.y);
      }
      ctx.restore();
    });
  }
}

class BarValueLabelsPaneView {
  constructor(source) {
    this._source = source;
    this._items = [];
  }

  update() {
    this._items = [];
    const {chart, series, data} = this._source;
    if (!chart || !series) return;
    const points = data.points || [];
    if (points.length === 0) return;

    const timeScale = chart.timeScale();
    const coords = [];
    for (let i = 0; i < points.length; i++) {
      const p = points[i];
      const x = timeScale.timeToCoordinate(p.time);
      const y = series.priceToCoordinate(p.value);
      if (x == null || y == null) continue;
      coords.push({idx: i, x, y, point: p});
    }
    if (coords.length === 0) return;

    // 표시 밀도: 바 간격 대비 최대 라벨 폭으로 step 을 정한다.
    // step 판정은 절대 인덱스(idx) 기준이라 패닝 중에도 같은 달에 라벨이 유지된다.
    const spacing = coords.length >= 2 ? Math.abs(coords[1].x - coords[0].x) : Infinity;
    const maxChars = points.reduce((m, p) => Math.max(m, p.label.length), 0);
    const step = spacing > 0 && Number.isFinite(spacing)
      ? Math.max(1, Math.ceil((maxChars * CHAR_WIDTH + LABEL_GAP) / spacing))
      : 1;

    for (const c of coords) {
      if (c.idx % step !== 0) continue;
      this._items.push({
        x: c.x,
        // 양수 바는 위쪽, 음수 바(Max DD)는 아래쪽에
        y: c.point.value >= 0 ? c.y - 5 : c.y + 13,
        label: c.point.label,
        color: c.point.color || CHART_COLORS.text,
      });
    }
  }

  renderer() {
    return new BarValueLabelsRenderer(this._items);
  }

  zOrder() {
    return 'top';
  }
}

export class BarValueLabelsPrimitive extends PaneViewPrimitive {
  constructor() {
    // data 형태: {points: Array<{time, value, label, color?}>}
    super({points: []}, (host) => [new BarValueLabelsPaneView(host)]);
  }
}
