// lightweight-charts v5 series primitive 의 공통 호스트 클래스.
// attach/detach 배선, setData→requestUpdate, pane view 목록 관리만 담당하고,
// 실제 그리기는 서브클래스가 넘긴 pane view(+renderer)가 한다.
// (TradeSegmentsPrimitive / BarValueLabelsPrimitive 가 공유)

export class PaneViewPrimitive {
  /**
   * @param initialData 서브클래스별 data 초기 형태
   * @param viewsFactory (host) => paneView[] — host(this)를 소스로 갖는 pane view 생성
   */
  constructor(initialData, viewsFactory) {
    this.chart = null;
    this.series = null;
    this.data = initialData;
    this._requestUpdate = null;
    this._paneViews = viewsFactory(this);
  }

  attached({chart, series, requestUpdate}) {
    this.chart = chart;
    this.series = series;
    this._requestUpdate = requestUpdate;
  }

  detached() {
    this.chart = null;
    this.series = null;
    this._requestUpdate = null;
  }

  setData(data) {
    this.data = data;
    if (this._requestUpdate) this._requestUpdate();
  }

  updateAllViews() {
    this._paneViews.forEach((view) => view.update());
  }

  paneViews() {
    return this._paneViews;
  }
}
