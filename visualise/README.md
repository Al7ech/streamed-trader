# Backtest Visualizer

React frontend for the backtest runs written by `core/`. It has no backend — you point it at the
`asset/` directory on your disk and everything is parsed in the browser.

## Running it

```bash
npm install
npm start        # http://localhost:3000
```

On first load, click **asset 폴더 선택** and choose the repository's `asset/` directory. The handle
is persisted in IndexedDB, so refreshes and later sessions reload it automatically — you only pick
it once.

This uses the **File System Access API** (`showDirectoryPicker`), which needs a Chromium-based
browser (Chrome/Edge/Brave). Firefox and Safari don't implement it.

Produce something to look at with:

```bash
cd ../core && python backtest.py "my run"
```

## What it reads

Everything under `asset/backtest/`, written by `core/backtest/result_writer.py`:

- `<run_id>.json` — metadata, summary metrics, downsampled equity curve, and the full trade list.
- `<run_id>.<YYYY-MM>.series.json` — one month-bucketed columnar shard per month, holding
  per-candle OHLC and indicator values.

Shards are loaded **lazily, per viewport**: a multi-year 1m run is tens of millions of candles, so
the app fetches only the months currently in view and aggregates to coarser timeframes (1h/1d) when
zoomed out. The run list never touches shards at all — it reads just the small run JSONs.

## Pages

**`/` — run list.** Every run JSON in the directory as a table: date, symbol, label, params, and
summary metrics (profit %, Sharpe, max drawdown, win rate, max leverage). Sorted newest first.

**`/run/:fileName` — run detail.** An AppShell with a collapsible sidebar listing the other runs,
and two tabs:

- **Visualise** — candlestick chart with indicator overlays, trade entry/exit markers and position
  segments, plus a timeline selector with an equity sparkline for jumping around the run.
  Indicators are placed by their `scale_group`: `"price"` overlays the candles, anything else gets
  its own pane.
- **Trades** — equity and drawdown charts, KPI tiles, and monthly performance bars. Mounted lazily
  the first time you open it, then kept mounted.

## Source layout

```
src/
  App.js                     routing + the directory-picker gate
  hooks/useBacktestFiles.js  directory selection/restoration, run file listing
  components/
    RunListPage.js           the run table
    RunDetailPage.js         detail layout + data orchestration
    RunNavbar.js             sidebar run switcher
    InfoPanel.js             header metadata/metrics
    TradingChart.js          candles + indicators + trades
    TradeTable.js            per-trade rows
    chart/                   lightweight-charts primitives, timeline selector, navigation hook
    stats/                   equity chart, monthly stats, KPI tiles
  utils/
    fileAccess.js            File System Access API + IndexedDB handle persistence
    runLoader.js             run JSON parsing
    seriesLoader.js          lazy per-viewport shard loading
    aggregate.js             1m → coarse timeframe aggregation
    runStats.js              summary/derived metrics
    monthlyStats.js          monthly bucketing
    tradeSegments.js         trades → position segments for the chart
    chartSync.js             cross-pane time-scale sync
    format.js                number/date formatting
```

## Stack

React 18, Mantine 8 (UI), lightweight-charts 5 (TradingView's charting library), React Router 6,
Create React App.

```bash
npm run build   # production bundle into build/
npm test        # CRA/jest
```
