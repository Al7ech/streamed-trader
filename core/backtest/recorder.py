"""백테스트 한 번의 결과를 모으고, 끝나면 산출물까지 쓰는 레코더.

엔진이 흘려보내는 이벤트/체결을 받아 ``Report``를 만들고, ``metadata``를 받았으면
``<result_path>/backtest/`` 에 런 JSON과 (선택적으로) 월별 시계열 샤드를 쓴다. 계약은
:class:`core.engine.recorder.Recorder`에 있다.

**무엇을 어디에 남길지는 레코더가 전부 갖는다.** 엔진은 실행만 알고, 백테스트 전용 산출물
포맷은 알지 못한다 — 라이브가 :class:`~core.live.recorder.LiveRecorder`로 같은 일을 하는 것과
같은 구조다.
"""

import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from core.domain.candle import Candle
from core.domain.report import Report
from core.domain.status import Status
from core.domain.trade import Trade
from core.engine.recorder import Recorder
from core.result.indicator_columns import collect_indicator_columns
from core.result.metrics import build_multi_symbol_buy_and_hold_curve
from core.result.writer import ShardWriter, write_run_json
from core.streamer import BaseStreamer


class BacktestRecorder(Recorder):
    """백테스트 한 번의 결과를 메모리에 모으고, :meth:`close`에서 산출물을 쓴다.

    :param status: 실행기의 ``Status`` **객체 자체**. ``Report.status``가 최종 상태여야 하므로
        참조로 들고 있는다. 진입 시점의 시가평가 자본 (수익률 기준선이자 buy & hold 곡선의
        원금)도 **여기서, 즉 실행 전에** 잡는다.
    :param interval_ms: 캔들 간격. 보통 ``producer.interval_ms``를 그대로 넘긴다 — 샤드 메타와
        Sharpe 리샘플링 주기가 여기서 나온다.
    :param metadata: 주면 런 JSON을 쓴다. 없으면 순수 메모리 실행이다 (대조 검사가 쓰는 경로).
    :param save_series: ``metadata``까지 있을 때만 의미가 있다. True면 무거운 시계열 샤드도 쓴다.
    :param run_id: None이면 ``<StreamerClassName>_<YYYYmmdd_HHMMSS>``.
    """

    def __init__(self, streamer: BaseStreamer, status: Status, *,
                 interval_ms: int = 0,
                 metadata: Optional[Dict] = None,
                 save_series: bool = False,
                 has_ohlc: bool = True,
                 result_path: str = "asset/"):
        self._streamer = streamer
        self._status = status
        self.symbols: List[str] = list(streamer.symbols)

        self.equity_curve: List[Tuple[int, float]] = []
        #: buy & hold 기준선을 만들기 위한 심볼별 종가 (구멍은 None) — equity_curve와 같은 길이
        self.closes_by_symbol: Dict[str, List[Optional[float]]] = {s: [] for s in self.symbols}
        #: 심볼별 최근 종가 캐리포워드. 캔들이 없는 이벤트에서도 직전 값을 이어 붙인다.
        self._latest_close: Dict[str, float] = {}
        self.trades: List[Trade] = []
        self.max_leverage = 0.0
        self.event_count = 0

        #: 진입 시점의 시가평가 자본. 실행 **전**의 값이어야 하므로 여기서 잡는다.
        self.init_equity = status.total_margin()
        self.run_id = f"{type(streamer).__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        self._metadata = metadata
        self._interval_ms = interval_ms
        self._has_ohlc = has_ohlc
        self._indicator_names, self._column_groups = collect_indicator_columns(streamer)

        self._dir = ensure_backtest_dir(result_path) if metadata is not None else None
        self._shard_writer: Optional[ShardWriter] = None
        if metadata is not None and save_series:
            self._shard_writer = ShardWriter(self._dir, self.run_id, self.symbols,
                                             self._indicator_names, has_ohlc, interval_ms)
        self._report: Optional[Report] = None
        self._closed = False

    def record_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        # 자본과 종가는 **항상 같이** 늘어나야 한다. buy & hold 곡선이 인덱스로 짝을 맞춘다.
        equity = self._status.total_margin()
        self.equity_curve.append((event_time, equity))
        for symbol, candle in candles.items():
            self._latest_close[symbol] = candle.close
        for symbol in self.symbols:
            self.closes_by_symbol[symbol].append(self._latest_close.get(symbol))
        self.event_count += 1

        if self._shard_writer is not None:
            symbol_data = {
                symbol: (candle, {name: ind.get_latest() for name, ind
                                  in self._streamer.indicators.get(symbol, {}).items()})
                for symbol, candle in candles.items()
            }
            self._shard_writer.add(event_time, equity, symbol_data)

    def record_trade(self, trade: Trade) -> None:
        self.trades.append(trade)
        if trade.leverage > self.max_leverage:
            self.max_leverage = trade.leverage

    def build_report(self, init_margin: float) -> Report:
        benchmark_curve = build_multi_symbol_buy_and_hold_curve(
            [t for t, _ in self.equity_curve], self.closes_by_symbol, init_margin)
        return Report(self.trades, self.max_leverage, self._status,
                      self.equity_curve, benchmark_curve)

    @property
    def report(self) -> Optional[Report]:
        """:meth:`close` 뒤의 결과. 엔진이 실행을 마치고 돌려주는 값이다."""
        return self._report

    def close(self) -> None:
        """결과를 확정하고 산출물을 쓴다. 엔진이 루프를 마친 뒤 부른다 — **멱등**이다."""
        if self._closed:
            return
        self._closed = True

        self._report = self.build_report(self.init_equity)
        shards = self._shard_writer.close() if self._shard_writer is not None else []
        if self._metadata is None:
            return

        meta = dict(self._metadata)
        meta.setdefault("streamer", type(self._streamer).__name__)
        meta["run_at"] = datetime.now(timezone.utc).isoformat()
        meta["init_margin"] = self.init_equity
        # 공급자가 아니라 **레코더가 센 이벤트 수**다. 실제로 처리된 수라 중간에 끊겨도 맞고,
        # CandleProducer 계약에 없는 속성을 읽지 않아도 된다.
        meta["candle_count"] = self.event_count
        meta.setdefault("interval_ms", self._interval_ms)
        write_run_json(self._dir, self.run_id, self._report, meta, shards,
                       symbols=self.symbols,
                       columns=self._indicator_names + ["balance"],
                       column_groups={**self._column_groups, "balance": "balance"},
                       has_ohlc=self._has_ohlc, interval_ms=self._interval_ms,
                       init_margin=self.init_equity)


def ensure_backtest_dir(result_path: str) -> str:
    """``<result_path>/backtest/``를 만들고 그 경로를 돌려준다."""
    backtest_dir = os.path.join(result_path, "backtest")
    os.makedirs(backtest_dir, exist_ok=True)
    return backtest_dir
