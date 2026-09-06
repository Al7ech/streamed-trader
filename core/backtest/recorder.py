"""백테스트 한 번의 결과를 메모리에 모으는 레코더.

엔진이 흘려보내는 이벤트/체결을 받아 ``Report``를 만든다. 계약은
:class:`core.engine.recorder.Recorder`에 있다.
"""

import os
from typing import Dict, List, Optional, Tuple

from core.domain.candle import Candle
from core.domain.report import Report
from core.domain.status import Status
from core.domain.trade import Trade
from core.engine.recorder import Recorder
from core.result.metrics import build_multi_symbol_buy_and_hold_curve
from core.result.writer import ShardWriter
from core.streamer import BaseStreamer


class BacktestRecorder(Recorder):
    """백테스트 한 번의 결과를 메모리에 모은다.

    예전 백테스터의 ``run``이 지역 변수로 들고 있던 것(자본 곡선, 심볼별 종가, 체결
    목록, 최대 레버리지, ShardWriter)을 대신 소유한다.

    :param status: 실행기의 ``Status`` **객체 자체**. ``Report.status``가 최종 상태여야 하고
        심볼별 종가를 매 이벤트 읽어야 하므로 참조로 들고 있는다.
    :param shard_writer: None이면 시계열을 남기지 않는다 (비교 스크립트가 쓰는 경로).
    """

    def __init__(self, streamer: BaseStreamer, status: Status,
                 shard_writer: Optional[ShardWriter] = None):
        self._streamer = streamer
        self._status = status
        self._shard_writer = shard_writer
        self.symbols: List[str] = list(streamer.symbols)

        self.equity_curve: List[Tuple[int, float]] = []
        #: buy & hold 기준선을 만들기 위한 심볼별 종가 (구멍은 None) — equity_curve와 같은 길이
        self.closes_by_symbol: Dict[str, List[Optional[float]]] = {s: [] for s in self.symbols}
        self.trades: List[Trade] = []
        self.max_leverage = 0.0
        self.event_count = 0

    def record_event(self, event_time: int, equity: float,
                     candles: Dict[str, Candle]) -> None:
        # 자본과 종가는 **항상 같이** 늘어나야 한다. buy & hold 곡선이 인덱스로 짝을 맞춘다.
        self.equity_curve.append((event_time, equity))
        for symbol in self.symbols:
            self.closes_by_symbol[symbol].append(self._status.last_close.get(symbol))
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

    def close_shards(self) -> List[Dict]:
        """샤드를 마무리하고 인덱스를 돌려준다. 샤드를 안 쓰면 빈 리스트."""
        return self._shard_writer.close() if self._shard_writer is not None else []


def ensure_backtest_dir(result_path: str) -> str:
    """``<result_path>/backtest/``를 만들고 그 경로를 돌려준다."""
    backtest_dir = os.path.join(result_path, "backtest")
    os.makedirs(backtest_dir, exist_ok=True)
    return backtest_dir
