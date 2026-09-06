"""실행 결과 적재 계층.

엔진은 이벤트 하나가 끝날 때마다 :meth:`Recorder.record_event`를, 체결이 날 때마다
:meth:`Recorder.record_trade`를 부른다. 무엇을 어디에 남길지는 전부 구현체가 정한다:

- :class:`BacktestRecorder` — 메모리에 모았다가 ``Report``를 만들고, 선택적으로 샤드를 쓴다.
- ``LiveRecorder`` (:mod:`core.trader.live_recorder`) — 주기적으로 체크포인트를 남기고
  재기동 시 이어쓴다.

지표 값은 인자로 받지 않고 **레코더가 직접 읽는다** (``streamer.indicators``). 필요 없는
레코더가 매 이벤트 전 지표를 순회하는 낭비를 없애기 위해서다 — 시리즈를 저장하지 않는
백테스트가 가장 흔한 경우다.
"""

import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from core.engine.metrics import build_multi_symbol_buy_and_hold_curve
from core.engine.report import Report
from core.engine.result_writer import ShardWriter
from core.engine.status import Status
from core.engine.trade import Trade
from core.streamer import BaseStreamer
from core.streamer.candle import Candle


class Recorder(ABC):
    """엔진이 흘려보내는 이벤트/체결을 받는다."""

    @abstractmethod
    def record_event(self, event_time: int, equity: float,
                     candles: Dict[str, Candle]) -> None:
        """이벤트 하나를 적재한다.

        엔진은 모든 지표를 갱신하고 ``decide_action``을 부른 **직후**에 이것을 부른다. 따라서
        지금 ``streamer.indicators``에서 읽는 값은 그 결정이 실제로 본 값이다.

        :param equity: 이 이벤트 시점의 계좌 전체 시가평가 자본. 미체결 주문 체결은 이미
            반영돼 있고, 이번 이벤트의 시장가 체결은 아직 반영되지 않았다.
        :param candles: 이번 이벤트에 캔들이 마감한 심볼만 담긴다.
        """

    @abstractmethod
    def record_trade(self, trade: Trade) -> None:
        """체결 하나를 적재한다. 호출 시점의 ``status``는 이미 이 체결이 반영된 상태다."""

    def end_event(self, event_time: int) -> None:
        """이벤트의 모든 액션 처리가 끝난 뒤. 기본은 아무것도 하지 않는다."""

    def close(self) -> None:
        """더 이상 기록하지 않는다. 기본은 아무것도 하지 않는다."""


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


class NullRecorder(Recorder):
    """아무것도 남기지 않는 레코더. 기록이 꺼진 라이브 실행처럼 엔진은 돌려야 하지만 결과를
    적재할 필요가 없을 때 쓴다 — 매번 ``if recorder`` 로 감싸지 않기 위한 것이다."""

    def record_event(self, event_time: int, equity: float,
                     candles: Dict[str, Candle]) -> None:
        pass

    def record_trade(self, trade: Trade) -> None:
        pass


def ensure_backtest_dir(result_path: str) -> str:
    """``<result_path>/backtest/``를 만들고 그 경로를 돌려준다."""
    backtest_dir = os.path.join(result_path, "backtest")
    os.makedirs(backtest_dir, exist_ok=True)
    return backtest_dir
