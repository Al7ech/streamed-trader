"""백테스트 진입점.

:func:`run_backtest`는 캔들 공급자,
:class:`~core.engine.engine.TradingEngine`, :class:`~core.backtest.simulated_executor.SimulatedExecutor`,
레코더를 조립해 ``Report``를 돌려준다.

캔들 소스는 두 가지 중 하나다:

- ``candles_by_symbol``을 넘기면 :class:`~core.backtest.in_memory_candle_producer.InMemoryCandleProducer`
  가 그것을 병합한다 (합성 캔들 검사, 비-바이낸스 소스).
- ``producer``를 직접 넘기면 그대로 쓴다 — 보통
  :class:`~core.backtest.binance_candle_producer.BinanceBacktestCandleProducer`
  로, 구간을 스스로 fetch한다.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional

from core.backtest.in_memory_candle_producer import InMemoryCandleProducer
from core.backtest.recorder import BacktestRecorder, ensure_backtest_dir
from core.backtest.simulated_executor import SimulatedExecutor
from core.domain.candle import Candle
from core.domain.report import Report
from core.domain.status import Status
from core.engine.candle_producer import CandleProducer
from core.engine.engine import TradingEngine
from core.engine.executor import resolve_fee_ratio, resolve_slippage_ratio
from core.result.indicator_columns import collect_indicator_columns
from core.result.writer import ShardWriter, write_run_json
from core.streamer import BaseStreamer

DEFAULT_INIT_MARGIN = 100_000.0


def run_backtest(streamer: BaseStreamer,
                 candles_by_symbol: Optional[Dict[str, List[Candle]]] = None,
                 *,
                 producer: Optional[CandleProducer] = None,
                 fee_ratio: Optional[float] = None,
                 slippage_ratio: Optional[float] = None,
                 init_margin: float = DEFAULT_INIT_MARGIN,
                 initial_status: Optional[Status] = None,
                 start_time: Optional[int] = None,
                 end_time: Optional[int] = None,
                 metadata: Optional[Dict] = None,
                 save_series: bool = False,
                 has_ohlc: bool = True,
                 result_path: str = "asset/",
                 progress: bool = True) -> Report:
    """병합된 멀티심볼 캔들 타임라인을 스트리머에 흘려보내고 결과를 돌려준다.

    :param candles_by_symbol: 심볼별 캔들 리스트 (각자 end_time 오름차순). 스트리머가 다루는
        심볼과 정확히 일치할 필요는 없다 — 여기 없는 심볼은 이벤트가 아예 발생하지 않는다.
        ``producer``를 주면 무시된다.
    :param producer: 캔들 공급자를 직접 넘긴다. 주면 ``candles_by_symbol`` /
        ``start_time`` / ``end_time``은 무시된다 (공급자가 이미 구간을 안다).
    :param fee_ratio: 실제로 부과할 수수료율. None이면 **스트리머의 값을 따라간다**.
    :param slippage_ratio: 조건부 시장가 체결에 얹을 비율. None이면 같은 규칙.
    :param initial_status: 포지션이나 미체결 주문을 들고 시작해야 할 때. 주면 ``init_margin``
        대신 이 상태로 시작한다. **이 객체는 실행 중 변형되고 ``Report.status``가 된다** —
        두 번 실행할 거라면 각각 새로 만들어 넘겨야 한다.
    :param metadata: 주면 ``<result_path>/backtest/`` 에 런 JSON을 쓴다. ``save_series``까지
        True면 무거운 시계열 샤드도 함께 쓴다. 둘 다 없으면 순수 메모리 실행이다.
    """
    if producer is None:
        if candles_by_symbol is None:
            raise ValueError("candles_by_symbol 또는 producer 중 하나는 넘겨야 한다")
        producer = InMemoryCandleProducer(candles_by_symbol,
                                          start_time=start_time, end_time=end_time,
                                          progress=progress)

    status = initial_status if initial_status is not None else Status(margin=init_margin)
    fee = resolve_fee_ratio(streamer, fee_ratio)
    slippage = resolve_slippage_ratio(streamer, slippage_ratio)

    indicator_names, column_groups = collect_indicator_columns(streamer)
    run_id = f"{type(streamer).__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    #: 진입 시점의 시가평가 자본. summary의 수익률 기준선이자 buy & hold 곡선의 원금이다.
    entry_equity = status.total_margin()

    write_output = metadata is not None
    backtest_dir = ensure_backtest_dir(result_path) if write_output else None
    write_series = write_output and save_series

    shard_writer = None
    if write_series:
        shard_writer = ShardWriter(backtest_dir, run_id, streamer.symbols,
                                   indicator_names, has_ohlc, producer.interval_ms)
    recorder = BacktestRecorder(streamer, status, shard_writer)
    executor = SimulatedExecutor(status, fee, slippage, on_trade=recorder.record_trade)
    asyncio.run(TradingEngine(streamer, executor, recorder).run_async(producer))
    report = recorder.build_report(entry_equity)
    shards = recorder.close_shards()

    if write_output:
        meta = dict(metadata)
        meta.setdefault("streamer", type(streamer).__name__)
        meta["run_at"] = datetime.now(timezone.utc).isoformat()
        meta["init_margin"] = entry_equity
        meta["candle_count"] = producer.event_count
        meta.setdefault("interval_ms", producer.interval_ms)
        write_run_json(backtest_dir, run_id, report, meta, shards,
                       symbols=streamer.symbols,
                       columns=indicator_names + ["balance"],
                       column_groups={**column_groups, "balance": "balance"},
                       has_ohlc=has_ohlc, interval_ms=producer.interval_ms,
                       init_margin=entry_equity)

    return report
