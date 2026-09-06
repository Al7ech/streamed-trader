"""백테스트 진입점.

:func:`run_backtest`는 :class:`~core.engine.candle_producer.BacktestCandleProducer`,
:class:`~core.engine.engine.TradingEngine`, :class:`~core.engine.executor.SimulatedExecutor`,
레코더를 조립해 ``Report``를 돌려준다.

``vectorized``만이 두 경로를 가른다:

- ``False`` — 지표를 캔들마다 갱신하고 자본 곡선을 이벤트마다 적재한다. 참조 구현이다.
- ``True`` — ``precompute_series``를 정의한 지표를 numpy로 한 번에 계산해 커서 심으로 갈아끼우고,
  자본 곡선을 루프 후에 벡터로 재구성한다. 빠르지만 결과는 같아야 한다.

둘이 같은 ``Report``를 내는지는 ``core/backtest_fast_check.py``가 검증한다.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional

from core.engine.candle_producer import BacktestCandleProducer
from core.engine.engine import TradingEngine
from core.engine.executor import SimulatedExecutor, resolve_fee_ratio, resolve_slippage_ratio
from core.engine.indicator_columns import collect_indicator_columns
from core.engine.recorder import BacktestRecorder, ensure_backtest_dir
from core.engine.report import Report
from core.engine.result_writer import ShardWriter, write_run_json
from core.engine.status import Status
from core.streamer import BaseStreamer
from core.streamer.candle import Candle

DEFAULT_INIT_MARGIN = 100_000.0


def run_backtest(streamer: BaseStreamer,
                 candles_by_symbol: Dict[str, List[Candle]],
                 *,
                 vectorized: bool = True,
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
    :param fee_ratio: 실제로 부과할 수수료율. None이면 **스트리머의 값을 따라간다**.
    :param slippage_ratio: 조건부 시장가 체결에 얹을 비율. None이면 같은 규칙.
    :param initial_status: 포지션이나 미체결 주문을 들고 시작해야 할 때. 주면 ``init_margin``
        대신 이 상태로 시작한다. **이 객체는 실행 중 변형되고 ``Report.status``가 된다** —
        두 번 실행할 거라면 각각 새로 만들어 넘겨야 한다.
    :param metadata: 주면 ``<result_path>/backtest/`` 에 런 JSON을 쓴다. ``save_series``까지
        True면 무거운 시계열 샤드도 함께 쓴다. 둘 다 없으면 순수 메모리 실행이다.
    """
    status = initial_status if initial_status is not None else Status(margin=init_margin)
    fee = resolve_fee_ratio(streamer, fee_ratio)
    slippage = resolve_slippage_ratio(streamer, slippage_ratio)

    producer = BacktestCandleProducer(candles_by_symbol, streamer.symbols,
                                      start_time=start_time, end_time=end_time,
                                      materialize=vectorized, progress=progress)

    indicator_names, column_groups = collect_indicator_columns(streamer)
    run_id = f"{type(streamer).__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    #: 진입 시점의 시가평가 자본. summary의 수익률 기준선이자 buy & hold 곡선의 원금이다.
    entry_equity = status.total_margin()

    write_output = metadata is not None
    backtest_dir = ensure_backtest_dir(result_path) if write_output else None
    write_series = write_output and save_series

    if vectorized:
        # 순환 임포트를 피하려고 여기서 임포트한다 (vectorized가 이 모듈의 상수를 쓴다).
        from core.engine.vectorized import run_vectorized
        report, shards = run_vectorized(
            streamer, producer, status, fee, slippage, entry_equity,
            indicator_names=indicator_names, has_ohlc=has_ohlc,
            shard_dir=backtest_dir if write_series else None, run_id=run_id)
    else:
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
