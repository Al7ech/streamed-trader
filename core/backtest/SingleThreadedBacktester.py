import copy
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from tqdm import tqdm

from core.backtest.metrics import build_buy_and_hold_curve
from core.backtest.report import Report
from core.backtest.result_writer import ShardWriter, write_run_json
from core.backtest.status import Status
from core.backtest.trade import Trade
from core.streamer import Action
from core.streamer import BaseStreamer
from core.streamer import Candle


DEFAULT_FEE_RATIO = 0.0004


class SingleThreadedBacktester:
    def __init__(self, streamer: BaseStreamer, candles: List[Candle],
                 fee_ratio: Optional[float] = None, result_path: str = "asset/"):
        """:param fee_ratio: 실제로 부과할 수수료율. None이면 **스트리머의 값을 따라간다**.

        스트리머는 자기 ``fee_ratio``로 사이징하고 백테스터는 자기 값으로 과금하므로, 둘이
        어긋나면 실효 레버리지가 의도와 달라진다. 예전 기본값(고정 0.0004)은 ``FastBacktester(
        streamer, candles)``처럼 인자 없이 부를 때 그 불일치를 조용히 만들 수 있었다.
        """
        self.streamer = streamer
        self.candles = candles
        self.status = Status(margin=100000.0)
        streamer_fee = getattr(streamer, "fee_ratio", None)
        if fee_ratio is None:
            self.fee_ratio = streamer_fee if streamer_fee is not None else DEFAULT_FEE_RATIO
        else:
            self.fee_ratio = fee_ratio
            if streamer_fee is not None and streamer_fee != fee_ratio:
                logging.getLogger(__name__).warning(
                    "수수료율 불일치: 백테스터 %s vs 스트리머 %s — 스트리머는 자기 값으로 "
                    "사이징하므로 실효 레버리지가 의도와 달라진다", fee_ratio, streamer_fee)
        self.result_path = result_path

    def run(self, start_time: Optional[int] = None, end_time: Optional[int] = None,
            metadata: Optional[Dict] = None, save_series: bool = False,
            has_ohlc: bool = True) -> Report:
        """Replay the candles through the streamer.

        When ``metadata`` is provided, a run JSON is written to ``<result_path>/backtest/``.
        When ``save_series`` is also True, the heavy per-candle OHLC + indicator time-series is
        streamed out as month-bucketed columnar shards next to it. Passing neither runs the
        backtest purely in memory (used by comparison scripts).
        """
        trades: List[Trade] = []
        max_leverage = 0.0
        equity_curve: List[Tuple[int, float]] = []
        closes: List[float] = []  # buy & hold 기준선을 만들기 위한 종가 (equity_curve 와 같은 길이)

        indicator_names = list(self.streamer.indicators.keys())
        column_groups = {name: self.streamer.indicators[name].scale_group for name in indicator_names}
        streamer_name = type(self.streamer).__name__
        run_id = f"{streamer_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        init_margin = self.status.total_margin()
        interval_ms = (self.candles[1].start_time - self.candles[0].start_time
                       if len(self.candles) >= 2 else 0)

        # Indicators are split by when they ingest the candle being decided on. Both groups get
        # the same pre-trade Status; only the position relative to decide_action differs.
        before_indicators = [ind for ind in self.streamer.indicators.values()
                             if ind.updates_before_decide]
        after_indicators = [ind for ind in self.streamer.indicators.values()
                            if not ind.updates_before_decide]

        write_output = metadata is not None
        backtest_dir = os.path.join(self.result_path, "backtest")
        shard_writer: Optional[ShardWriter] = None
        if write_output:
            os.makedirs(backtest_dir, exist_ok=True)
            if save_series:
                shard_writer = ShardWriter(backtest_dir, run_id, indicator_names + ["balance"],
                                           has_ohlc, interval_ms)

        candle_count = 0
        for candle in tqdm(self.candles, desc="Backtesting", unit="candle", file=sys.stdout):
            if start_time and candle.end_time < start_time:
                continue
            if end_time and candle.end_time > end_time:
                break
            candle_count += 1

            self.status.update_unrealised_pnl(candle.close)
            equity_curve.append((candle.end_time, self.status.total_margin()))
            closes.append(candle.close)

            # opt-in indicators ingest this candle before the decision sees them
            for indicator in before_indicators:
                indicator.update(candle, self.status)

            # 강제청산: 시가평가 자본(margin + 미실현손익)이 0 이하로 떨어지면 파산이다.
            # flat일 때도 이 줄을 타는데, 그때는 미실현이 0이라 조건이 margin <= 0 과 같고
            # Action(-0) = Action(0) 이므로 "파산 후에는 스트리머를 부르지 않는다"가 된다.
            if self.status.total_margin() <= 0.0:
                action = Action(-self.status.position)
            else:
                action = self.streamer.update_candle(candle, self.status)

            if shard_writer is not None:
                # recorded between the two update groups, so every column is the value
                # decide_action actually saw — whichever side of the decision it was fed on
                values = {name: self.streamer.indicators[name].get_latest()
                          for name in indicator_names}
                # pre-trade equity at this candle — same value equity_curve recorded above
                values["balance"] = self.status.total_margin()
                shard_writer.add(candle, values)

            # indicators see the same pre-trade status decide_action saw for this candle
            for indicator in after_indicators:
                indicator.update(candle, self.status)

            if action.quantity != 0:
                prev_status = copy.deepcopy(self.status)

                wnl, fee = self._trade(action, candle.close)

                leverage = self.status.update_leverage()
                max_leverage = max(max_leverage, leverage)
                trades.append(Trade(
                    timestamp=candle.end_time,
                    quantity=action.quantity,
                    price=candle.close,
                    wnl=wnl,
                    fee=fee,
                    status=prev_status,
                    leverage=leverage,
                ))

        benchmark_curve = build_buy_and_hold_curve([t for t, _ in equity_curve], closes,
                                                   init_margin)
        report = Report(trades, max_leverage, self.status, equity_curve, benchmark_curve)

        if write_output:
            shards = shard_writer.close() if shard_writer is not None else []
            meta = dict(metadata)
            meta.setdefault("streamer", streamer_name)
            meta["run_at"] = datetime.now(timezone.utc).isoformat()
            meta["init_margin"] = init_margin
            meta["candle_count"] = candle_count
            meta.setdefault("interval_ms", interval_ms)
            write_run_json(backtest_dir, run_id, report, meta, shards,
                           columns=indicator_names + ["balance"],
                           column_groups={**column_groups, "balance": "balance"},
                           has_ohlc=has_ohlc, interval_ms=interval_ms, init_margin=init_margin)

        return report

    def _trade(self, action: Action, price: float) -> Tuple[float, float]:
        """
        체결 회계는 ``Status.apply_fill``에 있다 — 라이브 트레이더의 dry-run 경로가 같은
        코드를 쓰기 위해서다.

        :param action:
        :param price:
        :return: tuple of wnl, fee
        """
        return self.status.apply_fill(action.quantity, price, self.fee_ratio)
