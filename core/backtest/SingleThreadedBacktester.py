import copy
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from tqdm import tqdm

from core.backtest.report import Report
from core.backtest.result_writer import ShardWriter, write_run_json
from core.backtest.status import Status
from core.backtest.trade import Trade
from core.streamer import Action
from core.streamer import BaseStreamer
from core.streamer import Candle


class SingleThreadedBacktester:
    def __init__(self, streamer: BaseStreamer, candles: List[Candle], fee_ratio: float = 0.0004,
                 result_path: str = "asset/"):
        self.streamer = streamer
        self.candles = candles
        self.status = Status(margin=100000.0)
        self.fee_ratio = fee_ratio
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

            # opt-in indicators ingest this candle before the decision sees them
            for indicator in before_indicators:
                indicator.update(candle, self.status)

            # force liquidation
            if self.status.margin <= self.status.unrealised_pnl:
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

        report = Report(trades, max_leverage, self.status, equity_curve)

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
        :param action:
        :param price:
        :return: tuple of wnl, fee
        """
        s = self.status
        qty = action.quantity
        wnl = 0
        fee = price * abs(action.quantity) * self.fee_ratio
        # 신규 진입 (롱/숏 방향 동일하게 처리)
        if s.position == 0.0:
            s.avg_price = price
            s.position = qty

        # 같은 방향 추가 진입 (롱/숏)
        elif (s.position > 0 and qty > 0) or (s.position < 0 and qty < 0):
            total_cost = abs(s.avg_price * s.position) + abs(price * qty)
            total_pos = s.position + qty
            s.avg_price = total_cost / abs(total_pos)
            s.position = total_pos
            # base_margin, unrealised_pnl는 변동 없음

        # 반대 방향 청산(부분/전부)
        else:
            if abs(qty) > abs(s.position):
                # 방향 전환: 기존 포지션 청산 후 신규 진입
                open_qty = qty + s.position

                wnl = s.unrealised_pnl
                # PNL/margin 계산 (전부 청산)
                s.avg_price = price
                s.margin += s.unrealised_pnl
                s.unrealised_pnl = 0.0
                s.position = open_qty
            else:
                # 부분 청산
                closed_qty = qty
                realised_pnl = s.unrealised_pnl * (-closed_qty / s.position)

                wnl = realised_pnl
                # avg_price는 변동 없음
                s.margin += realised_pnl
                s.unrealised_pnl -= realised_pnl
                s.position += closed_qty

        if s.position == 0.0:
            s.avg_price = 0.0

        s.margin -= fee

        return wnl, fee
