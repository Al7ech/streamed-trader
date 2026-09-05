import copy
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from tqdm import tqdm

from core.engine import order_book
from core.engine.candle_merge import merge_candle_timeline
from core.engine.indicator_columns import collect_indicator_columns
from core.engine.metrics import build_multi_symbol_buy_and_hold_curve
from core.engine.report import Report
from core.engine.result_writer import ShardWriter, write_run_json
from core.engine.status import Status
from core.engine.trade import Trade
from core.streamer import Action
from core.streamer import ActionType
from core.streamer import BaseStreamer
from core.streamer import Candle


DEFAULT_FEE_RATIO = 0.0004
DEFAULT_SLIPPAGE_RATIO = 0.0


class SingleThreadedBacktester:
    def __init__(self, streamer: BaseStreamer, candles_by_symbol: Dict[str, List[Candle]],
                 fee_ratio: Optional[float] = None, result_path: str = "asset/",
                 slippage_ratio: Optional[float] = None):
        """:param candles_by_symbol: 심볼별 캔들 리스트 (각자 end_time 오름차순). 스트리머가
            다루는 심볼(``streamer.symbols``)과 정확히 일치할 필요는 없다 — 여기 없는 심볼은
            그 심볼의 이벤트가 아예 발생하지 않는다.
        :param fee_ratio: 실제로 부과할 수수료율. None이면 **스트리머의 값을 따라간다**.
        :param slippage_ratio: 조건부 시장가(STOP_MARKET) 체결에 불리한 방향으로 얹을 비율.
            None이면 ``fee_ratio``와 같은 규칙으로 스트리머의 값을 따라간다. 기본 0.0이라
            지정가/조건부 주문을 쓰지 않는 기존 전략의 결과는 그대로다.

        스트리머는 자기 ``fee_ratio``로 사이징하고 백테스터는 자기 값으로 과금하므로, 둘이
        어긋나면 실효 레버리지가 의도와 달라진다. 예전 기본값(고정 0.0004)은 ``FastBacktester(
        streamer, candles)``처럼 인자 없이 부를 때 그 불일치를 조용히 만들 수 있었다.
        """
        self.streamer = streamer
        self.candles_by_symbol = candles_by_symbol
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
        streamer_slippage = getattr(streamer, "slippage_ratio", None)
        if slippage_ratio is None:
            self.slippage_ratio = (streamer_slippage if streamer_slippage is not None
                                   else DEFAULT_SLIPPAGE_RATIO)
        else:
            self.slippage_ratio = slippage_ratio
            if streamer_slippage is not None and streamer_slippage != slippage_ratio:
                logging.getLogger(__name__).warning(
                    "슬리피지율 불일치: 백테스터 %s vs 스트리머 %s", slippage_ratio,
                    streamer_slippage)
        self.result_path = result_path
        #: 미체결 주문 제출 순서. 같은 봉 안에서 체결 순서를 결정적으로 만드는 데 쓴다.
        self._order_seq = 0
        self.logger = logging.getLogger(__name__)

    def _interval_ms(self) -> int:
        """캔들 간격을 아무 심볼에서나 재고, 다른 심볼과 어긋나면 에러를 낸다.

        병합 타임라인은 "같은 시각 = 같은 봉 경계"를 전제하므로, 심볼마다 간격이 다르면
        병합 자체가 의미를 잃는다. 혼합 간격 멀티심볼 백테스트는 지원 범위 밖이다.
        """
        ref_ms: Optional[int] = None
        ref_symbol: Optional[str] = None
        for symbol in self.streamer.symbols:
            candles = self.candles_by_symbol.get(symbol) or []
            if len(candles) < 2:
                continue
            ms = candles[1].start_time - candles[0].start_time
            if ref_ms is None:
                ref_ms, ref_symbol = ms, symbol
            elif ms != ref_ms:
                raise ValueError(
                    f"심볼별 캔들 간격이 다르다: {ref_symbol}={ref_ms}ms vs {symbol}={ms}ms — "
                    f"멀티심볼 백테스트는 모든 심볼이 같은 간격이어야 한다.")
        return ref_ms or 0

    def run(self, start_time: Optional[int] = None, end_time: Optional[int] = None,
            metadata: Optional[Dict] = None, save_series: bool = False,
            has_ohlc: bool = True) -> Report:
        """Replay the merged multi-symbol candle timeline through the streamer.

        When ``metadata`` is provided, a run JSON is written to ``<result_path>/backtest/``.
        When ``save_series`` is also True, the heavy per-event OHLC + indicator time-series is
        streamed out as month-bucketed columnar shards next to it. Passing neither runs the
        backtest purely in memory (used by comparison scripts).
        """
        trades: List[Trade] = []
        max_leverage = 0.0
        equity_curve: List[Tuple[int, float]] = []
        # buy & hold 기준선을 만들기 위한 심볼별 종가 (구멍은 None) — equity_curve 와 같은 길이
        closes_by_symbol: Dict[str, List[Optional[float]]] = {
            s: [] for s in self.streamer.symbols}

        indicator_names, column_groups = collect_indicator_columns(self.streamer)

        streamer_name = type(self.streamer).__name__
        run_id = f"{streamer_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        init_margin = self.status.total_margin()
        interval_ms = self._interval_ms()

        write_output = metadata is not None
        backtest_dir = os.path.join(self.result_path, "backtest")
        shard_writer: Optional[ShardWriter] = None
        if write_output:
            os.makedirs(backtest_dir, exist_ok=True)
            if save_series:
                shard_writer = ShardWriter(backtest_dir, run_id, self.streamer.symbols,
                                           indicator_names, has_ohlc, interval_ms)

        event_count = 0
        for event_time, batch in tqdm(merge_candle_timeline(self.candles_by_symbol),
                                      desc="Backtesting", unit="event", file=sys.stdout):
            if start_time and event_time < start_time:
                continue
            if end_time and event_time > end_time:
                break
            event_count += 1

            event_candles: Dict[str, Candle] = {}
            for symbol, candle in batch:
                event_candles[symbol] = candle
                self.status.last_close[symbol] = candle.close

            # 미체결 주문은 봉이 닫히기 전에 거래소에서 채워진다 — 그러니 시가평가보다도,
            # decide_action보다도 **앞에서** 매칭한다. 뒤에 두면 전략이 "이미 손절된 포지션을
            # 아직 들고 있다"고 착각한 채 결정하게 된다.
            for trade in self._match_resting_orders(event_candles, event_time):
                max_leverage = max(max_leverage, trade.leverage)
                trades.append(trade)

            # 매 이벤트 시가평가 — 열린 포지션 전부, 최신 알려진 종가로 (이 이벤트에 캔들이
            # 없는 심볼은 직전 알려진 종가를 그대로 쓴다).
            for symbol, pos_state in self.status.positions.items():
                if pos_state.position != 0.0 and symbol in self.status.last_close:
                    self.status.update_unrealised_pnl(symbol, self.status.last_close[symbol])
            event_equity = self.status.total_margin()
            equity_curve.append((event_time, event_equity))
            for symbol in self.streamer.symbols:
                closes_by_symbol[symbol].append(self.status.last_close.get(symbol))

            # 강제청산: 시가평가 자본(margin + 미실현손익 합)이 0 이하로 떨어지면 파산이다.
            # 증거금이 공유 풀이므로 한 심볼이 자본을 다 태우면 나머지 심볼도 전부 청산한다.
            # flat인 심볼도 이 분기를 타는데, 그때는 Action(symbol, -0)이 quantity==0으로
            # 걸러지므로 "파산 후에는 스트리머를 부르지 않는다"가 유지된다. 지표 갱신과 샤드
            # 기록은 파산 여부와 무관하게 이 이벤트에 등장한 모든 심볼에 대해 계속된다 —
            # 건너뛰는 것은 오직 스트리머의 결정 호출뿐이다.
            bankrupt = event_equity <= 0.0
            if bankrupt:
                # 파산 후에도 손절 주문이 장부에 남으면, flat이 된 계좌에 나중에 유령
                # 포지션을 여는 체결이 생긴다.
                cancelled = order_book.cancel_all(self.status)
                if cancelled:
                    self.logger.warning("강제청산: 미체결 주문 %d건을 취소한다", len(cancelled))
            actions: List[Action] = []
            symbol_data: Dict[str, Tuple[Candle, Dict[str, Optional[float]]]] = {}
            for symbol in self.streamer.symbols:
                candle = event_candles.get(symbol)
                if candle is None:
                    continue

                # indicators always ingest the candle before the decision, with the same
                # pre-trade status decide_action sees for this candle
                for indicator in self.streamer.indicators.get(symbol, {}).values():
                    indicator.update(candle, self.status)

                if bankrupt:
                    position = self.status.position_for(symbol).position
                    symbol_actions = [Action(symbol, -position)] if position != 0.0 else []
                else:
                    symbol_actions = self.streamer.decide_action(symbol, candle, self.status)

                if shard_writer is not None:
                    # recorded right after decide_action, once every indicator for this symbol
                    # is already updated — so every column is the value the decision actually saw
                    values = {name: ind.get_latest()
                             for name, ind in self.streamer.indicators.get(symbol, {}).items()}
                    symbol_data[symbol] = (candle, values)

                actions.extend(symbol_actions)

            if shard_writer is not None:
                shard_writer.add(event_time, event_equity, symbol_data)

            for action in actions:
                # 장부 조작(취소/지정가·조건부 등록)이 먼저다 — CANCEL은 quantity가 0이라
                # 아래의 0 스킵에 걸려 조용히 사라진다.
                if self._submit_book_action(action, event_time):
                    continue
                if action.quantity == 0:
                    continue
                price = self.status.last_close.get(action.symbol)
                if price is None:
                    self.logger.warning(
                        "가격을 알 수 없는 심볼 %s 에 대한 액션을 건너뛴다: %s",
                        action.symbol, action)
                    continue

                trade = self._fill(action.symbol, action.quantity, price, event_time,
                                   ActionType.MARKET.value, event_time)
                max_leverage = max(max_leverage, trade.leverage)
                trades.append(trade)

        benchmark_curve = build_multi_symbol_buy_and_hold_curve(
            [t for t, _ in equity_curve], closes_by_symbol, init_margin)
        report = Report(trades, max_leverage, self.status, equity_curve, benchmark_curve)

        if write_output:
            shards = shard_writer.close() if shard_writer is not None else []
            meta = dict(metadata)
            meta.setdefault("streamer", streamer_name)
            meta["run_at"] = datetime.now(timezone.utc).isoformat()
            meta["init_margin"] = init_margin
            meta["candle_count"] = event_count
            meta.setdefault("interval_ms", interval_ms)
            write_run_json(backtest_dir, run_id, report, meta, shards,
                           symbols=self.streamer.symbols,
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
        return self.status.apply_fill(action.symbol, action.quantity, price, self.fee_ratio)

    def _fill(self, symbol: str, quantity: float, price: float, event_time: int,
              order_type: str, submitted_at: int) -> Trade:
        """체결 하나를 반영하고 Trade를 만든다. 두 백테스터가 공유한다.

        거래 전 스냅샷은 ``apply_fill`` **전에** 떠야 한다 — Trade.status의 계약이고,
        승패 분류(``result_writer._win_lose_counts``)가 그 시점의 포지션 부호를 본다.

        ``apply_fill``의 청산 손익은 ``unrealised_pnl``을 안분해서 구하므로, 그 값이 **체결가
        기준**이어야 실현손익이 맞는다. 시장가는 체결가가 곧 이벤트 종가라 직전 시가평가가
        이미 그 값이지만, 지정가/조건부는 봉 중간 가격에 체결되므로 여기서 다시 매긴다
        (시장가에는 같은 값을 다시 계산하는 무해한 no-op이다).
        """
        self.status.update_unrealised_pnl(symbol, price)
        prev_status = copy.deepcopy(self.status)
        wnl, fee = self.status.apply_fill(symbol, quantity, price, self.fee_ratio)
        leverage = self.status.update_leverage()
        return Trade(
            timestamp=event_time,
            symbol=symbol,
            quantity=quantity,
            price=price,
            wnl=wnl,
            fee=fee,
            status=prev_status,
            leverage=leverage,
            order_type=order_type,
            submitted_at=submitted_at,
        )

    def _match_resting_orders(self, event_candles: Dict[str, Candle],
                              event_time: int) -> List[Trade]:
        """이 이벤트의 캔들들로 미체결 주문을 체결시키고, 만료된 주문을 거둔다.

        심볼 순회는 ``streamer.symbols`` 순서다 — 결정 루프와 같은 순서를 써야 두 백테스터가
        같은 결과를 낸다. ``match_symbol``이 제너레이터이므로 체결은 한 건씩 즉시 반영되고,
        같은 봉의 뒤쪽 주문은 갱신된 포지션을 본다.
        """
        filled: List[Trade] = []
        for symbol in self.streamer.symbols:
            candle = event_candles.get(symbol)
            if candle is None:
                continue
            for order, price, quantity in order_book.match_symbol(
                    self.status, symbol, candle, self.slippage_ratio):
                filled.append(self._fill(symbol, quantity, price, event_time,
                                         order.order_type.value, order.created_at))
            order_book.tick_expiry(self.status, symbol)
        return filled

    def _submit_book_action(self, action: Action, event_time: int) -> bool:
        """장부를 건드리는 액션이면 처리하고 True. 즉시 체결(MARKET)이면 False."""
        if action.order_type is ActionType.CANCEL:
            order_book.cancel_orders(self.status, action.symbol, action.client_id)
            return True
        if action.is_resting:
            self._order_seq += 1
            order_book.register_order(self.status, action, event_time, self._order_seq)
            return True
        return False
