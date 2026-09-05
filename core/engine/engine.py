"""매매 순서 규약. 이 저장소에 **한 벌만** 존재하는 이벤트 처리 루프다.

백테스트, 드라이런, 라이브가 전부 :meth:`TradingEngine.process_event`를 지나간다. 셋의 차이는
꽂히는 부품뿐이다:

===========  =========================  ==================  ================
             CandleProducer             Executor            Recorder
===========  =========================  ==================  ================
백테스트     BacktestCandleProducer     SimulatedExecutor   BacktestRecorder
드라이런     LiveCandleProducer         SimulatedExecutor   LiveRecorder
라이브       LiveCandleProducer         LiveExecutor        LiveRecorder
===========  =========================  ==================  ================

**엔진은 완전히 동기다.** 거래소의 확인을 기다려야 하는 주문은 :class:`~core.engine.executor.
Dispatch`로 ``yield``해 호출자에게 넘긴다 — 동기 호출자(백테스트)는 그냥 소진하고, 비동기
호출자(라이브)만 그 사이에서 ``await``한다. 덕분에 백테스트의 타이트한 루프에는 await 오버헤드가
전혀 없으면서, 라이브의 "주문 N의 결과를 확인한 뒤 N+1을 보낸다"는 순서도 그대로 보존된다.
"""

import logging
from collections import deque
from typing import Dict, Iterator, List

from core.engine.candle_producer import CandleProducer
from core.engine.executor import Dispatch, Executor
from core.engine.recorder import Recorder
from core.streamer import BaseStreamer
from core.streamer.action import Action
from core.streamer.candle import Candle
from core.utils import generate_dict_string


class TradingEngine:
    def __init__(self, streamer: BaseStreamer, executor: Executor, recorder: Recorder):
        self.streamer = streamer
        self.executor = executor
        self.recorder = recorder
        self.logger = logging.getLogger(__name__)

    def process_event(self, event_time: int,
                      candles: Dict[str, Candle]) -> Iterator[Dispatch]:
        """이벤트 하나를 처리한다. 거래소 확인이 필요한 주문마다 :class:`Dispatch`를 yield.

        **호출자는 제너레이터를 끝까지 소진해야 한다** — 마지막 yield 뒤에
        ``recorder.end_event``가 있다.

        단계 순서가 이 클래스의 존재 이유다. 특히 미체결 주문 매칭이 ``decide_action`` **앞에**
        있는 것: 실제 거래소에서는 미체결 주문이 봉이 닫히기 전에 체결되므로, 뒤에 두면 전략이
        "이미 손절된 포지션을 아직 들고 있다"고 착각한 채 결정하게 된다.
        """
        st = self.executor.status

        # 0. 실행기가 이벤트 경계에서 정리할 것이 있으면 (라이브의 미체결 결정 폐기 등)
        self.executor.begin_event(event_time, candles)

        # 1. 이 이벤트의 종가를 **먼저 전부** 동결한다. 다른 심볼을 겨냥한 액션의 체결가가
        #    여기서 정해지므로, 심볼 처리 순서와 무관하게 결정적이어야 한다.
        for symbol, candle in candles.items():
            st.last_close[symbol] = candle.close

        # 2. 미체결 주문 매칭. 심볼 순회는 streamer.symbols 순서다 — 결정 루프와 같은 순서를
        #    써야 어떤 실행 경로에서도 같은 결과가 나온다.
        for symbol in self.streamer.symbols:
            candle = candles.get(symbol)
            if candle is not None:
                self.executor.match_resting(symbol, candle, event_time)

        # 3. 시가평가. 여기서 잰 자본이 이벤트의 자본 곡선 값이 된다 — 미체결 체결은 이미
        #    반영돼 있고, 아래 단계 7의 시장가 체결은 아직 반영되지 않은 시점이다.
        equity = self.executor.mark_to_market()

        # 4. 파산 판정. flat인 심볼도 이 분기를 타는데, 그때는 flatten 액션이 만들어지지 않아
        #    "파산 후에는 스트리머를 부르지 않는다"가 유지된다. 지표 갱신과 기록은 파산 여부와
        #    무관하게 계속된다 — 건너뛰는 것은 오직 스트리머의 결정 호출뿐이다.
        bankrupt = self.executor.force_liquidation(equity)

        # 5. 심볼별 지표 갱신 → 결정. 모든 지표가 decide_action보다 **먼저** 이 캔들을
        #    반영한다. status는 decide_action이 보는 것과 같은 거래 전 스냅샷이다.
        actions: List[Action] = []
        for symbol in self.streamer.symbols:
            candle = candles.get(symbol)
            if candle is None:
                continue

            symbol_indicators = self.streamer.indicators.get(symbol, {})
            for indicator in symbol_indicators.values():
                indicator.update(candle, st)

            if bankrupt:
                position = st.position_for(symbol).position
                if position != 0.0:
                    actions.append(Action(symbol, -position))
            else:
                actions.extend(self.streamer.decide_action(symbol, candle, st))

            # 캔들 하나당 DEBUG 한 줄. isEnabledFor로 감싸는 게 핵심이다 —
            # generate_dict_string은 전 지표를 순회하며 get_latest()를 부르는데, 인자로 넘기면
            # DEBUG가 꺼져 있어도 매 캔들 실행된 뒤 버려진다.
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("symbol=%s candle=%s actions=%s status=%s indicators=%s",
                                  symbol, candle, actions, st,
                                  generate_dict_string(symbol_indicators))

        # 6. 기록. 모든 지표가 이미 갱신되고 decide_action이 반환한 직후라, 레코더가 읽는 모든
        #    값이 그 결정이 실제로 본 값이다.
        self.recorder.record_event(event_time, equity, candles)

        # 7. 액션 처리. 리스트 안의 순서를 지켜야 한다 — 예를 들어 트레일링 스탑 전략은
        #    [취소, 재등록] 순으로 내놓으므로 뒤집히면 client_id가 충돌한다.
        for action in actions:
            dispatch = self.executor.submit(action, event_time)
            if dispatch is not None:
                yield dispatch

        # 8. 이벤트 마무리 (라이브 레코더의 flush 등)
        self.recorder.end_event(event_time)

    def warmup(self, candles_by_symbol: Dict[str, List[Candle]]) -> None:
        """지표에만 과거 캔들을 먹인다 — 주문도 기록도 일어나지 않는다.

        ``status``를 넘기지 않는 것이 규약이다 (라이브 프리피드와 같다): 이 구간에는 대응하는
        계좌 상태가 없으므로, status를 읽는 지표는 ``None``을 워밍업으로 다뤄야 한다.
        """
        for symbol, candles in candles_by_symbol.items():
            indicators = self.streamer.indicators.get(symbol, {})
            for candle in candles:
                for indicator in indicators.values():
                    indicator.update(candle)

    def run(self, producer: CandleProducer) -> None:
        """동기 소스를 끝까지 흘려보낸다.

        ``SimulatedExecutor``는 Dispatch를 만들지 않으므로 제너레이터는 아무것도 yield하지
        않는다. ``deque(..., maxlen=0)``은 그것을 C 레벨에서 소진하는 관용구다.
        """
        for event_time, candles in producer:
            deque(self.process_event(event_time, candles), maxlen=0)

    async def run_async(self, producer, timeout: float = 10.0) -> None:
        """비동기 소스를 끝까지 흘려보내고, 주문마다 거래소의 확인을 기다린다."""
        async for event_time, candles in producer:
            for dispatch in self.process_event(event_time, candles):
                await dispatch.wait(timeout)
