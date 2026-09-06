"""매매 순서 규약. 이 저장소에 **한 벌만** 존재하는 이벤트 처리 루프다.

백테스트, 드라이런, 라이브가 전부 :meth:`TradingEngine.process_event`를 지나간다. 셋의 차이는
꽂히는 부품뿐이다:

===========  ==============================  ==================  ================
             CandleProducer                  Executor            Recorder
===========  ==============================  ==================  ================
백테스트     BinanceBacktestCandleProducer   SimulatedExecutor   BacktestRecorder
             / InMemoryCandleProducer
드라이런     LiveCandleProducer              SimulatedExecutor   LiveRecorder
라이브       LiveCandleProducer              LiveExecutor        LiveRecorder
===========  ==============================  ==================  ================

**엔진은 완전히 동기다.** ``process_event``는 평범한 메서드고, 액션을 실행기에 넘기면 그걸로
끝이다 — 결과를 기다리지 않는다. 세 모드 모두 유일한 드라이버 :meth:`run_async`를 지나가고
(producer를 ``async for``로 순회한다), 백테스트 경로는 그 코루틴을 ``asyncio.run``으로
감싼다 (:func:`~core.backtest.run.run_backtest`는 여전히 동기 함수다). ``run_async``만이
``process_event`` 안에서 터진 예외를 ``on_error``로 넘긴다.

라이브에서 주문 제출은 :class:`~core.live.executor.LiveExecutor` 안에서 fire-and-forget이다.
예전의 "주문 N의 결과를 확인한 뒤 N+1을 보낸다"는 보장은 **의도적으로 없앴다** — 라이브 ``status``
는 거래소가 정답이라(유저 데이터 스트림이 이벤트 밖에서 갱신한다) 누락된 주문은 다음 캔들에 스스로
복구된다. 그 대신 한 이벤트 안 액션들의 실행 순서도 라이브에서는 보장되지 않는다 (백테스트/드라이런
은 ``SimulatedExecutor``가 동기라 리스트 순서를 지킨다).
"""

import logging
from typing import Awaitable, Callable, Dict, List, Optional

from core.domain.action import Action
from core.domain.candle import Candle
from core.engine.candle_producer import CandleProducer
from core.engine.executor import Executor
from core.engine.recorder import Recorder
from core.streamer import BaseStreamer
from core.utils import generate_dict_string


class TradingEngine:
    def __init__(self, streamer: BaseStreamer, executor: Executor, recorder: Recorder):
        self.streamer = streamer
        self.executor = executor
        self.recorder = recorder
        self.logger = logging.getLogger(__name__)

    def process_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        """이벤트 하나를 처리한다.

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

        # 7. 액션 처리. 백테스트/드라이런은 SimulatedExecutor가 동기라 리스트 순서대로
        #    체결/등록/취소가 반영된다 — 트레일링 스탑의 [취소, 재등록]도 그 순서로 처리된다.
        #    라이브는 LiveExecutor.submit이 주문을 스레드풀로 fire-and-forget하므로 이벤트 안
        #    액션들의 실행 순서가 보장되지 않는다 (문서화된 라이브 한정 동작 차이).
        for action in actions:
            self.executor.submit(action, event_time)

        # 8. 이벤트 마무리 (라이브 레코더의 flush 등)
        self.recorder.end_event(event_time)

    def warmup_windows(self) -> Dict[str, int]:
        """심볼별로 워밍업에 필요한 캔들 수 = 그 심볼 지표들의 최대 window."""
        return {symbol: max((ind.window for ind in indicators.values()), default=0)
                for symbol, indicators in
                ((s, self.streamer.indicators.get(s, {})) for s in self.streamer.symbols)}

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
            if indicators:
                self.logger.info("[%s] Pre-fed indicators: %s", symbol,
                                 generate_dict_string(indicators))

    async def run_async(self, producer: CandleProducer,
                        on_error: Optional[Callable[[Exception], Awaitable[None]]] = None
                        ) -> None:
        """producer를 끝까지 흘려보낸다 — 백테스트/드라이런/라이브의 **유일한 드라이버**다.

        백테스트/벡터화 경로는 동기 함수 안에서 이걸 ``asyncio.run``으로 감싸 부른다.

        :param on_error: 주면 한 이벤트의 처리 실패(전략/지표 버그 등)가 루프를 끝내지 않고
            여기로 넘어간다 — 몇 주씩 사는 프로세스가 일시적 버그 하나로 죽으면 안 되기
            때문이다. None이면 그대로 올라간다 (테스트/백필 재생에서 쓴다).

        주문 제출 결과는 여기서 기다리지 않는다. 라이브에서 주문 실패는 실행기가 비동기로
        자기 에러 싱크에 흘려보내고, 거래소 진실인 ``status``가 다음 캔들에 스스로 복구한다.
        """
        async for event_time, candles in producer:
            try:
                self.process_event(event_time, candles)
            except Exception as e:
                if on_error is None:
                    raise
                # exception()으로 스택트레이스까지 남긴다 — 한 줄만으로는 핸들러 안 어느
                # 줄에서 터졌는지 알 수 없다.
                self.logger.exception("이벤트 처리 실패 — 다음 캔들로 넘어간다: %s", e)
                await on_error(e)
