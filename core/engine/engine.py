"""매매 순서 규약. 이 저장소에 **한 벌만** 존재하는 이벤트 처리 루프다.

백테스트, 드라이런, 라이브가 전부 :meth:`TradingEngine.process_event`를 지나간다. 셋의 차이는
꽂히는 부품뿐이다:

===========  ==============================  ==================  ================
             CandleProducer                  Executor            Recorder
===========  ==============================  ==================  ================
백테스트     BinanceHistoricalCandleProducer SimulatedExecutor   BacktestRecorder
             / InMemoryCandleProducer
드라이런     LiveCandleProducer              SimulatedExecutor   LiveRecorder
라이브       LiveCandleProducer              LiveExecutor        LiveRecorder
===========  ==============================  ==================  ================

**부품을 엮는 것도 엔진의 일이다.** 호출자는 네 부품(스트리머·공급자·실행기·레코더)을
생성자에 넘기기만 하고, 체결 싱크 연결(``executor.on_trade``), 레코더 기본값, 루프,
마무리(``recorder.close()``)는 엔진이 한다. 지표 워밍업(:meth:`TradingEngine.warmup`)도
엔진이 갖지만 루프 밖의 별개 단계다 — 캔들 소스가 본 공급자와 다르고, 라이브는 소켓을 열기
전에 끝내야 하기 때문이다. 예전에는 이 배선이 진입점마다 손으로
반복됐다. 실행기와 레코더의 **구현체**는 각 포트 패키지(:mod:`core.executor`,
:mod:`core.recorder`)에 있고 엔진은 각 포트의 ``base`` ABC만 import한다 — 만들어 넘기는 것은
호출자다.

**엔진은 완전히 동기다.** ``process_event``는 평범한 메서드고, 액션을 실행기에 넘기면 그걸로
끝이다 — 결과를 기다리지 않는다. 세 모드 모두 유일한 드라이버 :meth:`run_async`를 지나가고
(producer를 ``async for``로 순회한다), 이벤트 루프가 없는 호출자를 위해 그것을 ``asyncio.run``
으로 감싼 :meth:`run`이 따로 있다 (백테스트가 쓴다). ``run_async``만이 ``process_event`` 안에서
터진 예외를 ``on_error``로 넘긴다.

라이브에서 주문 제출은 :class:`~core.executor.live.LiveExecutor` 안에서 fire-and-forget이다.
예전의 "주문 N의 결과를 확인한 뒤 N+1을 보낸다"는 보장은 **의도적으로 없앴다** — 라이브 ``status``
는 거래소가 정답이라(유저 데이터 스트림이 이벤트 밖에서 갱신한다) 누락된 주문은 다음 캔들에 스스로
복구된다. 그 대신 한 이벤트 안 액션들의 실행 순서도 라이브에서는 보장되지 않는다 (백테스트/드라이런
은 ``SimulatedExecutor``가 동기라 리스트 순서를 지킨다).
"""

import asyncio
import copy
import logging
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, NoReturn, Optional, Tuple

from core.account.report import Report
from core.candle.candle import Candle
from core.history.base import CandleHistory
from core.producer.base import CandleProducer, Event
from core.executor.base import Executor
from core.order.action import Action, ActionType
from core.recorder.base import NullRecorder, Recorder
from core.streamer import BaseStreamer
from core.utils import generate_dict_string, ms_timestamp_to_datetime


class TradingEngine:
    """네 부품을 엮어 매매 루프를 돌린다.

    :param producer: 캔들 공급자. 지표 워밍업용 과거 캔들은 여기서 나오지 않는다 —
        그건 아래 ``history``에서 온다.
    :param recorder: None이면 :class:`~core.recorder.base.NullRecorder` — 기록이 꺼진
        라이브 실행처럼 엔진은 돌려야 하지만 적재할 필요가 없을 때다.
    :param on_error: 주면 한 이벤트의 처리 실패가 루프를 끝내지 않고 여기로 넘어간다.
        None이면 그대로 올라간다 (:meth:`run_async` 참고).
    :param history: 과거 캔들 조회 (:class:`~core.history.base.CandleHistory`).
        **워밍업과 구멍 백필이 함께 쓴다** — 둘 다 "지금이 아닌 과거 구간을 받아온다"는 한
        가지 일이고, 구간이 런타임에 정해진다는 것도 같다(기동 시각/구멍이 난 시각). 본
        공급자(``producer``)는 구간이 생성 시점에 굳은 스트림이라 그것을 표현할 수 없어서
        포트가 따로 있다. None이면 워밍업을 부를 수 없고, 스트림의 점프도 채울 수 있는 구멍으로
        취급하지 않는다 (백테스트의 ragged 시계열은 그대로 통과).
    :param max_backfill_candles: 한 번에 백필할 캔들 수 상한. 넘으면 스트림을 치명적으로
        끊는다 — 프로세스가 오래 죽어 있었다는 뜻이고, 재기동이 지표를 프리피드로 다시
        세우고 포지션은 거래소에서 다시 읽는다.
    :param decide_deadline_ms: 결정 기한. 봉 경계(``event.time``)에서 이만큼 넘게 지나서야
        처리하게 된 봉은 **늦은 봉**으로 보고, 결정은 받되 포지션을 늘리는 시장가를 버린다
        (:meth:`process_event`의 ``late``). 시장가는 결정 봉의 종가에 체결된다고 가정하는데,
        늦게 낸 주문은 그 가정 밖의 가격에 체결되기 때문이다. None이면 끈다 — 백테스트는
        늦을 수가 없고, 드라이런은 늦어도 종가로 체결하므로 켜면 백테스트와 갈라지기만 한다.
        기한은 소비자가 봉을 **처리하는 시점**에 잰다: 네트워크 지연뿐 아니라 다른 심볼의
        구멍 백필이 소비자를 붙잡은 동안 큐에서 기다린 시간까지 들어가야 하기 때문이다.
    :param clock: 결정 기한을 잴 "지금"(ms epoch)을 돌려주는 함수. 봉 경계가 거래소 시각이라
        이것도 거래소 기준이어야 한다 — 로컬 시계가 몇 초만 어긋나도 기한 판정이 통째로
        틀린다. None이면 로컬 시계. 기한이 None이면 부르지 않는다.

    **생성자가 ``executor.on_trade``를 레코더로 덮어쓴다.** 체결 싱크를 잇는 이 한 줄은
    예전에 백테스트/드라이런/라이브 진입점 세 곳에 각각 있었다 — 배선을 한 곳으로 모으는 것이
    이 클래스가 조립까지 맡는 이유다. 엔진을 거치지 않고 실행기만 단독으로 쓰는 경로
    (검사 스크립트 등)는 여전히 실행기 생성자의 ``on_trade``를 그대로 쓴다.
    """

    def __init__(self, streamer: BaseStreamer, producer: CandleProducer, executor: Executor,
                 recorder: Optional[Recorder] = None, *,
                 on_error: Optional[Callable[[Exception], Awaitable[None]]] = None,
                 history: Optional[CandleHistory] = None,
                 max_backfill_candles: int = 60,
                 decide_deadline_ms: Optional[int] = None,
                 clock: Optional[Callable[[], int]] = None):
        self.streamer = streamer
        self.producer = producer
        self.executor = executor
        self.recorder = recorder if recorder is not None else NullRecorder()
        self.on_error = on_error
        self.history = history
        self.max_backfill_candles = max_backfill_candles
        self.decide_deadline_ms = decide_deadline_ms
        self._clock = clock if clock is not None else (lambda: round(time.time() * 1000))
        self.logger = logging.getLogger(__name__)

        #: 심볼별로 마지막으로 지표에 먹인 캔들의 ``start_time`` — 연속성 앵커.
        #: :meth:`warmup`이 워밍업 지점으로 채우고, :meth:`run_async`가 실시간 캔들마다
        #: 갱신하며, 그 사이 앞으로 건너뛴 구간을 :meth:`_backfill_gap`이 메운다.
        self._last_start: Dict[str, int] = {}

        self.executor.on_trade = self.recorder.record_trade

    def process_event(self, event: Event, *, decide: bool = True, late: bool = False) -> None:
        """이벤트 하나를 처리한다.

        단계 순서가 이 클래스의 존재 이유다. 특히 미체결 주문 매칭이 ``decide_action`` **앞에**
        있는 것: 실제 거래소에서는 미체결 주문이 봉이 닫히기 전에 체결되므로, 뒤에 두면 전략이
        "이미 손절된 포지션을 아직 들고 있다"고 착각한 채 결정하게 된다. 그 매칭은
        ``begin_event`` 안에서 일어난다 (백테스트/드라이런은 :class:`SimulatedExecutor`가
        캔들로 판정, 라이브는 거래소 몫이라 no-op).

        **``decide``가 False면 4b·7단계를 건너뛴다.** 이미 지나간 봉(:meth:`_backfill_gap`이
        메우는 구멍 캔들)으로 새 주문이 나가면 안 되기 때문이다 — 그 캔들은 지표와 실행기의
        이벤트 경계 훅까지만 도달하고 ``streamer``의 결정에는 아예 닿지 않는다. 무엇이 지나간
        봉인지는 :meth:`run_async`가 심볼별 연속성 앵커(``_last_start``)로 직접 판정해서 이
        파라미터로 넘긴다 — ``Event`` 자체는 그 구분을 모른다.

        **``late``가 True면 결정은 받되 포지션을 늘리는 시장가를 버린다.** 결정 기한
        (``decide_deadline_ms``)을 넘겨 처리하게 된 봉이다. 시장가는 결정 봉의 종가에 체결된다는
        가정 위에 있는데, 늦게 낸 주문은 그 뒤의 가격에 체결된다. 그렇다고 결정을 통째로 버리면
        청산 신호까지 사라진다 — 다음 봉에서 조건이 풀리면 그 청산은 영영 나가지 않는다. 그래서
        포지션을 줄이는 시장가는 늦어도 내고(반전은 청산분까지만), 지정가·조건부·취소는 체결가가
        종가와 무관하므로 그대로 둔다 (:meth:`_drop_exposure_increase`).

        **엔진은 파산을 모르고, 파산해도 루프는 안 멈춘다.** 시가평가 자본이 0 이하로
        떨어졌을 때의 강제청산은 ``begin_event`` 안(백테스트/드라이런 한정)에서 끝난다 —
        파산은 실행기 내부 상태일 뿐 런을 끝내는 사건이 아니다. 시장은 계속 움직이고 지표도
        계속 갱신되며 레코더도 그릴 자본곡선이 있다(0에 평평). ``decide_action``은 이후에도
        매 이벤트 불리지만 flat·소진된 계좌를 받아 낸 액션을 실행기가 버린다. 루프는 프로듀서의
        데이터가 끝날 때 끝난다.
        """
        st = self.executor.status
        candles = event.candles

        # 0-2. 이벤트 경계 훅. 백테스트/드라이런은 실행기가 심볼별 최근 종가를 이 이벤트
        #      캔들로 갱신하고, **미체결 주문을 이 캔들로 체결시키고**, 열린 포지션을 그 종가로
        #      시가평가하고, 자본이 0 이하면 장부를 비우고 전 포지션을 청산한다 — 그 결과가
        #      단계 6에서 레코더가 읽는 자본곡선 값이다. 다른 심볼을 겨냥한 MARKET 액션의
        #      체결가도 이 종가 캐시에서 정해진다. 라이브는 미체결 결정 폐기에만 쓴다
        #      (매칭·강제청산 모두 거래소 몫).
        #      백필 이벤트(decide=False)도 여기는 지나간다: 그 봉이 도는 동안 이미 장부에
        #      얹혀 있던 주문의 체결은 지나간 봉이 만든 새 결정이 아니라 그 사이 거래소에서
        #      실제로 벌어진 일이다.
        self.executor.begin_event(event.time, candles)

        # 4. 이 이벤트의 **모든** 심볼 지표를 먼저 갱신한 뒤, decide_action을 이벤트당 한 번
        #    부른다 — 결정 시점에 모든 심볼 지표가 이 캔들까지 반영돼 있어 크로스심볼 결정이
        #    가능하다. status는 decide_action이 보는 것과 같은 거래 전 스냅샷이다. 파산 후에도
        #    부르지만, 그때 나온 액션은 단계 7에서 실행기가 무시한다.
        for symbol in self.streamer.symbols:
            candle = candles.get(symbol)
            if candle is None:
                continue
            for indicator in self.streamer.indicators.get(symbol, {}).values():
                indicator.update(candle, st)

        actions = list(self.streamer.decide_action(candles, st)) if decide else []
        if late and actions:
            actions, dropped = self._drop_exposure_increase(actions, st)
            if dropped:
                self.logger.warning("늦은 봉이라 포지션을 늘리는 시장가를 버리거나 청산분으로 줄였다 "
                                    "(time=%s): %s -> 낸 것 %s",
                                    ms_timestamp_to_datetime(event.time), dropped, actions)

        # 이벤트당 DEBUG 한 줄. isEnabledFor로 감싸는 게 핵심이다 — generate_dict_string은
        # 전 지표를 순회하며 get_latest()를 부르는데, 인자로 넘기면 DEBUG가 꺼져 있어도
        # 매 이벤트 실행된 뒤 버려진다.
        if self.logger.isEnabledFor(logging.DEBUG):
            for symbol in candles:
                self.logger.debug(
                    "symbol=%s candle=%s decide=%s actions=%s status=%s indicators=%s",
                    symbol, candles[symbol], decide, actions, st,
                    generate_dict_string(self.streamer.indicators.get(symbol, {})))

        # 6. 기록. 모든 지표가 이미 갱신되고 decide_action이 반환한 직후라, 레코더가 읽는 모든
        #    값이 그 결정이 실제로 본 값이다. 자본곡선 값은 레코더가 이 시점의
        #    ``status.total_margin()``을 직접 읽는다 — 미체결 체결은 반영, 아래 단계 7의 시장가
        #    체결은 아직 미반영인 시점이다.
        self.recorder.record_event(event.time, candles)

        # 7. 액션 처리. 백테스트/드라이런은 SimulatedExecutor가 동기라 리스트 순서대로
        #    체결/등록/취소가 반영된다 — 트레일링 스탑의 [취소, 재등록]도 그 순서로 처리된다.
        #    라이브는 LiveExecutor.submit이 주문을 스레드풀로 fire-and-forget하므로 이벤트 안
        #    액션들의 실행 순서가 보장되지 않는다 (문서화된 라이브 한정 동작 차이).
        for action in actions:
            self.executor.submit(action, event.time)

        # 8. 이벤트 마무리 (라이브 레코더의 flush 등)
        self.recorder.end_event(event.time)

    async def warmup(self, end: Optional[datetime] = None) -> None:
        """``history`` 조회로 직전 구간을 받아 **지표에만** 먹이고, 연속성 앵커를 남긴다.

        구간을 엔진이 정하는 것이 요점이다 — 필요한 길이는 스트리머의
        :meth:`~core.streamer.base_streamer.BaseStreamer.warmup_windows`, 봉 크기는 본
        공급자의 ``interval_ms``에서 나오므로 둘 다 여기 있다. 그 구간을 실제로 받아오는
        일만 :class:`~core.history.base.CandleHistory`가 하고, 그건 구멍 백필이 쓰는
        바로 그 조회다.

        구간은 심볼별 window가 아니라 **전 심볼 공통 ``max(window)``** 다. 롤링 윈도우 지표에
        과거를 더 먹이는 것은 무해한 반면, 심볼마다 "지금"을 다시 재면 순차 fetch에 걸린
        시간만큼 뒤 심볼의 창이 앞 심볼과 어긋난다. ``end``는 **인터벌 경계**로 내린다 —
        분 단위로만 자르면 ``1h`` 런을 13:37에 기동할 때 구간이 :37에서 시작해 Binance가 주는
        정시 정렬 캔들과 어긋나고, 아래 개수 검증이 무조건 실패한다.

        이 캔들들은 :meth:`process_event`를 타지 않는다 — 주문도, 기록도, 시가평가도 없다.
        워밍업 구간은 이 프로세스가 돌고 있지 않던 과거라 대응하는 계좌 상태 자체가 없기
        때문이다. 같은 이유로 ``update``에 ``status``를 넘기지 않는다 (status를 읽는 지표는
        ``None``을 워밍업으로 다뤄야 한다). 그래서 이 단계는 백필과 소스는 같아도 경로가
        다르다 — 백필은 지나간 봉이어도 그 사이 계좌에서 실제로 벌어진 일이 있다.

        마지막으로 먹인 캔들의 ``start_time``을 심볼별로 ``_last_start``에 남긴다
        (``end_time``이 아니다 — 연속성 판정이 ``start_time + interval_ms`` 기준이다).
        :meth:`run_async`가 첫 실시간 캔들을 이 앵커와 대조한다: 워밍업이 먹은 캔들이 다시
        오면 **중복**(지표 이중 투입 + 결정 재실행 = 주문 이중 발행)이라 버리고, 워밍업과 첫
        실시간 캔들 사이가 벌어졌으면 **구멍**(모든 롤링 윈도우 지표가 백테스트와 영구히
        갈라진다)이라 :meth:`_backfill_gap`이 같은 ``history`` 조회로 메운다.

        라이브는 이걸 **소켓을 열기 전에** 부른다. 수십 초가 걸릴 수 있는데 그동안 유저 데이터
        스트림을 읽지 않으면 python-binance의 큐가 넘치기 때문이다. 그래서 :meth:`run_async`
        안이 아니라 호출자가 부르는 별개 단계다 (백테스트는 아예 안 부르고, 첫 window개
        이벤트로 지표가 데워진다).

        :param end: 워밍업 구간의 끝(배타). 인터벌 경계로 내려 쓴다. None이면 **로컬 시계**의
            지금이다. 라이브는 거래소 서버 시각을 넘긴다 — 로컬 시계가 거래소보다 앞서 있으면
            아직 진행 중인 봉이 구간에 들어와 마감봉처럼 지표에 먹히고, 그 봉의 진짜 마감
            메시지는 앵커에 막혀 중복으로 버려진다.
        :raise ValueError: 어떤 심볼이 자기 지표 window보다 적은 캔들을 받았을 때, 또는 받은
            캔들이 구간을 벗어나거나 중간에 구멍이 있을 때 (:meth:`_check_warmup_candles`).
            조용히 덜 데워진(혹은 구멍 난) 지표로 매매하는 것보다 기동에 실패하는 편이 낫다 —
            라이브에서는 상장 직후 심볼이나 데이터 구멍이 여기서 잡힌다. ``history`` 조회가
            아예 없을 때도 같다.
        """
        windows = self.streamer.warmup_windows()
        max_window = max(windows.values(), default=0)
        if max_window == 0:
            self.logger.info("워밍업할 지표가 없다 — 건너뛴다")
            return
        if self.history is None:
            raise ValueError("워밍업하려면 과거 캔들 조회(history)가 필요하다")

        interval = self.producer.interval_ms
        if interval <= 0:
            raise ValueError(f"워밍업 구간을 정할 수 없다: interval_ms={interval}")
        now_ms = (round(end.timestamp() * 1000) if end is not None
                  else round(datetime.now(tz=timezone.utc).timestamp() * 1000))
        end_ms = now_ms // interval * interval
        start_ms = end_ms - max_window * interval

        self.logger.info("Pre-feeding indicators with historical data: %s ~ %s (%d candles)",
                         ms_timestamp_to_datetime(start_ms), ms_timestamp_to_datetime(end_ms),
                         max_window)

        # 심볼별로 먹인다 — 지표는 자기 심볼 캔들만 보므로 이벤트로 묶어 시간순으로 섞을
        # 이유가 없다 (본 루프의 이벤트 병합은 크로스심볼 *결정*을 위한 것이고, 워밍업에는
        # 결정이 없다).
        fetched = await self.history.fetch(list(self.streamer.symbols),
                                           ms_timestamp_to_datetime(start_ms),
                                           ms_timestamp_to_datetime(end_ms))
        for symbol, candles in fetched.items():
            self._check_warmup_candles(symbol, candles, start_ms, end_ms)
        short = {s: f"{len(fetched.get(s, ()))}/{w}" for s, w in windows.items()
                 if len(fetched.get(s, ())) < w}
        if short:
            raise ValueError(f"워밍업 캔들이 모자란다 (심볼: 받은 개수/필요 window): {short}")

        last: Dict[str, int] = {}
        for symbol, candles in fetched.items():
            indicators = list(self.streamer.indicators.get(symbol, {}).values())
            for candle in candles:
                for indicator in indicators:
                    indicator.update(candle)
            if candles:
                last[symbol] = candles[-1].start_time

        for symbol, candles in fetched.items():
            self.logger.info("[%s] Pre-fed indicators with %d candles: %s", symbol, len(candles),
                             generate_dict_string(self.streamer.indicators.get(symbol, {})))
        self._last_start.update(last)

    def _check_warmup_candles(self, symbol: str, candles: List[Candle],
                              start_ms: int, end_ms: int) -> None:
        """워밍업 캔들이 ``[start_ms, end_ms)`` 안에서 빈틈없이 이어지는지 — 백필과 같은 기준.

        구간 밖 캔들은 조회 계약 위반이다. 특히 ``end_ms``를 넘는 봉은 아직 진행 중인 봉이라,
        먹이면 미마감 값이 지표에 들어가고 그 봉의 진짜 마감은 중복으로 버려진다. 앞쪽이 비는
        것(상장 직후)과 맨 끝 봉이 아직 안 온 것은 허용한다 — 충분한가는 개수 검사가, 끝의
        빈자리는 첫 실시간 캔들의 백필이 맡는다.
        """
        interval = self.producer.interval_ms
        expected: Optional[int] = None
        for c in candles:
            if c.start_time < start_ms or c.end_time > end_ms:
                raise ValueError(
                    f"워밍업 구간 밖 캔들이 왔다 (symbol={symbol}): {c} — 구간 "
                    f"{ms_timestamp_to_datetime(start_ms)} ~ {ms_timestamp_to_datetime(end_ms)}")
            if expected is not None and c.start_time != expected:
                raise ValueError(
                    f"워밍업 캔들이 이어지지 않는다 (symbol={symbol}): "
                    f"{ms_timestamp_to_datetime(expected)} 자리에 "
                    f"{ms_timestamp_to_datetime(c.start_time)}")
            expected = c.start_time + interval

    async def run_async(self) -> Optional[Report]:
        """공급자를 끝까지 흘려보낸다 — 백테스트/드라이런/라이브의 **유일한 드라이버**다.

        이벤트 루프 → ``recorder.close()`` 순서로, 한 번의 실행 전체를 여기가 갖는다.
        이벤트 루프가 없는 호출자는 동기 래퍼 :meth:`run`을 쓴다. 지표 워밍업은 여기 없다 —
        소스가 다른 별개의 단계라 :meth:`warmup`을 호출자가 먼저 부른다 (백테스트는
        아예 안 부르고 첫 window개 이벤트로 지표가 데워진다).

        생성자의 ``on_error``를 주면 한 이벤트의 처리 실패(전략/지표 버그 등)가 루프를 끝내지
        않는다 — 몇 주씩 사는 프로세스가 일시적 버그 하나로 죽으면 안 되기 때문이다. None이면
        그대로 올라간다 (검사/백필 재생에서 쓴다).

        **연속성 판정도 여기가 한다.** 각 이벤트의 캔들을 심볼별 앵커 ``_last_start``와
        대조해서:

        - ``start_time <= 앵커`` → **이미 처리한 캔들**. 통째로 버린다 (재연결 직후 재전송 등).
        - ``start_time``이 앵커보다 한 인터벌 넘게 앞 → **구멍**. :meth:`_backfill_gap`이 그
          구간용 공급자로 지표를 메운 뒤(``decide=False``) 이 이벤트를 정상 처리한다.
        - 그 외 → 연속. 앵커를 갱신하고 정상 처리한다.

        앵커는 **처리 결과와 무관하게** ``process_event``에 들어간 순간 소비된 것으로 본다.
        처리 중 예외가 나도 지표는 이미 그 봉을 먹었을 수 있으므로, 앵커가 남아 다음 캔들의
        백필이 같은 봉을 다시 먹이면 경로 의존 지표가 백테스트와 영구히 갈라진다.

        ``history`` 조회가 없는 런(백테스트)은 구멍 판정을 건너뛴다 — ragged 시계열의 빈
        구간은 유실이 아니라 데이터 그대로다.

        **결정 기한도 여기서 잰다** (:meth:`_is_late`). 백필을 마친 **뒤**, ``process_event``
        직전이다 — 백필 REST를 기다린 시간도 그 봉의 지연이다.

        ``recorder.close()``를 ``finally``가 아니라 루프 **뒤**에 두는 것은 의도적이다: 실행이
        예외로 끝났다면 반쪽짜리 산출물을 남기지 않는다.

        주문 제출 결과는 여기서 기다리지 않는다. 라이브에서 주문 실패는 실행기가 비동기로
        자기 에러 싱크에 흘려보내고, 거래소 진실인 ``status``가 다음 캔들에 스스로 복구한다.

        :return: 레코더가 결과를 들고 있으면 그것 (백테스트의 ``Report``), 아니면 None.
        """
        async for event in self.producer:
            try:
                for symbol, candle in list(event.candles.items()):
                    last = self._last_start.get(symbol)
                    if last is not None and candle.start_time <= last:
                        self.logger.warning(
                            "이미 처리한 캔들을 버린다 (symbol=%s): start=%s <= 마지막 %s",
                            symbol, ms_timestamp_to_datetime(candle.start_time),
                            ms_timestamp_to_datetime(last))
                        raise _SkipEvent
                    await self._backfill_gap(symbol, candle, last)
                # 앵커는 process_event **전에** 옮긴다. 뒤에 두면 처리 중 예외(on_error 경로)가
                # 앵커를 남겨, 이미 지표에 들어간 이 봉을 다음 캔들의 백필이 한 번 더 먹인다.
                for symbol, candle in event.candles.items():
                    self._last_start[symbol] = candle.start_time
                self.process_event(event, decide=True, late=self._is_late(event))
            except _SkipEvent:
                continue
            except _TerminalStream:
                break
            except Exception as e:
                if self.on_error is None:
                    raise
                # exception()으로 스택트레이스까지 남긴다 — 한 줄만으로는 핸들러 안 어느
                # 줄에서 터졌는지 알 수 없다.
                self.logger.exception("이벤트 처리 실패 — 다음 캔들로 넘어간다: %s", e)
                await self.on_error(e)
        self.recorder.close()
        return self.recorder.report

    async def _backfill_gap(self, symbol: str, candle: Candle, last: Optional[int]) -> None:
        """앵커 ``last``와 ``candle`` 사이에 빠진 캔들을 ``history`` 조회로 메운다.

        워밍업과 **같은 조회**다 (:meth:`warmup` 참고). 다른 것은 소스가 아니라 그 뒤다:
        워밍업은 지표에만 먹이지만, 여기 캔들들은 :meth:`process_event`\\ (``decide=False``)로
        흘려보낸다 — 지표 갱신·실행기의 이벤트 경계 훅·기록까지만 가고
        ``streamer.decide_action``에는 닿지 않는다
        (몇 분 전 봉을 보고 지금 가격에 시장가를 내면 안 된다). 그 사이 거래소에서 벌어진
        미체결 체결은 ``begin_event``에서 반영된다.

        복구 불가한 상황(봉 경계 불일치, 백필 상한 초과, fetch 실패, 개수/정렬 불일치)은
        :meth:`_fatal`로 스트림을 치명적으로 끊는다 — 재기동이 지표를 프리피드로 다시 세운다.
        """
        if last is None or self.history is None:
            return

        interval = self.producer.interval_ms
        elapsed = candle.start_time - last
        if elapsed == interval:
            return  # 연속 (같은/이전 봉은 run_async가 이미 걸렀다)

        if interval <= 0 or elapsed % interval != 0:
            self._fatal(
                f"캔들 경계가 인터벌과 맞지 않는다 (symbol={symbol}): "
                f"{elapsed}ms 는 {interval}ms 의 배수가 아니다 "
                f"({ms_timestamp_to_datetime(last)} -> "
                f"{ms_timestamp_to_datetime(candle.start_time)})")

        missing = elapsed // interval - 1
        if missing > self.max_backfill_candles:
            self._fatal(
                f"캔들 {missing}개가 비었다 (symbol={symbol}) — 백필 상한 "
                f"{self.max_backfill_candles}개를 넘어 재기동으로 복구한다")

        gap_start = last + interval
        gap_end = candle.start_time
        self.logger.warning(
            f"캔들 {missing}개가 비었다 (symbol={symbol}) — 백필해서 재생한다: "
            f"{ms_timestamp_to_datetime(gap_start)} ~ {ms_timestamp_to_datetime(gap_end)}")

        # fetch/공급자 오류는 치명적(지표가 이미 갈라졌다). 아래 process_event의 오류는
        # run_async의 on_error 경로로 올려보낸다 — 그건 일시적 버그일 수 있다.
        try:
            fetched = await self.history.fetch(
                [symbol], ms_timestamp_to_datetime(gap_start),
                ms_timestamp_to_datetime(gap_end))
        except Exception as e:
            self._fatal(f"빠진 캔들을 백필하지 못했다 (symbol={symbol}): {e}")
        candles = fetched.get(symbol, [])

        # 개수·정렬 검증 — 어긋난 캔들이 조용히 지표에 먹히는 일이 없게 (예전 _fetch_range 몫).
        expected = gap_start
        for c in candles:
            if c.start_time != expected:
                self._fatal(
                    f"백필 캔들의 시작 시각이 어긋난다 (symbol={symbol}): "
                    f"{ms_timestamp_to_datetime(c.start_time)} != "
                    f"{ms_timestamp_to_datetime(expected)}")
            expected += interval
        if len(candles) != missing:
            self._fatal(f"백필 캔들 개수가 맞지 않는다 (symbol={symbol}): "
                        f"{len(candles)} != {missing}")

        # 한 심볼짜리 이벤트로 감싸 본 루프와 같은 process_event를 태운다 — 백필은 정의상
        # 단일 심볼이라 병합할 것이 없다.
        for c in candles:
            self._last_start[symbol] = c.start_time  # run_async와 같은 이유로 처리 전에 옮긴다
            self.process_event(Event(c.end_time, {symbol: c}), decide=False)

    def _is_late(self, event: Event) -> bool:
        """이 봉을 결정 기한을 넘겨 처리하게 됐는가. 기한이 없으면 항상 False."""
        if self.decide_deadline_ms is None:
            return False
        delay = self._clock() - event.time
        if delay <= self.decide_deadline_ms:
            return False
        self.logger.warning("결정 기한을 넘긴 봉 (symbols=%s, 봉 마감 %s 뒤 %dms > 기한 %dms) "
                            "— 포지션을 늘리는 시장가는 내지 않는다",
                            list(event.candles), ms_timestamp_to_datetime(event.time), delay,
                            self.decide_deadline_ms)
        return True

    @staticmethod
    def _drop_exposure_increase(actions: List[Action], st) -> Tuple[List[Action], List[Action]]:
        """늦은 봉의 액션에서 포지션을 늘리는 시장가를 걷어낸다. ``(낼 것, 버리거나 줄인 원본)``.

        - 지정가·조건부·취소는 그대로 낸다. 체결가가 결정 봉의 종가와 무관하다.
        - 시장가는 심볼별 포지션(같은 이벤트의 앞선 액션까지 반영)과 부호를 비교한다. flat이거나
          같은 방향이면 버린다. 반대 방향이면 내되, 포지션을 넘어서는 반전분은 잘라 청산까지만
          낸다.

        포지션은 결정이 본 것과 같은 거래 전 ``status``에서 읽는다. 라이브에서는 아직 체결 통지가
        오지 않은 앞선 주문이 반영되지 않았을 수 있다 — 그 한계는 결정 자체와 같다.
        """
        kept: List[Action] = []
        dropped: List[Action] = []
        positions: Dict[str, float] = {}
        for action in actions:
            if action.order_type is not ActionType.MARKET or action.quantity == 0:
                kept.append(action)
                continue
            pos = positions.get(action.symbol)
            if pos is None:
                state = st.positions.get(action.symbol)
                pos = state.position if state is not None else 0.0
            if pos == 0 or (action.quantity > 0) == (pos > 0):
                dropped.append(action)
                continue
            if abs(action.quantity) > abs(pos):
                dropped.append(action)
                action = copy.copy(action)
                action.quantity = -pos
            kept.append(action)
            positions[action.symbol] = pos + action.quantity
        return kept, dropped

    def _fatal(self, reason: str) -> NoReturn:
        """스트림을 치명적으로 끊는다: 공급자에 사유를 남기고 정지 요청한 뒤 루프를 깬다."""
        self.logger.fatal(reason)
        self.producer.fatal_reason = reason
        self.producer.request_stop()
        raise _TerminalStream(reason)

    def run(self) -> Optional[Report]:
        """:meth:`run_async`의 동기 래퍼. 백테스트처럼 이벤트 루프가 없는 호출자용이다.

        **이미 도는 이벤트 루프 안에서 부르면 ``RuntimeError``다** — 그 경우는 ``run_async``를
        직접 await하면 된다.
        """
        return asyncio.run(self.run_async())


class _SkipEvent(Exception):
    """이 이벤트만 버리고 계속한다 (이미 처리한 캔들의 재수신)."""


class _TerminalStream(Exception):
    """스트림을 끝내야 하는 상황 (복구 불가한 구멍). :meth:`TradingEngine.run_async`가 잡아
    루프를 깬다."""
