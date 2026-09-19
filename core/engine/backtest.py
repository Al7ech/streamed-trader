"""백테스트 전용 엔진 — 캔들을 메모리에 얹고, 벡터화 가능한 지표를 먼저 계산한 뒤 루프를 돈다.

라이브 경로(:class:`~core.engine.engine.TradingEngine`)와 **순서 규약은 같지만 코드는 따로**다.
라이브 엔진이 갖는 것들 — 연속성 앵커, 구멍 백필, 결정 기한, 지표 워밍업, ``on_error``,
async 순회 — 은 전부 "스트림은 끊기고 시계는 흐른다"는 전제 위에 있고, 메모리에 다 들고 있는
과거 구간에는 그 전제가 없다. 그 전제를 걷어내면 이벤트마다 ``Event`` 객체도, async generator
프로토콜도, 심볼별 앵커 조회도 필요 없고, 무엇보다 **지표를 미리 계산해 둘 수 있다**.

그게 이 엔진의 존재 이유다. ``VectorizableNumericIndicator``(``core.streamer.indicator.
base_indicator``)를 상속한 지표는 그 심볼의 OHLCV 배열로 전 구간을 한 번에 계산해 두고,
루프에서는 값을 지표의 출력 자리에 하나씩 얹기만 한다. 그렇지 않은 지표(경로 의존적이거나
``status``를 읽는 것)는 그대로 캔들마다 갱신되므로 둘을 섞어 써도 된다. 미리 계산된 값은
루프가 냈을 값과 **비트 단위로 같다** — 그 계약과 그것을 지키는 검사는
:mod:`core.streamer.indicator.vector_ops` 참고.

**두 벌이 된 순서 규약이 갈라지지 않는다는 보장은 문서가 아니라 검사다.**
``core/checks/live_check.py`` 3절이 이 엔진과 라이브 엔진의 드라이런이 같은 캔들에서 **같은
체결**을 내는지 다섯 전략으로 확인한다 (시장가 / 상태 있는 전략 / 조건부 주문 / 지정가+취소 /
reduce_only+슬리피지). 아래 :meth:`BacktestEngine._loop`의 단계 순서를 바꾸면 그 검사가 깨진다.
"""

import logging
import sys
from typing import Callable, Dict, Iterator, List, Optional, Tuple

import numpy as np
from tqdm import tqdm

from core.account.report import Report
from core.candle.candle import Candle
from core.candle.merge import merge_by_end_time
from core.executor.base import Executor
from core.recorder.base import NullRecorder, Recorder
from core.streamer import BaseStreamer
from core.streamer.indicator.base_indicator import BaseIndicator, VectorizableIndicator

#: 미리 계산한 배열을 파이썬 float으로 바꿀 때 한 번에 처리할 개수. :func:`_iter_floats` 참고.
_TOLIST_CHUNK = 1 << 16

#: 심볼 하나의 재생 계획 — (미리 계산된 (sink, 값 이터레이터) 목록, 루프로 갱신할 지표 목록)
Playback = Tuple[List[Tuple[Callable[[Optional[float]], None], Iterator[float]]],
                 List[BaseIndicator]]


class BacktestEngine:
    """메모리에 올린 캔들로 백테스트 한 번을 돌린다.

    :param streamer: 전략. ``symbols``와 ``indicators``를 여기서 읽는다.
    :param candles_by_symbol: 심볼별 캔들 리스트 (각자 ``end_time`` 오름차순). 심볼마다 길이가
        달라도 된다 — 상장일이 다르거나 구간에 구멍이 있는 시계열은 병합이 그대로 처리한다.
        스트리머가 다루지 않는 심볼이 섞여 있어도 된다 (지표가 없을 뿐 실행기·레코더는 본다).
    :param recorder: None이면 :class:`~core.recorder.base.NullRecorder`.
    :param vectorize: False면 선계산을 통째로 끄고 전 지표를 ``update()``로 돌린다. 결과는
        같아야 하므로(위 모듈 docstring) 정확성을 위한 스위치가 아니라, 새로 쓴
        ``VectorizableIndicator.compute()``를 의심할 때 쓰는 대조 수단이다.
    :param progress: 이벤트 진행바 표시 여부.

    **생성자가 ``executor.on_trade``를 레코더로 덮어쓴다** — :class:`TradingEngine`과 같은
    배선이다. 부품을 만드는 것은 호출자, 엮고 돌리는 것은 엔진이다.

    이 엔진에 **없는 것**: ``producer``, ``history``, ``on_error``, ``decide_deadline_ms``,
    ``warmup()``, ``run_async()``, ``interval_ms``. 앞의 다섯은 라이브의 사정이고,
    ``interval_ms``는 레코더가 호출자에게서 직접 받는다 (캔들에서 재는 것보다 낫다 — 캔들이
    0~1개인 심볼에서도 맞다).
    """

    def __init__(self, streamer: BaseStreamer, candles_by_symbol: Dict[str, List[Candle]],
                 executor: Executor, recorder: Optional[Recorder] = None, *,
                 vectorize: bool = True, progress: bool = True):
        self.streamer = streamer
        self.candles_by_symbol = candles_by_symbol
        self.executor = executor
        self.recorder = recorder if recorder is not None else NullRecorder()
        self.vectorize = vectorize
        self.progress = progress
        self.logger = logging.getLogger(__name__)

        self.executor.on_trade = self.recorder.record_trade

    def run(self) -> Optional[Report]:
        """선계산 → 이벤트 루프 → 마무리. 완전히 동기다 (이벤트 루프를 열지 않는다).

        ``recorder.close()``를 ``finally``가 아니라 루프 **뒤**에 두는 것은 의도적이다:
        실행이 예외로 끝났다면 반쪽짜리 산출물을 남기지 않는다 (라이브 엔진과 같은 규약).

        :return: 레코더가 결과를 들고 있으면 그것 (``BacktestRecorder``의 ``Report``), 아니면 None.
        """
        self._check_one_interval()
        playback = self._precompute()
        self._loop(playback)
        self.recorder.close()
        return self.recorder.report

    # ------------------------------------------------------------------ 선계산

    def _precompute(self) -> Dict[str, Playback]:
        """심볼마다 "미리 계산된 것"과 "루프로 돌릴 것"을 갈라 미리 묶어 둔다.

        벡터화 대상 판정은 ``isinstance(indicator, VectorizableIndicator)``다 — "전 구간을
        미리 계산할 수 있는가"를 저장 방식과 무관하게 직접 묻는다. ``compute``는
        ``VectorizableIndicator``의 abstractmethod라, 정의를 빼먹은 지표는 클래스 정의
        시점에 인스턴스화 자체가 실패한다 — 여기서 조용히 루프로 폴백하는 실패 모드는 없다.
        ``sink()``는 이 판정을 통과한 지표라면 (실무에서는 전부 ``VectorizableIndicator``와
        ``NumericIndicator``를 함께 상속하는 ``VectorizableNumericIndicator``를 거치므로)
        항상 있다고 가정하고 바로 부른다.
        """
        playback: Dict[str, Playback] = {}
        for symbol, indicators in self.streamer.indicators.items():
            candles = self.candles_by_symbol.get(symbol) or []
            arrays = self._ohlcv(candles) if (self.vectorize and candles) else None

            sinks: List[Tuple[Callable[[Optional[float]], None], Iterator[float]]] = []
            loop: List[BaseIndicator] = []
            for name, indicator in indicators.items():
                if arrays is not None and isinstance(indicator, VectorizableIndicator):
                    series = self._precompute_one(symbol, name, indicator.compute, arrays,
                                                  len(candles))
                    sinks.append((indicator.sink(), _iter_floats(series)))
                else:
                    loop.append(indicator)
            playback[symbol] = (sinks, loop)

            # 심볼당 한 줄. VectorizableNumericIndicator를 빼먹어 조용히 루프로 도는 지표가
            # 있어도 10초를 먹는 일이 없게, 무엇이 미리 계산됐고 무엇이 루프로 도는지 런
            # 로그에 남긴다.
            self.logger.info("[%s] 캔들 %d개 — 미리 계산한 지표 %d개, 루프로 도는 지표 %d개",
                             symbol, len(candles), len(sinks), len(loop))
        return playback

    def _precompute_one(self, symbol: str, name: str, compute, arrays, n: int) -> np.ndarray:
        """지표 하나의 전 구간을 계산하고 재생 가능한 모양인지 확인한다."""
        series = compute(*arrays)
        # 길이가 어긋나면 커서가 조용히 밀려 전략이 다른 봉의 값을 보게 된다 — 결과가 틀린
        # 채로 끝까지 도는 것보다 여기서 죽는 편이 낫다.
        if len(series) != n:
            raise ValueError(
                f"{symbol}.{name}.compute()가 캔들 수와 다른 길이를 돌려줬다: "
                f"{len(series)} != {n}")
        # float64가 아니면 지표 deque와 샤드 JSON까지 다른 타입이 흘러간다.
        if series.dtype != np.float64:
            raise ValueError(
                f"{symbol}.{name}.compute()의 dtype이 float64가 아니다: {series.dtype}")
        return series

    @staticmethod
    def _ohlcv(candles: List[Candle]) -> Tuple[np.ndarray, ...]:
        """``VectorizableIndicator.compute()``에 넘길 (open, high, low, close, volume) 배열."""
        n = len(candles)
        return tuple(np.fromiter((getattr(c, k) for c in candles), np.float64, n)
                     for k in ("open", "high", "low", "close", "volume"))

    def _check_one_interval(self) -> None:
        """심볼별 캔들 간격이 서로 다르면 여기서 죽는다.

        병합 타임라인은 "같은 시각 = 같은 봉 경계"를 전제하므로, 심볼마다 간격이 다르면 병합
        자체가 의미를 잃는다. 혼합 간격 멀티심볼 백테스트는 지원 범위 밖이다. (간격 **값**은
        레코더가 호출자에게서 직접 받는다 — 여기서는 일관성만 본다.)
        """
        ref_ms: Optional[int] = None
        ref_symbol: Optional[str] = None
        for symbol, candles in self.candles_by_symbol.items():
            if len(candles) < 2:
                continue
            ms = candles[1].start_time - candles[0].start_time
            if ref_ms is None:
                ref_ms, ref_symbol = ms, symbol
            elif ms != ref_ms:
                raise ValueError(
                    f"심볼별 캔들 간격이 다르다: {ref_symbol}={ref_ms}ms vs {symbol}={ms}ms — "
                    f"멀티심볼 백테스트는 모든 심볼이 같은 간격이어야 한다.")

    # ------------------------------------------------------------------ 루프

    def _loop(self, playback: Dict[str, Playback]) -> None:
        """병합 이벤트를 시간순으로 처리한다. 단계 번호는 ``TradingEngine.process_event``와 같다.

        그쪽과 의도적으로 다른 점:

        - ``decide``/``late`` 파라미터가 없다. 백테스트는 늦을 수도, 이미 지나간 봉을 재생할
          수도 없다 — 둘 다 라이브에서 스트림이 끊겼을 때의 개념이다.
        - 이벤트당 DEBUG 줄이 없다. ``isEnabledFor`` 한 번이 345만 번이고, 백테스트에서 그
          로그를 읽는 사람은 없다.
        - 지표 순회가 ``streamer.symbols``가 아니라 병합 배치 순서다. 지표는 서로를 읽지 않고
          ``status``는 어느 쪽이든 같은 거래 전 스냅샷이라 결과가 같다.

        심볼별 커서가 따로 없는 것에 주의: ``merge_by_end_time``이 각 심볼의 캔들을 자기 리스트
        순서대로 정확히 한 번씩 방문하므로, 그 심볼의 값 이터레이터를 한 번 ``next()``하는 것이
        곧 커서 전진이다.
        """
        st = self.executor.status
        events = merge_by_end_time(self.candles_by_symbol)
        if self.progress:
            events = tqdm(events, desc="Backtesting", unit="event", file=sys.stdout)

        for event_time, batch in events:
            candles = dict(batch)

            # 0-2. 이벤트 경계 훅: 심볼별 최근 종가 갱신 → 미체결 주문 매칭(체결이 여기서
            #      일어난다) → 열린 포지션 시가평가 → 자본이 0 이하면 강제청산. 그 결과가
            #      단계 6에서 레코더가 읽는 자본곡선 값이다. 미체결 매칭이 decide_action
            #      **앞에** 있는 것이 핵심이다 — 실제 거래소에서는 미체결 주문이 봉이 닫히기
            #      전에 체결되므로, 뒤에 두면 전략이 이미 손절된 포지션을 들고 있다고 착각한다.
            self.executor.begin_event(event_time, candles)

            # 4. 이 이벤트의 **모든** 심볼 지표를 먼저 갱신한 뒤, decide_action을 이벤트당 한
            #    번 부른다 — 결정 시점에 모든 심볼 지표가 이 캔들까지 반영돼 있어야 크로스심볼
            #    결정이 가능하다. 미리 계산된 값은 update() 대신 지표의 출력 자리에 얹는다.
            for symbol, candle in batch:
                bound = playback.get(symbol)
                if bound is None:
                    continue  # 스트리머가 다루지 않는 심볼
                sinks, loop = bound
                for sink, values in sinks:
                    sink(next(values))
                for indicator in loop:
                    indicator.update(candle, st)

            actions = self.streamer.decide_action(candles, st)

            # 6. 기록. 모든 지표가 갱신되고 decide_action이 반환한 직후라, 레코더가 읽는 모든
            #    값이 그 결정이 실제로 본 값이다. 자본곡선 값은 레코더가 이 시점의
            #    status.total_margin()을 직접 읽는다 — 미체결 체결은 반영, 아래 시장가 체결은
            #    아직 미반영인 시점이다.
            self.recorder.record_event(event_time, candles)

            # 7. 액션 처리. SimulatedExecutor는 동기라 리스트 순서대로 반영된다 —
            #    트레일링 스탑의 [취소, 재등록]도 그 순서다.
            for action in actions:
                self.executor.submit(action, event_time)

            # 8. 이벤트 마무리
            self.recorder.end_event(event_time)


def _iter_floats(series: np.ndarray, chunk: int = _TOLIST_CHUNK) -> Iterator[float]:
    """미리 계산된 배열을 파이썬 float으로 하나씩 내준다 — 블록 단위 ``tolist``다.

    캔들 하나에 값 하나다. 워밍업 NaN도 그대로 내준다: 루프 경로는 그 구간에 아무것도 얹지
    않거나(``MovingAverage``) ``None``을 얹지만(돈치안), 어느 쪽이든 ``read``는 같은 답을
    준다 — 두 deque가 **끝에서 정렬**돼 있고, NaN도 짧은 deque의 빈자리도 똑같이 ``None``으로
    읽히기 때문이다. 보관 이력 경계(``IndexError``)도 같은 자리에서 걸린다.

    블록으로 끊는 이유는 두 가지다.

    **타입**: ndarray를 그냥 순회하면 ``np.float64``가 지표 deque와 ``Trade.price``, 샤드
    JSON까지 새어 나간다. ``tolist()``는 순수 파이썬 float을 준다.

    **메모리**: 통째로 ``tolist()``하면 1m 6.5년 런에서 지표 하나당 ~110MB(원소당 32B)가 런
    내내 살아 있다 (지표 7개면 ~770MB, 이미 2.9GB인 RSS 위에). 블록으로 끊으면 살아 있는
    파이썬 float은 지표당 6.5만 개(~2MB)뿐이고, 속도는 통째 ``tolist``와 같다.
    """
    for start in range(0, len(series), chunk):
        yield from series[start:start + chunk].tolist()
