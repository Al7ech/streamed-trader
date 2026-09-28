"""같은 캔들 위에서 전략 파라미터 격자를 fork 워커로 나눠 돌린다.

한 칸 = :class:`~core.engine.backtest.BacktestEngine` 한 번이다. 이 모듈은 루프를 갖지 않는다 —
부품(스트리머, ``SimulatedExecutor``, ``SimpleRecorder``)을 조립해 엔진에 넘기는 것까지가 일이다.
그래서 스윕 결과는 같은 파라미터로 엔진을 직접 돌린 결과와 **체결 단위로 같다** (검사:
``core/checks/backtest_check.py`` 6절).

**왜 fork이고 왜 열 캔들인가.** 1m 6.5년 캔들은 ``List[Candle]``로 심볼당 ~1.8GB이고, fork한
자식이 그것을 순회만 해도 refcount 갱신이 COW 페이지를 복사해 워커마다 한 벌씩 다시 든다.
:class:`~core.candle.columnar.ColumnarCandles`는 같은 데이터를 ~200MB의 numpy 버퍼로 들고,
버퍼는 읽어도 페이지가 복사되지 않는다. 부모가 캔들과 선계산 지표를 한 번 올린 뒤
``gc.freeze()``하고 fork하면 워커는 그것을 공유하고, 자기 몫(지표 deque, 자본곡선, 순회 중인
Candle 블록)만 따로 든다.

**선계산 캐시.** 격자의 칸들은 대개 지표 대부분을 공유한다 (max_loss 격자면 전부). 부모가
fork 전에 모든 칸의 스트리머를 한 번씩 만들어 보고, ``cache_key()``가 있는 지표를 (심볼, 키)별로
한 번씩만 계산해 둔다. 워커는 그 dict를 ``BacktestEngine(precomputed=...)``로 넘긴다.

**워커와 주고받는 것.** 팩토리·요약 함수·캔들·캐시는 fork 전에 모듈 전역에 걸어 두고, 워커에는
잡 인덱스만 보낸다 — 그래서 lambda/클로저 팩토리도 된다. 돌아오는 것은 요약 dict뿐이다.
``Report``를 돌려보내면 345만 튜플짜리 자본곡선이 pickle로 부모에 쌓인다.
"""

import asyncio
import gc
import logging
import multiprocessing as mp
import os
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Hashable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from core.account.report import Report
from core.candle.columnar import ColumnarCandles
from core.engine.backtest import BacktestEngine
from core.executor.simulated import SimulatedExecutor
from core.history.base import CandleHistory
from core.recorder.simple import SimpleRecorder
from core.result.metrics import summarise_report
from core.streamer import BaseStreamer
from core.streamer.indicator.base_indicator import VectorizableIndicator

logger = logging.getLogger(__name__)

#: 스트리머 팩토리: (심볼 목록, 파라미터) -> 스트리머. 칸마다 새 인스턴스를 만들어야 한다.
StreamerFactory = Callable[[List[str], Mapping[str, Any]], BaseStreamer]


@dataclass(frozen=True)
class SweepJob:
    """격자의 한 칸.

    :param symbols: 이 칸이 돌 심볼들. 문자열 하나를 넘기면 단일 심볼로 받는다. 엔진에는 이
        심볼들의 캔들만 넘어간다.
    :param params: 팩토리에 넘길 파라미터.
    :param tag: 호출자 몫의 꼬리표 (결과 매칭·로그용). 스윕은 읽지 않는다.
    """
    symbols: Tuple[str, ...]
    params: Mapping[str, Any] = field(default_factory=dict)
    tag: Any = None

    def __post_init__(self):
        if isinstance(self.symbols, str):
            object.__setattr__(self, "symbols", (self.symbols,))
        else:
            object.__setattr__(self, "symbols", tuple(self.symbols))


def load_columnar(history: CandleHistory, symbols: Sequence[str], start: datetime,
                  end: datetime) -> Dict[str, ColumnarCandles]:
    """심볼마다 캔들을 받아 곧바로 열로 옮기고 원본 리스트를 버린다.

    한 번에 다 받지 않는 이유는 메모리 피크다 — ``List[Candle]``은 심볼당 ~1.8GB라 다섯 심볼을
    한꺼번에 들면 그것만으로 9GB다. 하나씩 받아 옮기면 피크가 심볼 하나 분량에 그친다.
    """
    out: Dict[str, ColumnarCandles] = {}
    for symbol in symbols:
        candles = asyncio.run(history.fetch([symbol], start, end))[symbol]
        out[symbol] = ColumnarCandles.from_candles(candles)
        del candles
        gc.collect()
        logger.info("[%s] 열 캔들 %d개 (%.0fMB)", symbol, len(out[symbol]),
                    out[symbol].nbytes / 1e6)
    return out


@dataclass
class _SweepState:
    """fork 전에 부모가 모듈 전역에 걸어 두는 것. 워커는 이것을 fork로 물려받는다."""
    factory: StreamerFactory
    candles: Dict[str, ColumnarCandles]
    cache: Optional[Dict[Hashable, np.ndarray]]
    init_margin: float
    fee_ratio: Optional[float]
    summarise: Callable[[Report, BaseStreamer], Dict[str, Any]]
    jobs: List[SweepJob]


_STATE: Optional[_SweepState] = None


class ParameterSweep:
    """파라미터 격자를 fork 워커로 나눠 돌린다.

    :param factory: ``(symbols, params) -> 스트리머``. 칸마다 불린다.
    :param candles_by_symbol: 심볼별 캔들. ``ColumnarCandles``가 아니면 여기서 열로 옮긴다 —
        그래도 호출자가 원본 리스트를 들고 있으면 메모리는 줄지 않는다
        (:func:`load_columnar`를 쓸 것).
    :param init_margin: / :param fee_ratio: ``SimulatedExecutor``에 넘긴다.
    :param summarise: ``(Report, 스트리머) -> dict``. **워커에서** 불리고, 그 dict만 부모로
        돌아온다. 스트리머를 받는 것은 전략이 들고 있는 진단 카운터(예: 서킷브레이커 발동 수)를
        요약에 싣기 위해서다. 기본은 :func:`~core.result.metrics.summarise_report`.
    :param workers: 워커 수. ``None``이면 CPU와 가용 메모리로 정한다 (:meth:`resolve_workers`).
        ``1``이면 fork하지 않고 이 프로세스에서 순차로 돈다 — 디버깅과 대조용.
    :param worker_mem_gb: 워커 하나가 따로 쓰는 메모리 추정치. 워커 수 자동 결정에만 쓴다.
        결과 dict의 ``worker_private_gb``가 실측이다 (ETH 1m 6.5년 멀티채널에서 0.56GB).
    :param precompute: ``False``면 선계산 캐시를 만들지 않는다 (칸마다 워커가 계산한다).
    :param maxtasksperchild: 워커 하나가 칸 몇 개를 돈 뒤 새로 뜰지. 칸을 돌수록
        ``worker_private_gb``가 계속 는다면 설정한다.
    """

    def __init__(self, factory: StreamerFactory, candles_by_symbol: Mapping[str, Sequence],
                 *, init_margin: float, fee_ratio: Optional[float] = None,
                 summarise: Optional[Callable[[Report, BaseStreamer], Dict[str, Any]]] = None,
                 workers: Optional[int] = None, worker_mem_gb: float = 0.6,
                 precompute: bool = True, maxtasksperchild: Optional[int] = None):
        self.factory = factory
        self.candles: Dict[str, ColumnarCandles] = {
            s: c if isinstance(c, ColumnarCandles) else ColumnarCandles.from_candles(c)
            for s, c in candles_by_symbol.items()}
        self.init_margin = init_margin
        self.fee_ratio = fee_ratio
        self.summarise = summarise or (lambda report, _: summarise_report(report, init_margin))
        self.workers = workers
        self.worker_mem_gb = worker_mem_gb
        self.precompute = precompute
        self.maxtasksperchild = maxtasksperchild

    def run(self, jobs: Sequence[SweepJob],
            on_result: Optional[Callable[[SweepJob, Dict[str, Any]], None]] = None
            ) -> List[Tuple[SweepJob, Dict[str, Any]]]:
        """모든 칸을 돌려 ``jobs`` 순서대로 (칸, 요약) 목록을 돌려준다.

        :param on_result: 칸 하나가 끝날 때마다 **부모에서** 불린다 (완료 순서). 진행 출력용.
        """
        global _STATE
        jobs = list(jobs)
        missing = sorted({s for j in jobs for s in j.symbols} - set(self.candles))
        if missing:
            raise ValueError(f"캔들이 없는 심볼: {missing}")
        if not jobs:
            return []

        t0 = time.time()
        cache = self._warm_cache(jobs) if self.precompute else None
        if cache is not None:
            logger.info("선계산 캐시: 지표 %d개, %.0fMB (%.1f초)", len(cache),
                        sum(a.nbytes for a in cache.values()) / 1e6, time.time() - t0)
        workers = self.resolve_workers(len(jobs))

        _STATE = _SweepState(self.factory, self.candles, cache, self.init_margin,
                             self.fee_ratio, self.summarise, jobs)
        results: List[Optional[Dict[str, Any]]] = [None] * len(jobs)
        # 부모의 객체를 영구 세대로 옮긴다 — 워커의 GC가 그것을 훑으며 페이지를 복사하지
        # 않게 하려는 것이고, workers=1에서도 GC 비용(~1초/칸)을 덜어 준다.
        gc.collect()
        gc.freeze()
        try:
            if workers == 1:
                completed = map(_run_job, range(len(jobs)))
                self._collect(completed, jobs, results, on_result, t0)
            else:
                ctx = mp.get_context("fork")
                with ctx.Pool(workers, maxtasksperchild=self.maxtasksperchild) as pool:
                    completed = pool.imap_unordered(_run_job, range(len(jobs)))
                    self._collect(completed, jobs, results, on_result, t0)
        finally:
            _STATE = None
            gc.unfreeze()
        logger.info("스윕 완료: %d칸, 워커 %d개, %.1f초", len(jobs), workers, time.time() - t0)
        return list(zip(jobs, results))

    def resolve_workers(self, n_jobs: int) -> int:
        """워커 수: 지정값, 아니면 min(논리 CPU 수, 가용 메모리 / worker_mem_gb, 칸 수).

        논리 CPU 전부를 쓴다. 물리 코어 너머(HT 형제, E코어)의 워커는 칸 하나를 느리게
        하지만 총 처리량은 여전히 늘어난다 — i5-13600KF(20스레드) 실측, ETH 49칸:
        10워커 168초, 14워커 143초, 20워커 126초 (칸당 33→38→42초, 결과 동일). 가용 메모리는
        ``/proc/meminfo``의 ``MemAvailable``의 80%다 — 캔들과 캐시를 이미 올린 뒤에 재므로
        부모 몫은 빠져 있고, 다른 작업이 메모리를 쓰고 있으면 여기서 워커 수가 줄어든다.
        """
        if self.workers is not None:
            return max(1, min(self.workers, n_jobs))
        cpu = max(1, os.cpu_count() or 1)
        avail = _mem_available_gb()
        by_mem = cpu if avail is None else max(1, int(avail * 0.8 / self.worker_mem_gb))
        workers = max(1, min(cpu, by_mem, n_jobs))
        logger.info("워커 %d개 (CPU %d, 가용 메모리 %s GB / 워커당 %.1fGB → %d, 칸 %d)",
                    workers, cpu, "?" if avail is None else f"{avail:.1f}",
                    self.worker_mem_gb, by_mem, n_jobs)
        return workers

    # ------------------------------------------------------------------ 내부

    def _warm_cache(self, jobs: List[SweepJob]) -> Dict[Hashable, np.ndarray]:
        """모든 칸의 스트리머를 만들어 보고, 키가 있는 지표를 (심볼, 키)별로 한 번씩 계산한다.

        엔진의 캐시 규약(``BacktestEngine._cached_or_compute``)과 같은 키, 같은 입력
        배열(``ColumnarCandles.ohlcv()``)을 쓴다. 키가 없는 지표는 여기서 건너뛰고 워커가
        칸마다 계산한다.
        """
        cache: Dict[Hashable, np.ndarray] = {}
        for job in jobs:
            streamer = self.factory(list(job.symbols), job.params)
            for symbol in job.symbols:
                candles = self.candles[symbol]
                if not len(candles):
                    continue
                for indicator in streamer.indicators.get(symbol, {}).values():
                    if not isinstance(indicator, VectorizableIndicator):
                        continue
                    key = indicator.cache_key()
                    if key is None or (symbol, key) in cache:
                        continue
                    cache[(symbol, key)] = indicator.compute(*candles.ohlcv())
        return cache

    @staticmethod
    def _collect(completed, jobs, results, on_result, t0) -> None:
        for done, (i, summary) in enumerate(completed, 1):
            results[i] = summary
            logger.info("[%d/%d] %s %s %s — %.1f초 (누적 %.0f초)", done, len(jobs),
                        jobs[i].symbols, jobs[i].tag if jobs[i].tag is not None else "",
                        dict(jobs[i].params), summary["elapsed_s"], time.time() - t0)
            if on_result is not None:
                on_result(jobs[i], summary)


def _run_job(i: int) -> Tuple[int, Dict[str, Any]]:
    """워커 본체. ``_STATE``는 fork로 물려받은 것이다."""
    state = _STATE
    job = state.jobs[i]
    try:
        t0 = time.time()
        streamer = state.factory(list(job.symbols), job.params)
        executor = SimulatedExecutor(state.init_margin, fee_ratio=state.fee_ratio)
        recorder = SimpleRecorder(executor.status)
        report = BacktestEngine(streamer, {s: state.candles[s] for s in job.symbols}, executor,
                                recorder, progress=False, precomputed=state.cache).run()
        summary = dict(state.summarise(report, streamer))
        summary["elapsed_s"] = time.time() - t0
        summary["worker_private_gb"] = _private_gb()
        return i, summary
    except Exception as e:
        # 풀은 예외를 pickle로 옮기며 트레이스백을 잃는다 — 메시지에 실어 보낸다.
        raise RuntimeError(f"스윕 칸 {i} 실패 (symbols={job.symbols}, params={dict(job.params)}, "
                           f"tag={job.tag!r}): {type(e).__name__}: {e}\n"
                           f"{traceback.format_exc()}") from None


def _mem_available_gb() -> Optional[float]:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1e6
    except OSError:
        pass
    return None


def _private_gb() -> Optional[float]:
    """이 프로세스만 쓰는 메모리 (Private_Clean + Private_Dirty). RSS는 부모와 공유하는
    페이지까지 세므로 워커 수를 정하는 데는 이 값을 봐야 한다."""
    try:
        total = 0
        with open("/proc/self/smaps_rollup") as f:
            for line in f:
                if line.startswith(("Private_Clean:", "Private_Dirty:")):
                    total += int(line.split()[1])
        return total / 1e6
    except OSError:
        return None
