"""라이브/드라이런 실행 기록을 디스크에 남긴다 — **런 길이와 무관하게 봉당 O(1)**.

산출물은 ``<result_path>/live/`` 아래에 쌓인다:

- ``<run_id>.json`` — 재개에 필요한 것만 담은 작은 고정 크기 파일: metadata(실행 설정,
  ``last_status``) + series 인덱스(컬럼, 샤드 목록). 매 봉 원자적으로 다시 쓴다.
- ``<run_id>.trades.jsonl`` — 체결 로그. 체결마다 한 줄 append만 한다.
- ``<run_id>.<YYYY-MM>.series.json`` — 월별 컬럼형 OHLC + 지표 + balance 샤드 (백테스트와 같은 모양).

백테스트 런 JSON과 달리 summary/equity/benchmark/trades를 담지 않는다. 그것들은 매 봉 전체
이력으로 다시 계산해야 해서 몇 달 지나면 봉당 수백 ms가 asyncio 루프를 막고, 분할 서브계정에서는
리밸런서 이체가 손익으로 잡혀 값 자체가 틀린다. 필요하면 샤드의 ``balance``와 체결 로그로
사후에 계산한다. 그래서 visualiser는 라이브 런을 렌더링하지 못한다.

라이브는 몇 주씩 살아 있고 중간에 언제든 죽는다(도커 ``restart: always``). 그래서:

1. **체크포인트** — 매 봉 런 JSON을, 주기적으로/체결 시점에 현재 월 샤드를 다시 쓴다.
   쓰기는 전부 원자적이라(:func:`core.result.writer._write_json`) 중간에 죽어도
   직전 상태가 온전하다.
2. **재개** — 기동 시 같은 ``run_id``의 런 JSON과 **마지막 샤드 하나**만 읽어 이어쓴다.

asyncio에 의존하지 않는 순수 동기 클래스다 — 매매 경로에 await 지점을 만들지 않기 위해서다.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from core.candle.candle import Candle
from core.order import order_book
from core.account.status import PositionState, Status
from core.account.trade import Trade
from core.recorder.base import Recorder
from core.result.indicator_columns import collect_indicator_columns
from core.result.writer import ShardWriter, _write_json, trade_to_dict
from core.streamer import BaseStreamer

# 런 JSON은 작아서 매 봉 다시 써도 부담이 없다. 샤드는 1분봉 한 달이 수 MB라 매 봉 다시 쓰면
# 하루 수 GB가 되므로 기본값을 시간 단위로 둔다.
DEFAULT_SHARD_FLUSH_EVERY = 60

# 라이브 런 JSON 포맷 버전 (백테스트 ``SCHEMA_VERSION``과 별개).
# 1: summary/equity/benchmark 제거, trades를 ``.trades.jsonl``로 분리.
LIVE_SCHEMA_VERSION = 1
# 이 포맷 이전의 라이브 런 JSON은 백테스트 포맷(``schema_version`` 5, 샤드 v4 모양)이었다.
# 샤드 모양이 같으므로 그대로 이어쓰고, 인라인 trades만 jsonl로 한 번 옮긴다.
_LEGACY_SCHEMA_VERSION = 5
# 구 포맷 metadata에서 더는 쓰지 않는 키 — 재개할 때 버린다.
_LEGACY_META_KEYS = ("init_margin", "candle_count", "start", "interval_ms", "label")


def default_run_id(streamer: BaseStreamer, symbols: List[str], interval: str,
                   dry_run: bool) -> str:
    """재기동해도 같은 런에 이어쓰기 위한 **고정** run_id.

    백테스트 run_id(``<Streamer>_<YYYYmmdd_HHMMSS>``)와 겹치지 않게 모드를 접두어로 붙인다.

    드라이런과 라이브를 나누는 것도 필수다. 드라이런 자본은 합성값(1e6)이고 라이브는 실제
    지갑 잔고라, 한 곡선에 섞이면 아무 의미가 없어진다.

    심볼은 **정렬해서** 이어붙인다 — 설정 파일에 나열된 순서가 바뀌어도(같은 심볼 집합이면)
    같은 run_id를 얻어야 재기동 시 새 런으로 갈라지지 않는다.
    """
    mode = "dry" if dry_run else "live"
    joined = "-".join(sorted(s.upper() for s in symbols))
    return f"{mode}_{type(streamer).__name__}_{joined}_{interval}"


class LiveRecorder(Recorder):
    """라이브 트레이더의 봉/체결을 적재한다.

    :param status: 트레이더의 ``Status`` **객체 자체**(복사본 아님). 샤드의 balance와
        ``last_status``가 항상 최신이어야 하므로 참조로 들고 있는다.
    """

    def __init__(self,
                 result_path: str,
                 run_id: str,
                 streamer: BaseStreamer,
                 status: Status,
                 interval_ms: int,
                 metadata: Dict,
                 shard_flush_every: int = DEFAULT_SHARD_FLUSH_EVERY,
                 has_ohlc: bool = True):
        self.logger = logging.getLogger(__name__)
        self.dir_path = os.path.join(result_path, "live")
        os.makedirs(self.dir_path, exist_ok=True)

        self._streamer = streamer
        self.symbols: List[str] = list(streamer.symbols)
        self._status = status
        self._interval_ms = interval_ms
        self._has_ohlc = has_ohlc
        self._shard_flush_every = max(1, shard_flush_every)

        self._indicator_names, _ = collect_indicator_columns(streamer)
        self._columns = self._indicator_names + ["balance"]

        self._last_time: Optional[int] = None
        self._candles_since_shard_flush = 0
        self._closed = False

        self.run_id = run_id
        self._metadata = dict(metadata)
        self._metadata.setdefault("run_at", datetime.now(timezone.utc).isoformat())
        self._metadata["restart_count"] = 0

        #: 재개한 런에 저장돼 있던 계좌 상태. 드라이런은 이걸로 합성 자본·미체결 장부를 되돌리고,
        #: 라이브는 거래소 값과 대조만 한다 (거래소가 정답이다).
        self.resumed_status: Optional[Status] = None

        doc = self._load_existing(run_id)
        if doc is not None and self._is_compatible(doc):
            self._resume(doc)
        elif doc is not None:
            # 지표 구성이나 파라미터가 바뀐 채로 이어쓰면 샤드 컬럼이 뒤섞인 데이터가 된다.
            # .env에서 WINDOW만 고치고 재기동하는 게 가장 흔한 오염 경로라 여기서 끊는다.
            self.run_id = f"{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.logger.error(
                "기존 런(%s)과 설정이 달라 이어쓰지 않는다 — 새 런 %s 으로 기록한다",
                run_id, self.run_id)
            self._shard_writer = self._new_shard_writer()
        else:
            self._shard_writer = self._new_shard_writer()
        self._repair_trades_tail()

        self.logger.info("live recorder: %s (restart_count=%d)", self._run_json_path(self.run_id),
                         self._metadata["restart_count"])

    # ------------------------------------------------------------------ 기록

    def record_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        """이벤트 하나를 적재한다. 라이브 이벤트는 심볼 하나짜리다.

        엔진의 기록 지점이 백테스트와 1:1로 대응한다 — 모든 지표가 갱신되고 ``decide_action``이
        반환한 직후이므로, 여기서 읽는 지표 값은 그 결정이 실제로 본 값이다. 자본은 마지막
        ``ACCOUNT_UPDATE`` 기준 ``total_margin()``이다 (백테스트처럼 봉마다 재마킹하지 않는다).
        """
        self._last_time = event_time
        self._shard_writer.add(event_time, self._status.total_margin(), candles)
        self._candles_since_shard_flush += 1

    def record_trade(self, trade: Trade) -> None:
        """체결 하나를 로그에 append하고 **즉시** 체크포인트한다.

        라이브 체결은 이벤트 바깥(유저 데이터 스트림)에서도 도착하므로 이벤트 끝의 flush를
        기다릴 수 없다. 이걸 빼면 크래시 시 체결은 남았는데 그 체결을 낳은 봉 구간의
        시계열이 통째로 없는, 앞뒤가 맞지 않는 런이 남는다.
        """
        self._append_trades([trade_to_dict(trade)])
        self.flush(force=True)

    def end_event(self, event_time: int) -> None:
        """이벤트 끝. 런 JSON은 매번, 샤드는 주기가 됐을 때만 다시 쓴다."""
        self.flush()

    # ------------------------------------------------------------------ 영속화

    def flush(self, force: bool = False) -> None:
        """런 JSON을 다시 쓰고, 주기가 됐거나 ``force``면 현재 월 샤드도 체크포인트한다."""
        if force or self._candles_since_shard_flush >= self._shard_flush_every:
            self._shard_writer.checkpoint()
            self._candles_since_shard_flush = 0

        doc = {
            "live_schema_version": LIVE_SCHEMA_VERSION,
            "metadata": self._build_metadata(),
            "series": {
                "symbols": self.symbols,
                "columns": self._columns,
                "has_ohlc": self._has_ohlc,
                "interval_ms": self._interval_ms,
                "shards": self._shard_writer.shards,
            },
        }
        _write_json(self._run_json_path(self.run_id), doc)

    def close(self) -> None:
        """마지막 flush. **멱등이고 예외를 밖으로 내지 않는다.**

        ``BinanceTrader.stop()``은 main의 finally와 두 리스너의 치명적 오류 경로에서 최대 두 번
        불릴 수 있고, 그 안에서 예외가 나면 나머지 정리 작업이 통째로 건너뛰어진다.
        """
        if self._closed:
            return
        self._closed = True
        try:
            self.flush(force=True)
        except Exception:
            self.logger.exception("live recorder 마지막 flush 실패")

    def set_metadata(self, key: str, value) -> None:
        """런 JSON metadata에 값을 남긴다 (다음 flush부터 반영)."""
        self._metadata[key] = value

    # ------------------------------------------------------------------ 내부

    def _new_shard_writer(self) -> ShardWriter:
        return ShardWriter(self.dir_path, self.run_id, self.symbols, self._streamer.indicators,
                           self._indicator_names, self._has_ohlc, self._interval_ms)

    def _run_json_path(self, run_id: str) -> str:
        return os.path.join(self.dir_path, f"{run_id}.json")

    @property
    def trades_path(self) -> str:
        return os.path.join(self.dir_path, f"{self.run_id}.trades.jsonl")

    def _append_trades(self, rows: List[Dict]) -> None:
        """체결 로그에 줄을 덧붙인다. 체결은 드물어서 매번 fsync해도 부담이 없다."""
        with open(self.trades_path, "a") as f:
            for row in rows:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _repair_trades_tail(self) -> None:
        """쓰다 죽어 줄바꿈 없이 끝난 마지막 줄이 있으면 줄바꿈을 붙여, 다음 줄이 거기 들러붙지
        않게 한다 (잘린 줄 자체는 읽는 쪽이 건너뛴다)."""
        try:
            with open(self.trades_path, "rb+") as f:
                f.seek(0, os.SEEK_END)
                if f.tell() == 0:
                    return
                f.seek(-1, os.SEEK_END)
                if f.read(1) != b"\n":
                    f.write(b"\n")
        except FileNotFoundError:
            pass

    def _load_existing(self, run_id: str) -> Optional[Dict]:
        path = self._run_json_path(run_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except (OSError, ValueError) as e:
            self.logger.error("기존 런 JSON을 읽지 못했다 (%s): %s — 새 런으로 시작한다", path, e)
            return None

    def _is_compatible(self, doc: Dict) -> bool:
        """이어써도 되는 런인지 — 포맷, 지표 컬럼, 전략/심볼 설정이 그대로여야 한다.

        구 포맷(백테스트 ``schema_version`` 5) 런도 받는다: 샤드 모양이 같아서 이어쓸 수 있다.
        그보다 오래된 버전(v1-v3, 평평한 샤드)은 형식이 안 맞으므로 새 런으로 갈라진다.
        """
        series = doc.get("series") or {}
        meta = doc.get("metadata") or {}
        fmt_ok = (doc.get("live_schema_version") == LIVE_SCHEMA_VERSION
                  or doc.get("schema_version") == _LEGACY_SCHEMA_VERSION)
        if not fmt_ok:
            self.logger.error("런 포맷 불일치: live_schema_version=%r schema_version=%r",
                              doc.get("live_schema_version"), doc.get("schema_version"))
        checks = [
            ("columns", series.get("columns"), self._columns),
            ("symbols", meta.get("symbols"), self._metadata.get("symbols")),
            ("interval", meta.get("interval"), self._metadata.get("interval")),
            ("params", meta.get("params"), self._metadata.get("params")),
            ("streamer", meta.get("streamer"), self._metadata.get("streamer")),
        ]
        ok = fmt_ok
        for name, saved, current in checks:
            if saved != current:
                self.logger.error("런 설정 불일치 [%s]: 저장=%r 현재=%r", name, saved, current)
                ok = False
        return ok

    def _resume(self, doc: Dict) -> None:
        meta = doc.get("metadata") or {}
        series = doc.get("series") or {}

        saved_status = meta.get("last_status")
        if isinstance(saved_status, dict):
            positions = {
                sym: PositionState(avg_price=float(p.get("avg_price", 0.0)),
                                   position=float(p.get("position", 0.0)),
                                   unrealised_pnl=float(p.get("unrealised_pnl", 0.0)))
                for sym, p in (saved_status.get("positions") or {}).items()
            }
            # fee_ratio는 담지 않는다 — 기동 시점에 새로 정해진다 (라이브: 거래소 커미션 조회,
            # 시뮬/드라이런: SimulatedExecutor 생성자 인자).
            self.resumed_status = Status(
                margin=float(saved_status.get("margin", 0.0)),
                positions=positions,
                # 드라이런은 이 장부가 유일한 사본이다. 복원하지 않으면 재기동할 때마다
                # 걸어둔 손절이 사라진다. 라이브에서는 거래소 장부와 대조하는 데 쓴다
                # (LiveExecutor.reconcile_resumed).
                open_orders=order_book.deserialize(saved_status.get("open_orders")))

        # 마지막 샤드 하나만 버퍼로 되읽는다 — 그 이전 달은 인덱스만 이어받는다.
        self._shard_writer = ShardWriter.resume(self.dir_path, self.run_id, self.symbols,
                                                self._streamer.indicators,
                                                self._indicator_names, self._has_ohlc,
                                                self._interval_ms, series.get("shards") or [])

        # 구 포맷: 인라인 trades를 체결 로그로 한 번 옮긴다 (로그가 이미 있으면 옮긴 것이다).
        legacy_trades = doc.get("trades")
        if legacy_trades and not os.path.exists(self.trades_path):
            self._append_trades(legacy_trades)
            self.logger.info("구 포맷 런의 체결 %d건을 %s 로 옮겼다",
                             len(legacy_trades), self.trades_path)

        restart_count = int(meta.get("restart_count", 0) or 0) + 1
        merged = {k: v for k, v in {**meta, **self._metadata}.items()
                  if k not in _LEGACY_META_KEYS}
        merged["run_at"] = meta.get("run_at", merged.get("run_at"))
        merged["restart_count"] = restart_count
        merged["resumed_at"] = datetime.now(timezone.utc).isoformat()
        self._metadata = merged

    def _build_metadata(self) -> Dict:
        meta = dict(self._metadata)
        # 계좌 상태를 같이 남긴다. 드라이런은 합성 자본이라 재기동하면 초기값으로 돌아가는데,
        # 샤드의 balance는 이어붙으므로 복원하지 않으면 재기동 지점에서 곡선이 튄다.
        # 미체결 주문도 같이 남긴다. 드라이런은 장부가 프로세스 안에만 있어서, 이게 없으면
        # 재기동할 때마다 걸어둔 손절이 조용히 사라진다 (라이브는 거래소가 들고 있다).
        meta["last_status"] = {
            "margin": self._status.margin,
            "positions": {
                sym: {"avg_price": p.avg_price, "position": p.position,
                      "unrealised_pnl": p.unrealised_pnl}
                for sym, p in self._status.positions.items()
            },
            "open_orders": order_book.serialize(self._status.open_orders),
        }
        if self._last_time is not None:
            meta["end"] = _iso(self._last_time)
        return meta


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
