"""라이브/드라이런 실행 결과를 **백테스트와 같은 포맷으로** 디스크에 남긴다.

산출물은 ``<result_path>/live/`` 아래에 백테스터와 동일한 구조로 쌓인다:

- ``<run_id>.json`` — metadata / summary / equity / benchmark / trades / series 인덱스
- ``<run_id>.<YYYY-MM>.series.json`` — 월별 컬럼형 OHLC + 지표 샤드

백테스터는 캔들을 다 돌고 나서 한 번에 쓰면 그만이지만 라이브는 몇 주씩 살아 있고 중간에
언제든 죽는다(도커 ``restart: always``). 그래서 이 클래스는 두 가지를 더 한다:

1. **체크포인트** — 캔들마다 런 JSON을, 주기적으로/체결 시점에 현재 월 샤드를 다시 쓴다.
   쓰기는 전부 원자적이라(:func:`core.result.writer._write_json`) 중간에 죽어도
   직전 상태가 온전하다.
2. **재개** — 기동 시 같은 ``run_id``의 런 JSON과 샤드를 읽어 자본 곡선/체결/누적 지표를
   복원하고 이어쓴다. 재기동마다 새 파일이 생기면 곡선이 조각나 백테스트와 비교할 수 없다.

asyncio에 의존하지 않는 순수 동기 클래스다 — 매매 경로에 await 지점을 만들지 않기 위해서다.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from core.domain import Candle
from core.domain import order_book
from core.domain.report import Report
from core.domain.status import PositionState, Status
from core.domain.trade import Trade
from core.engine.recorder import Recorder
from core.result.indicator_columns import collect_indicator_columns
from core.result.metrics import build_multi_symbol_buy_and_hold_curve
from core.result.writer import SCHEMA_VERSION, ShardWriter, read_shard, write_run_json
from core.streamer import BaseStreamer

# 런 JSON은 작고(수백 KB) 자주 써도 부담이 없으므로 캔들마다 갱신한다. 샤드는 1분봉 한 달이
# 수 MB라 매 캔들 다시 쓰면 하루 수 GB가 되므로 기본값을 시간 단위로 둔다.
DEFAULT_SHARD_FLUSH_EVERY = 60


def default_run_id(streamer: BaseStreamer, symbols: List[str], interval: str,
                   dry_run: bool) -> str:
    """재기동해도 같은 런에 이어쓰기 위한 **고정** run_id.

    백테스트 run_id(``<Streamer>_<YYYYmmdd_HHMMSS>``)와 절대 겹치면 안 된다 — 프론트의 파일
    접근 계층이 디렉토리 트리 전체에서 **basename으로** 파일을 찾으므로, 같은 이름이면
    ``asset/live/``와 ``asset/backtest/`` 중 하나가 다른 하나를 가린다.

    드라이런과 라이브를 나누는 것도 필수다. 드라이런 자본은 합성값(1e6)이고 라이브는 실제
    지갑 잔고라, 한 곡선에 섞이면 아무 의미가 없어진다.

    심볼은 **정렬해서** 이어붙인다 — 설정 파일에 나열된 순서가 바뀌어도(같은 심볼 집합이면)
    같은 run_id를 얻어야 재기동 시 새 런으로 갈라지지 않는다.
    """
    mode = "dry" if dry_run else "live"
    joined = "-".join(sorted(s.upper() for s in symbols))
    return f"{mode}_{type(streamer).__name__}_{joined}_{interval}"


class LiveRecorder(Recorder):
    """라이브 트레이더의 캔들/체결을 백테스트 포맷으로 적재한다.

    백테스터의 ``run()`` 루프가 지역 변수로 들고 있던 것(자본 곡선, 종가, 체결 목록,
    최대 레버리지, ShardWriter)을 대신 소유한다.

    :param status: 트레이더의 ``Status`` **객체 자체**(복사본 아님). summary의 final_margin이
        항상 최신이어야 하므로 참조로 들고 있는다.
    """

    def __init__(self,
                 result_path: str,
                 run_id: str,
                 streamer: BaseStreamer,
                 status: Status,
                 interval_ms: int,
                 init_margin: float,
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

        self._indicator_names, self._column_groups = collect_indicator_columns(streamer)
        self._columns = self._indicator_names + ["balance"]

        self._equity: List[Tuple[int, float]] = []
        self._closes_by_symbol: Dict[str, List[Optional[float]]] = {
            s: [] for s in self.symbols}
        #: 심볼별 최근 종가 캐리포워드 (캔들이 없는 이벤트에서도 직전 값을 잇는다).
        self._latest_close: Dict[str, float] = {}
        self._trades: List[Trade] = []
        self._max_leverage = 0.0
        self._candles_since_shard_flush = 0
        self._closed = False

        self.run_id = run_id
        self._init_margin = init_margin
        self._metadata = dict(metadata)
        self._metadata.setdefault("run_at", datetime.now(timezone.utc).isoformat())
        self._metadata["restart_count"] = 0

        #: 재개한 런에 저장돼 있던 계좌 상태. **드라이런에서만** 의미가 있다 — 합성 자본은
        #: 프로세스가 죽으면 초기값으로 돌아가므로 이걸로 되돌려야 곡선이 이어진다.
        #: 라이브는 거래소가 정답이므로 트레이더가 이 값을 쓰지 않는다.
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

        self.logger.info("live recorder: %s (init_margin=%s, 복원 캔들=%d, 체결=%d)",
                         os.path.join(self.dir_path, f"{self.run_id}.json"),
                         self._init_margin, len(self._equity), len(self._trades))

    # ------------------------------------------------------------------ 기록

    def record_event(self, event_time: int, candles: Dict[str, Candle]) -> None:
        """이벤트 하나를 적재한다. 라이브 이벤트는 심볼 하나짜리다.

        엔진의 기록 지점이 백테스트와 1:1로 대응한다 — 모든 지표가 갱신되고 ``decide_action``이
        반환한 직후이므로, 여기서 읽는 지표 값은 그 결정이 실제로 본 값이다. 자본은 마지막
        ``ACCOUNT_UPDATE`` 기준 ``total_margin()``이다 (백테스트처럼 봉마다 재마킹하지 않는다).
        """
        # 자본과 종가는 **항상 같이** 늘어나야 한다. _downsample_equity가 길이로 stride를
        # 정하므로 어긋나면 equity와 benchmark의 인덱스 짝이 조용히 밀린다.
        equity = self._status.total_margin()
        self._equity.append((event_time, equity))
        for symbol, candle in candles.items():
            self._latest_close[symbol] = candle.close
        for s in self.symbols:
            self._closes_by_symbol[s].append(self._latest_close.get(s))

        symbol_data = {
            symbol: (candle, {name: ind.get_latest() for name, ind
                              in self._streamer.indicators.get(symbol, {}).items()})
            for symbol, candle in candles.items()
        }
        self._shard_writer.add(event_time, equity, symbol_data)
        self._candles_since_shard_flush += 1

    def record_trade(self, trade: Trade) -> None:
        """체결 하나를 적재하고 **즉시** 체크포인트한다.

        라이브 체결은 이벤트 바깥(유저 데이터 스트림)에서도 도착하므로 이벤트 끝의 flush를
        기다릴 수 없다. 이걸 빼면 크래시 시 체결은 남았는데 그 체결을 낳은 캔들 구간의
        시계열이 통째로 없는, 앞뒤가 맞지 않는 런이 남는다.
        """
        self._max_leverage = max(self._max_leverage, trade.leverage)
        self._trades.append(trade)
        self.flush(force=True)

    def end_event(self, event_time: int) -> None:
        """이벤트 끝. 런 JSON은 매번, 샤드는 주기가 됐을 때만 다시 쓴다."""
        self.flush()

    # ------------------------------------------------------------------ 영속화

    def flush(self, force: bool = False) -> None:
        """런 JSON을 다시 쓰고, 주기가 됐거나 ``force``면 현재 월 샤드도 체크포인트한다."""
        checkpointing = force or self._candles_since_shard_flush >= self._shard_flush_every
        if checkpointing:
            self._shard_writer.checkpoint()
            self._candles_since_shard_flush = 0

        benchmark = build_multi_symbol_buy_and_hold_curve(
            [t for t, _ in self._equity], self._closes_by_symbol, self._init_margin)
        report = Report(self._trades, self._max_leverage, self._status, self._equity, benchmark)
        write_run_json(self.dir_path, self.run_id, report, self._build_metadata(checkpointing),
                       self._shard_writer.shards,
                       symbols=self.symbols,
                       columns=self._columns,
                       column_groups={**self._column_groups, "balance": "balance"},
                       has_ohlc=self._has_ohlc, interval_ms=self._interval_ms,
                       init_margin=self._init_margin)

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

    @property
    def last_recorded_end_time(self) -> Optional[int]:
        return self._equity[-1][0] if self._equity else None

    @property
    def candle_count(self) -> int:
        return len(self._equity)

    # ------------------------------------------------------------------ 내부

    def _new_shard_writer(self) -> ShardWriter:
        return ShardWriter(self.dir_path, self.run_id, self.symbols, self._indicator_names,
                           self._has_ohlc, self._interval_ms)

    def _run_json_path(self, run_id: str) -> str:
        return os.path.join(self.dir_path, f"{run_id}.json")

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
        """이어써도 되는 런인지 — 지표 컬럼과 전략/심볼 설정이 그대로여야 한다.

        ``schema_version``도 검사한다: v4에서 샤드의 물리적 모양이 바뀌었으므로(평평한
        ohlc/indicators -> symbols 아래 중첩) 그 이전 버전 런은 다른 설정이 전부 일치해도
        이어쓰지 않는다 — v1-v3 샤드를 v4 코드로 재개하려 들면 형식이 안 맞아 깨진다.
        """
        series = doc.get("series") or {}
        meta = doc.get("metadata") or {}
        checks = [
            ("schema_version", doc.get("schema_version"), SCHEMA_VERSION),
            ("columns", series.get("columns"), self._columns),
            ("symbols", meta.get("symbols"), self._metadata.get("symbols")),
            ("interval", meta.get("interval"), self._metadata.get("interval")),
            ("params", meta.get("params"), self._metadata.get("params")),
            ("streamer", meta.get("streamer"), self._metadata.get("streamer")),
        ]
        ok = True
        for name, saved, current in checks:
            if saved != current:
                self.logger.error("런 설정 불일치 [%s]: 저장=%r 현재=%r", name, saved, current)
                ok = False
        return ok

    def _resume(self, doc: Dict) -> None:
        meta = doc.get("metadata") or {}
        summary = doc.get("summary") or {}
        series = doc.get("series") or {}

        # init_margin은 반드시 저장된 값을 쓴다. 현재 지갑 잔고로 덮으면 재기동할 때마다
        # profit_pct가 0으로 리셋돼 수익률이 영원히 0 근처를 맴돈다.
        self._init_margin = float(meta.get("init_margin", self._init_margin))
        self._max_leverage = float(summary.get("max_leverage", 0.0) or 0.0)
        self._trades = self._restore_trades(doc.get("trades") or [])

        saved_status = meta.get("last_status")
        if isinstance(saved_status, dict):
            positions = {
                sym: PositionState(avg_price=float(p.get("avg_price", 0.0)),
                                   position=float(p.get("position", 0.0)),
                                   unrealised_pnl=float(p.get("unrealised_pnl", 0.0)))
                for sym, p in (saved_status.get("positions") or {}).items()
            }
            self.resumed_status = Status(
                margin=float(saved_status.get("margin", 0.0)),
                positions=positions,
                leverage=float(saved_status.get("leverage", 0.0)),
                # 드라이런은 이 장부가 유일한 사본이다. 복원하지 않으면 재기동할 때마다
                # 걸어둔 손절이 사라진다. 라이브에서는 거래소 장부와 대조하는 데 쓴다
                # (BinanceTrader._reconcile_resumed_orders).
                open_orders=order_book.deserialize(saved_status.get("open_orders")))

        shards = series.get("shards") or []
        self._restore_curves(shards)
        self._shard_writer = ShardWriter.resume(self.dir_path, self.run_id, self.symbols,
                                                self._indicator_names, self._has_ohlc,
                                                self._interval_ms, shards)

        # 저장된 metadata를 베이스로 삼아 label 같은 프론트 소유 필드를 보존한다.
        restart_count = int(meta.get("restart_count", 0) or 0) + 1
        merged = {**meta, **self._metadata}
        merged["run_at"] = meta.get("run_at", merged.get("run_at"))
        merged["restart_count"] = restart_count
        merged["resumed_at"] = datetime.now(timezone.utc).isoformat()
        self._metadata = merged

    @staticmethod
    def _restore_trades(raw: List[Dict]) -> List[Trade]:
        """런 JSON의 체결 기록에서 Trade를 복원한다.

        ``status``는 거래 전 스냅샷의 **대용**이다: ``build_summary``가 쓰는 것은
        ``position``의 부호와 ``total_margin()``뿐이라, position/margin만 채운 Status면
        집계 결과가 원본과 같다.

        ``position``은 schema v3에서 추가됐다. 그 이전 파일에는 없어서 0으로 떨어지고, 그런
        체결은 승/패 어느 쪽으로도 세어지지 않는다 — 라이브 런은 v3부터 생기므로 실전에서는
        걸리지 않는 경로다.
        """
        trades = []
        for d in raw:
            symbol = d.get("symbol", "")
            trades.append(Trade(
                timestamp=int(d["timestamp"]),
                symbol=symbol,
                quantity=float(d["quantity"]),
                price=float(d["price"]),
                wnl=float(d["wnl"]),
                fee=float(d["fee"]),
                status=Status(margin=float(d.get("margin", 0.0)),
                              positions={symbol: PositionState(position=float(d.get("position", 0.0)))}),
                leverage=float(d.get("leverage", 0.0)),
                order_type=d.get("order_type", "MARKET"),
                submitted_at=d.get("submitted_at"),
            ))
        return trades

    def _restore_curves(self, shards: List[Dict]) -> None:
        """샤드에서 자본 곡선과 심볼별 종가를 되살린다.

        런 JSON의 ``equity``는 2000점으로 다운샘플된 값이라 쓰면 안 된다 — 그걸로 복원하면
        재기동할 때마다 곡선이 한 번 더 솎여서 계속 열화된다. 샤드의 ``balance`` 컬럼이
        백테스터가 equity_curve에 넣는 값과 정확히 같은 원본이다.

        ``balance``가 null인 행은 워밍업이거나 자본이 정확히 0(파산)인 경우라 통째로
        건너뛴다. 반면 한 심볼의 종가만 null인 행은 그 심볼의 캔들이 그 이벤트에 마감하지
        않은 정상적인 ragged-series 구멍이므로, equity를 유지한 채 그 심볼의 자리에만
        None을 채워 넣는다 — build_multi_symbol_buy_and_hold_curve가 내부적으로 앞선
        값으로 채운다.
        """
        for entry in shards:
            shard = read_shard(self.dir_path, entry.get("file", ""))
            if not shard:
                continue
            times = shard.get("time") or []
            balances = shard.get("balance") or []
            symbol_shards = shard.get("symbols") or {}
            closes_by_symbol = {
                s: ((symbol_shards.get(s) or {}).get("ohlc") or {}).get("close") or []
                for s in self.symbols
            }
            for i, t in enumerate(times):
                if i >= len(balances):
                    break
                balance = balances[i]
                if balance is None:
                    continue
                self._equity.append((int(t), float(balance)))
                for s in self.symbols:
                    col = closes_by_symbol[s]
                    self._closes_by_symbol[s].append(col[i] if i < len(col) else None)

    def _build_metadata(self, reread_label: bool = False) -> Dict:
        meta = dict(self._metadata)
        meta["interval_ms"] = self._interval_ms
        meta["init_margin"] = self._init_margin
        meta["candle_count"] = len(self._equity)
        # 계좌 상태를 같이 남긴다. 드라이런은 합성 자본이라 재기동하면 초기값으로 돌아가는데,
        # 자본 곡선은 이어붙으므로 복원하지 않으면 재기동 지점에서 곡선이 튄다.
        # 미체결 주문도 같이 남긴다. 드라이런은 장부가 프로세스 안에만 있어서, 이게 없으면
        # 재기동할 때마다 걸어둔 손절이 조용히 사라진다 (라이브는 거래소가 들고 있다).
        meta["last_status"] = {
            "margin": self._status.margin,
            "leverage": self._status.leverage,
            "positions": {
                sym: {"avg_price": p.avg_price, "position": p.position,
                     "unrealised_pnl": p.unrealised_pnl}
                for sym, p in self._status.positions.items()
            },
            "open_orders": order_book.serialize(self._status.open_orders),
        }
        if self._equity:
            meta.setdefault("start", _iso(self._equity[0][0]))
            meta["end"] = _iso(self._equity[-1][0])

        # label은 visualiser가 쓰는 필드인데, 그쪽은 런 JSON 전체를 다시 직렬화하는 식으로
        # 고친다. 되읽어 병합하지 않으면 사용자가 붙인 라벨이 다음 flush에 그대로 날아간다.
        # 매 캔들 읽을 필요는 없어서 체크포인트/종료 시에만 확인하고, 읽은 값은 캐시해 둔다.
        if reread_label:
            disk = self._load_existing(self.run_id)
            label = (disk or {}).get("metadata", {}).get("label")
            if label:
                self._metadata["label"] = label
                meta["label"] = label
        return meta


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
