"""MassiveStockFetcher 스모크 체크 — 미국 주식 캔들이 신뢰할 만한지 확인한다.

    uv run python core/fetch_stock_check.py            # AAPL, 최근 완료된 2주
    uv run python core/fetch_stock_check.py MSFT       # 심볼 지정
    uv run python core/fetch_stock_check.py AAPL 2025-11-24 2025-11-29   # 휴장/조기폐장 낀 주

.env의 MASSIVE_API_KEY가 필요하다 (무료 Basic 티어로 충분).
"""
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
from dotenv import load_dotenv

from core.backtest.FastBacktester import FastBacktester
from core.stock_candle_fetcher import nyse_session
from core.stock_candle_fetcher.massive_fetcher import MassiveStockFetcher
from core.streamer.keltner_streamer import KeltnerStreamer
from core.utils import ms_timestamp_to_datetime

ET = timezone(timedelta(hours=-4))  # 미국 동부 서머타임 (출력용)


def parse_args():
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    if len(sys.argv) > 3:
        start = datetime.fromisoformat(sys.argv[2]).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(sys.argv[3]).replace(tzinfo=timezone.utc)
    else:
        # 마지막으로 완료된 세션에서 2주 뒤로 (무료 티어는 EOD라 오늘 데이터가 없다)
        end = nyse_session.last_closed_session_end(datetime.now(timezone.utc)).to_pydatetime()
        start = end - timedelta(days=14)
    return symbol, start, end


def main():
    load_dotenv()
    symbol, start, end = parse_args()
    interval = "1m"
    print(f"=== {symbol} {interval} | {start} ~ {end} ===\n")

    fetcher = MassiveStockFetcher()
    cache_dir = os.path.join(fetcher.save_path, "candle",
                             fetcher._cache_dir_name(symbol, interval))
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)  # 캐시 왕복을 검증하려면 빈 상태에서 시작해야 한다

    candles = fetcher.get_candles_with_cache(symbol, start, end, interval)
    assert candles, "캔들이 하나도 없습니다"

    # 1. 캔들 시각이 NYSE 세션 그리드와 정확히 일치하는가 (forward-fill이 동작했다는 증거).
    #    첫 실거래 이전 구간은 채울 가격이 없어 버려지므로 앞쪽 dropped개만 빠질 수 있다.
    grid = nyse_session.candle_grid(start, end, interval)
    actual = [c.start_time for c in candles]
    dropped = len(grid) - len(actual)
    sessions = nyse_session.session_bounds(start, end)
    assert dropped >= 0, f"그리드({len(grid)})보다 캔들({len(actual)})이 많습니다"
    assert actual == grid[dropped:].tolist(), "캔들 시각이 NYSE 세션 그리드와 어긋납니다"
    print(f"[1] 그리드 일치 OK — {len(actual)}봉 = 캘린더 {len(grid)}봉 "
          f"({len(sessions)} 세션, 첫 실거래 이전 {dropped}봉 제외)")

    # 2. 갭 구조 — 세션 안은 빈틈없고, 점프는 세션 경계에만 있어야 한다.
    #    (밤 8시간을 합성봉으로 채우면 데이터의 1/3이 가짜가 되므로 세션을 넘겨 채우지 않는다)
    diffs = np.diff(actual)
    break_idx = np.where(diffs != 60_000)[0]
    session_starts = {int(session_start.timestamp() * 1000) for session_start, _ in sessions}
    for i in break_idx:
        assert actual[i + 1] in session_starts, (
            f"세션 시작이 아닌 곳에 {diffs[i] / 60_000:.0f}분 갭: "
            f"{ms_timestamp_to_datetime(actual[i])} 다음")
    print(f"[2] 갭 구조 OK — 세션 내부 {int((diffs == 60_000).sum())}봉 전부 60초 간격, "
          f"갭 {len(break_idx)}개는 전부 세션 시작에 정확히 착지 "
          f"(최대 {max(diffs) / 60_000:.0f}분 = 휴장/주말)")

    # 3. 합성봉 비율 — 정규장에서 높으면 데이터가 이상한 것
    synthetic = [c for c in candles if c.volume == 0]
    rth = [c for c in candles
           if 13 <= ms_timestamp_to_datetime(c.start_time).hour < 20]  # 대략 09:30~16:00 ET
    rth_synthetic = [c for c in rth if c.volume == 0]
    print(f"[3] 합성봉: 전체 {len(synthetic) / len(candles) * 100:.1f}% | "
          f"정규장 {len(rth_synthetic) / max(len(rth), 1) * 100:.1f}%  "
          f"(정규장 비율이 높으면 유동성이 낮거나 데이터가 이상한 것)")

    # 4. 값 검증 — 공개 시세와 눈으로 대조 (신뢰할 만한 소스인지 확인하는 게 목적)
    print("[4] 실거래 봉 표본 — 공개 시세와 대조해 보세요:")
    traded = [c for c in candles if c.volume > 0]
    for c in (traded[0], traded[len(traded) // 2], traded[-1]):
        t = ms_timestamp_to_datetime(c.start_time).astimezone(ET)
        print(f"    {t:%Y-%m-%d %H:%M} ET  O={c.open:<9.4f} H={c.high:<9.4f} "
              f"L={c.low:<9.4f} C={c.close:<9.4f} V={c.volume:<12,.0f} n={c.trade_count}")
    lo = min(c.low for c in traded)
    hi = max(c.high for c in traded)
    print(f"    기간 저가/고가: {lo:.4f} / {hi:.4f}")
    assert lo > 0, "가격이 0 이하입니다"

    # 5. 캐시 왕복 — 두 번째 호출은 API 없이 청크에서
    calls_before = fetcher._last_call
    cached = fetcher.get_candles_with_cache(symbol, start, end, interval)
    assert len(cached) == len(candles), f"캐시 왕복 불일치: {len(cached)} != {len(candles)}"
    assert all(a.start_time == b.start_time and a.close == b.close
               for a, b in zip(candles, cached)), "캐시된 값이 원본과 다릅니다"
    chunks = sorted(n for n in os.listdir(cache_dir) if n.endswith(".pkl"))
    print(f"[5] 캐시 왕복 OK — {len(cached)}개 동일. 청크: {chunks}")
    print(f"    (split 커서: {open(os.path.join(cache_dir, '_meta.json')).read()})")
    if fetcher._last_call == calls_before:
        print("    2회차는 API 호출 없음")

    # 6. 파이프라인 통합 — 크래시 없이 도는지만 (파라미터 튜닝은 범위 밖)
    streamer = KeltnerStreamer(symbol=symbol, window=120, m_entry=2.0, m_exit=0.0,
                               fee_ratio=0.0005, max_loss=0.08)
    backtester = FastBacktester(streamer, candles)
    init_margin = backtester.status.total_margin()
    report = backtester.run()
    # Trade.status는 거래 전 스냅샷이므로 최종 상태를 쓴다 (마지막 거래 손익/수수료 포함)
    final = report.status.total_margin()
    print(f"[6] FastBacktester OK — {len(report.trades)} trades, "
          f"max leverage {report.max_leverage}, "
          f"수익률 {(final / init_margin - 1) * 100:+.2f}% "
          f"(짧은 구간 + 임의 파라미터라 수치 자체는 의미 없음)")

    print("\n=== 전부 통과 ===")


if __name__ == "__main__":
    main()
