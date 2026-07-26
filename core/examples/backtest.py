import sys
from datetime import datetime, timezone

from core.backtest.FastBacktester import FastBacktester
from core.binance_candle_fetcher.vision_fetcher import BinanceVisionFetcher
from core.streamer.keltner_streamer import KeltnerStreamer

if __name__ == "__main__":
    # 1. 캔들 데이터 로드 (없는 달만 data.binance.vision에서 받아 asset/candle/ 에 캐시)
    symbol = "ETHUSDT"
    start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2026, 7, 10, tzinfo=timezone.utc)
    interval = "1m"

    fetcher = BinanceVisionFetcher(compress=False)
    candles = fetcher.get_candles_with_cache(symbol, start_date, end_date, interval)

    # 2. 전략 생성 — 다른 전략을 돌리려면 여기만 바꾸면 된다 (streamer/ 참고)
    #    max_loss는 스탑 거리에서의 손실 한도이자 사실상 레버리지 손잡이다. 0.08처럼 크게
    #    잡으면 1m 캔들에서는 스탑 거리가 워낙 좁아 항상 6x 캡에 붙고, 왕복 수수료가
    #    자본의 ~0.5%씩 수백 번 나가면서 전략과 무관하게 계좌가 녹는다. 데모 기본값은
    #    레버리지가 ~1.6x에 머무는 값을 쓴다.
    params = dict(window=72 * 60, m_entry=4.0, m_exit=3.0,
                  fee_ratio=0.0004, max_loss=0.005)
    streamer = KeltnerStreamer(symbol=symbol, **params)

    # 3. 백테스트 실행
    backtester = FastBacktester(streamer, candles)
    init_margin = backtester.status.total_margin()
    metadata = {
        "symbol": symbol,
        "interval": interval,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "params": params,
    }
    # 실험 목적 한 줄 라벨: uv run python core/examples/backtest.py "ATR 채널폭 2.0 검증"
    if len(sys.argv) > 1:
        metadata["label"] = sys.argv[1]
    report = backtester.run(metadata=metadata, save_series=True)

    # 4. 결과 출력
    print(f"Max Leverage: {report.max_leverage}")
    win_trades = 0
    lose_trades = 0
    for t in report.trades:
        if t.wnl > 0:
            win_trades += 1
        if t.wnl < 0:
            lose_trades += 1
    trades = win_trades + lose_trades
    print(f"Trade wins: {win_trades}/{trades} ({win_trades / trades * 100:.2f}%)")
    print(f"Profit: {(report.trades[-1].status.total_margin() / init_margin - 1) * 100:.2f}%")
