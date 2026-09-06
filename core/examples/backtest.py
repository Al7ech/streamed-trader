import sys
from datetime import datetime, timezone

from core.engine.backtest import DEFAULT_INIT_MARGIN, run_backtest
from core.engine.binance_backtest_candle_producer import BinanceBacktestCandleProducer
from core.logging_config import setup_logging
from core.streamer.keltner_streamer import KeltnerStreamer

if __name__ == "__main__":
    # 0. 로깅 설정. 이게 없으면 페처/백테스터의 경고가 lastResort 핸들러로 빠져
    #    레벨명 없는 맨 줄로 tqdm 진행바 사이에 섞인다 (월 청크 데이터 구멍 경고 등).
    setup_logging()

    # 1. 캔들 공급자 생성. producer가 내부에서 data.binance.vision에서 구간을 받아
    #    asset/candle/ 에 캐시하고 병합까지 한다 (없는 달만 새로 받는다).
    symbol = "ETHUSDT"
    start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2026, 7, 10, tzinfo=timezone.utc)
    interval = "1m"

    producer = BinanceBacktestCandleProducer(
        start_time=start_date, end_time=end_date, symbols=[symbol], interval=interval)

    # 2. 전략 생성 — 다른 전략을 돌리려면 여기만 바꾸면 된다 (streamer/ 참고)
    #    max_loss는 스탑 거리에서의 손실 한도이자 사실상 레버리지 손잡이다. 0.08처럼 크게
    #    잡으면 1m 캔들에서는 스탑 거리가 워낙 좁아 항상 6x 캡에 붙고, 왕복 수수료가
    #    자본의 ~0.5%씩 수백 번 나가면서 전략과 무관하게 계좌가 녹는다. 데모 기본값은
    #    레버리지가 ~1.6x에 머무는 값을 쓴다.
    params = dict(window=72 * 60, m_entry=4.0, m_exit=3.0,
                  fee_ratio=0.0004, max_loss=0.005)
    streamer = KeltnerStreamer(symbols=[symbol], **params)

    # 3. 백테스트 실행. producer를 직접 넘긴다 (candles_by_symbol 경로는 합성 캔들/비-바이낸스
    #    소스 검사용으로 여전히 열려 있다).
    init_margin = DEFAULT_INIT_MARGIN
    metadata = {
        "symbols": [symbol],
        "interval": interval,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "params": params,
    }
    # 실험 목적 한 줄 라벨: uv run python core/examples/backtest.py "ATR 채널폭 2.0 검증"
    if len(sys.argv) > 1:
        metadata["label"] = sys.argv[1]
    report = run_backtest(streamer, producer=producer, metadata=metadata, save_series=True)

    # 4. 결과 출력
    print(f"Max Leverage: {report.max_leverage}")
    # 런 JSON의 summary와 같은 규칙: 실현이 일어난 체결만, 수수료 차감 후로 승패를 가른다.
    closes = [t for t in report.trades if t.status.position_for(t.symbol).position * t.quantity < 0]
    win_trades = sum(1 for t in closes if t.wnl - t.fee > 0)
    lose_trades = sum(1 for t in closes if t.wnl - t.fee < 0)
    trades = win_trades + lose_trades
    win_pct = win_trades / trades * 100 if trades else 0.0
    print(f"Trade wins: {win_trades}/{trades} ({win_pct:.2f}%)")
    # Trade.status는 **거래 전** 스냅샷이라 마지막 거래의 손익/수수료가 빠진다. 최종 상태를 쓴다.
    print(f"Profit: {(report.status.total_margin() / init_margin - 1) * 100:.2f}%")
