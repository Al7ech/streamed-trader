import sys
from datetime import datetime, timezone

from core.executor.simulated import DEFAULT_INIT_MARGIN
from core.producer.historical import BinanceHistoricalCandleProducer
from core.recorder.backtest import BacktestRecorder
from core.executor.simulated import SimulatedExecutor
from core.engine.engine import TradingEngine
from core.logging_config import setup_logging
from core.streamer.strategies.keltner_streamer import KeltnerStreamer

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

    producer = BinanceHistoricalCandleProducer(
        start_time=start_date, end_time=end_date, symbols=[symbol], interval=interval)

    # 2. 전략 생성 — 다른 전략을 돌리려면 여기만 바꾸면 된다 (streamer/strategies/ 참고)
    #    max_loss는 스탑 거리에서의 손실 한도이자 사실상 레버리지 손잡이다. 0.08처럼 크게
    #    잡으면 1m 캔들에서는 스탑 거리가 워낙 좁아 항상 6x 캡에 붙고, 왕복 수수료가
    #    자본의 ~0.5%씩 수백 번 나가면서 전략과 무관하게 계좌가 녹는다. 데모 기본값은
    #    레버리지가 ~1.6x에 머무는 값을 쓴다.
    params = dict(window=72 * 60, m_entry=4.0, m_exit=3.0, max_loss=0.005)
    streamer = KeltnerStreamer(symbols=[symbol], **params)

    # 3. 나머지 부품 조립 후 실행. 엔진에 넘기는 것은 스트리머/공급자/실행기/레코더 넷뿐이고,
    #    순서 규약과 실행·마무리는 전부 엔진이 갖는다.
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

    # 계좌 상태는 실행기가 만들어 소유한다 — 레코더에는 그 참조(executor.status)를 넘긴다.
    # fee_ratio를 안 넘기면 Status가 DEFAULT_FEE_RATIO를 박는다. 회계와 전략 사이징이 같은
    # 값을 보고, 런 JSON metadata의 fee_ratio도 그 하나에서 읽는다. 라이브는 거래소 티어를 쓴다.
    executor = SimulatedExecutor(init_margin)
    metadata["fee_ratio"] = executor.status.fee_ratio
    # metadata를 주면 런 JSON을, save_series까지 주면 시계열 샤드도 asset/backtest/ 에 쓴다.
    recorder = BacktestRecorder(streamer, executor.status, interval_ms=producer.interval_ms,
                                metadata=metadata, save_series=True)
    report = TradingEngine(streamer, producer, executor, recorder).run()

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
