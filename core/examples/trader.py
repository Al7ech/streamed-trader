"""
Example usage of the BinanceTrader and BinanceOrderClient modules.

This file demonstrates how to integrate the trader components with a streamer
for live trading operations.
"""

import asyncio
import logging
import os

from dotenv import load_dotenv

from core.trader.trader import DEFAULT_DECIDE_DEADLINE_S, BinanceTrader
from core.logging_config import setup_logging
from core.streamer.strategies.ryul_streamer_edit2 import RyulStreamer_edit2

logger = logging.getLogger(__name__)


async def main():
    """Main function demonstrating trader usage."""
    # Load environment variables from .env file
    load_dotenv()

    # Configure logging. LOG_LEVEL은 이름(DEBUG/INFO/...) 또는 숫자 모두 허용.
    setup_logging()

    # Configuration from environment variables
    API_KEY = os.getenv("API_KEY")
    API_SECRET = os.getenv("API_SECRET")
    TESTNET = os.getenv("TESTNET", "false").lower() == "true"
    DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

    # Validate required environment variables
    if not API_KEY or not API_SECRET:
        raise ValueError("API_KEY and API_SECRET must be set in environment variables or .env file")

    SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()]
    INTERVAL = os.getenv("INTERVAL", None)

    if not SYMBOLS or not INTERVAL:
        raise ValueError("SYMBOLS and INTERVAL must be set in environment variables or .env file")

    # Result recording. Writes to <RESULT_PATH>/live/ in the same format the backtester
    # produces, so a live run and a backtest can be compared in the visualiser.
    RECORD = os.getenv("RECORD", "true").lower() == "true"
    RESULT_PATH = os.getenv("RESULT_PATH", "asset/")
    RUN_ID = os.getenv("LIVE_RUN_ID") or None
    SHARD_FLUSH_EVERY = int(os.getenv("LIVE_SHARD_FLUSH_EVERY", 60))

    # Live-only decision deadline (seconds after the candle close). A candle processed later
    # than this still reaches the strategy, but market orders that would grow a position are
    # dropped. 0 disables it. Dry run ignores it.
    DECIDE_DEADLINE_S = float(os.getenv("DECIDE_DEADLINE_S", DEFAULT_DECIDE_DEADLINE_S))

    # Initialize the strategy. Swap RyulStreamer_edit2 for any other BaseStreamer here — the
    # trader only needs `decide_action` and the `indicators` dict to prefeed.
    # `params` is built once and handed to both the streamer and the run metadata, so the
    # recorded params block has the same shape backtest.py writes.
    # Defaults are the ETH-optimal "edit2" set (entry_length=4500, win_exit_length=360,
    # lose_exit_length=180, max_loss=0.08, atr_length=1440, max_channel_pct=6.0,
    # max_atr_pct=0.11, atr_slope_lookback=1, min_defer_move_pct=3.0) — validated over
    # ETHUSDT 1m 2020-01-01~2026-07-24: profit 9009.03%, Sharpe 1.9258, MDD 28.21%, 727 fills
    # (docs/2609-backtest-profiling.md; core/streamer/strategies/ryul_streamer_edit2.py).
    params = dict(
        entry_length=int(os.getenv("ENTRY_LENGTH", 75 * 60)),
        win_exit_length=int(os.getenv("WIN_EXIT_LENGTH", 6 * 60)),
        lose_exit_length=int(os.getenv("LOSE_EXIT_LENGTH", 3 * 60)),
        max_loss=float(os.getenv("MAX_LOSS", 0.08)),
        atr_length=int(os.getenv("ATR_LENGTH", 24 * 60)),
        max_channel_pct=float(os.getenv("MAX_CHANNEL_PCT", 6.0)),
        max_atr_pct=float(os.getenv("MAX_ATR_PCT", 0.11)),
        atr_slope_lookback=int(os.getenv("ATR_SLOPE_LOOKBACK", 1)),
        min_defer_move_pct=float(os.getenv("MIN_DEFER_MOVE_PCT", 3.0)),
    )
    streamer = RyulStreamer_edit2(SYMBOLS, **params)

    # Create trader and executor. Traded symbols come from streamer.symbols — no separate
    # symbol/symbols argument here.
    trader = BinanceTrader(
        api_key=API_KEY,
        api_secret=API_SECRET,
        interval=INTERVAL,
        streamer=streamer,
        dry_run=DRY_RUN,
        testnet=TESTNET,
        record=RECORD,
        result_path=RESULT_PATH,
        run_id=RUN_ID,
        run_metadata={"params": params},
        shard_flush_every=SHARD_FLUSH_EVERY,
        decide_deadline_s=DECIDE_DEADLINE_S,
    )

    try:
        # Start the trader.
        # 프로세스 생명주기는 로그 스트림에 남아야 한다 — print로 stdout에 흘리면 도커
        # 로그에서 레벨도 타임스탬프도 없이 로그 사이에 섞여, 정작 "언제 죽고 언제 살아났나"를
        # 사후에 읽을 수 없다.
        logger.info("Starting BinanceTrader...")
        await trader.start()

        # Keep running. is_running을 봐야 리스너가 치명적 오류로 stop()을 부른 뒤 프로세스가
        # 빠져나온다 (docker의 restart: always가 재기동할 수 있게).
        logger.info("Trader is running. Press Ctrl+C to stop.")
        while trader.is_running:
            await asyncio.sleep(1)
        logger.warning("Trader is no longer running.")

    except KeyboardInterrupt:
        logger.info("Stopping trader...")
    finally:
        # Cleanup
        await trader.stop()
        logger.info("Trader stopped.")


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()
