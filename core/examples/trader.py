"""
Example usage of the BinanceTrader and BinanceExecutor modules.

This file demonstrates how to integrate the trader components with a streamer
for live trading operations.
"""

import asyncio
import logging
import os

from dotenv import load_dotenv

from core.streamer.keltner_streamer import KeltnerStreamer
from core.trader.BinanceTrader import BinanceTrader


async def main():
    """Main function demonstrating trader usage."""
    # Load environment variables from .env file
    load_dotenv()

    # Configure logging
    logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s] %(name)s:%(lineno)d %(message)s")
    logging.getLogger("websockets.client").setLevel(logging.INFO)
    logging.getLogger("binance.ws.reconnecting_websocket").setLevel(logging.INFO)

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

    # Initialize the strategy. Swap KeltnerStreamer for any other BaseStreamer here — the
    # trader only needs `decide_action` and the `indicators` dict to prefeed.
    # `params` is built once and handed to both the streamer and the run metadata, so the
    # recorded params block has the same shape backtest.py writes.
    params = dict(
        window=int(os.getenv("WINDOW", 72 * 60)),
        m_entry=float(os.getenv("M_ENTRY", 4.0)),
        m_exit=float(os.getenv("M_EXIT", 3.0)),
        max_loss=float(os.getenv("MAX_LOSS", 0.005)),
    )
    streamer = KeltnerStreamer(SYMBOLS, **params)

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
    )

    try:
        # Start the trader
        print("Starting BinanceTrader...")
        await trader.start()

        # Keep running. is_running을 봐야 리스너가 치명적 오류로 stop()을 부른 뒤 프로세스가
        # 빠져나온다 (docker의 restart: always가 재기동할 수 있게).
        print("Trader is running. Press Ctrl+C to stop.")
        while trader.is_running:
            await asyncio.sleep(1)
        print("Trader is no longer running.")

    except KeyboardInterrupt:
        print("\nStopping trader...")
    finally:
        # Cleanup
        await trader.stop()
        print("Trader stopped.")


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()
