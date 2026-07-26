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

    SYMBOL = os.getenv("SYMBOL", None)
    INTERVAL = os.getenv("INTERVAL", None)

    if not SYMBOL or not INTERVAL:
        raise ValueError("SYMBOL and INTERVAL must be set in environment variables or .env file")

    # Initialize the strategy. Swap KeltnerStreamer for any other BaseStreamer here — the
    # trader only needs `decide_action` and the `indicators` dict to prefeed.
    window = int(os.getenv("WINDOW", 72 * 60))
    m_entry = float(os.getenv("M_ENTRY", 4.0))
    m_exit = float(os.getenv("M_EXIT", 3.0))
    max_loss = float(os.getenv("MAX_LOSS", 0.005))
    streamer = KeltnerStreamer(SYMBOL, window, m_entry, m_exit, max_loss)

    # Create trader and executor
    trader = BinanceTrader(
        api_key=API_KEY,
        api_secret=API_SECRET,
        symbol=SYMBOL,
        interval=INTERVAL,
        streamer=streamer,
        dry_run=DRY_RUN,
        testnet=TESTNET
    )

    try:
        # Start the trader
        print("Starting BinanceTrader...")
        await trader.start()

        # Keep running
        print("Trader is running. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(1)

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
