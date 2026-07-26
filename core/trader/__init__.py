"""
Trader package for the StreamedTrader project.

This package contains modules for real-time trading operations including:
- BinanceTrader: WebSocket-based real-time candle streaming and streamer integration
- BinanceExecutor: Multi-threaded order execution with retry logic
"""

from .BinanceTrader import BinanceTrader
from .BinanceExecutor import BinanceExecutor

__all__ = ['BinanceTrader', 'BinanceExecutor']



