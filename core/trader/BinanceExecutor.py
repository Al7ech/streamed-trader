"""
BinanceExecutor: Multi-threaded futures order execution module for the StreamedTrader project.

This module handles futures order execution on Binance with:
- GIL-free multi-threading using concurrent.futures
- Retry logic with exponential backoff
- Error handling and propagation to trader
- Support for various futures order types
- Leverage and position management
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, Union

from binance import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException

from core.streamer.action import Action


class OrderType(Enum):
    """Supported futures order types."""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"


class OrderSide(Enum):
    """Order side (buy/sell)."""
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class OrderRequest:
    """Futures order request data structure."""
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    reduce_only: bool = False
    close_position: bool = False  # Close position flag for futures
    activation_price: Optional[float] = None  # For conditional orders
    callback_rate: Optional[float] = None  # For trailing stop orders


@dataclass
class OrderResult:
    """Order execution result."""
    success: bool
    order_id: Optional[str] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None


class BinanceExecutor:
    """
    Multi-threaded order executor for Binance Futures trading.
    
    This class provides:
    - Non-blocking futures order execution using ThreadPoolExecutor
    - Automatic retry with exponential backoff
    - Error handling and callback notifications
    - Support for various futures order types and parameters
    - Leverage and position management features
    """

    def __init__(self,
                 api_key: str,
                 api_secret: str,
                 testnet: bool = True,
                 max_workers: int = 4,
                 max_retries: int = 2,
                 base_retry_delay: float = 0.1):
        """
        Initialize the BinanceExecutor for Futures trading.
        
        Args:
            api_key: Binance API key
            api_secret: Binance API secret
            testnet: Whether to use testnet (default: True)
            max_workers: Maximum number of worker threads
            max_retries: Maximum number of retry attempts
            base_retry_delay: Base delay for exponential backoff (seconds)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.base_retry_delay = base_retry_delay

        # Thread pool for order execution
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # Logging
        self.logger = logging.getLogger(__name__)

        # Binance Futures client (thread-safe)
        self.client = Client(
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet,
        )
        self.logger.info(f"Executor base url: {self.client.FUTURES_URL}")

        # Statistics
        self.total_orders = 0
        self.successful_orders = 0
        self.failed_orders = 0

    def execute_order(self, order_request: OrderRequest) -> Future[OrderResult]:
        """
        Execute an order asynchronously using thread pool.
        
        Args:
            order_request: Order request details
            
        Returns:
            Future object that will contain the OrderResult
        """
        future = self.executor.submit(self._execute_order_with_retry, order_request)
        return future

    def execute_action(self, action: Action, symbol: str) -> Future[OrderResult]:
        """
        Execute a trading action as a market order.
        
        Args:
            action: Trading action from streamer
            symbol: Trading symbol
            
        Returns:
            Future object that will contain the OrderResult
        """
        if action.quantity == 0:
            # No action needed
            result = OrderResult(success=True, error="No action required")
            future = Future()
            future.set_result(result)
            return future

        # Determine order side based on quantity
        side = OrderSide.BUY if action.quantity > 0 else OrderSide.SELL
        quantity = abs(action.quantity)

        order_request = OrderRequest(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=quantity
        )

        return self.execute_order(order_request)

    def _execute_order_with_retry(self, order_request: OrderRequest) -> OrderResult:
        """
        Execute order with retry logic and exponential backoff.
        
        Args:
            order_request: Order request details
            
        Returns:
            OrderResult with execution details
        """
        start_time = time.time()
        self.total_orders += 1
        result: Optional[OrderResult] = None

        for attempt in range(self.max_retries + 1):
            try:
                # Execute the order
                result = self._execute_single_order(order_request)
                execution_time = time.time() - start_time

                if result.success:
                    self.successful_orders += 1
                    result.execution_time = execution_time
                    self.logger.info(f"Order executed successfully: {result}")

                    return result
                else:
                    # Order failed, will retry
                    self.logger.warning(f"Order failed (attempt {attempt + 1}): {result}")

            except Exception as e:
                self.logger.error(f"Unexpected error during order execution (attempt {attempt + 1}): {e}")
                result = OrderResult(success=False, error=str(e))

            # Wait before retry (exponential backoff)
            if attempt < self.max_retries:
                delay = self.base_retry_delay * (2 ** attempt)
                self.logger.info(f"Retrying order in {delay} seconds...")
                time.sleep(delay)

        # All retries failed.
        # 마지막 시도의 실제 오류(바이낸스 코드/메시지)를 함께 싣는다 — 예전엔 덮어써서
        # 영구 거부된 주문의 원인이 어디에도 남지 않았다.
        execution_time = time.time() - start_time
        self.failed_orders += 1
        last_error = getattr(result, "error", None) if result is not None else None
        final_result = OrderResult(
            success=False,
            error=f"Order failed after {self.max_retries + 1} attempts: {last_error}",
            execution_time=execution_time
        )

        self.logger.error(f"Order execution failed permanently: {final_result.error}")

        return final_result

    def _execute_single_order(self, order_request: OrderRequest) -> OrderResult:
        """
        Execute a single order on Binance.
        
        Args:
            order_request: Order request details
            
        Returns:
            OrderResult with execution details
        """
        try:
            # Prepare order parameters
            order_params = {
                'symbol': order_request.symbol,
                'side': order_request.side.value,
                'type': order_request.order_type.value,
                'quantity': order_request.quantity
            }

            # Add price for limit orders
            if order_request.order_type in [OrderType.LIMIT, OrderType.TAKE_PROFIT]:
                if order_request.price is None:
                    return OrderResult(success=False, error="Price required for limit orders")
                order_params['price'] = order_request.price

            # Add stop price for conditional orders
            if order_request.order_type in [OrderType.STOP, OrderType.STOP_MARKET,
                                            OrderType.TAKE_PROFIT, OrderType.TAKE_PROFIT_MARKET]:
                if order_request.stop_price is None:
                    return OrderResult(success=False, error="Stop price required for conditional orders")
                order_params['stopPrice'] = order_request.stop_price

            # Add activation price for conditional orders
            if order_request.activation_price is not None:
                order_params['activationPrice'] = order_request.activation_price

            # Add callback rate for trailing stop orders
            if order_request.order_type == OrderType.TRAILING_STOP_MARKET:
                if order_request.callback_rate is None:
                    return OrderResult(success=False, error="Callback rate required for trailing stop orders")
                order_params['callbackRate'] = order_request.callback_rate

            # Add reduce only flag for futures
            if order_request.reduce_only:
                order_params['reduceOnly'] = True

            # Add close position flag for futures
            if order_request.close_position:
                order_params['closePosition'] = True

            # Execute futures order
            self.logger.info(f"sending futures order with {order_params}")
            response = self.client.futures_create_order(**order_params)

            return OrderResult(
                success=True,
                order_id=str(response.get('orderId', '')),
                error=None
            )

        except BinanceOrderException as e:
            return OrderResult(success=False, error=f"Order error: {e}")
        except BinanceAPIException as e:
            return OrderResult(success=False, error=f"API error: {e}")
        except Exception as e:
            return OrderResult(success=False, error=f"Unexpected error: {e}")

    def cancel_order(self, symbol: str, order_id: Union[str, int]) -> Future[OrderResult]:
        """
        Cancel an existing order.
        
        Args:
            symbol: Trading symbol
            order_id: Order ID to cancel
            
        Returns:
            Future object that will contain the OrderResult
        """
        future = self.executor.submit(self._cancel_single_order, symbol, order_id)
        return future

    def _cancel_single_order(self, symbol: str, order_id: Union[str, int]) -> OrderResult:
        """Cancel a single order."""
        try:
            response = self.client.futures_cancel_order(symbol=symbol, orderId=order_id)
            return OrderResult(
                success=True,
                order_id=str(order_id),
                error=None
            )
        except BinanceAPIException as e:
            return OrderResult(success=False, error=f"Cancel error: {e}")
        except Exception as e:
            return OrderResult(success=False, error=f"Unexpected cancel error: {e}")

    def get_order_status(self, symbol: str, order_id: Union[str, int]) -> Future[Dict[str, Any]]:
        """
        Get order status.
        
        Args:
            symbol: Trading symbol
            order_id: Order ID
            
        Returns:
            Future object that will contain order status
        """
        future = self.executor.submit(self._get_single_order_status, symbol, order_id)
        return future

    def _get_single_order_status(self, symbol: str, order_id: Union[str, int]) -> Dict[str, Any]:
        """Get status of a single order."""
        try:
            response = self.client.futures_get_order(symbol=symbol, orderId=order_id)
            return response
        except BinanceAPIException as e:
            return {'error': f"API error: {e}"}
        except Exception as e:
            return {'error': f"Unexpected error: {e}"}

    def get_statistics(self) -> Dict[str, Any]:
        """Get execution statistics."""
        return {
            'total_orders': self.total_orders,
            'successful_orders': self.successful_orders,
            'failed_orders': self.failed_orders,
            'success_rate': self.successful_orders / max(self.total_orders, 1) * 100
        }

    def get_futures_account_info(self) -> Future[Dict[str, Any]]:
        """
        Get futures account information.
        
        Returns:
            Future object that will contain futures account info
        """
        future = self.executor.submit(self._get_futures_account_info)
        return future

    def _get_futures_account_info(self) -> Dict[str, Any]:
        """Get futures account information."""
        try:
            response = self.client.futures_account()
            return response
        except BinanceAPIException as e:
            return {'error': f"API error: {e}"}
        except Exception as e:
            return {'error': f"Unexpected error: {e}"}

    def get_futures_position_info(self, symbol: Optional[str] = None) -> Future[Dict[str, Any]]:
        """
        Get futures position information.
        
        Args:
            symbol: Optional symbol to get position for specific pair
            
        Returns:
            Future object that will contain position info
        """
        future = self.executor.submit(self._get_futures_position_info, symbol)
        return future

    def _get_futures_position_info(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get futures position information."""
        try:
            if symbol:
                response = self.client.futures_position_information(symbol=symbol)
            else:
                response = self.client.futures_position_information()
            return response
        except BinanceAPIException as e:
            return {'error': f"API error: {e}"}
        except Exception as e:
            return {'error': f"Unexpected error: {e}"}

    def shutdown(self, wait: bool = True):
        """Shutdown the executor and close connections."""
        self.logger.info("Shutting down BinanceExecutor...")
        self.executor.shutdown(wait=wait)

        # Close client connection
        try:
            self.client.close_connection()
        except Exception as e:
            self.logger.error(f"Error closing client connection: {e}")

        self.logger.info("BinanceExecutor shutdown complete")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()
