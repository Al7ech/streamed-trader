"""
BinanceOrderClient: Multi-threaded futures order execution module for the StreamedTrader project.

This is the **low-level REST order client** that :class:`~core.live.executor.LiveExecutor`
delegates to. It deliberately does *not* implement the :class:`~core.engine.executor.Executor`
port — it knows nothing about ``Status``, events or the trading engine, only how to put an
order on the exchange and retry it. (It was named ``BinanceExecutor`` until 2026-09, which read
as if it belonged to the ``Executor``/``LiveExecutor`` family; it never did.)

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

from core.domain.action import Action, ActionType


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
    #: LIMIT 계열에 필수. Binance는 timeInForce 없는 LIMIT 주문을 거부한다.
    time_in_force: Optional[str] = None
    #: newClientOrderId. 체결 이벤트를 자기 결정과 짝짓고, 나중에 이 id로 취소하기 위해 쓴다.
    client_order_id: Optional[str] = None


@dataclass
class OrderResult:
    """Order execution result."""
    success: bool
    order_id: Optional[str] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None


class BinanceOrderClient:
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
        Initialize the BinanceOrderClient for Futures trading.
        
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

    #: 엔진의 ActionType을 거래소 주문 타입으로 옮기는 표. STOP_MARKET은 트리거가 현재가의
    #: 어느 쪽에 있느냐에 따라 갈리므로 여기 없고 execute_action이 따로 고른다.
    _ORDER_TYPES = {
        ActionType.MARKET: OrderType.MARKET,
        ActionType.LIMIT: OrderType.LIMIT,
    }

    def execute_action(self, action: Action,
                       client_order_id: Optional[str] = None,
                       reference_price: Optional[float] = None) -> Future[OrderResult]:
        """
        Execute a trading action. ``action.symbol``이 대상 심볼이다 — Action이 자기 심볼을
        들고 다니므로 별도 symbol 인자를 받지 않는다.

        Args:
            action: Trading action from streamer
            client_order_id: 이 주문에 붙일 newClientOrderId. 호출자가 체결 이벤트를 자기
                결정과 짝짓고 나중에 취소하는 데 쓴다.
            reference_price: STOP_MARKET을 STOP_MARKET / TAKE_PROFIT_MARKET 중 어느 쪽으로
                보낼지 정하는 기준가 (보통 최근 종가). 거래소는 트리거가 현재가의 반대쪽에
                있는 조건부 주문을 거부하므로, ``action.trigger_above``와 이 값으로 맞는 쪽을
                고른다. None이면 trigger_above만 보고 정한다.

        Returns:
            Future object that will contain the OrderResult
        """
        if action.order_type is ActionType.CANCEL:
            return self.cancel_order(action.symbol, orig_client_order_id=action.client_id)

        if action.quantity == 0:
            # No action needed
            result = OrderResult(success=True, error="No action required")
            future = Future()
            future.set_result(result)
            return future

        # Determine order side based on quantity
        side = OrderSide.BUY if action.quantity > 0 else OrderSide.SELL
        quantity = abs(action.quantity)

        if action.order_type is ActionType.STOP_MARKET:
            trigger_above = action.trigger_above
            if trigger_above is None and reference_price is not None:
                trigger_above = action.trigger_price >= reference_price
            # 매수 주문의 트리거가 위에 있으면 돌파 매수(STOP), 아래면 익절 매수
            # (TAKE_PROFIT). 매도는 대칭이다.
            takes_profit = (action.quantity > 0) != bool(trigger_above)
            order_type = (OrderType.TAKE_PROFIT_MARKET if takes_profit
                          else OrderType.STOP_MARKET)
        else:
            order_type = self._ORDER_TYPES.get(action.order_type)
            if order_type is None:
                result = OrderResult(
                    success=False, error=f"Unsupported order type: {action.order_type}")
                future = Future()
                future.set_result(result)
                return future

        order_request = OrderRequest(
            symbol=action.symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=action.price,
            stop_price=action.trigger_price,
            reduce_only=action.reduce_only,
            client_order_id=client_order_id or action.client_id,
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
                    # 체결의 실체(수량/평균가/손익)는 ORDER_TRADE_UPDATE를 받는
                    # BinanceTrader._process_order_trade_update가 "order filled: [...]"로
                    # 남긴다. 여기서 남길 수 있는 건 주문 id와 왕복 시간뿐이라 DEBUG로 둔다.
                    self.logger.debug(f"Order accepted: {result}")

                    return result
                else:
                    # Order failed, will retry
                    self.logger.warning(f"Order failed (attempt {attempt + 1}): {result}")

            except Exception as e:
                self.logger.error(f"Unexpected error during order execution (attempt {attempt + 1}): {e}")
                result = OrderResult(success=False, error=str(e))

            # Wait before retry (exponential backoff)
            if attempt < self.max_retries:
                # 기본 backoff가 0.1/0.2초라 사람이 읽을 이유가 없다. 재시도가 있었다는
                # 사실은 바로 위 "Order failed (attempt N)" 경고가 이미 남긴다.
                delay = self.base_retry_delay * (2 ** attempt)
                self.logger.debug(f"Retrying order in {delay} seconds...")
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
                # timeInForce 없는 LIMIT은 거래소가 거부한다. 이 엔진의 미체결 주문은
                # 체결되거나 명시적으로 취소될 때까지 사는 GTC가 기본이다.
                order_params['timeInForce'] = order_request.time_in_force or 'GTC'

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

            if order_request.client_order_id:
                order_params['newClientOrderId'] = order_request.client_order_id

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

    def cancel_order(self, symbol: str, order_id: Optional[Union[str, int]] = None,
                     orig_client_order_id: Optional[str] = None) -> Future[OrderResult]:
        """
        Cancel an existing order.

        Args:
            symbol: Trading symbol
            order_id: 거래소가 매긴 주문 ID
            orig_client_order_id: 주문을 낼 때 붙인 newClientOrderId. 전략은 이쪽으로
                취소한다 — 거래소 ID는 주문을 낸 뒤에야 알 수 있어서 전략이 들고 있을 수 없다.

        둘 다 None이면 그 심볼의 **미체결 주문을 전부** 취소한다
        (``Action.cancel(symbol)``의 라이브 대응).

        Returns:
            Future object that will contain the OrderResult
        """
        future = self.executor.submit(self._cancel_single_order, symbol, order_id,
                                      orig_client_order_id)
        return future

    def _cancel_single_order(self, symbol: str, order_id: Optional[Union[str, int]] = None,
                             orig_client_order_id: Optional[str] = None) -> OrderResult:
        """Cancel a single order, or every open order on the symbol."""
        try:
            if order_id is None and orig_client_order_id is None:
                self.client.futures_cancel_all_open_orders(symbol=symbol)
                return OrderResult(success=True, order_id=None, error=None)
            if orig_client_order_id is not None:
                self.client.futures_cancel_order(symbol=symbol,
                                                 origClientOrderId=orig_client_order_id)
                return OrderResult(success=True, order_id=orig_client_order_id, error=None)
            self.client.futures_cancel_order(symbol=symbol, orderId=order_id)
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
        self.logger.info("Shutting down BinanceOrderClient...")
        self.executor.shutdown(wait=wait)

        # Close client connection
        try:
            self.client.close_connection()
        except Exception as e:
            self.logger.error(f"Error closing client connection: {e}")

        self.logger.info("BinanceOrderClient shutdown complete")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()
