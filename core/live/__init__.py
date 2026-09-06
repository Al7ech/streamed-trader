"""라이브/드라이런 실행에 필요한 부품들.

매매 순서 규약 자체는 :mod:`core.engine`에 있고, 여기 있는 것은 그 엔진에 꽂히는 **라이브 쪽
구현체**들이다:

- :class:`~core.live.trader.BinanceTrader` — 부품 조립 + 수명주기 (소켓 연결, 기동/정지)
- :class:`~core.live.candle_producer.LiveCandleProducer` — kline 웹소켓 → 마감 캔들
  (연속성 판정/구멍 백필/워밍업 페치 포함)
- :class:`~core.live.executor.LiveExecutor` — 실제 주문 실행 + 거래소가 정답인 계좌 상태
  (유저 데이터 스트림 처리)
- :class:`~core.live.recorder.LiveRecorder` — 실행 결과를 백테스트와 같은 포맷으로 적재
  (체크포인트/재개)
- :class:`~core.live.binance_order_client.BinanceOrderClient` — 저수준 REST 주문 클라이언트
  (스레드풀 + 재시도). ``Executor`` 포트를 구현하지 **않는다** — ``LiveExecutor``가 위임하는
  대상일 뿐이다.

드라이런은 ``LiveExecutor`` 대신 :class:`core.backtest.simulated_executor.SimulatedExecutor`
— 백테스트와 **같은 클래스** — 를 꽂는다. 그래서 이 패키지가 :mod:`core.backtest`를 import한다.
"""

from core.live.binance_order_client import BinanceOrderClient
from core.live.candle_producer import LiveCandleProducer
from core.live.executor import LiveExecutor
from core.live.recorder import LiveRecorder
from core.live.trader import BinanceTrader

__all__ = ['BinanceTrader', 'BinanceOrderClient', 'LiveCandleProducer', 'LiveExecutor',
           'LiveRecorder']
