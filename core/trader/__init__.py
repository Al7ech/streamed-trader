"""
Trader package for the StreamedTrader project.

라이브/드라이런 실행에 필요한 부품들. 매매 순서 규약 자체는 :mod:`core.engine`에 있고,
여기 있는 것은 그 엔진에 꽂히는 **라이브 쪽 구현체**들이다:

- BinanceTrader: 부품 조립 + 수명주기 (소켓 연결, 기동/정지)
- LiveCandleProducer: kline 웹소켓 → 마감 캔들 (연속성 판정/구멍 백필/워밍업 페치 포함)
- LiveExecutor: 실제 주문 실행 + 거래소가 정답인 계좌 상태 (유저 데이터 스트림 처리)
- LiveRecorder: 실행 결과를 백테스트와 같은 포맷으로 적재 (체크포인트/재개)
- BinanceExecutor: 저수준 주문 클라이언트 (스레드풀 + 재시도)

드라이런은 ``LiveExecutor`` 대신 :class:`core.engine.executor.SimulatedExecutor` — 백테스트와
**같은 클래스** — 를 꽂는다.
"""

from .BinanceTrader import BinanceTrader
from .BinanceExecutor import BinanceExecutor
from .live_candle_producer import LiveCandleProducer
from .live_executor import LiveExecutor
from .live_recorder import LiveRecorder

__all__ = ['BinanceTrader', 'BinanceExecutor', 'LiveCandleProducer', 'LiveExecutor',
           'LiveRecorder']
