"""예제 전략들 — **프레임워크의 사용 예시이지, 튜닝되거나 권장되는 전략이 아니다.**

새 전략을 쓸 때 참고할 순서:

1. :mod:`~core.streamer.strategies.cross_moving_average_streamer` — 가장 단순하다. 먼저 읽어라.
2. :mod:`~core.streamer.strategies.keltner_stop_streamer` — 진짜 ``STOP_MARKET`` 미체결 주문을
   매 봉 재무장(취소 + 재등록)하는 참조 예시.

나머지 다섯(``wick_rejection``, ``momentum_time_exit``, ``mean_reversion_zscore``,
``trendline_bounce``, ``keltner_streamer``)은 손절을 손으로 흉내 낸다 — 마감된 봉의
``low``/``high``를 저장된 스탑과 비교한 뒤 그 봉의 **종가**에 체결한다. 미체결 주문이
없애주는 바로 그 가격 오차이고, 옛 스타일의 illustration으로 일부러 남겨 두었다.

``trade_streamer``는 매 봉 롱/숏을 번갈아 내는 fixture이지 전략이 아니다.
"""
