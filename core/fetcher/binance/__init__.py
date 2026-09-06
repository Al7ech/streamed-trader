"""바이낸스 USD-M 선물 데이터 소스.

- :class:`~core.fetcher.binance.rest_fetcher.BinanceCandleFetcher` — REST klines를
  ≤1500개씩 끊어 받는다. 라이브 트레이더의 지표 프리피드가 쓴다.
- :class:`~core.fetcher.binance.vision_fetcher.BinanceVisionFetcher` —
  data.binance.vision의 월/일 zip을 대량 다운로드한다. 백테스트 진입점이 쓴다.
- :mod:`core.fetcher.binance.funding_fetcher` / :mod:`core.fetcher.binance.metrics_fetcher`
  — 가격이 아닌 입력 (펀딩비, OI, 롱숏 계정 비율). 캐시 방식은 같지만 ``Candle``이 아니라
  dict를 돌려준다.
"""
