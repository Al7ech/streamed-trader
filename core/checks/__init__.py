"""사실상의 테스트 스위트.

`pytest`/`unittest` 파일이 없는 저장소라, 정확성 검증은 이 스크립트들이 한다:

- :mod:`core.checks.live_check` — 라이브 경로 전체 (소켓과 거래소를 흉내 낸다). 캔들 공급자의
  연속성/백필 규칙, 라이브 실행기의 계좌/체결 처리, 그리고 **드라이런이 같은 캔들에 대한
  백테스트와 정확히 같은 체결을 내는지**를 단언한다.
- :mod:`core.checks.fetch_stock_check` — 미국 주식 페처 스모크 테스트 (``MASSIVE_API_KEY`` 필요).

검증 결과를 print로 내보내므로 기본 로그 레벨이 WARNING이다 (:mod:`core.logging_config` 참고).
"""
