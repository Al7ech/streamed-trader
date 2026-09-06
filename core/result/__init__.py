"""실행 결과 산출물 — 백테스트와 라이브가 **공유**한다.

레코더가 모은 것을 프론트엔드(:file:`visualise/`)가 읽는 JSON 포맷으로 내보내는 계층.
어느 한쪽 모드에 속하지 않으므로 :mod:`core.backtest`에도 :mod:`core.live`에도 두지 않는다.

- :mod:`core.result.writer` — 런 JSON + 월별 시계열 샤드 (``SCHEMA_VERSION``, ``ShardWriter``)
- :mod:`core.result.metrics` — Sharpe, MDD, buy & hold 기준선
- :mod:`core.result.indicator_columns` — 스트리머에서 컬럼 이름/``scale_group`` 추출
"""
