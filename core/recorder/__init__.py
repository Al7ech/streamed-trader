"""실행 결과 적재 포트 — ABC(+NullRecorder)는 여기, 구현은 서브모듈에.

- :class:`~core.recorder.base.Recorder` — 포트 ABC, :class:`~core.recorder.base.NullRecorder` —
  no-op 구현
- :class:`~core.recorder.full.FullRecorder` — 메모리에 모아 ``Report``를 만들고,
  ``metadata``를 받았으면 런 JSON/시계열 샤드까지 쓴다
- :class:`~core.recorder.simple.SimpleRecorder` — 체결·자본곡선·최대 레버리지만 모으는 가벼운
  백테스트 레코더 (buy & hold 기준선과 파일 산출물 없음)
- :class:`~core.recorder.live.LiveRecorder` — 주기적 체크포인트 + 재기동 시 이어붙이기,
  백테스트와 같은 출력 포맷

구현 서브모듈은 여기서 eager import하지 않는다.
"""

from core.recorder.base import NullRecorder, Recorder

__all__ = ["NullRecorder", "Recorder"]
