"""진입점들이 공유하는 로깅 설정.

예전에는 ``logging.basicConfig``가 ``core/examples/trader.py`` 한 곳에만 있었다. 그래서
백테스트와 체크 스크립트에서는 페처/백테스터의 ``logger.info``가 통째로 사라졌고,
``WARNING`` 이상은 파이썬의 ``logging.lastResort`` 핸들러로 빠져 **레벨명도 로거명도
타임스탬프도 없는 맨 메시지**만 stderr에 찍혔다. 결과의 신뢰도를 좌우하는 두 경고 —
월 청크의 데이터 구멍(:mod:`core.candle_fetcher.base`)과 파산 지점에서 잘린
Sharpe(:mod:`core.backtest.metrics`) — 가 하필 가장 알아보기 어려운 형태로 나왔다.

모든 진입점(``core/examples/*.py``, ``core/*_check.py``)은 다른 일을 하기 전에
:func:`setup_logging`을 부른다.
"""

import logging
import os
import sys

#: LOG_LEVEL이 없을 때의 기본 레벨. 라이브 트레이더는 도커에서 몇 주씩 사는 프로세스라
#: DEBUG를 기본으로 두면 1분봉 기준 하루 수천 줄이 쌓인다.
DEFAULT_LEVEL = "INFO"

#: asctime이 없으면 ``restart: always``로 죽고 살아나길 반복하는 라이브 프로세스의 로그를
#: 사후에 읽을 수 없다. lineno는 코드가 바뀔 때마다 달라져 grep 패턴을 깨뜨리므로 넣지 않는다.
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

#: 우리 로그보다 훨씬 시끄러운 서드파티 로거들. 루트를 DEBUG로 내려도 이쪽은 INFO에 묶는다.
_NOISY_LOGGERS = ("websockets.client", "binance.ws.reconnecting_websocket")


def _resolve_level(level, default) -> object:
    """레벨 이름(DEBUG/INFO/...)과 숫자 문자열을 모두 받아 logging이 아는 형태로 만든다."""
    if level is None:
        level = os.getenv("LOG_LEVEL") or default
    if isinstance(level, str):
        level = level.strip().upper()
        if level.isdigit():
            return int(level)
    return level


def setup_logging(level=None, *, default: str = DEFAULT_LEVEL,
                  quiet_third_party: bool = True) -> logging.Logger:
    """루트 로거를 설정하고 돌려준다.

    :param level: 로그 레벨을 못박는다. 이름과 숫자를 모두 받는다. None이면
        ``LOG_LEVEL`` 환경변수를 본다.
    :param default: ``LOG_LEVEL``이 없을 때 쓸 레벨. 진입점마다 성격이 달라서
        (체크 스크립트는 판정 결과가 본론이라 조용한 편이 낫다) 열어 둔다. 사용자가
        ``LOG_LEVEL``을 지정했다면 언제나 그쪽이 이긴다.
    :param quiet_third_party: True면 :data:`_NOISY_LOGGERS`를 INFO로 묶는다.

    ``force=True``를 쓰는 이유: 한 프로세스에서 두 번 불리거나 (임포트 부수효과로) 어딘가
    핸들러를 이미 붙여 놓았을 때, basicConfig가 조용히 아무것도 하지 않고 빠져나가면
    포맷이 적용되지 않은 채로 돌아간다.
    """
    logging.basicConfig(level=_resolve_level(level, default), format=LOG_FORMAT,
                        datefmt=DATE_FORMAT, stream=sys.stderr, force=True)
    if quiet_third_party:
        for name in _NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.INFO)
    return logging.getLogger()
