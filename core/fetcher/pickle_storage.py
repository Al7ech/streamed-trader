import io
import logging
import os
import pickle
from datetime import datetime
from typing import List

from core.domain import Candle

_logger = logging.getLogger(__name__)


class _CompatUnpickler(pickle.Unpickler):
    """옮겨간 클래스 경로를 되짚어 옛 캐시를 계속 읽는다.

    pickle은 클래스를 ``(모듈 경로, 이름)`` 문자열로 저장한다. 그래서 클래스를 다른 모듈로
    옮기면 **이미 캐시된 월 청크 전부**가 ``ModuleNotFoundError``로 죽고, 수백 GB가 될 수도
    있는 캔들을 다시 받아야 한다. 여기서 옛 경로를 새 클래스로 이어붙인다.

    ``Candle``이 이미 "옛 pickle에는 이 필드가 없다 → 클래스 속성 기본값으로 폴백"이라는
    같은 성격의 하위호환 장치를 갖고 있다. 새로 저장되는 청크는 새 경로로 기록되므로, 이
    표는 옛 파일이 소진될 때까지만 의미가 있다.
    """

    #: (옛 모듈 경로, 클래스 이름) -> 지금 클래스
    _MOVED = {
        ("core.streamer.candle", "Candle"): Candle,  # 2026-09: core.domain.candle 로 이전
    }

    def find_class(self, module: str, name: str):
        moved = self._MOVED.get((module, name))
        if moved is not None:
            return moved
        return super().find_class(module, name)


class PickleStorage:
    @staticmethod
    def save_to_pickle(candles: List[Candle], file_path: str, protocol: int = 5) -> None:
        """
        Candle 객체 리스트를 pickle 파일로 저장합니다. Protocol 5와 버퍼드 I/O를 사용합니다.
        :param candles: Candle 객체 리스트
        :param file_path: 저장할 파일 경로 (예: 'candle.pkl')
        :param protocol: pickle 프로토콜 버전 (기본값 5)
        """
        # 보장: 상위 디렉토리 생성
        os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

        # 임시 파일에 쓴 뒤 교체 — 중단되어도 손상된 파일이 남지 않음
        tmp_path = file_path + ".tmp"
        with open(tmp_path, "wb", buffering=io.DEFAULT_BUFFER_SIZE) as f:
            pickle.dump(candles, f, protocol=protocol)
        os.replace(tmp_path, file_path)

    @staticmethod
    def load_from_pickle(file_path: str, verbose: bool = True) -> List[Candle]:
        """
        pickle 파일에서 Candle 객체 리스트를 불러옵니다. 버퍼드 I/O를 사용합니다.
        :param file_path: 파일 경로 (예: 'candle.pkl')
        :param verbose: 로드 시간 출력 여부 (월 청크처럼 여러 파일을 읽을 때는 False로 두고 호출부에서 합산 출력)
        :return: Candle 객체 리스트
        """
        start_time = datetime.now()
        with open(file_path, "rb", buffering=io.DEFAULT_BUFFER_SIZE) as f:
            candles = _CompatUnpickler(f).load()

        if verbose:
            _logger.info("loaded %s in %.0fms", os.path.basename(file_path),
                         (datetime.now() - start_time).total_seconds() * 1000)

        # 타입 힌트 만족을 위한 간단한 검증 (선택적)
        if not isinstance(candles, list):
            raise TypeError("Loaded object is not a list of Candle")
        return candles
