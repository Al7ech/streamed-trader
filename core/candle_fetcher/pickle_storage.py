import io
import os
import pickle
from datetime import datetime
from typing import List

from core.streamer import Candle


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
            candles = pickle.load(f)

        if verbose:
            print(f"Loaded {os.path.basename(file_path)} in {(datetime.now() - start_time).total_seconds() * 1000}ms")

        # 타입 힌트 만족을 위한 간단한 검증 (선택적)
        if not isinstance(candles, list):
            raise TypeError("Loaded object is not a list of Candle")
        return candles
