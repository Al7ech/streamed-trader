import gzip
from typing import List

import pandas as pd
from tqdm import tqdm

from core.streamer import Candle


class CandleStorage:
    @staticmethod
    def save_to_csv(candles: List[Candle], file_path: str, compress: bool = True):
        """
        Candle 객체 리스트를 압축된 csv(.csv.gz) 또는 일반 csv(.csv) 파일로 저장합니다.
        :param candles: Candle 객체 리스트
        :param file_path: 저장할 파일 경로 (예: 'candle.csv.gz' 또는 'candle.csv')
        :param compress: True면 .gz, False면 .csv
        """
        df = pd.DataFrame([
            {
                'open': c.open,
                'high': c.high,
                'low': c.low,
                'close': c.close,
                'volume': c.volume,
                'start_time': c.start_time,
                'end_time': c.end_time
            } for c in candles
        ])
        if compress:
            if not file_path.endswith('.gz'):
                file_path += '.gz'
            with gzip.open(file_path, 'wt', encoding='utf-8') as f:
                df.to_csv(f, index=False, lineterminator='\n')
        else:
            if file_path.endswith('.gz'):
                file_path = file_path[:-3]
            with open(file_path, 'w', encoding='utf-8', newline='') as f:
                df.to_csv(f, index=False, lineterminator='\n')

    @staticmethod
    def load_from_csv(file_path: str, compress: bool = True) -> List[Candle]:
        """
        압축된 csv(.csv.gz) 또는 일반 csv(.csv) 파일에서 Candle 객체 리스트를 불러옵니다.
        :param file_path: 파일 경로 (예: 'candle.csv.gz' 또는 'candle.csv')
        :param compress: True면 .gz, False면 .csv
        :return: Candle 객체 리스트
        """
        if compress:
            if not file_path.endswith('.gz'):
                file_path += '.gz'
            df = pd.read_csv(file_path, compression='gzip')
        else:
            if file_path.endswith('.gz'):
                file_path = file_path[:-3]
            df = pd.read_csv(file_path)

        print("loading candles...")
        candles = [
            Candle(
                open=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close'],
                volume=row['volume'],
                start_time=int(row['start_time']),
                end_time=int(row['end_time'])
            ) for _, row in tqdm(df.iterrows(), total=len(df))
        ]
        return candles
