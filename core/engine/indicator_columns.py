from typing import Dict, List, Tuple

from core.streamer import BaseStreamer


def collect_indicator_columns(streamer: BaseStreamer) -> Tuple[List[str], Dict[str, str]]:
    """지표 이름의 합집합(첫 등장 순서) + scale_group.

    심볼마다 지표 구성이 다를 수 있으나, 컬럼 이름/그룹은 심볼 간 공유된 뜻으로 다룬다 —
    백테스터와 라이브 레코더가 동일한 규칙을 쓴다.
    """
    names: List[str] = []
    seen = set()
    for symbol in streamer.symbols:
        for name in streamer.indicators.get(symbol, {}):
            if name not in seen:
                seen.add(name)
                names.append(name)
    column_groups: Dict[str, str] = {}
    for name in names:
        for symbol in streamer.symbols:
            ind = streamer.indicators.get(symbol, {}).get(name)
            if ind is not None:
                column_groups[name] = ind.scale_group
                break
    return names, column_groups
