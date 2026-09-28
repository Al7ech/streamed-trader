"""Binance USD-M ``exchangeInfo``에서 심볼별 거래 규칙을 뽑는다.

두 곳이 **이 파서 하나**를 쓴다:

* 생성기 (이 모듈을 스크립트로 실행) — 공개 REST 응답으로 ``core/order/symbol_rules_data.py``를
  다시 쓴다. 키가 필요 없다::

      uv run python -m core.fetcher.binance.exchange_info

* 라이브 기동 검증 — :meth:`~core.executor.live.LiveExecutor.create`가
  ``futures_exchange_info()`` 응답을 여기서 파싱해 하드코딩 값과 :func:`diff_rules`로 비교한다.
  하드코딩 파일이 낡았으면 백테스트는 통과했는데 라이브는 거부되는 주문이 생기므로, 라이브는
  불일치를 치명적으로 본다.
"""

import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Mapping, Tuple

import requests

from core.order.symbol_rules import SymbolRules

EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

logger = logging.getLogger(__name__)

RawRules = Tuple[str, str, str, str, str, str]


def parse_exchange_info(payload: Dict) -> Dict[str, RawRules]:
    """``exchangeInfo`` 응답 → ``{symbol: (step, min_qty, market_step, market_min_qty,
    min_notional, tick)}``. 값은 거래소가 준 십진 문자열 그대로다.

    필터가 하나라도 없는 심볼은 건너뛴다 (경고) — 그 심볼은 규칙을 모르는 것으로 남아
    ``Status.rules_for``가 명시적으로 실패한다.
    """
    out: Dict[str, RawRules] = {}
    for s in payload.get("symbols", []):
        f = {x["filterType"]: x for x in s.get("filters", [])}
        try:
            out[s["symbol"]] = (
                f["LOT_SIZE"]["stepSize"],
                f["LOT_SIZE"]["minQty"],
                f["MARKET_LOT_SIZE"]["stepSize"],
                f["MARKET_LOT_SIZE"]["minQty"],
                f["MIN_NOTIONAL"]["notional"],
                f["PRICE_FILTER"]["tickSize"],
            )
        except KeyError as e:
            logger.warning("%s: 필터 %s가 없어 건너뛴다", s.get("symbol"), e)
    return out


def diff_rules(expected: Mapping[str, SymbolRules], fetched: Mapping[str, RawRules],
               symbols: Iterable[str]) -> List[str]:
    """``symbols``에 대해 하드코딩 규칙(``expected``)과 거래소 값(``fetched``)의 차이를 적는다.
    빈 리스트면 일치한다. 값 비교라 ``"0.001"``과 ``"0.00100000"``은 같다."""
    problems = []
    for symbol in symbols:
        if symbol not in fetched:
            problems.append(f"{symbol}: 거래소 exchangeInfo에 없다")
            continue
        if symbol not in expected:
            problems.append(f"{symbol}: 하드코딩 규칙에 없다 (거래소: {fetched[symbol]})")
            continue
        ours = expected[symbol].as_tuple()
        theirs = fetched[symbol]
        if any(Decimal(a) != Decimal(b) for a, b in zip(ours, theirs)):
            problems.append(f"{symbol}: 하드코딩 {ours} != 거래소 {theirs}")
    return problems


def _render(rules: Dict[str, RawRules]) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        '"""Binance USD-M 심볼별 거래 규칙 — **생성 파일, 직접 수정하지 말 것.**',
        "",
        f"생성: {stamp}, 출처: {EXCHANGE_INFO_URL}",
        "재생성: ``uv run python -m core.fetcher.binance.exchange_info``",
        "",
        "값: ``(stepSize, minQty, 시장가 stepSize, 시장가 minQty, 최소 명목가치, tickSize)`` —",
        "거래소가 준 십진 문자열 그대로. 해석은 :class:`core.order.symbol_rules.SymbolRules`.",
        '"""',
        "",
        "RULES = {",
    ]
    for symbol in sorted(rules):
        lines.append(f"    {symbol!r}: {rules[symbol]!r},")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main() -> None:
    from core.logging_config import setup_logging
    setup_logging()
    resp = requests.get(EXCHANGE_INFO_URL, timeout=30)
    resp.raise_for_status()
    rules = parse_exchange_info(resp.json())
    if not rules:
        sys.exit("exchangeInfo에서 규칙을 하나도 읽지 못했다 — 파일을 덮어쓰지 않는다")
    # 생성 파일은 패키지 안에 있으므로 cwd가 아니라 core.order 위치로 찾는다.
    import core.order
    path = os.path.join(os.path.dirname(core.order.__file__), "symbol_rules_data.py")
    # 모든 행이 해석되는지 먼저 확인한다 — 새로운 모양의 단위가 생겼으면 여기서 멈춘다.
    for symbol, raw in rules.items():
        SymbolRules(*raw)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(_render(rules))
    os.replace(tmp, path)
    logger.info("%d개 심볼 규칙을 %s에 썼다", len(rules), path)


if __name__ == "__main__":
    main()
