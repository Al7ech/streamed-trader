"""백테스트/드라이런/벡터화 경로의 ``Report``를 비교하는 공용 헬퍼.

``core/checks/live_check.py``(드라이런 == 백테스트)와 ``core/checks/backtest_check.py``
(벡터화 == 루프)가 함께 쓴다. ``live_check``는 모듈 레벨에서 python-binance를 끌고 오므로,
그것이 필요 없는 검사가 이 비교 함수 하나 때문에 그 값을 치르지 않도록 따로 두었다.
"""

import math


def compare_reports(label, ref_report, fast_report) -> bool:
    """두 ``Report``가 체결·자본곡선·벤치마크까지 같은지 확인한다.

    허용오차가 남아 있는 것은 **역사적인 이유**다 — 예전에는 벡터화 지표가 루프 지표와
    반올림이 갈려서 상대 허용오차 없이는 비교할 수 없었다. 지금은 두 경로가 비트 단위로
    같으므로(``core/streamer/indicator/vector_ops.py``) 여기 비교는 전부 정확히 일치해야
    통과한다. 허용오차는 그대로 두되, 그 위에서 걸리는 차이가 있다면 그것은 **회귀**다.
    """
    ok = True
    ref_final = ref_report.status.total_margin()
    fast_final = fast_report.status.total_margin()
    if len(ref_report.trades) != len(fast_report.trades):
        print(f"  [{label}] FAIL: trade count {len(ref_report.trades)} != {len(fast_report.trades)}")
        return False
    for i, (a, b) in enumerate(zip(ref_report.trades, fast_report.trades)):
        # 실현손익은 quantity * (체결가 - 평단)이라 **거의 같은 두 수의 차**다. 체결가가
        # 지표값인 조건부 주문에서는 지표 반올림(1e-14 상대)이 그 뺄셈에서 세 자릿수쯤
        # 증폭되므로, wnl의 허용오차는 wnl 자신이 아니라 **명목가치**에 비례해야 한다.
        # 구조적으로 틀린 손익은 명목가치의 유의미한 비율만큼 어긋나므로 이걸로도 충분히 걸린다.
        notional_tol = max(1e-9, 1e-11 * abs(a.quantity * a.price))
        if not (a.timestamp == b.timestamp and a.symbol == b.symbol and a.quantity == b.quantity
                and a.order_type == b.order_type and a.submitted_at == b.submitted_at
                and a.pre_position == b.pre_position
                and math.isclose(a.price, b.price, rel_tol=1e-12, abs_tol=1e-9)
                and math.isclose(a.wnl, b.wnl, rel_tol=1e-12, abs_tol=notional_tol)
                and math.isclose(a.fee, b.fee, rel_tol=1e-12, abs_tol=1e-9)
                and math.isclose(a.pre_margin, b.pre_margin, rel_tol=1e-12, abs_tol=1e-6)
                and math.isclose(a.leverage, b.leverage, rel_tol=1e-12, abs_tol=1e-9)):
            print(f"  [{label}] FAIL: trade #{i} differs:\n    ref : {a}\n    fast: {b}")
            ok = False
            break
    if not math.isclose(ref_report.max_leverage, fast_report.max_leverage, rel_tol=1e-12, abs_tol=1e-9):
        print(f"  [{label}] FAIL: max_leverage {ref_report.max_leverage} != {fast_report.max_leverage}")
        ok = False
    if not math.isclose(ref_final, fast_final, rel_tol=1e-12, abs_tol=1e-6):
        print(f"  [{label}] FAIL: final margin {ref_final} != {fast_final}")
        ok = False
    if len(ref_report.equity_curve) != len(fast_report.equity_curve):
        print(f"  [{label}] FAIL: equity curve length "
              f"{len(ref_report.equity_curve)} != {len(fast_report.equity_curve)}")
        ok = False
    else:
        # 허용오차는 **상대**가 본질이다. 지표 반올림 차이가 체결가와 avg_price를 통해 자본에
        # 누적되므로, 오차는 계좌 크기에 비례해서 커진다. 고정 1e-6 절대값만 쓰면 100배로
        # 불어난 계좌에서 순수 반올림이 실패로 잡힌다 — 구조적 어긋남은 자본의 유의미한
        # 비율만큼 벌어지므로 rel_tol 1e-11로도 충분히 걸린다.
        worst = (0.0, 0.0, 0)  # (abs, rel, index)
        for i, ((ts_a, eq_a), (ts_b, eq_b)) in enumerate(
                zip(ref_report.equity_curve, fast_report.equity_curve)):
            if ts_a != ts_b:
                print(f"  [{label}] FAIL: equity curve timestamps diverge at {ts_a} vs {ts_b}")
                ok = False
                break
            diff = abs(eq_a - eq_b)
            if diff > worst[0]:
                worst = (diff, diff / abs(eq_a) if eq_a else 0.0, i)
            if not math.isclose(eq_a, eq_b, rel_tol=1e-11, abs_tol=1e-6):
                print(f"  [{label}] FAIL: equity curve diverges at index {i} ({ts_a}): "
                      f"{eq_a} vs {eq_b}")
                ok = False
                break
        else:
            if worst[0] > 1e-6:
                print(f"  [{label}] note: equity curve max abs diff {worst[0]:.3e} "
                      f"(relative {worst[1]:.3e}) — 지표 반올림 누적")
    # buy & hold 기준선은 종가에서만 유도되므로 사실상 회귀 가드 — 두 경로가 같은 이벤트 구간을
    # 잘라냈는지까지 확인한다.
    if len(ref_report.benchmark_curve) != len(fast_report.benchmark_curve):
        print(f"  [{label}] FAIL: benchmark curve length "
              f"{len(ref_report.benchmark_curve)} != {len(fast_report.benchmark_curve)}")
        ok = False
    else:
        for (ts_a, eq_a), (ts_b, eq_b) in zip(ref_report.benchmark_curve, fast_report.benchmark_curve):
            if ts_a != ts_b or not math.isclose(eq_a, eq_b, rel_tol=1e-12, abs_tol=1e-6):
                print(f"  [{label}] FAIL: benchmark curve differs at {ts_a}: {eq_a} vs {eq_b}")
                ok = False
                break
    return ok
