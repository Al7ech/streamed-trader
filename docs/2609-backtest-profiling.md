# 2026-09 백테스트 프로파일링 — RyulStreamer_edit2, ETH 1m 6.5년

> 작업일 2026-09-11. 대상은 `~/ryul-streamer`의 `RyulStreamer_edit2`, 실행 스크립트는
> `ryul_streamer/examples/backtest_edit2.py`다 (ETHUSDT 1m, 2020-01-01 ~ 2026-07-24,
> **3,450,240 이벤트**, 체결 727건). 측정 환경은 WSL2 Linux 6.18, 단일 스레드, Python 3.14
> (ryul-streamer venv)다. 시간은 모두 wall-clock이다.

## 요약

**엔진 루프의 절반이 매매가 아니라 시계열 샤드 기록에 들어가고 있었다.** 샤드를 켠 전체 구간
실행은 엔진 루프만 59.5초였고, 그중 약 30.6초가 `save_series=True` 경로
(`ShardWriter.add`, 지표 값 수집, 샤드 JSON 쓰기)였다. 전략(`decide_action`)은 4.7초,
지표 갱신은 10.9초였다.

샤드 경로에서 **출력을 한 바이트도 바꾸지 않는** 개선 세 개를 적용했다.

| 단계 | 엔진 루프 (샤드 켬) | 스크립트 전체 wall |
|---|---|---|
| 기준 | 59.5초 | 66.8초 |
| + 달 키 캐싱 | 51.6초 | 62.6초 |
| + `ShardWriter` 지표 바인딩 + `get_latest` fast path | **~45.5초** | **54.2초** |

샤드 경로 비용은 약 30.6초에서 약 16.7초로 줄었다. 샤드를 끈 실행(약 28.8초)은 거의 그대로다.
edit2는 지표를 `read(-2)`로 읽어서 `get_latest` fast path의 영향을 받지 않는다.

---

## 1. 사전 작업: edit2를 현재 엔진으로 옮기기

ryul-streamer 전체가 옛 core API(`core.backtest.*`, `FastBacktester`, `get_index`)를 쓰고 있어
import 단계에서 실패했다. edit2 실행에 필요한 부분만 옮겼다 (ryul-streamer 쪽 변경).

- `streamer/ryul_streamer_edit1.py`, `ryul_streamer_edit2.py`
  - import를 새 경로로 바꿨다: `core.account.status`, `core.candle.candle`, `core.order.action`.
  - `get_index(i)`를 `read(i)`로 바꿨다.
  - `decide_action(symbol, candle, status)`를 `decide_action(candles, status)`로 바꾸고, 심볼마다
    `_decide_symbol`을 부르게 했다. edit2의 현재 캔들 보관 훅도 `_decide_symbol`로 옮겼다.
  - 전략 인자 `fee_ratio`를 없애고 포지션 크기는 `status.fee_ratio`로 계산한다 (core 규약).
    0.0004는 이제 `SimulatedExecutor(fee_ratio=0.0004)`에 넘긴다.
- `examples/backtest_edit2.py`를 `TradingEngine` 조립 방식으로 다시 썼다. 구간과 파라미터는 그대로다.
  - 승률 출력은 core 규칙(청산 체결만 세고 `wnl - fee` 기준)으로 바뀌었다.
- **캔들 캐시 이관**: `asset/candle/**/*.pkl` 400개가 옛 경로 `core.streamer.candle.Candle`로
  피클돼 있어 `ModuleNotFoundError`가 났다. CLAUDE.md의 절차대로 옛 모듈 경로를 새 모듈에
  alias한 뒤 load → `save_to_pickle`(tmp + `os.replace`)로 다시 저장했다. 호환 코드는 남기지 않았다.
- **동등성**: Profit 9009.033007605696%, Sharpe 1.9258097009583854, MDD 28.212878%, 체결 727건.
  옛 FastBacktester 런(`RyulStreamer_edit2_20260727_012753`, `_20260802_034922`)과 끝자리까지 같다.

아직 옛 API인 파일(edit3, edit2_stop, `trader.py`, 연구 스크립트들)은 건드리지 않았다. 이번
변경 전부터 import 단계에서 실패하던 파일들이다.

## 2. 측정 방법

도구 하나로는 답이 나오지 않아 네 가지를 겹쳐 썼다.

1. **wall-clock 기준선**: `/usr/bin/time`으로 스크립트를 그대로 실행했다 (66.8초, 최대 RSS 2.9GB).
2. **cProfile로 구조 파악**:
   `uv run python -m cProfile -o edit2.prof ryul_streamer/examples/backtest_edit2.py`
   - 호출당 오버헤드 때문에 **3.5배(232초)로 부푼다.** 작은 함수가 많은 코드라 비율이 왜곡된다.
   - tqdm 모니터 스레드가 끼어들면서 callee의 `cumtime`이 엉킨다. `tottime`(자기 시간)만 믿을 만했다.
   - 그래서 cProfile은 "누가 무엇을 몇 번 부르나"를 보는 데만 썼다. 자기 시간 상위는
     `ShardWriter.add`, `BacktestRecorder.record_event`, `process_event`, 돈치안 `update`,
     `NumericIndicator.read`였다.
3. **ablation으로 실제 비용 측정**: 캔들을 한 번만 로드하고 `InMemoryCandleProducer`로 같은
   이벤트를 흘리면서 부품을 하나씩 뺐다. 인접한 두 변형의 차이가 그 부품의 실제 비용이다.
   프로파일러 오버헤드가 없다.
4. **마이크로벤치**: 2024-01~02 캔들로 지표를 워밍업한 뒤 hot path 함수 하나씩의 호출당 비용을
   쟀다. 345만을 곱해서 ablation 차이와 맞는지 교차 확인했다.

### ablation 변형

| 변형 | 빠진 것 |
|---|---|
| full | 없음 (스크립트와 같은 구성: 런 JSON + 샤드) |
| run json, no series | 샤드 (`save_series=False`) |
| in-memory recorder | 런 JSON 쓰기 (`metadata=None`) |
| NullRecorder | 레코더 전체 |
| Null + no decide | `decide_action` (빈 리스트 반환) |
| Null + no decide + no ind | 지표 (지표 dict를 비움) |
| producer only | 엔진 전체 (공급자 `async for`만 돈다) |

측정용 스크립트는 리포에 넣지 않았다. 위 표대로 부품을 빼고 `TradingEngine.run()` 앞뒤에
`time.perf_counter()`를 두면 재현된다. 샤드 JSON 쓰기 시간은 `core.result.writer._write_json`을
감싸서 따로 쟀다.

## 3. 결과 (개선 전)

### 3.1 ablation

| 변형 | 시간 | µs/이벤트 |
|---|---|---|
| full + tqdm | 63.50초 | 18.40 |
| full | 59.54초 | 17.26 |
| full, `gc.freeze()` | 59.65초 | 17.29 |
| full, `gc.disable()` | 60.49초 | 17.53 |
| run json, no series | 28.96초 | 8.39 |
| in-memory recorder | 28.85초 | 8.36 |
| NullRecorder | 25.50초 | 7.39 |
| Null + no decide | 20.79초 | 6.02 |
| Null + no decide + no ind | 9.91초 | 2.87 |
| producer only | 2.67초 | 0.78 |
| producer only + tqdm | 2.97초 | 0.86 |

엔진 루프 밖에서는 캔들 로드(pickle 79개, Candle 345만 개)가 약 5.5초 들었다.

### 3.2 구간별 분해 (엔진 루프 59.5초 기준)

| 구간 | 시간 | 비중 |
|---|---|---|
| **시계열 샤드 기록** | **~30.6초** | **51%** |
|   `ShardWriter.add` | ~11초 | |
|     └ 그중 `_month_key` (이벤트마다 datetime 생성 + strftime) | ~6초 | |
|   샤드 JSON 쓰기 (80개, 약 440MB) | ~8.9초 | |
|   `record_event`의 지표 값 dict 수집 (7개) | ~4.6초 | |
|   나머지 (리스트 증가, 메모리) | ~5초 | |
| 지표 갱신 (돈치안 6개 + ATR) | ~10.9초 | 18% |
| 엔진 뼈대 + `SimulatedExecutor.begin_event` | ~7.2초 | 12% |
| `decide_action` | ~4.7초 | 8% |
| 메모리 기록 (자본곡선, buy & hold 비교용 종가) | ~3.4초 | 6% |
| 캔들 공급 (heap 병합 + `Event` 생성 + async gen) | ~2.7초 | 4.5% |

### 3.3 마이크로벤치 (호출당)

| 대상 | µs/호출 | ×345만 |
|---|---|---|
| `MaxDonchianIndicator.update` / `MinDonchianIndicator.update` | 0.27~0.31 | 개당 ~1.0초 |
| `ATRIndicator.update` | 0.37 | 1.3초 |
| `NumericIndicator.read(-2)` | 0.10 | 0.4초 |
| `decide_action` (포지션 없음) | 0.75 | 2.6초 |
| `SimulatedExecutor.begin_event` | 0.85 | 2.9초 |
| `Status.total_margin` | 0.30 | 1.0초 (이벤트당 2번 불린다) |
| `BacktestRecorder.record_event` (샤드 없음) | 1.52 | 5.2초 |
| 지표 dict `{n: get_latest()}` (7개) | 1.34 | 4.6초 |
| `ShardWriter.add` (디스크 쓰기 제외) | 3.28 | 11.3초 |
| `_month_key` | 1.73 | 6.0초 |

### 3.4 배제된 가설

- **GC 아님**: `gc.freeze()`와 `gc.disable()` 모두 차이가 없었다. Candle 객체가 수백만 개여도
  세대별 수집이 문제가 되지 않는다.
- **tqdm 아님**: 공급자만 도는 변형에서 0.3초다. full에서 보인 4초 차이는 실행 간 편차로 본다.
- **JSON 인코더 아님**: 이미 `json.dumps`(C 인코더)를 쓴다. 8.9초는 약 440MB의 float을 문자열로
  바꾸는 비용이다.

## 4. 적용한 개선

세 개 모두 `~/streamed-trader`의 core 변경이다.

### 4.1 달 키 캐싱 (`core/result/writer.py`)

`ShardWriter.add`는 이벤트마다 `_month_key(time_ms)`로 datetime을 만들고 strftime을 불러
달이 바뀌었는지 판정했다. 이제는 현재 달의 `[시작, 끝)`을 ms 정수로 들고 있다가, 시각이 그
구간을 벗어날 때만 `_month_range`로 키를 다시 계산하고 flush한다. `resume`도 같은
`_set_month`를 쓴다. 호출자가 없어진 `_month_key`는 지웠다.

효과: 엔진 루프 59.5초에서 51.6초로 **약 8초** 줄었다. 예상한 6초보다 큰 것은 datetime 객체를
이벤트마다 할당하던 비용까지 같이 사라졌기 때문으로 보인다.

### 4.2 `ShardWriter`가 지표를 직접 읽도록 (`core/result/writer.py`, `core/recorder/*.py`)

예전에는 레코더가 이벤트마다 `{symbol: (candle, {name: ind.get_latest()})}` dict를 만들어 넘기고,
`add`가 그 dict에서 이름으로 값을 다시 찾았다. 이제는 이렇게 한다.

- `ShardWriter`가 생성될 때 `streamer.indicators`를 받는다.
- 심볼마다 `(indicator.get_latest, column.append)` 쌍을 미리 묶어 둔다. 어떤 심볼에 없는 지표
  컬럼은 늘 `None`을 돌려주는 getter와 묶는다 (컬럼은 전 심볼 지표 이름의 합집합이다).
- 묶는 대상이 버퍼 **리스트 객체**라서, 버퍼를 새로 만들 때마다(달이 바뀔 때, `resume`) 다시 묶는다.

API가 바뀌었다.

| | 전 | 후 |
|---|---|---|
| 생성자 | `ShardWriter(dir, run_id, symbols, indicator_names, has_ohlc, interval_ms)` | `ShardWriter(dir, run_id, symbols, indicators, indicator_names, has_ohlc, interval_ms)` |
| `resume` | `resume(dir, run_id, symbols, indicator_names, …)` | `resume(dir, run_id, symbols, indicators, indicator_names, …)` |
| `add` | `add(time, balance, {sym: (candle, values)})` | `add(time, balance, {sym: candle})` |

호출부는 `BacktestRecorder`와 `LiveRecorder` 두 곳뿐이고 둘 다 고쳤다.

### 4.3 `NumericIndicator.get_latest` fast path (`core/streamer/indicator/base_indicator.py`)

`get_latest()`가 `read(-1)`을 부르고, 그게 다시 `_read_series`를 불러 파이썬 프레임이 3단이었다.
이제는 `_deque[-1]`을 바로 읽어 한 단으로 끝낸다.

- -1은 음수이고 항상 보관 범위 안이라 인덱스 검사가 필요 없다.
- 워밍업이면 `None`, NaN이면 `None`으로 바꾸는 규약은 `_read_series`와 같다.
- `NumericIndicator` 서브클래스 중 `read`나 `get_latest`를 오버라이드하는 것은 없다.
  `read`를 오버라이드하는 `PivotTrendlineIndicator`는 `BaseIndicator`를 직접 상속해서 영향이 없다.

4.2와 4.3을 합친 지표 스냅샷 경로의 비용은 따로 벤치했다 (이벤트 100만 번, 지표 7개).

| 방식 | ×345만 |
|---|---|
| 기존 (dict 생성 → `values.get` → `_clean_value`) | 6.0초 |
| 4.2만 (미리 묶은 쌍) | 3.9초 |
| **4.2 + 4.3** | **2.2초** |
| 4.2 + 4.3 + `_clean_value` 인라인 (적용 안 함) | 2.0초 |

효과: 엔진 루프 51.6초에서 약 45.5초로 줄었다 (두 번 측정해 46.35초, 44.59초).

## 5. 검증

출력이 바뀌지 않는다는 것을 네 가지로 확인했다.

1. **달 경계 계산**: 2010~2030년 무작위 시각 20만 개와 모든 월 경계 ±1ms에서 `_month_range`의
   키가 옛 `_month_key`와 같고, `시작 <= ms < 끝`이 성립하며, 경계 양옆의 키가 맞았다.
2. **edit2 전체 구간 재실행**: 개선을 하나 적용할 때마다 다시 돌렸다. 샤드 79개가 기준 런과
   **바이트 단위로 같았고**, 런 JSON도 `run_at`·label·run_id를 빼면 같았다.
3. **레코더 동등성 하네스**: 변경 **전** 출력을 스냅샷으로 떠 두고 변경 후와 비교했다.
   - 2심볼이고 심볼마다 지표 구성이 다르다 (합집합 컬럼 → 없는 컬럼은 null).
   - 한 심볼이 늦게 시작한다 (캔들 없는 행은 전부 null).
   - 월 경계를 넘는다 (flush 후 버퍼 재생성 → 재바인딩).
   - NaN, 0.0, None을 번갈아 내는 지표로 `_clean_value` 규약을 확인했다.
   - `BacktestRecorder`는 병합 이벤트로 돌렸다. `LiveRecorder`는 심볼별 단일 이벤트, flush
     주기 7, **중간 재기동(`resume`)**으로 돌렸다.
   - 결과: 샤드 전부 바이트 단위로 같았다. 런 JSON은 벽시계 타임스탬프(`run_at`, `resumed_at`)만 달랐다.
   - `core/checks/live_check.py`는 `LiveRecorder`를 거치지 않아서 이 하네스가 필요했다.
4. **`core/checks/live_check.py`**: 두 단계 모두 ALL OK였다.

## 6. 남은 개선 후보

적용 후 엔진 루프 약 45.5초의 분포는 대략 이렇다: 샤드 약 16.7초(JSON 약 8.8초 포함),
지표 갱신 약 11초, 엔진 뼈대와 `begin_event` 약 7초, `decide_action` 약 5초, 메모리 기록 약
3초, 공급자 약 3초.

| 후보 | 예상 효과 | 비고 |
|---|---|---|
| ~~진입 스크립트의 샤드를 옵트인 플래그로~~ | 샤드를 안 쓰는 런에서 45초 → 29초, 런당 약 440MB 절약 | **적용함** — `core/examples/backtest.py --no-series` (라벨 인자와 공존). `BacktestRecorder`의 기본값은 이미 `False`였다. 샤드가 없으면 시각화 도구에 가격 차트와 지표 패널이 나오지 않는다 |
| 샤드 JSON 크기 줄이기 (float 반올림, 컬럼 축소) | 최대 약 8.8초 | 샤드 포맷을 바꾸는 일이라 프론트까지 영향이 간다 |
| ~~벡터화 백테스트 경로 재도입 (`precompute_series`)~~ | 지표 갱신 약 11초의 대부분 | **적용함 — 8절 참고** |
| ~~`begin_event`에서 미체결 주문이 없으면 매칭·만료를 건너뛰기~~ | 1~2초 | **적용함 — 7절 참고** |
| `_clean_value` 인라인 | 약 0.2초 | 효과가 작아 적용하지 않았다 |

## 7. 적용한 개선 (2) — `begin_event` 매칭/만료 스킵

`~/streamed-trader`의 core 변경이다 (`core/executor/simulated.py`).

`SimulatedExecutor.begin_event`는 이벤트에 캔들이 있는 심볼마다 무조건
`order_book.match_symbol(...)`을 부르고 그 뒤 `order_book.tick_expiry(...)`를 불렀다. 두 함수
다 그 심볼의 미체결 장부(`status.open_orders.get(symbol)`)가 비어 있으면 바로 반환하므로
결과에는 영향이 없지만, 대부분의 캔들엔 미체결 주문이 없어서(전략 대부분이 시장가로만
진입·청산한다) 제너레이터 객체 생성 + 함수 호출 오버헤드가 이벤트·심볼마다 쌓였다.

이제는 호출 전에 장부를 한 번만 확인한다: `if st.open_orders.get(symbol): ...` 안에서만
`match_symbol`/`tick_expiry`를 부른다. 장부가 비었을 때 두 함수가 아무것도 하지 않는다는
점은 그대로 이용하므로 (그 자체는 손대지 않았다), 미체결 주문을 실제로 쓰는 전략
(`KeltnerStopStreamer`, 지정가/스탑 래더 등)의 출력은 바뀌지 않는다.

**검증**: `core/checks/live_check.py`를 변경 전후로 돌려 모든 드라이런 대조(Keltner, 시장가;
MeanReversionZScore, 상태 있는 전략; KeltnerStop, 조건부 주문; LimitLadder; StopLadder,
reduce_only + 슬리피지) 체결 수·최종 잔고가 **완전히 동일**함을 확인했다 — ALL OK.

이 저장소(streamed-trader)는 아직 3,450,240 이벤트짜리 실측 벤치마크 대상이 없어(edit2는
`~/ryul-streamer`) 정확한 초 단위 효과는 재측정하지 않았다. 미체결 주문 사용 빈도가 낮은
전략(`KeltnerStreamer` 등 시장가 전용)일수록 효과가 크고, 매 캔들 지정가/스탑을 다시 거는
전략(`KeltnerStopStreamer`)은 장부가 항상 차 있어 효과가 거의 없다.

## 8. 적용한 개선 (3) — 백테스트 전용 엔진과 지표 선계산

`~/streamed-trader`의 core 변경이다. 6절의 마지막 남은 큰 후보였다.

`core/engine/backtest.py`에 백테스트 전용 엔진 `BacktestEngine`을 만들고, `core/examples/
backtest.py`·`core/checks/live_check.py`·`core/checks/fetch_stock_check.py`를 그쪽으로 옮겼다.
`BinanceHistoricalCandleProducer`는 지웠다 (진입점이 `CandleHistory.fetch`를 직접 부른다).
`TradingEngine`은 드라이런/라이브 전용이 됐다.

새 엔진은 캔들을 메모리에 다 들고 시작하므로 `precompute_series`를 정의한 지표의 전 구간을 먼저
계산해 두고, 루프에서는 값을 지표의 출력 deque에 하나씩 얹기만 한다 (`NumericIndicator.
precomputed_sink()`). 정의하지 않은 지표는 그대로 `update()`로 돈다.

### 8.1 오차를 0으로 — 정확 벡터화

예전 벡터화 경로가 상대 허용오차로만 비교될 수 있었던 이유는 `precompute_series`가 pandas
`rolling().mean()`을 썼기 때문이다. pandas는 Kahan 보정을 쓰고 루프는 단순 증분 합산이라 **더하는
순서가 달라서**, 50만 캔들 중 47.5만 개가 상대오차 최대 5.6e-15로 갈렸다.

`np.cumsum`이 엄격한 순차 폴드(`c[i] = c[i-1] + x[i]`)라 파이썬 루프와 반올림까지 같다는 점을
이용해, **루프의 재귀식을 그대로 펼치는** 방식으로 바꿨다 (`core/streamer/indicator/
vector_ops.py`). `MovingAverage`의 `s = (s - old) + new`는 `(-old, +new)`를 번갈아 놓은 길이 `2n`
배열의 `cumsum`이다. `RollingStd`는 루프가 창을 매번 통째로 다시 재므로(`np.std(values, ddof=1)`)
벡터화 쪽도 슬라이딩 윈도우 뷰에서 같은 축약을 한다. 돈치안은 min/max라 산술이 없어 이미 같았다.

실제 지표 클래스로 확인한 결과 — `update()`는 한 줄도 바꾸지 않았다:

| 지표 | 기존 (pandas) | 현재 | 345만·w=4320 |
|---|---|---|---|
| `MovingAverage` / `ATRIndicator` / `VolumeMovingAverage` | 불일치 | **비트 동일** | 83ms (파이썬 루프 ~1500ms) |
| `MinDonchianIndicator` / `MaxDonchianIndicator` | 이미 동일 | 그대로 | — |
| `RollingStd` / `VolumeRollingStd` | 불일치 | **비트 동일** | w=60에서 400k당 113ms |

그래서 `vectorize` 플래그는 정확성을 위해 필요하지 않다 — 디버그 스위치로만 남겼다. MA/ATR/std
에서 pandas 의존도 빠졌다 (돈치안만 남는다).

### 8.2 측정

이 저장소의 진입점 그대로 (`core/examples/backtest.py`, ETHUSDT 1m, 2026-01-01 ~ 2026-07-10,
**273,600 이벤트**, 지표 2개(MA+ATR), 샤드 켬):

| | 엔진 루프 | 스크립트 전체 wall | 최대 RSS |
|---|---|---|---|
| 기준 (`TradingEngine` + 루프 지표) | 2.3초 (118k event/s) | 3.48초 | 298MB |
| `BacktestEngine` + 선계산 | **1.55초 (175k event/s)** | **2.79초** | 303MB |

지표가 2개뿐인 전략이라 절대 이득이 작다. 6절이 대상으로 삼은 edit2(지표 7개, 345만 이벤트)에서
지표 갱신 약 10.9초가 선계산 0.56초 + 재생 약 1.4초로 줄어드는 계산이므로, 거기서는 약 9초
(45.5초의 약 20%)가 빠질 것으로 본다. 남는 지배적 비용은 여전히 `ShardWriter`(약 16.7초)다.

### 8.3 검증

1. **`core/checks/backtest_check.py` (새 검사)** — `precompute_series`를 정의한 7개 지표 전부에
   대해, 선계산 수열이 `update()` 루프의 `get_latest()` 수열과 element-wise 비트 동일함을
   window/길이 엣지 케이스(창보다 짧음, 딱 맞음, n=0/1, window=1, `history_size`보다 큰 창)에서
   확인. 이어서 sink 재생의 `read(-1)…read(-(window+2))`가 전 구간 루프와 같고, 보관 이력 경계의
   `IndexError`와 양수 인덱스 거부까지 같은지 확인. 실제 캔들 위에서 `vectorize=True/False`의
   `Report` 대조 5전략 + ragged 멀티심볼 + 퇴화 입력. **ALL OK.**
2. **`core/checks/live_check.py`** — `run_bt`가 새 엔진(벡터화 기본값)을 쓰도록 바꾼 뒤 ALL OK.
   드라이런 대조 5개가 전부 **허용오차 없이** 통과한다. 순서 규약이 두 엔진에 나뉜 지금 이것이
   둘의 일치를 보증하는 유일한 검사다.
3. **런 산출물 회귀 대조** — 위 측정 구간을 변경 전후로 돌려 런 JSON(`run_at`/`run_id`/label 제외)
   전체와 월 샤드 7개가 **바이트 단위로 동일**함을 확인했다.
