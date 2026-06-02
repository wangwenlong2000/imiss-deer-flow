# retrieval_protocol —— 统一检索输入与结果证据格式

`task.md`《统一检索输入与结果证据格式》的参考实现。把检索请求统一成 **Input
Envelope**（task.md §3），把检索结果统一成 **SkillResult**（task.md §4，每条命中
是 `retrieval` 型 evidence，载荷为兼容 citybench 的 `evidence_unit`），并提供
§7 融合前校验器与 §6 兼容映射适配器。

**仅依赖 Python 标准库**，可直接拷贝进任意检索 skill 的运行环境。

## 目录结构

| 文件 | 对应 task.md | 内容 |
|---|---|---|
| `schema.py` | §2 / §4.4 | `DATA_TYPES`（10 类登记取值）、`FEATURES_REGISTRY`、各类枚举 |
| `envelope.py` | §3 | Input Envelope 构造器 + v1.1 `build_budget` |
| `evidence.py` | §4 | `evidence_unit` / evidence wrapper / SkillResult 构造器 |
| `errors.py` | v1.1 §4 errors | 标准化错误码、建议处理动作、status 联动校验 |
| `validate.py` | §7 + v1.1 | 融合前十条校验规则 + budget / errors 校验 |
| `adapters.py` | §6 + v1.1 | citybench / network-traffic / road-traffic / policy + 6 类新增 adapter |
| `middleware.py` | v1.1 | 统一检索中间件:LRU+TTL 缓存 / JSONL 日志 / 审计留痕 |
| `tests/` | §7 测试清单 | `unittest` 用例,零依赖运行(182 测试) |

## schema v1.1 新增

- **`Envelope.budget`**(可选顶层字段):Planner 主动声明 `max_evidence_count` /
  `max_token_estimate`,各 retrieve 必须尊重,避免并行调用撑爆 32k 上下文。
- **`SkillResult.errors[]` 标准化**:每条 error 必须含 `code` / `message` /
  `action` / `retryable`;status="success" 禁带 errors,"partial"/"error" 必带至少一条。
- **6 类新增 adapter**:`traffic_flow` / `telecom` / `code` / `streetview` /
  `remote_sensing` / `surveillance`;电话、视频默认按 PII 敏感标注。
- **统一检索中间件**:DeerFlow 在各 retrieve 出口加一层,一次性实现缓存、监控
  日志、合规审计;`RetrievalMiddleware.call(envelope, retrieve_fn=...)`。

## 快速上手

### 1. 构造统一检索输入（task.md §3）

```python
from retrieval_protocol import (
    build_input_envelope, build_filters, build_retrieval_params,
    build_time_range_absolute,
)

envelope = build_input_envelope(
    skill_name="citybench-rag-search",
    scenario="spatiotemporal_trajectory",   # SkillRouter 的 scene，不是 data_type 路由键
    capability="evidence_search",
    query="上海早高峰陆家嘴交通流量异常",
    data_type="traffic_flow",               # 数据类型标签，用于结果分桶 / 校验
    retrieval=build_retrieval_params(strategy="hybrid", rerank=True),
    filters=build_filters(
        city="Shanghai",
        time_range=build_time_range_absolute(
            start="2024-03-01T00:00:00+08:00", end="2024-03-31T23:59:59+08:00"),
        anomaly_only=True,
    ),
)
```

### 2. 构造统一结果证据（task.md §4）

```python
from retrieval_protocol import (
    build_evidence_unit, build_geo_scope, build_retrieval_evidence,
    build_skill_result, build_summary, build_key_metric,
)

unit = build_evidence_unit(
    evidence_id="traj_ev_20120601_0700_beijing_wx4g0",
    data_type="spatiotemporal_trajectory",
    text="2012年6月1日早高峰，Beijing(geohash:wx4g0)签到944次，较前周上升146.8%。",
    source_id="citybench_checkins_beijing",
    geo_scope=build_geo_scope(city="Beijing", geohash="wx4g0", landmark="国贸-CBD核心区"),
    features={"checkin_count": 944, "wow_change_pct": 146.78, "anomaly_flag": True},
)

# 检索分由 evidence wrapper 携带，不进 evidence_unit 本体。
wrapper = build_retrieval_evidence(
    evidence_ref="e-001", rank=1, score=0.92,
    method="bm25_vector_rrf", rrf_k=60, payload=unit,
)

skill_result = build_skill_result(
    skill_name="citybench-rag-search",
    scenario="spatiotemporal_trajectory",
    capability="evidence_search",
    summary=build_summary(
        title="陆家嘴早高峰交通异常检索", overview="命中 1 条证据。",
        key_metrics=[build_key_metric(name="result_count", value=1)]),
    evidence=[wrapper],
)
```

### 3. 融合前校验（task.md §7）

```python
from retrieval_protocol import validate_skill_result, validate_evidence, is_valid_evidence

errors = validate_skill_result(skill_result)   # 空列表 = 合规
assert errors == [], errors

if not is_valid_evidence(wrapper):
    ...  # 丢弃或修复
```

校验函数一览：

| 函数 | 校验对象 | 覆盖规则 |
|---|---|---|
| `validate_evidence(wrapper)` | 一条 evidence wrapper + payload | 规则 1–8 |
| `validate_evidence_unit(payload)` | 一个 evidence_unit | 规则 2、4、5、6、7、8 |
| `validate_evidence_list(wrappers)` | 一组 evidence wrapper | 规则 1–8 + 3（唯一性） |
| `validate_skill_result(sr)` | 完整 SkillResult | 规则 9、8、3 + 逐条 1–8 |
| `validate_jsonl_lines(text_or_list)` | 跨 skill 交换的 JSONL | 规则 3 + 逐条 |
| `validate_input_envelope(env)` | Input Envelope | task.md §3.2 / §3.3 |

> 规则 10（路由经 SkillRouter，不引入 `data_type` 路由分支）是设计约束，无逐条
> 数据校验项。规则 6 / 7 依赖语义判断，采用「字段给出即从严校验」策略。

## §6 兼容映射适配器

把已落地实现的**原生命中**转换成标准 evidence wrapper：

```python
from retrieval_protocol import (
    # task.md §6 原始 4 类
    adapt_citybench_result,        # §6.1 时空轨迹（source 零改动直通）
    adapt_network_traffic_result,  # §6.2 网络流量（薄映射，相对时间）
    adapt_road_traffic_result,     # §6.3 交通流量年报 RAG（gazetteer）
    adapt_policy_result,           # §6.4 政策法规 RAG（policy）
    build_network_traffic_skill_result,  # network-traffic 整份结果 → SkillResult
    # schema v1.1 新增 6 类
    adapt_traffic_flow_result,     # 交通流量(结构化 + 时空)
    adapt_telecom_result,          # 电话网络(PII 敏感,默认 pii_masked)
    adapt_code_result,             # 代码片段(text + locator file_path/line)
    adapt_streetview_result,       # 街景图像(image + bbox + objects)
    adapt_remote_sensing_result,   # 卫星遥感(tile_id + change_score)
    adapt_surveillance_result,     # 视频监控(clip_start/end + behavior,默认 pii_masked)
)

wrappers = adapt_network_traffic_result(rag_search_result["hits"])
skill_result = build_network_traffic_skill_result(rag_search_result)
```

## v1.1 统一检索中间件

DeerFlow 在每个 retrieve 调用出口包一层,实现缓存 / 日志 / 审计三件套:

```python
from retrieval_protocol import (
    RetrievalMiddleware, LRUCache, JsonlSink,
)

mw = RetrievalMiddleware(
    cache=LRUCache(max_entries=1024, ttl_seconds=600),
    log_sink=JsonlSink("logs/retrieval.jsonl"),
    audit_sink=my_compliance_sink,   # 合规组实现的钩子
)
result = mw.call(envelope, retrieve_fn=my_rag_search, user_id="zhangsan")
```

- **缓存键**:由 envelope 的 `skill_name + scenario + parameters + filters + budget`
  推导;**忽略** `request_id`,所以同语义重复调用必命中。
- **缓存策略**:只缓存 `status="success"/"partial"`,错误结果不入缓存。
- **日志字段**:`ts / request_id / skill_name / scenario / query_hash / cache_hit /
  latency_ms / hit_count / status`,一行 JSON。
- **审计字段**:`user_id / sensitivity_distribution / sensitive_hit_count / filters`,
  字段表与合规组对齐;sink 异常不阻塞主路径。

## 敏感度落档 + 6 类 adapter 预标（spec 2026-05-27 §3）

`adapt_road_traffic_hit` / `adapt_telecom_hit` / `adapt_code_hit` /
`adapt_streetview_hit` / `adapt_remote_sensing_hit` / `adapt_surveillance_hit`
在构造 `evidence_unit` 时调用 `classify_sensitivity`，按字段内容做**保守预标**。
预标只是初始档；最终是否过滤 / 脱敏 / 拒绝，仍由合规检测器统一决策。

```python
from retrieval_protocol import classify_sensitivity, DEFAULT_SENSITIVITY

level, policy = classify_sensitivity(
    data_type="telecom",
    evidence_unit={
        "text": "号码 13800138000 通话",
        "features": {},
    },
)
# ("restricted", "restricted") —— 明文手机号触发升档
```

四档语义见 `schema.py` 中 `SENSITIVITY_LEVELS` 注释；6 类默认级别表见
`sensitivity_rules.py` 的 `DEFAULT_SENSITIVITY`。**不动**的 4 类
(`spatiotemporal_trajectory` / `netflow` / `policy` / `traffic_flow`) 保留
现有 adapter 硬编码默认。

> **5/29 对齐**（streetview）：实际数据形态是嵌套 `metadata.{latitude, longitude, address.*}`，无 `bbox`，无人脸/号牌遮挡假设。adapter 同时兼容旧顶层字段（`lat`/`lon`/`city`/`objects`/`bbox`/...）。敏感度判定路径：精确经纬度（≥4 位小数）仍由 `_has_struct_id` 的 LATLON 正则升 `restricted`；详细门牌地址（`address.formatted_address` + `street_number`）属字符串，不触发 PII 正则但落进 features，由上层合规检测器二次判定。

> **5/29 对齐**（surveillance）：实际数据形态是 `video_id / camera_id / filename / raw_segment_uri / started_at / ended_at / labels / object_summary / location.{city,camera_lat,camera_lon} / metadata.{...}`，无逐帧 bbox。adapter 同时兼容旧顶层字段（`clip_start`/`clip_end`/`stream_url`/`objects` 等）。`object_summary` 是计数聚合（如 `{"person": 12, "car": 34}`），不是逐目标列表，不会触发 `_has_unmasked_face_plate`。当前形态下默认仍是 `restricted`（无 `objects` 列表 = 无法判定打码 = 保守取顶档），与师兄反馈"yolo 打码理论可做未测试"一致。

> **5/29 对齐**（remote_sensing）：实际数据形态是 `id / title / content / similarity / rank / url / hash / resolution / exif`。adapter 优先取新字段（`evidence_id=id`，`source_path=url`，`text=title+content`，`score=similarity`），同时向后兼容旧 `doc_id`/`caption`/`image_uri`。合规重点为"精确地理位置"，由 `_has_struct_id` 的 LATLON 正则覆盖（`content` 中含 4 位以上小数的经纬度即升 `restricted`）。

> **5/29 对齐**（code）：代码片段 skill 当前仅做敏感词分析（密钥/账号 token 检测），不参与 retrieval。本 adapter 接口保留，未启用。

## 审计字段扩展（spec 2026-05-27 §4）

`RetrievalMiddleware.call` 新增可选入参，自动注入到 audit record：

```python
from retrieval_protocol import (
    RetrievalMiddleware, JsonlSink,
    build_evidence_action,
)

action = build_evidence_action(
    evidence_id="ev_1", action="filter", action_status="applied",
    triggered_violation_types=["V_PII_PHONE"],
    risk_locations=[{"field_path": "meta.subject", "risk_type": "phone"}],
    reason_code="struct_id_detected",
    sensitivity_before="pii_masked", sensitivity_after="restricted",
)

mw = RetrievalMiddleware(
    audit_sink=JsonlSink("logs/retrieval.audit.jsonl").emit,
)
mw.call(
    envelope,
    retrieve_fn=my_rag_search,
    user_id="zhangsan",
    gate="OutputGate",          # InputGate / ContextGate / OutputGate
    scene="internal_org",       # self_use / internal_org / cross_org / public_release / research_anon
    policy_version="2026-05-27.1",
    detector_version="d-0.3.0",
    evidence_actions=[action],
)
```

audit record 字段：

| 字段 | 类型 | 备注 |
|---|---|---|
| `gate` / `scene` | str/None | 闸门 + 场景 |
| `data_type` | str/None | 从 envelope.input.parameters.data_type 自动抽 |
| `policy_version` / `detector_version` | str/None | 处置矩阵 / 检测器版本 |
| `evidence_actions[]` | list | 每条含 evidence_id / action / action_status / triggered_violation_types / risk_locations / reason_code / sensitivity_before / sensitivity_after |

**风险值脱敏护栏**：`build_evidence_action` 拒绝 `risk_locations[*]` 中除
`field_path` + `risk_type` 之外的任何键、长度 > 128 字符的字符串、空
`evidence_id` —— 审计日志不能成为二次泄露源。

默认 audit sink 复用 `JsonlSink`（已有日志 sink）。ES 后端只需提供
`Callable[[dict], None]` 接口写 bulk 即可。

## v1.1 标准化错误码

```python
from retrieval_protocol import build_error, derive_status, build_skill_result

err = build_error(code="E_INDEX_MISSING", message="netflow_demo_v3 索引未构建")
# action 自动填 "degrade_to_local",retryable=False

sr = build_skill_result(
    ..., status=derive_status([err]),  # "error"
    evidence=[], errors=[err],
)
```

错误码与缺省 action 表:

| code | 缺省 action | 场景 |
|---|---|---|
| `E_TIMEOUT` | `retry_with_backoff` | 请求超时 |
| `E_RATE_LIMIT` | `retry_with_backoff` | 触发限流 |
| `E_UPSTREAM_FAIL` | `retry_with_backoff` | 上游 ES/Milvus 报错 |
| `E_INDEX_MISSING` | `degrade_to_local` | 索引未构建 |
| `E_BAD_QUERY` | `report_to_user` | query 为空/语法错误 |
| `E_UNSUPPORTED_FILTER` | `report_to_user` | filter 字段不支持 |
| `E_OUT_OF_BUDGET` | `report_to_user` | 超 budget 已截断 |
| `E_PARTIAL_RESULT` | `report_to_user` | 仅返回部分桶/分片 |
| `E_PERMISSION_DENIED` | `abort` | 鉴权/合规拒绝 |
| `E_INTERNAL` | `abort` | 协议库内部断言失败 |

## 在检索 skill 中接入

skill 脚本把本包父目录加入 `sys.path` 即可 import（按脚本所在层级调整 `parents[N]`）：

```python
import sys
from pathlib import Path

# 例：skills/custom/<skill>/scripts/x.py —— parents[3] 即 skills/
_SHARED = Path(__file__).resolve().parents[3] / "_shared"
if str(_SHARED) not in sys.path:
    sys.path.insert(0, str(_SHARED))

from retrieval_protocol import build_network_traffic_skill_result
```

`network-traffic-analysis/scripts/rag_search.py` 已据此接入：`--format skillresult`
会输出符合本协议的 SkillResult JSON（`--format text` / `json` 行为不变）。

## 运行测试

零依赖，标准库 `unittest` 即可：

```bash
cd skills/_shared
python3 -m unittest discover -s retrieval_protocol/tests -t . -v
```

## 编排层：Planner / Aggregator（S2，task.md 反馈 #2/#3）

纯 stdlib，无 LLM、无 Skill 路由、无 LangGraph 接入（那是 S3，单开分支）。

### Planner（`planner.py`）

- `split_budget(total, fan_out, weights=None)`：把总 `budget`（`max_evidence_count` / `max_token_estimate`）按等权或加权拆给 N 个并行 retrieve；整除/取整余数补给第一个任务，保证每字段拆分后 sum 与 total 一致。
- `build_plan(*, parent_query_id, subqueries, total_budget=None)`：把**已路由好**的子问题（`{query_id, envelope, parallel_group?, weight?}`）组织成 `Plan`；给定 `total_budget` 时按 weight 注入各 task 的 `envelope["budget"]`。深拷贝 envelope，不改入参。
- 不含 LLM 拆问题语义与 SkillRouter —— 那是 S3 PlannerMiddleware 的职责。

### Aggregator（`aggregator.py`）

- 对齐契约：`aggregate(*, plan, skill_results)`，`skill_results` 每条 = `{"query_id": str, "skill_result": dict}`。
- 裁剪 5 步：① 桶内按 `dedup_key` 去重（保留 `sort_key` 最优）→ ② 全局按 `sort_key` 排序 → ③ 有 `max_evidence_count` 截前 N → ④ 有 `max_token_estimate` 累计 token 超限即丢后续 → ⑤ 任一裁剪发生 → `status="partial"` + `errors` 追加 `E_OUT_OF_BUDGET`。
- `AggregatorHooks` 可覆盖 `dedup_key`（默认 `payload.evidence_id`）/ `sort_key`（默认 `-score`）/ `token_estimator`（默认 `len(text)//4`）。
- 未对齐到 plan 的桶仍并入结果，不静默丢弃。

> **S3（下分支）**：`PlannerMiddleware`（`before_model` 调 LLM 拆子问题 → `build_plan`）+ `AggregatorMiddleware`（`after_model` 收集 retrieve 型 SkillResult → `Aggregator.aggregate`），挂在 `TodoListMiddleware` 之后、`MemoryMiddleware` 之前，由 `config.configurable.retrieval_planning_enabled`（默认 False）开关。
