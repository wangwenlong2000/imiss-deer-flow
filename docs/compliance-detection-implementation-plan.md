# imiss-deer-flow 合规违规检测接入实施计划

> 版本：v2（2026-07-26）
> 依据材料：`合规检测/合规评测标注指南7.23.docx`、`合规检测/提交.docx`、`合规检测/violation_detection_model_normalized_723.zip`
> 负责人范围：**模型类检测器（video_meta_leak / re_identify / domain）接入**，并为其余检测器预留统一接入位
>
> **v2 相对 v1 的修改**
> 1. 检测器彻底解耦：契约独立成 `contract.py`，检测器自带 manifest 独立成包，支持进程内/子进程/HTTP 三种接入方式（§4）
> 2. 废弃 T1–T6 原子检测器分类，不再出现在任何接口、配置和文档中
> 3. OutputGate 改为**流式 + 命中即撤回**，不做缓冲（§6.3）
> 4. 场景（scene）一期留空，矩阵走 `_unknown` 兜底列，预留 `SceneResolver` 接口（§7.2）
> 5. **`content_text` 拼接规则已反推完成并全量验证通过**（§5.1）

---

## 0. 范围确认

按 `合规评测标注指南7.23.docx` 第 1.2.1 节的 10 类编号，我负责的三类：

| # | 违规类型 | 技术代号 | 是否在交付包模型里 |
|---|---|---|---|
| 8 | 视频监控点位元数据 | `video_meta_leak` | ✅ 在 |
| 9 | 模型输出再识别风险 | `re_identify` | ✅ 在 |
| 10 | 城市治理领域专有敏感信息 | `domain` | ✅ 在 |

交付包 `model_detectors/README.md` 写明"本目录是第 8/9/10 类违规检测的模型类实现"，实测模型标签集：

```
['none', 'video_meta_leak', 're_identify', 'domain']
```

---

## 1. 材料结论速览

### 1.1 三维框架

- **维度 1**：10 类违规类型（`struct_id` / `geo_loc` / `hardcoded_cred` / `illegal_content` / `political` / `text_id` / `confidential` / `video_meta_leak` / `re_identify` / `domain`）
- **维度 2**：3 道闸 —— `InputGate`（query/上传数据进系统时）、`ContextGate`（检索完成后、进 LLM 上下文前）、`OutputGate`（LLM 生成后、推给用户前）
- **维度 3**：5 类场景 —— `self_use` / `internal_org` / `cross_org` / `public_release` / `research_anon`

### 1.2 架构要求（来自指南，必须遵守）

1. **不为每道闸单独造检测系统**，三道闸共用一套引擎、一套检测器集合、一套处置矩阵。
2. **识别与处置必须解耦**：检测器只输出"命中了什么"，处置由 `违规 × 场景` 二维矩阵决定。
3. **ContextGate 是性能敏感点**：一期只做规则/词典/标签/轻量模型，**不调重型 LLM**。
4. **关键词命中 ≠ 违规**：InputGate 必须"敏感实体 + 高风险意图 + 场景"三者同时成立才判违规。
5. **合规依据必须留痕**：每次判定要能回答"依据哪条规则/法律/制度"。
6. 第 3 类只在 InputGate 检测；第 9 类只在 OutputGate 检测。

> 指南里的 T1–T6"原子检测工具"分类**本计划不采用**。检测器只按 `违规类型 × 闸门 × 数据类型 × 开销等级` 描述自己，实现技术是检测器内部私事，引擎不需要知道，也不应该知道——这本身就是解耦要求的一部分。

### 1.3 我负责的三个模型检测器（实测数据）

| 项目 | 实测值 |
|---|---|
| 方法 | TF-IDF + kNN（k=1，cosine），**纯 Python 标准库，无第三方 ML 依赖** |
| 模型加载 | 0.20 s（`ml_detector_0624_fresh.json`，6.7 MB） |
| 单条推理 | **约 13 ms**（226 条训练样本的模型） |
| 0624 测试集 | accuracy 0.9821，macro_f1 0.9859（56 条，1 处误判） |
| 输入 row | `{data_type, gate, content_text, features:{字段路径: 值}}` |
| 可用 API | `TfidfKNNModel.load()` / `.predict()` / `.predict_proba()` / `.nearest_neighbors()` / `.explain()` |

> ⚠️ 更大的模型文件（`ml_detector.json`，654 条训练样本、11.7 MB）推理耗时按训练集规模线性增长，预计 ~40 ms/条，**且它的 `content_text` 规范与生产模型不同（见 §5.1），不可混用**。

---

## 2. 总体架构

```
                            ┌─────────────────────────────────────────┐
                            │      ComplianceEngine（统一入口）        │
                            │                                         │
  DetectionRequest  ──────► │  1. Normalizer   内容规范化（可插拔）    │
  (gate, scene, payload)    │     └─ text_items[] / field_items[]      │
                            │  2. Router       动态路由候选检测器      │
                            │     └─ 按 gate × data_type × 违规类型    │
                            │  3. 并行 detect()  多检测器并行 + 隔离   │
                            │  4. IntentGuard  意图与权限校验          │
                            │  5. PolicyMatrix 违规×场景 → 处置动作    │
                            │  6. Auditor      留痕                    │
                            └──────────────┬──────────────────────────┘
                                           │  ComplianceDecision
                    ┌──────────────────────┼──────────────────────┐
                    ▼                      ▼                      ▼
             InputGate 挂载点        ContextGate 挂载点       OutputGate 挂载点
        (before_agent + 上传路由)   (wrap_tool_call)      (after_model + 流式撤回)
```

### 2.1 放在 harness 还是 app？

**必须放在 harness**：`backend/packages/harness/deerflow/compliance/`。

原因：`backend/tests/test_harness_boundary.py` 在 CI 里强制 `deerflow.*` 永不 import `app.*`。三道闸中有两道（ContextGate / OutputGate）实现为 LangGraph middleware，本身就在 harness 里；InputGate 的上传扫描在 `app/gateway/routers/uploads.py`，由 app 侧**调用** harness 的引擎——方向合法。

---

## 3. 目录结构

```
backend/packages/harness/deerflow/compliance/
├── __init__.py
├── contract.py                  # ★ 检测器契约（检测器作者唯一需要 import 的模块）
├── types.py                     # 引擎内部数据契约
├── engine.py                    # ComplianceEngine：编排 1→6 步（不 import 任何具体检测器）
├── registry.py                  # 检测器注册表（唯一做反射装载的地方）
├── router.py                    # 候选检测器动态路由
├── policy.py                    # 违规 × 场景 处置矩阵（读 YAML）
├── scene.py                     # SceneResolver 接口（一期返回 None）
├── intent.py                    # 意图与权限校验
├── audit.py                     # 判定留痕（JSONL）
├── actions.py                   # 处置动作执行器（脱敏/模糊化/聚合/改写/拒答…）
├── normalizers/                 # 可插拔：不同来源 → DetectionUnit
│   ├── base.py
│   ├── skill_result.py          # SkillResult.evidence[] → unit（ContextGate）
│   ├── llm_output.py            # AIMessage → unit（OutputGate）
│   └── user_input.py            # query / 上传文件 → unit（InputGate）
├── adapters/                    # ★ 三种接入方式，检测器作者三选一
│   ├── inprocess.py             # 进程内 Python 类
│   ├── subprocess_cli.py        # 子进程 CLI（stdin JSON → stdout JSON）
│   └── http_service.py          # 独立 HTTP 服务
└── detectors/
    ├── __init__.py              # 空，禁止在这里 import 具体检测器
    └── model_tfidf_knn/         # ★ 我负责，自成一个独立包
        ├── manifest.yaml        # 能力声明（单一事实来源）
        ├── detector.py
        ├── row_builder.py       # content_text 拼接（§5.1）
        ├── vendor/              # 交付包代码零改动平移
        │   └── model_detectors/
        └── tests/

backend/packages/harness/deerflow/config/
└── compliance_config.py

backend/packages/harness/deerflow/agents/middlewares/
├── compliance_input_gate_middleware.py
├── compliance_context_gate_middleware.py
└── compliance_output_gate_middleware.py

config/compliance/
├── policy_matrix.yaml
└── detectors.yaml               # 只写"启用哪些 + 参数覆盖"，不重复声明能力

models/compliance/ml_detector_0624_fresh.json    # gitignore，见 §8
datasets/compliance/normalized/…                 # gitignore 大文件

scripts/
├── run_compliance_model_eval.py
├── run_compliance_gate_eval.py
└── verify_content_text_rule.py  # ★ §5.1 规则的回归校验
```

---

## 4. 解耦设计（本次重点修改）

解耦的目标很具体：**新增一个检测器，不需要读引擎代码，不需要改引擎代码，不需要装引擎的依赖，写错了也不会拖垮别的检测器。**

### 4.1 四条边界

| 边界 | 规则 | 强制手段 |
|---|---|---|
| **代码边界** | `engine.py` / `router.py` / `policy.py` **永不 import 任何具体检测器**；只有 `registry.py` 做反射装载 | 单测扫 AST，发现 `from deerflow.compliance.detectors.<具体名>` 直接 fail |
| **依赖边界** | 检测器的第三方依赖不进主 venv —— 需要 torch/spacy/OpenCV 的走子进程或 HTTP 适配器 | 适配器隔离，见 §4.4 |
| **数据边界** | 检测器只拿到只读 `DetectionUnit` + `DetectContext`，拿不到 agent state、messages、sandbox | 传入前 `deepcopy` + frozen dataclass |
| **故障边界** | 单个检测器超时/抛异常/返回非法结果 → 只影响它自己，记 warning，其余检测器照常出结果 | 引擎逐检测器 try/except + 独立超时 |

### 4.2 检测器契约 `contract.py`

**这是检测器作者唯一需要 import 的模块**，独立于引擎内部实现，单独版本号。

```python
CONTRACT_VERSION = "1.0"

Gate  = Literal["InputGate", "ContextGate", "OutputGate"]
ViolationType = Literal[
    "struct_id", "geo_loc", "hardcoded_cred", "illegal_content", "political",
    "text_id", "confidential", "video_meta_leak", "re_identify", "domain",
]

@dataclass(frozen=True)
class TextItem:
    item_id: str
    text: str
    source: str                     # evidence_id / "query" / "output" / 文件路径

@dataclass(frozen=True)
class FieldItem:
    item_id: str
    path: str                       # 如 "features.camera_id"
    value: Any
    source: str

@dataclass(frozen=True)
class DetectionUnit:
    unit_id: str
    gate: Gate
    data_type: str | None
    text_items: tuple[TextItem, ...]
    field_items: tuple[FieldItem, ...]
    raw: Mapping[str, Any]          # 只读原始 payload

@dataclass(frozen=True)
class DetectContext:
    gate: Gate
    scenes: tuple[Scene, ...]       # 一期恒为空 tuple，见 §7.2
    user: UserContext | None
    intent: IntentInfo | None
    budget_ms: int

@dataclass(frozen=True)
class RiskLocation:
    kind: Literal["field_path", "char_span", "frame", "bbox", "coordinate"]
    locator: str                    # 字段路径 / "12:34" / 帧号 …
    text: str | None = None
    entity_type: str | None = None

@dataclass(frozen=True)
class DetectionHit:                 # 检测器唯一输出物
    detector_id: str
    violation_type: ViolationType
    confidence: float               # 0–1
    severity: Literal["info", "low", "medium", "high", "critical"]
    risk_locations: tuple[RiskLocation, ...]
    reason_code: str
    evidence: Mapping[str, Any]     # 审计用，结构自由
    basis: tuple[str, ...] = ()     # 合规依据条款


class Detector(Protocol):
    """检测器只需满足这个协议。不强制继承，鸭子类型即可。"""

    def setup(self, params: Mapping[str, Any]) -> None: ...

    def detect(self, unit: DetectionUnit, ctx: DetectContext) -> Sequence[DetectionHit]: ...
```

**契约里没有 `tech` 字段**——实现技术是检测器内部私事。

### 4.3 能力声明 `manifest.yaml`（单一事实来源）

能力声明跟检测器代码放在一起，而不是散落在中心 YAML 里。检测器搬走/删掉，声明跟着走。

`detectors/model_tfidf_knn/manifest.yaml`
```yaml
contract_version: "1.0"
detector_id: model_tfidf_knn
adapter: inprocess
entry: deerflow.compliance.detectors.model_tfidf_knn.detector:ModelTfidfKnnDetector

violation_types: [video_meta_leak, re_identify, domain]
gates:
  video_meta_leak: [ContextGate, OutputGate]
  re_identify:     [OutputGate]          # 指南硬约束：只在输出闸
  domain:          [ContextGate, OutputGate]
data_types: null                          # null = 不限
cost_hint: light                          # light | medium | heavy

default_params:
  model_path: models/compliance/ml_detector_0624_fresh.json
  min_confidence: 0.35
  min_similarity: 0.20
  max_units_per_call: 32
  timeout_ms: 400
```

中心配置 `config/compliance/detectors.yaml` **只管开关和参数覆盖**，不重复声明能力：

```yaml
detectors:
  - id: model_tfidf_knn
    enabled: true
    params: { min_similarity: 0.25 }     # 覆盖 manifest 的 default_params

  # 其他负责人的检测器，写好后在这里开一行即可
  - { id: regex_struct_id,        enabled: false }
  - { id: regex_geo_loc,          enabled: false }
  - { id: entropy_hardcoded_cred, enabled: false }
  - { id: keyword_illegal_content,enabled: false }
  - { id: keyword_political,      enabled: false }
  - { id: ner_text_id,            enabled: false }
  - { id: llm_confidential,       enabled: false }
```

`registry.py` 启动时扫描 `detectors/*/manifest.yaml` 自动发现，校验：
- `contract_version` 兼容
- `violation_types` ⊆ 10 类、`gates` ⊆ 3 闸
- `cost_hint: heavy` 且声明了 `ContextGate` → **启动即报错**（落实"上下文闸不调重型 LLM"）
- 同一 `violation_type` 允许多个检测器共存（模型类 + 规则类基线做审计对照）

### 4.4 三种接入方式 `adapters/`

这是解耦最实际的一环：**别的负责人不必被我的技术栈绑架**。

| adapter | 适用 | 契约 |
|---|---|---|
| `inprocess` | 纯 Python、依赖轻（我的模型检测器属于这类） | 实现 `Detector` Protocol |
| `subprocess_cli` | 需要重依赖（torch/OpenCV/spacy）或非 Python | stdin 收 `DetectionUnit` JSON → stdout 吐 `DetectionHit[]` JSON |
| `http_service` | 已有独立服务、需要 GPU、需要独立扩缩容 | `POST /detect`，body/response 同上 JSON |

三种方式在引擎眼里**完全一样**——`registry` 按 `adapter` 字段包一层，`router` 和 `engine` 只看到统一的 `Detector` 协议。后两种天然带进程隔离，崩了也影响不到主服务。

JSON 结构由 `contract.py` 的 dataclass 直接导出 JSON Schema（`scripts/export_detector_schema.py`），跨语言接入不用手抄字段。

### 4.5 处置矩阵 `policy.py`

`config/compliance/policy_matrix.yaml` —— 把 docx 的表格原样数据化，**不写死在代码里**：

```yaml
version: "7.23"
matrix:
  video_meta_leak:
    ContextGate:
      _unknown:        [warn, manual_review]    # 一期生效，见 §7.2
      self_use:        [allow]
      internal_org:    [warn]
      cross_org:       [desensitize]
      public_release:  [refuse]
      research_anon:   [aggregate]
    OutputGate: { … }
  re_identify:
    OutputGate:
      _unknown:        [warn, manual_review]
      self_use:        [warn]
      internal_org:    [warn, rewrite]
      cross_org:       [rewrite, warn]
      public_release:  [rewrite, refuse]
      research_anon:   [aggregate, rewrite]
  domain: { … }
```

> 录入以 `合规评测标注指南7.23.docx` 第三章为准（`提交.docx` 第 2 类 research 列写 "k-匿名"，7.23 版写 "聚合化"，以 7.23 为准）。

---

## 5. 我负责的模型检测器实现

### 5.1 ★ `content_text` 拼接规则（已反推完成并全量验证）

这是 v1 里标记为"待解决"的头号风险。**现已解决**，结论如下。

#### 5.1.1 先说一个必须知道的事实

**生成 `content_text` 的归一化器不在交付包里**——包里只有消费它的 `feature_extractor.py`。所以规则只能从数据反推。我对 `normalized/` 全量数据做了反推与验证。

#### 5.1.2 两套数据用的是两套不同规范（重要）

| 数据集 | 训练出的模型 | `content_text` 规范 |
|---|---|---|
| `0624_supported_split/`（282 条） | **`ml_detector_0624_fresh.json`（推荐生产模型）** | `features` 的 **`key: value` 逐行拼接** |
| `all_normalized_800.jsonl`（818 条） | `ml_detector.json` | 递归遍历 `raw_content` 取字符串值拼接，**规则不同且不完整可复现** |

**两者不可混用。** 用 0624 模型就必须用 0624 规范；否则特征分布错位，离线 98% 的准确率线上直接失效。这是本次调查最有价值的发现——如果不查，上线后会是一个极难定位的静默降级。

#### 5.1.3 已验证的规则

```python
def build_content_text(features: dict[str, Any]) -> str:
    """0624 规范。features 的插入顺序即拼接顺序（Python 3.7+ dict 有序）。"""
    if len(features) == 1 and "content" in features:
        return str(features["content"])          # 单 content 字段：不加前缀
    return "\n".join(f"{k}: {v}" for k, v in features.items())
```

#### 5.1.4 验证结果

| 验证项 | 结果 |
|---|---|
| `0624_supported_split/train.jsonl` 字符串精确匹配 | **226 / 226 = 100%** |
| `0624_supported_split/test.jsonl` 字符串精确匹配 | **56 / 56 = 100%** |
| 端到端：仅用 `features` 重建 row 后模型预测一致率 | **56 / 56 = 100%** |
| 重建后测试集 accuracy | **0.9821**（与原始 row 完全相同） |

即：**线上只要拿到 `features`，就能 100% 复现训练时的 `content_text`，模型行为零漂移。**

#### 5.1.5 固化手段

1. 规则实现在 `detectors/model_tfidf_knn/row_builder.py`，**单一实现，禁止在别处重复拼接**。
2. `scripts/verify_content_text_rule.py` 作为回归脚本进 CI：对 0624 全量 282 条断言 100% 精确匹配 + 100% 预测一致率。**任何人改动 row_builder 都会被立刻拦下。**
3. 模型文件与规范版本绑定：`manifest.yaml` 里记 `content_text_spec: "0624"`；换模型必须同步声明规范版本，规范不匹配 → 启动报错。
4. 上线前用真实 SkillResult 抽样跑一遍 `normalizers/skill_result.py` → `row_builder`，人工核对 `features` 的键名是否与训练样本同构（这是剩余的唯一风险点，见 §11 风险 4）。

### 5.2 检测器主体

`detectors/model_tfidf_knn/detector.py`

```python
class ModelTfidfKnnDetector:
    detector_id = "model_tfidf_knn"

    def setup(self, params):
        self._params = params
        self._model = None                 # 懒加载 + 进程内单例（0.2s 加载不放在请求路径）

    def detect(self, unit, ctx):
        model = self._ensure_model()       # lru_cache by model_path，只读可无锁共享
        row = {
            "data_type": unit.data_type,
            "gate": unit.gate,
            "content_text": build_content_text(features := self._features(unit)),
            "features": features,
        }
        label, confidence, probs = model.predict(row)
        if label == "none":
            return []
        if unit.gate not in MANIFEST_GATES[label]:      # re_identify 只在 OutputGate
            return []

        neighbors = model.nearest_neighbors(row, limit=3)
        top_sim = neighbors[0]["similarity"] if neighbors else 0.0
        if top_sim < self._params["min_similarity"]:
            severity = "low"               # 低相似度 → 只走 manual_review，不进拒答/改写
        else:
            severity = self._severity(label, confidence)

        return [DetectionHit(
            detector_id=self.detector_id,
            violation_type=label,
            confidence=confidence,
            severity=severity,
            risk_locations=self._locate(unit, model.explain(row, label)),
            reason_code="ml_tfidf_knn_classifier",
            evidence={
                "probabilities": probs,
                "nearest_neighbors": neighbors,
                "evidence_features": model.explain(row, label, limit=12),
                "model_file": self._params["model_path"],
                "content_text_spec": "0624",
            },
            basis=(),      # 模型类不产出条款依据，由 policy 层按类型补默认依据
        )]
```

要点：

1. **闸门过滤在检测器内做**：模型是四分类，但 `re_identify` 只允许在 OutputGate 生效（指南硬约束）。
2. **低相似度不判实质违规**。交付包 README 明确写了 `k=1` 对否定语境敏感——0624 唯一误判就是一条否定表达的 `none` 被判成 `re_identify`，最近邻相似度只有 0.13。`min_similarity=0.20` 直接治这个病。
3. **vendor 代码零改动平移**，日后交付包升级直接覆盖目录。

---

## 6. 三道闸挂载点

### 6.1 InputGate

| 对象 | 挂载点 |
|---|---|
| 用户 query | 新增 `compliance_input_gate_middleware.py`，`before_agent` 钩子 |
| 上传文件 | [uploads.py](backend/app/gateway/routers/uploads.py) —— **落盘/转换之后、返回成功之前**扫描 |

顺序要求：必须排在 [IntentRecognitionMiddleware](backend/packages/harness/deerflow/agents/middlewares/intent_recognition_middleware.py) **之后**，才能拿到意图识别结果做"敏感实体 + 高风险意图"联合判定（指南 §9.7：不能因为 query 里出现手机号就判违规）。插入位置在 [agent.py:242](backend/packages/harness/deerflow/agents/lead_agent/agent.py#L242) 之后。

> 我的三个模型检测器**不挂 InputGate**。所以 InputGate 一期只建骨架、跑通链路、留好挂载位，检测器由第 1–7 类负责人填。

### 6.2 ContextGate ★ 我负责的主战场

新增 `compliance_context_gate_middleware.py`，实现 `wrap_tool_call` / `awrap_tool_call`：

```python
async def awrap_tool_call(self, request, handler):
    message = await handler(request)
    skill_result = try_parse_skill_result(message.content)   # 只处理 SkillResult 形状
    if skill_result is None:
        return message
    units = SkillResultNormalizer().to_units(skill_result, gate="ContextGate")
    decision = engine.check(units, gate="ContextGate", budget_ms=...)
    if decision.actions:
        message = rewrite_tool_message(message, decision)    # 脱敏/过滤/聚合后回写
    return message
```

三个实现要点：

1. **规范化严格按指南 1.2.3**：`result.evidence[]` → 每条 evidence 的 `data`/`description` 进 `text_items`，`metadata` 里的业务字段展开成 `features.xxx` 路径进 `field_items`。这与训练数据的 `features` 结构天然对齐（也正是 §5.1 规则能用上的原因）。
2. **中间件位置决定失败语义**。当前 [ToolErrorHandlingMiddleware](backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py) 在 `build_lead_runtime_middlewares()` 末尾（外层），会把任何异常吞成 error ToolMessage —— 对合规闸这是**静默失败开门**。因此 ContextGate 中间件要插到该列表**最前面（最外层）**，并自带 `fail_mode: closed|open`：`closed` 时检测异常直接拦截并告警。默认 `closed`。
3. **性能预算**：13 ms × evidence 条数。配置 `max_units: 32` + `budget_ms: 400`，超预算按 evidence `score` 降序截断，并在 `diagnostics.warnings` 写明"本次仅检测前 N 条证据"——**不做静默截断**。

### 6.3 OutputGate ★ 流式 + 命中即撤回（本次修改）

不做缓冲，保持现有流式体验；违规命中后立刻撤回。

#### 6.3.1 双层机制

```
  LLM 逐 token 流出 ──► 前端边收边渲染
          │
          ├─ 第 1 层：增量扫描（可选，配置开启）
          │    每累积 N 字符跑一次 OutputGate → 命中即刻发撤回，越早越少泄露
          │
          └─ 第 2 层：after_model 完整消息检测（必做，兜底）
               │
               命中 ──┬──► ① 发 custom 流事件 compliance_retract
                      │       前端立刻清空该条消息，替换为合规提示 / 改写版
                      │
                      └──► ② after_model 返回改写后的 AIMessage
                              持久层（checkpoint / values 快照）只留脱敏版本
```

**两层是不同职责，都要有**：
- ① 负责"**立刻从屏幕上撤掉**"——解决用户已经看到的问题
- ② 负责"**持久层不留违规原文**"——解决刷新页面、翻历史、导出报告又看到的问题

只做 ① 会导致刷新后违规内容重现；只做 ② 会导致当前屏幕上的内容撤不掉。

#### 6.3.2 撤回信道

前端 [stream-mode.ts](frontend/src/core/api/stream-mode.ts) 已经支持 `custom` 流模式（在 `SUPPORTED_RUN_STREAM_MODES` 里）。用 LangGraph 的 `get_stream_writer()` 发自定义事件，不需要改协议：

```python
# compliance_output_gate_middleware.py
writer = get_stream_writer()
writer({
    "type": "compliance_retract",
    "message_id": ai_message.id,
    "violation_types": [h.violation_type for h in decision.hits],
    "action": decision.actions,               # rewrite / refuse / …
    "replacement": decision.mutated_payload,  # 改写版正文，或标准拒答文案
    "audit_ref": decision.audit_ref,
})
```

需要改动：
- `app/gateway/routers/agents.py`：订阅的 streamMode 加上 `custom`
- 前端消息组件：收到 `compliance_retract` → 按 `message_id` 定位并替换内容，展示合规提示条

#### 6.3.3 执行顺序

LangChain 的 `after_model` 按 middleware 列表**逆序**执行。要让 OutputGate 成为最后一个改写 AIMessage 的环节，它得放在列表靠前位置。**实现时先写一个顺序断言测试钉死，不靠猜。**

#### 6.3.4 诚实说明残留风险

撤回是"事后补救"，不是"事前拦截"：

- 从违规 token 流出到撤回事件到达，中间有一个**可见窗口**（不开增量扫描时 = 整条消息生成完的剩余时长；开增量扫描时 ≈ 扫描间隔）
- 用户在窗口内**截屏或肉眼记住**的内容，技术手段无法收回
- 前端渲染被绕过（直接调 LangGraph SSE 的客户端、IM 渠道）时，撤回事件可能不被处理 —— **IM 渠道（Feishu/Slack/Telegram）必须单独适配**，`feishu.py` 是原地 patch 卡片，天然支持替换；Slack/Telegram 走 `runs.wait()` 拿完整结果，反而不受影响

因此配置项设计为：

```yaml
output:
  enabled: true
  mode: stream_retract              # 不缓冲
  incremental_scan:
    enabled: true
    interval_chars: 200             # 每 200 字符扫一次，权衡泄露窗口 vs 开销
  fail_mode: closed
```

`interval_chars` 是泄露窗口和 CPU 开销的直接权衡旋钮：200 字符 × 13 ms ≈ 每 200 字一次轻量检测，代价可接受。

---

## 7. 配置

### 7.1 `config.yaml` 新增顶级段

```yaml
compliance:
  enabled: true

  gates:
    input:   { enabled: true, fail_mode: closed }
    context: { enabled: true, fail_mode: closed, budget_ms: 400, max_units: 32 }
    output:
      enabled: true
      fail_mode: closed
      mode: stream_retract
      incremental_scan: { enabled: true, interval_chars: 200 }

  detectors_config_path: "config/compliance/detectors.yaml"
  policy_matrix_path:    "config/compliance/policy_matrix.yaml"

  scene:
    resolver: null            # 一期留空，见 §7.2
    fallback_key: _unknown

  audit:
    enabled: true
    path: "backend/.deer-flow/compliance/audit"
    retain_days: 90
```

对应 `backend/packages/harness/deerflow/config/compliance_config.py`（Pydantic，`get_compliance_config()` 单例，与 `get_skill_router_config()` 同构）。

### 7.2 场景（scene）一期留空（本次修改）

系统目前没有"使用权限和公开程度"的信息源，五类场景无从判定。一期处理：

1. `DetectContext.scenes` 恒为**空 tuple**，检测器不得依赖它。
2. `scene.py` 定义接口，一期实现返回 `None`：
   ```python
   class SceneResolver(Protocol):
       def resolve(self, request: DetectionRequest) -> Scene | None: ...

   class NullSceneResolver:
       """一期占位。日后接鉴权模块 / 请求参数 / agent 配置时替换。"""
       def resolve(self, request): return None
   ```
   通过 `compliance.scene.resolver` 配置项换实现，**换的时候不用改引擎**。
3. 矩阵走 `_unknown` 兜底列。**兜底列的填法遵循一个原则：宁可漏处置，不可误伤**——
   - 底线类（`hardcoded_cred` / `illegal_content` / `political`）：`[refuse, warn]`，任何场景都拦，场景未知也拦
   - 其余七类：`[warn, manual_review]`，只告警 + 留痕 + 入人工队列，**不自动脱敏、不自动拒答**

   理由：场景未知时自动脱敏/拒答会在 `self_use`（本该放行）场景下大面积误伤，可用性代价远大于收益；而告警 + 留痕保留了全部审计能力，等 scene 补全后直接切到正式矩阵列。
4. `_unknown` 列的存在是**临时状态**，在 `policy_matrix.yaml` 顶部加显式注释 + 单测断言"scene 补全后必须删除 `_unknown` 列"的 TODO 追踪。

---

## 8. 资产落地

| 资产 | 从 | 到 | 备注 |
|---|---|---|---|
| 模型权重 | zip 内 `model_detectors/models/*.json` | `models/compliance/` | **6.7–11.7 MB，必须 gitignore**；`make compliance-assets` 从 zip 解压 |
| vendor 代码 | zip 内 `model_detectors/*.py` | `detectors/model_tfidf_knn/vendor/` | 入库，零改动 |
| 标注/切分数据 | zip 内 `normalized/` | `datasets/compliance/normalized/` | 只入库 `0624_supported_split/`（评测基线 + §5.1 回归用），其余 gitignore |
| 原始 zip | 仓库根 `violation_detection_model_normalized_723.zip` | 移出仓库或 gitignore | ⚠️ 现在 13 MB 裸放在仓库根目录 |

`SHA256SUMS` 校验写进 `make compliance-assets`。

---

## 9. 测试与评测

项目 `CLAUDE.md` 里 TDD 是**强制**的（"Every new feature or bug fix MUST be accompanied by unit tests. No exceptions."）。

### 9.1 单元测试

| 文件 | 覆盖 |
|---|---|
| `test_compliance_contract.py` | 契约冻结性：dataclass 不可变、JSON Schema 导出稳定 |
| `test_compliance_decoupling.py` | **AST 扫描**：engine/router/policy 不得 import 具体检测器 |
| `test_compliance_registry.py` | manifest 自动发现、非法声明拒绝、`heavy` 挂 ContextGate 必须报错、禁用检测器不被路由 |
| `test_compliance_adapters.py` | 三种 adapter 行为一致；子进程/HTTP 崩溃时故障隔离生效 |
| `test_compliance_policy_matrix.py` | 矩阵全覆盖、`_unknown` 兜底列存在、底线三类在所有场景（含 `_unknown`）都拦截 |
| `test_compliance_row_builder.py` | ★ §5.1 规则：282 条 100% 精确匹配 + 100% 预测一致率 |
| `test_compliance_model_detector.py` | 低相似度降级、`re_identify` 不在 ContextGate 触发、模型单例只加载一次 |
| `test_compliance_gates.py` | 三个 middleware 挂载顺序、`after_model` 逆序断言、`fail_mode` 语义、预算截断写 warning |
| `test_compliance_output_retract.py` | 撤回事件结构、state 改写后 checkpoint 不含违规原文、增量扫描触发时机 |

### 9.2 离线评测（复用交付包口径）

```bash
python3 scripts/run_compliance_model_eval.py \
  --input datasets/compliance/normalized/0624_supported_split/test.jsonl \
  --model models/compliance/ml_detector_0624_fresh.json \
  --min-accuracy 0.98
```

基线（必须复现）：`accuracy = 0.9821`、`macro_f1 = 0.9859`、`mismatches = 1/56`。

**矩阵交叉校验**：标注样本自带 `expected_action`（按场景的预期处置），可直接验证 `policy_matrix.yaml` 录入是否正确——跑全量样本，比对"矩阵推出的动作" vs "标注的 expected_action"，不一致即为录入错误或标注争议。白捡的一致性测试。

### 9.3 端到端闸门评测

`scripts/run_compliance_gate_eval.py`：按标注样本的 `trigger_gate` 分组，构造对应闸的 payload，走完整 `ComplianceEngine`，产出指南第七章要求的四组指标：主指标（违规类型 P/R/F1）、按闸门、按场景、按数据类型。输出落 `outputs/compliance-eval/{timestamp}/`。

> ⚠️ 场景维度指标一期只能产出 `_unknown` 一列，等 scene 补全后才有完整数据。报告里要写明，不要让空白看起来像"通过"。

---

## 10. 分阶段实施

| 阶段 | 内容 | 产出 | 预估 |
|---|---|---|---|
| **P0 骨架** | `contract.py` / `registry.py`（manifest 发现）/ `adapters` / `policy.py` / 配置 + 解耦 AST 单测 | 空引擎可加载配置、0 个检测器不报错、契约可导出 JSON Schema | 2 天 |
| **P1 我的检测器** | vendor 平移、`row_builder`（§5.1 已定稿，直接实现）、`ModelTfidfKnnDetector`、离线评测复现 0.9821 | 三类模型检测器独立跑通 + 回归脚本进 CI | 1.5 天 |
| **P2 ContextGate** | `skill_result` normalizer、`wrap_tool_call` 中间件、预算/失败语义、脱敏动作 | 真实 skill 链路上能拦截并脱敏 | 2 天 |
| **P3 OutputGate + 撤回** | `after_model` 中间件、custom 撤回事件、gateway `custom` 流模式、前端替换逻辑、增量扫描、IM 渠道适配 | `re_identify` 在最终答复上生效且能撤回 | 2.5 天 |
| **P4 InputGate 骨架** | middleware + uploads 扫描点 + 意图联合判定框架（检测器留空） | 别人的检测器可直接插进来 | 1 天 |
| **P5 留痕与评测** | `audit.py`、端到端评测脚本、四组指标报告 | 可交付的评测报告 | 1.5 天 |
| **P6 文档** | 更新 `README.md` / `backend/CLAUDE.md`（项目强制要求）+《检测器接入指南》+ 契约 JSON Schema | 其他负责人自助接入 | 0.5 天 |

合计约 **11 人天**。P3 比 v1 增加 1 天（撤回涉及前端 + IM 渠道）；P1 比 v1 减少 0.5 天（`content_text` 规则已定稿，省掉反推工作）。

---

## 11. 风险与待确认事项

| # | 事项 | 状态 | 处理 |
|---|---|---|---|
| 1 | ~~"789" 编号口径~~ | ✅ 已确认 | 8/9/10 三类 |
| 2 | ~~`content_text` 拼接规则未文档化~~ | ✅ **已解决** | §5.1，282/282 精确匹配 + 100% 预测一致率，回归脚本进 CI |
| 3 | **两套数据集 `content_text` 规范不同** | 🆕 新发现 | 生产只用 0624 模型 + 0624 规范；`manifest.yaml` 记 `content_text_spec` 并做启动校验，规范不匹配直接报错 |
| 4 | **线上 `features` 键名与训练样本是否同构** | ⚠️ 剩余风险 | §5.1.5 第 4 条：上线前用真实 SkillResult 抽样人工核对。这是 `content_text` 链路上唯一还没被测试覆盖的环节 |
| 5 | 场景（scene）无信息源 | 🔵 一期留空 | §7.2：`_unknown` 兜底列，底线类照拦，其余只告警不误伤；`SceneResolver` 接口已留 |
| 6 | 流式撤回有可见窗口 | ⚠️ 设计取舍 | §6.3.4：已选定撤回方案，用 `interval_chars` 调节窗口；截屏无法收回，属于方案固有代价 |
| 7 | IM 渠道撤回适配 | ⚠️ 待实现 | Feishu 原地 patch 卡片天然支持；Slack/Telegram 走 `runs.wait()` 不受影响；需在 P3 逐一验证 |
| 8 | `k=1` 对否定语境敏感 | ⚠️ 已知 1/56 | `min_similarity=0.20` + 低置信降级 `manual_review`；后续补 hard negative 重训或试 k=3/5 |
| 9 | 训练集只有 226 条 | ⚠️ 泛化无保证 | 明确定位为一期基线；新数据类型上线前必须重新切分训练 |
| 10 | 模型文件 6.7–11.7 MB | 🔵 已有方案 | gitignore + `make compliance-assets` + SHA256 校验 |
| 11 | ContextGate 延迟 | 🔵 已有方案 | 预算 400 ms + 截断显式告警 |
| 12 | 用户权限信息源缺失 | 🔵 一期降级 | `role_check` 降级为 `warn` + 审计，等鉴权模块就绪再打开 |

---

## 12. 给其他检测器负责人的接入契约（一句话版）

> 在 `detectors/<你的检测器名>/` 下放一个 `manifest.yaml` 声明能力，写一个满足 `deerflow.compliance.contract.Detector` 协议的实现（进程内 Python / 子进程 CLI / HTTP 服务三选一），在 `config/compliance/detectors.yaml` 里 `enabled: true`，加一个单测。
>
> - **只 import `contract.py`**，不要 import 引擎的任何其他模块。
> - **重依赖走 `subprocess_cli` 或 `http_service` adapter**，不要往主 venv 塞 torch。
> - **不要在检测器里做处置决策**——只输出"命中了什么、在哪、多确信、依据什么"，处置由矩阵统一决定（指南 §9.1 硬要求）。
> - **不要依赖 `ctx.scenes`**，一期它恒为空。
