# 检测器接入指南

给第 1–7 类违规负责人的自助接入文档。

**一句话版**：在 `detectors/<你的检测器名>/` 下放一个 `manifest.yaml` 声明能力，
写一个满足 `deerflow.compliance.contract.Detector` 协议的实现
（进程内 Python / 子进程 CLI / HTTP 服务三选一），
在 `config/compliance/detectors.yaml` 里 `enabled: true`，加一个单测。

**不需要**读引擎代码，**不需要**改引擎代码，**不需要**装引擎的依赖，
写错了也不会拖垮别的检测器。

---

## 0. 四条硬规矩

1. **只 import `contract.py`**，不要 import 引擎的任何其他模块。
   `test_compliance_decoupling.py` 会 AST 扫描强制这一点。
2. **重依赖走 `subprocess_cli` 或 `http_service`**，不要往主 venv 塞 torch / OpenCV / spaCy。
3. **不要在检测器里做处置决策**。只输出"命中了什么、在哪、多确信、依据什么"，
   处置由 `违规 × 场景` 矩阵统一决定（指南 §9.1 硬要求）。
4. **不要依赖 `ctx.scenes`**。一期它恒为空 tuple，永远不要写 `if ctx.scenes[0] == ...`。

---

## 1. 契约

`backend/packages/harness/deerflow/compliance/contract.py` 是你唯一需要读的文件。
它是**纯标准库**的，没有任何第三方依赖，在精简环境里也能 import。

```python
from deerflow.compliance.contract import (
    DetectContext, DetectionHit, DetectionUnit, RiskLocation,
)


class MyDetector:
    detector_id = "regex_struct_id"          # 必须与 manifest 里的 detector_id 一致

    def setup(self, params):
        """注册时调用一次。params = manifest 的 default_params + 中心配置的覆盖。"""
        self._pattern = re.compile(params["pattern"])

    def detect(self, unit: DetectionUnit, ctx: DetectContext) -> list[DetectionHit]:
        """检查一个 unit。不要抛异常，不要修改 unit。"""
        hits = []
        for item in unit.text_items:
            for match in self._pattern.finditer(item.text):
                hits.append(DetectionHit(
                    detector_id=self.detector_id,
                    violation_type="struct_id",
                    confidence=0.95,
                    severity="high",
                    risk_locations=(RiskLocation(
                        kind="char_span",
                        locator=f"{match.start()}:{match.end()}",
                        text=match.group(),
                        entity_type="id_card",
                    ),),
                    reason_code="regex_id_card_18",
                    evidence={"pattern": self._pattern.pattern},
                ))
        return hits
```

不强制继承任何基类 —— `Detector` 是一个 `Protocol`，鸭子类型即可。

### 你会拿到什么

| 字段 | 说明 |
|---|---|
| `unit.text_items` | 自由文本，每条带 `source`（evidence_id / "query" / "output" / 文件路径） |
| `unit.field_items` | 结构化字段，`path` 是点号路径（如 `metadata.camera_id`） |
| `unit.data_type` | 数据类型，可能是 `None`（未知） |
| `unit.gate` | 当前闸门 |
| `unit.raw` | 只读原始 payload（`MappingProxyType`，改不了） |
| `ctx.intent` | 意图识别结果，可能是 `None` |
| `ctx.budget_ms` | 本次检测的时间预算 |
| `ctx.scenes` | **一期恒为空**，不要用 |

拿不到 agent state、messages、sandbox —— 这是有意的数据边界。

### `risk_locations` 值得认真填

处置层靠它做**外科手术式**的脱敏：能定位到具体字符区间或字段路径时，
只遮蔽那一小段；定位不到时只能整段拒答。填得好，可用性差别很大。

| `kind` | `locator` 格式 | 用于 |
|---|---|---|
| `char_span` | `"12:34"`（起止字符下标） | 文本内的精确片段 |
| `field_path` | `"metadata.camera_id"` | 结构化字段 |
| `frame` | 帧号 | 视频 |
| `bbox` | 边界框 | 图像 |
| `coordinate` | 坐标 | 地理数据 |

同时填 `text`（命中的原文）会更稳 —— 处置层优先按字面量替换，
不受下标漂移影响。

---

## 2. 能力声明 `manifest.yaml`

放在 `backend/packages/harness/deerflow/compliance/detectors/<你的名字>/manifest.yaml`。
这是**单一事实来源**，引擎启动时自动扫描发现。检测器目录搬走或删掉，声明跟着走。

```yaml
contract_version: "1.0"
detector_id: regex_struct_id
adapter: inprocess
entry: deerflow.compliance.detectors.regex_struct_id.detector:StructIdDetector

violation_types: [struct_id]
gates:
  struct_id: [InputGate, ContextGate, OutputGate]

data_types: null # null = 不限；也可以写 [surveillance, telecom]
cost_hint: light # light | medium | heavy

default_params:
  pattern: "\\b\\d{17}[\\dXx]\\b"
```

### 启动期校验（不通过直接报错，不会静默降级）

- `contract_version` 主版本必须匹配
- `violation_types` ⊆ 10 类，`gates` ⊆ 3 闸
- `gates` 必须覆盖你声明的每个 `violation_types`
- **`cost_hint: heavy` 且声明了 `ContextGate` → 启动即报错**
  （落实指南 §1.2.3"上下文闸不调重型 LLM"；上下文闸是性能敏感点）
- `detector_id` 全局唯一

### 闸门有硬约束

指南把某些违规类型钉死在特定闸门：

- 第 3 类 `hardcoded_cred`：**只在 InputGate** 检测
- 第 9 类 `re_identify`：**只在 OutputGate** 检测

引擎会做双重校验：路由时按 manifest 过滤，拿到结果后再按 manifest 过滤一次。
声明错了，你的命中会被丢弃并记 warning。

---

## 3. 三种接入方式，三选一

| adapter | 什么时候用 | 隔离级别 |
|---|---|---|
| `inprocess` | 纯 Python、依赖轻（正则、词典、轻量模型） | 无 |
| `subprocess_cli` | 需要重依赖（torch/OpenCV/spaCy），或不是 Python 写的 | 进程 |
| `http_service` | 已有独立服务、需要 GPU、需要独立扩缩容 | 网络 |

**三种方式在引擎眼里完全一样。** 后两种天然带进程隔离，崩了也影响不到主服务。

### `subprocess_cli`

stdin 收一个 JSON，stdout 吐一个 JSON。

```yaml
adapter: subprocess_cli
command: ["/opt/my-detector/venv/bin/python", "/opt/my-detector/cli.py"]
timeout_ms: 2000
```

```python
# cli.py
import json, sys
request = json.load(sys.stdin)
unit, ctx, params = request["unit"], request["ctx"], request.get("params", {})
hits = [...]                              # 你的逻辑
print(json.dumps({"hits": hits}, ensure_ascii=False))
```

只返回一个裸数组 `[...]` 也接受，不必包 `{"hits": ...}`。

### `http_service`

```yaml
adapter: http_service
endpoint: "http://detector-svc:8080/detect"
timeout_ms: 2000
headers: { Authorization: "Bearer xxx" }
```

`POST /detect`，body 和 response 与上面的 JSON 完全一致。

### JSON 结构不用手抄

```bash
make compliance-export-schema
```

生成 `docs/compliance-detector-contract.schema.json`，
是从 `contract.py` 的 dataclass 直接导出的 JSON Schema，跨语言接入直接拿去用。

**跨进程/跨网络返回的结果一律会被校验**（`contract.validate_hit`）：
未知的 `violation_type`、超出 `[0,1]` 的 `confidence`、非法的 `severity` 都会被拒绝。
管道另一头是不可信输入，不是可信调用。

---

## 4. 开关与调参

`config/compliance/detectors.yaml` **只管开关和参数覆盖**，不重复声明能力：

```yaml
detectors:
  - id: regex_struct_id
    enabled: true
    params: { pattern: "..." } # 覆盖 manifest 的 default_params
```

未在此列出的检测器，被发现即默认启用。显式写 `enabled: false` 才会跳过。

---

## 5. 加单测

TDD 在本项目是**强制**的（`backend/CLAUDE.md`："Every new feature or bug fix MUST be
accompanied by unit tests. No exceptions."）。

写在 `backend/tests/test_compliance_<你的检测器>.py`。至少覆盖：

```python
def test_detects_a_real_positive():
    detector = MyDetector(); detector.setup({...})
    unit = DetectionUnit(unit_id="u", gate="ContextGate",
                         text_items=(TextItem(item_id="t", text="身份证 310101199001011234", source="x"),))
    hits = detector.detect(unit, DetectContext(gate="ContextGate"))
    assert hits[0].violation_type == "struct_id"

def test_ignores_a_negative():
    ...

def test_risk_locations_point_at_the_actual_span():
    ...   # 处置层靠它做精准脱敏，务必覆盖

def test_satisfies_the_contract_protocol():
    from deerflow.compliance.contract import Detector
    assert isinstance(MyDetector(), Detector)
```

跑测试：

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_compliance_<你的检测器>.py -v
cd backend && make test          # 提交前跑全量
```

---

## 6. 常见问题

**Q：同一个违规类型能不能有多个检测器？**
能，而且鼓励。模型类 + 规则类基线并存，可以互相做审计对照。
引擎会把所有命中一起交给矩阵，取最强处置。

**Q：我的检测器抛异常了会怎样？**
只影响它自己。引擎逐检测器 try/except + 独立计时，其余检测器照常出结果，
异常记进 `diagnostics.detector_errors` 和 warning，不会静默吞掉。

**Q：超时了会怎样？**
`ctx.budget_ms` 是本次检测的总预算。超预算时引擎会停止后续检测并
**显式写 warning 说明哪些没跑**，不做静默截断。
子进程/HTTP adapter 另有自己的 `timeout_ms`。

**Q：为什么契约里没有 `tech` 字段（标注指南里的 T1–T6 原子检测器分类）？**
本项目不采用那套分类。实现技术是检测器内部私事，引擎不需要知道也不应该知道 ——
这本身就是解耦要求的一部分。检测器只按
`违规类型 × 闸门 × 数据类型 × 开销等级` 描述自己。

**Q：我能不能在检测器里直接拒答？**
不能。检测器只输出 `DetectionHit`。处置由矩阵决定（指南 §9.1 硬要求：识别与处置必须解耦）。
如果你觉得某个命中必须拒答，去 `config/compliance/policy_matrix.yaml` 里改矩阵，
或者跟合规负责人确认。

**Q：`severity` 怎么定？**
影响不了处置动作（那是矩阵的事），但会进审计留痕，也可以作为人工复核队列的排序依据。
按你对"这条命中有多确定 + 后果多严重"的判断填。

**Q：我怎么知道我的检测器被加载了？**

```bash
cd backend && PYTHONPATH=.:packages/harness python3 -c "
from deerflow.compliance.registry import build_registry
r = build_registry(config_path='../config/compliance/detectors.yaml', strict=True)
print([(x.detector_id, x.adapter_name, x.enabled) for x in r.all()])"
```

`strict=True` 会把 manifest 里的任何问题直接抛出来，方便定位。

---

## 7. 参考实现

`detectors/model_tfidf_knn/` 是一个完整的 `inprocess` 例子，覆盖第 8/9/10 类：

- `manifest.yaml` — 能力声明，含 `content_text_spec` 这类自定义元数据
- `detector.py` — 闸门过滤、低相似度降级、模型单例缓存
- `row_builder.py` — 私有的输入构造逻辑（引擎不需要知道）
- `vendor/` — 第三方交付代码零改动平移
- 对应测试：`backend/tests/test_compliance_model_detector.py`
