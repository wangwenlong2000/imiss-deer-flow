# Skill 输入输出规范改造指南

> 本指南面向各业务 Skill 开发人员，说明如何把现有 Skill 逐步接入统一输入输出规范。第一阶段不要求重写现有 Skill 实现，重点是统一接口说明、调度输入、输出结构和校验方式。

## 1. 改造目标

本次改造要统一三件事：

1. Skill 调用输入统一为 `SkillInputEnvelope`；
2. Skill 执行输出统一为 `SkillResult`；
3. `SKILL.md` 作为 Skill 行为说明的唯一真相来源，明确本 Skill 如何消费输入、如何产出输出。

对应的机器可校验 Schema：

```text
skills/skill_input_envelope.schema.json
skills/skill_result.schema.json
skills/router_card.schema.json
```

## 2. 总体原则

### 2.1 不要求现有 Skill 立即重写实现

现有 Skill 已经能运行的，不需要第一阶段修改脚本、命令行参数或内部处理逻辑。

推荐接入方式：

```text
Planner / SkillRouter
  ↓
SkillInputEnvelope
  ↓
Adapter
  ↓
现有 Skill 实现
  ↓
SkillResult
```

Adapter 负责把统一输入转换为现有 Skill 需要的形式，例如：

- 文件路径；
- CLI 参数；
- 环境变量；
- `selected_data_sources`；
- 现有配置文件或数据源引用。

### 2.2 Schema 管统一底座，SKILL.md 管本 Skill 细节

不要在每个 `SKILL.md` 里复制完整 JSON Schema。

正确做法：

```text
SkillInputEnvelope / SkillResult Schema
  ↓
定义统一字段、类型、状态、错误结构

SKILL.md
  ↓
说明本 Skill 需要哪些输入字段、如何解释参数、输出哪些 findings/evidence/artifacts
```

### 2.3 retrieve 不另起一套协议

retrieve 类 Skill 也使用同一个 `SkillInputEnvelope` 和 `SkillResult`。

retrieve 特有字段放在：

```text
输入侧：
- query.sub_queries[].query_id
- budget.max_evidence_count
- budget.max_token_estimate

输出侧：
- result.evidence[].query_id
- result.evidence[].token_estimate
- diagnostics.retrieval
```

## 3. 每个 Skill 需要改什么

### 3.1 必须确认 SKILL.md front matter

每个 `SKILL.md` 必须包含：

```yaml
---
name: <skill-id>
description: <一句话触发导向描述>
---
```

要求：

- `name` 必须和 Skill 目录名一致；
- `description` 描述“适合处理什么任务”，不要写成内部实现细节；
- 如果已有 `metadata`，应尽量包含 `scenes`、`task_types`、`input_types`、`output_types`。

### 3.2 新增或补齐 Input handling

每个 Skill 建议在 `SKILL.md` 中加入：

```markdown
## Input handling

This Skill accepts the unified SkillInputEnvelope.

Required:
- `query.raw`: 用户原始任务描述
- `inputs.files[]`: ...

Optional:
- `inputs.data_sources[]`: ...
- `inputs.parameters.<name>`: ...
- `budget.max_evidence_count`: ...
- `budget.max_token_estimate`: ...

Adapter behavior:
- ...
```

示例：

```markdown
## Input handling

This Skill accepts the unified SkillInputEnvelope.

Required:
- `query.raw`: 用户对流量分析的原始问题
- `inputs.files[]`: 至少一个 `.pcap`, `.pcapng`, `.cap`, 或 `.csv`

Optional:
- `inputs.parameters.view`: `auto`, `flow`, 或 `packet`
- `inputs.data_sources[]`: 已注册的数据集或历史索引
- `budget.max_evidence_count`: 返回证据条数上限
- `budget.max_token_estimate`: 返回证据 token 预算

Adapter behavior:
- `.pcap/.pcapng/.cap` 输入转为 `prepare_pcap.py --input <path>`
- `.csv` 输入转为 `analyze.py --input <path>`
- `inputs.parameters.view` 转为 `--view`
```

### 3.3 新增或补齐 Output handling

每个 Skill 建议在 `SKILL.md` 中加入：

```markdown
## Output handling

This Skill returns the unified SkillResult envelope.

SkillResult mapping:
- `skill_name`: <skill-id>
- `scenario`: <scene>
- `capability`: <capability>
- `result.summary`: ...
- `result.findings`: ...
- `result.evidence`: ...
- `result.artifacts`: ...
- `diagnostics.warnings`: ...
- `errors`: ...
```

示例：

```markdown
## Output handling

This Skill returns the unified SkillResult envelope.

SkillResult mapping:
- `skill_name`: `network-traffic-analysis`
- `scenario`: `network_traffic`
- `capability`: 当前执行的分析能力，例如 `pcap_analysis` 或 `domain_analysis`
- `result.summary`: 汇总核心风险、置信度和关键指标
- `result.findings`: 输出异常通信、可疑域名、协议异常等发现
- `result.evidence`: 引用流量表、统计指标、域名命中、报文片段或报告摘录
- `result.artifacts`: 引用生成的 Markdown 报告、CSV、JSON、图表等文件
- `diagnostics.warnings`: 记录字段缺失、样本不足、弱证据等问题
- `errors`: 记录无法读取文件、格式不支持、依赖缺失等错误
```

### 3.4 retrieve 类 Skill 的额外要求

如果 Skill 是检索型能力，需要补充：

```markdown
Retrieval-specific output:
- 每条 `result.evidence[]` 必须尽量带 `query_id`
- 如能估算上下文成本，填写 `token_estimate`
- `diagnostics.retrieval.budget` 回填本次预算
- `diagnostics.retrieval.token_estimate` 回填实际返回证据的 token 估算
```

示例：

```json
{
  "evidence_id": "e-001",
  "type": "text",
  "title": "政策条款摘录",
  "data": "……",
  "query_id": "q1",
  "token_estimate": 320,
  "source_refs": [{"source": "local-policy-index"}]
}
```

## 4. Adapter 怎么做

Adapter 是兼容层，用于在不修改现有 Skill 实现的情况下接入统一输入。

Adapter 应优先放在系统层面实现，不建议每个 custom Skill 各自写一套转换逻辑。

系统层入口工具：

```text
invoke_skill
```

`invoke_skill` 有两个使用阶段：

```text
prepare     # LeadAgent 选中 Skill 后，生成并校验 SkillInputEnvelope，返回 legacy invocation package
wrap_output # 现有 Skill 执行后，把 legacy output 包装并校验为 SkillResult
```

在 Agent 链路中的位置：

```text
用户问题
  ↓
场景过滤或 SkillRouter 生成候选 Skill
  ↓
LeadAgent / Planner 选择具体 selected_skill
  ↓
构造 SkillInputEnvelope
  ↓
Input Adapter 校验并转换输入
  ↓
执行现有 Skill
```

注意：场景过滤得到的是 `allowed_skills`，不是 adapter 的直接输入。只有在 LeadAgent 或 Planner 选出 `selected_skill` 后，Input Adapter 才能根据目标 Skill 做字段映射。

推荐分层：

```text
系统层 Skill Adapter Runtime
├── 校验 SkillInputEnvelope
├── 根据 skill_name 定位 Skill 目录
├── 读取通用映射配置或 SKILL.md Input/Output handling
├── 转换为现有 Skill 的文件路径、CLI 参数、环境变量或 selected_data_sources
├── 调用现有 Skill 实现
├── 将 legacy output 包装为 SkillResult
└── 校验 SkillResult

custom Skill 层
├── SKILL.md 声明 Input handling
├── SKILL.md 声明 Output handling
└── 可选 adapter 配置，只描述字段映射，不重复实现 runtime
```

复杂 Skill 可以保留专属 adapter，但必须实现系统层统一接口：

```text
SkillInputEnvelope -> legacy invocation -> SkillResult
```

### 4.1 Adapter 输入

Adapter 接收：

```text
selected_skill + SkillInputEnvelope
```

LeadAgent 不应把场景过滤得到的全部 `allowed_skills` 直接传给 adapter。必须先选出具体 `selected_skill`，再调用：

```text
invoke_skill(mode="prepare", skill_name="<selected_skill>", input_envelope={...})
```

### 4.2 Adapter 输出给现有 Skill

`invoke_skill(mode="prepare")` 会输出 legacy invocation package，包括：

- 命令行参数；
- 环境变量；
- 临时配置文件；
- `selected_data_sources`；
- 现有脚本需要的文件路径列表；
- Skill 文件路径。

示例映射：

```text
inputs.files[0].path        → --input
inputs.parameters.view      → --view
budget.max_evidence_count   → --max-evidence
budget.max_token_estimate   → --max-tokens
legacy.env                  → 环境变量
legacy.argv                 → 追加 CLI 参数
```

LeadAgent 随后读取返回的 Skill 文件路径，按 `SKILL.md` 的现有流程执行。

### 4.3 Output Adapter

现有 Skill 执行完成后，LeadAgent 调用：

```text
invoke_skill(
  mode="wrap_output",
  skill_name="<selected_skill>",
  input_envelope=<prepare 阶段返回的 input_envelope>,
  legacy_output=<现有执行结果>,
  legacy_artifacts=[...]
)
```

该阶段会输出符合 `skills/skill_result.schema.json` 的 `SkillResult`。

### 4.4 不推荐做法

不要让业务 Skill 直接解析完整对话上下文来猜输入。

不推荐：

```text
Skill 自己从原始 messages 中猜文件、猜参数、猜预算
```

推荐：

```text
Planner / Adapter 先生成结构化 Envelope，Skill 只消费明确字段
```

## 5. 校验流程

推荐在构建或测试阶段做三类校验。

### 5.1 输入校验

调用 Skill 前校验：

```text
skills/skill_input_envelope.schema.json
```

校验失败时，不应继续调用 Skill，应返回可解释错误，例如缺少文件、参数非法、预算非法。

### 5.2 输出校验

Skill 执行后校验：

```text
skills/skill_result.schema.json
```

校验失败时，应进入包装或修复逻辑：

```text
legacy output
  ↓
output adapter
  ↓
SkillResult
  ↓
schema validation
```

### 5.3 Router Card 校验

生成或更新 router card 后校验：

```text
skills/router_card.schema.json
```

需要重点检查：

- `scope.scenes` 是否存在；
- 不应使用 `scope.scene`；
- `routing.routing_text` 是否包含场景、任务、输入、输出、正反触发；
- `source.skill_md_path` 是否指向真实文件；
- `source.skill_md_hash` 是否和当前 `SKILL.md` 一致。

## 6. 最小改造清单

每个现有 Skill 的第一阶段最小改造：

- [ ] `SKILL.md` front matter 有 `name` 和 `description`
- [ ] 增加 `Input handling` 章节
- [ ] 增加 `Output handling` 章节
- [ ] 明确本 Skill 的 `inputs.files` / `inputs.data_sources` / `inputs.parameters`
- [ ] 明确本 Skill 的 `summary` / `findings` / `evidence` / `artifacts`
- [ ] 如为 retrieve Skill，明确 `query_id`、`budget`、`token_estimate`
- [ ] 重新生成 `router_card.json`
- [ ] 校验 `router_card.json`
- [ ] 确认 `registry.json` 路径、hash、状态正确

## 7. 新建 Skill 要求

新建 Skill 应直接按统一规范编写：

```text
SKILL.md
├── front matter
├── Hard rules
├── Input handling
├── Mandatory execution workflow
├── Output handling
└── Non-negotiable boundaries
```

新建 Skill 不应只输出自然语言报告。即使生成 Markdown、CSV、图片或 GeoJSON，也应在 `SkillResult.result.artifacts[]` 中引用这些文件。

## 8. 常见问题

### Q1：现有 Skill 已经能跑，为什么还要写 Input handling？

为了让 Planner、SkillRouter、adapter、测试脚本都知道这个 Skill 需要什么输入，而不是依赖人工阅读整篇 `SKILL.md` 或让模型猜。

### Q2：是否必须马上让 Skill 脚本直接读取 SkillInputEnvelope？

不必须。第一阶段可以由 adapter 转换为现有参数。

### Q3：SkillResult 会不会限制业务输出？

不会。`SkillResult` 只定义统一 envelope。业务细节可以放在 `findings[].details`、`evidence[].data`、`artifacts[].metadata`、`diagnostics.data_quality` 中。

### Q4：Anthropic messages 还能用吗？

可以。原始消息可放在 `inputs.messages`，但它不是唯一 Skill 输入协议。Skill 调度应优先消费结构化字段。

### Q5：retrieve 为什么必须关注 budget？

因为多个 retrieve 并行时，如果每个都返回大量证据，会撑爆上下文。Planner 设置预算，retrieve 尊重预算，聚合层再做兜底裁剪。

## 9. 推荐落地顺序

1. 先落地统一 Schema：`skill_input_envelope.schema.json`、`skill_result.schema.json`、`router_card.schema.json`；
2. 再明确 LeadAgent 选择边界：场景过滤只产出 `allowed_skills`，LeadAgent 或 Planner 负责产出 `selected_skill`；
3. 再建设系统层 Skill Adapter Runtime，用于把 `selected_skill + SkillInputEnvelope` 转成现有 Skill 调用，并把 legacy output 包装为 `SkillResult`；
4. 再补各 custom Skill 的 `SKILL.md`：增加 `Input handling` 和 `Output handling`，必要时增加轻量 adapter 映射配置；
5. 再重新生成并校验 `router_card.json`；
6. 再修 `registry.json` 路径、hash、状态和 ES 索引状态；
7. 再为少数复杂 Skill 增加专属 adapter plugin，但必须接入系统层统一接口；
8. 最后逐步把新建 Skill 改为原生消费 `SkillInputEnvelope`、原生输出 `SkillResult`。

关键顺序是：

```text
Schema 先定
  ↓
LeadAgent selected_skill 边界先明确
  ↓
系统层 Adapter Runtime 再接 selected_skill
  ↓
custom Skill 再逐个补声明和映射
```

不要先让每个 custom Skill 自己实现 adapter，否则统一输入输出规范会再次分叉。
