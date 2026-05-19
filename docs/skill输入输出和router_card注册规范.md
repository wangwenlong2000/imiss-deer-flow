# Skill 接口定义、注册机制及输出规范

> 本文档面向所有参与 Skill 开发、Router Card 构建、SkillRouter 中间件开发、ES 索引构建的工程师，定义统一的 Skill 接口契约。
> 所有新 Skill 必须遵循本规范，已有 Skill 应逐步迁移。

## 1. 文件关系总览

每个 Skill 由以下文件组成，各司其职：

```
skills/{public,custom}/<skill-id>/
├── SKILL.md                          # Agent 执行阶段的完整指导文档
├── router_card.json                  # SkillRouter 路由画像（构建产物）
└── references/                       # 场景本地参考文档（按需读取）
    ├── capability-catalog.md         # 能力清单
    ├── input-output-contract.md      # 输入输出映射说明
    └── playbooks.md                  # 多步工作流
```

三类文件的关系：

| 文件 | 面向对象 | 用途 | 生命周期 |
|------|---------|------|---------|
| `SKILL.md` | Agent 模型 | 执行阶段的行为指导，包含触发条件、执行流程、硬性规则 | 手工维护 |
| `router_card.json` | SkillRouter 中间件 | 路由决策的边界描述，包含适用场景、任务类型、输入输出类型 | 构建产物，自动生成 |
| `references/*.md` | Agent 模型 | Agent 在特定场景下按需展开的参考文档 | 手工维护 |

**核心原则：SKILL.md 是唯一的真相来源。router_card.json 从 SKILL.md 自动提取，不手工编辑。**

---

## 2. SKILL.md 接口定义

### 2.1 目录位置

| 类型 | 路径 | 说明 |
|------|------|------|
| 公共 Skill | `skills/public/<skill-id>/SKILL.md` | 跨场景复用，如 data-analysis、chart-visualization |
| 场景 Skill | `skills/custom/<skill-id>/SKILL.md` | 绑定具体业务场景，如 network-traffic-analysis、law-regulations-rag |

### 2.2 强制 Front Matter

每个 SKILL.md 必须包含 YAML front matter：

```yaml
---
name: <skill-id>
description: <简短的触发导向描述，面向路由友好格式>
---
```

字段要求：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | Skill 唯一标识，必须与目录名一致。建议使用 kebab-case，如 `network-traffic-analysis` |
| `description` | string | 是 | 一句话功能描述，面向路由而非执行细节。描述"做什么"而非"怎么做" |

**description 编写建议：**

- 好的：`用于解析 pcap/pcapng 网络流量文件，识别异常通信与潜在安全事件`
- 不好的：`使用 tshark 和 scapy 预处理 pcap 文件，调用 analyze.py 进行统计分析...`（执行细节属于 SKILL.md 正文）

### 2.3 推荐 Front Matter 扩展

场景 Skill 建议在 front matter 中额外声明元数据，供 Router Card 自动生成时提取：

```yaml
---
name: network-traffic-analysis
description: 用于解析 pcap/pcapng 网络流量文件，识别异常通信与潜在安全事件
metadata:
  scenes: [network_traffic]
  is_public: false
  task_types: [pcap_parse, protocol_analysis, anomaly_detect, domain_analysis, security_report]
  input_types: [pcap, pcapng, cap, csv]
  output_types: [flow_csv, domain_list, anomaly_findings, security_report]
  required_tools: [read_file, bash]
  optional_tools: [write_file, chart_generator]
---
```

### 2.4 SKILL.md 正文推荐结构

```markdown
# <Skill Name> Skill

## Hard rules
## Input resolution order
## File type handling
## Allowed tools
## Mandatory execution workflow
## Output handling
## Non-negotiable boundaries
```

SKILL.md 正文**不应**在开头罗列所有能力的完整清单。能力清单应放在 `references/capability-catalog.md` 中，按需读取。

---

## 3. Router Card 接口定义

### 3.1 定位

`router_card.json` 是 SkillRouter 中间件的路由画像，描述：

- 这个 Skill 适合什么任务、不适合什么任务
- 属于哪些场景、可处理什么输入、输出什么结果
- 依赖哪些工具、与哪些 Skill 可组合
- 与哪些 Skill 存在边界冲突

**Router Card 是构建产物，不在运行时生成，也不应手工编辑。**

### 3.2 存储位置

```
skills/{public,custom}/<skill-id>/router_card.json
```

### 3.3 完整字段规范

Router Card 遵循 `skills/router_card.schema.json` 的 JSON Schema 校验。

```
router_card.json
├── schema_version          # string, 必填, 如 "1.0.0"
├── identity                # 身份标识
│   ├── id                  # string, 必填, Skill 唯一标识
│   ├── name                # string, 必填, 人类可读名称
│   └── description         # string, 必填, 路由友好一句话描述
├── scope                   # 作用域
│   ├── scenes              # list[string], 必填, 所属场景
│   ├── is_public           # bool, 必填, 是否公共 Skill
│   ├── task_types          # list[string], 可处理的任务类型
│   ├── input_types         # list[string], 可处理的输入类型
│   └── output_types        # list[string], 输出的结果类型
├── routing                 # 路由信息
│   ├── routing_text        # string, 必填, 标准化路由文本（用于 Embedding）
│   ├── positive_triggers   # list[string], 典型正例触发描述
│   ├── negative_triggers   # list[string], 典型反例触发描述
│   ├── keywords            # list[string], 兜底关键词
│   └── anti_keywords       # list[string], 明确不应触发的关键词
├── body                    # 用于 Reranker 精排
│   ├── source              # string, 来源文件（通常 "SKILL.md"）
│   └── content             # string, 清洗后的 SKILL.md 正文
├── execution               # 执行约束
│   ├── required_tools      # list[string], 必需工具
│   ├── optional_tools      # list[string], 可选工具
│   ├── allowed_file_patterns # list[string], 可处理的文件格式
│   ├── can_run_standalone  # bool, 是否可独立执行
│   └── can_compose_with    # list[string], 可组合的 Skill ID 列表
├── routing_policy          # 路由策略
│   ├── priority            # int, 必填, 冲突时优先级（数值越高越优先）
│   ├── conflict_group      # string, 相似 Skill 分组标识
│   ├── prefer_when         # list[string], 何时优先选择该 Skill
│   └── defer_when          # list[string], 何时让位给其他 Skill
├── source                  # 溯源信息
│   ├── skill_dir           # string, Skill 目录路径
│   ├── skill_md_path       # string, SKILL.md 路径
│   ├── skill_md_hash       # string, SKILL.md 内容的 SHA256 哈希
│   ├── generated_at        # string, ISO 8601 时间戳
│   └── generator_version   # string, 生成器版本
├── embedding               # Embedding 信息
│   ├── model               # string, 使用的 Embedding 模型
│   ├── text_hash           # string, routing_text 的 SHA256 哈希
│   ├── es_index            # string, ES 索引名
│   └── es_doc_id           # string, ES 文档 ID
└── evaluation              # 评估信息（可选）
    ├── golden_queries      # list[object], 路由测试样例
    └── last_eval           # object, 最近一次评估结果
```

### 3.4 routing_text 生成模板

`routing_text` 是 Embedding 粗排召回的输入，必须包含足够的区分信息：

```
名称：{name}
描述：{description}
适用场景：{scenes}。
适用任务：{task_types}。
输入类型：{input_types}。
输出类型：{output_types}。
适合使用：{positive_triggers 语义化的中文描述}。
不适合使用：{negative_triggers 语义化的中文描述}。
```

示例：

```
名称：网络流量分析
描述：用于解析 pcap/pcapng 网络流量文件，识别异常通信、可疑域名和安全事件。
适用场景：network_traffic。
适用任务：pcap_parse, protocol_analysis, anomaly_detect, domain_analysis, security_report。
输入类型：pcap, pcapng, cap, csv。
输出类型：flow_csv, domain_list, anomaly_findings, security_report。
适合使用：分析 pcap 文件中的异常通信；识别网络流量中的可疑域名；统计 DNS、HTTP、TLS 和 TCP 会话。
不适合使用：分析整治台账合规风险；检索政策法规条款；分析车辆时空轨迹；预测交通路网流量。
```

### 3.5 task_types 和 input/output_types 规范

所有 Skill 共享以下类型词表，新增类型时需在本文档中登记：

**task_types（任务类型）：**

| 类型 | 含义 | 示例 Skill |
|------|------|-----------|
| `pcap_parse` | PCAP 文件解析 | network-traffic-analysis |
| `protocol_analysis` | 协议分析 | network-traffic-analysis |
| `anomaly_detect` | 异常检测 | network-traffic-analysis |
| `domain_analysis` | 域名分析 | network-traffic-analysis |
| `security_report` | 安全报告生成 | network-traffic-analysis |
| `law_retrieval` | 法规检索 | law-regulations-rag |
| `policy_retrieval` | 政策检索 | law-regulations-rag |
| `legal_basis_mapping` | 法规依据映射 | law-regulations-rag |
| `compliance_reference` | 合规判断依据 | law-regulations-rag |
| `data_query` | 数据查询 | data-analysis |
| `statistical_summary` | 统计摘要 | data-analysis |
| `chart_generation` | 图表生成 | chart-visualization |

**input_types（输入类型）：**

| 类型 | 含义 |
|------|------|
| `pcap` / `pcapng` / `cap` | 网络抓包文件 |
| `csv` | CSV 表格数据 |
| `xlsx` / `xls` | Excel 工作簿 |
| `parquet` | Parquet 列存储 |
| `json` / `jsonl` | JSON 数据 |
| `pdf` | PDF 文档 |
| `docx` | Word 文档 |
| `text` | 纯文本 |
| `image` | 图像文件 |
| `image_directory` | 图像目录 |
| `timeseries_csv` | 时序 CSV 数据 |

**output_types（输出类型）：**

| 类型 | 含义 |
|------|------|
| `flow_csv` | 流量 CSV 表格 |
| `domain_list` | 域名清单 |
| `anomaly_findings` | 异常发现 |
| `security_report` | 安全报告 |
| `law_articles` | 法规条款 |
| `policy_references` | 政策参考 |
| `legal_basis_mapping` | 法规依据映射 |
| `report` | 综合报告 |
| `chart` | 图表 |
| `risk_findings` | 风险发现 |
| `skill_result_json` | 标准化 SkillResult JSON |

### 3.6 routing_policy 约定

| 字段 | 说明 |
|------|------|
| `priority` | 整数，数值越高越优先。同类场景 Skill 建议设为 90，公共 Skill 设为 50 |
| `conflict_group` | 字符串，相同冲突组的 Skill 互斥。例如 `network_traffic_analysis` 组内的 Skill 只能选一个 |
| `prefer_when` | 字符串数组，描述何时优先选择该 Skill |
| `defer_when` | 字符串数组，描述何时应让位给其他 Skill |

**公共 Skill 的 defer_when 约定：** 公共 Skill 必须在 `defer_when` 中声明让位条件，确保场景 Skill 优先。例如：

```json
"defer_when": [
  "任务需要专业网络流量分析，应优先使用 network-traffic-analysis",
  "任务需要政策法规检索，应优先使用 law-regulations-rag"
]
```

---

## 4. 注册机制

### 4.1 注册层级

Skill 注册分为三个层级，数据逐层流动：

```
SKILL.md (手工维护)
    │
    ▼ extract_router_cards.py
router_card.json (构建产物，每个 Skill 目录下)
    │
    ▼ build_skill_router_registry.py
registry.json (全局注册索引)
    │
    ▼ build_skill_router_es_index.py
Elasticsearch SKILL_ROUTER_ES_INDEX (在线向量索引)
```

### 4.2 registry.json

全局注册索引，记录所有 Skill 的基础元信息：

```json
{
  "version": 1,
  "schema_version": "1.0.0",
  "router_index": {
    "type": "elasticsearch",
    "url_env": "ES_URL",
    "username_env": "ES_USERNAME",
    "password_env": "ES_PASSWORD",
    "index_env": "SKILL_ROUTER_ES_INDEX",
    "default_url": "http://172.17.0.1:3128",
    "default_index": "citybrain-skill-router-cards",
    "embedding_model": "SkillRouter-Embedding-0.6B",
    "vector_field": "embedding_vector",
    "text_field": "routing_text",
    "id_field": "skill_id"
  },
  "skills": [
    {
      "id": "network-traffic-analysis",
      "name": "网络流量分析",
      "scenes": ["network_traffic"],
      "is_public": false,
      "task_types": ["pcap_parse", "anomaly_detect"],
      "input_types": ["pcap", "pcapng", "cap", "csv"],
      "router_card_path": "custom/network-traffic-analysis/router_card.json",
      "skill_md_path": "custom/network-traffic-analysis/SKILL.md",
      "enabled": true,
      "routing_text_hash": "sha256:yyyy",
      "es_index": "citybrain-skill-router-cards",
      "es_doc_id": "network-traffic-analysis",
      "router_status": "ready",
      "es_indexed": true,
      "last_router_error": null
    }
  ]
}
```

### 4.3 Router 状态字段

registry.json 中每个 Skill 的路由状态：

| 状态 | 说明 | 是否参与路由 |
|------|------|-------------|
| `pending_card` | 已创建 SKILL.md，尚未生成 router_card.json | 否 |
| `pending_index` | 已生成 router_card.json，尚未写入 ES | 否 |
| `pending_review` | 已生成索引，冲突检测或人工检查未通过 | 否 |
| `ready` | Router Card、ES 索引、registry 均已完成 | 是 |
| `disabled` | Skill 被禁用 | 否 |
| `deleted` | Skill 被软删除 | 否 |
| `error` | 自动构建失败 | 否 |

只有同时满足以下条件的 Skill 才能参与 SkillRouter 路由：

```json
{
  "enabled": true,
  "router_status": "ready",
  "es_indexed": true
}
```

### 4.4 Elasticsearch 文档结构

每个 Skill 对应 SkillRouter 专用索引中的一条文档：

```json
{
  "skill_id": "network-traffic-analysis",
  "name": "网络流量分析",
  "description": "用于解析 pcap/pcapng 网络流量文件...",
  "scenes": ["network_traffic"],
  "is_public": false,
  "task_types": ["pcap_parse", "protocol_analysis", "anomaly_detect"],
  "input_types": ["pcap", "pcapng", "cap", "csv"],
  "output_types": ["flow_csv", "domain_list", "anomaly_findings", "security_report"],
  "routing_text": "名称：网络流量分析\n描述：...",
  "body": "# 网络流量分析\n\n## 适用场景\n...",
  "skill_dir": "custom/network-traffic-analysis",
  "skill_md_path": "custom/network-traffic-analysis/SKILL.md",
  "router_card_path": "custom/network-traffic-analysis/router_card.json",
  "skill_md_hash": "sha256:xxxx",
  "routing_text_hash": "sha256:yyyy",
  "embedding_model": "SkillRouter-Embedding-0.6B",
  "embedding_vector": [0.0123, -0.0345],
  "enabled": true,
  "updated_at": "2026-05-12T00:00:00Z"
}
```

### 4.5 ES Mapping

SkillRouter 专用索引的 ES mapping：

```json
{
  "mappings": {
    "properties": {
      "skill_id": {"type": "keyword"},
      "name": {"type": "text"},
      "description": {"type": "text"},
      "scenes": {"type": "keyword"},
      "is_public": {"type": "boolean"},
      "task_types": {"type": "keyword"},
      "input_types": {"type": "keyword"},
      "output_types": {"type": "keyword"},
      "routing_text": {"type": "text"},
      "body": {"type": "text"},
      "skill_dir": {"type": "keyword"},
      "skill_md_path": {"type": "keyword"},
      "router_card_path": {"type": "keyword"},
      "skill_md_hash": {"type": "keyword"},
      "routing_text_hash": {"type": "keyword"},
      "embedding_model": {"type": "keyword"},
      "embedding_vector": {
        "type": "dense_vector",
        "dims": <由 Embedding API 实际返回维度决定>,
        "index": true,
        "similarity": "cosine"
      },
      "enabled": {"type": "boolean"},
      "updated_at": {"type": "date"}
    }
  }
}
```

**dims 必须由构建脚本第一次调用 Embedding API 后动态读取向量长度，并据此创建 mapping。禁止硬编码维度。**

---

## 5. SkillResult 输出规范

所有自定义场景 Skill 在执行分析后，应将最终输出标准化为 `SkillResult` 形状。这确保前端、后端、报告生成和后续 Agent 能统一消费 Skill 输出。

### 5.1 顶层结构

```json
{
  "schema_version": "1.0",
  "request_id": "uuid",
  "skill_name": "network-traffic-analysis",
  "scenario": "network_traffic",
  "capability": "encrypted-flow-analysis",
  "status": "success",
  "result": {
    "summary": {},
    "findings": [],
    "evidence": [],
    "artifacts": []
  },
  "diagnostics": {},
  "errors": []
}
```

### 5.2 status 允许值

| 值 | 含义 |
|---|------|
| `success` | 任务完全成功 |
| `partial_success` | 主任务完成但数据不完整或部分失败 |
| `failed` | 任务失败 |

### 5.3 result 各节

#### summary — 用户-facing 结果摘要

```json
{
  "title": "Encrypted Flow Analysis",
  "overview": "Detected 9 JA3 fingerprints and 1 known malicious match.",
  "severity": "high",
  "confidence": 0.8,
  "key_metrics": [
    {"name": "flows_analyzed", "value": 12},
    {"name": "ja3_matches", "value": 1}
  ]
}
```

`severity` 允许值：`info`, `low`, `medium`, `high`, `critical`

#### findings — 核心发现

```json
{
  "finding_id": "f-001",
  "type": "ja3_match",
  "severity": "high",
  "confidence": 0.8,
  "title": "JA3 matched known malicious fingerprint",
  "description": "The JA3 hash matched SSLBL threat intelligence.",
  "entities": [
    {"type": "src_ip", "value": "10.10.10.102"},
    {"type": "ja3", "value": "4d7a28d6f2263ed61de88ca66eb011e3"}
  ],
  "evidence_refs": ["e-001"],
  "recommended_actions": ["Review destination reputation"]
}
```

#### evidence — 支撑发现的结构化数据

允许的证据类型：

| 类型 | 用途 |
|------|------|
| `table` | 表格数据 |
| `metric` | 指标数值 |
| `timeseries` | 时序数据 |
| `image_annotation` | 图像标注 |
| `geo_feature` | 地理要素 |
| `graph` | 实体关系图 |
| `text` | 纯文本 |
| `file` | 文件引用 |

每种类型的字段详见 `CUSTOM_SKILL_STANDARD.md`。

#### artifacts — 生成的文件

```json
{
  "artifact_id": "a-001",
  "type": "file",
  "title": "Analysis Report",
  "uri": "/mnt/user-data/outputs/report.md",
  "media_type": "text/markdown"
}
```

#### diagnostics — 执行详情

```json
{
  "warnings": [{"code": "WEAK_INFERENCE", "message": "...", "severity": "warning"}],
  "data_quality": {"rows_read": 55, "rows_analyzed": 12},
  "provenance": [{"source": "abuse.ch SSLBL", "url": "...", "retrieved_at": "..."}],
  "runtime": {"started_at": "...", "duration_ms": 1830}
}
```

#### errors — 错误信息

```json
{
  "code": "MISSING_REQUIRED_FIELD",
  "message": "encrypted-flow-analysis requires dst_port.",
  "severity": "error",
  "recoverable": true,
  "details": {"missing_fields": ["dst_port"]}
}
```

### 5.4 最小有效 SkillResult

```json
{
  "schema_version": "1.0",
  "request_id": "uuid",
  "skill_name": "example-skill",
  "scenario": "example",
  "capability": "summary",
  "status": "success",
  "result": {
    "summary": {"title": "Summary", "overview": "Completed."},
    "findings": [],
    "evidence": [],
    "artifacts": []
  },
  "diagnostics": {"warnings": [], "data_quality": {}, "provenance": []},
  "errors": []
}
```

---

## 6. SkillRouter 中间件如何使用这些接口

### 6.1 路由流程

```
用户 query + uploaded_files
    │
    ▼ L0: should_route 轻量跳过判断
    │
    ▼ L1: 任务粗拆分 → task_segments[]
    │
    ▼ L2: Embedding API 为每个 segment 生成 query vector
    │
    ▼ L3: Elasticsearch Top-K 召回候选 Router Cards
    │     (过滤: enabled=true; v1 后过滤: skill_id ∈ base_scope)
    │
    ▼ L4: Reranker API 精排
    │
    ▼ L5: 公共 Skill 约束（每 segment 最多 2 个）
    │
    ▼ L6: 生成 routing_context（写入 ThreadState）
    │
    ▼ L7: 生成 skills_override SystemMessage（注入 Agent）
```

### 6.2 SkillRouter 读取的 ES 文档字段

SkillRouter 中间件从 ES 召回的每条候选文档中读取以下字段：

| 字段 | 用途 |
|------|------|
| `skill_id` | 唯一标识，用于去重和 scope 过滤 |
| `name` | 展示和日志 |
| `description` | Reranker 输入 |
| `scenes` | 场景匹配 |
| `is_public` | 公共 Skill 约束 |
| `task_types` | 任务类型匹配 |
| `input_types` | 文件类型匹配 |
| `output_types` | 输出类型推断 |
| `routing_text` | Embedding 向量来源（不直接使用） |
| `body` | Reranker 精排输入 |
| `enabled` | 过滤禁用的 Skill |

### 6.3 routing_context 输出结构

最终写入 `state["routing_context"]` 的结构：

```json
{
  "route_mode": "multi_segment",
  "trigger": true,
  "primary_goal": "分析网络流量和政策法规依据",
  "scene_tasks": [
    {
      "scene_task_id": "task_001",
      "segment_id": "seg_001",
      "segment_text": "分析 pcap 文件中的异常通信",
      "scene": "network_traffic",
      "input_refs": ["traffic.pcap"],
      "task_types": ["pcap_parse", "anomaly_detect"],
      "selected_skills": [
        {"id": "network-traffic-analysis", "role": "primary", "score": 0.91}
      ],
      "expected_outputs": ["anomaly_findings"],
      "depends_on": []
    }
  ],
  "global_selected_skills": ["network-traffic-analysis", "law-regulations-rag"],
  "global_allowed_tools": ["read_file", "bash", "tshark", "write_file"],
  "confidence": 0.89,
  "route_reason": "Matched 2 task segment(s) from user query"
}
```

对应的 Pydantic 模型定义在 `backend/packages/harness/deerflow/routing/schema.py`。

### 6.4 Scope 过滤机制

SkillRouter 中间件执行两级 scope 过滤：

1. **base_scope**: `registry_enabled ∩ frontend_enabled` — 确保前端关闭的 Skill 不参与路由
2. **final_scope**: `base_scope ∩ routed_skills` — 确保路由结果不超出 base_scope

过滤在两个阶段执行：
- ES 召回后 → v1 后过滤 candidates
- Reranker 精排后 → final_scope 过滤 selected_skills

---

## 7. 新建 Skill 的完整流程

### 7.1 手工步骤

1. 在 `skills/custom/<skill-id>/` 或 `skills/public/<skill-id>/` 下创建目录
2. 编写 `SKILL.md`，包含 front matter 和正文
3. 按需编写 `references/capability-catalog.md`、`references/input-output-contract.md`、`references/playbooks.md`

### 7.2 自动构建步骤

```bash
# 全量构建
make extract-router-cards        # 从所有 SKILL.md 生成 router_card.json
make build-skill-router-index    # 生成 registry.json + 写入 ES 向量索引

# 增量构建（单个 Skill）
python scripts/update_skill_router_index.py --skill custom/<skill-id>
```

### 7.3 自动构建产物

| 产物 | 位置 | 说明 |
|------|------|------|
| `router_card.json` | `skills/{public,custom}/<skill-id>/router_card.json` | 该 Skill 的路由画像 |
| `registry.json` 条目 | `skills/registry.json` | 全局注册表中增加该 Skill |
| ES 文档 | `SKILL_ROUTER_ES_INDEX` 索引 | 在线向量索引中写入该 Skill |

### 7.4 验收检查清单

新建 Skill 后逐项检查：

- [ ] `SKILL.md` front matter 包含 `name` 和 `description`
- [ ] `SKILL.md` description 是触发导向的一句话描述
- [ ] `router_card.json` 已通过 `router_card.schema.json` 校验
- [ ] `router_card.json` 中 `routing_text` 包含完整的场景/任务/输入输出信息
- [ ] `router_card.json` 中 `negative_triggers` 明确排除相似 Skill 的场景
- [ ] `router_card.json` 中 `routing_policy.priority` 已设置
- [ ] `router_card.json` 中 `routing_policy.conflict_group` 已设置
- [ ] 公共 Skill 的 `defer_when` 声明了让位条件
- [ ] ES 索引中该 Skill 的文档 `enabled=true`
- [ ] `registry.json` 中该 Skill 的 `router_status=ready`
- [ ] 代表性 query 能被 SkillRouter 正确召回该 Skill

---

## 8. 公共 Skill 与场景 Skill 的差异

| 维度 | 公共 Skill | 场景 Skill |
|------|-----------|-----------|
| 路径 | `skills/public/<skill-id>/` | `skills/custom/<skill-id>/` |
| `scope.is_public` | `true` | `false` |
| `scope.scenes` | 必须包含 `"public"` | 填写具体场景名 |
| `routing_policy.priority` | 建议 50 | 建议 90 |
| `routing_policy.defer_when` | **必须**声明让位条件 | 可选 |
| 每 segment 上限 | 最多 2 个 | 无硬限制 |
| 注入方式 | 按需注入，服务具体 segment | 按需注入，绑定场景 |

---

## 9. 不要做的事

- **不要**在 `SKILL.md` front matter 之外手工编辑 `router_card.json`
- **不要**创建全局能力清单（能力清单分散在各 Skill 的 `references/capability-catalog.md`）
- **不要**将所有场景能力目录注入 Agent 上下文
- **不要**让 SkillRouter 读写 `NETWORK_TRAFFIC_ES_INDEX`（RAG 索引）
- **不要**返回纯自然语言作为机器可消费的最终输出（应输出 `SkillResult` JSON）
- **不要**隐藏部分失败或弱证据质量（应写入 `diagnostics.warnings`）
