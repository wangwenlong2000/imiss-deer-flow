# 产品需求文档（PRD）——城市超脑 SkillRouter 中间件

## 1. 项目背景

城市超脑智能体系统需要面向多类城市治理场景提供智能分析能力，当前已包含并计划持续扩展以下场景：

- **网络流量分析场景**：PCAP 文件解析、协议统计、异常通信识别、可疑域名检测、安全事件研判。
- **政策法规分析场景**：政策法规检索、政策条款召回、整治台账合规分析、执法程序风险判断。
- **公共能力场景**：数据清洗、表格解析、图表生成、文件处理、文本摘要等。
- **后续扩展场景**：时空轨迹分析、交通流量分析等。

随着场景和 Skill 数量增加，系统需要从“全量 Skill 注入”升级为“按任务精准路由 Skill”。因此，需要在 DeerFlow Agent Runtime 中新增 **SkillRouterMiddleware**，在主 Agent 执行前完成任务拆分、场景识别、Skill 粗排召回、Skill 精排选择和 Skill 注入裁剪。

本方案统一采用 **Router Card** 作为 Skill 的路由画像文件。每个 Skill 对应一个 `router_card.json`，用于描述该 Skill 的适用边界、触发条件、输入输出、执行约束和与其他 Skill 的组合关系。

本方案的向量索引统一存储在在线 Elasticsearch 服务中，向量由 `SkillRouter-Embedding-0.6B` 在线服务生成，精排由 `SkillRouter-Reranker-0.6B` 在线服务完成。

---

## 2. 问题定义

### 2.1 全量 Skill 注入导致上下文污染

当前 enabled skills 如果全部注入 system prompt，会带来以下问题：

- 用户请求实际只需要少量 Skill，但模型会看到全部 Skill。
- 无关 Skill 增加上下文 token 消耗。
- 相似 Skill 容易互相干扰，导致误触发。
- 后续新增 Skill 后，路由稳定性会下降。

目标是改成：

```text
用户请求 → SkillRouter 选择相关 Skill → 只注入命中 Skill
```

---

### 2.2 用户 query 可能包含多个场景和多个任务

用户一次请求可能同时涉及多个场景，例如：

```text
上传 pcap 和整治台账，分别分析网络异常和政策合规风险，最后生成综合研判结论。
```

该请求至少包含：

- 网络流量分析任务
- 政策法规分析任务
- 综合结论汇总任务

因此，SkillRouter 不能只输出一个 Skill，而应输出多个结构化任务包，每个任务包绑定对应场景和 Skill 组合。

---

### 2.3 公共 Skill 不能全量注入

系统中存在一类不绑定具体业务场景的公共 Skill，例如数据分析、表格解析、图表生成、文件处理、文本摘要等。这类 Skill 可以被多个场景复用，但不能因为“通用”就默认全部注入。

公共 Skill 的选择应遵循：

```text
公共 Skill 不绑定单一场景，但必须绑定具体任务。
```

也就是说，公共 Skill 只有在当前任务包确实需要它时，才进入候选集合并参与 Reranker 精排。

例如：

```text
分析 pcap 文件中的异常通信，并输出分析结论。
```

当前阶段应优先选择：

- network-traffic-analysis

如果当前系统尚未提供独立的报告生成 Skill，则“输出分析结论/报告”应由 `network-traffic-analysis` 的自身输出能力或主 Agent 汇总能力完成，而不是假设存在 `report-generation` Skill。

对于政策法规类请求，例如：

```text
查一下相关法律条文，并判断这个台账是否存在合规风险。
```

当前阶段应优先选择：

- law-regulations-rag

如果系统中同时存在专门的政策风险研判 Skill，例如 `policy-risk-analysis`，则可以组合为：

- law-regulations-rag
- policy-risk-analysis

其中，`law-regulations-rag` 负责法规、政策条款检索与依据召回，`policy-risk-analysis` 负责结合台账、通知和法规依据进行合规风险判断。

后续计划新增的 custom 场景 Skill，例如：

- trajectory-analysis
- traffic-flow-analysis

应在对应 Skill 实际加入系统并生成 Router Card 后，再参与路由候选。当前 PRD 不将这些后续 Skill 作为当前可用 Skill，也不作为当前阶段的固定反例。

---

### 2.4 Skill 描述相似，需要更强的路由边界

多个 Skill 可能名称相似、description 相似，仅依靠名称和描述无法准确区分。真正决定 Skill 是否适用的是：

- 输入类型
- 输出类型
- 适用场景
- 执行流程
- 使用工具
- 不适用边界
- 典型正例和反例

因此需要为每个 Skill 生成独立的 **Router Card**，作为 SkillRouter 的路由画像。

---

## 3. 产品目标

### 3.1 核心目标

建设一套适配 DeerFlow 的城市超脑 SkillRouter 路由机制，实现：

1. 用户 query 粗粒度任务拆分。
2. 多场景识别。
3. 场景专用 Skill 与公共 Skill 的精准筛选。
4. 只注入当前任务需要的 Skill。
5. 将路由结果写入 `routing_context`。
6. 与 TodoMiddleware 协作生成更稳定的执行计划。
7. 支持后续新增场景和新增 Skill，无需修改中间件核心逻辑。

---

### 3.2 交付目标

完成以下能力：

- Router Card 规范与自动生成。
- Router Card 向量索引写入 SkillRouter 专用 Elasticsearch index。
- Skill registry 构建。
- SkillRouterMiddleware 运行时路由。
- SkillRouter-Embedding-0.6B API 粗排召回。
- SkillRouter-Reranker-0.6B API 精排选择。
- `routing_context` 写入 ThreadState。
- `skills_override` 注入主 Agent。
- TodoMiddleware 对接增强。
- 单元测试、路由测试和端到端验收。

---

## 4. 总体方案

### 4.1 架构设计

```text
Router Card
  ↓
SkillRouter-Embedding-0.6B API 生成 routing_text 向量
  ↓
Elasticsearch 专用索引存储 Router Card 向量
  ↓
SkillRouterMiddleware
  ↓
Embedding API 生成 query/task segment 向量
  ↓
Elasticsearch Top-K 粗排召回
  ↓
SkillRouter-Reranker-0.6B API 精排选择
  ↓
routing_context + skills_override
  ↓
TodoMiddleware / Lead Agent
```

---

### 4.2 在线服务与环境变量

系统使用三个在线服务：

1. SkillRouter-Embedding-0.6B 服务
2. SkillRouter-Reranker-0.6B 服务
3. Elasticsearch 服务

环境变量如下：

```env
# SkillRouter Embedding 服务
SKILLROUTER_EMBEDDING_BASE_KEY=unused
SKILLROUTER_EMBEDDING_BASE_URL=http://192.168.200.1:7800/v1

# SkillRouter Reranker 服务
SKILLROUTER_RERANKER_BASE_KEY=unused
SKILLROUTER_RERANKER_BASE_URL=http://192.168.200.1:7801/v1

# Elasticsearch 公共连接配置
ES_URL=http://172.17.0.1:3128
ES_USERNAME=citybrain-street
ES_PASSWORD=123456

# 现有 RAG 索引，继续保留给 RAG 模块使用
ES_INDEX=network-traffic-rag-smoke-clean

# 新增：SkillRouter 专用索引，用于存储 Router Card 向量
SKILL_ROUTER_ES_INDEX=citybrain-skill-router-cards
```

其中：

- `ES_INDEX` 是现有 RAG 索引，继续服务网络流量 RAG / 文档片段检索。
- `SKILL_ROUTER_ES_INDEX` 是新增的 SkillRouter 专用索引，只存储 `router_card.routing_text` 的向量和 Router Card 元数据。
- 两个索引可以部署在同一个 Elasticsearch 服务中，但必须使用不同 index，避免 RAG 文档与 Skill 路由文档混在一起。

服务职责：

| 服务 | 作用 | 输入 | 输出 |
|---|---|---|---|
| SkillRouter-Embedding-0.6B | 向量生成 | `routing_text` / task segment | embedding vector |
| Elasticsearch | 向量索引存储与 Top-K 检索 | query embedding | candidate Router Cards |
| SkillRouter-Reranker-0.6B | 精排判断 | query + candidate skill body | 相关性分数 / 排序结果 |

DeerFlow 不在进程内加载模型权重，只通过 API 调用模型服务。

---

## 5. Router Card 设计

### 5.1 Router Card 定位

Router Card 是每个 Skill 的路由画像，负责描述：

- 这个 Skill 适合什么任务。
- 不适合什么任务。
- 属于哪些场景。
- 可处理什么输入。
- 输出什么结果。
- 依赖哪些工具。
- 与哪些 Skill 可以组合。
- 与哪些 Skill 存在冲突边界。

三类文件关系如下：

```text
SKILL.md
  面向 Agent 执行阶段，描述完整执行方法。

router_card.json
  面向 SkillRouter，描述路由边界和选择条件。

registry.json
  面向系统加载，记录所有 Router Card 的路径和基础元信息。
```

---

### 5.2 存储结构

每个 Skill 目录下单独维护一个 `router_card.json`。

```text
skills/
├── public/
│   ├── data-analysis/
│   │   ├── SKILL.md
│   │   └── router_card.json
│   ├── chart-visualization/
│   │   ├── SKILL.md
│   │   └── router_card.json
│   └── ...
│
├── custom/
│   ├── network-traffic-analysis/
│   │   ├── SKILL.md
│   │   └── router_card.json
│   ├── law-regulations-rag/
│   │   ├── SKILL.md
│   │   └── router_card.json
│   └── ...
│
├── router_card.schema.json
└── registry.json
```

向量索引写入 Elasticsearch 的 SkillRouter 专用索引：

```text
Elasticsearch
└── index: ${SKILL_ROUTER_ES_INDEX}
```

当前推荐索引名：

```env
SKILL_ROUTER_ES_INDEX=citybrain-skill-router-cards
```

---

### 5.3 Router Card 字段规范

| 模块 | 字段 | 类型 | 说明 |
|---|---|---|---|
| identity | id | string | Skill 唯一标识 |
| identity | name | string | Skill 名称 |
| identity | description | string | 路由友好的一句话功能描述 |
| scope | scenes | list[string] | 所属场景，公共 Skill 填 `public` |
| scope | is_public | bool | 是否公共 Skill |
| scope | task_types | list[string] | 可处理的任务类型 |
| scope | input_types | list[string] | 输入类型，如 pcap、xlsx、docx、text |
| scope | output_types | list[string] | 输出类型，如 report、chart、risk_findings |
| routing | routing_text | string | 用于 Embedding 粗排的标准化文本 |
| routing | positive_triggers | list[string] | 典型正例触发描述 |
| routing | negative_triggers | list[string] | 典型反例，用于区分相似 Skill |
| routing | keywords | list[string] | 兜底关键词 |
| routing | anti_keywords | list[string] | 明确不应触发的关键词 |
| body | content | string | 清洗后的 SKILL.md 正文，用于 Reranker 精排 |
| execution | required_tools | list[string] | 必需工具 |
| execution | optional_tools | list[string] | 可选工具 |
| execution | allowed_file_patterns | list[string] | 可处理文件格式 |
| execution | can_run_standalone | bool | 是否可独立执行 |
| execution | can_compose_with | list[string] | 可组合 Skill |
| routing_policy | priority | int | 冲突时优先级 |
| routing_policy | conflict_group | string | 相似 Skill 分组 |
| routing_policy | prefer_when | list[string] | 什么时候优先选择该 Skill |
| routing_policy | defer_when | list[string] | 什么时候让位给其他 Skill |
| source | skill_dir | string | Skill 目录 |
| source | skill_md_path | string | SKILL.md 路径 |
| source | skill_md_hash | string | SKILL.md hash |
| source | generated_at | string | 生成时间 |
| embedding | model | string | Embedding 模型 |
| embedding | text_hash | string | routing_text hash |
| embedding | es_index | string | ES 索引名，对应 `SKILL_ROUTER_ES_INDEX` |
| embedding | es_doc_id | string | ES 文档 ID |
| evaluation | golden_queries | list[object] | 路由测试样例 |
| evaluation | last_eval | object | 最近一次评估结果 |

---

### 5.4 Router Card 示例：network-traffic-analysis

```json
{
  "schema_version": "1.0.0",
  "identity": {
    "id": "network-traffic-analysis",
    "name": "网络流量分析",
    "description": "用于解析 pcap/pcapng 网络流量文件，提取协议、会话、DNS、HTTP、TLS 等字段，并识别异常通信与潜在安全事件。"
  },
  "scope": {
    "scenes": ["network_traffic"],
    "is_public": false,
    "task_types": ["pcap_parse", "protocol_analysis", "anomaly_detect", "domain_analysis", "security_report"],
    "input_types": ["pcap", "pcapng", "cap", "csv"],
    "output_types": ["flow_csv", "domain_list", "anomaly_findings", "security_report"]
  },
  "routing": {
    "routing_text": "名称：网络流量分析\n描述：用于解析 pcap/pcapng 网络流量文件，识别异常通信、可疑域名和安全事件。\n适用场景：网络流量分析。\n适用任务：pcap解析、协议统计、DNS分析、异常通信检测、安全报告生成。\n输入类型：pcap、pcapng、cap、流量CSV。\n输出类型：协议统计、会话表、可疑域名、异常通信清单、安全分析报告。\n适合使用：用户上传 pcap 文件、要求分析流量异常、识别可疑域名、判断安全事件。\n不适合使用：政策法规分析、整治台账合规判断、车辆轨迹分析、交通拥堵预测。",
    "positive_triggers": ["分析 pcap 文件中的异常通信", "识别网络流量中的可疑域名", "统计 DNS、HTTP、TLS 和 TCP 会话"],
    "negative_triggers": ["分析整治台账合规风险", "检索政策法规条款", "分析车辆时空轨迹", "预测交通路网流量"],
    "keywords": ["pcap", "pcapng", "流量", "DNS", "HTTP", "TLS", "异常通信", "可疑域名"],
    "anti_keywords": ["法规", "政策", "台账", "轨迹", "交通流量"]
  },
  "body": {
    "source": "SKILL.md",
    "content": "# 网络流量分析\n\n## 适用场景\n...\n## 输入要求\n...\n## 执行流程\n...\n## 不适用边界\n..."
  },
  "execution": {
    "required_tools": ["read_file", "bash", "tshark"],
    "optional_tools": ["write_file", "chart_generator"],
    "allowed_file_patterns": ["*.pcap", "*.pcapng", "*.cap", "*.csv"],
    "can_run_standalone": true,
    "can_compose_with": ["data-analysis", "chart-visualization"]
  },
  "routing_policy": {
    "priority": 90,
    "conflict_group": "network_traffic_analysis",
    "prefer_when": ["用户上传 pcap/pcapng/cap 文件", "用户要求分析异常通信、协议行为、可疑域名或安全事件"],
    "defer_when": ["任务只是通用 CSV 统计，应优先使用 data-analysis", "任务只是生成图表，应优先使用 chart-visualization"]
  },
  "source": {
    "skill_dir": "custom/network-traffic-analysis",
    "skill_md_path": "custom/network-traffic-analysis/SKILL.md",
    "skill_md_hash": "sha256:xxxx",
    "generated_at": "2026-05-12T00:00:00Z",
    "generator_version": "0.1.0"
  },
  "embedding": {
    "model": "SkillRouter-Embedding-0.6B",
    "text_hash": "sha256:yyyy",
    "es_index": "citybrain-skill-router-cards",
    "es_doc_id": "network-traffic-analysis"
  }
}
```

---

### 5.5 Router Card 示例：law-regulations-rag

```json
{
  "schema_version": "1.0.0",
  "identity": {
    "id": "law-regulations-rag",
    "name": "政策法规检索",
    "description": "用于检索法律法规、政策文件和相关条款，为台账合规分析、政策依据查询和执法风险判断提供法规依据。"
  },
  "scope": {
    "scenes": ["policy_regulation"],
    "is_public": false,
    "task_types": ["law_retrieval", "policy_retrieval", "legal_basis_mapping", "compliance_reference"],
    "input_types": ["text", "docx", "pdf", "xlsx"],
    "output_types": ["law_articles", "policy_references", "legal_basis_mapping"]
  },
  "routing": {
    "routing_text": "名称：政策法规检索\n描述：用于检索法律法规、政策文件和相关条款，为台账合规分析、政策依据查询和执法风险判断提供法规依据。\n适用场景：政策法规分析。\n适用任务：法规检索、政策条款召回、法规依据映射、合规判断依据检索。\n输入类型：用户问题、政策通知、整治台账、法规文本。\n输出类型：法规条款、政策依据、依据映射结果。\n适合使用：用户要求查询法律法规、查找政策依据、结合台账判断是否有合规风险。\n不适合使用：网络流量异常检测、pcap 解析、车辆轨迹分析、交通流量预测。",
    "positive_triggers": ["查一下相关法律条文", "检索这个问题对应的政策依据", "判断台账处置措施有没有法规依据"],
    "negative_triggers": ["分析 pcap 文件中的异常通信", "识别 DNS 可疑域名", "分析车辆轨迹异常停留", "预测道路交通流量"],
    "keywords": ["法规", "政策", "条款", "法律", "依据", "合规", "台账", "通知"],
    "anti_keywords": ["pcap", "DNS", "HTTP", "轨迹", "交通流量"]
  },
  "body": {
    "source": "SKILL.md",
    "content": "# 政策法规检索\n\n## 适用场景\n...\n## 输入要求\n...\n## 检索流程\n...\n## 不适用边界\n..."
  },
  "execution": {
    "required_tools": ["read_file", "law_search"],
    "optional_tools": ["write_file"],
    "allowed_file_patterns": ["*.txt", "*.docx", "*.pdf", "*.xlsx"],
    "can_run_standalone": true,
    "can_compose_with": ["data-analysis", "chart-visualization"]
  },
  "routing_policy": {
    "priority": 90,
    "conflict_group": "policy_regulation_retrieval",
    "prefer_when": ["用户明确要求查询法规、政策条款或合规依据", "任务需要为台账或通知匹配法律政策依据"],
    "defer_when": ["任务只是解析 Excel 表格，应优先使用 data-analysis", "任务只是绘制图表，应优先使用 chart-visualization"]
  },
  "source": {
    "skill_dir": "custom/law-regulations-rag",
    "skill_md_path": "custom/law-regulations-rag/SKILL.md",
    "skill_md_hash": "sha256:xxxx",
    "generated_at": "2026-05-12T00:00:00Z",
    "generator_version": "0.1.0"
  },
  "embedding": {
    "model": "SkillRouter-Embedding-0.6B",
    "text_hash": "sha256:yyyy",
    "es_index": "citybrain-skill-router-cards",
    "es_doc_id": "law-regulations-rag"
  }
}
```

---

## 6. Registry 与 Elasticsearch 向量索引设计

### 6.1 Registry 设计

`registry.json` 只保存 Skill 基础信息、Router Card 路径和 SkillRouter 专用 ES 索引配置，不存 embedding 向量。

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
      "es_doc_id": "network-traffic-analysis"
    }
  ]
}
```

---

### 6.2 Elasticsearch 索引划分

Elasticsearch 连接配置共用：

```env
ES_URL=http://172.17.0.1:3128
ES_USERNAME=citybrain-street
ES_PASSWORD=123456
```

不同模块使用不同索引：

| 环境变量 | 索引名 | 用途 |
|---|---|---|
| `ES_INDEX` | `network-traffic-rag-smoke-clean` | 现有 RAG 索引，用于网络流量 RAG / 文档片段检索 |
| `SKILL_ROUTER_ES_INDEX` | `citybrain-skill-router-cards` | 新增 SkillRouter 专用索引，用于 Router Card 向量检索 |

二者共用同一个 Elasticsearch 服务，但数据结构、检索目标和生命周期不同，必须分开存储。

---

### 6.3 SkillRouter ES 文档结构

每个 Router Card 对应 SkillRouter 专用索引中的一条文档。

```json
{
  "skill_id": "network-traffic-analysis",
  "name": "网络流量分析",
  "description": "用于解析 pcap/pcapng 网络流量文件，提取协议、会话、DNS、HTTP、TLS 等字段，并识别异常通信与潜在安全事件。",
  "scenes": ["network_traffic"],
  "is_public": false,
  "task_types": ["pcap_parse", "protocol_analysis", "anomaly_detect"],
  "input_types": ["pcap", "pcapng", "cap", "csv"],
  "output_types": ["flow_csv", "domain_list", "anomaly_findings", "security_report"],
  "routing_text": "名称：网络流量分析\n描述：用于解析 pcap/pcapng 网络流量文件...",
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

---

### 6.4 ES Mapping 示例

`embedding_vector.dims` 需要以 `SkillRouter-Embedding-0.6B` 实际输出维度为准。构建脚本第一次调用 Embedding API 后读取向量长度，并据此创建 mapping。

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
        "dims": 1024,
        "index": true,
        "similarity": "cosine"
      },
      "enabled": {"type": "boolean"},
      "updated_at": {"type": "date"}
    }
  }
}
```

说明：

```text
dims: 1024 仅为示例值，实际值由 Embedding API 返回向量长度决定。
```

---

## 7. Router Card 构建流程

### 7.1 构建命令

Router Card 是构建产物，不在运行时生成。

```bash
make extract-router-cards
```

内部执行：

```bash
python scripts/extract_router_cards.py
python scripts/build_skill_router_registry.py
python scripts/build_skill_router_es_index.py
```

---

### 7.2 构建流程

```text
扫描 skills/public 和 skills/custom
        │
        ▼
读取每个 SKILL.md
        │
        ▼
解析 frontmatter + 正文
        │
        ▼
提取 identity / scope / execution
        │
        ▼
生成 routing_text
        │
        ▼
清洗 body.content
        │
        ▼
写入 router_card.json
        │
        ▼
调用 SkillRouter-Embedding-0.6B API 生成 routing_text 向量
        │
        ▼
连接 Elasticsearch：ES_URL + ES_USERNAME + ES_PASSWORD
        │
        ▼
创建或更新 SkillRouter 专用索引：SKILL_ROUTER_ES_INDEX
        │
        ▼
写入每个 Skill 的 ES 文档
        │
        ▼
生成 registry.json
```

---

### 7.3 增量更新机制

每个 Router Card 记录：

```json
"skill_md_hash": "sha256:xxxx"
```

以下内容变化时重新生成 Router Card 并更新 SkillRouter 专用 ES 索引：

- SKILL.md 内容变化。
- Router Card schema 变化。
- routing_text 模板变化。
- embedding 模型变化。
- scenes/task_types 规范变化。
- ES 索引 mapping 变化。

---

## 8. Skill Creator 与 Router Card 自动更新

### 8.1 设计目标

系统中存在 `skill-creator`，用于创建新 Skill、修改已有 Skill、优化 Skill 描述以及评估 Skill 触发效果。Skill Creator 创建新 Skill 后，不能只生成 `SKILL.md`，还必须自动完成 Router Card 和 SkillRouter 索引更新，否则新 Skill 虽然存在于文件系统中，但不会被 SkillRouter 检索、召回和使用。

因此，Skill Creator 创建或修改 Skill 后，必须触发以下路由资产更新流程：

```text
skill-creator
  ↓
生成或修改 SKILL.md
  ↓
自动生成或更新 router_card.json
  ↓
校验 router_card.schema.json
  ↓
生成 router_card.routing_text
  ↓
调用 SkillRouter-Embedding-0.6B 生成向量
  ↓
写入 SKILL_ROUTER_ES_INDEX
  ↓
更新 skills/registry.json
  ↓
执行路由冲突检测
  ↓
新 Skill 进入 ready 状态
```

---

### 8.2 新 Skill 创建后的完整流程

当用户通过 `skill-creator` 创建新 Skill 时，系统执行：

```text
1. 创建 Skill 目录
   skills/custom/{new_skill}/

2. 写入 SKILL.md
   skills/custom/{new_skill}/SKILL.md

3. 自动生成 Router Card
   skills/custom/{new_skill}/router_card.json

4. 校验 Router Card
   使用 skills/router_card.schema.json 校验字段完整性

5. 生成 routing_text
   从 name、description、scene、task_types、input_types、output_types、
   positive_triggers、negative_triggers 等字段生成标准化路由文本

6. 生成向量
   调用 SkillRouter-Embedding-0.6B API，对 routing_text 生成 embedding_vector

7. 写入 SkillRouter 专用 ES 索引
   index = SKILL_ROUTER_ES_INDEX
   document_id = skill_id

8. 更新 registry.json
   写入新 Skill 的基本信息、router_card_path、skill_md_path、es_doc_id

9. 执行冲突检测
   与已有 Router Card 进行语义相似度和规则冲突检测

10. 更新状态
   检查通过后，设置 enabled=true，router_status=ready
```

---

### 8.3 Skill 修改后的更新流程

当用户通过 `skill-creator` 修改已有 Skill 时，系统需要判断是否影响路由资产。

以下字段变化时必须更新 Router Card 和 ES 索引：

- `SKILL.md` frontmatter 中的 `name`
- `SKILL.md` frontmatter 中的 `description`
- Skill 正文中的适用场景、输入输出、执行流程、边界条件
- Skill 依赖工具
- Skill 可组合关系
- Skill 的触发示例或反例

更新流程：

```text
修改 SKILL.md
  ↓
计算新的 skill_md_hash
  ↓
如果 hash 变化
  ↓
重新生成 router_card.json
  ↓
重新生成 routing_text_hash
  ↓
调用 Embedding API 生成新向量
  ↓
upsert 到 SKILL_ROUTER_ES_INDEX
  ↓
更新 registry.json
  ↓
执行路由回归测试
```

---

### 8.4 Skill 删除或禁用后的索引处理

如果某个 Skill 被删除或禁用，需要同步处理 Router Card 和 ES 索引。

禁用 Skill 时：

```json
{
  "enabled": false,
  "router_status": "disabled"
}
```

并同步更新 ES 文档：

```json
{
  "enabled": false
}
```

删除 Skill 时：

```text
删除 skills/custom/{skill_id}/
  ↓
从 registry.json 移除或标记 deleted
  ↓
从 SKILL_ROUTER_ES_INDEX 删除对应 ES 文档
```

建议第一版采用“软删除”：

```json
{
  "enabled": false,
  "router_status": "deleted",
  "deleted_at": "2026-05-12T00:00:00Z"
}
```

避免误删导致无法回滚。

---

### 8.5 Router 状态字段

每个 Skill 在 registry 中增加 Router 状态字段：

```json
{
  "id": "new-skill",
  "enabled": false,
  "router_status": "pending_index",
  "router_card_path": "custom/new-skill/router_card.json",
  "skill_md_path": "custom/new-skill/SKILL.md",
  "es_indexed": false,
  "last_indexed_at": null,
  "last_router_error": null
}
```

状态枚举：

| 状态 | 说明 |
|---|---|
| `pending_card` | 已创建 SKILL.md，但尚未生成 router_card.json |
| `pending_index` | 已生成 router_card.json，但尚未写入 ES |
| `pending_review` | 已生成索引，但冲突检测或人工检查未通过 |
| `ready` | Router Card、ES 索引、registry 均已完成，可参与路由 |
| `disabled` | Skill 被禁用，不参与路由 |
| `deleted` | Skill 被删除或软删除，不参与路由 |
| `error` | 自动构建失败，需要查看 `last_router_error` |

只有当状态满足：

```json
{
  "enabled": true,
  "router_status": "ready",
  "es_indexed": true
}
```

该 Skill 才能参与 SkillRouter 路由。

---

### 8.6 自动生成 Router Card 的输入与输出

输入：

```text
skills/custom/{skill_id}/SKILL.md
```

输出：

```text
skills/custom/{skill_id}/router_card.json
```

自动提取内容包括：

- `identity.id`
- `identity.name`
- `identity.description`
- `scope.scenes`
- `scope.is_public`
- `scope.task_types`
- `scope.input_types`
- `scope.output_types`
- `routing.routing_text`
- `routing.positive_triggers`
- `routing.negative_triggers`
- `routing.keywords`
- `routing.anti_keywords`
- `body.content`
- `execution.required_tools`
- `execution.optional_tools`
- `routing_policy.priority`
- `routing_policy.conflict_group`
- `source.skill_md_hash`
- `embedding.model`
- `embedding.es_index`
- `embedding.es_doc_id`

如果自动提取无法确定字段，应标记为待审查：

```json
{
  "router_status": "pending_review",
  "review_required_fields": [
    "scope.scenes",
    "scope.task_types",
    "routing.negative_triggers"
  ]
}
```

---

### 8.7 自动索引命令

系统需要支持全量构建和增量构建两种模式。

全量重建：

```bash
make build-skill-router-index
```

适用场景：

- 第一次部署 SkillRouter
- Router Card schema 变化
- Embedding 模型变化
- ES mapping 变化
- 大规模 Skill 迁移

增量更新：

```bash
python scripts/update_skill_router_index.py --skill custom/new-skill
```

适用场景：

- Skill Creator 新建单个 Skill
- Skill Creator 修改单个 Skill
- 单个 Skill 的 Router Card 需要重建

增量更新内部流程：

```text
读取指定 Skill 的 SKILL.md
  ↓
生成或更新 router_card.json
  ↓
校验 schema
  ↓
生成 embedding_vector
  ↓
upsert 到 SKILL_ROUTER_ES_INDEX
  ↓
更新 registry.json 中该 Skill 的状态
  ↓
执行局部冲突检测
```

---

### 8.8 Skill Creator 失败处理

如果 Skill Creator 创建 Skill 后，Router Card 或 ES 索引构建失败，新 Skill 不应直接进入可用状态。

失败状态示例：

```json
{
  "id": "new-skill",
  "enabled": false,
  "router_status": "error",
  "es_indexed": false,
  "last_router_error": {
    "stage": "build_embedding",
    "message": "Embedding API timeout",
    "updated_at": "2026-05-12T00:00:00Z"
  }
}
```

处理规则：

- `router_card.json` 生成失败：Skill 不参与路由。
- schema 校验失败：Skill 不参与路由。
- Embedding API 调用失败：Skill 不写入 ES，不参与路由。
- ES 写入失败：Skill 不参与路由。
- 冲突检测失败：进入 `pending_review`，待人工确认后再启用。
- 所有失败都必须写入 `last_router_error`，方便排查。

---

### 8.9 路由冲突检测

新增或修改 Skill 后，需要与已有 Skill 做冲突检测。

检测维度：

| 维度 | 说明 |
|---|---|
| scene overlap | 是否属于相同或相近场景 |
| task_types overlap | 是否处理相同任务类型 |
| input_types overlap | 是否接收相同输入 |
| output_types overlap | 是否输出相同结果 |
| routing_text similarity | routing_text 向量相似度 |
| positive_triggers similarity | 正向触发语义相似度 |
| negative_triggers conflict | 是否缺少反向边界 |
| required_tools overlap | 是否调用相似工具链 |

冲突报告示例：

```json
{
  "new_skill_id": "new-policy-checker",
  "conflicts": [
    {
      "existing_skill_id": "law-regulations-rag",
      "overlap_score": 0.82,
      "overlap_dimensions": [
        "scenes",
        "task_types",
        "positive_triggers"
      ],
      "suggestion": "请明确 new-policy-checker 是负责法规检索，还是负责合规风险判断；如果只做风险判断，应在 negative_triggers 中排除单纯法规检索任务。"
    }
  ]
}
```

冲突处理规则：

```text
overlap_score < 0.70
  → 允许进入 ready

0.70 <= overlap_score < 0.85
  → 进入 pending_review，可人工确认

overlap_score >= 0.85
  → 默认不启用，需要修改 Router Card 边界
```

---

### 8.10 Skill Creator 联动验收标准

| 验收项 | 预期 |
|---|---|
| 创建新 Skill | 自动生成 `SKILL.md` |
| Router Card 生成 | 自动生成 `router_card.json` |
| Schema 校验 | `router_card.json` 通过 `router_card.schema.json` |
| Embedding 构建 | 成功调用 SkillRouter-Embedding-0.6B |
| ES 索引写入 | 成功写入 `SKILL_ROUTER_ES_INDEX` |
| Registry 更新 | `registry.json` 增加新 Skill |
| 状态更新 | 成功后 `router_status=ready` |
| 冲突检测 | 输出冲突报告或通过 |
| 路由验证 | 新 Skill 能被相关 query 召回 |
| 失败处理 | 构建失败时 `enabled=false` 且写入 `last_router_error` |

---

## 9. SkillRouterMiddleware 设计

### 9.1 中间件职责

`SkillRouterMiddleware` 负责：

1. 读取用户 query 和上传文件信息。
2. 判断是否需要专业 Skill 路由。
3. 将 query 拆成粗粒度 task segments。
4. 对每个 task segment 调用 Embedding API 生成 query 向量。
5. 使用 query 向量查询 SkillRouter 专用 ES 索引，召回候选 Router Cards。
6. 对候选 Skill 调用 Reranker API 精排。
7. 选择场景专用 Skill 和公共 Skill。
8. 汇总生成 `routing_context`。
9. 构建 `skills_override`。
10. 将结果写入 ThreadState 和消息上下文。

---

### 9.2 路由流程

```text
用户 query + uploaded_files
        │
        ▼
L0：轻量跳过判断
        │
        ▼
L1：任务粗拆分
        │
        ▼
L2：Embedding API 生成 query 向量
        │
        ▼
L3：Elasticsearch Top-K 向量召回
        │
        ▼
L4：Reranker API 精排
        │
        ▼
L5：公共 Skill 过滤与去重
        │
        ▼
L6：生成 routing_context
        │
        ▼
L7：生成 skills_override
```

---

### 9.3 L0：轻量跳过判断

跳过明显闲聊或无任务 query：

```text
你好
在吗
谢谢
ok
你是谁
介绍一下自己
```

以下情况不跳过：

- 存在上传文件。
- 用户引用“这个文件”“这个表”“这个数据”。
- 当前 thread 已有任务上下文。
- 用户请求包含处理、分析、生成、判断、统计等任务意图。
- query 虽模糊但存在数据输入或文件输入。

---

### 9.4 L1：任务粗拆分

SkillRouterMiddleware 内部做粗粒度任务拆分，不依赖 TodoMiddleware。

输出结构：

```json
{
  "task_segments": [
    {
      "segment_id": "seg_001",
      "text": "分析 pcap 文件中的异常通信",
      "input_refs": ["traffic.pcap"],
      "expected_output": "异常通信清单和安全分析结论"
    },
    {
      "segment_id": "seg_002",
      "text": "查找台账合规判断所需的法规依据",
      "input_refs": ["ledger.xlsx"],
      "expected_output": "法规依据和政策条款"
    }
  ]
}
```

---

### 9.5 L2：Embedding API 生成 query 向量

调用：

```env
SKILLROUTER_EMBEDDING_BASE_URL=http://192.168.200.1:7800/v1
```

输入：

```text
task_segment.text + uploaded_file_signals
```

输出：

```json
{
  "embedding": [0.0123, -0.0345, "..."]
}
```

---

### 9.6 L3：Elasticsearch Top-K 召回

使用 query embedding 在 `SKILL_ROUTER_ES_INDEX` 指定的 SkillRouter 专用索引中执行向量检索。

查询目标字段：

```text
embedding_vector
```

过滤条件：

```text
enabled = true
```

可选过滤条件：

```text
scenes 包含候选场景
或 is_public = true
input_types 与上传文件类型匹配
```

推荐默认配置：

```yaml
top_k: 8
min_score: 0.45
```

---

### 9.7 L4：Reranker 精排

调用：

```env
SKILLROUTER_RERANKER_BASE_URL=http://192.168.200.1:7801/v1
```

输入：

```text
Query = task_segment.text
Document = candidate_skill.name + description + body
```

输出：

```json
[
  {
    "skill_id": "network-traffic-analysis",
    "score": 0.91,
    "role": "primary"
  },
  {
    "skill_id": "data-analysis",
    "score": 0.71,
    "role": "supporting"
  }
]
```

推荐默认配置：

```yaml
final_top_k_per_segment: 3
min_score: 0.65
```

---

### 9.8 L5：公共 Skill 筛选

公共 Skill 参与候选的条件：

- task_type 匹配。
- input_type 匹配。
- output_type 匹配。
- ES 向量召回进入 top-k。
- Reranker 分数高于阈值。

公共 Skill 选择约束：

```text
每个 task segment 最多选择 2 个 public skills。
公共 Skill 必须服务当前 segment，不允许全局默认注入。
```

---

## 10. routing_context 设计

SkillRouter 最终写入：

```python
state["routing_context"]
```

结构如下：

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
        {
          "id": "network-traffic-analysis",
          "role": "primary",
          "score": 0.91
        }
      ],
      "expected_outputs": ["anomaly_findings", "security_findings"],
      "depends_on": []
    },
    {
      "scene_task_id": "task_002",
      "segment_id": "seg_002",
      "segment_text": "检索台账合规判断所需的法规依据",
      "scene": "policy_regulation",
      "input_refs": ["ledger.xlsx"],
      "task_types": ["law_retrieval", "compliance_reference"],
      "selected_skills": [
        {
          "id": "law-regulations-rag",
          "role": "primary",
          "score": 0.89
        }
      ],
      "expected_outputs": ["law_articles", "policy_references"],
      "depends_on": []
    }
  ],
  "global_selected_skills": [
    "network-traffic-analysis",
    "law-regulations-rag"
  ],
  "global_allowed_tools": [
    "read_file",
    "bash",
    "tshark",
    "law_search",
    "write_file"
  ],
  "confidence": 0.89,
  "route_reason": "用户同时提供网络流量分析任务和政策法规依据检索任务"
}
```

---

## 11. skills_override 设计

SkillRouter 触发后构建一个 system message：

```text
<skills_override>
本次请求已由 SkillRouter 路由。

请优先使用以下 Skill，不要主动使用未列出的 Skill：

1. network-traffic-analysis
用途：解析 pcap/pcapng 文件，识别异常通信、可疑域名和安全事件。
适用任务：pcap_parse, anomaly_detect, domain_analysis
路径：custom/network-traffic-analysis/SKILL.md

2. law-regulations-rag
用途：检索法律法规、政策文件和相关条款，为合规分析提供依据。
适用任务：law_retrieval, policy_retrieval, legal_basis_mapping
路径：custom/law-regulations-rag/SKILL.md

任务包：
- task_001：分析 pcap 文件中的异常通信
- task_002：检索台账合规判断所需的法规依据

约束：
- 不要使用未列出的 Skill。
- 如确需额外 Skill，先说明原因。
- 若当前请求不需要专业 Skill，则直接普通回答。
</skills_override>
```

---

## 12. TodoMiddleware 对接

### 12.1 plan mode 开启

当 `is_plan_mode=True`：

- TodoMiddleware 读取 `routing_context.scene_tasks`。
- 按任务包生成 todo。
- 每个 todo 绑定推荐 Skill。
- 保留任务依赖关系。

示例：

```text
1. 使用 network-traffic-analysis 解析 pcap 文件并提取基础流量字段
2. 使用 network-traffic-analysis 检测异常通信和可疑域名
3. 使用 law-regulations-rag 检索与台账合规判断相关的法规和政策依据
4. 由主 Agent 汇总分析结果并输出综合结论
```

---

### 12.2 plan mode 未开启

当 `is_plan_mode=False`：

- TodoMiddleware 不触发。
- SkillRouter 仍写入 `routing_context`。
- SkillRouter 仍注入 `skills_override`。
- 主 Agent 根据 `routing_context` 和命中 Skill 直接执行。

---

## 13. ThreadState 扩展

```python
class ThreadState(AgentState):
    ...
    routing_context: NotRequired[dict | None]
```

要求：

- 可序列化。
- 可日志记录。
- 可调试。
- 可回放。
- 可供前端展示路由结果。

---

## 14. 配置文件

### 14.1 环境变量

```env
# SkillRouter Embedding 服务
SKILLROUTER_EMBEDDING_BASE_KEY=unused
SKILLROUTER_EMBEDDING_BASE_URL=http://192.168.200.1:7800/v1

# SkillRouter Reranker 服务
SKILLROUTER_RERANKER_BASE_KEY=unused
SKILLROUTER_RERANKER_BASE_URL=http://192.168.200.1:7801/v1

# Elasticsearch 公共连接配置
ES_URL=http://172.17.0.1:3128
ES_USERNAME=citybrain-street
ES_PASSWORD=123456

# RAG 索引，继续保留给 RAG 模块使用
ES_INDEX=network-traffic-rag-smoke-clean

# SkillRouter 专用索引，新增
SKILL_ROUTER_ES_INDEX=citybrain-skill-router-cards
```

说明：

- `ES_URL`、`ES_USERNAME`、`ES_PASSWORD` 是 Elasticsearch 公共连接配置。
- `ES_INDEX` 是现有 RAG 索引，不用于 SkillRouter 路由。
- `SKILL_ROUTER_ES_INDEX` 是新增环境变量，专门用于 SkillRouter 的 Router Card 向量索引。
- RAG 与 SkillRouter 可以共用同一个 ES 服务，但必须使用不同 index。

---

### 14.2 config.yaml

```yaml
skill_router:
  enabled: true

  router_cards:
    registry_path: "skills/registry.json"
    card_root: "skills"
    strict_missing_router_card: false

  vector_store:
    provider: "elasticsearch"
    url_env: "ES_URL"
    username_env: "ES_USERNAME"
    password_env: "ES_PASSWORD"
    index_env: "SKILL_ROUTER_ES_INDEX"
    default_index: "citybrain-skill-router-cards"
    vector_field: "embedding_vector"
    text_field: "routing_text"
    id_field: "skill_id"
    top_k: 8
    min_score: 0.45

  embedding:
    provider: "skillrouter_embedding_api"
    model_name: "SkillRouter-Embedding-0.6B"
    base_url_env: "SKILLROUTER_EMBEDDING_BASE_URL"
    api_key_env: "SKILLROUTER_EMBEDDING_BASE_KEY"
    default_base_url: "http://192.168.200.1:7800/v1"
    batch_size: 16
    timeout_seconds: 30

  reranker:
    provider: "skillrouter_reranker_api"
    model_name: "SkillRouter-Reranker-0.6B"
    base_url_env: "SKILLROUTER_RERANKER_BASE_URL"
    api_key_env: "SKILLROUTER_RERANKER_BASE_KEY"
    default_base_url: "http://192.168.200.1:7801/v1"
    final_top_k_per_segment: 3
    min_score: 0.65
    timeout_seconds: 60

  segmentation:
    enabled: true
    provider: "llm"
    model_name: null
    max_segments: 5

  public_skills:
    enabled: true
    max_public_skills_per_segment: 2

  debug:
    log_routing_trace: true
```

---

## 15. 中间件接入位置

SkillRouterMiddleware 应插入到 TodoMiddleware 之前。

```text
build_lead_runtime_middlewares()
SummarizationMiddleware
SkillRouterMiddleware
TodoMiddleware
TitleMiddleware
MemoryMiddleware
RunHistoryMiddleware
ViewImageMiddleware
SubagentLimitMiddleware
LoopDetectionMiddleware
ClarificationMiddleware
```

原因：

- TodoMiddleware 需要读取 `routing_context`。
- 主 Agent 需要在模型调用前获得 `skills_override`。
- SkillRouter 需要在 Agent Core 推理之前完成 Skill 裁剪。

---

## 16. 变更文件清单

| 文件 | 操作 | 说明 |
|---|---|---|
| `skills/router_card.schema.json` | 新建 | Router Card schema |
| `scripts/extract_router_cards.py` | 新建 | 从 SKILL.md 生成 router_card.json |
| `scripts/build_skill_router_registry.py` | 新建 | 生成 registry.json |
| `scripts/build_skill_router_es_index.py` | 新建 | 生成 SkillRouter 专用 ES 向量索引 |
| `scripts/update_skill_router_index.py` | 新建 | Skill Creator 新建/修改 Skill 后增量更新 Router Card 与 ES 索引 |
| `scripts/check_skill_router_conflicts.py` | 新建 | 检测新 Skill 与已有 Skill 的路由冲突 |
| `scripts/eval_skill_router.py` | 新建 | 路由评估脚本 |
| `Makefile` | 修改 | 新增 `extract-router-cards`、`build-skill-router-index`、`eval-skill-router` |
| `skills/{public,custom}/*/router_card.json` | 新建 | 每个 Skill 的 Router Card |
| `skills/registry.json` | 新建 | Router Card 注册索引 |
| `backend/packages/harness/deerflow/agents/middlewares/skill_router_middleware.py` | 新建 | SkillRouter 中间件 |
| `backend/packages/harness/deerflow/agents/thread_state.py` | 修改 | 新增 routing_context |
| `backend/packages/harness/deerflow/agents/lead_agent/agent.py` | 修改 | 接入中间件 |
| `backend/packages/harness/deerflow/skills/loader.py` | 修改 | 支持 selected_skills 过滤加载 |
| `config.yaml` | 修改 | 新增 skill_router 配置 |
| `.env` / 部署环境变量 | 修改 | 新增 `SKILL_ROUTER_ES_INDEX` |
| `skills/registry.json` | 修改 | 增加 `router_status`、`es_indexed`、`last_router_error` 等状态字段 |
| `backend/tests/test_skill_router_middleware.py` | 新建 | 中间件单元测试 |
| `backend/tests/test_router_card_schema.py` | 新建 | Router Card schema 测试 |
| `backend/tests/test_skill_router_es_index.py` | 新建 | ES 索引测试 |
| `backend/tests/test_skill_creator_router_update.py` | 新建 | Skill Creator 创建 Skill 后自动生成 Router Card 和更新 ES 索引测试 |
| `backend/tests/test_skill_router_conflicts.py` | 新建 | 新 Skill 路由冲突检测测试 |
| `backend/tests/test_skill_router_eval.py` | 新建 | 路由评估测试 |

---

## 17. 验证方案

### 17.1 Router Card 构建验证

执行：

```bash
make extract-router-cards
```

验收标准：

- 所有 enabled Skill 都生成 `router_card.json`。
- `registry.json` 正确生成。
- Router Card schema 校验通过。
- hash 不变时不会重复生成。

---

### 17.2 Elasticsearch 索引验证

执行：

```bash
make build-skill-router-index
```

验收标准：

- 成功连接 `ES_URL`。
- 使用 `ES_USERNAME` 和 `ES_PASSWORD` 完成认证。
- 成功创建或更新 `SKILL_ROUTER_ES_INDEX=citybrain-skill-router-cards` 指定的索引。
- 不修改、不覆盖 `ES_INDEX=network-traffic-rag-smoke-clean` 的 RAG 索引。
- 每个 enabled Skill 在 SkillRouter 专用索引中有一条文档。
- 每条文档包含 `embedding_vector` 字段。
- `embedding_vector` 维度与 Embedding API 返回维度一致。
- 可以用 query embedding 在 SkillRouter 专用索引中召回 Top-K 候选 Skill。

---

### 17.3 模型服务连通性验证

| 服务 | 地址 / 变量 | 预期 |
|---|---|---|
| Embedding | `http://192.168.200.1:7800/v1` | 可对 query 和 routing_text 生成 embedding |
| Reranker | `http://192.168.200.1:7801/v1` | 可对 query-document pair 返回相关性分数 |
| Elasticsearch | `ES_URL=http://172.17.0.1:3128` | 使用 `ES_USERNAME`、`ES_PASSWORD` 认证后可连接 |
| RAG 索引 | `ES_INDEX=network-traffic-rag-smoke-clean` | 继续保留给 RAG 模块使用 |
| SkillRouter 索引 | `SKILL_ROUTER_ES_INDEX=citybrain-skill-router-cards` | 存储 Router Card 向量并支持 Top-K 检索 |

---

### 17.4 单场景路由测试

| Query | 预期 Skill |
|---|---|
| 帮我分析这个 pcap 文件有没有异常通信 | network-traffic-analysis |
| 查一下相关法律条文并判断这个台账是否合规 | law-regulations-rag |
| 上传 Excel，帮我做统计并画图 | data-analysis + chart-visualization |
| 分析车辆轨迹的异常停留点 | 后续 custom：trajectory-analysis |
| 分析道路交通流量变化趋势 | 后续 custom：traffic-flow-analysis |

---

### 17.5 多场景路由测试

Query：

```text
上传 pcap 和整治台账，分别分析网络异常和政策合规风险，最后输出综合结论。
```

预期：

```text
scene_tasks:
1. network_traffic → network-traffic-analysis
2. policy_regulation → law-regulations-rag
3. 综合结论 → 由已命中场景 Skill 或主 Agent 汇总完成
```

如果后续新增政策风险研判 Skill，则政策法规场景可扩展为：

```text
policy_regulation → law-regulations-rag + policy-risk-analysis
```

---

### 17.6 闲聊跳过测试

| Query | 预期 |
|---|---|
| 你好 | trigger=false |
| 在吗 | trigger=false |
| 谢谢 | trigger=false |
| 你是谁 | trigger=false |

---

### 17.7 文件上传测试

| 上传文件 | Query | 预期 |
|---|---|---|
| traffic.pcap | 帮我看看 | network-traffic-analysis |
| table.xlsx | 做个统计 | data-analysis |
| notice.docx + ledger.xlsx | 判断有没有风险 | law-regulations-rag；如存在 policy-risk-analysis，则组合使用 |

---

### 17.8 TodoMiddleware 对接测试

- `is_plan_mode=True` 时，TodoMiddleware 能基于 scene_tasks 生成 todo。
- `is_plan_mode=False` 时，主 Agent 仍能基于 skills_override 执行。
- 无 routing_context 时，TodoMiddleware 保持原行为。

---

## 18. 分阶段实施计划

### 第一阶段：Router Card、Registry 与 Skill Creator 联动

目标：完成 Skill 路由数据标准化。

任务：

1. 定义 `router_card.schema.json`。
2. 编写 `extract_router_cards.py`。
3. 编写 `build_skill_router_registry.py`。
4. 生成所有 Skill 的 Router Card。
5. 生成 `registry.json`。
6. 为 registry 增加 `router_status`、`es_indexed`、`last_router_error` 等状态字段。
7. 定义 Skill Creator 创建新 Skill 后的自动 Router Card 生成流程。
8. 手工检查重点 Skill 的 Router Card 质量。

---

### 第二阶段：SkillRouter 专用 Elasticsearch 向量索引

目标：完成 Router Card 向量索引构建。

任务：

1. 新增环境变量 `SKILL_ROUTER_ES_INDEX=citybrain-skill-router-cards`。
2. 保留原有 `ES_INDEX=network-traffic-rag-smoke-clean` 供 RAG 使用。
3. 编写 `build_skill_router_es_index.py`。
4. 调用 SkillRouter-Embedding-0.6B API 生成 `routing_text` 向量。
5. 使用 `ES_URL`、`ES_USERNAME`、`ES_PASSWORD` 连接 ES。
6. 创建或更新 `SKILL_ROUTER_ES_INDEX` 指定的索引。
7. 写入 Router Card 文档和 `embedding_vector`。
8. 验证 ES Top-K 向量召回效果。

---

### 第三阶段：SkillRouterMiddleware MVP

目标：完成运行时路由闭环。

任务：

1. 加载 registry。
2. 对 task segment 调用 Embedding API。
3. 使用 query embedding 查询 `SKILL_ROUTER_ES_INDEX`。
4. Top-K 召回候选 Router Cards。
5. 调用 Reranker API 精排。
6. 输出 `routing_context`。
7. 构建 `skills_override`。
8. 接入 lead_agent middleware chain。

---

### 第四阶段：TodoMiddleware 对接

目标：让 Todo 基于 routing_context 生成更稳定的任务列表。

任务：

1. Todo prompt 增加 routing_context 使用说明。
2. todo item 绑定推荐 Skill。
3. 测试 plan mode 和非 plan mode 两种情况。

---

### 第五阶段：Skill Loader 改造

目标：从源头避免全量 Skill 注入。

任务：

1. 修改 Skill loader 支持 `available_skills` 参数。
2. `apply_prompt_template()` 接收 selected skills。
3. SkillRouter 触发时只加载 `global_selected_skills`。
4. 使用原生 filtered skill prompt 替代临时 skills_override。

---

### 第六阶段：冲突检测与 Skill Creator 联动

目标：新增 Skill 时自动检查 Router Card 是否和已有 Skill 重叠。

任务：

1. 新 Skill 创建后自动生成 Router Card。
2. 与现有 Router Card 做语义相似度检测。
3. 检查 `conflict_group`。
4. 自动生成 golden_queries。
5. 执行路由测试。
6. 不通过则要求修改 Router Card 边界。

---

## 19. 风险控制

| 风险 | 处理方式 |
|---|---|
| Router Card 质量差 | 引入 schema 校验、人工抽检、golden_queries |
| 公共 Skill 误注入 | 每个 segment 限制最多 2 个 public skills |
| 多场景拆分错误 | 保留 routing_trace，用测试集持续修正 |
| Reranker 输出分数异常 | 增加阈值、排序和角色校验 |
| Skill body 过长 | 构建阶段清洗并截断 body |
| registry 与 Router Card 不一致 | 使用 hash 校验和构建时校验 |
| ES mapping 维度不匹配 | 构建脚本根据 Embedding API 返回维度自动创建 mapping |
| SkillRouter 索引数据陈旧 | 通过 routing_text_hash 和 skill_md_hash 增量更新 |
| ES 认证失败 | 检查 `ES_USERNAME`、`ES_PASSWORD` 和 ES 权限配置 |
| RAG 与 SkillRouter 索引混用 | 强制 SkillRouter 只读写 `SKILL_ROUTER_ES_INDEX`，不读写 `ES_INDEX` |
| Skill Creator 新建 Skill 后无法被路由 | 创建 Skill 后自动生成 Router Card、更新 ES 索引和 registry |
| 新 Skill 与已有 Skill 边界重叠 | 创建后执行冲突检测，必要时进入 `pending_review` |
| Router Card 自动生成失败 | 新 Skill 保持 `enabled=false`，写入 `last_router_error` |

---

## 20. 最终交付物

1. `router_card.schema.json`
2. 每个 Skill 的 `router_card.json`
3. `skills/registry.json`
4. 新增环境变量：`SKILL_ROUTER_ES_INDEX=citybrain-skill-router-cards`
5. SkillRouter 专用 Elasticsearch index：`citybrain-skill-router-cards`
6. `extract_router_cards.py`
7. `build_skill_router_registry.py`
8. `build_skill_router_es_index.py`
9. `update_skill_router_index.py`
10. `check_skill_router_conflicts.py`
11. `SkillRouterMiddleware`
12. `routing_context` ThreadState 扩展
13. `skills_override` 注入逻辑
14. TodoMiddleware 对接逻辑
15. 配置项与环境变量
16. Skill Creator 联动逻辑
17. 单元测试、ES 索引测试、Skill Creator 联动测试与路由评估测试

---

## 21. 总结

本方案通过 Router Card、SkillRouter 双模型服务和 Elasticsearch 专用向量索引，为 DeerFlow 城市超脑构建一套可扩展的 Skill 路由机制。

最终运行链路为：

```text
SKILL.md
  ↓ 自动提取
router_card.json
  ↓ 生成 routing_text
SkillRouter-Embedding-0.6B API
  ↓ 生成 Router Card 向量
Elasticsearch SKILL_ROUTER_ES_INDEX
  ↓ 存储 SkillRouter 专用向量索引
SkillRouterMiddleware
  ↓ query 向量检索 Top-K
SkillRouter-Reranker-0.6B API
  ↓ 精排选择
routing_context + skills_override
  ↓
TodoMiddleware / Lead Agent
  ↓ 精准执行
```

该方案的核心价值：

- 不再全量注入 Skill。
- 支持多场景、多任务 query。
- 公共 Skill 按需注入。
- RAG 索引与 SkillRouter 索引分离。
- `ES_INDEX` 继续给 RAG 使用。
- 新增 `SKILL_ROUTER_ES_INDEX` 专门存储 Router Card 向量。
- 向量由 SkillRouter-Embedding-0.6B 构建。
- 精排由 SkillRouter-Reranker-0.6B 完成。
- 相似 Skill 通过 Router Card body 和 Reranker 精排区分。
- 新增场景和 Skill 时自动生成或更新 Router Card，并同步更新 SkillRouter 专用 ES 索引。
- TodoMiddleware 未触发时，主 Agent 仍可基于 routing_context 执行。
