# 产品需求文档（PRD）—— 城市超脑 SkillRouter 中间件

## 1. 背景

随着城市超脑系统的建设，系统需要处理多种数据类型和多场景任务，包括但不限于：

* **网络流量分析**：PCAP 文件解析、异常通信识别、可疑域名检测等。
* **时空轨迹分析**：人员/车辆轨迹统计、区域热力分析、异常行为检测等。
* **交通流量分析**：路网流量预测、拥堵分析、交通调度优化等。
* **政策法规分析**：整治台账解析、政策条款检索、合规风险判断等。

此外，系统中还存在大量**公共技能（public skill）**，如数据清洗、表格解析、文本摘要等，这些技能并不绑定具体场景，但可能被多个任务复用。

### 现有系统现状

* 已有 **TodoMiddleware**：负责任务跟踪，但仅在 `is_plan_mode=True` 时提供 `write_todos` 工具。
* 所有 enabled skills **全量注入 system prompt**，导致无关技能干扰模型、上下文 token 浪费、路由不精准。
* 需要统一的 **SkillRouter 中间件**：负责多场景任务的路由与技能选择。

---

## 2. 痛点

1. **全量注入导致无关技能干扰**
   * 用户一个 query 可能只涉及 1-2 个技能，但所有 19 个 enabled skills 的描述都被注入 prompt。
   * 增加上下文 token 消耗，模型容易被无关技能带偏。

2. **关键词触发太局限**
   * 用户可能不会明确使用规则词（如"分析"、"生成"）。
   * 多模态 query（上传文件 + 模糊描述）无法用关键词捕捉。
   * 新增场景/技能需要人工维护关键词列表。

3. **技能描述相似但实现不同**
   * 多个技能在 description 层面很接近，只有看 SKILL.md body 中的实现细节（用什么 API、脚本、参数）才能准确区分。

4. **TodoMiddleware 不一定触发**
   * 现有拆解逻辑依赖 `is_plan_mode`。
   * 若未触发，主智能体缺乏清晰的技能任务路径。

---

## 3. 目标

1. 实现**多场景、多任务 query 的精准路由**。
2. 精准匹配场景专用技能 + 公共技能，不注入无关技能。
3. 保持可扩展性：新增场景或技能无需改中间件逻辑。
4. 与 TodoMiddleware 对接，支持子任务拆解和执行顺序管理。
5. 对闲聊或无关 query 不触发 SkillRouter，节约资源。

---

## 4. 核心架构：Root Card + Bi-Encoder + Cross-Encoder

参考 SKILLROUTER 论文工业级架构，针对 ~19 skills 的规模裁剪：

### 4.1 Root Card 规范

每个 Skill 对应一个 `root_card.json` 文件，记录路由边界、触发条件和约束。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 唯一标识 |
| name | string | Skill 名称 |
| description | string | 功能描述 |
| scene | string | 绑定场景 ID，公共 Skill 填 `public` |
| task_types | list[str] | 该 Skill 能处理的任务类型 |
| keywords | list[str] | 边界关键词（用于快速匹配/降级兜底） |
| inputs | list[str] | 可接受的输入类型（文件类型、文本等） |
| outputs | list[str] | 输出类型 |
| constraints | object | Skill 执行约束或条件逻辑 |
| priority | int | rerank 或冲突解决优先级 |
| embedding_vector | list[float] | 由 SR-Emb-0.6B 对 name+description+code_snippet 编码 |
| skill_dir | string | 相对于 skills 根目录的路径 |
| skill_md_hash | string | SKILL.md 内容 hash，用于增量更新检测 |

### 4.2 存储结构

**主存放**：每个 Skill 目录下单独一个 Root Card 文件。

```
skills/
├── public/
│   ├── data-analysis/
│   │   ├── SKILL.md
│   │   └── root_card.json
│   ├── chart-visualization/
│   │   ├── SKILL.md
│   │   └── root_card.json
│   └── ...
├── custom/
│   ├── network-traffic-analysis/
│   │   ├── SKILL.md
│   │   └── root_card.json
│   └── law-regulations-rag/
│       ├── SKILL.md
│       └── root_card.json
├── root_card.schema.json
└── registry.json
```

**索引 registry**：集中维护路径和基本信息，用于快速粗排。

```json
{
  "version": 1,
  "skills": [
    {
      "id": "network-traffic-analysis",
      "name": "网络流量分析",
      "scene": "network_traffic",
      "task_types": ["pcap_parse", "anomaly_detect"],
      "root_card_path": "custom/network-traffic-analysis/root_card.json",
      "embedding_vector": [0.12, -0.34, ...]
    }
  ]
}
```

### 4.3 自动提取策略

Root Card 作为**构建产物提前生成**，运行时只负责读取。

**落地顺序**：

1. 定义 `root_card.schema.json`
2. 写 `scripts/extract_root_cards.py` — 解析 SKILL.md，提取元数据，生成 embedding
3. 写 `scripts/build_registry.py` — 汇总所有 root_card.json 生成 registry.json
4. Makefile 加 `make extract-root-cards`
5. 生成每个 skill 的 `root_card.json`
6. 生成 `skills/registry.json`

**增量更新机制**：基于 SKILL.md hash，只有 hash 变化的 skill 才重新提取。

**降级策略**：Root Card 缺失时开发环境可临时生成，生产环境降级为 SKIP 路由（维持现有全量注入行为）。

---

## 5. SkillRouter 路由机制

### 5.1 三层混合触发策略

```
用户 query
    │
    ├── L0: 闲聊/问候过滤 (规则，零成本)
    │   ├── 纯问候/无实质内容 → SKIP（返回 None）
    │   └── 匹配模式：^(你好|hi|hello|谢谢|再见|bye|在吗|ok|好的)...
    │
    ├── L1: Bi-Encoder 召回 (SR-Emb-0.6B)
    │   ├── __init__: 加载 registry.json → 缓存所有 skill embedding_vector
    │   ├── query 用 SR-Emb 编码 → cosine similarity → top-k 候选
    │   ├── 最高相似度 >= 阈值 → 进入 L2，携带 candidate skills
    │   └── 最高相似度 < 阈值 → 降级（有上传文件走L2全量，否则SKIP）
    │
    └── L2: Cross-Encoder 精排 (LLM)
        ├── 输入: query + top-k 候选的 root_card + SKILL.md body 关键段
        ├── 机制: LLM 深度注意力对比 query 意图与 skill 实现逻辑
        ├── 输出: {"trigger": bool, "segments": [{"text": str, "scene": str, "skills": [str]}]}
        └── 本质: listwise ranking — 同时对比多个候选选出最精确匹配
        │
        注入 <skills_override> + 写入 routing_context
```

### 5.2 Bi-Encoder 召回（L1）

- 使用 **SR-Emb-0.6B**（SKILLROUTER 论文推荐的轻量检索模型）
- 所有 skill embedding 在 middleware 初始化时加载缓存（`registry.json` 中已预计算）
- 用户 query 每次请求编码一次
- Cosine similarity 计算，阈值默认 0.6（可配置）
- 返回 top-k 候选（默认 5）

### 5.3 Cross-Encoder 精排（L2）

将 query + top-k 候选 skills 的完整信息拼接为一个 prompt，由 LLM 执行：

```
你是一个任务路由分析器。用户提出了一个请求，你需要判断应该调用哪些专业技能。

可用技能列表（每个包含名称、描述、输入输出和实现细节）：

<skill id="data-analysis">
名称: data-analysis
描述: Use this skill when the user uploads Excel/CSV files...
任务类型: [data_analysis, excel, csv]
输入: [excel, csv, xlsx]
输出: [table, csv, json, markdown]
实现细节: This skill analyzes user-uploaded Excel/CSV files using DuckDB...
</skill>

<skill id="network-traffic-analysis">
名称: network-traffic-analysis
描述: Use this skill to investigate network traffic...
任务类型: [pcap_parse, anomaly_detect, flow_analysis]
输入: [pcap, pcapng, cap, csv]
输出: [flow_csv, report]
实现细节: This skill is a strict, script-driven network traffic investigation...
</skill>

用户请求: {user_query}

请分析：
1. 这个请求是否需要调用专业技能？（闲聊/问候/简单问答 → trigger=false）
2. 如果需要，将请求拆分为独立任务 segment
3. 每个 segment 选择最匹配的技能（通过对比实现细节，不只是描述）

输出 JSON（只输出JSON，不要其他内容）：
{"trigger": true/false, "segments": [{"text": "...", "scene": "...", "skills": ["skill_name1", ...]}]}
```

### 5.4 路由输出格式

```json
{
  "segment_text": "分析用户提交的表单",
  "scene": "form_analysis",
  "selected_skills": [
    {"id": "search_form", "primary": true},
    {"id": "data_cleaning", "primary": false}
  ],
  "skill_plan": [
    {"order": 1, "skill_id": "search_form"},
    {"order": 2, "skill_id": "data_cleaning"}
  ],
  "allowed_tools": ["read_file", "table_parser"]
}
```

### 5.5 公共 Skill 处理

对每个候选任务，匹配公共 Skill（scene = public）：
- 按 task_type 或语义匹配
- 仅注入与任务相关的公共 Skill
- 防止全量注入造成误触

### 5.6 SkillRouter 中间件实现

```
agents/middlewares/skill_router_middleware.py

class SkillRouterMiddleware(AgentMiddleware):
    def __init__(self):
        self.registry = load_registry()          # 加载 registry.json
        self.skill_embeddings = cache_embeddings() # 缓存 embedding_vector

    def before_model(self, state, runtime):
        # 1. 提取 query + uploaded_files
        # 2. L0 闲聊过滤
        # 3. L1 Bi-Encoder 召回
        # 4. L2 LLM Cross-Encoder 精排
        # 5. 构建 <skills_override> SystemMessage
        # 6. 写入 state["routing_context"]
        return {"messages": [SystemMessage(content=override)], "routing_context": ctx}

    def abefore_model(self, state, runtime):
        return self.before_model(state, runtime)
```

---

## 6. 数据流

```
用户 query + uploaded_files
    │
    ▼
SkillRouter.before_model()
    ├── L0: 闲聊过滤 → 闲聊/问候 → 返回 None（维持现有行为）
    ├── L1: Bi-Encoder 召回 → query embedding → top-k candidate skills
    ├── L2: LLM Cross-Encoder 精排 → trigger + segments + selected_skills
    ├── 构建 <skills_override> SystemMessage（仅注入匹配技能）
    └── 写入 state["routing_context"] = {scene_tasks}
    │
    ▼
TodoMiddleware / TodoListMiddleware
    ├── 遍历 scene_tasks
    ├── 拆解每个任务包为子任务
    ├── 每个子任务绑定 skill
    └── 输出可执行 todolist
    │
    ▼
Lead Agent 执行
    ├── 看到 <skills_override> 中的技能列表
    ├── read_file 加载对应 SKILL.md
    └── 按 todolist 执行
```

---

## 7. TodoMiddleware 对接

- SkillRouter 已触发 → `state["routing_context"]` 包含 scene_tasks
- TodoMiddleware 读取 `routing_context`，按 segment 拆解子任务
- 如果 SkillRouter 未触发（闲聊等）→ TodoMiddleware 保持现有行为（仅 `is_plan_mode` 时触发）
- 不修改 TodoMiddleware 核心逻辑，只在其读取 `routing_context` 时增强子任务拆解精度

---

## 8. ThreadState 扩展

```python
# agents/thread_state.py
class ThreadState(AgentState):
    ...
    routing_context: NotRequired[dict | None]  # SkillRouter 输出
```

---

## 9. 配置文件

### 9.1 config.yaml

```yaml
skill_router:
  enabled: true
  # 用于路由决策的模型（null 使用默认模型）
  model_name: null
  # Embedding 触发阈值（cosine similarity）
  embedding_threshold: 0.6
  # Bi-Encoder 召回返回 top-k
  top_k_skills: 5
```

### 9.2 SR-Emb-0.6B 模型

下载模型到 `.models/sr-emb-0.6b/`，作为独立的 embedding provider。

---

## 10. 变更文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `skills/root_card.schema.json` | 新建 | Root Card 字段定义 |
| `scripts/extract_root_cards.py` | 新建 | Root Card 生成脚本 |
| `scripts/build_registry.py` | 新建 | 注册表构建脚本 |
| `Makefile` | Edit | 新增 `extract-root-cards` target |
| `skills/{public,custom}/*/SKILL.md` | Edit | 添加 scene/task_types/keywords |
| `skills/{public,custom}/*/root_card.json` | 新建 | 每个 skill 的 Root Card |
| `skills/registry.json` | 新建 | 集中式索引 |
| `backend/packages/harness/deerflow/skills/types.py` | Edit | Skill dataclass 新增 scene/task_types/keywords |
| `backend/packages/harness/deerflow/skills/parser.py` | Edit | 支持列表值解析 |
| `backend/packages/harness/deerflow/agents/middlewares/skill_router_middleware.py` | 新建 | SkillRouter 中间件 |
| `backend/packages/harness/deerflow/agents/thread_state.py` | Edit | 新增 routing_context |
| `backend/packages/harness/deerflow/agents/lead_agent/agent.py` | Edit | 接入 middleware 链 |
| `config.yaml` | Edit | 新增 skill_router 配置段 |
| `backend/tests/test_skill_router_middleware.py` | 新建 | 单元测试 |

---

## 11. 验证

1. `make test` — 全量测试通过
2. `make extract-root-cards` — 生成所有 root_card.json + registry.json
3. Embedding 触发测试：
   - "帮我看看这段网络数据有没有异常" → 应匹配 network-traffic-analysis
   - "查一下相关法律条文" → 应匹配 law-regulations-rag
4. LLM 意图测试：
   - "我想画个热力图看看分布" → trigger=true, 选 chart-visualization
   - "你好，今天天气不错" → trigger=false, 不注入 skills
5. 文件上传测试：
   - 上传 PCAP + "分析一下" → 必触发, 选 network-traffic-analysis
   - 上传 Excel + "做统计" → 触发, 选 data-analysis
6. 多场景测试：
   - 上传 PCAP + "分析流量并生成报告" → 选 network-traffic-analysis + 对应 report skill
7. TodoMiddleware 对接：
   - routing_context 存在时按 segment 拆解子任务
   - 无 routing_context 时维持现有行为
