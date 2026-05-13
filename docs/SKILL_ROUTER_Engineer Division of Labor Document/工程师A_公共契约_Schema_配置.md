# 城市超脑 SkillRouter 中间件并行实施文档

> 本文档是从最新版 PRD 和工程实施方案拆分出来的单人工作包。  
> 请先打开最新版 PRD，再阅读本文档中的“必须阅读的 PRD 章节”。  
> 所有工程师统一从 `feature/skill-router-integration` 拉分支开发，最终先合入集成分支，不直接合入 main。

## 最新版 PRD 查阅位置

请所有工程师统一查阅项目内最新版 PRD：

```text
/home/wwl/imiss-deer-flow-main/docs/SKILL_ROUTER_PRD.md
```

后续本文档中提到的“PRD 第 X 章 / 第 X.Y 节”，均指这个文件中的章节。

---

## PRD 目录

| PRD 章节 | 章节名称 | 主要内容 |
|---|---|---|
| 第 1 章 | 项目背景 | 城市超脑多场景、Skill 数量增加、需要 SkillRouter 的背景 |
| 第 2 章 | 问题定义 | 全量 Skill 注入、多场景 query、公共 Skill 注入、相似 Skill 边界问题 |
| 第 3 章 | 产品目标 | 核心目标与交付目标 |
| 第 4 章 | 总体方案 | 整体架构、在线服务与环境变量 |
| 第 5 章 | Router Card 设计 | Router Card 定位、存储结构、字段规范、核心示例 |
| 第 6 章 | Registry 与 Elasticsearch 向量索引设计 | registry 结构、RAG 索引与 SkillRouter 索引分离、ES 文档结构、mapping |
| 第 7 章 | Router Card 构建流程 | 构建命令、构建流程、增量更新机制 |
| 第 8 章 | Skill Creator 与 Router Card 自动更新 | 新 Skill 创建后自动生成 Router Card、更新 ES、状态字段、冲突检测 |
| 第 9 章 | SkillRouterMiddleware 设计 | Middleware 职责、L0-L7 路由流程、任务拆分、ES 召回、Reranker 精排 |
| 第 10 章 | routing_context 设计 | routing_context 输出结构 |
| 第 11 章 | skills_override 设计 | 运行时临时约束注入格式 |
| 第 12 章 | TodoMiddleware 对接 | plan mode 与非 plan mode 的处理方式 |
| 第 13 章 | ThreadState 扩展 | 新增 routing_context 状态字段 |
| 第 14 章 | 配置文件 | 环境变量与 config.yaml |
| 第 15 章 | 中间件接入位置 | SkillRouterMiddleware 在 Agent middleware chain 中的位置 |
| 第 16 章 | 变更文件清单 | 新增和修改文件总表 |
| 第 17 章 | 验证方案 | Router Card、ES、模型服务、路由、Todo、Skill Creator 验收 |
| 第 18 章 | 分阶段实施计划 | 分阶段开发任务 |
| 第 19 章 | 风险控制 | Router Card、ES、Reranker、索引混用、Skill Creator 失败等风险 |
| 第 20 章 | 最终交付物 | 最终需要交付的文件、脚本和能力 |
| 第 21 章 | 总结 | 最终链路和核心价值 |

---


# 工程师 A：公共契约、Schema、配置

## 1. 你的职责

你负责 SkillRouter 的公共契约层。其他工程师都依赖你的输出，因此你的分支必须最先合并。

你要固定：

```text
环境变量
config.yaml 配置结构
Router Card JSON Schema
Python routing schema
公共字段命名
```

---

## 2. 分支

```bash
git checkout feature/skill-router-integration
git checkout -b feature/skill-router-contracts
```

---

## 3. 必须阅读的 PRD 章节

请打开：

```text
/home/wwl/imiss-deer-flow-main/docs/SKILL_ROUTER_PRD.md
```

重点阅读：

| PRD 章节 | 阅读目的 |
|---|---|
| 第 3 章 产品目标 | 明确公共契约要支撑的交付目标 |
| 第 4.2 节 在线服务与环境变量 | 明确 Embedding、Reranker、ES 变量 |
| 第 5.3 节 Router Card 字段规范 | 写 `router_card.schema.json` 的依据 |
| 第 6.1 节 Registry 设计 | registry 公共结构 |
| 第 6.2 节 Elasticsearch 索引划分 | `NETWORK_TRAFFIC_ES_INDEX` 与 `SKILL_ROUTER_ES_INDEX` 的边界 |
| 第 10 章 routing_context 设计 | Python schema 的依据 |
| 第 13 章 ThreadState 扩展 | 后续状态字段依赖 |
| 第 14 章 配置文件 | `config.yaml` 的最终结构 |
| 第 16 章 变更文件清单 | 你负责哪些文件 |
| 第 19 章 风险控制 | 重点看“索引混用”和“契约变更”风险 |

---

## 4. 你需要新增或修改的文件

### 新增

```text
skills/router_card.schema.json

backend/packages/harness/deerflow/routing/__init__.py
backend/packages/harness/deerflow/routing/schema.py
backend/packages/harness/deerflow/routing/config.py
```

### 修改

```text
config.yaml
.env.example
```

---

## 5. 开发任务

### 5.1 新增环境变量

在 `.env.example` 中新增：

```env
SKILLROUTER_EMBEDDING_BASE_KEY=unused
SKILLROUTER_EMBEDDING_BASE_URL=http://192.168.200.1:7800/v1

SKILLROUTER_RERANKER_BASE_KEY=unused
SKILLROUTER_RERANKER_BASE_URL=http://192.168.200.1:7801/v1

ES_URL=http://172.17.0.1:3128
ES_USERNAME=citybrain-street
ES_PASSWORD=123456

NETWORK_TRAFFIC_ES_INDEX=network-traffic-rag-smoke-clean
SKILL_ROUTER_ES_INDEX=citybrain-skill-router-cards
```

### 5.2 新增 skill_router 配置

在 `config.yaml` 中新增：

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

  reranker:
    provider: "skillrouter_reranker_api"
    model_name: "SkillRouter-Reranker-0.6B"
    base_url_env: "SKILLROUTER_RERANKER_BASE_URL"
    api_key_env: "SKILLROUTER_RERANKER_BASE_KEY"
    default_base_url: "http://192.168.200.1:7801/v1"
```

### 5.3 新增 Router Card Schema

新增：

```text
skills/router_card.schema.json
```

必须覆盖：

```text
identity
scope
routing
body
execution
routing_policy
source
embedding
evaluation
```

### 5.4 新增 Python Schema

新增：

```text
backend/packages/harness/deerflow/routing/schema.py
```

建议定义：

```python
from pydantic import BaseModel
from typing import Literal

class SelectedSkill(BaseModel):
    id: str
    role: Literal["primary", "supporting", "fallback"]
    score: float
    reason: str | None = None

class SceneTask(BaseModel):
    scene_task_id: str
    segment_id: str
    segment_text: str
    scene: str | None = None
    input_refs: list[str] = []
    task_types: list[str] = []
    selected_skills: list[SelectedSkill] = []
    expected_outputs: list[str] = []
    depends_on: list[str] = []

class RoutingContext(BaseModel):
    route_mode: Literal["none", "single_segment", "multi_segment"]
    trigger: bool
    primary_goal: str | None = None
    scene_tasks: list[SceneTask] = []
    global_selected_skills: list[str] = []
    global_allowed_tools: list[str] = []
    confidence: float = 0.0
    route_reason: str | None = None
```

---

## 6. 验收标准

```bash
python -c "from backend.packages.harness.deerflow.routing.schema import RoutingContext; print(RoutingContext(route_mode='none', trigger=False))"
```

必须满足：

```text
.env.example 包含 SKILL_ROUTER_ES_INDEX
config.yaml 包含 skill_router 配置
skills/router_card.schema.json 存在
routing/schema.py 可 import
没有 SKILL_ROUTER_ES_HOST
没有 NETWORK_TRAFFIC_ES_HOST
```
