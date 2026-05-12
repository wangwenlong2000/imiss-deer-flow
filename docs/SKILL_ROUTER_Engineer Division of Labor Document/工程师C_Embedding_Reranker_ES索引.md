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


# 工程师 C：Embedding / Reranker / Elasticsearch 索引

## 1. 你的职责

你负责模型服务客户端和 SkillRouter 专用 ES 向量索引。

你要实现：

```text
Embedding client
Reranker client
Elasticsearch store
build_skill_router_es_index.py
```

---

## 2. 分支

```bash
git checkout feature/skill-router-integration
git checkout -b feature/skill-router-es-index
```

---

## 3. 依赖

依赖工程师 A、B：

```text
config.yaml
skills/registry.json
skills/**/router_card.json
backend/packages/harness/deerflow/routing/schema.py
```

---

## 4. 必须阅读的 PRD 章节

请打开：

```text
/home/wwl/imiss-deer-flow-main/docs/SKILL_ROUTER_PRD.md
```

重点阅读：

| PRD 章节 | 阅读目的 |
|---|---|
| 第 4.2 节 在线服务与环境变量 | 三类服务地址 |
| 第 6.2 节 Elasticsearch 索引划分 | RAG 与 SkillRouter 索引分离 |
| 第 6.3 节 SkillRouter ES 文档结构 | ES 文档字段 |
| 第 6.4 节 ES Mapping 示例 | `dense_vector` 字段 |
| 第 7.2 节 构建流程 | 从 Router Card 到 ES |
| 第 9.5 节 Embedding API 生成 query 向量 | 运行时也会复用你的 client |
| 第 9.6 节 Elasticsearch Top-K 召回 | 查询接口依据 |
| 第 9.7 节 Reranker 精排 | 精排接口依据 |
| 第 17.2 节 Elasticsearch 索引验证 | 你的验收标准 |
| 第 19 章 风险控制 | ES mapping、认证、索引混用风险 |

---

## 5. 你需要新增的文件

```text
backend/packages/harness/deerflow/routing/embedding_client.py
backend/packages/harness/deerflow/routing/reranker_client.py
backend/packages/harness/deerflow/routing/es_store.py
scripts/build_skill_router_es_index.py
```

---

## 6. 开发任务

### 6.1 embedding_client.py

接口：

```python
class SkillRouterEmbeddingClient:
    def embed_text(self, text: str) -> list[float]:
        ...

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        ...
```

读取：

```text
SKILLROUTER_EMBEDDING_BASE_URL
SKILLROUTER_EMBEDDING_BASE_KEY
```

### 6.2 reranker_client.py

接口：

```python
class SkillRouterRerankerClient:
    def rerank(self, query: str, candidates: list[dict]) -> list[dict]:
        ...
```

输入 candidate 至少包含：

```text
skill_id
name
description
body
```

### 6.3 es_store.py

接口：

```python
class SkillRouterElasticStore:
    def search(self, query_vector: list[float], top_k: int, filters: dict | None = None) -> list[dict]:
        ...

    def upsert_card(self, card_doc: dict) -> None:
        ...
```

读取：

```text
ES_URL
ES_USERNAME
ES_PASSWORD
SKILL_ROUTER_ES_INDEX
```

严禁写入：

```text
ES_INDEX
```

### 6.4 build_skill_router_es_index.py

职责：

```text
读取 skills/registry.json
读取每个 router_card.json
调用 Embedding API
自动推断 embedding_vector dims
创建或更新 SKILL_ROUTER_ES_INDEX
写入 ES 文档
更新 registry 状态
```

---

## 7. 验收命令

```bash
python scripts/build_skill_router_es_index.py
```

验收：

```text
ES 中存在 citybrain-skill-router-cards
network-traffic-analysis 文档存在
law-regulations-rag 文档存在
embedding_vector 存在
registry 中对应 Skill 的 es_indexed=true
registry 中对应 Skill 的 router_status=ready
ES_INDEX=network-traffic-rag-smoke-clean 未被写入
```
