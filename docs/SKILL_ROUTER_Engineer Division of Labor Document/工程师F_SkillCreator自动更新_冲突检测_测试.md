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


# 工程师 F：Skill Creator 自动更新、冲突检测、测试

## 1. 你的职责

你负责 Skill Creator 创建或修改 Skill 后的自动路由资产更新闭环，以及冲突检测和相关测试。

目标：

```text
skill-creator
  ↓
SKILL.md
  ↓
router_card.json
  ↓
embedding
  ↓
SKILL_ROUTER_ES_INDEX
  ↓
registry
  ↓
ready
```

---

## 2. 分支

```bash
git checkout feature/skill-router-integration
git checkout -b feature/skill-router-skill-creator
```

---

## 3. 依赖

依赖工程师 B、C：

```text
scripts/extract_router_cards.py
scripts/build_skill_router_registry.py
backend/packages/harness/deerflow/routing/embedding_client.py
backend/packages/harness/deerflow/routing/es_store.py
skills/registry.json
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
| 第 8.1 节 设计目标 | 为什么 Skill Creator 要自动更新 |
| 第 8.2 节 新 Skill 创建后的完整流程 | 新建 Skill 后完整链路 |
| 第 8.3 节 Skill 修改后的更新流程 | 修改已有 Skill 后如何重建索引 |
| 第 8.4 节 Skill 删除或禁用后的索引处理 | 禁用和软删除 |
| 第 8.5 节 Router 状态字段 | `router_status` 状态机 |
| 第 8.7 节 自动索引命令 | 增量更新命令 |
| 第 8.8 节 Skill Creator 失败处理 | 失败时怎么记录 |
| 第 8.9 节 路由冲突检测 | 冲突检测规则 |
| 第 8.10 节 Skill Creator 联动验收标准 | 你的主要验收表 |
| 第 16 章 变更文件清单 | 你负责哪些文件 |
| 第 17.2 节 ES 索引验证 | 写入 ES 的验收 |
| 第 19 章 风险控制 | 新 Skill 不可路由、冲突、失败状态 |

---

## 5. 你需要新增或修改的文件

```text
scripts/update_skill_router_index.py
scripts/check_skill_router_conflicts.py

backend/tests/test_skill_creator_router_update.py
backend/tests/test_skill_router_conflicts.py
```

可能需要修改：

```text
skills/public/skill-creator/SKILL.md
skills/registry.json
Makefile
```

---

## 6. 开发任务

### 6.1 update_skill_router_index.py

命令：

```bash
python scripts/update_skill_router_index.py --skill custom/new-skill
```

职责：

```text
读取指定 Skill 的 SKILL.md
生成或更新 router_card.json
校验 schema
生成 routing_text
调用 SkillRouter-Embedding-0.6B
upsert 到 SKILL_ROUTER_ES_INDEX
更新 registry.json
执行局部冲突检测
更新 router_status
```

### 6.2 状态字段

新 Skill 初始：

```json
{
  "enabled": false,
  "router_status": "pending_index",
  "es_indexed": false,
  "last_indexed_at": null,
  "last_router_error": null
}
```

成功后：

```json
{
  "enabled": true,
  "router_status": "ready",
  "es_indexed": true,
  "last_indexed_at": "2026-05-12T00:00:00Z",
  "last_router_error": null
}
```

失败后：

```json
{
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

### 6.3 check_skill_router_conflicts.py

检测：

```text
scene overlap
task_types overlap
input_types overlap
output_types overlap
routing_text similarity
positive_triggers similarity
negative_triggers conflict
required_tools overlap
```

状态规则：

```text
overlap_score < 0.70 → ready
0.70 <= overlap_score < 0.85 → pending_review
overlap_score >= 0.85 → 不启用，需要修改 Router Card
```

### 6.4 Skill Creator 串联

如果可以修改 Skill Creator 流程，则在创建新 Skill 后自动调用：

```bash
python scripts/update_skill_router_index.py --skill custom/{new_skill}
```

如果短期不能自动调用，则先在 `skills/public/skill-creator/SKILL.md` 中补充说明。

---

## 7. 验收命令

```bash
python scripts/update_skill_router_index.py --skill custom/test-demo-skill
python scripts/check_skill_router_conflicts.py --skill custom/test-demo-skill
python -m pytest backend/tests/test_skill_creator_router_update.py
python -m pytest backend/tests/test_skill_router_conflicts.py
```

验收：

```text
router_card.json 自动生成
ES 文档自动 upsert
registry 自动更新
失败时 enabled=false
失败时 last_router_error 有内容
冲突高时进入 pending_review
```
