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


# 工程师 B：Router Card 生成与 Registry

## 1. 你的职责

你负责构建侧的 Router Card 和 registry。

目标：

```text
SKILL.md
  ↓
router_card.json
  ↓
skills/registry.json
```

你不负责调用模型服务和写入 ES。

---

## 2. 分支

```bash
git checkout feature/skill-router-integration
git checkout -b feature/skill-router-card-builder
```

---

## 3. 依赖

依赖工程师 A：

```text
skills/router_card.schema.json
backend/packages/harness/deerflow/routing/schema.py
config.yaml 中 skill_router 配置
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
| 第 5.1 节 Router Card 定位 | Router Card 是什么 |
| 第 5.2 节 存储结构 | `router_card.json` 放在哪里 |
| 第 5.3 节 Router Card 字段规范 | 生成字段依据 |
| 第 5.4 节 network-traffic-analysis 示例 | 网络流量核心样例 |
| 第 5.5 节 law-regulations-rag 示例 | 政策法规核心样例 |
| 第 6.1 节 Registry 设计 | `skills/registry.json` 结构 |
| 第 7 章 Router Card 构建流程 | 构建脚本流程 |
| 第 8.6 节 自动生成 Router Card 的输入与输出 | 后续 Skill Creator 会复用你的生成逻辑 |
| 第 16 章 变更文件清单 | 你负责哪些文件 |
| 第 17.1 节 Router Card 构建验证 | 验收标准 |

---

## 5. 你需要新增或修改的文件

```text
scripts/extract_router_cards.py
scripts/build_skill_router_registry.py

skills/custom/network-traffic-analysis/router_card.json
skills/custom/law-regulations-rag/router_card.json
skills/registry.json
```

---

## 6. 开发任务

### 6.1 实现 extract_router_cards.py

职责：

```text
扫描 skills/public 和 skills/custom
读取每个 SKILL.md
解析 YAML frontmatter
提取 name / description
生成 identity
推断 scope
生成 routing_text
清洗 body.content
计算 skill_md_hash
计算 routing_text_hash
写入 router_card.json
```

### 6.2 生成两个核心 Router Card

必须优先保证：

```text
skills/custom/network-traffic-analysis/router_card.json
skills/custom/law-regulations-rag/router_card.json
```

重点人工校准字段：

```text
scope.scenes
scope.task_types
scope.input_types
scope.output_types
routing.positive_triggers
routing.negative_triggers
routing.keywords
routing.anti_keywords
execution.required_tools
routing_policy.prefer_when
routing_policy.defer_when
```

### 6.3 实现 build_skill_router_registry.py

生成：

```text
skills/registry.json
```

每个 Skill 包含：

```json
{
  "id": "network-traffic-analysis",
  "router_card_path": "custom/network-traffic-analysis/router_card.json",
  "skill_md_path": "custom/network-traffic-analysis/SKILL.md",
  "enabled": true,
  "router_status": "pending_index",
  "es_indexed": false,
  "last_indexed_at": null,
  "last_router_error": null,
  "routing_text_hash": "sha256:xxxx",
  "es_doc_id": "network-traffic-analysis"
}
```

---

## 7. 验收命令

```bash
python scripts/extract_router_cards.py
python scripts/build_skill_router_registry.py
```

验收：

```text
skills/custom/network-traffic-analysis/router_card.json 存在
skills/custom/law-regulations-rag/router_card.json 存在
skills/registry.json 存在
所有 router_card.json 通过 schema 校验
registry 中有 router_status / es_indexed / last_router_error
```
