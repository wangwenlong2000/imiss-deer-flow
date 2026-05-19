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


# 分工总览

## 1. 总体并行分工

| 工程师 | 工作包 | 分支名 | 主要产出 |
|---|---|---|---|
| A | 公共契约、Schema、配置 | `feature/skill-router-contracts` | 环境变量、config、Router Card schema、routing schema |
| B | Router Card 生成、Registry | `feature/skill-router-card-builder` | `extract_router_cards.py`、`build_skill_router_registry.py`、样例 Router Card |
| C | Embedding / Reranker / ES 索引 | `feature/skill-router-es-index` | 模型客户端、ES store、ES 索引构建脚本 |
| D | SkillRouterMiddleware | `feature/skill-router-middleware` | query segmenter、resolver、middleware、skills_override |
| E | Agent 集成、ThreadState、Todo、Skill Loader | `feature/skill-router-agent-integration` | 接入 lead agent、扩展 ThreadState、Todo 对接、Skill Loader 过滤 |
| F | Skill Creator 自动更新、冲突检测、测试 | `feature/skill-router-skill-creator` | 增量索引、冲突检测、Skill Creator 联动、测试 |
| 集成负责人 | 合并、冲突处理、总体验收 | `feature/skill-router-integration` | 集成分支、端到端验收 |

---

## 2. 推荐合并顺序

```text
1. feature/skill-router-contracts
2. feature/skill-router-card-builder
3. feature/skill-router-es-index
4. feature/skill-router-middleware
5. feature/skill-router-agent-integration
6. feature/skill-router-skill-creator
```

---

## 3. 公共环境变量

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
NETWORK_TRAFFIC_ES_INDEX=network-traffic-rag-smoke-clean

# SkillRouter 专用索引，新增
SKILL_ROUTER_ES_INDEX=citybrain-skill-router-cards
```

---

## 4. 严格约束

```text
NETWORK_TRAFFIC_ES_INDEX 只给 RAG 用。
SKILL_ROUTER_ES_INDEX 只给 SkillRouter 用。
SkillRouter 不能写入 NETWORK_TRAFFIC_ES_INDEX。
Router Card 文件名统一叫 router_card.json，不使用 root_card.json。
```

---

## 5. 每个 PR 必须说明

```md
## 变更内容

## 对应工作包

## 必须阅读的 PRD 章节是否已阅读

## 验证命令

## 是否修改公共契约

## 是否影响 NETWORK_TRAFFIC_ES_INDEX

## 是否影响 SKILL_ROUTER_ES_INDEX
```
