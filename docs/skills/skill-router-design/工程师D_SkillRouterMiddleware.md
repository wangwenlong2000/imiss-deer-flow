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


# 工程师 D：SkillRouterMiddleware

## 1. 你的职责

你负责运行时路由逻辑。

目标：

```text
用户 query
  ↓
should_route
  ↓
任务粗拆分
  ↓
Embedding query
  ↓
ES Top-K
  ↓
Reranker
  ↓
routing_context
  ↓
skills_override
```

你不负责接入 lead agent，也不负责修改 Skill Loader。

---

## 2. 分支

```bash
git checkout feature/skill-router-integration
git checkout -b feature/skill-router-middleware
```

---

## 3. 依赖

依赖工程师 A、C：

```text
backend/packages/harness/deerflow/routing/schema.py
backend/packages/harness/deerflow/routing/embedding_client.py
backend/packages/harness/deerflow/routing/reranker_client.py
backend/packages/harness/deerflow/routing/es_store.py
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
| 第 2.2 节 多场景和多任务 | 为什么要 task segment |
| 第 2.3 节 公共 Skill 不能全量注入 | public skill 选择规则 |
| 第 4.1 节 架构设计 | 总体链路 |
| 第 9.1 节 中间件职责 | 你的核心职责 |
| 第 9.2 节 路由流程 | L0 到 L7 |
| 第 9.3 节 L0 轻量跳过判断 | should_route |
| 第 9.4 节 L1 任务粗拆分 | segment 输出 |
| 第 9.5 节 L2 Embedding API | query 向量 |
| 第 9.6 节 L3 ES Top-K 召回 | ES 检索 |
| 第 9.7 节 L4 Reranker 精排 | 精排 |
| 第 9.8 节 L5 公共 Skill 筛选 | public skill 限制 |
| 第 10 章 routing_context 设计 | 输出结构 |
| 第 11 章 skills_override 设计 | 注入文本格式 |
| 第 17.4～17.7 节 | 路由测试用例 |

---

## 5. 你需要新增的文件

```text
backend/packages/harness/deerflow/routing/query_segmenter.py
backend/packages/harness/deerflow/routing/resolver.py
backend/packages/harness/deerflow/routing/middleware.py
```

也可以按项目现有风格放到：

```text
backend/packages/harness/deerflow/agents/middlewares/skill_router_middleware.py
```

但需要提前和工程师 E 约定 import 路径。

---

## 6. 开发任务

### 6.1 query_segmenter.py

第一版规则拆分即可：

```text
pcap / pcapng / cap → network_traffic
法规 / 政策 / 法律 / 合规 / 台账 → policy_regulation
Excel / csv / 统计 / 图表 → public data-analysis / chart-visualization
```

### 6.2 should_route

跳过：

```text
你好
在吗
谢谢
ok
你是谁
介绍一下自己
```

不跳过：

```text
有上传文件
用户说“这个文件”“这个表”“这个数据”
包含分析、判断、生成、统计、检索、处理等任务意图
```

### 6.3 resolver.py

输入：

```text
task segment
ES Top-K candidates
Reranker scores
```

输出：

```text
selected_skills
primary_skill
confidence
```

公共 Skill 限制：

```text
每个 task segment 最多 2 个 public skills
public skill 必须服务当前 segment
```

### 6.4 middleware.py

主流程：

```text
读取用户最后一条 message
读取上传文件信息
should_route
task segment
embedding_client.embed_text()
es_store.search()
reranker_client.rerank()
resolver.resolve()
生成 routing_context
生成 skills_override
写回 state
```

---

## 7. 验收标准

输入：

```text
帮我分析这个 pcap 文件有没有异常通信
```

输出：

```json
{
  "trigger": true,
  "global_selected_skills": ["network-traffic-analysis"]
}
```

输入：

```text
查一下相关法律条文并判断这个台账是否合规
```

输出：

```json
{
  "trigger": true,
  "global_selected_skills": ["law-regulations-rag"]
}
```

输入：

```text
你好
```

输出：

```json
{
  "trigger": false
}
```
