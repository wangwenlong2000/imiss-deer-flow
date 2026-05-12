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


# 工程师 E：Agent 集成、ThreadState、TodoMiddleware、Skill Loader

## 1. 你的职责

你负责把 SkillRouter 接入 DeerFlow Agent Runtime。

目标：

```text
扩展 ThreadState
将 SkillRouterMiddleware 插入 Lead Agent 中间件链
让 TodoMiddleware 读取 routing_context
让 Skill Loader 支持 selected_skills 过滤加载
```

---

## 2. 分支

```bash
git checkout feature/skill-router-integration
git checkout -b feature/skill-router-agent-integration
```

---

## 3. 依赖

依赖工程师 A、D：

```text
routing_context schema
SkillRouterMiddleware
skills_override 格式
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
| 第 9 章 SkillRouterMiddleware 设计 | Middleware 运行时职责 |
| 第 10 章 routing_context 设计 | ThreadState 新字段 |
| 第 11 章 skills_override 设计 | 临时注入方式 |
| 第 12 章 TodoMiddleware 对接 | plan mode / 非 plan mode |
| 第 13 章 ThreadState 扩展 | 状态字段要求 |
| 第 15 章 中间件接入位置 | 必须放在 TodoMiddleware 前 |
| 第 16 章 变更文件清单 | 你负责哪些文件 |
| 第 18 章 第三、四、五阶段 | Middleware、Todo、Loader 逐步实施 |
| 第 17.8 节 TodoMiddleware 对接测试 | 验收标准 |

---

## 5. 你需要修改的文件

```text
backend/packages/harness/deerflow/agents/lead_agent/agent.py
backend/packages/harness/deerflow/agents/thread_state.py
backend/packages/harness/deerflow/skills/loader.py
TodoMiddleware 所在文件
```

---

## 6. 开发任务

### 6.1 扩展 ThreadState

新增字段：

```python
routing_context: NotRequired[dict | None]
```

### 6.2 接入 Lead Agent

在：

```text
backend/packages/harness/deerflow/agents/lead_agent/agent.py
```

把 SkillRouterMiddleware 放在 TodoMiddleware 前面。

目标顺序：

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

### 6.3 TodoMiddleware 对接

当 `is_plan_mode=True`：

```text
TodoMiddleware 读取 routing_context.scene_tasks
根据 selected_skills 生成 todo
```

当 `is_plan_mode=False`：

```text
主 Agent 使用 skills_override 执行
```

### 6.4 Skill Loader 过滤加载

修改：

```text
backend/packages/harness/deerflow/skills/loader.py
```

新增或扩展接口：

```python
load_skills(available_skills: set[str] | None = None)
```

行为：

```text
available_skills is None → 保持原有全量加载行为
available_skills = {"network-traffic-analysis"} → 只加载 network-traffic-analysis
```

MVP 阶段可先依赖 `skills_override`，但最终要做原生过滤。

---

## 7. 验收标准

```text
ThreadState 可以保存 routing_context
SkillRouterMiddleware 在 TodoMiddleware 之前执行
plan mode 下 Todo 能读取 scene_tasks
非 plan mode 下 Agent 能使用 skills_override
Skill Loader 支持 available_skills 过滤
```
