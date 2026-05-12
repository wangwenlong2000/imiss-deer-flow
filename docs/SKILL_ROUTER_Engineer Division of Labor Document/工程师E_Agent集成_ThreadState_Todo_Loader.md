# 工程师 E：Agent 集成、ThreadState、TodoMiddleware、Skill Loader

> 本文档为工程师 E 的最新版实施文档。  
> 已补充工程师 A 的交接说明：`skill_router_config.py` 已完成，但还需要在 `app_config.py` 的 `from_file()` 中接入 `load_skill_router_config_from_dict()`，让 `config.yaml` 中的 `skill_router` 配置真正生效。

---

## 0. 最新版 PRD 查阅位置

请先打开最新版 PRD：

```text
/home/wwl/imiss-deer-flow-main/docs/SKILL_ROUTER_PRD.md
```

本文档中提到的 PRD 章节，均指该文件中的章节。

---

## 1. 你的职责

你负责把 SkillRouter 接入 DeerFlow Agent Runtime。

你的目标是完成以下几类集成工作：

```text
1. 接收 A 的配置模块交接，并把 SkillRouter 配置接入 app_config.py
2. 扩展 ThreadState，新增 routing_context
3. 将 SkillRouterMiddleware 插入 Lead Agent 中间件链
4. 让 TodoMiddleware 能读取 routing_context
5. 让 Skill Loader 支持 selected_skills 过滤加载
```

---

## 2. 分支
  当前集成分支状态

  feature/skill-router-integration: 5 commits (A+B+C+D 已合并)

  需要交代给 E 的事项

  1. E 需要改的 4 个文件中，2 个还未被修改

  ┌────────────────────────────┬─────────────────────────────────────────────────────────────────────────┐
  │        E 要改的文件        │                                当前状态                                 │
  ├────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ config/app_config.py       │ ⚠️  未修改 — from_file() 里没有调用 load_skill_router_config_from_dict() │
  ├────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ agents/thread_state.py     │ ⚠️  未修改 — 还没有 routing_context 字段                                 │
  ├────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ agents/lead_agent/agent.py │ ⚠️  未修改 — 还没插入 SkillRouterMiddleware                              │
  ├────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ skills/loader.py           │ ⚠️  未修改 — 还没加过滤支持                                              │
  └────────────────────────────┴─────────────────────────────────────────────────────────────────────────┘

  这些是 E 自己的任务，需要在 feature/skill-router-agent-integration 分支上完成。

  2. __init__.py 导出状态

  routing/__init__.py 当前导出了：
  - RoutingContext, SceneTask, SelectedSkill（A 的 schema）
  - SkillRouterEmbeddingClient, SkillRouterElasticStore, SkillRouterRerankerClient（C 的客户端）
  - 缺少 should_route, segment_query, resolve, pick_primary（D 的函数）

  E 不需要管这个，但如果后续导入这些函数发现找不到，是正常现象，后续可以补充。

  3. E 的重点注意事项

  E 的任务中，最关键的是第 2 和第 3 步：

  - Middleware 顺序：SkillRouterMiddleware 必须插在 TodoMiddleware 前面。如果顺序反了，Todo 读不到 routing_context。
  - 配置开关：skill_router.enabled=false 时不能加载 SkillRouterMiddleware，否则会影响原有 Agent 行为。
  - ThreadState 字段：用 NotRequired[dict | None]，不要破坏原有的 AgentState 结构。
  - app_config.py 的 from_file()：加一行 load_skill_router_config_from_dict(config_data.get("skill_router", 
  {}))，不能覆盖或删改已有的 memory/title/subagents/summarization 加载。

  4. E 可以直接使用的已有组件

  from deerflow.routing import SkillRouterEmbeddingClient, SkillRouterElasticStore, SkillRouterRerankerClient
  from deerflow.routing.query_segmenter import should_route, segment_query
  from deerflow.routing.resolver import resolve, pick_primary
  from deerflow.routing.schema import RoutingContext, SceneTask, SelectedSkill
  from deerflow.config.skill_router_config import get_skill_router_config
  from deerflow.agents.middlewares.skill_router_middleware import SkillRouterMiddleware

  所有这些在集成分支上都已经可以正常导入了。

```bash
git checkout feature/skill-router-integration
git pull origin feature/skill-router-integration

git checkout -b feature/skill-router-agent-integration
```

---

## 3. 依赖关系

你依赖工程师 A、D 的输出。

### 3.1 依赖工程师 A

A 已完成或将完成：

```text
backend/packages/harness/deerflow/config/skill_router_config.py
```

其中包含：

```text
SkillRouterConfig
global _skill_router_config = SkillRouterConfig()
get_skill_router_config()
set_skill_router_config()
load_skill_router_config_from_dict()
```

A 的说明是：

```text
skill_router_config.py 已经遵循 memory_config.py、title_config.py、
subagents_config.py、summarization_config.py 的统一模式。

但 app_config.py 的 from_file() 里还没有调用
load_skill_router_config_from_dict()。

这个接入点属于运行时配置加载集成，需要由工程师 E 处理。
```

### 3.2 依赖工程师 D

D 会提供：

```text
SkillRouterMiddleware
routing_context 输出结构
skills_override 生成逻辑
```

你需要把 D 的 Middleware 接入 Lead Agent 的 middleware chain。

---

## 4. 必须阅读的 PRD 章节

请重点阅读：

| PRD 章节 | 阅读目的 |
|---|---|
| 第 9 章 SkillRouterMiddleware 设计 | 明确 Middleware 的运行时职责 |
| 第 10 章 routing_context 设计 | ThreadState 新字段的数据结构 |
| 第 11 章 skills_override 设计 | 非 plan mode 下的临时注入方式 |
| 第 12 章 TodoMiddleware 对接 | plan mode 与非 plan mode 的处理方式 |
| 第 13 章 ThreadState 扩展 | 新增 `routing_context` 字段 |
| 第 14 章 配置文件 | `skill_router` 配置如何从 `config.yaml` 加载 |
| 第 15 章 中间件接入位置 | SkillRouterMiddleware 必须放在 TodoMiddleware 前 |
| 第 16 章 变更文件清单 | 你负责修改哪些文件 |
| 第 17.8 节 TodoMiddleware 对接测试 | Todo 对接验收标准 |
| 第 18 章 第三、四、五阶段 | Middleware、Todo、Loader 分阶段实施 |
| 第 19 章 风险控制 | 配置未加载、Skill 全量注入、routing_context 不可序列化等风险 |

---

## 5. 你需要修改的文件

根据当前分工，你主要修改这些文件：

```text
backend/packages/harness/deerflow/config/app_config.py
backend/packages/harness/deerflow/agents/thread_state.py
backend/packages/harness/deerflow/agents/lead_agent/agent.py
backend/packages/harness/deerflow/skills/loader.py
TodoMiddleware 所在文件
```

说明：

```text
TodoMiddleware 所在文件以仓库实际路径为准。
如果现有 TodoMiddleware 路径不同，请搜索 TodoMiddleware 类所在文件后再修改。
```

---

# 6. 任务 E0：接入 A 的 SkillRouter 配置模块

## 6.1 背景

工程师 A 已完成：

```text
backend/packages/harness/deerflow/config/skill_router_config.py
```

并实现了统一配置模式：

```python
class SkillRouterConfig(BaseModel):
    ...

_skill_router_config = SkillRouterConfig()

def get_skill_router_config() -> SkillRouterConfig:
    ...

def set_skill_router_config(config: SkillRouterConfig) -> None:
    ...

def load_skill_router_config_from_dict(config_dict: dict) -> None:
    ...
```

但是现在还缺少一个关键集成点：

```text
app_config.py 的 from_file() 方法还没有调用 load_skill_router_config_from_dict()
```

如果不做这一步，`config.yaml` 里的：

```yaml
skill_router:
  enabled: true
```

不会真正加载到运行时的 `SkillRouterConfig`。

---

## 6.2 你要做什么

修改：

```text
backend/packages/harness/deerflow/config/app_config.py
```

在 `from_file()` 或项目现有的配置加载函数中，加入对 SkillRouter 配置的加载。

示例逻辑：

```python
from deerflow.config.skill_router_config import load_skill_router_config_from_dict
```

然后在读取完 YAML 配置后加入：

```python
load_skill_router_config_from_dict(config.get("skill_router", {}))
```

如果项目里变量名不是 `config`，而是 `config_data`、`yaml_config` 或类似名称，请按实际代码调整。

目标逻辑是：

```python
config_data = load_yaml_config(...)
load_memory_config_from_dict(config_data.get("memory", {}))
load_title_config_from_dict(config_data.get("title", {}))
load_subagents_config_from_dict(config_data.get("subagents", {}))
load_summarization_config_from_dict(config_data.get("summarization", {}))

# 新增
load_skill_router_config_from_dict(config_data.get("skill_router", {}))
```

---

## 6.3 注意事项

### 不能覆盖其他配置加载

不要删除或改坏已有的：

```text
memory_config
title_config
subagents_config
summarization_config
```

SkillRouter 应该作为新增配置模块接入，而不是替换原有配置逻辑。

### 缺省配置要能运行

如果 `config.yaml` 中没有 `skill_router` 字段，也不能报错。

所以调用时要使用：

```python
config_data.get("skill_router", {})
```

而不是：

```python
config_data["skill_router"]
```

### 配置读取路径要保持项目原有风格

如果当前项目统一使用相对 import，就按现有风格写。

例如可能是：

```python
from .skill_router_config import load_skill_router_config_from_dict
```

或者：

```python
from deerflow.config.skill_router_config import load_skill_router_config_from_dict
```

以同目录已有 `memory_config.py`、`title_config.py` 的写法为准。

---

## 6.4 验收方式

在 `config.yaml` 中配置：

```yaml
skill_router:
  enabled: true
  vector_store:
    index_env: "SKILL_ROUTER_ES_INDEX"
```

然后执行：

```bash
python - <<'PY'
from backend.packages.harness.deerflow.config.skill_router_config import get_skill_router_config

cfg = get_skill_router_config()
print("enabled =", cfg.enabled)
print("vector_store.index_env =", cfg.vector_store.index_env)
PY
```

预期：

```text
enabled = True
vector_store.index_env = SKILL_ROUTER_ES_INDEX
```

如果项目 import 路径不是 `backend.packages...`，请按项目实际运行方式改成对应 import。

---

## 6.5 E0 完成标准

```text
app_config.py 已 import load_skill_router_config_from_dict
from_file() 已调用 load_skill_router_config_from_dict(config.get("skill_router", {}))
没有影响 memory/title/subagents/summarization 的配置加载
config.yaml 中 skill_router 配置能被 get_skill_router_config() 读取
缺少 skill_router 配置时系统仍可启动
```

---

# 7. 任务 E1：扩展 ThreadState

## 7.1 要做什么

找到 ThreadState 定义文件：

```text
backend/packages/harness/deerflow/agents/thread_state.py
```

或仓库实际的 ThreadState 所在文件。

新增字段：

```python
routing_context: NotRequired[dict | None]
```

---

## 7.2 字段用途

`routing_context` 用于保存 SkillRouterMiddleware 的路由结果。

示例结构：

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
      "selected_skills": [
        {
          "id": "network-traffic-analysis",
          "role": "primary",
          "score": 0.91
        }
      ]
    }
  ],
  "global_selected_skills": ["network-traffic-analysis"],
  "confidence": 0.89
}
```

---

## 7.3 要求

```text
可序列化
可日志记录
可被 TodoMiddleware 读取
可被 Agent 执行阶段读取
不影响原有 ThreadState 字段
```

---

## 7.4 验收标准

能在运行时写入：

```python
state["routing_context"] = {
    "route_mode": "none",
    "trigger": False,
}
```

并且不会破坏 Agent 状态序列化。

---

# 8. 任务 E2：接入 Lead Agent Middleware Chain

## 8.1 要做什么

修改：

```text
backend/packages/harness/deerflow/agents/lead_agent/agent.py
```

将 SkillRouterMiddleware 插入到 TodoMiddleware 前面。

---

## 8.2 正确顺序

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

关键点：

```text
SkillRouterMiddleware 必须在 TodoMiddleware 前。
```

原因：

```text
TodoMiddleware 需要读取 routing_context.scene_tasks。
如果 SkillRouterMiddleware 在 TodoMiddleware 后面，Todo 就读不到路由结果。
```

---

## 8.3 示例代码

根据项目实际结构调整 import：

```python
from deerflow.routing.middleware import SkillRouterMiddleware
```

或：

```python
from deerflow.agents.middlewares.skill_router_middleware import SkillRouterMiddleware
```

插入逻辑：

```python
middlewares = build_lead_runtime_middlewares(lazy_init=True)

summarization_middleware = _create_summarization_middleware()
if summarization_middleware is not None:
    middlewares.append(summarization_middleware)

# 新增：必须在 TodoMiddleware 前
middlewares.append(SkillRouterMiddleware())

todo_list_middleware = _create_todo_list_middleware(is_plan_mode)
if todo_list_middleware is not None:
    middlewares.append(todo_list_middleware)
```

---

## 8.4 配置开关

接入时建议读取：

```python
from deerflow.config.skill_router_config import get_skill_router_config

if get_skill_router_config().enabled:
    middlewares.append(SkillRouterMiddleware())
```

这样可以通过 `config.yaml` 控制开关：

```yaml
skill_router:
  enabled: true
```

---

## 8.5 验收标准

开启配置：

```yaml
skill_router:
  enabled: true
```

运行时日志或断点应能确认：

```text
SkillRouterMiddleware 在 TodoMiddleware 前执行
```

关闭配置：

```yaml
skill_router:
  enabled: false
```

应不加载 SkillRouterMiddleware，原有 Agent 行为不受影响。

---

# 9. 任务 E3：TodoMiddleware 对接 routing_context

## 9.1 要做什么

找到 TodoMiddleware 所在文件，并让它在生成 todo 时读取：

```python
state.get("routing_context")
```

---

## 9.2 plan mode 开启时

当：

```python
is_plan_mode=True
```

TodoMiddleware 如果发现：

```python
routing_context = state.get("routing_context")
```

且其中存在：

```text
routing_context.scene_tasks
```

则 Todo 生成应优先参考：

```text
scene_task.segment_text
scene_task.selected_skills
scene_task.depends_on
scene_task.expected_outputs
```

---

## 9.3 Todo 示例

对于多场景任务：

```text
上传 pcap 和整治台账，分别分析网络异常和政策合规风险，最后输出综合结论。
```

Todo 应生成类似：

```text
1. 使用 network-traffic-analysis 分析 pcap 文件中的异常通信
2. 使用 law-regulations-rag 检索台账合规判断所需法规依据
3. 由主 Agent 汇总两个任务的结果并输出综合结论
```

---

## 9.4 非 plan mode

当：

```python
is_plan_mode=False
```

TodoMiddleware 不触发。

此时主 Agent 仍应通过工程师 D 注入的：

```text
skills_override
```

和：

```text
routing_context
```

进行执行约束。

---

## 9.5 验收标准

```text
plan mode=true 时，Todo 能读取 routing_context.scene_tasks
Todo 内容能体现 selected_skills
plan mode=false 时，不依赖 Todo，Agent 仍可通过 skills_override 执行
没有 routing_context 时，TodoMiddleware 保持原行为
```

---

# 10. 任务 E4：Skill Loader 过滤加载

## 10.1 要做什么

修改：

```text
backend/packages/harness/deerflow/skills/loader.py
```

让 Skill Loader 支持只加载选中的 Skill。

---

## 10.2 推荐接口

在不破坏原逻辑的前提下，新增可选参数：

```python
def load_skills(available_skills: set[str] | None = None):
    ...
```

或：

```python
def get_available_skills(selected_skill_ids: set[str] | None = None):
    ...
```

以现有函数名为准，不强行改名。

---

## 10.3 行为要求

```text
available_skills is None
  → 保持原有全量加载行为

available_skills = {"network-traffic-analysis"}
  → 只加载 network-traffic-analysis

available_skills = {"network-traffic-analysis", "law-regulations-rag"}
  → 只加载这两个 Skill
```

---

## 10.4 和 skills_override 的关系

MVP 阶段可以先用工程师 D 的 `skills_override` 跑通。

但是最终目标是：

```text
Skill Loader 原生过滤
```

原因：

```text
skills_override 只是运行时约束，原始 prompt 中可能仍然包含全量 Skill。
Loader 过滤可以从源头避免无关 Skill 注入。
```

---

## 10.5 验收标准

当路由结果是：

```json
{
  "global_selected_skills": ["network-traffic-analysis"]
}
```

最终注入的 Skill 只包含：

```text
network-traffic-analysis
```

不应继续全量注入其他无关 Skill。

---

# 11. E 的最终验收清单

工程师 E 完成后，集成负责人应检查：

```text
[ ] app_config.py 已接入 load_skill_router_config_from_dict()
[ ] config.yaml 中 skill_router 配置能被 get_skill_router_config() 读取
[ ] skill_router.enabled=false 时不会加载 SkillRouterMiddleware
[ ] skill_router.enabled=true 时会加载 SkillRouterMiddleware
[ ] ThreadState 新增 routing_context
[ ] SkillRouterMiddleware 在 TodoMiddleware 前执行
[ ] TodoMiddleware 能读取 routing_context.scene_tasks
[ ] 没有 routing_context 时 TodoMiddleware 保持原行为
[ ] Skill Loader 支持 selected_skills / available_skills 过滤
[ ] available_skills=None 时保持原有行为
```

---

# 12. 建议提交顺序

```text
commit 1: integrate skill router config into app_config
commit 2: add routing_context to ThreadState
commit 3: integrate SkillRouterMiddleware into lead agent
commit 4: make TodoMiddleware aware of routing_context
commit 5: add filtered skill loading support
```

---

# 13. 给工程师 A 的回执说明

E 完成后，可以回复 A：

```text
已在 app_config.py 的 from_file() 中接入 load_skill_router_config_from_dict()。
现在 config.yaml 中的 skill_router 配置可以正确加载到 get_skill_router_config()。
后续 SkillRouterMiddleware 接入时会读取 get_skill_router_config().enabled 作为开关。
```

---

# 14. 不属于 E 的任务

E 不负责：

```text
Router Card schema
Router Card 自动生成
Registry 构建
Embedding / Reranker API 客户端
ES 索引构建
SkillRouterMiddleware 内部召回和精排逻辑
Skill Creator 自动更新
冲突检测
```

这些分别由 A、B、C、D、F 负责。

---

# 15. 最终结论

这个新增任务：

```text
app_config.py 中调用 load_skill_router_config_from_dict()
```

应由工程师 E 完成。

原因：

```text
A 负责定义配置模块
E 负责运行时集成
app_config.py 是配置进入运行时的集成点
```
