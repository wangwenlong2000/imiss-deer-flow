# 意图识别耗时分析（2026-10-22）

本文记录优化前的多调用流程及历史日志。后续已完成一次结构化识别改造，当前实现、真实模型评测和回滚说明见 [优化报告](intent-recognition-optimization-20261022.md)。下文“当前流程”指改造前的基线。

## 结论与证据范围

当前意图识别节点承担了任务切分、场景判断、参数抽取、任务改写和执行规划，普通单场景任务通常也串行调用 5 次大模型。另有一个确定的参数传递缺陷：调用者指定 `thinking_enabled=False` 时，模型工厂原先没有发送 Qwen 所需的 `enable_thinking: false`，因此不能保证关闭思考。本次已修复该缺陷并限制本部署的对话模型为 `Qwen3.6-35B-A3B`。

依据是当前代码、`config.yaml`、本地 `logs/langgraph.log` 和离线回归测试，没有执行新的在线性能压测。以下时间不能代表平均值、P95 或本地部署速度，也不能用来宣称修复后的提速比例。

## 日志中的实际延迟

样本 run：`01a13f6c-b852-7b23-8712-2d5cea787e40`，任务为基于上传台账、模拟整治通知与法规核验索引生成决策简报。下表已将日志 UTC 时间转换为北京时间，日期均为 2026-10-15。

| 事件 | 北京时间 |
| --- | --- |
| 后台任务开始 | 19:57:32.705 |
| 意图识别节点第 1 次 HTTP 200 | 19:57:33.094 |
| 第 2 次 HTTP 200 | 19:57:43.991 |
| 第 3 次 HTTP 200 | 19:57:57.802 |
| 第 4 次 HTTP 200 | 19:58:17.420 |
| 第 5 次 HTTP 200 | 19:58:32.302 |
| 意图识别结果写入日志 | 19:58:51.226 |
| 后台任务成功结束 | 20:01:04.117 |

从任务启动到意图识别结束约 **78.5 秒**；整个任务约 **211.4 秒**，前者约占 **37%**。该样本证明意图识别显著拖慢首轮响应，但不支持“每个任务大部分时间都消耗在意图识别”的结论。意图节点真正开始时间未独立记录，78.5 秒也包含启动后的少量前置开销。

HTTP 200 日志不是流式响应完整生成结束时间，不能把相邻时间差当成各模型调用的精确耗时。现有日志没有逐阶段输出 token、思考 token 或耗时，无法准确拆出网络、排队、输入处理和解码各占多少。

## 为什么慢

### 1. 单场景任务也执行多次串行模型调用

入口为 `backend/packages/harness/deerflow/agents/middlewares/intent_recognition_middleware.py`，具体实现在 `backend/packages/harness/deerflow/routing/intent/classifier.py` 的同步及异步分类函数中。常见单场景路径为：

1. `_extract_task_spans_with_llm`：任务切分，返回用户任务文本。
2. `_recognize_scenes_with_llm`：场景列表识别。
3. `_resolve_scene_for_span`：再次解析单场景；规则明确时不再调用模型，模糊时增加一次模型请求。
4. `_extract_scene_params_with_llm`：参数抽取。
5. `_rewrite_question_with_llm`：问题改写。
6. `_generate_planning_steps_with_llm`：生成执行规划。

第 3 步无需模型时，总共仍有 5 次模型请求。第一步和改写步骤可能重新输出较长任务文本，规划步骤要求简单任务 2–3 步、复杂任务 4–7 步，增加了生成量。后续主智能体还要处理这些规划和任务文本。

异步实现也是逐个 `await`；多个任务片段通过 `for` 循环逐个执行场景解析和参数抽取。因此“使用异步 API”并没有缩短单次任务的这些串行依赖。

### 2. Qwen 的关闭思考参数原先没有生效

中间件创建模型时传入 `thinking_enabled=False`。但原 `models/factory.py` 的关闭逻辑只识别 `thinking.type`，本部署配置使用的是 `when_thinking_enabled.extra_body.enable_thinking`。结果是请求遗漏显式关闭参数，而非发送 `false`。

Qwen 官方说明该模型默认思考，并区分了两种关闭方式：DashScope 使用 `extra_body.enable_thinking=false`；本地 vLLM/SGLang 使用 `extra_body.chat_template_kwargs.enable_thinking=false`。参见 [官方模型卡](https://huggingface.co/Qwen/Qwen3.6-35B-A3B#instruct-or-non-thinking-mode)。多次分类调用都可能因此产生额外思考开销；由于历史日志未记录思考 token，具体耗时占比仍需实测。

当前 `thinking_budget: 2048` 仅在显式开启思考时注入。原先“关闭失败”的路径也不会注入这个预算，不能认为这些隐式思考已受 2048 token 限制。

### 3. 多轮和多任务路径还会继续增加调用

- 有历史任务时，`dialogue_act.py` 可调用对话轮次分类模型；判定为新任务后，分类器还可能调用上一场景关联判断模型。
- pending_action 的自然语言回答解析可能依次尝试结构化解析、回答关联判断等模型调用。
- 多任务片段逐个做场景解析及参数抽取，调用次数随片段数量增加。
- 单场景路径先取场景列表，再调用场景解析；解析无结果时还可能再次执行场景识别。规则高置信度只节省局部场景判断，不能跳过整个 LLM 流程。
- `abefore_agent` 内仍同步调用 `classify_dialogue_act`；多轮场景下可能同步请求模型，阻塞事件循环。这会影响并发响应，需要单独处理同步/异步对话分类接口。

### 4. 提示词、输出上限和容错路径也有影响

场景候选提示词包含描述、参数、示例、正反例、关键词、领域锚点和排除词；不少提示词同时在系统消息与用户消息重复放入原始问题。规划又携带原始任务、改写任务、任务片段和场景任务，长任务输入重复较多。

离线检查还发现：加载器将内置 6 个场景与部署配置的 10 个场景合并，实际候选为 **16 个**，候选描述本身共 **4765 字符**。一个政策法规任务的模拟调用中，场景识别系统提示词为 5058 字符，完整流程确实调用 5 次模型。这是字符数及调用结构验证，不是 token 测量或在线压测；内置场景是否应参与本部署，需结合现有场景回归集评估。

意图识别沿用通用模型的 `max_tokens: 8192` 和 `temperature: 0.7`，没有按分类、抽参、规划分别设置输出预算。8192 是上限，不代表每次都会生成这么多；但缺少阶段预算会放大异常长输出的尾延迟。

本部署没有显式配置意图识别专用 timeout 或重试预算，依赖 SDK 默认行为。分类器多个异常分支直接回退规则结果、缺少阶段日志，可能掩盖“等待模型后才回退”的耗时。当前样本不能证明发生了重试。

### 5. 本次瓶颈不是向量检索或重排

`config.yaml` 中 `skill_router.enabled: false`。意图识别仍然运行，是因为 `scene_filter_when_disabled` 默认开启，随后由 `SceneSkillFilterMiddleware` 按场景过滤技能。当前路径不调用 SkillRouter embedding/reranker，不能通过调整其参数解释这 78.5 秒。

输入合规门挂载在意图识别之后；工具、输出合规检查以及标题生成也会增加总耗时，但不能解释上述意图节点内的 5 次调用。

## 已完成的修改与验证

- 本地 `config.yaml` 的 `models` 仅保留 `qwen3.6-35b-a3b`，删除 `qwen3.6-plus`；标题、摘要和记忆模块的模型名也显式指定为前者。子智能体的内置配置使用继承模型，合规判断使用当前会话模型。
- 主智能体对请求和自定义智能体的模型名执行已有的配置解析：旧页面提交已移除的模型名时，记录警告并回退到唯一配置的 Qwen 模型。
- 模型工厂支持显式关闭 DashScope 和本地 chat-template 两种 Qwen 思考开关，保留已有的其他请求体选项。主任务显式开启思考的行为保留。
- 新增 Qwen 开关开/关及两种服务参数格式的回归测试；新增已移除模型名的主智能体回退用例。修改前复现了关闭参数缺失；同时确认既有两个 `reasoning_effort=minimal` 测试断言已过时，更新为现有实现规定的“关闭思考时移除 reasoning_effort”。
- 模型工厂、主模型解析、意图识别、对话轮次、场景过滤和合规意图等针对性测试共 **122 项通过**。真实 `ChatOpenAI` 的离线构造也验证了当前唯一模型及正确的思考开关，无外部模型调用。

本次保留了任务切分、抽参、改写和规划语义，没有进行调用链合并。配置文件属于本地部署配置，受 gitignore 管理；提交代码本身不会携带本机的 `config.yaml`。部署时需同步该配置，并重启 LangGraph/Gateway 以清除进程内配置缓存。

## 后续优化优先级

1. **先测修复后的端到端耗时。** 对固定问题、相同附件和历史上下文运行多次，记录各阶段耗时、调用次数、输入/输出 token、思考 token、超时/重试与回退原因，计算中位数和 P95。
2. **合并切分、场景、参数抽取。** 一次结构化输出返回 `task_spans`、`scenes`、`scene_tasks` 与 `params`；继续执行领域证据校验、场景白名单和既有缺参处理，避免缩短耗时却误选场景。
3. **减少前置改写和规划的独立调用。** 对清晰的原始任务保留原文；优先将执行规划合并进同一次结构化结果，继续返回 `planning_steps`，以兼容下述 OneCityAgent 输出链。若移交主智能体，必须同步适配规划事件，不能只删除当前字段。建议先以每轮 1–2 次路由模型调用为目标，效果与耗时需通过回归集验证。
4. **缩短候选提示词、设置阶段预算。** 保留领域边界，去掉重复原文，按场景选择、抽参、规划分别配置合理输出上限、timeout 和重试次数。不要使用过短统一上限截断长任务或 JSON。
5. **修复异步对话分类，再优化独立任务并行。** 控制并发并保留任务依赖；简单地将所有调用并行会破坏抽参→改写等依赖，也可能增加本地推理服务排队。

## OneCityAgent 任务规划依赖核实

当前规划确实在意图识别阶段生成：`classifier.py` 调用规划模型，将结果写入 `RoutingIntentResult.planning_steps`；中间件将其保存进 `intent_context`。`todo_middleware.py` 随后将这些步骤渲染为 `todo_routing_guidance` 消息里的“任务规划”段落。

`backend/app/gateway/routers/mobile.py` 从当前轮事件中提取该消息，调用 `_split_intent_and_planner_content()` 拆出规划，并构造 `agent_id=planner`、`source_event=task_planning` 的分段。对外映射为 `role=规划智能体`：非流式返回在 `ordered_segments` 中，流式通过 SSE `segment` 事件返回。

适配器在没有“任务规划”段落时会回退提取“任务切分”。因此，直接删掉 `planning_steps` 可能让平台仍显示“规划智能体”，但内容退化为原始任务片段，不能以“仍有规划分段”证明兼容。

已用两组合成事件离线验证：有 `planning_steps` 时返回编号执行步骤；没有它时返回任务切分文本。此验证覆盖后端格式化与适配链，没有向 OneCityAgent 发送真实任务，也未验证平台前端展示。

## 本地验收部署

本次固定的是模型，当前实际地址仍为 DashScope 云端，尚未部署本地模型。该模型的官方仓库提供权重、Apache-2.0 许可证和本地服务示例，参见 [Qwen 官方模型仓库](https://huggingface.co/Qwen/Qwen3.6-35B-A3B)。

验收改成本地 vLLM/SGLang 时，保留配置注册名与辅助模块引用 `qwen3.6-35b-a3b`，将同一个模型条目的服务字段调整为实际本地地址与服务公布的模型 ID，例如：

```yaml
models:
  - name: qwen3.6-35b-a3b
    display_name: Qwen3.6-35B-A3B
    use: langchain_openai:ChatOpenAI
    model: Qwen/Qwen3.6-35B-A3B
    base_url: "http://<本地推理服务地址>:8000/v1"
    api_key: $LOCAL_LLM_API_KEY
    max_tokens: 8192
    temperature: 0.7
    supports_vision: true
    supports_thinking: true
    supports_reasoning_effort: false
    when_thinking_enabled:
      extra_body:
        chat_template_kwargs:
          enable_thinking: true
```

本地服务使用的认证值与模型 ID 必须和实际服务一致。该配置替换现有模型条目，不能同时保留一个同名条目。工厂会为意图识别自动发送 `chat_template_kwargs.enable_thinking=false`；原 DashScope 的 `thinking_budget` 不应直接照搬到本地服务。
