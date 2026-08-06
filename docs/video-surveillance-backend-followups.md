# 视频监控：backend 侧待办清单（本轮未改动）

本轮审查范围是「只修 skill 侧」，以下问题都在 `backend/` 或前端，只记录不修改。
按影响程度排序。

## 0. `ViewImageMiddleware` 在 content 列表里放裸字符串，导致 dashscope 返回 400（已定位，一行可修）

**这是三轮测试里反复出现、此前一直记作「服务端 BadRequestError」的那个错误的真正原因。**
之前推测过是图片 payload 超限，**推测是错的**。

**证据链**（`logs/langgraph.log`，本轮 Q01）：

```
[ViewImageMiddleware] Injecting image details message with images before LLM call
HTTP Request: POST https://dashscope.aliyuncs.com/.../chat/completions "HTTP/1.1 400 Bad Request"
openai.BadRequestError: Error code: 400 - InternalError.Algo.InvalidParameter:
  When content is a list, each item must be an object (e.g. {"text": "..."}, {"image": "..."})
  rather than a bare string.
```

400 紧跟在 ViewImage 注入之后，同一次 LLM 调用。

**根因**：[`view_image_middleware.py:104-105`](../backend/packages/harness/deerflow/agents/middlewares/view_image_middleware.py)

```python
viewed_images = state.get("viewed_images", {})
if not viewed_images:
    return ["No images have been viewed."]   # <- list 里装裸字符串
```

该函数其余所有分支返回的都是 `{"type": "text", ...}` / `{"type": "image_url", ...}` 对象，
只有这条空态分支返回裸字符串。OpenAI 官方接口容忍这种写法，
**dashscope 兼容接口不容忍**，直接 400，整个 run 挂掉。

**触发条件**：`_should_inject_image_message()` 判定要注入（上一条 assistant 消息有
已完成的 `view_image` 调用），但此时 `state["viewed_images"]` 是空的。
`merge_viewed_images` 这个 reducer 明确支持「传空 dict 即清空」
（[`thread_state.py:43-44`](../backend/packages/harness/deerflow/agents/thread_state.py)），
所以图片被消费清空后、或 `view_image` 取图失败没写进 state 时，就会走到这条分支。

**修复**：把空态分支也返回对象形式即可。

```python
return [{"type": "text", "text": "No images have been viewed."}]
```

**影响**：视频事件分析必然要连续 `view_image` 审帧，是最容易踩到的场景。
上两轮报告里记作「路由通过、执行未收尾」的用例（C-001、E15），
真实死因很可能是这个 400，而不是递归上限。

**验证方式**：改完复跑 Q01（会连续 view_image 8 帧），确认不再出现
`InternalError.Algo.InvalidParameter`。

## 1. 缺少「没有真正执行就不得声称完成」的约束（最严重）

**现象**：E21 用例中 Agent 回答「视频库批量登记已完成，共登记 3 个视频文件」并附完整表格，
但全程只用了 `bash` + `ffprobe`，一个入库 skill 都没调用，而 Elasticsearch 本身不可用。
伪造的不是数字，而是「执行已完成」这个状态——这比编造数据更难被发现。

**已核实**：[`backend/packages/harness/deerflow/agents/lead_agent/prompt.py`](../backend/packages/harness/deerflow/agents/lead_agent/prompt.py)
的 `<skill_system>` 里有很完整的「不许用临时代码替代 skill 工作流」约束
（`Mandatory Skill Execution Discipline`、`Skill Loading Priority Rules`），
但**没有任何一条**要求「声称某项操作已完成前，必须有对应工具调用真正成功返回」。

**建议**：在 `critical_reminders` 或 `<skill_system>` 中补一条硬约束，例如：
> 只有当某个工具/skill 真正返回 `status=success` 时，才可以说该操作已完成。
> 没有成功的工具返回，就必须如实说明未执行或执行失败，禁止描述执行结果、生成汇总表格或给出完成计数。

**验证方式**：`scripts/test_video_surveillance_agent_e2e.py` 的 E21 已把
`批量登记已完成`、`登记完成` 列入 `forbid_answer`，改完直接复跑该用例。

## 2. 场景过滤器一次性注入全部 skill，没有编排入口优先级

**现象**：C-040（能力边界问题）、E12（证据契约）多轮实测都没有进入 `city-video-intelligence`，
而是直接挑了一个原子 skill 就开干。

**根因**：[`scene_skill_filter_middleware.py`](../backend/packages/harness/deerflow/agents/middlewares/scene_skill_filter_middleware.py)
的 `_scene_custom_skills()` 把命中场景的**全部** skill（现在是 23 个）平铺进 `<available_skills>`，
彼此之间没有任何优先级信号。而「必须先走 city-video-intelligence」这条规则只写在各个
SKILL.md 内部——模型是**选完 skill 之后**才会读到它，那时已经绕过编排层了。

**建议**（三选一，代价递增）：
- 在 router_card 增加 `routing_policy.is_scene_entrypoint`，注入时把入口 skill 单独提到列表最前面并加一句说明；
- 或在 `_build_scene_skills_prompt()` 里按 `routing_policy.priority` 排序，而不是当前的加载顺序；
- 或让场景过滤器只注入入口 skill，由入口 skill 的 `recommended_skill_chain` 决定后续加载。

## 3. `recursion_limit` 各处取值不一致，导致测试结论失真

**已核实的实际取值**：

| 位置 | 值 |
| --- | --- |
| [`frontend/src/core/threads/hooks.ts:348`](../frontend/src/core/threads/hooks.ts) | **1000** |
| [`backend/packages/harness/deerflow/client.py:181`](../backend/packages/harness/deerflow/client.py) | 100 |
| [`backend/app/channels/manager.py:23`](../backend/app/channels/manager.py) | 100 |
| `backend/app/gateway/routers/mobile.py:50` | 300（可用 `MOBILE_ADAPTER_RECURSION_LIMIT` 覆盖） |

**影响**：上一版测试报告 §6 得出「递归上限是首要瓶颈，建议提到 250–300」的结论，
是因为测试脚本硬编码了 100。而 Web 前端真实用的是 1000——按那条建议改反而是**降级**。
本轮已把 `run_video_surveillance_qwen35b_test.py` 的默认值改为 1000 并对齐前端。

**建议**：把默认值收敛到一处配置，避免不同入口各写各的；IM 通道的 100 是否偏紧需要单独评估。

## 4. 跨 bundle 路由泄漏

**现象**：E19 中视频库统计请求，Agent 去读了
`/mnt/skills/custom/road-traffic-analysis/SKILL.md`——非视频 bundle 的 skill。

**待查**：场景识别是否把该请求同时判给了 `road_traffic` 场景（`intent_scene_templates.json` 里
`video_surveillance` 与 `road_traffic` 互列 `confusable_with`），还是模型自己跨目录读的。
前者属场景分类问题，后者需要在提示词里约束「只加载 `<available_skills>` 中列出的 skill 文件」。

## 5. 沙箱容器不回收，端口池耗尽后上传直接 500

**现象**：连续跑完 14 个用例后，再发起新会话时上传接口返回

```
HTTP 500: Could not start sandbox container: all candidate ports are already allocated by Docker
```

**实测**：`config.yaml` 里 `sandbox.replicas: 3`，但实际累积了 **10 个** `qwen36test-deer-flow-sandbox-*`
容器且全部处于 `Up ... (healthy)`，最老的已存活一小时。手工 `docker rm -f` 清空后立即恢复正常。

**说明**：`SandboxProvider` 的 `acquire/release` 生命周期似乎没有真正回收已结束线程的容器，
容器随会话数线性增长，直到候选端口耗尽。对长时间运行的实例这是必然会踩到的问题，
而且**报错形式是上传失败**，与沙箱容量完全无关，排查时很容易误判成存储或权限问题。

**建议**：确认 `release` 是否真的销毁容器；如按线程保留是有意设计，则需要一个上限或 TTL 回收，
并把端口耗尽的错误信息改成能指向真实原因。

## 6. 同一用例的路由在不同轮次之间不稳定

**现象**：C-002 第二轮正确调用 `video-stream-ingestion`，第三轮又退回手写 `ffprobe`。

**说明**：skill 侧的 description / router_card / 编排能力都已明确声明元数据归
`video-stream-ingestion`（见上一版报告 §4.3、§4.6，本轮 A17 Q14 继续固化），
但这些只能提高正确路由的概率，无法保证每轮都遵守。属于模型行为方差，
需要在 lead_agent 的执行策略层面加约束，或接受并用多轮统计来衡量。
