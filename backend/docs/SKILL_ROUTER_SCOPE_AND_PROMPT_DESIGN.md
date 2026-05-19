# SkillRouter 与前端 Skill 开关协同改造方案

## 1. 背景问题

当前实现存在两个核心问题：

```text
1. 全量 skill 仍然被注入 system prompt。
2. skills_override 只是额外追加 SystemMessage，并没有替换原有全量 skill。
```

结果是：

```text
Skill 数量增长时，system prompt 仍然线性膨胀；
SkillRouter 虽然能命中相关 skill，但没有真正节省上下文；
模型同时看到全量 skill 和命中 skill，容易产生干扰；
前端关闭 skill 后，如果 SkillRouter 直接从全量 skill 中检索，可能绕过前端开关。
```

本次改造目标：

```text
前端 skill 开关始终可控；
SkillRouter 开启后 system prompt 不再随 skill 总数线性膨胀；
SkillRouter 只能在前端允许的 skill 范围内筛选；
工具调用层必须防止越权调用；
历史上下文不能污染当前轮 skill 可用性。
```

---

## 2. 总体设计原则

### 2.1 前端 Skill 开关是硬约束

前端开关决定当前用户或当前会话允许使用哪些 skill。

```text
前端关闭的 skill：
- 不进入 prompt；
- 不进入 SkillRouter 候选集；
- 不允许被工具执行层调用。
```

### 2.2 SkillRouter 只是二次筛选器

SkillRouter 不决定全局可用性，只在前端允许的 skill 集合里选择本轮最相关的 skill。

正确关系：

```text
final_skills = SkillRouter(frontend_allowed_skills)
```

错误关系：

```text
final_skills = SkillRouter(all_enabled_skills)
```

### 2.3 开启 SkillRouter 后不得全量注入 skill

开启 SkillRouter 后：

```text
base system prompt 只包含通用规则；
不包含全量 skill；
不包含所有 enabled skill；
不包含前端开启的全部 skill；
只在本轮动态注入 SkillRouter 命中的 top-k skills。
```

---

## 3. 最终 Skill 计算链路

推荐统一成下面的链路：

```text
系统注册的全部 skills
        ↓
registry_enabled=true 的 skills
        ↓
前端当前开启的 skills
        ↓
SkillRouter 是否开启？
        ↓
关闭：使用前端开启的全部 skills
开启：在前端开启的 skills 中检索 top-k
        ↓
final_skills
        ↓
prompt 注入 + 工具调用白名单
```

对应公式：

```text
SkillRouter 关闭：
final_skills = registry_enabled_skills ∩ frontend_enabled_skills

SkillRouter 开启：
final_skills = SkillRouter(
    registry_enabled_skills ∩ frontend_enabled_skills
)
```

---

## 4. 如何保证开启 SkillRouter 后 system prompt 不膨胀

这是本次改造的重点。

### 4.1 system prompt 分层

需要把 system prompt 拆成两层：

```text
Layer 1：Base System Prompt
- 通用角色
- 工具调用规则
- fail-closed 规则
- 当前轮 skill 可用性规则
- 输出格式规则
- 安全边界

Layer 2：Routed Skills Prompt
- 仅包含本轮 SkillRouter 命中的 top-k skills
- 包含 skill 名称、描述、使用条件、输入输出、allowed-tools
```

开启 SkillRouter 后，基础 prompt 应该是稳定长度，不随 skill 数量增加而增长。

```text
base_system_prompt_size = O(1)
routed_skill_prompt_size = O(k)
```

其中 `k` 是 top-k 命中数量，通常固定为 3、5 或 8。

不能变成：

```text
system_prompt_size = O(total_skills)
```

### 4.2 apply_prompt_template 不允许自动回退全量 skill

当前问题大概率来自类似逻辑：

```python
apply_prompt_template()
```

没有传 `available_skills` 时，内部默认加载所有 enabled skill。

这会导致 SkillRouter 开启时，即使 Middleware 后面注入了命中 skill，前面的 system prompt 仍然已经包含全量 skill。

必须改成：

```python
def apply_prompt_template(
    *,
    base_context,
    prompt_skills: list[Skill],
):
    return render_prompt(
        base_context=base_context,
        skills=prompt_skills,
    )
```

模板函数只负责渲染，不负责加载 skill。

也就是说：

```text
给它哪些 skill，它就渲染哪些 skill；
不给 skill，它就不渲染 skill；
绝不能在函数内部偷偷加载全量 skill。
```

如果短期需要兼容旧逻辑，至少要严格区分：

```python
if available_skills is None:
    available_skills = load_all_enabled_skills()
elif available_skills == []:
    available_skills = []
```

禁止写成：

```python
if not available_skills:
    available_skills = load_all_enabled_skills()
```

因为 `[]` 会被误判为 false，导致重新加载全量 skill。

### 4.3 SkillRouter 开启时必须显式传空 skill

开启 SkillRouter 时，不要让 `apply_prompt_template()` 自己决定 skill。

应明确传入：

```python
base_system_prompt = apply_prompt_template(
    base_context=base_context,
    prompt_skills=[],
)
```

然后由 `SkillRouterMiddleware` 或消息构造层注入本轮命中的 skill：

```python
routed_skill_prompt = build_skill_system_message(final_skills)
```

最终消息结构：

```python
messages = [
    base_system_message,          # 不包含具体 skill
    routed_skill_system_message,  # 只包含本轮命中 skill
    *history_messages,
    user_message,
]
```

### 4.4 不允许把动态 skill prompt 写入长期历史

这是另一个容易导致 prompt 膨胀的地方。

错误做法：

```text
每轮把 routed skill SystemMessage 追加进 conversation history。
```

这样多轮之后会变成：

```text
第 1 轮 skill A、B
第 2 轮 skill C、D
第 3 轮 skill E、F
...
历史里累计越来越多 skill
```

正确做法：

```text
每一轮重新构造当前轮 system messages；
routed skill message 只参与当前轮模型调用；
不持久化进普通对话历史；
不进入 thread state 的 messages 历史，或者在持久化前剔除。
```

推荐规则：

```text
Base SystemMessage：每轮动态构造，不写入长期 history。
Routed Skill SystemMessage：每轮动态构造，不写入长期 history。
User / Assistant / Tool messages：按需要写入 history。
```

如果当前框架必须保存 SystemMessage，也要在读取历史时过滤旧的 skill system messages：

```python
history_messages = [
    msg for msg in history_messages
    if msg.metadata.get("message_type") != "routed_skill_prompt"
]
```

### 4.5 当前轮 SystemMessage 必须覆盖历史 skill

由于历史消息中可能残留旧 skill 描述，base prompt 里要明确写：

```text
只能使用当前轮 “Available Skills” 部分列出的 skills。
历史对话中出现过但当前未列出的 skill，视为不可用。
不得根据历史中出现过的 skill 名称调用未授权工具。
```

英文版：

```text
Only skills listed in the current "Available Skills" section are available.
Ignore any skills mentioned in previous turns if they are not listed here.
Do not call tools associated with unavailable skills.
```

### 4.6 设置 top-k 上限和 token budget

SkillRouter 需要有固定上限。

建议配置：

```yaml
skill_router:
  enabled: true
  top_k: 5
  max_skill_prompt_tokens: 3000
  strict_mode: true
```

要求：

```text
最多注入 top_k 个 skill；
如果 skill 内容过长，需要摘要化或截断；
总 skill prompt token 不得超过 max_skill_prompt_tokens；
超过预算时按 router score 从高到低保留。
```

示例：

```python
routed_skills = router.route(
    query=query,
    candidate_skills=base_scope,
    top_k=config.skill_router.top_k,
)

routed_skills = trim_skills_by_token_budget(
    skills=routed_skills,
    max_tokens=config.skill_router.max_skill_prompt_tokens,
)
```

这样即使单个 skill 很长，也不会导致 system prompt 不受控增长。

### 4.7 Prompt 膨胀检测

建议在每轮调用前记录 prompt 体积：

```json
{
  "skill_router_enabled": true,
  "total_registry_skills": 120,
  "base_scope_skill_count": 40,
  "routed_skill_count": 5,
  "base_system_prompt_tokens": 1200,
  "routed_skill_prompt_tokens": 2600,
  "total_system_prompt_tokens": 3800
}
```

加硬性断言：

```python
if skill_router_enabled:
    assert total_system_prompt_tokens <= BASE_PROMPT_BUDGET + ROUTED_SKILL_BUDGET
    assert routed_skill_count <= top_k
```

回归测试中要验证：

```text
当总 skill 从 10 增加到 100、1000 时，
SkillRouter 开启状态下 system prompt token 数基本保持稳定，
只随 top_k 和单个 skill 长度变化，不随总 skill 数线性增长。
```

---

## 5. 后端改造方案

### 5.1 新增 SkillScopeResolver

新增统一模块：

```text
SkillScopeResolver
```

负责计算：

```text
base_scope：前端允许的 skill 集合；
final_scope：本轮真正可用的 skill 集合。
```

示例：

```python
class SkillScopeResolver:
    def __init__(self, skill_registry, skill_router):
        self.skill_registry = skill_registry
        self.skill_router = skill_router

    def resolve_base_scope(
        self,
        *,
        frontend_enabled_skill_ids: list[str] | None,
    ) -> list[Skill]:
        registry_enabled_skills = self.skill_registry.get_enabled_skills()

        # None 表示前端没有传选择结果，使用后端默认 enabled skills
        if frontend_enabled_skill_ids is None:
            return registry_enabled_skills

        # [] 表示用户明确关闭所有 skill
        frontend_enabled_set = set(frontend_enabled_skill_ids)

        return [
            skill
            for skill in registry_enabled_skills
            if skill.id in frontend_enabled_set
        ]

    def resolve_final_scope(
        self,
        *,
        query: str,
        skill_router_enabled: bool,
        base_scope: list[Skill],
        top_k: int,
    ) -> list[Skill]:
        if not skill_router_enabled:
            return base_scope

        if not base_scope:
            return []

        routed_skills = self.skill_router.route(
            query=query,
            candidate_skills=base_scope,
            top_k=top_k,
        )

        # 二次保险：防止 router 返回 base_scope 之外的 skill
        base_ids = {skill.id for skill in base_scope}
        return [
            skill
            for skill in routed_skills
            if skill.id in base_ids
        ]
```

### 5.2 修改消息构造逻辑

#### SkillRouter 关闭

```python
base_scope = resolver.resolve_base_scope(
    frontend_enabled_skill_ids=request.enabled_skill_ids,
)

final_skills = resolver.resolve_final_scope(
    query=request.query,
    skill_router_enabled=False,
    base_scope=base_scope,
    top_k=config.skill_router.top_k,
)

messages = [
    build_base_system_message(),
    build_skill_system_message(final_skills),
    *history_messages,
    user_message,
]
```

此时：

```text
prompt 注入前端开启的全部 skill。
```

#### SkillRouter 开启

```python
base_scope = resolver.resolve_base_scope(
    frontend_enabled_skill_ids=request.enabled_skill_ids,
)

final_skills = resolver.resolve_final_scope(
    query=request.query,
    skill_router_enabled=True,
    base_scope=base_scope,
    top_k=config.skill_router.top_k,
)

base_system_prompt = apply_prompt_template(
    base_context=base_context,
    prompt_skills=[],
)

messages = [
    SystemMessage(
        content=base_system_prompt,
        metadata={"message_type": "base_system_prompt"},
    ),
    SystemMessage(
        content=build_skill_system_message(final_skills),
        metadata={"message_type": "routed_skill_prompt"},
    ),
    *filter_system_skill_messages(history_messages),
    user_message,
]
```

此时：

```text
base system prompt 不包含全量 skill；
routed skill prompt 只包含本轮 top-k skill；
历史里的旧 routed skill prompt 被过滤。
```

### 5.3 修改 SkillRouterMiddleware

错误做法：

```python
candidate_skills = skill_registry.get_enabled_skills()
routed_skills = router.route(query, candidate_skills)
```

正确做法：

```python
candidate_skills = request.context.base_skill_scope

routed_skills = router.route(
    query=query,
    candidate_skills=candidate_skills,
    top_k=top_k,
)
```

如果 Middleware 当前拿不到 `base_scope`，需要在 request context 中加入：

```python
request.context.skill_scope = {
    "skill_router_enabled": request.skill_router_enabled,
    "base_skill_ids": [skill.id for skill in base_scope],
    "final_skill_ids": [skill.id for skill in final_skills],
}
```

### 5.4 工具执行层加白名单

工具白名单应来自本轮 `final_skills` 的 `allowed-tools`。

示例：

```python
allowed_tool_names = get_tool_names_from_skills(final_skills)

if tool_name not in allowed_tool_names:
    raise ToolNotAllowedError(
        f"Tool {tool_name} is not allowed in current skill scope"
    )
```

规则：

```text
SkillRouter 关闭：
allowed_tools = 前端开启 skills 对应 tools

SkillRouter 开启：
allowed_tools = SkillRouter 命中 skills 对应 tools
```

---

## 6. 前端请求协议

前端请求建议增加或明确两个字段：

```json
{
  "skill_router_enabled": true,
  "enabled_skill_ids": [
    "pcap_summary",
    "netflow_anomaly",
    "protocol_identify"
  ],
  "query": "帮我分析这个 pcap 是否存在异常通信"
}
```

语义必须明确：

```text
enabled_skill_ids = null
表示使用后端默认 enabled skills

enabled_skill_ids = []
表示用户明确关闭所有 skill

enabled_skill_ids = ["a", "b"]
表示当前会话只允许 a、b
```

必须区分：

```text
null ≠ []
```

---

## 7. 边界条件

### 7.1 enabled_skill_ids = null

行为：

```text
使用后端 registry_enabled=true 的 skills。
```

SkillRouter 关闭：

```text
注入后端默认 enabled skills。
```

SkillRouter 开启：

```text
在后端默认 enabled skills 中路由 top-k。
```

### 7.2 enabled_skill_ids = []

行为：

```text
用户明确关闭所有 skill。
```

结果：

```text
base_scope = []
final_skills = []
prompt 不注入任何 skill
allowed_tools = []
```

模型应该返回：

```text
当前没有可用 skill，无法执行该任务。
```

不能自动回退到全量 skill。

### 7.3 前端传不存在的 skill_id

推荐行为：

```text
忽略不存在的 skill_id；
记录 warning 日志；
只使用存在且 registry_enabled=true 的 skill。
```

管理端配置接口可以更严格，直接返回 400。

### 7.4 前端开启但系统禁用

如果：

```text
frontend_enabled = true
registry_enabled = false
```

最终：

```text
不能使用。
```

系统级禁用优先级高于前端选择。

### 7.5 SkillRouter 返回越权 skill

正常情况下不应该发生，因为候选集已经被前端过滤。

但仍要做二次过滤：

```python
routed_skills = [
    skill
    for skill in routed_skills
    if skill.id in base_scope_ids
]
```

如果发生越权返回，记录 error：

```text
SkillRouter returned skill outside base scope
```

### 7.6 SkillRouter 无命中

推荐使用 strict mode：

```text
strict_mode = true
```

行为：

```text
不注入 skill；
不允许调用 tool；
返回 fail-closed 结果。
```

不建议自动回退到 base_scope，因为这会削弱 SkillRouter 减少上下文的目标。

### 7.7 会话中途切换 skill 开关

每轮都必须重新计算：

```text
base_scope
final_skills
allowed_tools
system messages
```

不能复用上一轮结果。

### 7.8 历史消息里有旧 skill 描述

处理规则：

```text
旧 skill system messages 不进入当前轮 history；
或者在构造 messages 时过滤掉；
当前轮 base prompt 明确要求忽略历史中的旧 skill。
```

---

## 8. 防止 Prompt 膨胀的测试检查

### 8.1 SkillRouter 开启时不注入全量 skill

输入：

```json
{
  "skill_router_enabled": true,
  "enabled_skill_ids": ["A", "B", "C"],
  "query": "适合 A 的问题"
}
```

路由结果：

```text
A
```

预期：

```text
base system prompt 不包含 A、B、C 的完整 skill 内容；
routed skill prompt 只包含 A；
prompt 中不出现未命中的 B、C；
prompt 中不出现前端关闭的 D。
```

### 8.2 skill 总数增长时 prompt 不线性增长

构造测试：

```text
注册 10 个 skill
注册 100 个 skill
注册 1000 个 skill
```

SkillRouter 开启，top_k=5。

预期：

```text
system prompt token 数基本稳定；
不会随着总 skill 数线性增长；
只与 base prompt、top_k、单个 skill 长度有关。
```

断言示例：

```python
tokens_10 = count_system_prompt_tokens(total_skills=10, router_enabled=True)
tokens_100 = count_system_prompt_tokens(total_skills=100, router_enabled=True)
tokens_1000 = count_system_prompt_tokens(total_skills=1000, router_enabled=True)

assert tokens_100 < tokens_10 * 1.5
assert tokens_1000 < tokens_10 * 1.5
```

更严格可以断言：

```python
assert routed_skill_count <= top_k
assert total_system_prompt_tokens <= BASE_PROMPT_BUDGET + ROUTED_SKILL_BUDGET
```

### 8.3 apply_prompt_template 空列表测试

输入：

```python
prompt = apply_prompt_template(
    base_context=base_context,
    prompt_skills=[],
)
```

预期：

```text
不会自动加载全部 skill。
```

断言：

```python
assert "skill A" not in prompt
assert "skill B" not in prompt
assert "Available Skills" not in prompt or available_skills_section_is_empty(prompt)
```

这个测试专门防止错误逻辑：

```python
if not available_skills:
    available_skills = load_all_enabled_skills()
```

### 8.4 动态 skill prompt 不进入历史

第一轮路由命中：

```text
A、B
```

第二轮路由命中：

```text
C
```

预期第二轮 messages：

```text
只包含当前轮 C 的 routed skill prompt；
不包含第一轮 A、B 的 routed skill prompt。
```

断言：

```python
assert "skill C" in current_messages
assert "skill A" not in current_messages
assert "skill B" not in current_messages
```

### 8.5 history 过滤测试

输入历史中包含：

```text
metadata.message_type = routed_skill_prompt
```

构造当前轮 messages 时：

```python
filtered_history = filter_system_skill_messages(history)
```

预期：

```python
assert all(
    msg.metadata.get("message_type") != "routed_skill_prompt"
    for msg in filtered_history
)
```

---

## 9. 前端开关防冲突测试

### 9.1 SkillRouter 关闭 + 前端开启部分 skill

输入：

```json
{
  "skill_router_enabled": false,
  "enabled_skill_ids": ["A", "B"]
}
```

预期：

```text
prompt 中只出现 A、B；
prompt 中不出现 C、D；
allowed_tools 只包含 A、B 对应工具；
调用 C 对应工具会被拒绝。
```

### 9.2 SkillRouter 开启 + 前端开启部分 skill

输入：

```json
{
  "skill_router_enabled": true,
  "enabled_skill_ids": ["A", "B", "C"],
  "query": "适合 A 的问题"
}
```

路由结果：

```text
A
```

预期：

```text
prompt 只注入 A；
allowed_tools 只包含 A 对应工具；
B、C 虽然前端开启，但本轮没命中，也不能调用；
D 前端关闭，更不能调用。
```

### 9.3 SkillRouter 不得绕过前端开关

输入：

```json
{
  "skill_router_enabled": true,
  "enabled_skill_ids": ["A"],
  "query": "明显适合 D 的问题"
}
```

其中 D 是系统存在但前端关闭的 skill。

预期：

```text
SkillRouter 不得返回 D；
final_skills 要么是 A，要么为空；
prompt 中不出现 D；
allowed_tools 不包含 D 的工具。
```

这是最关键的冲突测试。

### 9.4 enabled_skill_ids = []

输入：

```json
{
  "enabled_skill_ids": []
}
```

预期：

```text
base_scope = []
final_skills = []
prompt 中没有 skill
allowed_tools = []
不会回退全量 skill
```

### 9.5 工具白名单测试

当前：

```text
final_skills = ["A"]
```

预期：

```python
execute_tool("tool_a")  # success
execute_tool("tool_b")  # raise ToolNotAllowedError
```

这是最终安全边界。

---

## 10. 日志与可观测性

每轮请求建议记录：

```json
{
  "skill_router_enabled": true,
  "total_registry_skill_count": 120,
  "frontend_enabled_skill_ids": ["A", "B", "C"],
  "base_scope_skill_ids": ["A", "B", "C"],
  "routed_skill_ids": ["B"],
  "final_skill_ids": ["B"],
  "allowed_tool_names": ["tool_b1", "tool_b2"],
  "base_system_prompt_tokens": 1200,
  "routed_skill_prompt_tokens": 2600,
  "total_system_prompt_tokens": 3800
}
```

重点观察：

```text
SkillRouter 开启后 total_system_prompt_tokens 是否稳定；
routed_skill_count 是否超过 top_k；
是否有 router 返回 base_scope 外 skill；
是否有工具调用被白名单拒绝；
enabled_skill_ids=[] 时是否错误回退全量 skill。
```

---

## 11. 推荐实施步骤

### 第一步：修改 apply_prompt_template

目标：

```text
模板函数不再自动加载全量 skill；
prompt_skills=[] 时必须保持空 skill。
```

必须补测试：

```text
apply_prompt_template(prompt_skills=[]) 不出现任何 skill。
```

### 第二步：新增 SkillScopeResolver

目标：

```text
统一计算 base_scope 和 final_scope。
```

避免前端过滤、router 过滤、工具过滤逻辑分散。

### 第三步：改造 SkillRouterMiddleware

目标：

```text
SkillRouter 只在 base_scope 中检索；
禁止直接读取 all_enabled_skills 作为候选集。
```

### 第四步：改造消息构造

目标：

```text
SkillRouter 关闭：注入前端开启的全部 skill；
SkillRouter 开启：base prompt 不含 skill，只动态注入 top-k skill；
动态 skill SystemMessage 不写入长期 history。
```

### 第五步：工具执行层加白名单

目标：

```text
final_skills 之外的工具不能调用。
```

这是比 prompt 更可靠的硬边界。

### 第六步：增加 prompt token 监控和回归测试

目标：

```text
证明 SkillRouter 开启后 system prompt 不随 skill 总数线性增长。
```

---

## 12. 最终结论

最终方案可以概括为：

```text
前端 skill 开关始终作为 hard filter，决定当前用户/会话允许使用的 skill 范围。
SkillRouter 只在该范围内进行 top-k 筛选。
SkillRouter 关闭时，prompt 注入前端开启的全部 skill；
SkillRouter 开启时，base system prompt 显式传空 skill，不注入全量 skill，只在当前轮动态注入命中的 top-k skill。
动态 skill prompt 不写入长期历史，每轮重新构造。
工具执行层始终基于 final_skills 生成白名单，拒绝调用未授权 skill 对应工具。
```

核心保证：

```text
SkillRouter 开启后：
system_prompt_size = base_prompt_size + top_k_skill_prompt_size
```

而不是：

```text
system_prompt_size = base_prompt_size + all_enabled_skills_prompt_size
```

这样才能同时解决：

```text
system prompt 膨胀；
前端 skill 开关失效；
SkillRouter 绕过前端控制；
历史 skill prompt 累积；
工具调用越权。
```
