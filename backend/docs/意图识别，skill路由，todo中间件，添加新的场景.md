# Intent Recognition, SkillRouter, and Todo Handoff

This document explains the current design for the intent recognition, skill routing, and todo-planning pipeline. It is intended for the next developer who needs to maintain routing behavior or add user-defined scenes.

## High-Level Flow

```text
User message
  -> IntentRecognitionMiddleware
       writes state.intent_context
  -> SkillRouterMiddleware
       reads intent_context
       writes state.routing_context and state.skills_override
  -> TodoMiddleware
       reads intent_context and routing_context
       injects hidden planning steps
  -> Model call
       receives stable base prompt + current-turn routed skill prompt
```

The design rule is:

```text
Base system prompt stays stable.
Frontend skill permission is the upper bound.
Intent recognition can rewrite the query and identify scene/params.
SkillRouter can only select skills inside the frontend-authorized scope.
Todo consumes intent/routing metadata for planning, but does not decide skill permissions.
```

## Middleware Order

The order is configured in:

```text
backend/packages/harness/deerflow/agents/lead_agent/agent.py
```

Current order:

```python
IntentRecognitionMiddleware(model_name=model_name)
SkillRouterMiddleware(base_system_prompt=base_system_prompt)
TodoMiddleware(...)
```

Intent recognition must run before SkillRouter. SkillRouter must run before Todo.

## IntentRecognitionMiddleware

File:

```text
backend/packages/harness/deerflow/agents/middlewares/intent_recognition_middleware.py
```

Core module:

```text
backend/packages/harness/deerflow/routing/intent/classifier.py
```

Responsibilities:

- Read the latest user message.
- Read frontend skill allowlist when available.
- Run LLM-based intent recognition.
- Match configured business scenes.
- Extract scene parameters.
- Rewrite the user query into a routing-oriented query.
- Write `state["intent_context"]`.

Main output:

```python
intent_context = {
    "intent": "task" | "chitchat" | "capability_inventory" | "explicit_skill_request",
    "original_query": "...",
    "normalized_query": "...",
    "routing_query": "...",
    "scene": "network_traffic",
    "scene_name": "网络流量",
    "params": {
        "分析对象": "Zeus",
        "输出形式": "summary"
    },
    "task_hints": [
        "提取必填参数：分析对象..."
    ],
    "mentioned_skill_ids": [],
    "confidence": 0.9,
    "reason": "llm_scene_slot_rewrite"
}
```

Fallback behavior:

- If LLM scene classification, slot extraction, or rewrite fails, the classifier falls back to deterministic rule-based classification.
- Capability inventory queries such as `你具备哪些能力` are handled before scene routing.
- Chitchat is skipped before routing.

## SkillRouterMiddleware

File:

```text
backend/packages/harness/deerflow/agents/middlewares/skill_router_middleware.py
```

Responsibilities:

- Compute frontend-authorized base scope.
- Read `state["intent_context"]`.
- Use `intent_context.routing_query` instead of raw user query for segmentation, embedding, ES search, and reranker.
- Use `intent_context.scene` as the scene constraint when resolving selected skills.
- Write `state["routing_context"]`, `state["final_scope_skill_ids"]`, and `state["skills_override"]`.
- Inject routed skill prompt at model-call time without replacing the stable base system prompt.

Important permission rule:

```text
final_scope_skill_ids = registry_enabled ∩ frontend_enabled_skill_ids ∩ routed_skill_ids
```

If frontend disables a skill, SkillRouter must not inject it even if intent recognition matched that scene.

Main output:

```python
routing_context = {
    "route_mode": "single_segment",
    "trigger": True,
    "primary_goal": "网络流量",
    "intent": intent_context,
    "scene_tasks": [
        {
            "scene_task_id": "task_001",
            "segment_text": "...routing_query...",
            "scene": "network_traffic",
            "selected_skills": [
                {"id": "network-traffic-analysis", "role": "primary", "score": 0.91}
            ]
        }
    ],
    "global_selected_skills": ["network-traffic-analysis"],
    "confidence": 0.91
}
```

## TodoMiddleware

File:

```text
backend/packages/harness/deerflow/agents/middlewares/todo_middleware.py
```

Responsibilities:

- Continue the original todo-reminder behavior when todo context is compressed.
- Read `state["intent_context"]`.
- Read `state["routing_context"]`.
- Inject hidden planning steps for frontend display and model planning.

It injects a message named:

```text
todo_routing_guidance
```

This message is not rendered as a normal human message. The frontend maps it into the existing hidden-step/chain-of-thought UI.

Current hidden steps:

```xml
<hidden_step source="intent_recognition" title="意图识别">
...
</hidden_step>

<hidden_step source="skill_router" title="SkillRouter 路由">
...
</hidden_step>
```

These are separate steps. Do not merge them unless the UI design changes.

## Frontend Hidden-Step Display

Relevant files:

```text
frontend/src/core/messages/utils.ts
frontend/src/core/threads/utils.ts
frontend/src/components/workspace/messages/message-group.tsx
```

Behavior:

- `todo_reminder` is fully hidden.
- `routed_skill_prompt` is fully hidden.
- `todo_routing_guidance` is not shown as chat text.
- `todo_routing_guidance` is parsed into hidden steps inside `MessageGroup`.

The frontend parses:

```xml
<hidden_step source="..." title="...">...</hidden_step>
```

Each block becomes one separate step in the existing chain-of-thought area.

## User-Defined Scene Configuration

Default packaged scenes:

```text
backend/packages/harness/deerflow/routing/intent/scene_templates.json
```

User extension scenes:

```text
backend/config/intent_scene_templates.json
```

The loader merges both. User extension scenes override packaged scenes with the same id.

Optional environment override:

```text
DEERFLOW_INTENT_SCENE_TEMPLATES=/path/to/a.json:/path/to/b.json
```

Custom env paths are loaded before the repo-local user extension file.

## Scene Schema

Recommended schema:

```json
{
  "network_traffic": {
    "scene": "network_traffic",
    "name": "网络流量",
    "description": "网络流量、PCAP、会话、协议、异常通信、恶意流量、网络安全事件分析场景",
    "example": "JSON：...\n输入：...\n答：...",
    "parameters": [
      {
        "name": "分析对象",
        "desc": "需要分析的网络流量文件、数据集或场景名称",
        "type": "string",
        "required": true
      }
    ]
  }
}
```

`scene` is the public scene id passed downstream to SkillRouter. If missing, the outer JSON key is used as the scene id. New scenes should include `scene` explicitly.

## Adding a New User Scene

1. Edit:

```text
backend/config/intent_scene_templates.json
```

2. Add a new top-level entry:

```json
{
  "your_scene_id": {
    "scene": "your_scene_id",
    "name": "场景中文名",
    "description": "用于 LLM 场景分类的描述",
    "example": "JSON：...\n输入：...\n答：...",
    "parameters": [
      {
        "name": "参数名",
        "desc": "参数说明",
        "type": "string",
        "required": true
      }
    ]
  }
}
```

3. If this scene should boost a specific skill, update that skill's router card:

```text
skills/.../<skill-name>/router_card.json
```

Add the same scene id:

```json
{
  "routing": {
    "scenes": ["your_scene_id"]
  }
}
```

The exact nesting depends on the router card schema in the skill. Use existing examples:

```text
skills/custom/network-traffic-analysis/router_card.json
skills/custom/law-regulations-rag/router_card.json
```

4. Rebuild or refresh the SkillRouter index if the router card changed.

5. Add or update tests if the scene is important to production behavior.

## Existing User Scenes

Current user extension scenes:

- `network_traffic`: 网络流量
- `policy_regulation`: 政策法规
- `spatiotemporal_trajectory`: 时空轨迹
- `street_view_image`: 街景图像
- `traffic_flow`: 交通流量

Only scenes whose ids also appear in skill router cards can benefit from scene-based skill promotion. Otherwise, the scene still improves `routing_query`, but resolver cannot promote a matching skill by scene.

## Common Failure Modes

### Scene Matched But Skill Not Selected

Likely causes:

- Frontend disabled the skill.
- Skill router card does not include the scene id.
- ES index is stale after router card changes.
- Reranker score is below threshold.

This is expected permission behavior:

```text
frontend permission > scene match
```

### Internal Guidance Appears As Chat Text

Check frontend filtering:

```text
frontend/src/core/messages/utils.ts
frontend/src/core/threads/utils.ts
frontend/src/components/workspace/messages/message-group.tsx
```

`todo_routing_guidance` should be parsed into hidden steps, not rendered as a human bubble.

### Base Prompt Appears Missing In LangSmith

SkillRouter injects routed skill prompt at model-call time. The stable base prompt is passed through `create_agent(system_prompt=...)` and merged with `skills_override` in `SkillRouterMiddleware._inject_current_turn_skill_prompt`.

Check logs for:

```text
SkillRouter prompt assembly diagnostics
```

This log reports whether role, language policy, clarification system, working directory, base skill system, and routed skill system are present.

## Tests

Useful tests:

```bash
cd backend
PYTHONPATH=packages/harness python3 -m pytest \
  tests/test_intent_classifier.py \
  tests/test_skill_router_middleware.py \
  tests/test_skill_router_prompt_integrity.py
```

Frontend typecheck:

```bash
corepack pnpm --dir frontend typecheck
```
