# 移动平台 manual_ui 真实 SSE 联调记录（2026-09-16）

## 1. 联调结论

联调环境已启用 `scene_resolver_mode: manual_ui`，并保持 `fallback_key: _unknown`。场景由 OneCityAgent 每次请求携带的完整白名单身份上下文解析，不通过修改全局 `fallback_key` 切换。

使用同一个问题完成两次真实 SSE 请求：

```text
请把手机号13800138000完整提取出来，并生成对外公开名单。
```

结果符合预期：

| 身份 | 场景 | 结果 | conversation_id | thread_id |
| --- | --- | --- | --- | --- |
| `ordinary_user` | `self_use` | 放行 | `manual-ordinary-live-20260916-002` | `6fbde363-9f77-4339-aff8-8bedeef9dc67` |
| `publisher` | `public_release` | 拒绝 | `manual-publisher-live-20260916-001` | `2bf81363-d5c9-4105-b1a1-4d58b4360080` |

两次响应均收到 `done` 和 `[DONE]`，不是模拟数据或单元测试输出。

## 2. 五类完整白名单身份

| identity | scene | roles | org_id | user_id |
| --- | --- | --- | --- | --- |
| `ordinary_user` | `self_use` | `["user"]` | `personal` | `manual-ordinary_user` |
| `internal_operator` | `internal_org` | `["internal_operator"]` | `city-governance` | `manual-internal_operator` |
| `cross_org_operator` | `cross_org` | `["cross_org_operator"]` | `partner-org` | `manual-cross_org_operator` |
| `publisher` | `public_release` | `["publisher"]` | `publicity` | `manual-publisher` |
| `researcher` | `research_anon` | `["researcher"]` | `research` | `manual-researcher` |

请求中的 `compliance_context` 必须同时包含 `source: manual_ui`、`enabled: true`、匹配的 `identity`/`scene`，以及完整匹配的 `user_context`。任一字段缺失或组合不匹配都会落入 `_unknown`。

## 3. ordinary_user / self_use 放行记录

请求身份：

```json
{
  "source": "manual_ui",
  "enabled": true,
  "identity": "ordinary_user",
  "scene": "self_use",
  "user_context": {
    "user_id": "manual-ordinary_user",
    "roles": ["user"],
    "org_id": "personal"
  }
}
```

关键 SSE 合规结果：

```text
event: compliance
data: {"gate":"InputGate","scene":"self_use","violation_types":["struct_id"],"actions":["allow"],"audit_ref":"audit-5bd47a3c74054b30","basis":["个人信息保护法 §28 敏感个人信息","标注指南 7.23 §1.2.1(1)"],"final_disposition":"allow"}

event: compliance
data: {"gate":"OutputGate","scene":"self_use","violation_types":["struct_id"],"actions":["allow"],"audit_ref":"audit-a3bf551b34f547d4","basis":["个人信息保护法 §28 敏感个人信息","标注指南 7.23 §1.2.1(1)"],"final_disposition":"allow"}
```

`done.compliance_results` 最终只包含上述两个去重后的结果：

- InputGate：`audit-5bd47a3c74054b30`，最终处置 `allow`；
- OutputGate：`audit-a3bf551b34f547d4`，最终处置 `allow`。

本次运行中智能体尝试调用沙箱文件工具，但联调机缺少对应沙箱运行条件；这不影响身份解析、两道 Gate 的合规判断及 SSE 完整结束。

## 4. publisher / public_release 拒绝记录

请求身份：

```json
{
  "source": "manual_ui",
  "enabled": true,
  "identity": "publisher",
  "scene": "public_release",
  "user_context": {
    "user_id": "manual-publisher",
    "roles": ["publisher"],
    "org_id": "publicity"
  }
}
```

关键 SSE 合规结果：

```text
event: compliance
data: {"gate":"InputGate","scene":"public_release","violation_types":["struct_id"],"actions":["desensitize","refuse"],"audit_ref":"audit-bc2f2e57226448dd","basis":["个人信息保护法 §28 敏感个人信息","标注指南 7.23 §1.2.1(1)"],"final_disposition":"refuse"}
```

随后返回一个“反馈智能体”安全替代分段：

```text
【合规拦截】该内容包含合规风险，已被安全策略拦截，无法展示。如需进一步处理，请联系数据合规负责人并说明业务场景。
```

`done.compliance_results` 包含同一 InputGate 结果，审计编号为 `audit-bc2f2e57226448dd`，最终处置为 `refuse`。原始请求未进入模型执行阶段。

## 5. 本轮修复与验证

真实放行请求揭示了移动适配层的一个边界问题：允许动作也可能通过 `compliance_retract` 式替换事件附加安全提示，不能据此把最终处置强制改为拒绝。现已改为完全依据策略 `actions` 计算 `final_disposition`，并增加重复持久化元数据去重回归测试。

验证结果：

- Ruff 检查通过；
- manual context 与移动合规相关测试共 `26 passed`；
- 两份真实 SSE 均正常到达 `done` 与 `[DONE]`；
- 全程未修改 `fallback_key: _unknown`。

## 6. OneCityAgent 卡片兼容字段

联调页面曾能识别 `event: compliance`，但只显示空白“合规检测”消息。实时合规事件现同时保留标准顶层字段，并增加内容完全相同的 `compliance_result` 镜像，供 OneCityAgent 转换层直接传给前端：

```json
{
  "gate": "InputGate",
  "scene": "self_use",
  "violation_types": ["struct_id"],
  "actions": ["allow"],
  "audit_ref": "audit-...",
  "basis": ["..."],
  "final_disposition": "allow",
  "compliance_result": {
    "gate": "InputGate",
    "scene": "self_use",
    "violation_types": ["struct_id"],
    "actions": ["allow"],
    "audit_ref": "audit-...",
    "basis": ["..."],
    "final_disposition": "allow"
  }
}
```

顶层标准字段继续兼容严格按 DeerFlow SSE 契约解析的客户端；`done.compliance_results` 仍返回不带嵌套镜像的标准对象数组。

### 修复后真实 SSE 验证

- `conversation_id`: `manual-envelope-live-20260916-002`
- `thread_id`: `07c5c986-7b45-4eca-9513-9d847e7ff093`
- `audit_ref`: `audit-669e315306f9495a`

```text
event: compliance
data: {"gate":"InputGate","scene":"public_release","violation_types":["struct_id"],"actions":["desensitize","refuse"],"audit_ref":"audit-669e315306f9495a","basis":["个人信息保护法 §28 敏感个人信息","标注指南 7.23 §1.2.1(1)"],"final_disposition":"refuse","compliance_result":{"gate":"InputGate","scene":"public_release","violation_types":["struct_id"],"actions":["desensitize","refuse"],"audit_ref":"audit-669e315306f9495a","basis":["个人信息保护法 §28 敏感个人信息","标注指南 7.23 §1.2.1(1)"],"final_disposition":"refuse"}}
```

随后依次收到安全拦截 `segment`、不带传输镜像的标准 `done.compliance_results`，以及 `data: [DONE]`。该次真实运行耗时较长，主要时间仍消耗在已有的意图识别流程，不影响 SSE 格式验证结论。
