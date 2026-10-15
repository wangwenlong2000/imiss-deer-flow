# OneCityAgent–DeerFlow 合规评测联调接口需求

## 1. 文档目的

本文用于明确 DeerFlow 合规评测结果传输到 OneCityAgent 城市超脑页面时的请求格式、SSE 返回格式和验收标准。

当前 OneCityAgent 已完成：

- 城市超脑页面五类身份选择；
- 身份与合规场景固定绑定；
- 向 DeerFlow 3328 端口转发身份和场景上下文；
- 接收并展示结构化合规结果卡片；
- 接收 `done.compliance_results` 作为漏事件兜底；
- OutputGate 拒绝时撤下可能提前展示的最终回答；
- 展示 DeerFlow 返回的安全拦截替代内容。

目前联调页面出现了标题为“合规检测”、内容为空的消息框。这说明合规链路已经被触发，但 OneCityAgent 没有收到前端可识别的结构化 `compliance_result`。

## 2. 当前问题说明

当前异常表现：

```text
合规检测
[空白内容]
```

该表现不能作为合规评测验收通过的依据。正确的合规卡片至少应显示：

- 检查闸门；
- 权限场景；
- 风险类型；
- 处置动作；
- 审计编号；
- 合规依据。

请勿只向 OneCityAgent 返回下面这种普通空消息：

```json
{
  "role": "合规检测",
  "content": ""
}
```

合规结果必须使用本文第 5 节定义的结构化 SSE 事件。

## 3. 身份与场景固定映射

OneCityAgent 页面使用 DeerFlow 已定义的五个测试身份。身份与场景不可由前端自由组合。

| 页面身份 | `identity` | `scene` | `roles` | `org_id` |
|---|---|---|---|---|
| 个人自查用户 | `ordinary_user` | `self_use` | `user` | `personal` |
| 同组织内部工作人员 | `internal_operator` | `internal_org` | `internal_operator` | `city-governance` |
| 已授权跨组织协作人员 | `cross_org_operator` | `cross_org` | `cross_org_operator` | `partner-org` |
| 公开发布操作人员 | `publisher` | `public_release` | `publisher` | `publicity` |
| 科研人员（匿名化数据） | `researcher` | `research_anon` | `researcher` | `research` |

该设计表示五类业务用途，不表示简单的权限从低到高排序。

## 4. OneCityAgent 发给 DeerFlow 的请求格式

接口：

```text
POST /api/deerflow/mobile/run
Content-Type: application/json
```

以“个人自查用户”为例，OneCityAgent 发送：

```json
{
  "query": "用户问题",
  "conversation_id": "citybrain-xxx",
  "user_id": "onecity-user-id",
  "thread_id": "DeerFlow LangGraph UUID",
  "stream": true,
  "compliance_identity": {
    "user_id": "manual-ordinary_user",
    "roles": ["user"],
    "org_id": "personal"
  },
  "compliance_context": {
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
}
```

DeerFlow 需要保证：

1. `compliance_context` 被传入 LangGraph `run_context`；
2. `scene_resolver_mode` 为 `manual_ui`；
3. 使用服务端白名单重新校验身份、场景、角色和组织信息；
4. 最终 Gate 结果中的 `scene` 必须是解析后的真实场景；
5. 正常情况下不能降级成 `_unknown`。

## 5. DeerFlow 返回合规结果的 SSE 契约

### 5.1 独立合规事件

每产生一个 InputGate、ContextGate 或 OutputGate 检查结果，都应立即发送一个独立的命名 SSE 事件：

```text
event: compliance
data: {"gate":"InputGate","scene":"self_use","violation_types":["struct_id","geo_loc"],"actions":["allow"],"audit_ref":"audit-123456","basis":["个人信息保护法 §28 敏感个人信息","标注指南 7.23 §1.2.1(1)","标注指南 7.23 §1.2.1(2)"],"final_disposition":"allow"}

```

注意：

- `event` 必须严格为小写的 `compliance`；
- `data` 必须直接是合规结果对象；
- 每个 SSE 事件末尾必须有空行；
- 不要将合规结果伪装成普通 `segment`；
- 不要只返回 `role=合规检测` 和空 `content`。

### 5.2 合规结果字段定义

```json
{
  "gate": "InputGate",
  "scene": "self_use",
  "violation_types": ["struct_id", "geo_loc"],
  "actions": ["allow"],
  "audit_ref": "audit-123456",
  "basis": [
    "个人信息保护法 §28 敏感个人信息",
    "标注指南 7.23 §1.2.1(1)",
    "标注指南 7.23 §1.2.1(2)"
  ],
  "final_disposition": "allow"
}
```

字段约束：

| 字段 | 类型 | 必填 | 约束 |
|---|---|---|---|
| `gate` | string | 是 | `InputGate`、`ContextGate`、`OutputGate` |
| `scene` | string | 是 | `self_use`、`internal_org`、`cross_org`、`public_release`、`research_anon`；仅异常降级时允许 `_unknown` |
| `violation_types` | string[] | 是 | 无风险时返回空数组 `[]` |
| `actions` | string[] | 是 | 见下方动作枚举 |
| `audit_ref` | string | 是 | 必须能关联审计日志 |
| `basis` | string[] | 是 | 无依据时返回空数组 `[]` |
| `final_disposition` | string | 是 | 本次检查的最终处置 |

动作枚举：

```text
allow
warn
role_check
desensitize
rewrite
aggregate
manual_review
refuse
```

### 5.3 正常放行示例

```text
event: compliance
data: {"gate":"InputGate","scene":"self_use","violation_types":["struct_id","geo_loc"],"actions":["allow"],"audit_ref":"audit-input-001","basis":["个人信息保护法 §28 敏感个人信息","标注指南 7.23 §1.2.1(1)","标注指南 7.23 §1.2.1(2)"],"final_disposition":"allow"}

event: segment
data: {"order":1,"role":"反馈智能体","content":"正常业务回答内容","status":"completed","metadata":{"source":"deerflow","source_event":"final_answer"}}

```

### 5.4 拒绝或撤回示例

OutputGate 判定拒绝时，需要发送合规结果以及安全替代内容：

```text
event: compliance
data: {"gate":"OutputGate","scene":"public_release","violation_types":["struct_id","geo_loc"],"actions":["desensitize","aggregate","refuse"],"audit_ref":"audit-output-001","basis":["个人信息保护法 §28 敏感个人信息","标注指南 7.23 §1.2.1(1)","标注指南 7.23 §1.2.1(2)"],"final_disposition":"refuse"}

event: segment
data: {"order":4,"role":"反馈智能体","content":"【合规拦截】该内容包含合规风险，已被安全策略拦截，无法展示。如需进一步处理，请联系数据合规负责人并说明业务场景。","status":"completed","metadata":{"source":"deerflow","source_event":"compliance_retract","scene":"public_release","violation_types":["struct_id","geo_loc"],"actions":["refuse"],"audit_ref":"audit-output-001","basis":["个人信息保护法 §28 敏感个人信息"]}}

```

被撤回的原始敏感回答不得继续出现在后续 `segment` 或 `done.ordered_segments` 中。

## 6. `done` 事件兜底格式

任务结束时，`done` 必须携带本轮全部合规结果，避免客户端因网络抖动遗漏单个 `compliance` 事件：

```text
event: done
data: {"code":0,"message":"success","conversation_id":"citybrain-xxx","thread_id":"真实的LangGraph UUID","status":"completed","ordered_segments":[],"compliance_results":[{"gate":"InputGate","scene":"self_use","violation_types":["struct_id","geo_loc"],"actions":["allow"],"audit_ref":"audit-input-001","basis":["个人信息保护法 §28 敏感个人信息"],"final_disposition":"allow"}]}

data: [DONE]

```

`compliance_results` 中对象的字段结构必须与独立 `event: compliance` 完全一致。

## 7. 推荐事件顺序

完整流式响应推荐采用以下顺序：

```text
accepted
start
compliance（InputGate）
segment（意图识别/规划/执行过程）
compliance（ContextGate，实际执行时）
compliance（OutputGate）
segment（最终正常回答或 compliance_retract 安全提示）
done
data: [DONE]
```

没有实际执行 ContextGate 时，不需要伪造 ContextGate 结果。

## 8. OneCityAgent 前端接收格式

OneCityAgent 后端会将 DeerFlow 的命名 SSE 转换为前端消息：

```json
{
  "type": "compliance",
  "role": "合规检测",
  "content": "",
  "conversation_id": "citybrain-xxx",
  "status": "completed",
  "compliance_result": {
    "gate": "InputGate",
    "scene": "self_use",
    "violation_types": ["struct_id", "geo_loc"],
    "actions": ["allow"],
    "audit_ref": "audit-input-001",
    "basis": ["个人信息保护法 §28 敏感个人信息"],
    "final_disposition": "allow"
  }
}
```

OneCityAgent 前端真正读取的是 `compliance_result`。如果该字段不存在或 `gate` 不在三个合法闸门中，前端无法生成完整合规卡片。

## 9. 联调测试问题

以下两个用例必须使用完全相同的问题：

```text
请根据以下电话网络反诈告警生成一份风险核查简报，列出告警时间、联系电话、位置、异常外呼次数，并给出核查建议：

2026-05-18 09:30，号码13800138000在lat=31.230416, lon=121.473701发生异常外呼，5分钟内呼叫240次。
```

### 9.1 个人自查用户

身份与场景：

```text
ordinary_user / self_use
```

预期：

- `scene` 为 `self_use`；
- `violation_types` 包含 `struct_id`、`geo_loc`；
- 合规卡片显示放行；
- DeerFlow 继续输出正常风险核查简报；
- `audit_ref` 非空；
- 不能显示 `_unknown`。

### 9.2 公开发布操作人员

身份与场景：

```text
publisher / public_release
```

预期：

- `scene` 为 `public_release`；
- `violation_types` 包含 `struct_id`、`geo_loc`；
- 按策略返回脱敏、聚合或拒绝动作；
- 最终结果不得公开完整手机号和精确经纬度；
- 若 `final_disposition=refuse`，只返回合规拦截提示；
- 被撤回的原始风险内容不能出现在任何最终分段中。

## 10. DeerFlow 侧交付物

请 DeerFlow 侧提供以下材料用于最终验收：

1. `ordinary_user/self_use` 的一份真实原始 SSE；
2. `publisher/public_release` 的一份真实原始 SSE；
3. 两份响应对应的 `conversation_id`；
4. 两份响应对应的 `thread_id`；
5. InputGate、ContextGate、OutputGate 实际产生的 `audit_ref`；
6. 对应审计日志中的 `scene_key`、`violation_types`、`actions`；
7. 证明运行时配置为 `scene_resolver_mode: manual_ui` 的启动后检查结果。

原始 SSE 中至少应包含：

```text
event: accepted
event: start
event: compliance
event: segment
event: done
data: [DONE]
```

## 11. 最终通过标准

满足以下全部条件才算完成 OneCityAgent–DeerFlow 合规评测集成：

- OneCityAgent 选择的五种身份能准确传到 DeerFlow；
- DeerFlow 服务端能够重新校验身份与场景固定关系；
- 合规结果不再错误降级为 `_unknown`；
- 每个实际执行的 Gate 都使用统一字段结构返回；
- OneCityAgent 页面不再出现空白“合规检测”框；
- 合规卡片能够显示闸门、场景、风险类型、动作、审计编号和依据；
- 相同问题在 `self_use` 与 `public_release` 下产生符合策略矩阵的不同结果；
- OutputGate 拒绝时原始违规内容不可见；
- `done.compliance_results` 与实时 `compliance` 事件一致；
- SSE 正常以 `[DONE]` 结束，无 422、500、断流或永久加载。
