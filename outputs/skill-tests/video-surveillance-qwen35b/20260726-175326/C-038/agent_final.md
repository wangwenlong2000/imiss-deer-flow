根据视频监控系统中 `human-review-routing` 技能的实际执行逻辑，当以下**任一条件触发**时，事件候选必须进入人工复核队列：

---

## 一、四类强制入审触发条件

| 触发条件 | 判定逻辑 | 原因标记 |
|---------|---------|---------|
| **事件置信度低于阈值** | `event.confidence < review_threshold`（默认 0.85） | `confidence_below_threshold` |
| **高风险事件类型** | `event.event_type` 命中高危列表 | `high_risk_event_type` |
| **摄像头健康状态降级** | `camera_health.health_status == "degraded"`（含遮挡、模糊、黑屏等） | `camera_health_degraded` |
| **涉及执法取证** | `event.affects_enforcement == true` | `affects_enforcement` |

只要命中其中任意一项，结果即被路由至 `manual_review` 队列；四项均未命中才走 `auto_pass`。

---

## 二、三类情形的具体说明

### 1. 事件置信度低
- **默认阈值**：`0.85`（可通过 `config.review_thresholds[<event_type>]` 按事件类型覆盖）
- **适用场景**：LLM 审帧或检测模型对打架、摔倒、烟火等事件的判断把握不足
- **复核要点**：人工确认画面中是否存在该行为，避免误报或漏报

### 2. 摄像头画面有遮挡
- **判定方式**：通过 `camera-health-check` 技能获取镜头健康状态，当状态为 `degraded` 时触发
- **涵盖状态**：遮挡(occluded)、模糊(blurred)、黑屏(black-screen)、冻结(frozen)、偏移(shifted) 等
- **复核要点**：人工评估画面质量是否足以支撑事件判定的准确性

### 3. 事件涉及执法
- **判定方式**：事件记录中标记 `affects_enforcement = true`
- **典型场景**：交通违章抓拍、占道经营取证、违规入侵处罚等可能产生法律后果的事件
- **复核要点**：确保证据链完整、画面清晰可辨，满足行政执法证据标准

---

## 三、输出结构

每个事件经过分流后返回：
```json
{
  "event_id": "事件编号",
  "review_required": true/false,
  "reasons": ["触发原因列表"],
  "review_threshold": 0.85,
  "queue": "manual_review" / "auto_pass"
}
```

这套机制确保所有存在不确定因素或法律风险的事件都能得到人工二次确认，是视频智能分析系统中不可或缺的兜底环节。