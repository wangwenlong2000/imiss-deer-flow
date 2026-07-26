基于已加载的 `human-review-routing` 专业技能及其底层实现逻辑，以下是三类触发条件进入人工复核的完整判定规则：

---

## 必须进入人工复核的结果清单

系统通过四个独立判断条件做分流决策，**满足任一条件即强制进入 `manual_review` 队列**：

### 一、事件置信度低于阈值

- **判定条件**：`event.confidence < review_threshold`
- **默认阈值**：`0.85`（85%）
- **可配置覆盖**：按事件类型设置差异化阈值（如打架事件阈值可设为 0.90）
- **路由原因码**：`confidence_below_threshold`
- **含义**：算法对事件的判断把握不足，需要人工二次确认是否存在真实事件

### 二、摄像头画面质量降级（含遮挡）

- **判定条件**：`camera_health.health_status == "degraded"`
- **覆盖场景**：离线（offline）、黑屏（black-screen）、模糊（blurred）、**遮挡（occluded）**、冻结（frozen）、损坏（corrupted）、偏移（shifted）等所有降质状态
- **路由原因码**：`camera_health_degraded`
- **含义**：画面质量下降直接影响事件判定的可靠性，无论事件置信度多高都必须人工复核

### 三、事件涉及执法影响

- **判定条件**：`event.affects_enforcement == true`
- **路由原因码**：`affects_enforcement`
- **典型事件类型**：交通违法抓拍、违停处罚、闯红灯取证、占道经营查处等任何可能作为行政处罚或司法证据的事件
- **含义**：执法结果具有法律后果，必须经过人工审核才能生效

### 四、高风险事件类型

- **判定条件**：`event.event_type ∈ high_risk_event_types`
- **路由原因码**：`high_risk_event_type`
- **典型类型**：打架斗殴、跌倒/摔倒、烟火/火灾、非法入侵、人群聚集等安全敏感类事件
- **含义**：即使置信度达标且画面正常，因事件本身的高风险属性仍需人工确认

---

## 综合判定矩阵

| 触发条件 | 原因码 | 是否可豁免 |
|---|---|---|
| 置信度 < 阈值 | `confidence_below_threshold` | ❌ 不可豁免 |
| 摄像头降质/遮挡 | `camera_health_degraded` | ❌ 不可豁免 |
| 涉及执法 | `affects_enforcement` | ❌ 不可豁免 |
| 高风险事件类型 | `high_risk_event_type` | ❌ 不可豁免 |

> **关键原则**：以上四类为**"一票否决"**机制——只要命中任意一项，结果就进入 `manual_review` 队列；只有四项全部不命中时，才会走 `auto_pass` 自动通过。

---

## 输出结构

每个事件的路由决策包含以下字段：

```json
{
  "event_id": "...",
  "review_required": true,
  "reasons": ["confidence_below_threshold", "affects_enforcement"],
  "review_threshold": 0.85,
  "queue": "manual_review"
}
```

一个事件可以同时命中多个原因码（例如低置信度 + 涉及执法），意味着该事件同时存在算法不确定性和法律合规双重审查需求。