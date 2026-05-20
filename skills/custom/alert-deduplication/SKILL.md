---
name: alert-deduplication
description: 告警研判与去重，支持告警合并、置信度过滤、同一事件去重和误报反馈记录。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 告警研判与去重

## 定位

本 skill 把结构化事件转成可用告警。它负责过滤低置信度、合并同一摄像头同类事件、压制重复帧告警，并保留误报反馈入口。

## 触发关键词

- 告警去重
- 告警合并
- 置信度过滤
- 误报反馈
- 告警研判
- 重复告警

## 脚本入口

```bash
python3 /mnt/skills/custom/alert-deduplication/scripts/deduplicate_alerts.py <events_json> --min-confidence 0.75
```

## 输出要求

输出 `alerts`、`suppressed`、`dedup_groups`、`feedback_actions`。下游派单、报告和证据包必须使用去重后的 `alerts`。
