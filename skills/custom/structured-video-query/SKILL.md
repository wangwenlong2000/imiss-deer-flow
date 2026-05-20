---
name: structured-video-query
description: 自然语言查询与摘要，基于结构化事件和指标回答城市视频数据问题，而不是直接盲看所有视频。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 自然语言查询与摘要

## 定位

本 skill 用于回答“今天人民路违停最多的点位有哪些”“过去 1 小时有哪些高风险事件”等问题。它只查询结构化事件、告警、指标和证据索引，不直接扫描原始视频。

## 触发关键词

- 查询
- 摘要
- 今天
- 过去 1 小时
- 高风险事件
- 违停最多
- 视频数据问答

## 脚本入口

```bash
python3 /mnt/skills/custom/structured-video-query/scripts/query_events.py <structured_json> --query "今天人民路违停最多的点位有哪些"
```

## 输出要求

输出 `answer`、`supporting_facts`、`filters` 和 `residual_risk`。回答必须引用结构化事件或告警作为依据。
