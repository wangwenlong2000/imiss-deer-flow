---
name: scenario-event-recognition
description: 按业务场景识别城市事件，覆盖交通拥堵、违停、逆行、占道经营、人员聚集、摔倒、烟火、施工占道、垃圾堆放等。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 事件识别

## 定位

本 skill 面向业务场景输出事件，不做笼统“视频分析”。它应优先消费 `object-tracking-analysis` 的结构化目标、轨迹和基础事件结果。

## 触发关键词

- 交通拥堵
- 违停
- 逆行
- 占道经营
- 人员聚集
- 摔倒
- 烟火
- 施工占道
- 垃圾堆放
- 城市事件识别

## 脚本入口

```bash
python3 /mnt/skills/custom/scenario-event-recognition/scripts/recognize_events.py <tracking_json_or_video> --scenario traffic|governance|safety|all
```

## 输出要求

必须输出 `events`，每个事件包含 `event_type`、`severity`、`confidence`、`camera_id`、`location`、`timestamp_start`、`timestamp_end` 和 `evidence`。
