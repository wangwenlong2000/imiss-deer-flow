---
name: object-tracking-analysis
description: 目标检测与跨帧跟踪，识别人、车、非机动车、特殊车辆、遗留物、区域入侵并输出轨迹、计数和结构化目标事件。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 目标检测与跟踪

## 定位

本 skill 是事件识别、交通分析、安防研判和证据生成的结构化抽取底座。它只负责“看见什么、在哪里、怎么移动”，不负责最终业务定性。

## 触发关键词

- 目标检测
- 人车识别
- 非机动车
- 特殊车辆
- 轨迹生成
- 目标计数
- 遗留物
- 区域入侵

## 脚本入口

```bash
python3 /mnt/skills/custom/object-tracking-analysis/scripts/detect_and_track.py <video_path> --camera-id <camera_id>
```

## 输出要求

输出 `objects`、`tracks`、`counts`、`primitive_events`。下游事件识别、空间规则、告警研判应优先消费这些结构化结果，而不是重复解析视频。
