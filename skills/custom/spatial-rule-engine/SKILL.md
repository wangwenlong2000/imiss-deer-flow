---
name: spatial-rule-engine
description: 空间规则配置与判定，支持 ROI 区域、越线、电子围栏、方向规则和时间段规则。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 空间规则配置

## 定位

城市场景必须判断“在哪里发生”和“是否违反规则”。本 skill 将 ROI、越线、电子围栏、方向、时间段等规则应用到结构化目标轨迹和事件上。

## 触发关键词

- ROI
- 越线
- 电子围栏
- 方向规则
- 时间段规则
- 区域入侵
- 规则配置

## 脚本入口

```bash
python3 /mnt/skills/custom/spatial-rule-engine/scripts/apply_spatial_rules.py <events_or_tracks_json> --rules <rules_json>
```

如果未提供规则文件，脚本会使用默认城市治理规则模板。

## 输出要求

输出 `rule_matches`、`violations`、`rule_version` 和 `unmatched_events`，供告警研判和证据生成使用。
