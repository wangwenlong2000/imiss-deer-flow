---
name: crowd-density-analysis
description: 实时估算公共区域（如广场、站台）的人数密度，并根据阈值自动触发拥挤预警。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 人群密度分析预警技能 (Crowd Density Analysis)

## 技能说明
该技能用于预防踩踏等安全事故。它能计算单位面积内的人头数，并划分为“舒适、拥挤、危险”三个等级。

## 触发关键词
- "人群密度", "有多少人", "拥挤预警", "人头计数", "crowd-check"

## 操作规范
1. **执行分析**：`python3 /mnt/skills/custom/crowd-density-analysis/scripts/calculate_density.py <video_path>`
2. **预警逻辑**：如果密度超过 4人/平米，必须在回复中加粗显示“危险”警告。
