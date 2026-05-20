---
name: anomaly-behavior-detection
description: 识别监控视频中的异常人体行为，如摔倒、打斗、长时间徘徊或攀爬围栏。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 异常行为识别预警技能 (Anomaly Behavior Detection)

## 技能说明
该技能专注于保障公共安全和特殊人群（如老人、安保重点区域）的安全。它通过骨架提取和时序分析来识别异常动作。

## 触发关键词
- "异常行为", "有人摔倒", "打架斗殴", "翻越围栏", "anomaly-behavior"

## 操作规范
1. **执行检测**：`python3 /mnt/skills/custom/anomaly-behavior-detection/scripts/detect_anomaly.py <video_path>`
2. **分类输出**：脚本将返回行为类别（Fall/Fighting/Loitering/Climbing）及发生的时间段。
