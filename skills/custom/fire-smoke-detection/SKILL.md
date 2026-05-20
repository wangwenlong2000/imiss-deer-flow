---
name: fire-smoke-detection
description: 实时监控视频中的烟雾和火光特征，一旦发现疑似火灾，立即触发高优先级预警。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 烟火灾害感知预警技能 (Fire & Smoke Detection)

## 技能说明
该技能是城市的“数字消防员”。它通过多尺度特征分析，识别初起火灾的烟雾和火光。

## 触发关键词
- "火灾预警", "哪里着火了", "冒烟检测", "视频查火", "fire-detect"

## 操作规范
1. **执行检测**：`python3 /mnt/skills/custom/fire-smoke-detection/scripts/detect_fire.py <video_path>`
2. **紧急处理**：一旦 `is_fire` 为 `True`，回复必须以“🚨 **重大安全警告**”开头。
