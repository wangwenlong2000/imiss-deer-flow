---
name: skill-motion-detector
description: 自动检测监控视频中的物体移动，并返回精确的时间戳片段。有助于在海量录像中快速定位事件。
version: 1.0.0
author: Gemini Proactive
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 智能运动检测技能 (Motion Detector)

## 技能说明
该技能解决了“城市超脑”在处理海量长视频时的效率问题。它通过背景减除算法，自动过滤无人的静态画面，仅提取有物体移动的时刻。

## 触发关键词
- "有动静的时间", "什么时候有人经过", "定位运动片段", "motion-detect"

## 操作规范
1. **执行检测**：`python3 /mnt/skills/custom/skill-motion-detector/scripts/detect_motion.py <video_path>`
2. **后续动作**：获取返回的时间戳（start/end）后，建议配合 `ffmpeg-utils` 截取对应的视频片段进行精细化分析。
