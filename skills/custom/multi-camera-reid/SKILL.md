---
name: multi-camera-reid
description: 跨相机目标重识别，支持在多个监控摄像头之间追踪特定行人或车辆的轨迹。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 跨相机目标追踪技能 (Multi-Camera ReID)

## 技能说明
该技能解决了单相机视野局限的问题。它提取目标的特征向量（Feature Embedding），并在整个城市摄像头网络中进行检索和匹配。

## 触发关键词
- "跨相机追踪", "追踪轨迹", "他在哪里消失了", "找这个人", "multi-camera-trace"

## 操作规范
1. **执行检索**：`python3 /mnt/skills/custom/multi-camera-reid/scripts/trace_target.py <target_id> <time_range>`
2. **轨迹输出**：脚本将返回一系列包含【时间、地点、坐标】的路径点。
