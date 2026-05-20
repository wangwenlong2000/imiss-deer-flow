---
name: skill-virtual-ptz
description: 模拟摄像头云台的转向与缩放。允许智能体在大图中选取特定坐标点进行放大查看。
version: 1.0.0
author: Gemini Proactive
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 虚拟云台缩放技能 (Virtual PTZ)

## 技能说明
该技能模拟了真实摄像头的缩放操作。Agent 可以根据全景图中的可疑点，指定坐标进行“局部放大”，获取更清晰的画面细节。

## 触发关键词
- "放大这里", "拉近镜头", "查看细节", "ptz-zoom"

## 操作规范
1. **执行缩放**：`python3 /mnt/skills/custom/skill-virtual-ptz/scripts/virtual_ptz.py <input> <output> <x> <y> <zoom>`
2. **坐标系说明**：x 和 y 是 0.0 到 1.0 之间的浮点数（例如 0.5, 0.5 表示正中心）。
