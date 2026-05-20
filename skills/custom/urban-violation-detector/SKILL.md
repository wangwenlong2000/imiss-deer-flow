---
name: urban-violation-detector
description: 自动识别城市管理中的违规行为，如非法占道经营、乱丢垃圾、机动车占用非机动车道及违章停放。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 城市违章行为检测技能 (Urban Violation Detector)

## 技能说明
该技能解决了城管部门巡检压力大的问题。它通过 CV 算法自动截取违章瞬间并生成证据闭环。

## 触发关键词
- "城市违章", "非法占道", "乱丢垃圾", "占用非机动车道", "violation-detect"

## 操作规范
1. **执行检测**：`python3 /mnt/skills/custom/urban-violation-detector/scripts/detect_violations.py <video_path>`
2. **证据提取**：脚本将返回违章类型、时间戳及对应的截图路径。
