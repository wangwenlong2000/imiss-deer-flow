---
name: video-quality-diagnostics
description: 自动检测监控视频的质量问题，如亮度异常、对比度不足、噪声、镜头遮挡或画面冻结。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 视频质量诊断技能 (Video Quality Diagnostics)

## 技能说明
该技能用于监控系统的健康巡检。通过分析视频帧的统计特性，自动识别硬件故障或环境干扰。

## 触发关键词
- "检查视频质量", "画面模糊", "视频亮度", "质量诊断", "quality-check"

## 操作规范
1. **执行诊断**：`python3 /mnt/skills/custom/video-quality-diagnostics/scripts/diagnose_quality.py <video_path>`
2. **返回参数**：脚本将返回 JSON 格式的评分（0-100）及故障类型。
