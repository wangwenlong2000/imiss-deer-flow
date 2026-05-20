---
name: privacy-anonymization
description: 自动对监控视频中的敏感信息（如人脸、车牌、特定区域）进行模糊化处理，确保隐私合规。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
  - read_file
---

# 视频隐私脱敏技能 (Privacy Anonymization)

## 技能说明
该技能是城市超脑的“合规哨兵”。它通过 `ffmpeg` 或 `opencv` 算法，对视频流中的隐私特征进行自动化脱敏。

## 触发关键词
- "隐私脱敏", "模糊人脸", "打码", "视频匿名化", "privacy-mask"

## 操作规范
1. **全自动脱敏**：`python3 /mnt/skills/custom/privacy-anonymization/scripts/anonymize_video.py <input_path> <output_path> --mode face`
2. **区域脱敏**：指定坐标进行打码。
3. **输出要求**：脱敏后的视频必须存放在 `/mnt/user-data/outputs/` 目录下以便用户下载。
