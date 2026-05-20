---
name: skill-plate-forensics
description: 专门针对模糊车牌的取证增强技能。通过灰度变换、非锐化掩模及自适应锐化提升车牌字符的可辨识度。
version: 1.0.0
author: Gemini Proactive
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 车牌增强取证技能 (Plate Forensics)

## 技能说明
当原始监控画面中的车牌由于光线或距离原因无法清晰识别时，使用此技能对局部截图进行图像修复。

## 触发关键词
- "看不清车牌", "增强车牌", "修复违停图片", "plate-enhance"

## 操作规范
1. **执行增强**：`python3 /mnt/skills/custom/skill-plate-forensics/scripts/enhance_plate.py <input_img> <output_path>`
2. **注意事项**：该技能会改变图片原始色彩（转为灰度）以最大程度突出边缘特征。
