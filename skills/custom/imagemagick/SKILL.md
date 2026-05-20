---
name: imagemagick
description: 图像处理与增强工具。专注于监控画面的画质修复、夜间提亮及噪点处理。
version: 1.0.0
author: Gemini
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 图像增强技能 (ImageMagick)

## 技能说明
利用 ImageMagick 引擎对监控图像进行优化。

## 触发关键词
- "增强画质", "锐化图片", "夜间提亮", "修复车牌"

## 操作规范
1. **自动增强**：`python3 /mnt/skills/custom/imagemagick/scripts/image_enhance.py <input> <output> auto`
2. **夜间模式**：`python3 /mnt/skills/custom/imagemagick/scripts/image_enhance.py <input> <output> night`
