---
name: semantic-video-search
description: 支持使用自然语言对视频内容进行语义搜索，例如“寻找穿红衣服的人”或“寻找白色的轿车”。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 语义视频搜索技能 (Semantic Video Search / VQA)

## 技能说明
该技能赋予 Agent “理解并搜索”视频的能力。它通过多模态特征匹配，将用户的自然语言描述与视频帧进行关联。

## 触发关键词
- "搜索视频", "寻找...", "有没有看到...", "匹配特征", "video-search"

## 操作规范
1. **执行搜索**：`python3 /mnt/skills/custom/semantic-video-search/scripts/search_video.py <video_path> "<query>"`
2. **返回结果**：脚本将返回匹配度最高的几个时间戳及置信度。
