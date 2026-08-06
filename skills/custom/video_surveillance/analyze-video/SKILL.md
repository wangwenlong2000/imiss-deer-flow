---
name: analyze-video
description: 专业视频抽帧与取证准备技能。支持场景检测、多阶段帧提取和 metadata 生成，供 single-video-event-analysis 做 LLM 审帧使用；本 skill 不再作为最终事件检测入口。
version: 1.0.0
author: jdrodriguez
compatibility: ">=2.0.0"
allowed-tools:
  - bash
  - read_file
  - write_file
---

# 专业视频抽帧取证准备技能 (Analyze Video)

## Execution Priority

Prioritize calling this skill's `scripts/extract_frames.py` entrypoint to produce timestamped frames and metadata. For final event detection, use `single-video-event-analysis`.

## 技能说明
`analyze-video` 是一个高精度的视频抽帧和取证准备工具。它通过 `ffmpeg` 在本地执行场景变更检测，并自动将长视频划分为多个“章节”，为后续 LLM 审帧提供可回放、可审计的帧证据。

## 核心能力
- **自动分段**：自动识别视频中的重要场景并进行章节切分。
- **自适应抓图**：在场景剧烈变化处自动增加抓拍频率，确保不遗漏关键动作。
- **审帧准备**：输出便于 LLM 审查的帧目录和 `metadata.json`。
- **结构化帧元数据**：为每一帧生成精确的时间戳和元数据。

## 触发关键词
- "视频抽帧", "场景检测", "关键帧提取", "取证准备", "视频回溯", "analyze-video", "metadata.json"

## 权限声明
- `bash`: 用于执行 `python3 /mnt/skills/custom/video_surveillance/analyze-video/scripts/extract_frames.py`。
- `ffmpeg`: 核心依赖。

## 操作规范 (Guidelines)
1. **工作流程**：
   - 调用：`python3 /mnt/skills/custom/video_surveillance/analyze-video/scripts/extract_frames.py <video_path> --output-dir /mnt/user-data/workspace/video_review`。
   - 读取产生的 `metadata.json` 了解视频章节、场景变化和帧分布。
   - 如需最终事件检测，交给 `single-video-event-analysis` 统一完成。
2. **注意事项**：
   - 提取过程可能耗时较长（视视频长度而定）。
   - 本 skill 不输出最终事件类型、事件置信度或人工复核结论。

## 示例 (Example)
**用户：** "把 `/mnt/datasets/surveillance/raw/test_video_01.mp4` 抽帧用于取证。"
**Agent 动作：**
- 执行：`python3 /mnt/skills/custom/video_surveillance/analyze-video/scripts/extract_frames.py /mnt/datasets/surveillance/raw/test_video_01.mp4`
- 根据输出的 `output_dir` 读取元数据。
- 回复用户： "视频提取完成，共识别出 3 个关键场景，可交给 single-video-event-analysis 做事件审查。"
