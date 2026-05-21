---
name: analyze-video
description: 专业法医级视频分析技能。支持场景检测、多阶段帧提取及结构化报告生成。适用于车祸取证、行为追踪等高精度场景。
version: 1.0.0
author: jdrodriguez
compatibility: ">=2.0.0"
allowed-tools:
  - bash
  - read_file
  - write_file
---

# 专业视频取证分析技能 (Analyze Video)

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## 技能说明
`analyze-video` 是一个高精度的视频帧分析工具，专为“城市超脑”中的法医级取证设计。它通过 `ffmpeg` 在本地执行场景变更检测，并自动将长视频划分为多个“章节”，以便 Agent 进行并行化的视觉审核。

## 核心能力
- **自动分段**：自动识别视频中的重要场景并进行章节切分。
- **自适应抓图**：在场景剧烈变化处自动增加抓拍频率，确保不遗漏关键动作。
- **结构化分析**：为每一帧生成精确的时间戳和元数据。

## 触发关键词
- "分析视频", "取证", "视频回溯", "analyze-video", "检查监控"

## 权限声明
- `bash`: 用于执行 `python3 /mnt/skills/custom/analyze-video/scripts/extract_frames.py`。
- `ffmpeg`: 核心依赖。

## 操作规范 (Guidelines)
1. **工作流程**：
   - 调用：`python3 /mnt/skills/custom/analyze-video/scripts/extract_frames.py <video_path> --output-dir /mnt/user-data/workspace/video_review`。
   - 读取产生的 `metadata.json` 了解视频章节分布。
   - 对感兴趣的章节（chapters/）进行视觉描述。
2. **注意事项**：
   - 提取过程可能耗时较长（视视频长度而定）。
   - 最终产物应汇总为一份包含时间轴的 PDF 或 Markdown 报告。

## 示例 (Example)
**用户：** "分析一下 `/mnt/datasets/surveillance/raw/test_video_01.mp4` 里的异常行为。"
**Agent 动作：**
- 执行：`python3 /mnt/skills/custom/analyze-video/scripts/extract_frames.py /mnt/datasets/surveillance/raw/test_video_01.mp4`
- 根据输出的 `output_dir` 读取元数据。
- 回复用户： "视频提取完成，共识别出 3 个关键场景，正在为您逐一分析..."
