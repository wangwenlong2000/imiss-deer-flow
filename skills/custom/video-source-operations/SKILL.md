---
name: video-source-operations
description: 视频源接入与巡检，覆盖摄像头在线状态、码流质量、延迟、遮挡、黑屏、花屏、RTSP/GB28181/平台视频源接入检查。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 视频源接入与巡检

## 定位

这是所有视频智能分析的前置 skill。任何视频分析、事件识别、证据生成任务，如果输入是摄像头、RTSP、GB28181、平台流或历史录像，都应先调用本 skill 判断视频源是否可用。

## 触发关键词

- 摄像头在线状态
- 视频源巡检
- RTSP 接入
- GB28181 接入
- 码流质量
- 黑屏
- 花屏
- 遮挡
- 延迟

## 脚本入口

```bash
python3 /mnt/skills/custom/video-source-operations/scripts/source_probe.py <source_uri> --source-type file|rtsp|gb28181|platform
```

## 输出要求

必须返回标准 JSON，包含：

- `connectivity`: 在线状态、接入协议、延迟。
- `stream_quality`: 分辨率、帧率、码率估计、丢帧风险。
- `visual_health`: 黑屏、花屏、遮挡、冻结、模糊。
- `readiness`: 是否允许进入后续智能分析。

如果 `readiness.ready_for_analysis` 为 `false`，Agent 不应继续调用目标检测或事件识别 skill。
