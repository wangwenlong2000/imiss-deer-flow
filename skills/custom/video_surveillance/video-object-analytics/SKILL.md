---
name: video-object-analytics
description: Deterministic structured entrypoint for local-video object detection and cross-frame tracking. Use for direct requests to count visible people, vehicles, or other objects, or to track them across frames. The dedicated video_object_analytics tool owns all CLI parameters, model configuration, persistent paths, and downstream skill order.
---

# Video Object Analytics

This skill is the structured entrypoint for direct object detection and tracking requests.

## Mandatory execution rule

Call the `video_object_analytics` tool. Do not call `bash`, `frame-sampling`, `object-detection`, or `object-tracking` scripts directly for this workflow. Do not invent CLI flags.

The tool accepts structured fields only:

- `video_path`
- `operation`: `detect` or `track`
- `camera_id`
- `capture_seconds`
- `interval_seconds`
- `labels`
- `every_frame`: 用户明确要求逐帧时必须设为 true；默认 interval 模式只是抽样，不能称为逐帧
- `started_at`: 仅在用户或权威记录提供真实拍摄时间时填写

The tool internally runs the fixed chain:

```text
detect: frame-sampling -> object-detection
track:  frame-sampling -> object-detection -> object-tracking
```

All intermediate files are placed under the thread's `/mnt/user-data/workspace` directory and the local YOLO model is selected by the tool. Do not use `/tmp` for artifacts that another tool must read.

## Output contract

Return the tool's structured result. Use its `summary`, `track_summary`, `artifacts`, and `executed_skills` fields. Do not manually recalculate counts from raw JSON or claim a confidence threshold that is not present in the tool result.

## Constraints

- YOLO detects visible objects only; it does not decide whether an event occurred.
- Tracking output is object-level trajectory analytics only; it does not infer events or identities.
- If the tool returns `status=failed`, report its concrete error and stop instead of trying alternative shell commands.

时间区间为 `[0, capture_seconds)`。未知摄像头和拍摄时间留空；使用 `source_elapsed_seconds` / `timestamp_display` 解释相对时间，不能把内部文件标识当作真实摄像头 ID。

完整的逐帧计数和检测框明细由固定 exporter 生成，路径在 artifacts.per_frame_csv / detection_details_csv；summary.per_frame 只预览前20帧，不能据此声称只处理了20帧。导出时直接展示工具生成的 CSV，不要手工重写或遗漏零目标帧。
