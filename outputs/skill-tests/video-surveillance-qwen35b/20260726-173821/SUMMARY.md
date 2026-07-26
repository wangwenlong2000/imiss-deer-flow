# qwen35B 视频监控自然问题小规模测试

- model: `qwen3.6-35b-a3b`
- total: `5`
- passed: `4`
- route_mismatch: `1`
- model_mismatch: `0`
- errors: `0`
- history_preserved: `True`
- video_attachments_uploaded: `4`

| ID | 通过 | 实际技能 | 缺少预期技能 | 模型 |
| --- | --- | --- | --- | --- |
| `C-001` | `True` | `analyze-video, batch-video-ingestion, camera-health-check, city-video-intelligence, data-analysis, duplicate-event-merge, evidence-package-generation, evidence-snapshot, ffmpeg-utils, frame-sampling, human-review-routing, models, object-detection, object-statistics, object-tracking, privacy-masking, roi-mapping, single-video-event-analysis, video-embedding-index, video-object-analytics, video-search, video-segment-extraction, video-stream-ingestion` | `` | `qwen3.6-35b-a3b` |
| `C-002` | `True` | `analyze-video, batch-video-ingestion, camera-health-check, city-video-intelligence, data-analysis, duplicate-event-merge, evidence-package-generation, evidence-snapshot, ffmpeg-utils, frame-sampling, human-review-routing, object-detection, object-statistics, object-tracking, privacy-masking, roi-mapping, single-video-event-analysis, video-embedding-index, video-object-analytics, video-search, video-segment-extraction, video-stream-ingestion` | `` | `qwen3.6-35b-a3b` |
| `C-011` | `True` | `analyze-video, batch-video-ingestion, camera-health-check, city-video-intelligence, data-analysis, duplicate-event-merge, evidence-package-generation, evidence-snapshot, ffmpeg-utils, frame-sampling, human-review-routing, models, object-detection, object-statistics, object-tracking, privacy-masking, roi-mapping, single-video-event-analysis, video-embedding-index, video-object-analytics, video-search, video-segment-extraction, video-stream-ingestion` | `` | `qwen3.6-35b-a3b` |
| `C-038` | `True` | `analyze-video, batch-video-ingestion, camera-health-check, city-video-intelligence, data-analysis, duplicate-event-merge, evidence-package-generation, evidence-snapshot, ffmpeg-utils, frame-sampling, human-review-routing, human-review-routing
, human-review-routing"}, "id": "call_def70feae4fa4d6296ef6a74", "type": "tool_call"}, {"name": "invoke_skill", "args": {"skill_name": "human-review-routing", "mode": "prepare", "input_envelope": "{\"task\": \"确定事件置信度低、摄像头画面有遮挡或事件涉及执法时，哪些结果必须进入人工复核的完整规则\"}"}, "id": "call_b7150d2ddac547beabb8eb00", "type": "tool_call"}, {"name": "read_file", "args": {"description": "读取人工复核路由技能文件，获取三类触发条件的判定规则", "path": ", human-review-routing"}, "id": "call_def70feae4fa4d6296ef6a74", "type": "tool_call"}, {"name": "read_file", "args": {"description": "查看路由卡片中的配置信息", "path": ", object-detection, object-statistics, object-tracking, privacy-masking, roi-mapping, single-video-event-analysis, video-embedding-index, video-object-analytics, video-search, video-segment-extraction, video-stream-ingestion` | `` | `qwen3.6-35b-a3b` |
| `C-040` | `False` | `data-analysis` | `city-video-intelligence` | `qwen3.6-35b-a3b` |
