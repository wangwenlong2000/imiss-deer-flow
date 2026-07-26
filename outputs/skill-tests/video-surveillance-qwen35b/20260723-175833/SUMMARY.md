# qwen35B 视频监控自然问题小规模测试

- model: `qwen3.6-35b-a3b`
- total: `5`
- passed: `1`
- route_mismatch: `2`
- model_mismatch: `0`
- errors: `2`
- history_preserved: `True`
- video_attachments_uploaded: `4`

| ID | 通过 | 实际技能 | 缺少预期技能 | 模型 |
| --- | --- | --- | --- | --- |
| `C-001` | `False` | `analyze-video, batch-video-ingestion, camera-health-check, city-video-intelligence, city-video-intelligence && python scripts, data-analysis, duplicate-event-merge, evidence-package-generation, evidence-snapshot, ffmpeg-utils, frame-sampling, human-review-routing, object-detection, object-statistics, object-tracking, privacy-masking, road-traffic-analysis, roi-mapping, single-video-event-analysis, video-embedding-index, video-object-analytics, video-search, video-segment-extraction, video-stream-ingestion` | `` | `qwen3.6-35b-a3b` |
| `C-002` | `False` | `data-analysis, spatiotemporal_trajectory` | `video-stream-ingestion` | `qwen3.6-35b-a3b` |
| `C-011` | `False` | `analyze-video, batch-video-ingestion, camera-health-check, city-video-intelligence, data-analysis, duplicate-event-merge, evidence-package-generation, evidence-snapshot, ffmpeg-utils, frame-sampling, human-review-routing, object-detection, object-statistics, object-tracking, privacy-masking, roi-mapping, single-video-event-analysis, video-embedding-index, video-object-analytics, video-search, video-segment-extraction, video-stream-ingestion` | `` | `qwen3.6-35b-a3b` |
| `C-038` | `True` | `analyze-video, batch-video-ingestion, camera-health-check, city-video-intelligence, data-analysis, duplicate-event-merge, evidence-package-generation, evidence-snapshot, ffmpeg-utils, frame-sampling, human-review-routing, object-detection, object-statistics, object-tracking, privacy-masking, roi-mapping, single-video-event-analysis, video-embedding-index, video-object-analytics, video-search, video-segment-extraction, video-stream-ingestion` | `` | `qwen3.6-35b-a3b` |
| `C-040` | `False` | `data-analysis` | `city-video-intelligence` | `qwen3.6-35b-a3b` |
