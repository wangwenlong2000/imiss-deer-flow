# 视频监控 skill 测试结果

运行时间：2026-07-26 17:19:30

## 环境

| 项目 | 值 |
| --- | --- |
| 仓库 | /home/huangxiao/City_brain/imiss-deer-flow-qwen36-35b-a3b-test |
| 沙箱镜像 | huangxiao-deerflow-sandbox:network-tools |
| 数据集挂载 | /home/huangxiao/City_brain/imiss-deer-flow-qwen36-35b-a3b-test/datasets |
| LangGraph API | HTTP 200 |
| Elasticsearch | 不可用 (TimeoutError) |
| StreetModel | 不可用 (RemoteDisconnected) |
| skill_router | disabled (config.yaml skill_router.enabled=false) |

## Layer `exec`（14/16 通过）

| 用例 | 标题 | 结果 | 说明 |
| --- | --- | --- | --- |
| B0 | 沙箱依赖自检（ffmpeg / OpenCV / Ultralytics / 视频 / 权重） | pass | {"ffmpeg": "/usr/bin/ffmpeg", "ffprobe": "/usr/bin/ffprobe", "cv2": "5.0.0", "numpy": "2.2.6", "ultralytics": "8.4.103", "torch": "2.13.0+cu130", "video_exists" |
| B1 | 真实视频接入 -> 返回时长/分辨率/帧率元数据 | pass | {"camera_id": "CAM_DEERFLOW_001", "source_type": "local_file", "file_status": "ok", "video_session_id": "VS_313b477f", "started_at": "2026-05-20T10:00:00+08:00" |
| B2 | 抽帧 -> 帧数与磁盘上的图片文件一致 | pass | {"frame_count": 6, "first_frame": "/mnt/out/work/frames/frames/CAM_DEERFLOW_001/CAM_DEERFLOW_001_202605201000000800_0001.jpg"} |
| B3 | YOLO 目标检测 -> 有检出、类别受限、bbox/置信度合法 | pass | {"frames_with_detection": 6, "objects": 63, "labels": ["person"], "model": "/mnt/skills/custom/models/yolov8n.pt"} |
| B4 | 目标跟踪 -> 生成轨迹且不输出事件结论 | pass | {"tracks": 14, "sample": {"track_id": "track_0001", "camera_id": "CAM_DEERFLOW_001", "label": "person", "start_time": "2026-05-20T10:00:00+08:00", "end_time": " |
| B5 | ROI 匹配 -> 全画面 ROI 命中真实轨迹 | pass | {"matches": 14, "matched": 14} |
| B6 | 摄像头健康检查 -> 返回合法 health_status | pass | {"camera_id": "CAM_DEERFLOW_001", "health_status": "ok", "health_score": 100, "issues": []} |
| B7 | 单视频事件分析 -> 生成 review_manifest 审帧产物 | pass | {"review_manifest_uri": "/mnt/out/work/event-analysis/review_manifest.json", "keys": ["all_frames_uri", "frame_count", "frames", "metadata_uri", "next_step", "o |
| B8 | 证据截图 -> 生成真实图片文件与 sha256 哈希 | pass | {"uri": "/mnt/out/work/evidence/evidence/EVT_TEST_0001_snapshot.jpg", "hash": "sha256:017c1b292f60c4e887738c3fbb3c79fbd06b9b44eab76e4c0eee7037dec9499a", "privac |
| B9 | 片段截取 -> 生成前后各 2 秒、实际时长约 4 秒的 mp4 | pass | {"uri": "/mnt/out/work/segments/evidence/EVT_TEST_0001_clip.mp4", "duration_seconds": 4.0} |
| B10 | 隐私打码 -> 输出与原图不同的打码副本 | pass | {"masked_uri": "/mnt/out/work/evidence/evidence/EVT_TEST_0001_snapshot_masked.jpg", "method": "gaussian_blur"} |
| B11 | 证据事件 -> 低置信度进入人工复核队列 | pass | {"event_id": "EVT_TEST_0001", "review_required": true, "reasons": ["confidence_below_threshold"], "review_threshold": 0.7, "queue": "manual_review"} |
| B12 | ffmpeg-utils -> 按时间戳导出关键帧 | pass | {"uri": "/mnt/out/work/b12-keyframe.jpg", "size": 258494} |
| B13 | 目标检测输入缺失 -> 失败契约且不伪造检测结果 | fail | object-detection 对缺失的 --frames-json 直接抛出 FileNotFoundError: [Errno 2] No such file or directory: '/mnt/out/work/does-not-exist.json'，未返回 status=failed 的标准失败 JSO |
| B14 | 片段截取缺少事件时间 -> MISSING_EVENT 且不产出文件 | pass | {"error_code": "MISSING_EVENT", "message": "--event-time or --start-time is required"} |
| B15 | 输入文件缺失时的失败契约横向扫描 | fail | 以下 skill 在输入文件缺失时未返回标准失败 JSON: {"object-detection": "未返回失败 JSON：FileNotFoundError: [Errno 2] No such file or directory: '/mnt/out/work/no-such-input.json'", "ob |
