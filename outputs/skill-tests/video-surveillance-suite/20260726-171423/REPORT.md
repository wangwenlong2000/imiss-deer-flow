# 视频监控 skill 测试结果

运行时间：2026-07-26 17:14:45

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

## Layer `plan`（13/14 通过）

| 用例 | 标题 | 结果 | 说明 |
| --- | --- | --- | --- |
| A1 | 单视频事件请求 -> single-video-event-analysis 且要求人工复核 | pass | {"capability": "single_video_event_understanding", "recommended_skill_chain": ["single-video-event-analysis"], "requires_human_review": true} |
| A2 | 交警调阅请求 -> video-search，不误入事件分析 | pass | {"business_scenario": "traffic_police_video_review", "capability": "video_asset_retrieval", "recommended_skill_chain": ["video-search"]} |
| A3 | 证据包缺少视频路径/事件时间 -> 单一合并追问 | pass | {"capability": "evidence_preservation", "missing_fields": ["视频文件路径", "事件时间或片段范围"], "follow_up_question": "请问视频文件路径和事件时间或片段范围是什么？"} |
| A4 | 语义检索请求 -> video-search（仅计划，执行依赖 StreetModel） | pass | 计划正确；StreetModel/ES 未启用，检索执行不在本次范围 |
| A5 | 越界能力请求 -> capability_gap / 护栏 | pass | {"capability": "camera_health_operations", "business_scenario": "camera_health_ops", "capability_gap": {"supported": "partial", "gaps": ["Real-time stream conne |
| A6 | 不存在的本地视频 -> SOURCE_NOT_FOUND 且非零退出码 | pass | {"error_code": "SOURCE_NOT_FOUND", "retryable": false} |
| A7 | 事件去重空输入 -> 标准成功 JSON | pass | {"events": 0} |
| A8 | 同摄像头同 ROI 重复事件 -> 合并并取高置信度与时间并集 | pass | {"merged_events": 2, "duplicates": [{"main_event_id": "E1", "merged_event_id": "E2"}]} |
| A9 | ROI 几何 -> 多边形内命中、多边形外不命中 | pass | {"matches": {"T_IN": true, "T_OUT": false}} |
| A10 | 人工复核契约 -> 低置信/画面退化进 manual_review，高置信 auto_pass | pass | {"low_confidence": ["confidence_below_threshold", "high_risk_event_type", "camera_health_degraded"], "high_confidence_queue": "auto_pass"} |
| A11 | Router Card 基础字段与 ID 唯一性 | pass | {"router_cards": 117, "unique_ids": 117} |
| A12 | registry.json 与视频 bundle 路径一致（文档 7.1 回归） | pass | {"bundle_skills": 21, "registry_entries": 21} |
| A13 | 无 embedding 的 Router Card 集合边界检查 -> conflicts=[] | pass | {"conflicts": 0} |
| A14 | 在线检索类 skill 执行（依赖 StreetModel/ES） | skip | StreetModel(219.245.185.245:3130) 与 Elasticsearch(172.17.0.1:3128) 未启用，video-search / video-embedding-index / object-statistics 的在线检索路径不在本次执行范围 |

## Layer `exec`（8/14 通过）

| 用例 | 标题 | 结果 | 说明 |
| --- | --- | --- | --- |
| B0 | 沙箱依赖自检（ffmpeg / OpenCV / Ultralytics / 视频 / 权重） | pass | {"ffmpeg": "/usr/bin/ffmpeg", "ffprobe": "/usr/bin/ffprobe", "cv2": "5.0.0", "numpy": "2.2.6", "ultralytics": "8.4.103", "torch": "2.13.0+cu130", "video_exists" |
| B1 | 真实视频接入 -> 返回时长/分辨率/帧率元数据 | pass | {"camera_id": "CAM_DEERFLOW_001", "source_type": "local_file", "file_status": "ok", "video_session_id": "VS_4b505912", "started_at": "2026-05-20T10:00:00+08:00" |
| B2 | 抽帧 -> 帧数与磁盘上的图片文件一致 | pass | {"frame_count": 6, "first_frame": "/mnt/out/work/frames/frames/CAM_DEERFLOW_001/CAM_DEERFLOW_001_202605201000000800_0001.jpg"} |
| B3 | YOLO 目标检测 -> 有检出、类别受限、bbox/置信度合法 | fail | 检出了未申请的类别: {None} |
| B4 | 目标跟踪 -> 生成轨迹且不输出事件结论 | skip | 依赖 B3 检测结果 |
| B5 | ROI 匹配 -> 全画面 ROI 命中真实轨迹 | skip | 依赖 B4 轨迹结果 |
| B6 | 摄像头健康检查 -> 返回合法 health_status | pass | {"camera_id": "CAM_DEERFLOW_001", "health_status": "ok", "health_score": 100, "issues": []} |
| B7 | 单视频事件分析 -> 生成 review_manifest 审帧产物 | pass | {"review_manifest_uri": "/mnt/out/work/event-analysis/review_manifest.json", "keys": ["all_frames_uri", "frame_count", "frames", "metadata_uri", "next_step", "o |
| B8 | 证据截图 -> 生成真实图片文件与 sha256 哈希 | pass | {"uri": "/mnt/out/work/evidence/evidence/EVT_TEST_0001_snapshot.jpg", "hash": "sha256:2d1c8e07974759b8418600ab6753c5c9c016de264d14056b9b4ae3307a602df2", "privac |
| B9 | 片段截取 -> 生成前后各 2 秒、实际时长约 4 秒的 mp4 | fail | video-segment-extraction 退出码 1 != 0; stderr= |
| B10 | 隐私打码 -> 输出与原图不同的打码副本 | pass | {"masked_uri": "/mnt/out/work/evidence/evidence/EVT_TEST_0001_snapshot_masked.jpg", "method": "gaussian_blur"} |
| B11 | 证据事件 -> 低置信度进入人工复核队列 | pass | {"event_id": "EVT_TEST_0001", "review_required": true, "reasons": ["confidence_below_threshold"], "review_threshold": 0.7, "queue": "manual_review"} |
| B12 | ffmpeg-utils -> 按时间戳导出关键帧 | fail | ffmpeg-utils 退出码 2 != 0; stderr=usage: run.py [-h] [--input INPUT] [--operation {segment,keyframe}]               [--input-path INPUT_PATH] [--start-time START_ |
| B13 | 目标检测输入缺失 -> 失败契约且不伪造检测结果 | fail | object-detection 无 JSON 输出; stderr=etection/scripts/run.py", line 225, in main     frames = load_list(args.frames_json, "frames") if args.frames_json else input |
