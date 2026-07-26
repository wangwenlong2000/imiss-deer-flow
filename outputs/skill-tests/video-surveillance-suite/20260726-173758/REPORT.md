# 视频监控 skill 测试结果

运行时间：2026-07-26 17:38:04

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

## Layer `plan`（16/17 通过）

| 用例 | 标题 | 结果 | 说明 |
| --- | --- | --- | --- |
| A1 | 单视频事件请求 -> single-video-event-analysis 且要求人工复核 | pass | {"capability": "single_video_event_understanding", "recommended_skill_chain": ["single-video-event-analysis"], "requires_human_review": true} |
| A2 | 交警调阅请求 -> video-search，不误入事件分析 | pass | {"business_scenario": "traffic_police_video_review", "capability": "video_asset_retrieval", "recommended_skill_chain": ["video-search"]} |
| A3 | 证据包缺少视频路径/事件时间 -> 单一合并追问 | pass | {"capability": "evidence_preservation", "missing_fields": ["视频文件路径", "事件时间或片段范围"], "follow_up_question": "请问视频文件路径和事件时间或片段范围是什么？"} |
| A4 | 语义检索请求 -> video-search（仅计划，执行依赖 StreetModel） | pass | 计划正确；StreetModel/ES 未启用，检索执行不在本次范围 |
| A5 | 越界能力请求 -> capability_gap 覆盖全部 4 项越界能力 | pass | {"capability": "cross_video_investigation", "business_scenario": "generic_video_intelligence", "gaps": ["Real-time stream connection is not covered by current l |
| A5b | 真实画面质量问题仍路由到摄像头健康运维（回归） | pass | {"摄像头 CAM_008 画面模糊、有遮挡，请检查一下健康状态": "camera_health_operations", "这个路口的监控是不是黑屏了，画面质量怎么样": "camera_health_operations"} |
| A5c | 视频元数据请求 -> video-stream-ingestion，不误入事件分析 | pass | {"capability": "video_metadata_normalization", "recommended_skill_chain": ["video-stream-ingestion"]} |
| A5d | 单视频计数 -> video-object-analytics；视频库统计 -> object-statistics | pass | {"单视频计数": "object_detection", "视频库统计": "object_statistics"} |
| A6 | 不存在的本地视频 -> SOURCE_NOT_FOUND 且非零退出码 | pass | {"error_code": "SOURCE_NOT_FOUND", "retryable": false} |
| A7 | 事件去重空输入 -> 标准成功 JSON | pass | {"events": 0} |
| A8 | 同摄像头同 ROI 重复事件 -> 合并并取高置信度与时间并集 | pass | {"merged_events": 2, "duplicates": [{"main_event_id": "E1", "merged_event_id": "E2"}]} |
| A9 | ROI 几何 -> 多边形内命中、多边形外不命中 | pass | {"matches": {"T_IN": true, "T_OUT": false}} |
| A10 | 人工复核契约 -> 低置信/画面退化进 manual_review，高置信 auto_pass | pass | {"low_confidence": ["confidence_below_threshold", "high_risk_event_type", "camera_health_degraded"], "high_confidence_queue": "auto_pass"} |
| A11 | Router Card 基础字段与 ID 唯一性 | pass | {"router_cards": 117, "unique_ids": 117} |
| A12 | registry.json 与视频 bundle 路径/scope 一致（文档 7.1 回归） | pass | {"bundle_skills": 21, "registry_entries": 21} |
| A13 | 无 embedding 的 Router Card 集合边界检查 -> conflicts=[] | pass | {"conflicts": 0} |
| A14 | 在线检索类 skill 执行（依赖 StreetModel/ES） | skip | StreetModel(219.245.185.245:3130) 与 Elasticsearch(172.17.0.1:3128) 未启用，video-search / video-embedding-index / object-statistics 的在线检索路径不在本次执行范围 |
