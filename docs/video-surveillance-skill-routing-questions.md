# 视频监控 Skill 路由测试问题

以下题干均从普通用户角度编写，不直接指定 skill。实际测试时只发送“用户问题”，括号中的内容仅用于核对预期路由。

1. 用户问题：请分析这段监控视频，找出其中的关键场景和异常事件，按时间线还原事件经过，并生成一份复核报告。  
   预期路由：`analyze-video`

2. 用户问题：请检查这批摄像头最近的视频质量，找出黑屏、花屏、模糊、过曝、断流和时间戳异常的设备。  
   预期路由：`camera-health-check`

3. 用户问题：这批摄像头上报了很多相似告警，请根据时间、地点、目标和证据相似度合并重复事件，并保留合并依据。  
   预期路由：`duplicate-event-merge`

4. 用户问题：请从这段视频中截取能证明车辆闯入限制区域的关键画面，保留时间戳、摄像头编号和原始文件信息，导出证据图片。  
   预期路由：`evidence-snapshot`

5. 用户问题：请把这段视频转换成统一格式，提取关键帧，并截取 00:12 到 00:28 的片段保存下来。  
   预期路由：`ffmpeg-utils`

6. 用户问题：请按每 2 秒从视频中抽取一帧，并为每帧生成包含时间戳、来源和图像尺寸的结构化记录。  
   预期路由：`frame-sampling`

7. 用户问题：请根据告警置信度、证据完整性和潜在影响，把事件分成自动通过、需要人工复核和优先复核三类。  
   预期路由：`human-review-routing`

8. 用户问题：请识别视频里的人员和车辆，持续记录它们的位置和运动轨迹，并输出对象类别、时间和轨迹数据。  
   预期路由：`video-object-analytics`

9. 用户问题：请检测视频每一帧中的人、车、自行车和摩托车，返回每个目标的边界框、类别和置信度。  
   预期路由：`object-detection`

10. 用户问题：请持续跟踪画面中的每辆车，生成它们的进入时间、离开时间、运动轨迹和行驶方向。  
    预期路由：`object-tracking`

11. 用户问题：请对视频中的人脸、车牌和其他敏感区域进行模糊处理，输出一份可以公开使用的脱敏视频。  
    预期路由：`privacy-masking`

12. 用户问题：请在画面中标出门口和停车区两个区域，并分别统计进入、离开和停留的目标数量。  
    预期路由：`roi-mapping`

13. 用户问题：请从 8 小时监控录像中提取包含异常行为的多个短视频片段，每段保留前后缓冲时间并附上事件说明。  
    预期路由：`video-segment-extraction`

14. 用户问题：请接入这组 RTSP 摄像头地址，规范化视频源信息，记录摄像头会话信息，并报告无法连接或频繁断开的源。  
    预期路由：`video-stream-ingestion`

## 测试建议

- 每次只发送一条题干，不要把括号中的 skill 名称一起发送。
- 先用没有上传文件的题目测试意图路由，再补充视频、图片或 RTSP 数据测试实际执行。
- `object-detection`、`object-tracking` 和 `video-object-analytics` 的问题容易相互混淆，建议分别单独测试。

## 变更记录

2026-07-26：移除 6 条预期路由指向已删除 skill 的题目（`density-aggregation-event`、`event-rule-engine`、
`event-template-mapping`、`object-composition-event`、`spatial-occupancy-event`、`temporal-persistence-event`）。
这些按密度、规则、目标组合、ROI 占用和时间持续性直接判定业务事件的旧 skill 已不在可执行 bundle 中，
新架构下事件语义统一由 `city-video-intelligence` -> `single-video-event-analysis` 承担，
详见 [`deerflow-agent-skill-test.md`](deerflow-agent-skill-test.md) 1.2 节。
