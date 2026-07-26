占位示例目录 —— 禁止把这里的取值当成真实数据
================================================

本目录下所有 *.example.json 与 *.template.json 都是**占位示例**，只用于说明各 skill 的
输入/输出字段结构。文件中的摄像头编号、地点、拍摄时间、时长、分辨率、ROI 坐标、检测框和
事件候选**全部是编造的占位值**，不对应任何真实视频。

硬性要求：

  1. 不得把本目录中的任何取值写进分析报告、证据包、时间线或最终回答。
  2. 真实视频的元数据（时长、分辨率、帧率、编码、总帧数、文件大小）只能来自
     video-stream-ingestion 对该视频的实测输出。
  3. 摄像头编号、地理位置、拍摄时间属于外部业务档案。没有可靠来源时必须留空并说明缺失，
     不得用本目录的示例值填补，也不得自行推断。
  4. 事件结论只能来自 single-video-event-analysis 对真实审帧的判断，并引用 evidence_frame_ids。

每个 JSON 顶层都带有 "_example": true 和 "_example_notice" 两个字段作为机器可读标记。
读取本目录文件时，只要看到 "_example": true，就说明该文件不是真实档案。

挂载与运行说明
--------------

本 bundle 作为 DeerFlow 的 custom skills 根目录挂载：

  /mnt/skills/custom

期望的视频输入目录：

  /mnt/data/videos

期望的输出目录：

  /mnt/data/video-monitoring-runs

沙箱内的示例命令：

  python /mnt/skills/custom/video_surveillance/frame-sampling/scripts/run.py \
    --input /mnt/data/video-monitoring-runs/run_001/frame_sampling_input.json \
    --config /mnt/skills/custom/video_surveillance/configs/deerflow_config.json \
    --output /mnt/data/video-monitoring-runs/run_001/frame_sampling_result.json

所有独立的 run.py 都接受 --input JSON。命令行参数会覆盖 JSON 中的同名字段，
因此可以从一个输入文件出发，只覆盖需要改动的少数取值。
