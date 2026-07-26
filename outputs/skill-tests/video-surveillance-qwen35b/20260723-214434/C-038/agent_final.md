<system_reminder>
Use the following current-turn intent and routing guidance when deciding whether to create or update todos. Treat it as planning context, not as a user request.
<hidden_step source="intent_recognition" title="意图识别">
改写后的任务：场景：视频监控。场景ID：video_surveillance
识别场景：视频监控 (video_surveillance)；场景模式：single；候选场景：video_surveillance
场景任务：
- video_surveillance: 事件置信度低、摄像头画面有遮挡或事件涉及执法时，哪些结果必须进入人工复核？；参数：{'视频对象': '', '监控目标': '', '时间范围': '', '输出形式': ''}
已提取参数：{'视频对象': '', '监控目标': '', '时间范围': '', '输出形式': ''}
任务提示：
- 提取必填参数：视频对象。需要分析的监控视频、摄像头、视频流、录像文件、实时画面、卡口视频或视频帧
- 提取可选参数：监控目标。需要识别或分析的对象、行为或事件，如人员聚集、车辆违停、越界、烟火、异常停留、摔倒、入侵、打架、遗留物、通行计数等
- 提取可选参数：时间范围。视频分析的起止时间、日期、时段、时刻或实时窗口
- 提取可选参数：输出形式。用户期望的输出，如告警、截图、时间点清单、检测结果、表格、报告、标注说明或风险摘要
</hidden_step>
</system_reminder>