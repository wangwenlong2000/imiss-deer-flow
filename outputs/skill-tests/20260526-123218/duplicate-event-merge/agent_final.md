## 测试结果

**成功判定：✅ 通过**

### `/mnt/user-data/outputs/duplicate-event-merge/result.json` 内容：

```json
{
  "skill": "duplicate-event-merge",
  "version": "1.0.0",
  "status": "success",
  "confidence": 1.0,
  "data": {
    "events": [
      {
        "event_id": "EVT_DUP_001",
        "event_type": "traffic_congestion",
        "camera_id": "CAM_DEERFLOW_001",
        "start_time": "2026-05-20T10:00:01+08:00",
        "end_time": "2026-05-20T10:00:08+08:00",
        "confidence": 0.8,
        "roi_id": "ROI_FULL_FRAME",
        "evidence_frame_ids": [
          "frame_001"
        ],
        "merged_event_ids": [
          "EVT_DUP_002"
        ]
      }
    ],
    "duplicates": [
      {
        "main_event_id": "EVT_DUP_001",
        "merged_event_id": "EVT_DUP_002"
      }
    ]
  }
}
```

### 简短结论

`duplicate-event-merge` skill 真链路测试**成功**。输入两个重复的交通拥堵事件（同一相机、同一 ROI、时间重叠），skill 正确地将它们合并为一个 canonical 事件：
- 保留 `EVT_DUP_001` 为主事件
- 合并 `EVT_DUP_002` 并记录到 `merged_event_ids`
- 时间范围扩展为 `10:00:01` 至 `10:00:08`（覆盖两个事件的并集）
- 置信度取最高值 `0.8`
- 生成 `duplicates` 映射记录合并关系