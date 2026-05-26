## 测试结果

**✅ 测试成功**

`/mnt/user-data/outputs/evidence-snapshot/result.json` 内容：

```json
{
  "skill": "evidence-snapshot",
  "version": "1.0.0",
  "status": "success",
  "confidence": 1.0,
  "data": {
    "evidence_id": "EVD_29adcf02",
    "event_id": "EVT_TEST_001",
    "type": "snapshot",
    "uri": "/mnt/user-data/outputs/evidence-snapshot/artifacts/evidence/EVT_TEST_001_snapshot.jpg",
    "hash": "sha256:c1b4abc8ee0eed50b30b2fc0189e714b8ac0e39679098f67523610b9863f50d4",
    "created_at": "2026-05-26T04:37:54.819736+00:00",
    "privacy_masked": false
  }
}
```

**结论：**
- ✅ `frame-sampling` skill 成功从视频提取了 3 帧（0s, 1s, 2s）
- ✅ `evidence-snapshot` skill 成功生成了证据快照，包含完整的 Evidence schema：
  - `evidence_id`: EVD_29adcf02
  - `event_id`: EVT_TEST_001
  - `type`: snapshot
  - `uri`: 证据图片路径
  - `hash`: SHA-256 完整性校验值
  - `created_at`: 时间戳
  - `privacy_masked`: false（未启用隐私遮蔽）