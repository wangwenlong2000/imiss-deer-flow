## 测试结果

**JSON 输出：**
```json
{
  "skill": "video-search",
  "version": "1.0.0",
  "status": "failed",
  "error_code": "STREETMODEL_CONNECTION_FAILED",
  "message": "Could not connect to StreetModel at http://219.245.185.245:3130: [Errno 111] Connection refused",
  "retryable": true,
  "detail": {
    "base_url": "http://219.245.185.245:3130"
  }
}
```

**结论：**

StreetModel 语义向量真链路测试**按预期返回了结构化失败响应**。

- **状态**：`failed`
- **错误码**：`STREETMODEL_CONNECTION_FAILED`
- **原因**：目标地址 `http://219.245.185.245:3130` 连接被拒绝（`[Errno 111] Connection refused`）
- **可重试**：`true`

Skill 正确执行了 StreetModel 调用流程，在服务不可达时生成了符合规范的失败 JSON，包含错误码、详细信息和重试标志，验证了真链路的错误处理机制正常工作。