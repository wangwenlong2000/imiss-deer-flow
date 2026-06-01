## 测试结果

**JSON 输出：**
```json
{
  "skill": "video-embedding-index",
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

StreetModel 服务不可达。连接被拒绝（Connection refused），说明目标服务器 `http://219.245.185.245:3130` 当前无法访问。

技能已按预期返回了结构化的失败 JSON，包含：
- 明确的错误码：`STREETMODEL_CONNECTION_FAILED`
- 详细的错误信息
- 标记为可重试（`retryable: true`）
- 包含失败详情（base_url）

这符合技能规范中定义的错误处理模式，验证了技能在 StreetModel 不可达时能够正确生成结构化失败响应。