## 测试结果

### 核心字段

| 字段 | 值 |
|------|-----|
| `skill` | video-embedding-index |
| `version` | 1.0.0 |
| `status` | failed |
| `error_code` | STREETMODEL_REQUEST_FAILED |
| `message` | StreetModel request failed: HTTP 400 |
| `retryable` | false |
| `detail.path` | /embed |
| `detail.response` | 视频文件不存在: /nfsdat2/home/xhuangslm/shared_videos/codex_unmounted_workspace/direct_uploads/shanghaitech-07-007-agent-test-streetmodel-workspace-882669cabd92.avi |

### 简短结论

测试用例 **按预期失败**。错误符合测试说明中的预期行为：

- Skill 成功触达 StreetModel `/embed` 接口（HTTP 400 响应证明连接成功）
- 失败原因是视频文件在 GPU-side 路径 `/nfsdat2/home/xhuangslm/shared_videos/codex_unmounted_workspace/...` 不存在
- 这正是测试目的：验证当 `--copy-video-to-shared` 和 `--deerflow-path-prefix` 指向可写本地前缀时，视频被复制到 DeerFlow 侧路径，但 StreetModel 无法访问该非挂载路径

**真链路验证通过**：read_file 读取 SKILL.md → bash 执行 skill 入口脚本 → 生成 result.json 包含预期错误。