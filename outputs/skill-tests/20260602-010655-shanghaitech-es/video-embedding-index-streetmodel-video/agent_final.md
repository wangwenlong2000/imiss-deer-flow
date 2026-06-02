## 测试结果

**测试用例**: `video-embedding-index-streetmodel-video`

### 核心字段

| 字段 | 值 |
|------|-----|
| `skill` | `video-embedding-index` |
| `version` | `1.0.0` |
| `status` | `failed` |
| `error_code` | `VIDEO_COPY_TO_SHARED_FAILED` |
| `retryable` | `false` |

### 错误详情

- **错误类型**: 权限拒绝 (Permission denied)
- **失败原因**: 无法将视频复制到 DeerFlow 共享前缀 `/data/deerflow/videos/direct_uploads`
- **源视频路径**: `/mnt/datasets/shanghaitech-agent-eval/07_007.avi`
- **目标路径**: `/data/deerflow/videos/direct_uploads/shanghaitech-07-007-agent-test-streetmodel-882669cabd92.avi`

### 简短结论

测试**失败**。技能入口脚本正常执行，但在尝试将视频文件复制到 StreetModel GPU 可访问的共享目录时遇到权限问题。`/data/deerflow/videos/direct_uploads` 目录不存在或当前用户无写入权限。这属于部署配置问题，需要先确保共享目录存在且具有正确的写入权限，或调整 `deerflow_path_prefix` 配置。