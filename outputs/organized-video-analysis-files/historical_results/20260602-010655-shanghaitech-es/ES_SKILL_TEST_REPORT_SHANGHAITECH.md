# ShanghaiTech ES Skill Agent Test Report

## 结论

本次使用真实 ShanghaiTech 样例 `/home/huangxiao/City_brain/shanghaitech/shanghaitech/training/videos/07_007.avi` 测试 ES 相关 skill，所有 case 都通过 DeerFlow `lead_agent` 真链路调用：

```text
LangGraph API -> lead_agent -> read_file SKILL.md -> bash -> AioSandbox -> skill script -> ES / StreetModel
```

- 主测试目录：`outputs/skill-tests/20260602-010655-shanghaitech-es`
- 补充 StreetModel video `/embed` 测试目录：`outputs/skill-tests/20260602-011116-shanghaitech-es`
- 样例 SHA256：`6c7f67d1353ce7d6b914b4ff0809397c9c9348cab376e868a39f726f2220cba1`
- sandbox 可访问样例：`/mnt/datasets/shanghaitech-agent-eval/07_007.avi`
- ES 源索引：`huangxiao-shanghaitech-es-agent-test`
- ES 向量索引：`huangxiao-shanghaitech-vector-agent-test`

主测试 Agent 链路 `7/7` 通过，功能结果 `6/7` 成功。补充测试 Agent 链路 `1/1` 通过，功能按预期失败并确认 StreetModel `/embed` 已被触达。

## 主测试结果

| Case | Skill | Agent 链路 | 功能结果 | 关键结果 |
| --- | --- | --- | --- | --- |
| `batch-video-ingestion-shanghaitech` | `batch-video-ingestion` | 通过 | 成功 | 入库 1 条，检测到 `person`，`failed_count=0` |
| `video-search-shanghaitech` | `video-search` | 通过 | 成功 | 命中 1 条，`query_mode=keyword_filter` |
| `object-statistics-shanghaitech` | `object-statistics` | 通过 | 成功 | `total_objects=4`，`total_tracks=2`，`person=4` |
| `evidence-package-generation-shanghaitech` | `evidence-package-generation` | 通过 | 成功 | 从 ES 取回视频并生成 package manifest、snapshot、clip |
| `video-embedding-index-deterministic-shanghaitech` | `video-embedding-index` | 通过 | 成功 | 写入 1 条 1024 维 `deterministic-hash` 向量 |
| `video-embedding-index-streetmodel-query` | `video-embedding-index` | 通过 | 成功 | `Qwen3-VL-Embedding-2B` 文本 query embedding 成功，2048 维 |
| `video-embedding-index-streetmodel-video` | `video-embedding-index` | 通过 | 失败 | `VIDEO_COPY_TO_SHARED_FAILED`，无法写 `/data/deerflow/videos/direct_uploads` |

## Embedding 重点记录

### 1. StreetModel 文本 embedding 成功

`video-embedding-index-streetmodel-query` 通过 Agent 调用 `video-embedding-index`，使用：

```bash
--embedding-provider streetmodel
--query 上海科技大学校园监控中有人在路面行走
```

结果：

- `status=success`
- `embedding_provider=streetmodel`
- `embedding_model=Qwen3-VL-Embedding-2B`
- `dimensions=2048`
- `vector_field=video_vector-Qwen3-VL-Embedding-2B_urban_governance`

说明 StreetModel 服务、模型发现、文本 `/embed` 调用和 2048 维 shape 校验均通过。

### 2. 默认视频 embedding 复制阶段失败

`video-embedding-index-streetmodel-video` 使用真实样例：

```bash
--video-uri /mnt/datasets/shanghaitech-agent-eval/07_007.avi
--copy-video-to-shared
```

结果：

```json
{
  "status": "failed",
  "error_code": "VIDEO_COPY_TO_SHARED_FAILED",
  "message": "Failed to copy video to DeerFlow shared prefix /data/deerflow/videos: [Errno 13] Permission denied: '/data/deerflow/videos/direct_uploads'"
}
```

问题判断：sandbox 用户无法在默认 DeerFlow 共享前缀 `/data/deerflow/videos` 下创建 `direct_uploads`。这会阻止真实本地视频进入 StreetModel video embedding 流程。

### 3. 可写前缀变体确认 StreetModel `/embed` 可触达，但 GPU 侧路径不可见

补充 case `video-embedding-index-streetmodel-video-workspace-prefix` 将 `deerflow_path_prefix` 改到可写 workspace，强制走到 StreetModel `/embed`：

```bash
--deerflow-path-prefix /mnt/user-data/workspace/streetmodel_shared
--streetmodel-path-prefix /nfsdat2/home/xhuangslm/shared_videos/codex_unmounted_workspace
```

结果：

```json
{
  "status": "failed",
  "error_code": "STREETMODEL_REQUEST_FAILED",
  "message": "StreetModel request failed: HTTP 400",
  "detail": {
    "path": "/embed",
    "response": "{\"detail\":\"视频文件不存在: /nfsdat2/home/xhuangslm/shared_videos/codex_unmounted_workspace/direct_uploads/shanghaitech-07-007-agent-test-streetmodel-workspace-882669cabd92.avi\"}"
  }
}
```

问题判断：Agent 和 skill 已成功触达 StreetModel `/embed` 的 video 分支，但 StreetModel/GPU 节点看不到 sandbox workspace 里的复制文件。真实视频 embedding 需要一个 DeerFlow sandbox 和 StreetModel Server 同时可访问的共享挂载，或提前把样例放到 `/nfsdat2/home/xhuangslm/shared_videos/...`。

## ES 状态

测试后索引状态：

```text
huangxiao-shanghaitech-es-agent-test      docs.count=1  status=yellow
huangxiao-shanghaitech-vector-agent-test  docs.count=1  status=green
```

`huangxiao-shanghaitech-es-agent-test` 为单节点 ES 上默认 `replicas=1` 的测试索引，因此显示 `yellow`，不影响本次读写与检索。

## 其他观察

- `batch-video-ingestion` 的 Agent 首次 `read_file result.json` 偶发 404，随后 Agent 自行 `ls` 确认文件存在并重读成功；最终 artifact 中 result 完整。该现象更像 sandbox 文件可见性/时序问题，未影响结果。
- 所有 case 的 `tool_calls.json` 均能看到先 `read_file /mnt/skills/custom/<skill>/SKILL.md`，再 `bash` 执行对应 skill 脚本入口。

## 产物

- 主汇总：`outputs/skill-tests/20260602-010655-shanghaitech-es/SUMMARY.md`
- 主 manifest：`outputs/skill-tests/20260602-010655-shanghaitech-es/manifest.json`
- 补充 StreetModel video `/embed` 汇总：`outputs/skill-tests/20260602-011116-shanghaitech-es/SUMMARY.md`
- 测试驱动：`outputs/skill-tests/run_shanghaitech_es_agent_tests.py`
