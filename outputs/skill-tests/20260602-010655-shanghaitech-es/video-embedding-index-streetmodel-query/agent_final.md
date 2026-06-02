测试完成。以下是测试结果：

## 测试结果

**状态**: ✅ 成功

### 核心字段

| 字段 | 值 |
|------|-----|
| `skill` | video-embedding-index |
| `version` | 1.0.0 |
| `status` | success |
| `confidence` | 1.0 |
| `query` | 上海科技大学校园监控中有人在路面行走 |
| `embedding_provider` | streetmodel |
| `embedding_model` | Qwen3-VL-Embedding-2B |
| `dimensions` | 2048 |
| `vector_field` | video_vector-Qwen3-VL-Embedding-2B_urban_governance |

### 简短结论

`video-embedding-index` skill 的 StreetModel 文本嵌入路径真链路测试**成功**。该 skill 成功调用 StreetModel/Qwen3-VL 文本嵌入接口，将中文查询"上海科技大学校园监控中有人在路面行走"转换为 2048 维向量，输出文件已正确生成至 `/mnt/user-data/outputs/video-embedding-index-streetmodel-query/result.json`，同时查询向量也已保存至 `query_vector.json`。