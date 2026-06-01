# StreetModel 语义视频检索测试报告

## 结论

本次通过 `lead_agent` 跑了真实 StreetModel 语义链路测试，Agent 均按要求先读取 `SKILL.md`，再调用 `bash` 执行 skill 入口，并读取 `result.json`。

结果：**未跑通真实视频语义检索**。失败点不是 ES，也不是 skill 调用链，而是 StreetModel 服务 `http://219.245.185.245:3130` 当前不可达。

## 测试结果

| Case | Skill | Agent 调用链 | 结果 | 错误 |
| --- | --- | --- | --- | --- |
| `streetmodel-video-embedding` | `video-embedding-index` | `read_file -> bash -> read_file` | failed | `STREETMODEL_CONNECTION_FAILED` |
| `streetmodel-semantic-search` | `video-search` | `read_file -> bash -> read_file` | failed | `STREETMODEL_CONNECTION_FAILED` |

结构化错误：

```json
{
  "status": "failed",
  "error_code": "STREETMODEL_CONNECTION_FAILED",
  "message": "Could not connect to StreetModel at http://219.245.185.245:3130: [Errno 111] Connection refused",
  "retryable": true
}
```

## 网络探测

本机直连：

```text
curl --noproxy '*' http://219.245.185.245:3130/health
connect to 219.245.185.245 port 3130 failed: Connection refused
```

通过当前 socks 代理访问 HTTP：

```text
curl http://219.245.185.245:3130/health
Empty reply from server
```

通过当前 socks 代理访问 HTTPS：

```text
curl -k https://219.245.185.245:3130/health
unexpected eof while reading
```

## ES 检查

真实 StreetModel 视频向量字段当前计数：

```json
{
  "count": 0
}
```

检查字段：

```text
video_vector-Qwen3-VL-Embedding-2B_urban_governance
```

说明：目前 `huangxiao-video-library-vector-v1` 中没有真实 `Qwen3-VL-Embedding-2B` 视频向量；之前可用的是 deterministic-hash 测试向量，不代表真实语义检索。

## 执行命令

视频向量入库测试：

```bash
python /mnt/skills/custom/video-embedding-index/scripts/run.py \
  --source-index citybrain-video-library \
  --target-index huangxiao-video-library-vector-v1 \
  --owner huangxiao \
  --limit 1 \
  --embedding-provider streetmodel \
  --base-url http://219.245.185.245:3130 \
  --timeout-seconds 120 \
  --config /mnt/skills/custom/configs/deerflow_config.json \
  --output /mnt/user-data/outputs/streetmodel-video-embedding/result.json
```

语义检索测试：

```bash
python /mnt/skills/custom/video-search/scripts/run.py \
  --query "路口有很多车辆经过" \
  --index huangxiao-video-library-vector-v1 \
  --embedding-provider streetmodel \
  --vector-query-mode semantic \
  --top-k 3 \
  --base-url http://219.245.185.245:3130 \
  --timeout-seconds 120 \
  --config /mnt/skills/custom/configs/deerflow_config.json \
  --output /mnt/user-data/outputs/streetmodel-video-search/result.json
```

## 下一步

需要先恢复或开放 StreetModel 服务：

- `http://219.245.185.245:3130/health` 能返回 HTTP 200。
- `http://219.245.185.245:3130/models` 能返回 `Qwen3-VL-Embedding-2B`。
- DeerFlow sandbox 容器能访问该地址。
- 视频路径需要能被 StreetModel 节点读取；目前本仓库只有 `datasets/Vedio-demo/Trafic.mp4`，未发现本机挂载的 `/nfsdat2/home/xhuangslm/shared_videos` 视频文件。

服务恢复后，再重跑本目录中的同类 Agent 测试即可验证真实语义闭环。
