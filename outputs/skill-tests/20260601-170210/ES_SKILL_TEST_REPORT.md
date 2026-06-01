# DeerFlow Agent 调用 ES Skill 检测报告

## 结论

本次按 `docs/deerflow-agent-skill-test.md` 的方式，通过 LangGraph `lead_agent` 调用 skill 完成 ES 相关真链路测试。主测试覆盖 `batch-video-ingestion`、`video-search`、`video-embedding-index`、`object-statistics`，4 项全部通过。

验证链路为：

```text
LangGraph API -> lead_agent -> read_file -> bash -> AioSandbox Docker -> skill scripts -> Elasticsearch
```

测试中确认 Agent 先读取对应 `SKILL.md`，再调用 `bash` 执行 skill 入口脚本，并生成 `result.json`。

## 测试环境

- 测试时间：`2026-06-01 16:53:33 +0800` 至 `2026-06-01 17:03:33 +0800`
- LangGraph API：`http://localhost:3230/api/langgraph`
- Assistant：`lead_agent`
- Elasticsearch：`http://localhost:3128`
- ES 用户：`citybrain-street`
- 测试视频：`/mnt/datasets/Vedio-demo/Trafic.mp4`
- skill 配置：`/mnt/skills/custom/configs/deerflow_config.json`
- 主测试目录：`outputs/skill-tests/20260601-165333`
- timeout 修复后复测目录：`outputs/skill-tests/20260601-165957`
- 修复后回归目录：`outputs/skill-tests/20260601-170210`

## 主测试结果

| Skill | 结果 | Thread | Run | 关键输出 |
| --- | --- | --- | --- | --- |
| `batch-video-ingestion` | 通过 | `6b3361b6-dd4e-420b-8335-e9f62b025e8c` | `019e8263-6958-7362-8499-8e693b841bdd` | 入库 `1` 条，失败 `0` 条，写入 `citybrain-video-library` |
| `video-search` | 通过 | `a5ed59e3-584f-4da2-95fe-eead83528ff7` | `019e8263-fea0-74d3-9e4c-bee5ee50fa2e` | 命中 `2` 条，`query_mode=keyword_filter` |
| `video-embedding-index` | 通过 | `8171a60b-9a38-4f6f-bfe3-5c174a9fa182` | `019e8264-8b62-74f3-a886-c0d5b458d31c` | 写入个人向量索引 `3` 条，失败 `0` 条 |
| `object-statistics` | 通过 | `1823af36-dc52-40c3-8044-651961d0b31c` | `019e8265-0d0a-7391-af5a-1ba7b7e69c64` | 匹配 `3` 个视频，统计 `54` 个目标 |

关键业务结果：

- `batch-video-ingestion` 将 `video-traffic-agent-test` 写入 `citybrain-video-library`，检测标签包含 `bus`、`car`。
- `video-search` 以 `Trafic + CAM_DEERFLOW_001 + labels` 检索到 `video-traffic-agent-test` 和 `video-traffic-skill-smoke`。
- `video-embedding-index` 使用 `deterministic-hash` provider 写入 `huangxiao-video-library-vector-v1`，向量维度 `1024`。
- `object-statistics` 从 ES 聚合得到 `bus=9`、`car=45`，`moving=12`、`stationary=14`。

## ES 状态

- `_cluster/health`：`yellow`
- `citybrain-video-library`：`docs.count=3`，`number_of_replicas=1`
- `huangxiao-video-library-vector-v1`：`docs.count=3`，状态 `green`

`citybrain-video-library` 为单节点 ES 上的共享索引，副本数为 `1` 会导致 unassigned replica，因此集群显示 `yellow`。本次未直接修改共享索引副本配置，避免影响公共环境；该状态不影响本次读写、检索和统计测试。

## 问题与修复

### 1. 两个 ES skill 首次 bash 调用出现包装错误

现象：

```text
Command failed: 'ErrorObservation' object has no attribute 'exit_code'
```

影响范围：

- 初始主测试中 `video-search`、`video-embedding-index` 第一次 bash 调用出现该信息。
- Agent 后续自行复查并用单行命令重跑成功，最终业务结果为通过。

处理：

- 在 `backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox.py` 中给 AioSandbox `shell.exec_command` 显式传入 `timeout=600`，作为长命令防御。
- 在 `outputs/skill-tests/run_agent_skill_tests.py` 中将简单 ES 命令由两行 `mkdir` + `python` 调整为单行 `mkdir -p ... && python ...`，避免触发该 sandbox 包装问题。

验证：

- 仅增加 `timeout=600` 后的复测目录为 `outputs/skill-tests/20260601-165957`，业务通过但仍可复现 `ErrorObservation`。
- 修复后回归目录：`outputs/skill-tests/20260601-170210`
- 回归覆盖 `video-search`、`video-embedding-index`、`object-statistics`
- 结果：`3/3` 通过
- 原始 SSE 检查：未再出现 `ErrorObservation`
- 每项均为 `read_file -> bash -> read_file result.json`，bash 调用次数均为 `1`

### 2. py_compile 首次遇到 root-owned __pycache__ 权限

现象：

```text
Permission denied: backend/packages/harness/deerflow/community/aio_sandbox/__pycache__/...
```

处理：

- 使用 `PYTHONPYCACHEPREFIX=/tmp/codex-pycache python3 -m py_compile ...` 重新执行。
- 结果通过。

该问题是本地 root-owned `__pycache__` 文件权限导致，不影响应用运行。

## 验证命令

主测试：

```bash
python3 outputs/skill-tests/run_agent_skill_tests.py --skills batch-video-ingestion video-search video-embedding-index object-statistics --timeout 420
```

修复后回归：

```bash
python3 outputs/skill-tests/run_agent_skill_tests.py --skills video-search video-embedding-index object-statistics --timeout 420
```

静态检查：

```bash
PYTHONPYCACHEPREFIX=/tmp/codex-pycache python3 -m py_compile backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox.py
```

## 最终判断

ES 相关 skill 的 Agent 真链路可用。入库、检索、向量索引、统计四类能力均通过实际 LangGraph Agent 调用验证。已记录并小范围修复 bash 包装错误触发条件；共享 ES 的 `yellow` 状态保留为环境风险，不在本次任务中修改。
