# 网络流量 RAG 实施方案

## 1. 方案目标

本方案用于在现有网络流量分析技能基础上，增加一套可持续扩展的 RAG 能力，使系统既能对预处理后的流量数据进行统计分析和异常分析，也能基于索引做检索增强回答。

当前目标优先支持以下问题：

- 这份流量主要是什么类型的通信
- 有没有明显的 DNS、TLS 或 HTTP 特征
- 有没有很多异常短连接
- 有没有可疑扫描源
- 有没有明显的异常流量高峰

本方案不把 RAG 单独拆成新的 skill，而是继续挂在现有 `network-traffic-analysis` skill 下，作为分析能力的一条增强链路。

## 2. 总体链路

完整流程如下：

1. 原始 `pcap` 通过 `prepare_pcap.py` 预处理，生成：
   - `packet.csv`
   - `flow.csv`
   - `metadata.json`
2. 基于 `flow.csv` 运行 `build_rag_docs.py`，生成：
   - `rag_docs.jsonl`
   - `rag_manifest.json`
3. 基于 `rag_docs.jsonl` 运行 `embed_rag_docs.py`，生成：
   - `rag_embeddings.jsonl`
   - `embedding_manifest.json`
4. 基于 `rag_embeddings.jsonl` 运行 `index_rag_docs.py`，写入 Elasticsearch，并生成：
   - `index_manifest.json`
5. 后续再增加检索脚本后，可在同一索引上进行 RAG 检索，并结合 `analyze.py` 输出最终回答

## 3. 预处理与目录组织

### 3.1 原始数据目录

原始网络流量建议放在：

`datasets/network-traffic/raw/`

推荐在 `raw` 下按数据集建立子目录，例如：

- `datasets/network-traffic/raw/USTC-TFC2016/`
- `datasets/network-traffic/raw/CICIDS2017/`
- `datasets/network-traffic/raw/MyDataset/`

### 3.2 处理结果目录

当前推荐的处理方式是：

- 每个 `pcap` 单独预处理
- 每个 `pcap` 对应一个独立的 `processed/<dataset_name>/` 目录

例如：

- `datasets/network-traffic/processed/BitTorrent/`
- `datasets/network-traffic/processed/FTP/`

单个目录下通常包含：

- `<dataset_name>.packet.csv`
- `<dataset_name>.flow.csv`
- `metadata.json`
- `rag/rag_docs.jsonl`
- `rag/rag_manifest.json`
- `rag/rag_embeddings.jsonl`
- `rag/embedding_manifest.json`
- `rag/index_manifest.json`

### 3.3 metadata 与文件来源

当前链路会尽量保留文件来源信息：

- `packet.csv` 中会保留 `pcap_name`
- `packet.csv` 和 `flow.csv` 中会保留 `source_file`
- `metadata.json` 中会保留 `source_files`
- 构建出的 RAG 文档也会保留：
  - `dataset_name`
  - `source_file`

这样做的目的，是为了后续在统一索引中仍然可以：

- 针对单个 `pcap` 提问
- 针对多个 `pcap` 联合提问

## 4. RAG 文档设计

### 4.1 文档格式

第一版统一使用 JSONL 作为中间文档格式，每条文档结构如下：

```json
{
  "doc_id": "string",
  "dataset_name": "string",
  "source_file": "string",
  "doc_type": "flow_summary | protocol_summary | anomaly_summary",
  "title": "string",
  "content": "string",
  "summary": "string",
  "keywords": ["string"],
  "metadata": {
    "protocol": "string",
    "app_protocol": "string",
    "traffic_family": "string",
    "src_ip": "string",
    "dst_ip": "string",
    "dst_port": 0,
    "time_bucket": "string",
    "risk_level": "low | medium | high",
    "tags": ["string"]
  }
}
```

字段说明：

- `content`：主要用于生成 embedding
- `summary`：短摘要，便于调试和展示
- `metadata`：用于 Elasticsearch 过滤检索
- `keywords`：用于补充关键词召回
- `doc_id`：文档唯一标识，用于避免重复入库和支持稳定更新

### 4.2 第一版文档类型

第一版仅生成三类文档：

#### 1. `flow_summary`

每条双向会话生成一条文档，适合回答：

- 这条通信在做什么
- 主要协议和应用提示是什么
- 会话状态如何

主要字段来源：

- `src_ip / dst_ip`
- `src_port / dst_port`
- `protocol / app_protocol`
- `bytes / packets`
- `duration_ms`
- `session_state`
- `tcp_flags_seen`
- `dns_query / tls_sni / http_host`

#### 2. `protocol_summary`

按协议特征聚合生成文档，第一版至少覆盖：

- DNS 视图：按 `dns_query`
- TLS 视图：按 `tls_sni`
- HTTP 视图：按 `http_host`

适合回答：

- 有没有明显 DNS、TLS、HTTP 特征
- 最常见的域名、SNI、Host 是什么

#### 3. `anomaly_summary`

按异常主题生成文档，第一版先做：

- 短连接摘要
- 扫描源摘要
- 流量高峰摘要

适合回答：

- 有没有很多异常短连接
- 有没有可疑扫描源
- 有没有异常流量高峰

## 5. 索引策略

### 5.1 为什么不直接向量化整个 flow.csv

不建议把所有 flow 按一个统一模板直接转成自然语言后入库，原因如下：

- 网络流量问题高度结构化
- 不同问题关注点差异很大
- 统一模板会降低语义区分度
- 纯模板化后，向量检索准确率容易下降

因此当前采用的是：

- 先将结构化流量转成不同用途的检索文档
- 再对这些文档生成 embedding

### 5.2 为什么采用统一索引

当前推荐的策略不是“每个 pcap 一个 Elasticsearch 索引”，而是：

- 每个 `pcap` 单独预处理
- 每个 `pcap` 单独生成文档
- 统一写入一个共享索引

默认索引名：

`network-traffic-rag`

这样既可以：

- 按 `source_file` 或 `dataset_name` 过滤，回答单个 `pcap` 的问题
- 也可以跨多个文件或全局范围做联合检索

### 5.3 后续追加新数据

后续新增数据集时，可以继续使用同一套流程追加写入原索引。

当前总脚本会根据：

- `processed/<dataset_name>/rag/index_manifest.json`

判断某个数据集是否已经处理过。

因此：

- 新数据集会自动处理
- 已处理过的数据集默认跳过
- 如需强制重建，可使用 `--rebuild-existing`

注意：

- 当前“是否已处理”的判断依据是 `dataset_name`
- 如果文件内容变了但文件名没变，默认仍会被视为已处理过

## 6. embedding 配置

### 6.1 配置位置

embedding 配置已接入项目根目录的 `config.yaml`。

示例：

```yaml
embedding:
  provider: dashscope
  model: text-embedding-v3
  api_key: $DASHSCOPE_API_KEY
  base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
  dimensions:
```

### 6.2 当前建议

当前建议：

- `provider` 使用 `dashscope`
- `model` 使用 `text-embedding-v3`
- `dimensions` 先留空，走模型默认维度

原因：

- 第一版优先保证链路稳定
- 后续如果要做维度压缩，再统一修改 Elasticsearch mapping 和 embedding 参数

### 6.3 .env 自动读取

为避免单独运行脚本时手动 `source .env`，当前以下脚本会自动读取项目根目录 `.env`：

- `embed_rag_docs.py`
- `build_full_rag_index.py`

规则如下：

- 只在当前环境变量中没有该 key 时，才从 `.env` 注入
- 不会覆盖已经手动设置的环境变量

## 7. Elasticsearch 部署

### 7.1 当前部署方式

第一版采用服务器本机单节点部署 Elasticsearch。

部署目标：

- 先跑通向量入库与检索链路
- 暂不引入复杂的安全认证和多节点集群配置

### 7.2 服务器配置建议

在 `elasticsearch.yml` 中建议使用：

```yaml
cluster.name: network-traffic-rag
node.name: node-1

network.host: 127.0.0.1
http.port: 9200

discovery.type: single-node

path.data: /nfsdat1/home/wlwangslm/elasticsearch-data
path.logs: /nfsdat1/home/wlwangslm/elasticsearch-logs

xpack.security.enabled: false
xpack.security.autoconfiguration.enabled: false
```

说明：

- `network.host: 127.0.0.1` 表示只允许本机访问
- `http.port: 9200` 是项目配置中要连接的端口
- `discovery.type: single-node` 用于单节点启动
- 第一版关闭安全配置，便于脚本直接连通

### 7.3 启动与验证

启动后可通过以下命令检查：

```bash
curl http://127.0.0.1:9200
```

如果返回集群 JSON 信息，说明 Elasticsearch 已正常运行。

## 8. Elasticsearch 项目配置

`config.yaml` 中建议配置：

```yaml
elasticsearch:
  hosts:
    - http://127.0.0.1:9200
  index_name: network-traffic-rag
  api_key:
  username:
  password:
  verify_certs: true
  request_timeout: 30
```

第一版通常不需要：

- `api_key`
- `username`
- `password`

如后续启用 Elasticsearch 安全认证，再补充这些字段。

## 9. 已实现脚本

当前已经实现以下脚本：

- `prepare_pcap.py`
- `build_rag_docs.py`
- `embed_rag_docs.py`
- `index_rag_docs.py`
- `build_full_rag_index.py`

### 9.1 `build_rag_docs.py`

作用：

- 从 `flow.csv` 构建三类 RAG 文档

输出：

- `rag_docs.jsonl`
- `rag_manifest.json`

### 9.2 `embed_rag_docs.py`

作用：

- 读取 `rag_docs.jsonl`
- 调用 embedding 模型生成向量

输出：

- `rag_embeddings.jsonl`
- `embedding_manifest.json`

当前针对 DashScope 做了兼容：

- 默认 `batch_size = 10`

原因：

- 当前接口单次批量输入不能超过 10 条

### 9.3 `index_rag_docs.py`

作用：

- 读取 `rag_embeddings.jsonl`
- 自动创建或检查 Elasticsearch 索引
- 写入向量文档

输出：

- `index_manifest.json`

### 9.4 `build_full_rag_index.py`

作用：

- 扫描原始 `pcap`
- 逐个 `pcap` 执行完整链路
- 将所有结果统一写入一个 Elasticsearch 索引

默认行为：

- 每个 `pcap` 单独处理
- 已存在 `index_manifest.json` 的数据集默认跳过
- 支持 `--rebuild-existing` 强制重建

## 10. 后端执行命令

### 10.1 先用少量数据做快速测试

第一次验证整条链路时，建议不要一上来就扫描整个 `raw` 目录，而是先准备一个只包含少量 `pcap` 的测试目录。这样有两个好处：

- 构建索引更快，便于快速发现脚本或配置问题
- 测试成功后，后续仍可继续使用原来的全量命令做增量追加

建议先建立一个小规模测试目录，例如：

```bash
mkdir -p ./datasets/network-traffic/raw-smoke
cp ./datasets/network-traffic/raw/USTC-TFC2016/Benign/BitTorrent.pcap ./datasets/network-traffic/raw-smoke/
cp ./datasets/network-traffic/raw/USTC-TFC2016/Benign/FTP.pcap ./datasets/network-traffic/raw-smoke/
```

然后先运行：

```bash
cd /nfsdat1/home/wlwangslm/imiss-deer-flow-main
python ./skills/custom/network-traffic-analysis/scripts/build_full_rag_index.py --raw-dir ./datasets/network-traffic/raw-smoke --index-name network-traffic-rag --format text --verbose
```

如果你只想先测单个文件，也可以进一步缩小范围，只保留一个 `pcap` 再执行同样的命令。

### 10.2 单步执行

如果你希望逐步检查每一段链路，也可以使用单步命令。

#### 1. 构建 RAG 文档

```bash
python ./skills/custom/network-traffic-analysis/scripts/build_rag_docs.py --files ./datasets/network-traffic/processed/codex-expanded-smoke/codex-expanded-smoke.flow.csv --format json
```

#### 2. 生成 embedding

```bash
python ./skills/custom/network-traffic-analysis/scripts/embed_rag_docs.py --files ./datasets/network-traffic/processed/codex-expanded-smoke/rag/rag_docs.jsonl --format json
```

#### 3. 写入 Elasticsearch

```bash
python ./skills/custom/network-traffic-analysis/scripts/index_rag_docs.py --files ./datasets/network-traffic/processed/codex-expanded-smoke/rag/rag_embeddings.jsonl --format json
```

### 10.3 全量构建与后续增量追加

当小规模测试确认无误后，再使用原始全量命令扫描 `raw` 目录：

```bash
cd /nfsdat1/home/wlwangslm/imiss-deer-flow-main
python ./skills/custom/network-traffic-analysis/scripts/build_full_rag_index.py --raw-dir ./datasets/network-traffic/raw --index-name network-traffic-rag --format text --verbose
```

这条命令当前的默认行为是：

- 每个 `pcap` 单独处理
- 已经完成索引的数据集会跳过
- 只处理还没有 `index_manifest.json` 的新数据集
- 新数据会继续追加到已经构建好的统一索引 `network-traffic-rag`

因此推荐的工作流是：

1. 先用 `raw-smoke` 或少量样本目录做测试
2. 测试成功后，再跑 `raw` 全量目录
3. 后续有新数据集加入 `raw` 后，继续复用同一条全量命令
4. 脚本会自动跳过已处理数据，只把新数据加进原索引

### 10.4 强制重建

如需强制重建已处理数据集，可使用：

```bash
cd /nfsdat1/home/wlwangslm/imiss-deer-flow-main
python ./skills/custom/network-traffic-analysis/scripts/build_full_rag_index.py --raw-dir ./datasets/network-traffic/raw --index-name network-traffic-rag --rebuild-existing --format text --verbose
```

## 11. 索引验证方法

### 11.1 查看索引是否创建成功

```bash
curl http://127.0.0.1:9200/_cat/indices?v
```

### 11.2 查看文档总数

```bash
curl http://127.0.0.1:9200/network-traffic-rag/_count
```

### 11.3 查看样本文档

```bash
curl http://127.0.0.1:9200/network-traffic-rag/_search?pretty
```

### 11.4 查看 mapping

```bash
curl http://127.0.0.1:9200/network-traffic-rag/_mapping?pretty
```

### 11.5 对照检查

建议同时对照：

- `rag_embeddings.jsonl`
- `index_manifest.json`
- Elasticsearch `_count`

确保：

- 入库文档数与期望一致
- 没有重复写入或漏写

## 12. 前端提问策略

### 12.1 核心原则

想让 RAG 效果好，问题应尽量包含以下四部分：

- 对象
- 步骤
- 关注点
- 输出目标

推荐提问结构：

`对象 + workflow + focus + output goal`

例如：

- 针对哪个文件或数据集
- 是只检索，还是先检索再分析
- 重点关注 DNS、TLS、HTTP、短连接、扫描源还是流量高峰
- 最后想要摘要、TopN、对比还是结论

### 12.2 适合当前索引的主要问题类型

当前最适合的问题包括：

- 通信类型判断
- DNS、TLS、HTTP 特征检索
- 异常短连接检索
- 可疑扫描源检索
- 流量峰值检索
- 单个 pcap 的通信画像
- 多个 pcap 的横向比较

## 13. 前端提问示例

### 13.1 单纯使用 RAG 获取数据并回答

适合场景：

- 索引已经构建完成
- 希望直接从索引中检索和总结

示例：

- 从已经构建好的网络流量索引中，检索 `BitTorrent.pcap` 的主要通信类型，并总结是否有明显的 DNS、TLS 或 HTTP 特征。
- 基于当前网络流量 RAG 索引，查找 `FTP.pcap` 中与异常短连接相关的摘要，并归纳最可疑的源 IP。
- 从统一索引中比较 `FTP.pcap` 和 `BitTorrent.pcap` 的协议特征差异，重点说明常见端口、DNS/TLS/HTTP 痕迹和可疑行为摘要。

### 13.2 单纯使用分析脚本，对预处理后的数据回答

适合场景：

- 已经有 `flow.csv`
- 不需要依赖检索

示例：

- 读取 `BitTorrent.flow.csv`，分析这份流量主要是什么类型的通信，是否有明显的 DNS、TLS 或 HTTP 特征。
- 对 `FTP.flow.csv` 做会话分析，看看有没有很多异常短连接，并筛一下可疑扫描源。
- 基于 `BitTorrent.flow.csv` 做时间序列分析，看看有没有明显的异常流量高峰，并给出主要贡献协议。

### 13.3 先预处理 pcap，再使用 analyze.py 回答

适合场景：

- 输入是原始 `pcap`
- 还没有 `flow.csv`

示例：

- 先预处理 `BitTorrent.pcap`，生成 `flow.csv` 和 `packet.csv`，再分析这份流量的主要通信类型以及 DNS、TLS、HTTP 特征。
- 先对 `FTP.pcap` 做预处理，然后基于生成的 `flow.csv` 检查异常短连接、可疑扫描源和异常流量高峰。
- 请先把 `Shifu.pcap` 预处理成结构化流量数据，再做协议分析和异常分析，重点关注会话状态和可疑通信行为。

### 13.4 先使用 RAG 检索，再使用 analyze.py 分析

适合场景：

- 已经有索引
- 希望先从索引中召回线索，再做结构化验证

示例：

- 先从网络流量索引中检索 `BitTorrent.pcap` 的主要协议特征和异常摘要，再结合 `BitTorrent.flow.csv` 做详细统计分析，给出最终结论。
- 先用 RAG 检索 `FTP.pcap` 中与短连接和扫描源相关的摘要，再基于预处理后的 `FTP.flow.csv` 进行验证和细化分析。
- 请先从统一索引里召回 `Shifu.pcap` 相关的异常线索，再对对应的 `flow.csv` 运行分析脚本，确认是否存在明显扫描、会话异常或流量峰值。

### 13.5 全链路问题：预处理、构建索引、RAG 检索、analyze 联合回答

适合场景：

- 新数据刚加入 `raw`
- 既没有预处理结果，也没有索引

示例：

- 请对新加入 `/nfsdat1/home/wlwangslm/imiss-deer-flow-main/datasets/network-traffic/raw` 的网络流量数据先完成预处理，再构建 RAG 索引，然后检索主要协议特征，并结合 `analyze.py` 给出最终分析结论。
- 请对新增的 pcap 数据执行完整流程：预处理、生成 `flow.csv`、构建 RAG 文档和向量索引，然后检索异常摘要，并使用 `analyze.py` 对异常短连接、扫描源和流量峰值做综合分析。
- 请对新加的网络流量数据集做全流程处理：先预处理并构建统一索引，再通过 RAG 检索相关通信特征和异常线索，最后结合 `analyze.py` 输出面向安全研判的综合分析结果。

## 14. 查询与策略匹配建议

为了让后续检索效果更稳定，建议在实现 `rag_search.py` 时，先做问题意图分类，再决定优先检索哪类文档：

- `traffic-profile`：优先查 `flow_summary`
- `protocol-feature`：优先查 `protocol_summary`
- `short-connection`：优先查 `anomaly_summary`，必要时补 `flow_summary`
- `scan-source`：优先查 `anomaly_summary`
- `traffic-peak`：优先查 `anomaly_summary`

这样可以避免所有问题都做“全索引纯向量检索”，提高 query 和文档类型的匹配度。

## 15. 当前状态与下一步

当前已完成：

- RAG 文档构建脚本
- embedding 脚本
- Elasticsearch 写入脚本
- 全量批处理脚本
- `.env` 自动读取
- Elasticsearch 单节点部署

当前尚未完成：

- `rag_search.py`
- 查询改写
- metadata 过滤与向量召回组合策略
- 检索结果与 `analyze.py` 的自动编排回答

下一步建议优先实现：

1. `rag_search.py`
2. 问题意图分类
3. `doc_type` 优先级检索
4. RAG 检索结果与 `analyze.py` 联动回答

## 附录A：当前检索策略说明

当前已经实现第一版检索脚本：

- `rag_search.py`

它的检索策略不是“所有问题都对全索引做一次纯向量检索”，而是先做一个轻量的意图判断，再决定优先检索哪些文档类型。

### A.1 当前支持的检索意图

当前会根据 query 中的关键词，自动归到以下几类：

- `traffic-profile`
- `protocol-feature`
- `short-connection`
- `scan-source`
- `traffic-peak`
- `general`

### A.2 意图与文档类型匹配关系

当前默认匹配策略如下：

- `traffic-profile`
  - 优先查：`flow_summary`
  - 补充查：`anomaly_summary`
- `protocol-feature`
  - 优先查：`protocol_summary`
  - 补充查：`flow_summary`
- `short-connection`
  - 优先查：`anomaly_summary`
  - 补充查：`flow_summary`
- `scan-source`
  - 优先查：`anomaly_summary`
  - 补充查：`flow_summary`
- `traffic-peak`
  - 优先查：`anomaly_summary`
- `general`
  - 不主动限制 `doc_type`

### A.3 当前检索流程

当前 `rag_search.py` 的处理流程为：

1. 读取项目根目录 `.env`
2. 读取 `config.yaml` 中的 `embedding` 和 `elasticsearch` 配置
3. 对自然语言 query 生成查询向量
4. 根据 query 识别意图并推断优先 `doc_type`
5. 如果问题中出现了明确的文件名或手动传入了 `--dataset-name`，则增加 `dataset_name` 过滤
6. 使用：
   - `dataset_name` 过滤
   - `doc_type` 过滤
   - 向量相似度召回
   - 文本字段辅助匹配
   共同完成检索
7. 返回命中的文档摘要

### A.4 当前能力边界

当前检索脚本已经支持：

- 单个数据集检索
- 多数据集统一索引检索
- 简单 query 意图分类
- 基于 `doc_type` 的优先检索
- 向量检索与文本字段混合召回

当前还没有完成：

- 更复杂的 query 改写
- 更细粒度的 metadata 过滤组合
- 检索结果自动交给 `analyze.py` 做联合推理
- 前端自然语言问题自动路由到 RAG 工作流

### A.5 当前推荐的测试命令

如果当前索引里只有 `BitTorrent`，推荐这样测试：

```bash
cd /nfsdat1/home/wlwangslm/imiss-deer-flow-main
python ./skills/custom/network-traffic-analysis/scripts/rag_search.py --query "检索 BitTorrent.pcap 的主要通信类型，并判断是否存在明显的异常短连接或可疑扫描源" --dataset-name BitTorrent --format text
```

## 附录B：后续增强方案

当前第一版已经完成：

- `pcap` 预处理
- `flow.csv` 文档化
- embedding 生成
- Elasticsearch 入库
- 第一版 `rag_search.py` 检索

但如果要继续提升检索质量、回答质量和系统稳定性，建议后续按“摘要层 -> 检索层 -> 回答层 -> 数据层”的顺序增强。

### B.1 摘要层增强

摘要层决定了索引里到底存什么内容，是后续 RAG 效果的基础。

#### 1. 新增中层聚合文档

当前只有：

- `flow_summary`
- `protocol_summary`
- `anomaly_summary`

后续建议增加：

- `endpoint_summary`
  - 按主机聚合
  - 描述某个 IP 的主要通信对象、主要端口、总流数、总字节数、异常占比
- `port_summary`
  - 按端口聚合
  - 描述某个端口的活跃度、主要协议、主要主机和风险特征
- `host_pair_summary`
  - 按通信对聚合
  - 适合回答“两台主机之间主要在做什么”

这些文档可以弥补：

- 单条 `flow_summary` 太碎
- `anomaly_summary` 太粗

之间的空档。

#### 2. 增强 `protocol_summary`

后续建议在协议摘要里补充更多信息，例如：

- 总流数
- 总字节数
- Top 源 IP
- Top 目的 IP
- Top 目的端口
- 高频会话模式
- 是否伴随短连接或流量高峰

这样在回答 DNS、TLS、HTTP 相关问题时，摘要会更完整。

#### 3. 给 `flow_summary` 增加标签字段

建议后续在文档层额外输出结构化标签，例如：

- `is_short_connection`
- `is_scan_like`
- `is_tls`
- `is_http`
- `is_dns`
- `state_bucket`
- `risk_bucket`

这样后续检索不只依赖向量，还能做更稳定的过滤与排序。

### B.2 检索层增强

当前 `rag_search.py` 已经能做第一版检索，但还可以继续增强。

#### 1. 增加 query 改写

后续建议增加一层 query rewrite，把自然语言问题改写成：

- 检索目标
- 优先 `doc_type`
- 过滤条件
- 补充关键词

例如：

- “BitTorrent 有没有异常短连接”

可以改写成：

- `dataset_name = BitTorrent`
- `intent = short-connection`
- `doc_type = anomaly_summary, flow_summary`
- `keywords = short connection, low bytes, short duration`

#### 2. 增强 metadata 过滤

后续建议支持更多过滤项：

- `dataset_name`
- `source_file`
- `doc_type`
- `protocol`
- `app_protocol`
- `traffic_family`
- `risk_level`
- `dst_port`
- `time_bucket`

这样可以形成：

- 结构化过滤 + 向量检索

的混合方式。

#### 3. 增加混合排序

后续可以综合以下分数排序：

- 向量相似度
- 文本匹配得分
- 风险级别
- 异常优先级

而不是只看单一向量相似度。

### B.3 回答层增强

这是让系统从“能检索”走向“能回答”的关键一步。

#### 1. 检索结果与 `analyze.py` 联动

建议后续做成：

1. 先运行 `rag_search.py`
2. 召回相关摘要与证据
3. 再自动调用：
   - `overview-report`
   - `protocol-review`
   - `session-review`
   - `scan-review`
   - `timeseries`
4. 最后把 RAG 结果与分析结果合并成回答

也就是：

- RAG 负责证据召回
- `analyze.py` 负责结构化验证

#### 2. 增加回答模板

后续建议按问题意图分别设计回答模板，例如：

- 协议特征问题模板
- 短连接问题模板
- 扫描源问题模板
- 流量峰值问题模板
- 综合画像问题模板

这样输出会更稳定。

#### 3. 增加引用和证据回溯

后续回答里建议带上：

- 命中的 `doc_id`
- 对应 `source_file`
- 对应 `dataset_name`

这样更适合正式研判和结果追踪。

### B.4 数据层增强

如果后面希望继续提升质量，底层数据也可以增强。

#### 1. 增加 packet 级索引

当前第一版只做了 flow 级索引。后续可以逐步增加：

- `packet_summary`
- `flag_summary`
- `handshake_summary`

这会让以下问题回答更强：

- TCP flags
- 握手异常
- RST-heavy
- ICMP 探测

#### 2. 从 `scapy` 逐步转向 `tshark`

当前 `prepare_pcap.py` 还是以 `scapy` 为主。

后续如果数据规模继续变大，建议逐步改成：

- `tshark` 负责 packet 字段提取
- Python 负责 flow 聚合和结构化输出

这样会更适合大文件处理，并且与 Wireshark 的字段一致性更好。

### B.5 推荐实施顺序

为了减少返工，后续推荐按以下顺序推进：

1. 先增强 `build_rag_docs.py`
   - 增加 `endpoint_summary`
   - 增加 `port_summary`
   - 增强 `protocol_summary`
2. 再增强 `rag_search.py`
   - 增加 query 改写
   - 增加 metadata 过滤
   - 增加混合排序
3. 再做检索与 `analyze.py` 联动
   - 先检索
   - 再分析
   - 最后统一回答
4. 最后再升级底层预处理引擎
   - 从 `scapy` 向 `tshark` 过渡

### B.6 当前判断

当前第一版方案已经足够完成：

- 索引链路验证
- 单数据集检索
- 小规模 RAG 演示

如果要继续做成更稳定、更像正式产品的版本，下一阶段最值得优先做的是：

- 提高摘要层信息密度
- 强化检索策略
- 打通检索结果与 `analyze.py` 的联动回答
## 附录C：Packet 文档索引的后续增强建议

### C.1 当前结论

当前第一版 RAG 文档主要基于 `flow.csv` 生成，包含：

- `flow_summary`
- `protocol_summary`
- `anomaly_summary`

这一版已经能够支撑基础检索、异常线索召回和后续与 `analyze.py` 的联动分析，但暂时**不包含 `packet.csv` 的文档索引**。

### C.2 是否有必要增加 `packet.csv` 的文档索引

有必要，但不建议作为当前阶段的最高优先级。

原因如下：

- `flow.csv` 更适合整体通信类型、协议分布、短连接、扫描源、流量峰值这类问题。
- `packet.csv` 更适合补充包级细节，例如 TCP flags 模式、SYN / SYN-ACK / RST / FIN 行为、握手失败、ICMP 探测、小包突发等。
- 当前系统的主要短板还在于 `flow` 层摘要的信息密度和检索策略，而不是完全缺少 packet 级信息。

因此，更合理的实施顺序是：

1. 先继续增强 `flow` 层文档质量与检索效果。
2. 再在第二阶段增加 packet 级聚合摘要索引。

### C.3 为什么不建议直接把每个 packet 建成一条索引文档

不建议直接对每一个原始 packet 建索引，原因包括：

- 文档数量会急剧膨胀，embedding 和索引成本明显上升。
- 检索噪声会显著增加，容易反复命中相似的小包记录。
- 单包文本信息密度低，不利于语义检索。
- 前端和 agent 在后续回答时更容易受到过多细碎证据干扰。

因此，packet 级索引应采用**聚合摘要文档**而不是“原始逐包文档”。

### C.4 推荐的 packet 级文档类型

后续可以增加以下 packet 级聚合文档：

- `handshake_summary`
  - 描述 TCP 握手相关行为，例如 SYN、SYN-ACK、ACK 完整度和失败情况。
- `flag_pattern_summary`
  - 描述 TCP flags 分布，例如 SYN-only、RST-heavy、FIN、PSH/ACK 等模式。
- `icmp_summary`
  - 描述 ICMP 类型、代码及其分布，适合探测行为识别。
- `small_packet_burst_summary`
  - 描述小包突发、短时大量小包等特征。
- `packet_size_summary`
  - 描述包长分布、payload size band、异常包长模式。

这些文档应当由 `packet.csv` 聚合生成，而不是由原始 `pcap` 直接逐包文本化生成。

### C.5 与当前 `flow` 索引的关系

未来更合理的索引层次是：

- `flow_summary`
  - 负责整体会话和主要通信行为
- `protocol_summary`
  - 负责 DNS、TLS、HTTP 等协议特征
- `anomaly_summary`
  - 负责 flow 级异常摘要
- `packet_*_summary`
  - 负责包级细节、flags 模式和握手类异常

也就是说，packet 文档不是替代 flow 文档，而是作为第二层补充证据。

### C.6 推荐的落地顺序

建议按以下顺序推进：

1. 先继续增强 `flow_summary / protocol_summary / anomaly_summary`
   - 提高信息密度
   - 提高检索命中质量
2. 再设计 `packet.csv` 到 packet 聚合摘要文档的转换脚本
3. 最后把 packet 级文档并入统一 Elasticsearch 索引

### C.7 当前阶段建议

当前阶段仍以 `flow.csv` 为主。

如果后续出现以下需求，再启动 packet 索引增强更合适：

- 需要回答 TCP 握手异常、RST-heavy、SYN-only 等问题
- 需要识别 ICMP 探测、小包突发、包级 flags 模式
- 发现仅靠 flow 级摘要不足以支持精细异常分析

一句话总结：

当前第一版先继续把 `flow` 层 RAG 做稳；`packet.csv` 的文档索引是有价值的第二阶段增强，但应以**聚合摘要索引**而不是**逐包原始索引**的方式引入。

## 附录D：后续混合检索增强策略

### D.1 当前检索策略

当前 `rag_search.py` 使用的是一套轻量混合策略，包含：

- query 意图判断
- `doc_type` 路由
- `dataset_name / source_file / doc_type` 等 metadata 过滤
- Elasticsearch 向量检索

因此，当前并不是完整意义上的 BM25 + 向量联合排序，而是：

**意图路由 + 元数据过滤 + 向量召回**

### D.2 为什么后续要增强为真正的混合检索

当前仅依赖向量检索时，会有以下问题：

- 对 IP、端口、域名、SNI、Host 这类关键词型查询不够稳定
- 对非常接近的 `flow_summary` 文档，向量排序容易出现语义重复命中
- 面对“扫描源”“短连接”“流量高峰”这类结构化问题时，仅靠向量相似度不一定最优

因此，后续建议升级为真正的混合检索：

- 一部分依赖文本匹配
- 一部分依赖向量相似度
- 再进行联合排序或重排

### D.3 推荐的混合检索组成

建议后续检索链包含 4 层：

1. query 意图识别
2. metadata 过滤
3. 文本召回（BM25 / keyword match）
4. 向量召回（dense vector similarity）

最后再做：

- 分数融合
- 或二阶段重排

### D.4 推荐的 query 意图分类

建议继续沿用当前思路，并逐步增强为更明确的意图分类器：

- `traffic-profile`
- `protocol-feature`
- `short-connection`
- `scan-source`
- `traffic-peak`
- `general`

不同意图对应不同的优先文档类型：

- `traffic-profile` -> `flow_summary` + `anomaly_summary`
- `protocol-feature` -> `protocol_summary` + `flow_summary`
- `short-connection` -> `anomaly_summary` + `flow_summary`
- `scan-source` -> `anomaly_summary` + `flow_summary`
- `traffic-peak` -> `anomaly_summary`

### D.5 推荐的 metadata 过滤字段

后续应优先支持以下过滤维度：

- `dataset_name`
- `source_file`
- `doc_type`
- `protocol`
- `app_protocol`
- `traffic_family`
- `risk_level`
- `dst_port`
- `time_bucket`

这样可以避免“所有问题都在全索引里盲搜”。

### D.6 推荐的文本检索字段

后续 BM25 / 文本检索建议优先作用在以下字段：

- `title`
- `summary`
- `content`
- `keywords`

其中：

- `title` 适合快速命中明显主题
- `summary` 适合短文本高密度召回
- `content` 适合完整语义描述
- `keywords` 适合协议名、异常名、扫描、峰值、端口等关键词补强

### D.7 推荐的混合排序方式

后续可以采用以下两种实现路径之一：

#### 方案一：分数融合

同时做：

- 文本检索得分
- 向量检索得分

然后做加权融合，例如：

- `final_score = alpha * bm25_score + beta * vector_score`

这种方式实现直接，适合第一版混合检索升级。

#### 方案二：两阶段检索

先做第一阶段召回：

- 先 BM25 召回一批
- 或先向量召回一批

再做第二阶段重排：

- 用另一种信号重新排序

这种方式更灵活，也更适合后续扩展复杂策略。

### D.8 建议的增强顺序

建议按以下顺序实施：

1. 保留当前意图路由
2. 增强 metadata 过滤
3. 引入 `title / summary / keywords` 的文本检索
4. 做 BM25 + 向量分数融合
5. 视效果再升级为两阶段召回与重排

### D.9 当前阶段建议

当前阶段不必马上重写检索层。

更稳的做法是：

- 先继续增强摘要文档质量
- 再把当前向量检索结果观察稳定
- 等 `flow_summary / protocol_summary / anomaly_summary` 的质量更稳定后，再切入混合检索增强

一句话总结：

后续推荐把当前“意图路由 + metadata 过滤 + 向量召回”升级为“文本检索 + 向量检索 + 分数融合”的真正混合检索体系，以提升协议特征、异常模式和结构化查询的命中质量。

## 附录E：问题自动路由策略

### E.1 为什么需要自动路由

当前系统已经同时具备两类能力：

- `analyze.py`
  - 适合对当前数据做结构化统计分析与异常分析
- `rag_search.py`
  - 适合从已构建索引中召回历史证据、异常摘要和相似线索

如果所有问题都统一走 RAG，容易出现：

- 结构化问题回答不够精确
- 当前数据明明可以直接统计，却先去检索历史摘要
- 检索结果命中很多相似 `flow_summary`，但不能直接替代正式分析

如果所有问题都统一走 `analyze.py`，又会失去：

- 跨数据集检索
- 历史索引复用
- 已入库流量的快速召回能力

因此，更合理的方式是让系统根据问题自动判断：

- 直接分析
- 直接检索
- 检索后再分析

### E.2 推荐的三类路由结果

建议将用户问题先路由到以下三类之一：

#### 1. `analysis-only`

直接执行 `analyze.py`

适合：

- 当前正在分析某个 `flow.csv`
- 当前正在分析某个新上传的 `pcap`
- 问题本身是结构化统计或规则检测问题

#### 2. `rag-only`

直接执行 `rag_search.py`

适合：

- 明确要求“从索引中检索”
- 需要在历史已入库数据中找答案
- 需要跨多个数据集做召回或比较

#### 3. `rag-plus-analysis`

先执行 `rag_search.py`，再执行 `analyze.py`

适合：

- 先召回历史线索，再验证当前数据
- 同时需要历史证据和当前统计结论
- 问题本身既包含“索引召回”，也包含“结构化验证”

### E.3 推荐的默认判断原则

默认情况下，建议：

- **优先分析当前数据**
- **仅在问题明显指向历史索引或跨数据集召回时才优先 RAG**

也就是说：

- 如果用户在分析当前文件或当前数据集，默认优先 `analysis-only`
- 如果用户明确要求“从索引中检索”或“比较多个已索引数据集”，则优先 `rag-only`
- 如果用户表达了“先检索再验证”的意图，则走 `rag-plus-analysis`

### E.4 适合直接走 `analyze.py` 的问题特征

以下问题更适合直接分析：

- 读取某个 `flow.csv` 并分析
- 先预处理某个 `pcap` 再分析
- 当前数据的协议分布、通信类型、端口分布
- DNS / TLS / HTTP 特征分析
- 短连接检测
- 扫描源检测
- 流量高峰检测

典型关键词包括：

- `分析`
- `读取`
- `统计`
- `协议特征`
- `异常短连接`
- `扫描源`
- `流量高峰`
- `DNS`
- `TLS`
- `HTTP`

### E.5 适合直接走 RAG 的问题特征

以下问题更适合直接检索：

- 从索引中检索某个数据集的异常摘要
- 在历史已索引数据里寻找相似模式
- 比较多个已索引数据集
- 从历史流量库中找相似通信

典型关键词包括：

- `从索引中`
- `检索`
- `召回`
- `已索引`
- `历史数据`
- `相似`
- `比较多个数据集`

### E.6 适合联动的问法

以下问题更适合先检索再分析：

- 先从索引中找异常线索，再对当前数据验证
- 先找历史相似流量，再分析当前上传文件
- 同时需要“证据召回”和“正式分析结果”

典型表达包括：

- `先检索再分析`
- `先从索引中找，再验证`
- `结合索引和分析结果`
- `先找历史线索，再分析当前数据`

### E.7 建议的实现方式

后续可以在 skill 层增加一个轻量问题路由器，步骤如下：

1. 读取用户问题
2. 判断是否包含当前文件、索引、历史数据、多数据集等关键信号
3. 将问题归类为：
   - `analysis-only`
   - `rag-only`
   - `rag-plus-analysis`
4. 路由到对应执行链

### E.8 推荐的执行链

#### `analysis-only`

- 如有需要先执行 `prepare_pcap.py`
- 再执行 `analyze.py`

#### `rag-only`

- 直接执行 `rag_search.py`

#### `rag-plus-analysis`

- 先执行 `rag_search.py`
- 再执行 `analyze.py`
- 最后融合召回证据与分析结论

### E.9 当前阶段建议

当前阶段最稳的做法是：

- 对当前文件或当前数据集问题，默认优先 `analyze.py`
- 对历史索引检索和多数据集比较问题，优先 `rag_search.py`
- 对需要“先找线索再验证”的问题，再走联动链

一句话总结：

后续系统应增加一个轻量问题路由层，让系统根据问题内容自动判断是走 `analyze.py`、`rag_search.py`，还是先检索再分析，以兼顾结构化分析精度与历史索引召回能力。

## 附录F：是否将当前 RAG 抽象为通用 Skill 的建议

### F.1 当前判断

后续**有必要**将当前 RAG 能力抽象成一个通用 Skill，但**现在还不是最合适的拆分时机**。

### F.2 为什么现在不建议立刻拆

当前这套 RAG 仍然和网络流量场景强绑定，主要体现在：

- 文档类型是网络流量专用的
  - `flow_summary`
  - `protocol_summary`
  - `anomaly_summary`
- 异常语义与网络流量分析强相关
  - 短连接
  - 扫描源
  - 流量高峰
  - DNS / TLS / HTTP 特征
- 检索意图分类也明显围绕网络流量问题展开
- 当前还在持续调整：
  - 摘要文档结构
  - 异常摘要逻辑
  - 检索策略
  - 与 `analyze.py` 的联动方式

如果现在就直接抽成通用 Skill，容易把仍在变化中的网络流量专用逻辑一并固化，反而会增加后续维护成本。

### F.3 为什么后续值得拆成通用 Skill

虽然当前仍偏领域化，但系统已经形成了一套明显可复用的 RAG 框架能力，包括：

- 文档 embedding
- Elasticsearch 向量入库
- 向量检索
- metadata 过滤
- 意图路由
- 后续混合检索扩展能力

这些能力本身并不只适用于网络流量，后续如果还要支持：

- 轨迹数据
- 年鉴数据
- 遥感或卫星数据
- 其他结构化分析数据

那么将通用部分抽成一个独立 Skill 是合理的。

### F.4 推荐的拆分方式

后续如果要抽象，建议拆成两层：

#### 1. 通用 RAG Skill

负责：

- embedding 配置与执行
- Elasticsearch 索引管理
- 文档入库
- 检索执行
- metadata 过滤
- 混合检索框架

#### 2. 领域适配 Skill

继续保留在具体领域内，例如网络流量 Skill 负责：

- `build_rag_docs.py`
- 网络流量专用文档结构
- 网络流量专用异常摘要逻辑
- 网络流量问题路由
- 与 `analyze.py` 的联动

也就是说：

- 通用 Skill 管 RAG 基础设施
- 领域 Skill 管文档语义和业务逻辑

### F.5 推荐的拆分时机

建议在以下条件满足后再拆：

- `build_rag_docs.py` 的文档结构基本稳定
- `rag_search.py` 的检索策略基本稳定
- `analyze.py` 的联动方式基本稳定
- 已经出现第二个值得复用同类 RAG 框架的数据领域

### F.6 当前阶段建议

当前阶段仍建议：

- 继续将 RAG 保留在 `network-traffic-analysis` skill 内
- 把当前网络流量场景的文档、检索和联动逻辑先做稳
- 等框架足够稳定、并出现跨领域复用需求时，再抽成通用 Skill

一句话总结：

当前 RAG 方案后续值得抽象为通用 Skill，但现在仍更适合保留在网络流量 Skill 内，等领域逻辑和检索框架稳定后再做通用化拆分会更合理。

### 单文件融合问法
1.
先从已构建索引中检索 Virut.pcap 的主要通信类型、异常短连接和可疑扫描源，再结合当前数据的正式分析结果，给出统一结论。

2.
请先基于 RAG 索引召回 Virut.pcap 的异常摘要和主机画像，再用分析脚本验证这些异常是否成立，并输出最终研判。

3.
先检索 Virut.pcap 在索引中的通信画像、扫描摘要和流量峰值证据，再结合 analyze.py 的结果总结这份流量的主要恶意特征。

4.
请先从索引中找出 Virut.pcap 的关键异常线索，再对当前预处理后的数据做正式分析，最后合并成一个结论一致的报告。

### 更明确限制 RAG 只做证据召回的问法
5.
请先用 RAG 检索 Virut.pcap 的异常证据和主机画像，不要直接下最终结论；然后用分析层结果做验证，并输出最终判断。

6.
先从已构建索引中召回 Virut 的异常摘要、endpoint summary 和 port summary，再用分析脚本确认主要通信类型、短连接和扫描行为。

7.
请把 Virut.pcap 分成两步处理：第一步做索引检索找证据，第二步做正式分析验证结论，最后给出统一回答。

### 强调“以分析层为主、RAG 为辅”的问法
8.
请以分析层结果为主、RAG 检索结果为辅，综合回答 Virut.pcap 的主要通信类型、异常短连接和可疑扫描源。

9.
先检索 Virut.pcap 的历史摘要和异常线索，再以分析脚本的正式统计为准，给出最终结论。

10.
请先用 RAG 找到 Virut.pcap 的关键证据，再结合分析层结果做交叉验证，避免只依赖检索摘要。

### 多文件融合问法
11.
先从索引中检索 Virut 和 BitTorrent 的通信画像与异常摘要，再结合分析层结果比较两者在短连接、扫描源和协议特征上的差异。

12.
请先检索多个已索引流量样本中与 Virut 最相似的异常模式，再结合当前 Virut.pcap 的正式分析结果给出综合判断。


/nfsdat1/home/wlwangslm/elasticsearch-9.2.3本地目录是这个，怎么启动


直接这样启动就行。

前台启动
cd /nfsdat1/home/wlwangslm/elasticsearch-9.2.3
./bin/elasticsearch
这样会占住当前终端，但最适合先看有没有报错。

后台启动
如果你想让它在后台跑：

cd /nfsdat1/home/wlwangslm/elasticsearch-9.2.3
nohup ./bin/elasticsearch > /nfsdat1/home/wlwangslm/elasticsearch-logs/es-console.log 2>&1 &
启动后检查
等几秒再执行：

curl http://127.0.0.1:9200
如果起来了，应该会返回一段 JSON。

如果没起来，看日志
tail -n 50 /nfsdat1/home/wlwangslm/elasticsearch-logs/es-console.log
然后继续你的索引构建
cd /nfsdat1/home/wlwangslm/imiss-deer-flow-main
python ./skills/custom/network-traffic-analysis/scripts/build_full_rag_index.py --raw-dir ./datasets/network-traffic/raw --index-name network-traffic-rag --format text --verbose
一句话：先在 /nfsdat1/home/wlwangslm/elasticsearch-9.2.3 里运行 ./bin/elasticsearch，确认服务起来后再继续构建。
关闭服务
. 先找进程
ps -ef | grep elasticsearch
2. 杀掉主进程
找到 Elasticsearch 的 PID 后执行：

kill <PID>
例如：

kill 12345




## 附录H：预处理升级与索引重建计划

### H.1 当前已完成的预处理升级

当前 `prepare_pcap.py` 已完成以下关键升级：

- 原先按整文件 conversation 粗聚合的 flow 逻辑，已升级为 session 级 flow 切分。
- TCP flow 现在按以下规则切分：
  - `SYN` 优先开启新 session
  - `FIN` / `RST` 结束当前 session
  - `60s` idle timeout 切分新 flow
- UDP / ICMP / 其他协议按 `30s` idle timeout 切分新 flow。
- `flow_id` 已升级为 session 级唯一标识，不再是同一对端点永远共用一个 id。
- `src_bytes` / `dst_bytes`、`src_packets` / `dst_packets`、`direction` 已恢复 session 级统计意义。
- `session_state` 已补充 `SYN_ONLY`，与后续 RAG 风险判断逻辑对齐。
- 预处理结果中已新增：
  - `flow_start_reason`
  - `flow_end_reason`
  用于标记 session 的开启和结束原因。

### H.2 当前版本的定位

当前版本的 `prepare_pcap.py` 已经达到：

- 可以作为后续 `analyze.py`
- `build_rag_docs.py`
- `rag_search.py`
- 前端融合回答

的正式基础版本。

当前实现属于“增强后的轻量 sessionizer”，已经解决了原先会系统性污染 flow 统计语义的问题。

### H.3 后续仍可增强但不是当前阻塞项

后续如需继续提升预处理精度，可在第二阶段考虑以下增强：

- 更精细的并发 TCP 会话区分
  - 特别是同一对端点、同一端口短时间内存在多个并发连接的场景
- 更细的 TCP 行为建模
  - 重传
  - 乱序
  - duplicate ACK
  - 半开连接
- 更强的协议字段提取
  - TLS SNI 分片场景
  - 更完整的 HTTP 字段提取
  - 更丰富的 DNS 字段
- 更高保真的 packet 级行为特征
  - 为后续 packet 聚合摘要文档做准备

这些增强属于“协议级精细化优化”，不是当前第一优先级。

### H.4 当前不建议继续扩展到 IDS 级 TCP 重组

虽然更完整的 IDS 级 TCP 重组器可以进一步提高底层精度，但当前项目阶段不建议继续做到这一层，原因包括：

- 工程复杂度明显上升
- 调试和验证成本高
- 对当前面向分析、RAG 和甲方演示的收益不成比例

当前增强版 `prepare_pcap.py` 已足够支撑当前系统目标。

### H.5 修改后必须执行的重建动作

由于本次修改影响了 flow 的基础语义，后续所有依赖 flow 的结果都需要重建。

必须重建的内容包括：

- `processed/<dataset>/<dataset>.flow.csv`
- 基于 flow 构建的 RAG 文档
- embedding 向量文件
- Elasticsearch 索引 `network-traffic-rag`

### H.6 立即执行的重建步骤

推荐按以下顺序执行：

#### 1. 删除旧索引

```bash
curl -X DELETE http://127.0.0.1:9200/network-traffic-rag
```

#### 2. 重新预处理原始数据

对需要继续使用的数据集重新运行 `prepare_pcap.py`，生成新的 `packet.csv` 和 `flow.csv`。

如果要对整个 raw 目录重新预处理，应逐数据集重新跑预处理脚本，确保所有 flow 都基于新的 session 切分逻辑生成。

#### 3. 重新全量构建 RAG 索引

```bash
python ./skills/custom/network-traffic-analysis/scripts/build_full_rag_index.py --raw-dir ./datasets/network-traffic/raw --index-name network-traffic-rag --format text --verbose
```
python ./skills/custom/network-traffic-analysis/scripts/build_full_rag_index.py --raw-dir ./datasets/network-traffic/raw/USTC-TFC2016/smoke --index-name network-traffic-rag --format text --verbose
#### 4. 重建完成后验证

建议至少检查：

- Elasticsearch 索引是否重建成功
- 文档总数是否稳定
- `overview-report`
- `scan-review`
- `short-connection-review`
- `rag_search.py`

是否都比旧版本更合理。

### H.7 当前阶段的执行建议

当前最推荐的执行顺序是：

1. 停止继续基于旧 flow 结果做分析或检索对比
2. 删除旧索引
3. 使用新版本 `prepare_pcap.py` 重跑预处理
4. 全量重建 RAG 索引
5. 再继续进行前端自然语言测试、RAG 评测和融合回答验证

一句话总结：

当前最重要的不是继续扩展新功能，而是基于新的 flow 语义完成一次彻底重建，确保后续分析、RAG 和前端回答都建立在同一套正确的预处理基础上。
