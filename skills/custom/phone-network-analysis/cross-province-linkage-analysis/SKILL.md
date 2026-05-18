# cross-province-linkage-analysis

## 作用定位

`cross-province-linkage-analysis` 是电话网络数据分析体系中的**跨省联动分析 skill**，基于已经预处理好的 `unified` 统一索引，识别四川与陕西等不同省份之间的跨省关联线索。

它用于回答：

- 有没有同一设备同时挂载不同省份的号码？
- 有没有同一个对端被不同省份的号码共同联系？
- 有没有跨省直接通话或强关联号码对？
- 哪些号码更像跨省桥接对象？
- 跨省线索应该继续用哪些 skill 下钻验证？

本 skill 输出的是**跨省关联线索**，不是案件定性结论。不能把“存在跨省关联线索”直接表述成“已经确认跨省犯罪联动”。

---

## 与 `sichuan-shaanxi-comparison` 的区别

两者都与地域有关，但分析目标不同：

| skill | 关注点 | 适合回答 |
|---|---|---|
| `sichuan-shaanxi-comparison` | 两地差异对比 | 四川和陕西有什么不同 |
| `cross-province-linkage-analysis` | 跨省联动关系 | 四川和陕西之间有没有关联线索 |

只要用户问“跨省共享设备、跨省强关联对象、跨省关系链路、跨区域联动线索”，必须优先运行本 skill：

```bash
cross_province_linkage_wrapper.py
```

不要用 `sichuan-shaanxi-comparison` 或 `condition-based-screening` 代替本 skill。

---

## 默认数据

默认使用电话网络统一数据集：

```text
/mnt/datasets/phone-network/processed/unified/user_nodes.csv
/mnt/datasets/phone-network/processed/unified/call_edges.csv
/mnt/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet
```

本地 Docker 测试时通常使用：

```text
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/unified/user_nodes.csv
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/unified/call_edges.csv
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet
```

如果用户没有指定数据集，默认使用：

```text
dataset=unified
province_a=sichuan
province_b=shaanxi
```

---

## 与基础算子的组合关系

本 skill 不是孤立脚本，而是组合电话网络基础图分析算子实现。

| 分析内容 | 对应基础算子风格 |
|---|---|
| 省份范围限定 | `node_lookup + province_filter` |
| 跨省共享设备识别 | `query_shared_device + aggregation_query` |
| 跨省共同对端识别 | `common_neighbor + relationship_filter + aggregation_query` |
| 跨省直接通话识别 | `relationship_filter(src_province != dst_province)` |
| 跨省强关联号码对排序 | `aggregation_query + scoring_layer` |
| 代表性跨省链路重构 | `path_query style evidence reconstruction` |
| 桥接对象识别 | `subgraph_by_nodes / aggregation_query + scoring_layer` |

---

## 分析内容

本 skill 会输出以下内容：

1. 省份对象规模概览；
2. 跨省共享设备证据；
3. 跨省共同对端证据；
4. 跨省直接通话证据；
5. 跨省强关联号码对；
6. 重点桥接对象；
7. 代表性跨省关系链路；
8. 后续下钻建议。

---

## 运行方式

### 前端/平台默认运行

```bash
cd /mnt/skills/custom/phone-network-analysis/cross-province-linkage-analysis/scripts && python3 cross_province_linkage_wrapper.py \
  --dataset unified \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 10 \
  --artifact-mode essential
```

### 本地项目容器运行

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/cross-province-linkage-analysis/scripts
python3 cross_province_linkage_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --dataset unified \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 10
```

### 一键测试

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/cross-province-linkage-analysis/scripts
bash test_cross_province_linkage_analysis.sh
```

---

## 主要参数

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--dataset-root` | 电话网络数据根目录 | 自动识别 |
| `--dataset` | 数据集名称 | `unified` |
| `--province-a` | 第一个省份 | `sichuan` |
| `--province-b` | 第二个省份 | `shaanxi` |
| `--top-k` | 返回 Top 证据数量 | `10` |
| `--min-shared-phone-per-province` | 共享设备每省最少挂载号码数 | `1` |
| `--min-common-sources` | 共同对端每省最少来源号码数 | `2` |
| `--max-hub-degree` | 共同对端公共 hub 标记阈值 | `500` |
| `--max-pair-common-hub-degree` | 构造号码对时允许的最大共同对端度数 | `200` |
| `--artifact-mode` | 前端附件展示模式 | `full` |
| `--output-dir` | 输出目录 | `/mnt/user-data/outputs` |

### artifact-mode 说明

| 模式 | 展示内容 | 适用场景 |
|---|---|---|
| `full` | markdown + 全部 csv/json/xlsx | 命令行验收、完整证据链 |
| `essential` | markdown + summary.json + evidence.xlsx | 常规前端分析 |
| `markdown_only` | 只展示两个 markdown 报告 | 用户明确只要报告时 |

无论选择哪种模式，脚本都会在输出目录生成完整证据文件；区别只是哪些文件写入 `artifacts` 供前端展示。

---

## 输出文件

默认输出前缀：

```text
cross_province_linkage_unified
```

主要输出包括：

- `cross_province_linkage_unified.md`：技术版完整报告；
- `cross_province_linkage_unified_presentation.md`：甲方汇报版摘要；
- `cross_province_linkage_unified_summary.json`：结构化摘要；
- `cross_province_linkage_unified_evidence.xlsx`：证据工作簿；
- `cross_province_linkage_unified_cross_shared_devices.csv`：跨省共享设备证据；
- `cross_province_linkage_unified_cross_common_counterparties.csv`：跨省共同对端证据；
- `cross_province_linkage_unified_direct_cross_calls.csv`：跨省直接通话证据；
- `cross_province_linkage_unified_strong_pairs.csv`：跨省强关联号码对；
- `cross_province_linkage_unified_bridge_objects.csv`：桥接对象；
- `cross_province_linkage_unified_linkage_paths.csv`：代表性跨省链路。

---

## 前端测试提示词

### Q1：完整跨省联动分析

```text
请使用 cross-province-linkage-analysis skill，基于 unified 电话网络数据识别四川和陕西之间的跨省联动线索。要求输出跨省共享设备、跨省共同对端、跨省直接通话、跨省强关联对象、桥接节点和代表性关系链路，并生成 markdown 报告和 csv/xlsx/json 证据附件。
```

### Q2：甲方汇报版

```text
请使用 cross-province-linkage-analysis skill，生成一份适合甲方汇报的跨省联动研判摘要，重点说明四川和陕西之间是否存在跨省共享设备、共同对端、强关联号码对和代表性链路。请展示 markdown 报告，并提供核心证据附件。
```

### Q3：只要 Markdown 报告

```text
请使用 cross-province-linkage-analysis skill，基于 unified 数据分析四川和陕西之间的跨省联动线索。只需要展示 markdown 报告下载入口，不要展示 csv、json、xlsx 证据附件。执行参数请使用 artifact_mode=markdown_only。
```

---

## 解释边界

必须使用谨慎表述：

- 可以说“发现跨省关联线索”；
- 可以说“存在跨省共享设备/共同对端/强关联号码对”；
- 可以说“建议继续下钻复核”；
- 不要说“已确认跨省团伙”；
- 不要说“已证明跨省犯罪联动”。

---

## 推荐后续下钻

- 对 Top 跨省共享设备：调用 `shared-device-analysis`；
- 对 Top 跨省号码对：调用 `association-path-analysis` 和 `overlap-analysis`；
- 对桥接对象：调用 `single-number-analysis` 或 `risk-evidence-pack`；
- 对集中簇：调用 `gang-cluster-analysis`。
