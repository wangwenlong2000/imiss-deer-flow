# topn-high-risk-discovery

## 这个 skill 是做什么的

这是一个**批量重点对象发现 skill**，用于从电话网络数据里快速找出最值得优先核查的号码，并直接生成：

- 综合风险总榜
- 未标注高可疑榜
- 设备驱动高可疑榜
- Markdown 报告
- CSV 风险名单

它适合回答下面这类问题：

- 现在这批电话网络数据里，最值得优先查的 TopN 号码是谁？
- 有没有“还没被打成风险标签，但结构已经很可疑”的号码？
- 哪些号码主要是因为共享设备关系异常而值得重点排查？
- 能不能直接生成一份可以下载的重点对象报告？

---

## 它是怎么基于基础算子实现的

这个 skill 不是硬编码业务逻辑，而是通过你已经接入的基础算子组合得到的：

- `node_lookup`：读取号码画像、标签、省份等节点属性
- `aggregation_query + neighbor_query`：聚合通话记录、对端数、联系人广度
- `neighbor_query + subgraph_by_nodes`：扩展共享设备关系，得到共享设备数、牵出号码数、最强共享设备
- `relationship_filter + aggregation_query`：按规则过滤候选对象，并做多视角排序

因此它本质上就是：

**“基础图分析算子” -> “电话网络重点对象发现业务 skill”**

---

## 当前版的增强点

相比之前版本，这一版重点增强了四件事：

### 1. 报告模板按模式自适应

- 当 `candidate_scope=unlabeled_only` 时，自动避免“综合总榜”和“未标注榜”重复展示
- 当 `analysis_mode=device_only` 且 `ranking_view=device_priority` 时，自动避免“综合总榜”和“设备榜”重复展示

### 2. 解释层更完整

现在每个上榜对象不只是给一句“入榜原因”，还会输出：

- 驱动类型（标签驱动 / 设备驱动 / 联系人广度驱动 / 通话活跃驱动 / 混合驱动）
- Top3 分数组件
- Top3 证据包
- 与下一名的关键差异说明

### 3. 未标注对象解释更清楚

当号码是 `label=0` 且 `sub_label=normal/whitelist` 时，报告里会明确提醒：

> 这是“高可疑线索”，不等于最终认定风险。

### 4. 过滤能力更强

新增支持：

- `--include-sub-labels`
- `--exclude-sub-labels`
- `--min-device-count`
- `--min-shared-peer-total`

这几个参数对后面做 `condition-based-screening` 也有帮助。

---

## 核心输入参数

### 排序与分析参数

- `--top-n`：综合总榜 TopN，默认 `20`
- `--discovery-top-n`：未标注榜 / 设备榜 TopN，默认 `10`
- `--analysis-mode`：`mixed` / `call_only` / `device_only`
- `--ranking-view`：
  - `all_views`
  - `overall`
  - `unlabeled_only`
  - `device_priority`
- `--candidate-scope`：
  - `all`
  - `labeled_only`
  - `unlabeled_only`
- `--province`：省份过滤，例如 `sichuan`

### 过滤参数

- `--min-call-records`
- `--min-counterparties`
- `--min-shared-device-count`
- `--min-device-count`
- `--min-shared-peer-total`
- `--include-sub-labels`
- `--exclude-sub-labels`

例如：

```bash
--exclude-sub-labels whitelist
```

或：

```bash
--include-sub-labels risk,purefraud
```

---

## 输出内容

JSON 输出里最关键的字段包括：

- `top_overall_numbers`
- `top_unlabeled_numbers`
- `top_device_driven_numbers`
- `top3_evidence_pack`
- `view_summaries`
- `discovery_insights`
- `report_context_flags`
- `report_path`
- `risk_list_csv_path`
- `artifacts`

其中：

- `top3_evidence_pack`：是这一版新增的“证据包”字段
- `report_context_flags`：会告诉你当前报告是否自动隐藏了重复榜单

---

## 命令行示例

### 示例 1：完整重点对象发现

```bash
cd /mnt/skills/custom/phone-network-analysis/topn-high-risk-discovery/scripts && python3 topn_high_risk_discovery_wrapper.py \
  --top-n 20 \
  --discovery-top-n 10 \
  --analysis-mode mixed \
  --ranking-view all_views \
  --candidate-scope all
```

### 示例 2：只看未标注高可疑对象

```bash
cd /mnt/skills/custom/phone-network-analysis/topn-high-risk-discovery/scripts && python3 topn_high_risk_discovery_wrapper.py \
  --top-n 10 \
  --discovery-top-n 10 \
  --analysis-mode mixed \
  --ranking-view unlabeled_only \
  --candidate-scope unlabeled_only
```

### 示例 3：设备驱动重点对象发现

```bash
cd /mnt/skills/custom/phone-network-analysis/topn-high-risk-discovery/scripts && python3 topn_high_risk_discovery_wrapper.py \
  --top-n 10 \
  --discovery-top-n 10 \
  --analysis-mode device_only \
  --ranking-view device_priority \
  --candidate-scope all \
  --min-shared-device-count 1
```

### 示例 4：四川省 + 排除白名单

```bash
cd /mnt/skills/custom/phone-network-analysis/topn-high-risk-discovery/scripts && python3 topn_high_risk_discovery_wrapper.py \
  --top-n 20 \
  --discovery-top-n 10 \
  --analysis-mode mixed \
  --ranking-view all_views \
  --candidate-scope all \
  --province sichuan \
  --exclude-sub-labels whitelist
```

---

## 快速测试

建议直接运行脚本目录下的：

```bash
bash test_topn_high_risk_discovery_fast.sh
```

这个测试脚本会：

1. 自动创建项目根目录下的 `logs/topn-high-risk-discovery/`
2. 把每次测试的完整终端输出保存成 `.log`
3. 自动列出生成的 `.md` 和 `.csv` 文件

这样你后面不用再盯着终端滚屏了。

---

## 什么时候该用这个 skill

当问题是：

- “一批号码里谁最值得先查？”
- “帮我先发现重点对象并生成名单”
- “帮我找未标注但很可疑的号码”
- “帮我找设备关系特别异常的号码”

就优先用它。

如果问题变成：

- “这个号码为什么可疑？”
- “它周围的局部关系圈是什么样？”

那就切到：

- `single-number-analysis`

如果问题变成：

- “两个号码之间怎么连起来的？”

那就切到：

- `association-path-analysis`

如果问题变成：

- “两个号码是不是处在同一个联系圈？”

那就切到：

- `overlap-analysis`
