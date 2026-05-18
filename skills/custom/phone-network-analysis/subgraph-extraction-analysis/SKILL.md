---
name: subgraph-extraction-analysis
description: 围绕单个号码抽取 1-2 跳局部关系圈，输出局部结构信息。仅适合“抽局部图/看邻居结构”类问题；如果用户要求完整单号码综合分析，必须改用 single-number-analysis。
allowed-tools: Bash
---

# subgraph-extraction-analysis

## 这个 skill 的定位

这个 skill 只负责：

- 抽 1 跳 / 2 跳局部关系圈
- 看局部结构
- 看邻居、局部 Hub、局部桥接点

它 **不是完整的单号码综合分析 skill**。

---

## 什么时候应该用它

只在下面这些场景使用：

- “帮我抽一下这个号码的一跳子图”
- “围绕这个号码看两跳局部图”
- “这个号码周围的局部结构是什么样”
- “先给我看这个号码附近的局部关系圈”

---

## 什么时候不要用它

如果用户要求下面这些完整分析内容，就 **不要优先用本 skill**，而要改用 `single-number-analysis`：

- 输出号码画像
- 输出 Top 可疑节点排序
- 输出共享设备主线索
- 输出推荐二次下钻节点
- 输出完整单号码分析报告
- 同时要求通话关系 + 设备关系的综合判断

典型问法：

- “请完整分析这个号码”
- “这个号码值不值得重点查”
- “输出这个号码的画像、可疑节点和共享设备”
- “帮我生成完整 markdown 报告”

这些都应该优先使用：

```text
phone-network-analysis/single-number-analysis
```

---

## 输入要求

至少提供：

- `phone_id`：目标号码哈希 ID

可选参数：

- `hops`：1 或 2，默认 1
- `max_nodes`：默认 100
- `top_k`：默认 10
- `directed`：是否按有向通话图抽取，默认不加

---

## 执行步骤

### 1. 进入脚本目录

```bash
cd /mnt/skills/custom/phone-network-analysis/subgraph-extraction-analysis/scripts
```

### 2. 运行分析脚本

```bash
python3 subgraph_extraction_wrapper.py \
  --phone-id "<PHONE_ID>"
```

两跳子图示例：

```bash
python3 subgraph_extraction_wrapper.py \
  --phone-id "<PHONE_ID>" \
  --hops 2 \
  --max-nodes 100 \
  --top-k 10
```

如需按有向通话图分析：

```bash
python3 subgraph_extraction_wrapper.py \
  --phone-id "<PHONE_ID>" \
  --hops 1 \
  --directed
```

---

## 报告处理要求

如果脚本返回 `report_path` / `artifacts`：

1. 先检查文件存在
2. 再读取完整 Markdown
3. 最终把 Markdown 作为附件展示/下载
4. 不要只把路径字符串展示给用户
