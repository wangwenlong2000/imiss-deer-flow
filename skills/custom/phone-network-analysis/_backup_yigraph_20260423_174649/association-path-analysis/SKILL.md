cat > association-path-analysis/SKILL.md <<'MD'
# association-path-analysis

## 这个 skill 是做什么的

这个 skill 用于分析**两个号码之间的关联路径**，并且支持两层分析：

1. **direct_call_analysis**  
   基于电话网络原始通话边图，按**有向图**分析号码A是否能沿着通话方向到达号码B。

2. **composite_analysis**  
   把三类关系联合起来做复合路径分析：
   - call（通话关系）
   - shared_device（共享设备关系）
   - common_counterparty（共同对端关系）

也就是说，这不是只看“有没有通话路径”，而是在更高级的层面上分析两个号码之间是否存在**多关系混合连接**。

---

## 适合回答什么问题

当用户的问题属于下面这些类型时，优先使用本 skill：

- “帮我看这两个号码有没有关系”
- “这两个号码之间怎么连起来的”
- “这两个号码有没有间接联系”
- “中间桥接号码是谁”
- “除了通话关系以外，它们有没有设备共用或者共同对端”
- “请做复合路径分析”

---

## 默认数据源

### 通话边图
- `/mnt/datasets/phone-network/processed/unified/call_edges.csv`

字段：
- `src_user_id`
- `dst_counterparty_id`

### 号码-设备图
- `/mnt/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet`

字段：
- `user_id`
- `imei`

---

## 默认行为

### 通话路径
默认按**有向图**分析，也就是优先保留真实通话方向：
- `src_user_id -> dst_counterparty_id`

### 复合路径
默认联合启用三类关系：
- call
- shared_device
- common_counterparty

---

## 输入参数

核心参数：

- `--phone-a`
- `--phone-b`

重要可选参数：

- `--analysis-mode`
  - `direct_call`
  - `composite`
  - `both`（默认）

- `--call-graph-path`
- `--device-graph-path`
- `--graph-format`
- `--source-col`
- `--target-col`

- `--max-hops`
- `--per-relation-limit`
- `--max-expand-nodes`

关系开关：

- `--disable-call`
- `--disable-shared-device`
- `--disable-common-counterparty`

通话方向控制：

- 默认开启：`--directed-call`
- 可改为：`--undirected-call`

---

## 输出内容

本 skill 应输出：

### 一、pair_relation_signals
两个号码之间的直接配对信号，包括：

- A是否直接打给过B
- B是否直接打给过A
- 共享设备数
- 共同对端数
- 共享设备预览
- 共同对端预览

### 二、direct_call_analysis
基于有向通话图的直接路径分析：

- 是否存在路径
- 路径长度
- 路径节点序列
- 桥接节点
- 中文解释

### 三、composite_analysis
基于多关系联合图的复合路径分析：

- 是否存在路径
- 路径长度
- 路径节点序列
- 每一步的关系类型
- 每一步的证据信息
- 中文解释

### 四、recommended_view
推荐优先查看哪一种分析结果。

---

## 执行方式

```bash
cd /mnt/skills/custom/phone-network-analysis/association-path-analysis/scripts && python3 association_path_wrapper.py --phone-a "<号码A>" --phone-b "<号码B>" --analysis-mode both
```
## 当前定位

这是一个 YiGraph 风格的高级 graph skill，但底层并不是直接运行 YiGraph 原始工程，而是：

1.复用 YiGraph 的 query 语义与 skill 设计思路
2.结合当前 DeerFlow + 电话网络数据结构
3.调用本地适配后的路径分析与复合关系分析逻辑

所以它属于：

- YiGraph 语义复用
- DeerFlow 本地实现
- 电话网络任务定制版高级 graph skill