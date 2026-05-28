# YiGraph 复用说明（本项目本地适配版）

## 1. 这份说明是干什么的

这份文档用来明确说明：

1. 我们从 YiGraph 中到底复用了什么
2. 哪些内容是直接参考
3. 哪些内容不能直接拿来跑
4. 我们在 DeerFlow 里是怎么做“本地适配”的

---

## 2. 当前的复用原则

我们没有把 YiGraph 整体直接接入 DeerFlow。

原因：

1. 原始代码依赖其自己的工程结构与运行环境
2. 原始代码里有 Neo4j / AAG / NebulaGraph / Reasoner 等依赖
3. 直接 import 进 DeerFlow，成本高、容易炸
4. 当前阶段更适合“抽取语义结构 + 本地适配”

所以我们采用的是：

- **文档语义复用**
- **查询类型复用**
- **算法能力参考复用**
- **本地 wrapper 适配复用**

---

## 3. 各文件的复用方式

### 3.1 graph_query.md
原作用：
- 总结 Graph Query 能力集合
- 定义 node_lookup / relationship_filter / aggregation_query / neighbor_query / path_query / common_neighbor / subgraph / subgraph_by_nodes 这些查询类型

当前复用方式：
- 作为“高级 graph skill 应该长什么样”的说明书
- 用来指导 DeerFlow 中高级 skill 的问题类型设计
- 用来写自己的 query template 和 skill 说明

当前不直接执行的原因：
- 它是文档，不是运行时代码

---

### 3.2 path.md
原作用：
- 总结路径类能力：shortest_path、dijkstra、bellman_ford、johnson、has_path、dag_longest_path、eulerian 等

当前复用方式：
- 作为 association-path-analysis 的能力设计参考
- 用来确定路径 skill 应该输出哪些关键信息
- 后续做更高级路径分析时继续参考

当前不直接执行的原因：
- 它是文档，不是运行时代码

---

### 3.3 templates.py
原作用：
- 定义 query type 模板和修饰符

当前复用方式：
- 参考它的 query type 结构
- 在 common/yigraph_query_templates.py 中做电话网络任务版精简重写

当前不直接执行的原因：
- 原模板是面向其原始系统的，不完全适配当前项目参数体系

---

### 3.4 graph_query.py
原作用：
- 提供 Neo4jGraphClient
- 支持查点、查邻居、查公共邻居、查路径、抽子图、聚合统计等

当前复用方式：
- 主要参考它的“查询能力分类”和“接口组织方式”
- 不直接复用其数据库执行逻辑
- 当前改为用现有 graph-operator 进行本地适配

当前不直接执行的原因：
- 强依赖 Neo4j
- 也依赖原项目工程环境

---

### 3.5 nl_query_engine.py
原作用：
- 自然语言 -> 查询类型 -> 参数提取 -> 查询执行

当前复用方式：
- 参考它的“问题路由”设计思路
- 当前先简化成 common/yigraph_intent_router.py 的规则版本
- 后续如果要做真正自然语言 graph QA，再继续增强

当前不直接执行的原因：
- 依赖 Reasoner / AAG / Neo4j 等整套环境
- 当前阶段过重

---

### 3.6 graph_computation_processor.py
原作用：
- 把图数据转成 NetworkX 图
- 运行 PageRank、连通分量、最短路、介数中心性、接近中心性、度中心性、Louvain 社区检测等算法

当前复用方式：
- 作为后续高级图分析 skill 的能力来源
- 尤其适合后面扩展：
  - key-node-analysis
  - graph-cluster-analysis
  - community-detection-analysis

当前不直接执行的原因：
- 依赖原始 datatype / GraphProcessor / NebulaGraphClient
- 不能直接无缝丢进 DeerFlow 跑

---

## 4. 当前阶段的结论

当前最优做法不是“整个 YiGraph 接进来”，而是：

1. 保留原始资料做参考
2. 自己抽 query template
3. 自己写意图路由
4. 用现有 graph-operator 当底层执行器
5. 先做第一个 YiGraph 风格高级 skill：association-path-analysis

这条路线最稳，也最适合你现在的项目进度。
