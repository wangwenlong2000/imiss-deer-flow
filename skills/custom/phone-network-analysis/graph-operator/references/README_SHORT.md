# graph-operator 最小可运行版说明

这是一个面向 DeerFlow 的第一版图分析基础 skill。

## 当前目标

今天先把最基础的图分析能力跑通，不追求一次性覆盖全部图算法。

当前建议先验证以下 4 个核心 operator：

1. `load_graph`
2. `expand_neighbors`
3. `shortest_path`
4. `basic_graph_metrics`

如果这 4 个能在 DeerFlow 中跑通，就说明第一版 skill 已经构建成功。

## 当前依赖

建议安装：

```bash
pip install networkx pandas
```

如果后续要做可视化，可再安装：

```bash
pip install matplotlib
```

## 推荐测试数据

建议先用一个小型 CSV 文件测试，例如：

```csv
src,dst
A,B
B,C
A,D
D,C
C,E
```

## 当前实现原则

- Skill 做壳
- Wrapper 脚本做真正执行
- NetworkX 做基础后端
- 输出统一 JSON

