# Graph Query（节选，供 overlap-analysis 使用）

## 本 skill 实际复用的 YiGraph 思路

`overlap-analysis` 主要对齐 YiGraph 里的 3 类查询能力：

### 1. common_neighbor
用于回答：
- 两个节点有没有共同邻居
- 共同邻居有多少
- 哪些共同邻居最值得看

在电话网络里，对应：
- 两个号码有没有共同对端
- 共同对端是否构成异常重叠

### 2. relationship_filter
用于回答：
- 某类关系是否存在
- 某类关系有多少条
- 某类关系是否满足某些筛选条件

在电话网络里，对应：
- 是否存在共享设备关系
- 共享设备数量是多少

### 3. aggregation_query
用于回答：
- 计数、求和、分组聚合、比例判断

在电话网络里，对应：
- 共同对端数
- 共享设备数
- Jaccard 重叠率
- 强弱等级划分
