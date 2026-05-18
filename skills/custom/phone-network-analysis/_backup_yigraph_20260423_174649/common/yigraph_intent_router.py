#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
轻量版 YiGraph 风格问题路由器。
当前先用规则法，不直接复用原始 nl_query_engine.py。
原因：
1. 原文件依赖 aag / Reasoner / Neo4j 等完整环境；
2. 你当前目标是先在 DeerFlow 里快速做出可运行版本；
3. 所以这里先做“稳定可控的本地规则路由版”。
"""

import re


PATH_KEYWORDS = [
    "路径", "最短路", "最短路径", "怎么联系", "怎么关联", "中间经过谁",
    "几跳", "桥接", "链路", "trace", "path"
]

NEIGHBOR_KEYWORDS = [
    "邻居", "周围", "扩展", "一跳", "二跳", "联系人", "关联对象",
    "neighbor", "neighbors"
]

SUBGRAPH_KEYWORDS = [
    "子图", "局部图", "关系图", "局部关系", "围绕这个号码", "ego",
    "subgraph"
]

COMMON_KEYWORDS = [
    "共同联系人", "共同对端", "共同关系", "共同联系过", "都联系过谁",
    "共同设备", "共享设备", "common"
]

AGGREGATION_KEYWORDS = [
    "统计", "概览", "全图指标", "规模", "排名", "top", "metrics",
    "基础指标", "聚合"
]


def normalize_query(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def classify_query_intent(user_query: str) -> str:
    q = normalize_query(user_query)

    if any(k in q for k in [kw.lower() for kw in PATH_KEYWORDS]):
        return "path_query"

    if any(k in q for k in [kw.lower() for kw in COMMON_KEYWORDS]):
        return "common_neighbor"

    if any(k in q for k in [kw.lower() for kw in SUBGRAPH_KEYWORDS]):
        return "subgraph"

    if any(k in q for k in [kw.lower() for kw in AGGREGATION_KEYWORDS]):
        return "aggregation_query"

    if any(k in q for k in [kw.lower() for kw in NEIGHBOR_KEYWORDS]):
        return "neighbor_query"

    # 默认兜底成 path_query，不乱猜太复杂的类型
    return "path_query"


def explain_intent(user_query: str) -> str:
    intent = classify_query_intent(user_query)
    mapping = {
        "path_query": "这句话更像是在问两个号码之间怎么连起来。",
        "common_neighbor": "这句话更像是在问两个号码有没有共同关系对象。",
        "subgraph": "这句话更像是在问某个号码周围的局部关系图。",
        "aggregation_query": "这句话更像是在问整体统计或概览。",
        "neighbor_query": "这句话更像是在问某个号码的邻居或扩展关系。",
    }
    return mapping.get(intent, "未识别。")


if __name__ == "__main__":
    tests = [
        "帮我看这两个号码之间怎么关联",
        "帮我找这个号码的一跳邻居",
        "围绕这个号码抽一个局部子图",
        "统计这个图的基础指标",
        "看看这两个号码有没有共同联系人",
    ]
    for t in tests:
        print(t, "=>", classify_query_intent(t), "|", explain_intent(t))
