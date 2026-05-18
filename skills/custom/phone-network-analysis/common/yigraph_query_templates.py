#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
YiGraph 风格的轻量 query 模板定义。
注意：
1. 这不是原始 YiGraph 的直接拷贝。
2. 这是面向 DeerFlow + 电话网络任务的“本地适配版”。
3. 当前先重点支持 path_query，其他类型先保留模板定义，后续逐步接上。
"""

YIGRAPH_QUERY_TEMPLATES = {
    "path_query": {
        "description": "查询两个号码之间的关联路径",
        "required_params": ["phone_a", "phone_b"],
        "optional_params": [
            "graph_path",
            "graph_format",
            "source_col",
            "target_col",
            "directed",
        ],
        "backend_operator": "path_trace",
        "skill_name": "association-path-analysis",
    },
    "neighbor_query": {
        "description": "查询某个号码的一跳或多跳邻居",
        "required_params": ["phone_id"],
        "optional_params": [
            "hops",
            "graph_path",
            "graph_format",
            "source_col",
            "target_col",
            "directed",
            "max_return",
        ],
        "backend_operator": "expand_neighbors",
        "skill_name": "graph-neighbor-analysis",
    },
    "subgraph": {
        "description": "抽取某个号码周围的局部子图",
        "required_params": ["center_node"],
        "optional_params": [
            "hops",
            "max_nodes",
            "graph_path",
            "graph_format",
            "source_col",
            "target_col",
            "directed",
        ],
        "backend_operator": "subgraph_extract",
        "skill_name": "subgraph-extraction-analysis",
    },
    "common_neighbor": {
        "description": "查询两个号码的共同联系人或共同关系对象",
        "required_params": ["phone_a", "phone_b"],
        "optional_params": [
            "graph_path",
            "graph_format",
            "source_col",
            "target_col",
            "max_return",
        ],
        "backend_operator": "common_counterparty",
        "skill_name": "common-relation-analysis",
    },
    "aggregation_query": {
        "description": "做基础聚合统计，比如全图规模、度分布概览等",
        "required_params": [],
        "optional_params": [
            "graph_path",
            "graph_format",
            "source_col",
            "target_col",
            "directed",
        ],
        "backend_operator": "basic_graph_metrics",
        "skill_name": "graph-metrics-analysis",
    },
}


def get_supported_query_types():
    return list(YIGRAPH_QUERY_TEMPLATES.keys())


def get_query_template(query_type: str):
    return YIGRAPH_QUERY_TEMPLATES.get(query_type)


if __name__ == "__main__":
    import json
    print(json.dumps(YIGRAPH_QUERY_TEMPLATES, ensure_ascii=False, indent=2))
