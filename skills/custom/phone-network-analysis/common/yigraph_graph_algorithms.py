# -*- coding: utf-8 -*-

"""
YiGraph 风格图算法公共层（纯 NetworkX 本地版）

用途：
1. 给 association-path-analysis 做桥接点排序
2. 给后续 subgraph / overlap skill 复用
3. 不依赖 Neo4j / AAG / community
"""

from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import networkx as nx


def shortest_path_nodes(
    graph: nx.Graph,
    source: Any,
    target: Any,
) -> Optional[List[Any]]:
    if graph is None:
        raise ValueError("graph is None")
    if source not in graph or target not in graph:
        return None
    try:
        return nx.shortest_path(graph, source=source, target=target)
    except nx.NetworkXNoPath:
        return None


def connected_components_summary(graph: nx.Graph) -> Dict[str, Any]:
    if graph is None:
        raise ValueError("graph is None")

    if graph.is_directed():
        comps = list(nx.weakly_connected_components(graph))
        return {
            "is_directed": True,
            "components_count": len(comps),
            "largest_component_size": max((len(c) for c in comps), default=0),
        }

    comps = list(nx.connected_components(graph))
    return {
        "is_directed": False,
        "components_count": len(comps),
        "largest_component_size": max((len(c) for c in comps), default=0),
    }


def degree_centrality_topk(
    graph: nx.Graph,
    top_k: int = 10,
) -> List[Dict[str, Any]]:
    if graph is None:
        raise ValueError("graph is None")

    scores = nx.degree_centrality(graph)
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
    return [
        {
            "node": node,
            "degree_centrality": score,
            "degree": int(graph.degree(node)),
        }
        for node, score in ranked
    ]


def _normalize_candidate_path(path_obj: Any) -> List[Any]:
    if isinstance(path_obj, list):
        return path_obj

    if isinstance(path_obj, tuple):
        return list(path_obj)

    if isinstance(path_obj, dict):
        for key in ("path_nodes", "path", "nodes"):
            value = path_obj.get(key)
            if isinstance(value, list):
                return value

    return []


def extract_local_subgraph(
    graph: nx.Graph,
    centers: Sequence[Any],
    radius: int = 2,
    max_nodes: int = 300,
) -> nx.Graph:
    if graph is None:
        raise ValueError("graph is None")

    selected = set()
    for center in centers:
        if center not in graph:
            continue
        ego = nx.ego_graph(graph, center, radius=radius, undirected=not graph.is_directed())
        for node in ego.nodes():
            selected.add(node)
            if len(selected) >= max_nodes:
                break
        if len(selected) >= max_nodes:
            break

    if not selected:
        return graph.subgraph([]).copy()

    return graph.subgraph(selected).copy()


def betweenness_centrality_topk(
    graph: nx.Graph,
    top_k: int = 10,
) -> List[Dict[str, Any]]:
    if graph is None:
        raise ValueError("graph is None")

    if graph.number_of_nodes() == 0:
        return []

    scores = nx.betweenness_centrality(graph)
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
    return [
        {
            "node": node,
            "betweenness_centrality": score,
            "degree": int(graph.degree(node)),
        }
        for node, score in ranked
    ]


def bridge_node_ranking(
    graph: nx.Graph,
    candidate_paths: Sequence[Any],
    local_radius: int = 2,
    max_local_nodes: int = 300,
    top_k: int = 10,
) -> List[Dict[str, Any]]:
    """
    对候选路径里的桥接点做简易排序：
    - 在候选路径中出现次数
    - 局部子图介数中心性
    - 局部度
    """
    if graph is None:
        raise ValueError("graph is None")

    bridge_counter: Counter = Counter()

    normalized_paths: List[List[Any]] = []
    for item in candidate_paths:
        path_nodes = _normalize_candidate_path(item)
        if len(path_nodes) >= 3:
            normalized_paths.append(path_nodes)
            for node in path_nodes[1:-1]:
                bridge_counter[node] += 1

    if not bridge_counter:
        return []

    bridge_nodes = [node for node, _ in bridge_counter.most_common()]
    local_graph = extract_local_subgraph(
        graph=graph,
        centers=bridge_nodes,
        radius=local_radius,
        max_nodes=max_local_nodes,
    )

    if local_graph.number_of_nodes() == 0:
        return [
            {
                "node": node,
                "path_frequency": freq,
                "local_betweenness_centrality": 0.0,
                "local_degree": 0,
                "score": float(freq),
            }
            for node, freq in bridge_counter.most_common(top_k)
        ]

    bc_scores = nx.betweenness_centrality(local_graph)
    ranked: List[Dict[str, Any]] = []

    for node, freq in bridge_counter.items():
        local_degree = int(local_graph.degree(node)) if node in local_graph else 0
        bc = float(bc_scores.get(node, 0.0))
        score = (freq * 3.0) + (bc * 10.0) + (local_degree * 0.05)
        ranked.append(
            {
                "node": node,
                "path_frequency": int(freq),
                "local_betweenness_centrality": bc,
                "local_degree": local_degree,
                "score": score,
            }
        )

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked[:top_k]


if __name__ == "__main__":
    G = nx.Graph()
    G.add_edges_from(
        [
            ("A", "X"),
            ("X", "B"),
            ("A", "Y"),
            ("Y", "B"),
            ("X", "Z"),
        ]
    )

    candidate_paths = [
        ["A", "X", "B"],
        ["A", "Y", "B"],
        ["A", "X", "Z", "B"],
    ]

    print(shortest_path_nodes(G, "A", "B"))
    print(connected_components_summary(G))
    print(degree_centrality_topk(G, top_k=3))
    print(betweenness_centrality_topk(G, top_k=3))
    print(bridge_node_ranking(G, candidate_paths, top_k=3))
