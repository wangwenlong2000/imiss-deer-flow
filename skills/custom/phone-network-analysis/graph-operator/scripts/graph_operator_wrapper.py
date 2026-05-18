#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import networkx as nx
import pandas as pd


def resolve_graph_path(graph_path: str) -> str:
    candidates = []

    raw = str(graph_path)
    candidates.append(Path(raw))

    if raw.startswith("/mnt/user-data/workspace/imiss-deer-flow-main/datasets/phone-network/"):
        candidates.append(
            Path(
                raw.replace(
                    "/mnt/user-data/workspace/imiss-deer-flow-main/datasets/phone-network/",
                    "/mnt/datasets/phone-network/",
                    1,
                )
            )
        )

    if raw.startswith("/workspace/imiss-deer-flow-main/datasets/phone-network/"):
        candidates.append(
            Path(
                raw.replace(
                    "/workspace/imiss-deer-flow-main/datasets/phone-network/",
                    "/mnt/datasets/phone-network/",
                    1,
                )
            )
        )

    if raw.startswith("datasets/phone-network/"):
        candidates.append(
            Path(
                raw.replace(
                    "datasets/phone-network/",
                    "/mnt/datasets/phone-network/",
                    1,
                )
            )
        )

    for p in candidates:
        if p.exists():
            return str(p)

    raise FileNotFoundError(
        f"Graph file not found. Tried: {[str(p) for p in candidates]}"
    )


def try_resolve_graph_path(graph_path: str) -> Optional[str]:
    try:
        return resolve_graph_path(graph_path)
    except Exception:
        return None


CSV_READ_KWARGS = {
    "low_memory": False,
}


def ok_response(
    operator: str,
    input_summary: Dict[str, Any],
    result: Dict[str, Any],
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "ok": True,
        "operator": operator,
        "input_summary": input_summary,
        "result": result,
        "notes": notes or [],
    }


def err_response(
    operator: str,
    message: str,
    input_summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "ok": False,
        "operator": operator,
        "input_summary": input_summary or {},
        "error": message,
        "notes": [],
    }


def _normalize_node_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _safe_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    try:
        return value.item() if hasattr(value, "item") else value
    except Exception:
        return value


def _row_to_clean_dict(row: pd.Series) -> Dict[str, Any]:
    result = {}
    for k, v in row.to_dict().items():
        result[str(k)] = _safe_scalar(v)
    return result


def build_graph_from_csv(path: str, source_col: str, target_col: str, directed: bool) -> nx.Graph:
    df = pd.read_csv(path, usecols=[source_col, target_col], **CSV_READ_KWARGS)

    if source_col not in df.columns or target_col not in df.columns:
        raise ValueError(f"CSV 中找不到列: {source_col}, {target_col}. 当前列为: {list(df.columns)}")

    df[source_col] = _normalize_node_series(df[source_col])
    df[target_col] = _normalize_node_series(df[target_col])
    df = df[(df[source_col] != "") & (df[target_col] != "")].copy()

    graph_cls = nx.DiGraph if directed else nx.Graph
    g = nx.from_pandas_edgelist(
        df,
        source=source_col,
        target=target_col,
        create_using=graph_cls(),
    )
    return g


def build_graph_from_edgelist(path: str, directed: bool) -> nx.Graph:
    graph_cls = nx.DiGraph if directed else nx.Graph
    return nx.read_edgelist(path, create_using=graph_cls(), nodetype=str)


def build_graph_from_graphml(path: str, directed: bool) -> nx.Graph:
    g = nx.read_graphml(path)
    if directed and not g.is_directed():
        g = nx.DiGraph(g)
    if not directed and g.is_directed():
        g = nx.Graph(g)
    return g


def build_graph_from_json(path: str, directed: bool) -> nx.Graph:
    from networkx.readwrite import json_graph

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    g = json_graph.node_link_graph(data, directed=directed)
    return g


def load_graph(path: str, graph_format: str, source_col: str, target_col: str, directed: bool) -> nx.Graph:
    path = resolve_graph_path(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"图文件不存在: {path}")

    fmt = graph_format.lower()
    if fmt == "csv":
        return build_graph_from_csv(path, source_col, target_col, directed)
    if fmt == "parquet":
        df = pd.read_parquet(path, columns=[source_col, target_col])
        if source_col not in df.columns or target_col not in df.columns:
            raise ValueError(f"Parquet 中找不到列: {source_col}, {target_col}. 当前列为: {list(df.columns)}")
        df[source_col] = _normalize_node_series(df[source_col])
        df[target_col] = _normalize_node_series(df[target_col])
        df = df[(df[source_col] != "") & (df[target_col] != "")].copy()

        graph_cls = nx.DiGraph if directed else nx.Graph
        return nx.from_pandas_edgelist(
            df,
            source=source_col,
            target=target_col,
            create_using=graph_cls(),
        )
    if fmt == "edgelist":
        return build_graph_from_edgelist(path, directed)
    if fmt == "graphml":
        return build_graph_from_graphml(path, directed)
    if fmt == "json":
        return build_graph_from_json(path, directed)

    raise ValueError(f"不支持的 graph-format: {graph_format}")


def graph_summary(g: nx.Graph) -> Dict[str, Any]:
    num_nodes = g.number_of_nodes()
    num_edges = g.number_of_edges()

    if g.is_directed():
        in_degree_dict = dict(g.in_degree())
        out_degree_dict = dict(g.out_degree())
        total_degree_dict = {
            n: in_degree_dict.get(n, 0) + out_degree_dict.get(n, 0)
            for n in g.nodes()
        }
        degree_values = list(total_degree_dict.values())
        connected_components_count = nx.number_weakly_connected_components(g)

        return {
            "num_nodes": num_nodes,
            "num_edges": num_edges,
            "is_directed": True,
            "weakly_connected_components_count": connected_components_count,
            "max_in_degree": max(in_degree_dict.values()) if in_degree_dict else 0,
            "max_out_degree": max(out_degree_dict.values()) if out_degree_dict else 0,
            "avg_in_degree": (sum(in_degree_dict.values()) / len(in_degree_dict)) if in_degree_dict else 0.0,
            "avg_out_degree": (sum(out_degree_dict.values()) / len(out_degree_dict)) if out_degree_dict else 0.0,
            "max_total_degree": max(degree_values) if degree_values else 0,
            "min_total_degree": min(degree_values) if degree_values else 0,
            "avg_total_degree": (sum(degree_values) / len(degree_values)) if degree_values else 0.0,
            "top_out_degree_nodes": sorted(out_degree_dict.items(), key=lambda x: (-x[1], str(x[0])))[:10],
            "top_in_degree_nodes": sorted(in_degree_dict.items(), key=lambda x: (-x[1], str(x[0])))[:10],
            "top_total_degree_nodes": sorted(total_degree_dict.items(), key=lambda x: (-x[1], str(x[0])))[:10],
        }

    degree_dict = dict(g.degree())
    degree_values = list(degree_dict.values())
    connected_components_count = nx.number_connected_components(g)

    return {
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "is_directed": False,
        "connected_components_count": connected_components_count,
        "max_degree": max(degree_values) if degree_values else 0,
        "min_degree": min(degree_values) if degree_values else 0,
        "avg_degree": (sum(degree_values) / len(degree_values)) if degree_values else 0.0,
        "top_degree_nodes": sorted(degree_dict.items(), key=lambda x: (-x[1], str(x[0])))[:10],
    }


def export_graph(g: nx.Graph, output_path: str, export_format: str) -> Dict[str, Any]:
    fmt = export_format.lower()
    if fmt == "edgelist":
        nx.write_edgelist(g, output_path, data=False)
    elif fmt == "graphml":
        nx.write_graphml(g, output_path)
    elif fmt == "json":
        from networkx.readwrite import json_graph

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(json_graph.node_link_data(g), f, ensure_ascii=False, indent=2)
    else:
        raise ValueError(f"不支持的 export-format: {export_format}")

    return {
        "output_path": output_path,
        "export_format": fmt,
        "num_nodes": g.number_of_nodes(),
        "num_edges": g.number_of_edges(),
    }


def load_pair_columns(path: str, graph_format: str, source_col: str, target_col: str) -> pd.DataFrame:
    path = resolve_graph_path(path)
    fmt = graph_format.lower()

    if fmt == "csv":
        df = pd.read_csv(
            path,
            usecols=[source_col, target_col],
            dtype={source_col: "string", target_col: "string"},
            low_memory=False,
        )
    elif fmt == "parquet":
        df = pd.read_parquet(path, columns=[source_col, target_col])
        df[source_col] = df[source_col].astype("string")
        df[target_col] = df[target_col].astype("string")
    else:
        raise ValueError(f"该 operator 仅支持 csv/parquet，当前是: {graph_format}")

    df = df.dropna(subset=[source_col, target_col]).copy()
    df[source_col] = df[source_col].astype(str).str.strip()
    df[target_col] = df[target_col].astype(str).str.strip()
    df = df[(df[source_col] != "") & (df[target_col] != "")]
    return df


def op_common_counterparty(
    graph_path: str,
    graph_format: str,
    source_col: str,
    target_col: str,
    phone_a: str,
    phone_b: str,
    max_return: int = 50,
) -> Dict[str, Any]:
    if not phone_a or not phone_b:
        raise ValueError("common_counterparty 需要 --phone-a 和 --phone-b")

    df = load_pair_columns(graph_path, graph_format, source_col, target_col)

    a_set = set(df.loc[df[source_col] == phone_a, target_col].tolist())
    b_set = set(df.loc[df[source_col] == phone_b, target_col].tolist())
    common = sorted(a_set & b_set)

    return {
        "phone_a": phone_a,
        "phone_b": phone_b,
        "phone_a_counterparty_count": len(a_set),
        "phone_b_counterparty_count": len(b_set),
        "common_count": len(common),
        "common_counterparties": common[:max_return],
        "returned_count": min(len(common), max_return),
        "truncated": len(common) > max_return,
    }


def op_common_device(
    graph_path: str,
    graph_format: str,
    user_col: str,
    device_col: str,
    phone_a: str,
    phone_b: str,
    max_return: int = 50,
) -> Dict[str, Any]:
    if not phone_a or not phone_b:
        raise ValueError("common_device 需要 --phone-a 和 --phone-b")

    df = load_pair_columns(graph_path, graph_format, user_col, device_col)

    a_set = set(df.loc[df[user_col] == phone_a, device_col].tolist())
    b_set = set(df.loc[df[user_col] == phone_b, device_col].tolist())
    common = sorted(a_set & b_set)

    return {
        "phone_a": phone_a,
        "phone_b": phone_b,
        "phone_a_device_count": len(a_set),
        "phone_b_device_count": len(b_set),
        "common_device_count": len(common),
        "common_devices": common[:max_return],
        "returned_count": min(len(common), max_return),
        "truncated": len(common) > max_return,
    }


def op_query_phone_node(
    graph_path: str,
    graph_format: str,
    id_col: str,
    phone_id: str,
    max_return: int = 10,
) -> Dict[str, Any]:
    if not phone_id:
        raise ValueError("query_phone_node 需要 --phone-id 或 --node")

    path = resolve_graph_path(graph_path)
    fmt = graph_format.lower()

    if fmt != "csv":
        raise ValueError("query_phone_node 当前要求 user_nodes.csv，因此 graph-format 必须是 csv")

    user_df = pd.read_csv(path, low_memory=False)
    if id_col not in user_df.columns:
        raise ValueError(f"user_nodes 文件中找不到主键列: {id_col}")

    user_df[id_col] = user_df[id_col].astype("string").fillna("").str.strip()
    matched = user_df[user_df[id_col] == phone_id].head(1)

    node_found = not matched.empty
    raw_node_attrs = _row_to_clean_dict(matched.iloc[0]) if node_found else {}

    notes: List[str] = []
    call_record_count = 0
    counterparty_count = 0
    sample_counterparties: List[str] = []
    device_count = 0
    sample_devices: List[str] = []

    call_path = path.replace("/processed/unified/user_nodes.csv", "/processed/unified/call_edges.csv")
    call_path = try_resolve_graph_path(call_path)
    if call_path:
        try:
            call_df = pd.read_csv(
                call_path,
                usecols=["src_user_id", "dst_counterparty_id"],
                dtype={"src_user_id": "string", "dst_counterparty_id": "string"},
                low_memory=False,
            ).dropna(subset=["src_user_id", "dst_counterparty_id"])
            call_df["src_user_id"] = call_df["src_user_id"].astype(str).str.strip()
            call_df["dst_counterparty_id"] = call_df["dst_counterparty_id"].astype(str).str.strip()

            phone_calls = call_df[call_df["src_user_id"] == phone_id]
            call_record_count = int(len(phone_calls))
            unique_counterparties = sorted(phone_calls["dst_counterparty_id"].drop_duplicates().tolist())
            counterparty_count = len(unique_counterparties)
            sample_counterparties = unique_counterparties[:max_return]
        except Exception as e:
            notes.append(f"读取 call_edges.csv 失败：{e}")

    device_path = path.replace(
        "/processed/unified/user_nodes.csv",
        "/processed/graph_views/unified/edges_phone_imei.parquet",
    )
    device_path = try_resolve_graph_path(device_path)
    if device_path:
        try:
            device_df = pd.read_parquet(device_path, columns=["user_id", "imei"]).dropna(subset=["user_id", "imei"])
            device_df["user_id"] = device_df["user_id"].astype("string").fillna("").str.strip()
            device_df["imei"] = device_df["imei"].astype("string").fillna("").str.strip()
            device_df = device_df[(device_df["user_id"] != "") & (device_df["imei"] != "")]
            devices = sorted(device_df.loc[device_df["user_id"] == phone_id, "imei"].drop_duplicates().tolist())
            device_count = len(devices)
            sample_devices = devices[:max_return]
        except Exception as e:
            notes.append(f"读取 edges_phone_imei.parquet 失败：{e}")

    return {
        "phone_id": phone_id,
        "node_found": node_found,
        "raw_node_attrs": raw_node_attrs,
        "call_record_count": call_record_count,
        "counterparty_count": counterparty_count,
        "sample_counterparties": sample_counterparties,
        "device_count": device_count,
        "sample_devices": sample_devices,
        "notes_preview": notes,
    }


def op_query_shared_device(
    graph_path: str,
    graph_format: str,
    user_col: str,
    device_col: str,
    phone_id: str,
    max_return: int = 10,
) -> Dict[str, Any]:
    if not phone_id:
        raise ValueError("query_shared_device 需要 --phone-id 或 --node")

    df = load_pair_columns(graph_path, graph_format, user_col, device_col)

    phone_devices = sorted(df.loc[df[user_col] == phone_id, device_col].drop_duplicates().tolist())

    shared_entries = []
    for device_id in phone_devices:
        shared_phones = sorted(
            [
                x for x in df.loc[df[device_col] == device_id, user_col].drop_duplicates().tolist()
                if x != phone_id
            ]
        )
        if shared_phones:
            shared_entries.append(
                {
                    "device_id": device_id,
                    "shared_phone_count": len(shared_phones),
                    "shared_phones": shared_phones[:max_return],
                    "shared_phones_returned": min(len(shared_phones), max_return),
                    "shared_phones_truncated": len(shared_phones) > max_return,
                }
            )

    shared_entries = sorted(
        shared_entries,
        key=lambda x: (-x["shared_phone_count"], str(x["device_id"]))
    )

    return {
        "phone_id": phone_id,
        "device_count": len(phone_devices),
        "devices": phone_devices[:max_return],
        "shared_device_count": len(shared_entries),
        "shared_devices": shared_entries[:max_return],
        "returned_shared_device_count": min(len(shared_entries), max_return),
        "truncated": len(shared_entries) > max_return,
    }


def op_path_trace(
    graph_path: str,
    graph_format: str,
    source_col: str,
    target_col: str,
    source: str,
    target: str,
    directed: bool = False,
) -> Dict[str, Any]:
    if not source or not target:
        raise ValueError("path_trace 需要 --source 和 --target，或者通过 --phone-a / --phone-b 映射")

    g = load_graph(graph_path, graph_format, source_col, target_col, directed)

    if source not in g:
        return {
            "source": source,
            "target": target,
            "path_found": False,
            "reason": f"图中不存在 source 节点: {source}",
            "path": [],
            "length": None,
        }

    if target not in g:
        return {
            "source": source,
            "target": target,
            "path_found": False,
            "reason": f"图中不存在 target 节点: {target}",
            "path": [],
            "length": None,
        }

    try:
        path = nx.shortest_path(g, source=source, target=target)
        return {
            "source": source,
            "target": target,
            "path_found": True,
            "path": path,
            "length": len(path) - 1,
        }
    except nx.NetworkXNoPath:
        return {
            "source": source,
            "target": target,
            "path_found": False,
            "reason": "两节点之间不存在路径",
            "path": [],
            "length": None,
        }


def op_subgraph_extract(
    graph_path: str,
    graph_format: str,
    source_col: str,
    target_col: str,
    center_node: str,
    hops: int = 1,
    max_nodes: int = 200,
    directed: bool = False,
) -> Dict[str, Any]:
    if not center_node:
        raise ValueError("subgraph_extract 需要 --center-node 或 --node")

    g = load_graph(graph_path, graph_format, source_col, target_col, directed)

    if center_node not in g:
        raise ValueError(f"图中不存在中心节点: {center_node}")

    if hops < 1:
        raise ValueError("hops 必须 >= 1")

    if max_nodes < 2:
        raise ValueError("max_nodes 至少应 >= 2")

    # 先找出 cutoff=hops 内的所有候选节点
    # directed=False 时，g 本身就是 Graph
    # directed=True 时，g 是 DiGraph，这里按有向可达关系取局部图
    lengths = nx.single_source_shortest_path_length(g, center_node, cutoff=hops)
    candidate_nodes = list(lengths.keys())

    # 先看“完整候选局部图”有多大
    candidate_subg = g.subgraph(candidate_nodes).copy()

    # 按 距离优先、度数优先、节点 id 稳定排序
    ranked_nodes = sorted(
        candidate_nodes,
        key=lambda n: (
            lengths[n],              # 先保留距离近的
            -g.degree(n),            # 再保留度数高的
            str(n),                  # 最后按 id 稳定排序
        ),
    )

    # 强制中心节点永远保留
    selected_nodes = [center_node]
    for n in ranked_nodes:
        if n == center_node:
            continue
        if len(selected_nodes) >= max_nodes:
            break
        selected_nodes.append(n)

    truncated = len(candidate_nodes) > len(selected_nodes)

    # 用保留下来的节点重新构造诱导子图
    subg = g.subgraph(selected_nodes).copy()

    # 兜底检查：中心节点必须在结果中
    if center_node not in subg:
        raise RuntimeError("subgraph_extract 截断后丢失了中心节点，这不应该发生")

    return {
        "center_node": center_node,
        "hops": hops,
        "candidate_num_nodes_before_truncation": candidate_subg.number_of_nodes(),
        "candidate_num_edges_before_truncation": candidate_subg.number_of_edges(),
        "num_nodes": subg.number_of_nodes(),
        "num_edges": subg.number_of_edges(),
        "nodes": list(subg.nodes()),
        "edges": list(subg.edges()),
        "is_directed": subg.is_directed(),
        "truncated": truncated,
        "center_node_included": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Graph operator wrapper based on NetworkX")

    parser.add_argument("--operator", required=True)
    parser.add_argument("--graph-path")
    parser.add_argument("--graph-format", default="csv")
    parser.add_argument("--source-col", default="src")
    parser.add_argument("--target-col", default="dst")
    parser.add_argument("--node")
    parser.add_argument("--source")
    parser.add_argument("--target")
    parser.add_argument("--nodes")
    parser.add_argument("--output-path")
    parser.add_argument("--export-format", default="graphml")
    parser.add_argument("--directed", action="store_true", help="按有向图处理：source -> target")

    parser.add_argument("--phone-a", default=None)
    parser.add_argument("--phone-b", default=None)
    parser.add_argument("--phone-id", default=None)
    parser.add_argument("--max-return", type=int, default=50)

    parser.add_argument("--center-node", default=None)
    parser.add_argument("--hops", type=int, default=1)
    parser.add_argument("--max-nodes", type=int, default=200)

    args = parser.parse_args()

    operator = args.operator
    input_summary: Dict[str, Any] = {
        "graph_path": args.graph_path,
        "graph_format": args.graph_format,
        "directed": args.directed,
    }

    try:
        if operator == "load_graph":
            if not args.graph_path:
                raise ValueError("load_graph 需要 --graph-path")
            resolved_path = resolve_graph_path(args.graph_path)
            input_summary["graph_path"] = resolved_path
            g = load_graph(args.graph_path, args.graph_format, args.source_col, args.target_col, args.directed)
            result = graph_summary(g)
            notes = []
            if not args.directed:
                notes.append("当前按无向图构建；若要保留通话方向，请增加 --directed。")
            print(json.dumps(ok_response(operator, input_summary, result, notes), ensure_ascii=False, indent=2))
            return

        if operator == "common_counterparty":
            if not args.graph_path:
                raise ValueError("common_counterparty 需要 --graph-path")
            resolved_path = resolve_graph_path(args.graph_path)
            input_summary.update({
                "graph_path": resolved_path,
                "phone_a": args.phone_a,
                "phone_b": args.phone_b,
                "source_col": args.source_col,
                "target_col": args.target_col,
            })
            result = op_common_counterparty(
                graph_path=args.graph_path,
                graph_format=args.graph_format,
                source_col=args.source_col,
                target_col=args.target_col,
                phone_a=args.phone_a,
                phone_b=args.phone_b,
                max_return=args.max_return,
            )
            print(json.dumps(ok_response(operator, input_summary, result), ensure_ascii=False, indent=2))
            return

        if operator == "common_device":
            if not args.graph_path:
                raise ValueError("common_device 需要 --graph-path")
            resolved_path = resolve_graph_path(args.graph_path)
            input_summary.update({
                "graph_path": resolved_path,
                "phone_a": args.phone_a,
                "phone_b": args.phone_b,
                "source_col": args.source_col,
                "target_col": args.target_col,
            })
            result = op_common_device(
                graph_path=args.graph_path,
                graph_format=args.graph_format,
                user_col=args.source_col,
                device_col=args.target_col,
                phone_a=args.phone_a,
                phone_b=args.phone_b,
                max_return=args.max_return,
            )
            print(json.dumps(ok_response(operator, input_summary, result), ensure_ascii=False, indent=2))
            return

        if operator == "query_phone_node":
            if not args.graph_path:
                raise ValueError("query_phone_node 需要 --graph-path")
            phone_id = args.phone_id or args.node
            resolved_path = resolve_graph_path(args.graph_path)
            input_summary.update({
                "graph_path": resolved_path,
                "phone_id": phone_id,
                "source_col": args.source_col,
            })
            result = op_query_phone_node(
                graph_path=args.graph_path,
                graph_format=args.graph_format,
                id_col=args.source_col,
                phone_id=phone_id,
                max_return=args.max_return,
            )
            print(json.dumps(ok_response(operator, input_summary, result), ensure_ascii=False, indent=2))
            return

        if operator == "query_shared_device":
            if not args.graph_path:
                raise ValueError("query_shared_device 需要 --graph-path")
            phone_id = args.phone_id or args.node
            resolved_path = resolve_graph_path(args.graph_path)
            input_summary.update({
                "graph_path": resolved_path,
                "phone_id": phone_id,
                "source_col": args.source_col,
                "target_col": args.target_col,
            })
            result = op_query_shared_device(
                graph_path=args.graph_path,
                graph_format=args.graph_format,
                user_col=args.source_col,
                device_col=args.target_col,
                phone_id=phone_id,
                max_return=args.max_return,
            )
            print(json.dumps(ok_response(operator, input_summary, result), ensure_ascii=False, indent=2))
            return

        if operator == "path_trace":
            if not args.graph_path:
                raise ValueError("path_trace 需要 --graph-path")

            source = args.source or args.phone_a
            target = args.target or args.phone_b

            resolved_path = resolve_graph_path(args.graph_path)
            input_summary.update({
                "graph_path": resolved_path,
                "source": source,
                "target": target,
                "source_col": args.source_col,
                "target_col": args.target_col,
                "directed": args.directed,
            })

            result = op_path_trace(
                graph_path=args.graph_path,
                graph_format=args.graph_format,
                source_col=args.source_col,
                target_col=args.target_col,
                source=source,
                target=target,
                directed=args.directed,
            )

            notes = ["当前 path_trace 第一版基于单图最短路径。"]
            if args.directed:
                notes.append("按有向路径计算，只允许沿 source -> target 方向搜索。")

            print(json.dumps(ok_response(operator, input_summary, result, notes), ensure_ascii=False, indent=2))
            return

        if operator == "subgraph_extract":
            if not args.graph_path:
                raise ValueError("subgraph_extract 需要 --graph-path")

            center_node = args.center_node or args.node
            resolved_path = resolve_graph_path(args.graph_path)
            input_summary.update({
                "graph_path": resolved_path,
                "center_node": center_node,
                "hops": args.hops,
                "max_nodes": args.max_nodes,
                "source_col": args.source_col,
                "target_col": args.target_col,
                "directed": args.directed,
            })

            result = op_subgraph_extract(
                graph_path=args.graph_path,
                graph_format=args.graph_format,
                source_col=args.source_col,
                target_col=args.target_col,
                center_node=center_node,
                hops=args.hops,
                max_nodes=args.max_nodes,
                directed=args.directed,
            )

            notes = ["当前 subgraph_extract 第一版基于 ego graph（局部邻域子图）。"]
            print(json.dumps(ok_response(operator, input_summary, result, notes), ensure_ascii=False, indent=2))
            return

        if operator in {
            "expand_neighbors",
            "shortest_path",
            "basic_graph_metrics",
            "extract_subgraph",
            "export_graph",
        }:
            if not args.graph_path:
                raise ValueError(f"{operator} 需要 --graph-path")
            resolved_path = resolve_graph_path(args.graph_path)
            input_summary["graph_path"] = resolved_path
            g = load_graph(args.graph_path, args.graph_format, args.source_col, args.target_col, args.directed)
        else:
            if operator not in {
                "load_graph",
                "common_counterparty",
                "common_device",
                "query_phone_node",
                "query_shared_device",
                "path_trace",
                "subgraph_extract",
            }:
                raise ValueError(f"不支持的 operator: {operator}")

        if operator == "expand_neighbors":
            if not args.node:
                raise ValueError("expand_neighbors 需要 --node")
            if args.node not in g:
                raise ValueError(f"图中不存在节点: {args.node}")

            neighbors = list(g.neighbors(args.node))
            input_summary.update({"node": args.node})
            result = {
                "node": args.node,
                "neighbor_count": len(neighbors),
                "neighbors": neighbors,
            }
            notes = []
            if args.directed:
                notes.append("当前返回的是该节点的后继邻居（successors / outgoing neighbors）。")
            print(json.dumps(ok_response(operator, input_summary, result, notes), ensure_ascii=False, indent=2))
            return

        if operator == "shortest_path":
            if not args.source or not args.target:
                raise ValueError("shortest_path 需要 --source 和 --target")
            if args.source not in g:
                raise ValueError(f"图中不存在 source 节点: {args.source}")
            if args.target not in g:
                raise ValueError(f"图中不存在 target 节点: {args.target}")

            path = nx.shortest_path(g, source=args.source, target=args.target)
            input_summary.update({"source": args.source, "target": args.target})
            result = {
                "path": path,
                "length": len(path) - 1,
            }
            notes = []
            if args.directed:
                notes.append("当前按有向路径计算，只允许沿 source -> target 方向搜索。")
            print(json.dumps(ok_response(operator, input_summary, result, notes), ensure_ascii=False, indent=2))
            return

        if operator == "basic_graph_metrics":
            result = graph_summary(g)
            notes = []
            if args.directed:
                notes.append("已按有向图统计入度、出度和弱连通分量。")
            else:
                notes.append("当前按无向图统计；若要区分呼入/呼出，请增加 --directed。")
            print(json.dumps(ok_response(operator, input_summary, result, notes), ensure_ascii=False, indent=2))
            return

        if operator == "extract_subgraph":
            if not args.nodes:
                raise ValueError("extract_subgraph 需要 --nodes，例如 A,B,C")
            nodes = [x.strip() for x in args.nodes.split(",") if x.strip()]
            missing = [n for n in nodes if n not in g]
            if missing:
                raise ValueError(f"这些节点不在图中: {missing}")
            subg = g.subgraph(nodes).copy()
            input_summary.update({"nodes": nodes})
            result = {
                "nodes": list(subg.nodes()),
                "edges": list(subg.edges()),
                "num_nodes": subg.number_of_nodes(),
                "num_edges": subg.number_of_edges(),
                "is_directed": subg.is_directed(),
            }
            print(json.dumps(ok_response(operator, input_summary, result), ensure_ascii=False, indent=2))
            return

        if operator == "export_graph":
            if not args.output_path:
                raise ValueError("export_graph 需要 --output-path")
            result = export_graph(g, args.output_path, args.export_format)
            input_summary.update({"output_path": args.output_path, "export_format": args.export_format})
            print(json.dumps(ok_response(operator, input_summary, result), ensure_ascii=False, indent=2))
            return

    except Exception as e:
        print(json.dumps(err_response(operator, str(e), input_summary), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()