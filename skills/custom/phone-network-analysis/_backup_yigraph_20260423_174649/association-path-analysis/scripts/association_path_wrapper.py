#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
PHONE_NETWORK_ROOT = CURRENT_FILE.parents[2]
COMMON_DIR = PHONE_NETWORK_ROOT / "common"

if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from yigraph_adapter import run_graph_path_query, resolve_default_call_graph_path
from composite_path_engine import (
    CompositePathEngine,
    resolve_call_graph_path,
    resolve_device_graph_path,
)


def summarize_direct_call(path_found: bool, path: list, length: int) -> str:
    if not path_found:
        return "在有向通话图里，没有找到从号码A到号码B的可达路径。"

    if length == 0:
        return "两个输入号码是同一个节点。"

    if length == 1:
        return "在有向通话图里，号码A可以直接到达号码B，说明存在直接通话关系。"

    bridge_count = max(0, len(path) - 2)
    if bridge_count == 1:
        return f"在有向通话图里，号码A通过 1 个中间桥接号码可以到达号码B，属于 2 跳通话链路。"

    return f"在有向通话图里，号码A经过 {bridge_count} 个中间节点后到达号码B，最短路径长度为 {length}。"


def summarize_composite(path_found: bool, relation_sequence: list, path_steps: list) -> str:
    if not path_found:
        return "在复合关系图里，也没有找到从号码A到号码B的可达路径。"

    if not path_steps:
        return "两个输入号码是同一个节点。"

    if len(path_steps) == 1:
        step = path_steps[0]
        rel = step["relation"]
        if rel == "call":
            return "在复合关系图里，这两个号码通过直接通话关系相连。"
        if rel == "shared_device":
            return "在复合关系图里，这两个号码通过共享设备直接相连，说明它们存在明显设备共用关系。"
        if rel == "common_counterparty":
            return "在复合关系图里，这两个号码通过共同对端直接相连，说明它们联系对象高度重叠。"
        return "在复合关系图里，这两个号码通过单跳关系直接相连。"

    rel_text = " -> ".join(relation_sequence)
    return (
        f"在复合关系图里，找到了一条长度为 {len(path_steps)} 的混合关系路径。"
        f"这条路径的关系序列是：{rel_text}。"
    )


def convert_direct_call_result(raw: dict) -> dict:
    if not raw.get("ok", False):
        return {
            "ok": False,
            "error": raw,
        }

    result = raw.get("result", {})
    path = result.get("path", []) or []
    path_found = bool(result.get("path_found", False) or path)
    length = result.get("length", None)
    if length is None and path_found:
        length = max(0, len(path) - 1)

    bridge_nodes = path[1:-1] if len(path) >= 3 else []

    return {
        "ok": True,
        "path_found": path_found,
        "path_length": length if path_found else None,
        "path_nodes": path if path_found else [],
        "middle_nodes_count": len(bridge_nodes),
        "bridge_nodes": bridge_nodes,
        "bridge_nodes_preview": bridge_nodes[:10],
        "human_summary": summarize_direct_call(path_found, path, length if length is not None else -1),
        "notes": raw.get("notes", []),
        "adapter_meta": raw.get("adapter_meta", {}),
    }


def main():
    parser = argparse.ArgumentParser(description="Association path analysis wrapper (enhanced)")
    parser.add_argument("--phone-a", required=True, help="起始号码")
    parser.add_argument("--phone-b", required=True, help="目标号码")

    parser.add_argument(
        "--analysis-mode",
        choices=["direct_call", "composite", "both"],
        default="both",
        help="分析模式：direct_call / composite / both",
    )

    parser.add_argument("--call-graph-path", default=None, help="通话边文件路径")
    parser.add_argument("--device-graph-path", default=None, help="号码-设备关系文件路径")

    parser.add_argument("--graph-format", default="csv", help="通话图格式")
    parser.add_argument("--source-col", default="src_user_id", help="通话图源列")
    parser.add_argument("--target-col", default="dst_counterparty_id", help="通话图目标列")

    parser.add_argument("--max-hops", type=int, default=4, help="复合路径最大跳数")
    parser.add_argument("--per-relation-limit", type=int, default=100, help="每种关系扩展时的最大邻居数")
    parser.add_argument("--max-expand-nodes", type=int, default=5000, help="复合路径搜索时最大展开节点数")

    parser.add_argument("--directed-call", dest="directed_call", action="store_true", default=True, help="通话关系按有向图处理（默认开启）")
    parser.add_argument("--undirected-call", dest="directed_call", action="store_false", help="通话关系按无向方式扩展")

    parser.add_argument("--disable-call", action="store_true", help="复合路径里禁用通话关系")
    parser.add_argument("--disable-shared-device", action="store_true", help="复合路径里禁用共享设备关系")
    parser.add_argument("--disable-common-counterparty", action="store_true", help="复合路径里禁用共同对端关系")

    args = parser.parse_args()

    call_graph_path = resolve_call_graph_path(args.call_graph_path or resolve_default_call_graph_path())
    device_graph_path = resolve_device_graph_path(args.device_graph_path)

    output = {
        "ok": True,
        "skill": "association-path-analysis",
        "query_type": "path_query",
        "input_summary": {
            "phone_a": args.phone_a,
            "phone_b": args.phone_b,
            "analysis_mode": args.analysis_mode,
            "call_graph_path": call_graph_path,
            "device_graph_path": device_graph_path,
            "graph_format": args.graph_format,
            "source_col": args.source_col,
            "target_col": args.target_col,
            "directed_call": args.directed_call,
            "max_hops": args.max_hops,
            "per_relation_limit": args.per_relation_limit,
            "max_expand_nodes": args.max_expand_nodes,
        },
        "result": {},
        "notes": [],
    }

    # 先做 pair-level 关系信号和 composite engine 初始化
    engine = CompositePathEngine(
        call_graph_path=call_graph_path,
        device_graph_path=device_graph_path,
        per_relation_limit=args.per_relation_limit,
    )

    pair_signals = engine.get_pair_signals(
        phone_a=args.phone_a,
        phone_b=args.phone_b,
        preview_limit=10,
    )
    output["result"]["pair_relation_signals"] = pair_signals

    # direct call analysis
    if args.analysis_mode in ("direct_call", "both"):
        raw_direct = run_graph_path_query(
            phone_a=args.phone_a,
            phone_b=args.phone_b,
            graph_path=call_graph_path,
            graph_format=args.graph_format,
            source_col=args.source_col,
            target_col=args.target_col,
            directed=args.directed_call,
        )
        output["result"]["direct_call_analysis"] = convert_direct_call_result(raw_direct)

    # composite analysis
    if args.analysis_mode in ("composite", "both"):
        composite_result = engine.find_composite_path(
            source=args.phone_a,
            target=args.phone_b,
            max_hops=args.max_hops,
            directed_call=args.directed_call,
            enable_call=not args.disable_call,
            enable_shared_device=not args.disable_shared_device,
            enable_common_counterparty=not args.disable_common_counterparty,
            max_expand_nodes=args.max_expand_nodes,
        )

        composite_result["human_summary"] = summarize_composite(
            composite_result["path_found"],
            composite_result.get("relation_sequence", []),
            composite_result.get("path_steps", []),
        )
        output["result"]["composite_analysis"] = composite_result

    # 推荐视图
    if args.analysis_mode == "both":
        composite_ok = output["result"].get("composite_analysis", {}).get("path_found", False)
        direct_ok = output["result"].get("direct_call_analysis", {}).get("path_found", False)

        if composite_ok:
            output["result"]["recommended_view"] = "composite_analysis"
        elif direct_ok:
            output["result"]["recommended_view"] = "direct_call_analysis"
        else:
            output["result"]["recommended_view"] = "no_path_found"
    elif args.analysis_mode == "direct_call":
        output["result"]["recommended_view"] = "direct_call_analysis"
    else:
        output["result"]["recommended_view"] = "composite_analysis"

    # 总体中文总结
    pair_text = []
    if pair_signals["a_calls_b"]:
        pair_text.append("A直接打给过B")
    if pair_signals["b_calls_a"]:
        pair_text.append("B直接打给过A")
    if pair_signals["shared_device_count"] > 0:
        pair_text.append(f"两者共享设备 {pair_signals['shared_device_count']} 个")
    if pair_signals["common_counterparty_count"] > 0:
        pair_text.append(f"两者共同对端 {pair_signals['common_counterparty_count']} 个")

    if not pair_text:
        pair_signal_summary = "这两个号码之间当前没有观察到直接通话、共享设备或共同对端这类强配对信号。"
    else:
        pair_signal_summary = "；".join(pair_text) + "。"

    output["result"]["pair_signal_summary"] = pair_signal_summary

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
