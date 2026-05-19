"""
Graph Analysis Action

Action handler for network communication graph analysis including
community detection, centrality analysis, and attack path discovery.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import append_file_errors, fetch_rows, format_dict_rows, present_fields, scoped_where
from analysis.graph_analysis import TrafficGraphAnalyzer


def execute_graph_analysis(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    **kwargs,
) -> dict:
    limit = kwargs.get("limit", 100)
    source_ip = kwargs.get("source_ip")
    target_ip = kwargs.get("target_ip")

    results = {
        "action": "graph-analysis",
        "files_analyzed": [],
        "summary": {
            "total_nodes": 0,
            "total_edges": 0,
            "communities_found": 0,
            "anomalous_nodes": 0,
            "attack_paths_found": 0,
        },
        "graph_data": None,
    }

    analyzer = TrafficGraphAnalyzer()
    available = present_fields(mappings)
    file_result = {
        "file": files[0] if files else "selected scope",
        "graph_summary": "",
        "communities": [],
        "central_nodes": [],
        "anomalous_nodes": [],
        "attack_paths": [],
    }

    try:
        if not {"src_ip", "dst_ip"}.issubset(available):
            file_result["error"] = "Graph analysis requires src_ip and dst_ip in the canonical flow view."
            results["files_analyzed"].append(file_result)
            return results

        sql = f"""
            SELECT
                src_ip,
                dst_ip,
                COALESCE(dst_port, 0) AS dst_port,
                COALESCE(protocol, 'UNKNOWN') AS protocol,
                COALESCE(bytes, 0) AS bytes,
                COALESCE(packets, 0) AS packets,
                COALESCE(analysis_time_display, '') AS timestamp
            FROM flows
            {scoped_where(where_clause, "src_ip IS NOT NULL AND dst_ip IS NOT NULL")}
            ORDER BY COALESCE(bytes, 0) DESC, COALESCE(packets, 0) DESC
            LIMIT {limit}
        """
        flows = fetch_rows(con, sql)
        graph_result = analyzer.analyze_graph(flows)
        file_result["graph_summary"] = graph_result.graph_summary
        file_result["communities"] = graph_result.communities[:10]
        file_result["central_nodes"] = graph_result.central_nodes[:10]
        file_result["anomalous_nodes"] = graph_result.anomalous_nodes[:10]
        results["summary"]["total_nodes"] += graph_result.node_count
        results["summary"]["total_edges"] += graph_result.edge_count
        results["summary"]["communities_found"] += len(graph_result.communities)
        results["summary"]["anomalous_nodes"] += len(graph_result.anomalous_nodes)

        graph = analyzer.build_graph(flows)
        if source_ip and target_ip:
            attack_paths = analyzer.discover_attack_paths(graph, source_ip, target_ip)
            file_result["attack_paths"] = attack_paths
            results["summary"]["attack_paths_found"] += len(attack_paths)

        file_result["graph_data"] = {
            "nodes": graph["nodes"][:50],
            "edges": [{"src": edge["src"], "dst": edge["dst"], "bytes": edge["total_bytes"]} for edge in graph["edges"][:100]],
        }
        results["files_analyzed"].append(file_result)
        results["graph_data"] = file_result["graph_data"]
    except Exception as e:
        file_result["error"] = str(e)
        results["files_analyzed"].append(file_result)

    return results


def format_results(results: dict) -> str:
    output = []
    output.append("# Graph Analysis Results\n")

    summary = results["summary"]
    output.append("## Summary\n")
    output.append(f"- **Total Nodes**: {summary['total_nodes']}")
    output.append(f"- **Total Edges**: {summary['total_edges']}")
    output.append(f"- **Communities Found**: {summary['communities_found']}")
    output.append(f"- **Anomalous Nodes**: {summary['anomalous_nodes']}")
    output.append(f"- **Attack Paths Found**: {summary['attack_paths_found']}\n")

    for file_result in results["files_analyzed"]:
        output.append(f"\n## File: {file_result['file']}\n")
        output.append(f"**{file_result['graph_summary']}**\n")

        if file_result.get("communities"):
            output.append("### Top Communities\n")
            community_table = []
            for comm in file_result["communities"][:5]:
                community_table.append(
                    {
                        "community_id": comm["community_id"],
                        "member_count": comm["member_count"],
                        "density": f"{comm['density']:.3f}",
                        "sample_members": ", ".join(comm["members"][:3]),
                    }
                )
            output.append(format_dict_rows(community_table))

        if file_result.get("central_nodes"):
            output.append("\n### Most Central Nodes\n")
            output.append(format_dict_rows(file_result["central_nodes"][:10]))

        if file_result.get("anomalous_nodes"):
            output.append("\n### Anomalous Nodes\n")
            anomalous_table = []
            for node in file_result["anomalous_nodes"][:10]:
                anomalous_table.append(
                    {
                        "node": node["node"],
                        "degree_centrality": f"{node['degree_centrality']:.4f}",
                        "total_bytes": node["total_bytes"],
                        "anomaly_reasons": ", ".join(node["anomaly_reasons"]),
                    }
                )
            output.append(format_dict_rows(anomalous_table))

        if file_result.get("attack_paths"):
            output.append("\n### Attack Paths\n")
            for index, path in enumerate(file_result["attack_paths"], 1):
                output.append(f"{index}. {path['path_string']} ({path['hop_count']} hops)")

    append_file_errors(output, results)
    return "\n".join(output)


def build_skill_result_parts(results: dict, raw_output: str) -> dict[str, Any]:
    summary = results.get("summary", {})
    files = results.get("files_analyzed", [])
    errors = [
        {"file": item.get("file", "selected scope"), "error": item["error"]}
        for item in files
        if item.get("error")
    ]

    anomalous_nodes = []
    central_nodes = []
    communities = []
    attack_paths = []
    for file_result in files:
        anomalous_nodes.extend(file_result.get("anomalous_nodes") or [])
        central_nodes.extend(file_result.get("central_nodes") or [])
        communities.extend(file_result.get("communities") or [])
        attack_paths.extend(file_result.get("attack_paths") or [])

    findings: list[dict[str, Any]] = []
    for index, node in enumerate(anomalous_nodes[:20], 1):
        reasons = node.get("anomaly_reasons") or []
        findings.append(
            {
                "finding_id": f"f-graph-node-{index:03d}",
                "type": "graph_anomalous_node",
                "severity": "medium",
                "confidence": 0.7,
                "title": f"Graph outlier node {node.get('node', 'unknown')}",
                "description": ", ".join(reasons) if reasons else "Node ranked as anomalous by graph-derived traffic features.",
                "entities": [{"type": "ip", "value": node.get("node", "")}],
                "evidence_refs": ["e-anomalous-nodes", "e-communication-graph"],
                "recommended_actions": [
                    "Review this node in session-review to validate whether centrality reflects expected service behavior.",
                    "Correlate high-byte or high-degree graph outliers with threat-intel-match and protocol-review.",
                ],
            }
        )

    for index, path in enumerate(attack_paths[:10], len(findings) + 1):
        findings.append(
            {
                "finding_id": f"f-graph-path-{index:03d}",
                "type": "attack_path_candidate",
                "severity": "medium",
                "confidence": 0.65,
                "title": "Candidate communication path",
                "description": path.get("path_string", "Attack path candidate found between requested endpoints."),
                "entities": [{"type": "path", "value": path.get("path_string", "")}],
                "evidence_refs": ["e-attack-paths", "e-communication-graph"],
                "recommended_actions": [
                    "Use this path as a traversal hypothesis and validate each hop with timestamped session evidence."
                ],
            }
        )

    evidence: list[dict[str, Any]] = []
    if results.get("graph_data"):
        evidence.append(
            {
                "evidence_id": "e-communication-graph",
                "type": "graph",
                "title": "Communication Graph",
                "content": results["graph_data"],
            }
        )
    if central_nodes:
        evidence.append(
            {
                "evidence_id": "e-central-nodes",
                "type": "table",
                "title": "Most Central Nodes",
                "columns": list(central_nodes[0].keys()),
                "rows": central_nodes[:20],
            }
        )
    if anomalous_nodes:
        evidence.append(
            {
                "evidence_id": "e-anomalous-nodes",
                "type": "table",
                "title": "Anomalous Nodes",
                "columns": list(anomalous_nodes[0].keys()),
                "rows": anomalous_nodes[:20],
            }
        )
    if communities:
        evidence.append(
            {
                "evidence_id": "e-communities",
                "type": "table",
                "title": "Graph Communities",
                "columns": list(communities[0].keys()),
                "rows": communities[:20],
            }
        )
    if attack_paths:
        evidence.append(
            {
                "evidence_id": "e-attack-paths",
                "type": "table",
                "title": "Attack Path Candidates",
                "columns": list(attack_paths[0].keys()),
                "rows": attack_paths[:10],
            }
        )
    evidence.append(
        {
            "evidence_id": "e-raw-report",
            "type": "text",
            "title": "Raw Graph Analysis Report",
            "content": raw_output,
        }
    )

    return {
        "summary": {
            "title": "Graph Analysis",
            "overview": (
                f"Built a communication graph with {summary.get('total_nodes', 0)} nodes, "
                f"{summary.get('total_edges', 0)} edges, and {summary.get('anomalous_nodes', 0)} anomalous nodes."
            ),
            "severity": "medium" if summary.get("anomalous_nodes", 0) or summary.get("attack_paths_found", 0) else "info",
            "confidence": 0.72,
            "key_metrics": [
                {"name": "total_nodes", "value": summary.get("total_nodes", 0)},
                {"name": "total_edges", "value": summary.get("total_edges", 0)},
                {"name": "communities_found", "value": summary.get("communities_found", 0)},
                {"name": "anomalous_nodes", "value": summary.get("anomalous_nodes", 0)},
                {"name": "attack_paths_found", "value": summary.get("attack_paths_found", 0)},
            ],
        },
        "findings": findings,
        "evidence": evidence,
        "diagnostics": {
            "warnings": [
                "Graph anomalies are topology and traffic-volume indicators; validate with protocol and session evidence before assigning intent."
            ],
            "data_quality": {
                "files_with_errors": len(errors),
                "central_nodes_returned": len(central_nodes),
                "anomalous_nodes_returned": len(anomalous_nodes),
            },
            "errors": errors,
        },
    }
