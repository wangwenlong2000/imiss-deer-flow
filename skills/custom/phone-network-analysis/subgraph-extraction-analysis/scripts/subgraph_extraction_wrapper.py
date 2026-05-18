#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import networkx as nx


SCRIPT_DIR = Path(__file__).resolve().parent


def _repo_root() -> Path:
    candidates = [
        Path.cwd(),
        Path('/workspace/imiss-deer-flow-main'),
        Path.home() / 'imiss-deer-flow-main',
        SCRIPT_DIR.parents[4] if len(SCRIPT_DIR.parents) >= 5 else SCRIPT_DIR,
    ]
    for p in candidates:
        if (p / 'skills').exists() and (p / 'datasets').exists():
            return p
    return Path.cwd()


REPO_ROOT = _repo_root()


def _pick_python_bin() -> str:
    candidates = [
        REPO_ROOT / 'backend' / '.venv' / 'bin' / 'python',
        Path('/app/backend/.venv/bin/python'),
        Path('/usr/bin/python3'),
        Path('/usr/bin/python'),
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return sys.executable


PYTHON_BIN = _pick_python_bin()


def _resolve_path(explicit_path: Optional[str], relative_candidates: Sequence[str]) -> str:
    if explicit_path:
        return explicit_path
    base_candidates = [
        Path('/mnt/datasets/phone-network'),
        REPO_ROOT / 'datasets' / 'phone-network',
        Path('/workspace/imiss-deer-flow-main/datasets/phone-network'),
    ]
    for base in base_candidates:
        for rel in relative_candidates:
            p = base / rel
            if p.exists():
                return str(p)
    return str((REPO_ROOT / 'datasets' / 'phone-network' / relative_candidates[0]).resolve())


def _resolve_output_dir() -> Path:
    candidates = [
        Path('/mnt/user-data/outputs'),
        REPO_ROOT / 'backend' / '.deer-flow' / 'manual-outputs',
        REPO_ROOT / 'outputs',
    ]
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            continue
    return Path.cwd()


OUTPUT_DIR = _resolve_output_dir()


def _graph_operator_path() -> str:
    candidates = [
        REPO_ROOT / 'skills' / 'custom' / 'phone-network-analysis' / 'graph-operator' / 'scripts' / 'graph_operator_wrapper.py',
        Path('/mnt/skills/custom/phone-network-analysis/graph-operator/scripts/graph_operator_wrapper.py'),
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return str(candidates[0])


GRAPH_OPERATOR = _graph_operator_path()


def _run_json(cmd: List[str]) -> Dict[str, Any]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return {
            'ok': False,
            'error': 'subprocess_failed',
            'returncode': proc.returncode,
            'stdout': proc.stdout,
            'stderr': proc.stderr,
            'cmd': cmd,
        }
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {
            'ok': False,
            'error': 'json_decode_failed',
            'stdout': proc.stdout,
            'stderr': proc.stderr,
            'cmd': cmd,
        }


def _call_graph_operator(operator: str, extra_args: List[str]) -> Dict[str, Any]:
    cmd = [PYTHON_BIN, GRAPH_OPERATOR, '--operator', operator] + extra_args
    return _run_json(cmd)


def _preview(node_id: str, keep: int = 12) -> str:
    if not node_id:
        return ''
    if len(node_id) <= keep:
        return node_id
    return f'{node_id[:keep]}...'


def _normalize_edges(raw_edges: Any) -> List[Tuple[str, str]]:
    edges: List[Tuple[str, str]] = []
    if not isinstance(raw_edges, list):
        return edges
    for item in raw_edges:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            edges.append((str(item[0]), str(item[1])))
        elif isinstance(item, dict):
            src = item.get('source') or item.get('src') or item.get('from')
            dst = item.get('target') or item.get('dst') or item.get('to')
            if src is not None and dst is not None:
                edges.append((str(src), str(dst)))
    return edges


def _build_local_graph(nodes: List[str], edges: List[Tuple[str, str]], directed: bool) -> nx.Graph:
    g = nx.DiGraph() if directed else nx.Graph()
    g.add_nodes_from(nodes)
    g.add_edges_from(edges)
    return g


def _rank_top_neighbors(graph: nx.Graph, center: str, shared_phone_set: set, top_k: int) -> List[Dict[str, Any]]:
    if center not in graph:
        return []
    try:
        neighbors = list(graph.neighbors(center))
    except Exception:
        return []
    if not neighbors:
        return []

    if graph.number_of_nodes() > 1:
        bet = nx.betweenness_centrality(graph)
    else:
        bet = {}

    scored = []
    for node in neighbors:
        scored.append({
            'node': node,
            'node_preview': _preview(node),
            'local_degree': int(graph.degree(node)),
            'local_betweenness_centrality': round(float(bet.get(node, 0.0)), 6),
            'shared_device_related': node in shared_phone_set,
        })
    scored.sort(
        key=lambda x: (
            1 if x['shared_device_related'] else 0,
            x['local_degree'],
            x['local_betweenness_centrality'],
            x['node'],
        ),
        reverse=True,
    )
    for idx, item in enumerate(scored[:top_k], start=1):
        item['rank'] = idx
    return scored[:top_k]


def _rank_key_roles(graph: nx.Graph, center: str, shared_phone_set: set, top_k: int) -> Dict[str, Any]:
    if graph.number_of_nodes() == 0:
        return {
            'center_node': center,
            'center_preview': _preview(center),
            'local_hubs': [],
            'bridge_nodes': [],
            'shared_device_related_nodes': [],
        }

    deg_sorted = sorted(graph.degree(), key=lambda x: (x[1], x[0]), reverse=True)
    bet = nx.betweenness_centrality(graph) if graph.number_of_nodes() > 1 else {}
    bet_sorted = sorted(
        ((n, float(v)) for n, v in bet.items() if n != center),
        key=lambda x: (x[1], graph.degree(x[0]), x[0]),
        reverse=True,
    )

    local_hubs = [
        {
            'node': n,
            'node_preview': _preview(n),
            'local_degree': int(d),
            'role': 'local_hub' if n != center else 'center',
            'rank': idx,
        }
        for idx, (n, d) in enumerate([x for x in deg_sorted if x[0] != center][:top_k], start=1)
    ]

    bridge_nodes = [
        {
            'node': n,
            'node_preview': _preview(n),
            'local_betweenness_centrality': round(v, 6),
            'local_degree': int(graph.degree(n)),
            'role': 'bridge_node',
            'rank': idx,
        }
        for idx, (n, v) in enumerate(bet_sorted[:top_k], start=1)
    ]

    shared_nodes = [
        {
            'node': n,
            'node_preview': _preview(n),
            'local_degree': int(graph.degree(n)) if n in graph else 0,
            'role': 'shared_device_related',
            'rank': idx,
        }
        for idx, n in enumerate(sorted(shared_phone_set)[:top_k], start=1)
    ]

    return {
        'center_node': center,
        'center_preview': _preview(center),
        'local_hubs': local_hubs,
        'bridge_nodes': bridge_nodes,
        'shared_device_related_nodes': shared_nodes,
    }


def _rank_suspicious_nodes(graph: nx.Graph, center: str, shared_phone_set: set, top_k: int) -> List[Dict[str, Any]]:
    if graph.number_of_nodes() <= 1:
        return []
    bet = nx.betweenness_centrality(graph)
    candidates = []
    for node in graph.nodes():
        if node == center:
            continue
        roles = []
        if node in shared_phone_set:
            roles.append('shared_device_related')
        if graph.degree(node) >= 2:
            roles.append('multi_link_neighbor')
        if bet.get(node, 0.0) > 0:
            roles.append('bridge_like')
        if not roles:
            continue
        score = (5 if node in shared_phone_set else 0) + graph.degree(node) + float(bet.get(node, 0.0)) * 10
        candidates.append({
            'node': node,
            'node_preview': _preview(node),
            'local_degree': int(graph.degree(node)),
            'local_betweenness_centrality': round(float(bet.get(node, 0.0)), 6),
            'roles': roles,
            'score': round(score, 6),
        })
    candidates.sort(key=lambda x: (x['score'], x['local_degree'], x['node']), reverse=True)
    for idx, item in enumerate(candidates[:top_k], start=1):
        item['rank'] = idx
    return candidates[:top_k]


def _collect_shared_phone_set(shared_device_result: Dict[str, Any]) -> Tuple[set, List[Dict[str, Any]]]:
    shared_phone_set = set()
    shared_devices_preview = []
    result = shared_device_result.get('result', {}) if isinstance(shared_device_result, dict) else {}
    shared_devices = result.get('shared_devices', [])
    for item in shared_devices:
        if not isinstance(item, dict):
            continue
        device_id = str(item.get('device_id', ''))
        phones = [str(x) for x in item.get('shared_phones', []) if x]
        shared_phone_set.update(phones)
        shared_devices_preview.append({
            'device_id': device_id,
            'device_preview': _preview(device_id),
            'shared_phone_count': int(item.get('shared_phone_count', len(phones))),
            'shared_phones_preview': [_preview(x) for x in phones[:5]],
        })
    return shared_phone_set, shared_devices_preview[:5]


def _build_markdown(payload: Dict[str, Any]) -> str:
    result = payload['result']
    phone_id = result['phone_profile'].get('phone_id', payload['input_summary']['phone_id'])
    lines = []
    lines.append(f'# 子图抽取分析报告：{_preview(phone_id, 16)}')
    lines.append('')
    lines.append('## 1. 分析对象')
    lines.append(f'- 号码ID：`{phone_id}`')
    lines.append(f"- 子图跳数：{payload['input_summary']['hops']}")
    lines.append(f"- 最大节点数：{payload['input_summary']['max_nodes']}")
    lines.append('')
    lines.append('## 2. 节点画像摘要')
    profile = result['phone_profile']
    lines.append(f"- 是否命中画像：{profile.get('node_found', False)}")
    if profile.get('raw_node_attrs'):
        attrs = profile['raw_node_attrs']
        lines.append(f"- 省份：{attrs.get('province')}")
        lines.append(f"- 标签：{attrs.get('label')}")
        lines.append(f"- 子标签：{attrs.get('sub_label')}")
        lines.append(f"- 通话记录数：{profile.get('call_record_count')}")
        lines.append(f"- 对端数量：{profile.get('counterparty_count')}")
        lines.append(f"- 设备数量：{profile.get('device_count')}")
    lines.append('')
    lines.append('## 3. 局部子图摘要')
    sub = result['subgraph_analysis']
    lines.append(f"- 中心节点：`{sub.get('center_node')}`")
    lines.append(f"- 候选子图节点数（截断前）：{sub.get('candidate_num_nodes_before_truncation')}")
    lines.append(f"- 候选子图边数（截断前）：{sub.get('candidate_num_edges_before_truncation')}")
    lines.append(f"- 实际返回节点数：{sub.get('num_nodes')}")
    lines.append(f"- 实际返回边数：{sub.get('num_edges')}")
    lines.append(f"- 是否截断：{sub.get('truncated')}")
    lines.append('')
    lines.append('## 4. Top 邻居')
    top_neighbors = result.get('top_neighbors', [])
    if top_neighbors:
        for item in top_neighbors:
            lines.append(
                f"- {item['rank']}. `{item['node_preview']}` | 局部度={item['local_degree']} | betweenness={item['local_betweenness_centrality']} | 共享设备关联={item['shared_device_related']}"
            )
    else:
        lines.append('- 无可展示邻居。')
    lines.append('')
    lines.append('## 5. 关键角色')
    roles = result.get('key_roles', {})
    hubs = roles.get('local_hubs', [])
    bridges = roles.get('bridge_nodes', [])
    shared_nodes = roles.get('shared_device_related_nodes', [])
    lines.append('- 局部 Hub：')
    if hubs:
        for item in hubs:
            lines.append(f"  - `{item['node_preview']}` | 局部度={item['local_degree']}")
    else:
        lines.append('  - 无')
    lines.append('- 局部桥接点：')
    if bridges:
        for item in bridges:
            lines.append(f"  - `{item['node_preview']}` | betweenness={item['local_betweenness_centrality']} | 局部度={item['local_degree']}")
    else:
        lines.append('  - 无')
    lines.append('- 共享设备关联节点：')
    if shared_nodes:
        for item in shared_nodes:
            lines.append(f"  - `{item['node_preview']}` | 局部度={item['local_degree']}")
    else:
        lines.append('  - 无')
    lines.append('')
    lines.append('## 6. 可疑证据节点')
    suspicious = result.get('suspicious_evidence_nodes', [])
    if suspicious:
        for item in suspicious:
            lines.append(
                f"- {item['rank']}. `{item['node_preview']}` | 角色={','.join(item['roles'])} | 局部度={item['local_degree']} | betweenness={item['local_betweenness_centrality']} | score={item['score']}"
            )
    else:
        lines.append('- 当前局部子图中未识别到明显的高优先级可疑证据节点。')
    lines.append('')
    lines.append('## 7. 共享设备线索')
    shared_preview = result.get('shared_device_analysis', {}).get('shared_devices_preview', [])
    if shared_preview:
        for item in shared_preview:
            lines.append(
                f"- 设备 `{item['device_preview']}` | 关联号码数={item['shared_phone_count']} | 共享号码预览={item['shared_phones_preview']}"
            )
    else:
        lines.append('- 当前未发现可直接展开的共享设备线索。')
    lines.append('')
    lines.append('## 8. 结论摘要')
    lines.append(f"- {result.get('human_summary', '')}")
    lines.append('')
    lines.append('## 9. 下一步调查建议')
    for step in result.get('investigation_next_steps', []):
        lines.append(f'- {step}')
    lines.append('')
    lines.append('## 10. 推荐联动 Skill')
    for item in result.get('recommended_followups', []):
        lines.append(f"- `{item['skill']}`：{item['reason']}")
    lines.append('')
    return '\n'.join(lines).strip() + '\n'


def main() -> None:
    parser = argparse.ArgumentParser(description='YiGraph 风格的局部子图分析 skill')
    parser.add_argument('--phone-id', required=True)
    parser.add_argument('--hops', type=int, default=1, choices=[1, 2])
    parser.add_argument('--max-nodes', type=int, default=100)
    parser.add_argument('--top-k', type=int, default=10)
    parser.add_argument('--directed', action='store_true')
    parser.add_argument('--user-node-path', default=None)
    parser.add_argument('--call-graph-path', default=None)
    parser.add_argument('--device-graph-path', default=None)
    args = parser.parse_args()

    phone_id = args.phone_id.strip()
    user_node_path = _resolve_path(args.user_node_path, ['processed/unified/user_nodes.csv'])
    call_graph_path = _resolve_path(args.call_graph_path, ['processed/unified/call_edges.csv'])
    device_graph_path = _resolve_path(args.device_graph_path, ['processed/graph_views/unified/edges_phone_imei.parquet'])

    profile = _call_graph_operator(
        'query_phone_node',
        [
            '--graph-path', user_node_path,
            '--graph-format', 'csv',
            '--source-col', 'user_id',
            '--phone-id', phone_id,
            '--max-return', '10',
        ],
    )
    shared_device = _call_graph_operator(
        'query_shared_device',
        [
            '--graph-path', device_graph_path,
            '--graph-format', 'parquet',
            '--source-col', 'user_id',
            '--target-col', 'imei',
            '--phone-id', phone_id,
            '--max-return', '10',
        ],
    )
    subgraph = _call_graph_operator(
        'subgraph_extract',
        [
            '--graph-path', call_graph_path,
            '--graph-format', 'csv',
            '--source-col', 'src_user_id',
            '--target-col', 'dst_counterparty_id',
            '--center-node', phone_id,
            '--hops', str(args.hops),
            '--max-nodes', str(args.max_nodes),
        ] + (['--directed'] if args.directed else []),
    )
    expand = _call_graph_operator(
        'expand_neighbors',
        [
            '--graph-path', call_graph_path,
            '--graph-format', 'csv',
            '--source-col', 'src_user_id',
            '--target-col', 'dst_counterparty_id',
            '--node', phone_id,
            '--max-return', str(max(args.top_k, 20)),
        ] + (['--directed'] if args.directed else []),
    )

    if not subgraph.get('ok'):
        print(json.dumps({'ok': False, 'skill': 'subgraph-extraction-analysis', 'error': subgraph}, ensure_ascii=False, indent=2))
        return

    sub_result = subgraph.get('result', {})
    nodes = [str(x) for x in sub_result.get('nodes', []) if x]
    edges = _normalize_edges(sub_result.get('edges', []))
    local_graph = _build_local_graph(nodes, edges, args.directed)
    shared_phone_set, shared_devices_preview = _collect_shared_phone_set(shared_device)

    top_neighbors = _rank_top_neighbors(local_graph, phone_id, shared_phone_set, args.top_k)
    if not top_neighbors and expand.get('ok'):
        fallback_neighbors = [str(x) for x in expand.get('result', {}).get('neighbors', []) if x]
        top_neighbors = [
            {
                'rank': idx,
                'node': node,
                'node_preview': _preview(node),
                'local_degree': 0,
                'local_betweenness_centrality': 0.0,
                'shared_device_related': node in shared_phone_set,
            }
            for idx, node in enumerate(fallback_neighbors[:args.top_k], start=1)
        ]

    key_roles = _rank_key_roles(local_graph, phone_id, shared_phone_set, args.top_k)
    suspicious = _rank_suspicious_nodes(local_graph, phone_id, shared_phone_set, args.top_k)

    candidate_nodes = int(sub_result.get('candidate_num_nodes_before_truncation', sub_result.get('num_nodes', len(nodes))))
    candidate_edges = int(sub_result.get('candidate_num_edges_before_truncation', sub_result.get('num_edges', len(edges))))
    actual_nodes = int(sub_result.get('num_nodes', len(nodes)))
    actual_edges = int(sub_result.get('num_edges', len(edges)))
    truncated = bool(sub_result.get('truncated', False))

    human_summary_parts = [
        f'围绕该号码抽取了 {args.hops} 跳局部关系圈。',
        f'候选子图共有 {candidate_nodes} 个节点、{candidate_edges} 条边。',
        f'实际返回 {actual_nodes} 个节点、{actual_edges} 条边。',
    ]
    if shared_device.get('ok'):
        shared_count = int(shared_device.get('result', {}).get('shared_device_count', 0))
        if shared_count > 0:
            human_summary_parts.append(f'该号码还关联 {shared_count} 个共享设备线索，可进一步排查共用设备号码。')
    if truncated:
        human_summary_parts.append('由于节点上限约束，子图结果已发生截断。')
    human_summary = ' '.join(human_summary_parts)

    recommended_followups = [
        {
            'skill': 'association-path-analysis',
            'reason': '若想继续分析中心号码与某个可疑节点之间如何连起来，可切换到路径型联合分析。',
        },
        {
            'skill': 'overlap-analysis',
            'reason': '若想判断中心号码与某个邻居是否处于同一联系圈，可切换到重叠分析。',
        },
        {
            'skill': 'graph-operator',
            'reason': '若想继续钻取单个节点画像、共享设备或更大范围子图，可回到底层算子继续分析。',
        },
    ]

    investigation_next_steps = [
        '先核查 Top 邻居与局部桥接点，判断其中是否存在明显的中介号码或高频公共对端。',
        '若发现共享设备关联节点，优先排查这些共用设备号码的画像与历史关系。',
        '如需继续深挖，可围绕 Top 可疑证据节点再次抽取 1-2 跳子图。',
    ]
    if suspicious:
        investigation_next_steps.insert(0, f"优先核查排名第 1 的可疑证据节点 `{suspicious[0]['node_preview']}`。")

    result = {
        'phone_profile': profile.get('result', {'phone_id': phone_id, 'node_found': False}),
        'subgraph_analysis': {
            'center_node': phone_id,
            'hops': args.hops,
            'candidate_num_nodes_before_truncation': candidate_nodes,
            'candidate_num_edges_before_truncation': candidate_edges,
            'num_nodes': actual_nodes,
            'num_edges': actual_edges,
            'nodes_preview': [_preview(x) for x in nodes[:20]],
            'edges_preview': [[_preview(u), _preview(v)] for u, v in edges[:20]],
            'truncated': truncated,
        },
        'top_neighbors': top_neighbors,
        'key_roles': key_roles,
        'suspicious_evidence_nodes': suspicious,
        'shared_device_analysis': {
            'device_count': int(shared_device.get('result', {}).get('device_count', 0)) if shared_device.get('ok') else 0,
            'shared_device_count': int(shared_device.get('result', {}).get('shared_device_count', 0)) if shared_device.get('ok') else 0,
            'shared_devices_preview': shared_devices_preview,
        },
        'human_summary': human_summary,
        'investigation_next_steps': investigation_next_steps,
        'recommended_followups': recommended_followups,
    }

    payload = {
        'ok': True,
        'skill': 'subgraph-extraction-analysis',
        'query_type': 'subgraph',
        'input_summary': {
            'phone_id': phone_id,
            'hops': args.hops,
            'max_nodes': args.max_nodes,
            'top_k': args.top_k,
            'directed': args.directed,
            'user_node_path': user_node_path,
            'call_graph_path': call_graph_path,
            'device_graph_path': device_graph_path,
        },
        'result': result,
        'notes': [
            '本技能底层复用了 graph-operator 的 query_phone_node、query_shared_device、subgraph_extract、expand_neighbors。',
            '局部 Hub 和桥接点基于返回的局部子图做二次排序，更接近 YiGraph 的局部子图分析风格。',
        ],
        'yigraph_meta': {
            'recommended_query_type': 'subgraph',
            'related_query_types': ['subgraph', 'neighbor_query', 'node_lookup'],
            'explanation': 'subgraph-extraction-analysis 主要对应 YiGraph 的 subgraph / neighbor_query 分析风格。',
        },
    }

    report_name = f"subgraph_extraction_report_{phone_id[:8]}_h{args.hops}.md"
    report_path = OUTPUT_DIR / report_name
    report_text = _build_markdown(payload)
    report_path.write_text(report_text, encoding='utf-8')
    payload['report_path'] = str(report_path)
    payload['artifacts'] = [
        {
            'type': 'markdown',
            'title': f'子图抽取分析报告_{phone_id[:8]}_h{args.hops}',
            'path': str(report_path),
        }
    ]

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
