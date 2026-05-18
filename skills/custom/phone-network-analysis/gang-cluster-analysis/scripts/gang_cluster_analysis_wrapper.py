#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Optional

import duckdb
import pandas as pd

try:
    import networkx as nx  # optional, used for better bridge ranking
except Exception:
    nx = None

SCRIPT_VERSION = "gang-cluster-analysis-final-v4"

TIME_CANDIDATES = [
    "call_time",
    "event_time",
    "start_time",
    "timestamp",
    "ts",
    "datetime",
    "call_start_time",
    "start_ts",
    "create_time",
]


def json_default(obj: Any) -> Any:
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def preview(value: Any, keep: int = 12) -> str:
    text = str(value)
    return text if len(text) <= keep else text[:keep] + "..."


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def safe_int(value: Any) -> int:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def safe_float(value: Any) -> float:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def read_csv_columns(path: Path) -> list[str]:
    return list(pd.read_csv(path, nrows=0).columns)


def detect_time_column(columns: list[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for candidate in TIME_CANDIDATES:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def resolve_dataset_root(explicit_root: Optional[str], dataset: str = "unified") -> Path:
    candidates: list[Path] = []
    if explicit_root:
        candidates.append(Path(explicit_root).expanduser())
    env_root = os.getenv("PHONE_NETWORK_DATASETS_ROOT")
    if env_root:
        candidates.append(Path(env_root).expanduser())
    candidates.extend(
        [
            Path("/mnt/datasets/phone-network"),
            Path("/workspace/imiss-deer-flow-main/datasets/phone-network"),
            Path.home() / "imiss-deer-flow-main/datasets/phone-network",
        ]
    )
    for root in candidates:
        if (root / "processed" / dataset / "call_edges.csv").exists() and (
            root / "processed" / dataset / "user_nodes.csv"
        ).exists() and (
            root / "processed" / "graph_views" / dataset / "edges_phone_imei.parquet"
        ).exists():
            return root
    raise FileNotFoundError("Could not resolve phone-network dataset root. Checked: " + "; ".join(map(str, candidates)))


def resolve_output_root(explicit_output_root: Optional[str]) -> Path:
    if explicit_output_root:
        return Path(explicit_output_root).expanduser()
    env_output = os.getenv("PHONE_NETWORK_OUTPUT_ROOT")
    if env_output:
        return Path(env_output).expanduser()
    for candidate in [
        Path("/mnt/user-data/outputs"),
        Path("/workspace/imiss-deer-flow-main/outputs/phone_network_graph"),
        Path.home() / "imiss-deer-flow-main/outputs/phone_network_graph",
    ]:
        if candidate.exists():
            return candidate
    return Path("/mnt/user-data/outputs")


def values_sql(column_name: str, values: Iterable[str]) -> str:
    escaped = [str(v).replace("'", "''") for v in values]
    if not escaped:
        return f"SELECT NULL::{column_name} WHERE FALSE"
    return "SELECT * FROM (VALUES " + ", ".join([f"('{v}')" for v in escaped]) + f") AS t({column_name})"


def resolve_phone_id_file(phone_id_file: str) -> Path:
    raw = Path(phone_id_file).expanduser()
    script_dir = Path(__file__).resolve().parent
    candidates = [
        raw,
        Path.cwd() / raw,
        script_dir / raw.name,
        Path("/mnt/skills/custom/phone-network-analysis/gang-cluster-analysis/scripts") / raw.name,
        Path("/workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/gang-cluster-analysis/scripts") / raw.name,
        Path.home() / "imiss-deer-flow-main/skills/custom/phone-network-analysis/gang-cluster-analysis/scripts" / raw.name,
    ]
    checked = []
    for c in candidates:
        try:
            cc = c.resolve(strict=False)
        except Exception:
            cc = c
        checked.append(str(cc))
        if c.exists() and c.is_file():
            return c
    raise FileNotFoundError(f"phone-id file not found: {phone_id_file}. Checked: " + "; ".join(checked))


def parse_phone_ids(args: argparse.Namespace) -> list[str]:
    ids: list[str] = []
    if args.phone_ids:
        ids.extend([x.strip() for x in args.phone_ids.split(",") if x.strip()])
    if args.phone_id_file:
        resolved_file = resolve_phone_id_file(args.phone_id_file)
        ids.extend([line.strip() for line in resolved_file.read_text(encoding="utf-8").splitlines() if line.strip()])
    if args.input_csv:
        df = pd.read_csv(Path(args.input_csv).expanduser())
        if args.phone_id_column not in df.columns:
            raise ValueError(f"phone-id column '{args.phone_id_column}' not found in input csv")
        ids.extend([str(x).strip() for x in df[args.phone_id_column].dropna().tolist() if str(x).strip()])

    deduped: list[str] = []
    seen: set[str] = set()
    for pid in ids:
        if pid not in seen:
            seen.add(pid)
            deduped.append(pid)
    if not deduped:
        raise ValueError("No phone ids provided. Use --phone-ids, --phone-id-file, or --input-csv.")
    return deduped
def union_find_components(nodes: list[str], edges: list[tuple[str, str]]) -> list[list[str]]:
    parent = {n: n for n in nodes}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for a, b in edges:
        if a in parent and b in parent:
            union(a, b)

    groups: dict[str, list[str]] = defaultdict(list)
    for n in nodes:
        groups[find(n)].append(n)
    return sorted(groups.values(), key=lambda x: (-len(x), x))


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gang / cluster analysis for phone-network data")
    parser.add_argument("--phone-ids", type=str, default=None, help="Comma-separated seed phone ids")
    parser.add_argument("--phone-id-file", type=str, default=None, help="Text file with one phone id per line")
    parser.add_argument("--input-csv", type=str, default=None, help="CSV containing seed phone ids")
    parser.add_argument("--phone-id-column", type=str, default="phone_id")
    parser.add_argument("--group-name", type=str, default="gang_cluster")
    parser.add_argument("--dataset-root", type=str, default=None)
    parser.add_argument("--dataset", type=str, default="unified")
    parser.add_argument("--artifact-mode", choices=["full", "essential", "markdown_only"], default="full")
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--candidate-scope", type=str, default="mixed", choices=["input_only", "shared_device", "common_counterparty", "mixed"])
    parser.add_argument("--max-expand-nodes", type=int, default=120)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--min-shared-device-count", type=int, default=1)
    parser.add_argument("--min-common-counterparty-count", type=int, default=2)
    parser.add_argument("--min-neighbor-overlap", type=float, default=0.03)
    parser.add_argument("--min-edge-score", type=float, default=8.0)
    parser.add_argument("--focus-min-cluster-size", type=int, default=3)
    parser.add_argument("--night-start-hour", type=int, default=22)
    parser.add_argument("--night-end-hour", type=int, default=6)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    seed_ids = parse_phone_ids(args)
    dataset_root = resolve_dataset_root(args.dataset_root, args.dataset)
    output_root = resolve_output_root(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    user_nodes_path = dataset_root / "processed" / args.dataset / "user_nodes.csv"
    call_edges_path = dataset_root / "processed" / args.dataset / "call_edges.csv"
    device_edges_path = dataset_root / "processed" / "graph_views" / args.dataset / "edges_phone_imei.parquet"

    time_col = detect_time_column(read_csv_columns(call_edges_path))

    conn = duckdb.connect(database=":memory:")
    user_nodes_sql = str(user_nodes_path).replace("'", "''")
    call_edges_sql = str(call_edges_path).replace("'", "''")
    device_edges_sql = str(device_edges_path).replace("'", "''")
    seed_ids_sql = values_sql("phone_id", seed_ids)

    profiles = conn.execute(
        f"""
        WITH seed_ids AS ({seed_ids_sql})
        SELECT s.phone_id,
               u.province,
               u.dataset_name,
               u.label,
               u.sub_label,
               u.source_table
        FROM seed_ids s
        LEFT JOIN read_csv_auto('{user_nodes_sql}') u
        ON s.phone_id = u.user_id
        """
    ).df()

    seed_device_candidates = conn.execute(
        f"""
        WITH seed_ids AS ({seed_ids_sql}),
        seed_devices AS (
            SELECT DISTINCT imei
            FROM read_parquet('{device_edges_sql}')
            WHERE user_id IN (SELECT phone_id FROM seed_ids)
        )
        SELECT e.user_id AS phone_id,
               COUNT(DISTINCT e.imei) AS shared_device_count_with_seed,
               COUNT(*) AS raw_device_hits
        FROM read_parquet('{device_edges_sql}') e
        WHERE e.imei IN (SELECT imei FROM seed_devices)
          AND e.user_id NOT IN (SELECT phone_id FROM seed_ids)
        GROUP BY 1
        ORDER BY shared_device_count_with_seed DESC, raw_device_hits DESC, phone_id
        LIMIT {max(args.max_expand_nodes, 1)}
        """
    ).df()

    seed_neighbor_candidates = conn.execute(
        f"""
        WITH seed_ids AS ({seed_ids_sql}),
        seed_counterparties AS (
            SELECT DISTINCT dst_counterparty_id AS cp
            FROM read_csv_auto('{call_edges_sql}')
            WHERE src_user_id IN (SELECT phone_id FROM seed_ids)
        )
        SELECT c.src_user_id AS phone_id,
               COUNT(DISTINCT c.dst_counterparty_id) AS common_counterparty_count_with_seed,
               COUNT(*) AS raw_call_hits
        FROM read_csv_auto('{call_edges_sql}') c
        WHERE c.dst_counterparty_id IN (SELECT cp FROM seed_counterparties)
          AND c.src_user_id NOT IN (SELECT phone_id FROM seed_ids)
        GROUP BY 1
        ORDER BY common_counterparty_count_with_seed DESC, raw_call_hits DESC, phone_id
        LIMIT {max(args.max_expand_nodes, 1)}
        """
    ).df()

    candidate_ids: list[str] = list(seed_ids)
    candidate_reason: dict[str, list[str]] = {pid: ["seed_input"] for pid in seed_ids}

    def add_candidates(df: pd.DataFrame, pid_col: str, reason_col: str, label: str) -> None:
        for _, row in df.iterrows():
            pid = str(row[pid_col])
            if label == "shared_device" and safe_int(row[reason_col]) < args.min_shared_device_count:
                continue
            if label == "common_counterparty" and safe_int(row[reason_col]) < args.min_common_counterparty_count:
                continue
            if pid not in candidate_reason:
                candidate_ids.append(pid)
                candidate_reason[pid] = []
            candidate_reason[pid].append(f"{label}:{safe_int(row[reason_col])}")

    if args.candidate_scope in {"shared_device", "mixed"}:
        add_candidates(seed_device_candidates, "phone_id", "shared_device_count_with_seed", "shared_device")
    if args.candidate_scope in {"common_counterparty", "mixed"}:
        add_candidates(seed_neighbor_candidates, "phone_id", "common_counterparty_count_with_seed", "common_counterparty")

    if args.candidate_scope == "input_only":
        candidate_ids = list(seed_ids)

    # trim expanded candidates while keeping all seeds
    seed_set = set(seed_ids)
    if len(candidate_ids) > args.max_expand_nodes:
        non_seed_scored = []
        device_map = {str(r["phone_id"]): safe_int(r["shared_device_count_with_seed"]) for _, r in seed_device_candidates.iterrows()}
        cp_map = {str(r["phone_id"]): safe_int(r["common_counterparty_count_with_seed"]) for _, r in seed_neighbor_candidates.iterrows()}
        for pid in candidate_ids:
            if pid in seed_set:
                continue
            score = 10 * device_map.get(pid, 0) + 3 * cp_map.get(pid, 0)
            non_seed_scored.append((score, pid))
        non_seed_scored.sort(key=lambda x: (-x[0], x[1]))
        keep_non_seed = [pid for _, pid in non_seed_scored[: max(args.max_expand_nodes - len(seed_ids), 0)]]
        candidate_ids = list(seed_ids) + keep_non_seed

    candidate_ids = list(dict.fromkeys(candidate_ids))
    candidate_ids_sql = values_sql("phone_id", candidate_ids)

    candidate_profiles = conn.execute(
        f"""
        WITH candidate_ids AS ({candidate_ids_sql})
        SELECT c.phone_id,
               u.province,
               u.dataset_name,
               u.label,
               u.sub_label,
               u.source_table
        FROM candidate_ids c
        LEFT JOIN read_csv_auto('{user_nodes_sql}') u
        ON c.phone_id = u.user_id
        """
    ).df()

    call_metrics = conn.execute(
        f"""
        WITH candidate_ids AS ({candidate_ids_sql})
        SELECT src_user_id AS phone_id,
               COUNT(*) AS call_record_count,
               COUNT(DISTINCT dst_counterparty_id) AS counterparties
        FROM read_csv_auto('{call_edges_sql}')
        WHERE src_user_id IN (SELECT phone_id FROM candidate_ids)
        GROUP BY 1
        """
    ).df()

    if time_col:
        night_expr = (
            f"CASE WHEN EXTRACT('hour' FROM TRY_CAST({time_col} AS TIMESTAMP)) >= {args.night_start_hour} "
            f"OR EXTRACT('hour' FROM TRY_CAST({time_col} AS TIMESTAMP)) < {args.night_end_hour} THEN 1 ELSE 0 END"
        )
        night_metrics = conn.execute(
            f"""
            WITH candidate_ids AS ({candidate_ids_sql})
            SELECT src_user_id AS phone_id,
                   SUM({night_expr}) AS night_call_count,
                   COUNT(*) AS total_timed_calls
            FROM read_csv_auto('{call_edges_sql}')
            WHERE src_user_id IN (SELECT phone_id FROM candidate_ids)
              AND TRY_CAST({time_col} AS TIMESTAMP) IS NOT NULL
            GROUP BY 1
            """
        ).df()
    else:
        night_metrics = pd.DataFrame(columns=["phone_id", "night_call_count", "total_timed_calls"])

    device_metrics = conn.execute(
        f"""
        WITH candidate_ids AS ({candidate_ids_sql}),
        device_usage AS (
            SELECT imei, COUNT(DISTINCT user_id) AS device_phone_count
            FROM read_parquet('{device_edges_sql}')
            GROUP BY 1
        )
        SELECT e.user_id AS phone_id,
               COUNT(DISTINCT e.imei) AS device_count,
               COUNT(DISTINCT CASE WHEN du.device_phone_count >= 2 THEN e.imei END) AS shared_device_count,
               SUM(CASE WHEN du.device_phone_count >= 2 THEN du.device_phone_count - 1 ELSE 0 END) AS shared_peer_total
        FROM read_parquet('{device_edges_sql}') e
        JOIN device_usage du ON e.imei = du.imei
        WHERE e.user_id IN (SELECT phone_id FROM candidate_ids)
        GROUP BY 1
        """
    ).df()

    member_df = pd.DataFrame({"phone_id": candidate_ids}).merge(candidate_profiles, on="phone_id", how="left")
    for extra in [call_metrics, night_metrics, device_metrics]:
        member_df = member_df.merge(extra, on="phone_id", how="left")
    for col in ["call_record_count", "counterparties", "night_call_count", "total_timed_calls", "device_count", "shared_device_count", "shared_peer_total"]:
        if col not in member_df.columns:
            member_df[col] = 0
        member_df[col] = member_df[col].fillna(0)
    member_df["night_call_ratio"] = member_df.apply(
        lambda r: safe_float(r["night_call_count"]) / safe_float(r["total_timed_calls"]) if safe_float(r["total_timed_calls"]) > 0 else 0.0,
        axis=1,
    )
    member_df["is_seed"] = member_df["phone_id"].isin(seed_set)
    member_df["candidate_reason"] = member_df["phone_id"].map(lambda x: ", ".join(candidate_reason.get(str(x), [])))

    # pairwise evidence among candidates
    pair_df = conn.execute(
        f"""
        WITH candidate_ids AS ({candidate_ids_sql}),
        call_neighbors AS (
            SELECT src_user_id AS phone_id, dst_counterparty_id AS counterparty
            FROM read_csv_auto('{call_edges_sql}')
            WHERE src_user_id IN (SELECT phone_id FROM candidate_ids)
        ),
        cp_pairs AS (
            SELECT a.phone_id AS phone_a,
                   b.phone_id AS phone_b,
                   COUNT(DISTINCT a.counterparty) AS common_counterparty_count
            FROM call_neighbors a
            JOIN call_neighbors b
              ON a.counterparty = b.counterparty
             AND a.phone_id < b.phone_id
            GROUP BY 1,2
        ),
        direct_pairs AS (
            SELECT LEAST(src_user_id, dst_counterparty_id) AS phone_a,
                   GREATEST(src_user_id, dst_counterparty_id) AS phone_b,
                   COUNT(*) AS internal_call_records
            FROM read_csv_auto('{call_edges_sql}')
            WHERE src_user_id IN (SELECT phone_id FROM candidate_ids)
              AND dst_counterparty_id IN (SELECT phone_id FROM candidate_ids)
            GROUP BY 1,2
        ),
        device_pairs AS (
            SELECT LEAST(a.user_id, b.user_id) AS phone_a,
                   GREATEST(a.user_id, b.user_id) AS phone_b,
                   COUNT(DISTINCT a.imei) AS shared_device_count
            FROM read_parquet('{device_edges_sql}') a
            JOIN read_parquet('{device_edges_sql}') b
              ON a.imei = b.imei
             AND a.user_id < b.user_id
            WHERE a.user_id IN (SELECT phone_id FROM candidate_ids)
              AND b.user_id IN (SELECT phone_id FROM candidate_ids)
            GROUP BY 1,2
        )
        SELECT COALESCE(cp.phone_a, dp.phone_a, sp.phone_a) AS phone_a,
               COALESCE(cp.phone_b, dp.phone_b, sp.phone_b) AS phone_b,
               COALESCE(cp.common_counterparty_count, 0) AS common_counterparty_count,
               COALESCE(dp.internal_call_records, 0) AS internal_call_records,
               COALESCE(sp.shared_device_count, 0) AS shared_device_count
        FROM cp_pairs cp
        FULL OUTER JOIN direct_pairs dp
          ON cp.phone_a = dp.phone_a AND cp.phone_b = dp.phone_b
        FULL OUTER JOIN device_pairs sp
          ON COALESCE(cp.phone_a, dp.phone_a) = sp.phone_a
         AND COALESCE(cp.phone_b, dp.phone_b) = sp.phone_b
        """
    ).df()

    member_stats = member_df.set_index("phone_id")[["counterparties", "call_record_count", "shared_device_count", "night_call_ratio"]].to_dict("index")
    pair_rows: list[dict[str, Any]] = []
    for _, row in pair_df.iterrows():
        a = str(row["phone_a"])
        b = str(row["phone_b"])
        cp_count = safe_int(row["common_counterparty_count"])
        shared_device_count = safe_int(row["shared_device_count"])
        internal_calls = safe_int(row["internal_call_records"])
        a_neighbors = safe_int(member_stats.get(a, {}).get("counterparties", 0))
        b_neighbors = safe_int(member_stats.get(b, {}).get("counterparties", 0))
        denom = max(a_neighbors + b_neighbors - cp_count, 1)
        overlap_ratio = cp_count / denom
        relation_score = shared_device_count * 8 + cp_count * 3 + internal_calls * 4 + overlap_ratio * 100
        relation_types = []
        if shared_device_count > 0:
            relation_types.append("shared_device")
        if cp_count > 0:
            relation_types.append("common_counterparty")
        if internal_calls > 0:
            relation_types.append("internal_call")
        if relation_score >= args.min_edge_score and (
            shared_device_count >= args.min_shared_device_count
            or cp_count >= args.min_common_counterparty_count
            or overlap_ratio >= args.min_neighbor_overlap
            or internal_calls > 0
        ):
            pair_rows.append(
                {
                    "phone_a": a,
                    "phone_b": b,
                    "relation_types": ", ".join(relation_types),
                    "shared_device_count": shared_device_count,
                    "common_counterparty_count": cp_count,
                    "internal_call_records": internal_calls,
                    "neighbor_overlap_ratio": round(overlap_ratio, 4),
                    "relation_score": round(relation_score, 2),
                }
            )
    pair_evidence_df = pd.DataFrame(pair_rows).sort_values(
        ["relation_score", "shared_device_count", "common_counterparty_count", "internal_call_records"],
        ascending=[False, False, False, False],
    ) if pair_rows else pd.DataFrame(columns=[
        "phone_a", "phone_b", "relation_types", "shared_device_count", "common_counterparty_count", "internal_call_records", "neighbor_overlap_ratio", "relation_score"
    ])

    graph_edges = [(str(r["phone_a"]), str(r["phone_b"])) for _, r in pair_evidence_df.iterrows()]
    components = union_find_components(candidate_ids, graph_edges)
    clusters: list[dict[str, Any]] = []
    component_map: dict[str, int] = {}
    for idx, members_in_cluster in enumerate(components, start=1):
        cluster_pairs = pair_evidence_df[
            pair_evidence_df["phone_a"].isin(members_in_cluster) & pair_evidence_df["phone_b"].isin(members_in_cluster)
        ] if not pair_evidence_df.empty else pd.DataFrame(columns=pair_evidence_df.columns)
        cluster_seed_count = sum(1 for x in members_in_cluster if x in seed_set)
        cluster_devices = conn.execute(
            f"""
            WITH cluster_ids AS ({values_sql('phone_id', members_in_cluster)}),
            device_usage AS (
                SELECT e.imei, COUNT(DISTINCT e.user_id) AS total_phone_count
                FROM read_parquet('{device_edges_sql}') e
                GROUP BY 1
            )
            SELECT e.imei,
                   COUNT(DISTINCT e.user_id) AS cluster_member_count,
                   MAX(du.total_phone_count) AS total_phone_count
            FROM read_parquet('{device_edges_sql}') e
            JOIN device_usage du ON e.imei = du.imei
            WHERE e.user_id IN (SELECT phone_id FROM cluster_ids)
            GROUP BY 1
            HAVING COUNT(DISTINCT e.user_id) >= 2
            ORDER BY cluster_member_count DESC, total_phone_count DESC, imei
            LIMIT 10
            """
        ).df()
        cluster_counterparts = conn.execute(
            f"""
            WITH cluster_ids AS ({values_sql('phone_id', members_in_cluster)})
            SELECT dst_counterparty_id AS counterparty_id,
                   COUNT(DISTINCT src_user_id) AS cluster_member_count,
                   COUNT(*) AS total_calls
            FROM read_csv_auto('{call_edges_sql}')
            WHERE src_user_id IN (SELECT phone_id FROM cluster_ids)
            GROUP BY 1
            HAVING COUNT(DISTINCT src_user_id) >= 2
            ORDER BY cluster_member_count DESC, total_calls DESC, counterparty_id
            LIMIT 10
            """
        ).df()
        clusters.append(
            {
                "cluster_id": f"cluster_{idx}",
                "size": len(members_in_cluster),
                "seed_member_count": cluster_seed_count,
                "members": members_in_cluster,
                "edge_count": len(cluster_pairs),
                "density": round((2 * len(cluster_pairs)) / max(len(members_in_cluster) * (len(members_in_cluster) - 1), 1), 4),
                "top_relation_score": round(float(cluster_pairs["relation_score"].max()), 2) if not cluster_pairs.empty else 0.0,
                "top_devices_preview": [preview(x) for x in cluster_devices["imei"].tolist()[:5]] if not cluster_devices.empty else [],
                "top_counterparts_preview": [preview(x) for x in cluster_counterparts["counterparty_id"].tolist()[:5]] if not cluster_counterparts.empty else [],
                "focus_reason": "包含较多输入号码且内部共享设备/共同对端关系较密",
            }
        )
        for member in members_in_cluster:
            component_map[member] = idx

    focus_cluster = None
    for cluster in clusters:
        if cluster["size"] >= args.focus_min_cluster_size and cluster["seed_member_count"] >= 2:
            focus_cluster = cluster
            break
    if focus_cluster is None and clusters:
        focus_cluster = clusters[0]

    focus_members = focus_cluster["members"] if focus_cluster else candidate_ids
    focus_member_df = member_df[member_df["phone_id"].isin(focus_members)].copy()
    focus_pairs_df = pair_evidence_df[
        pair_evidence_df["phone_a"].isin(focus_members) & pair_evidence_df["phone_b"].isin(focus_members)
    ].copy() if not pair_evidence_df.empty else pd.DataFrame(columns=pair_evidence_df.columns)

    # graph role analysis
    weighted_degree = Counter()
    bridge_rank_rows: list[dict[str, Any]] = []
    if not focus_pairs_df.empty:
        for _, row in focus_pairs_df.iterrows():
            a = str(row["phone_a"])
            b = str(row["phone_b"])
            score = safe_float(row["relation_score"])
            weighted_degree[a] += score
            weighted_degree[b] += score
        if nx is not None:
            g = nx.Graph()
            for member in focus_members:
                g.add_node(member)
            for _, row in focus_pairs_df.iterrows():
                g.add_edge(str(row["phone_a"]), str(row["phone_b"]), weight=max(safe_float(row["relation_score"]), 0.1))
            try:
                betweenness = nx.betweenness_centrality(g, normalized=True, weight=None)
            except Exception:
                betweenness = {n: 0.0 for n in g.nodes()}
            for member in focus_members:
                bridge_rank_rows.append(
                    {
                        "phone_id": member,
                        "node_preview": preview(member),
                        "weighted_degree": round(weighted_degree.get(member, 0.0), 2),
                        "betweenness": round(float(betweenness.get(member, 0.0)), 4),
                        "is_seed": member in seed_set,
                    }
                )
        else:
            for member in focus_members:
                bridge_rank_rows.append(
                    {
                        "phone_id": member,
                        "node_preview": preview(member),
                        "weighted_degree": round(weighted_degree.get(member, 0.0), 2),
                        "betweenness": 0.0,
                        "is_seed": member in seed_set,
                    }
                )
    bridge_rank_df = pd.DataFrame(bridge_rank_rows)
    if bridge_rank_df.empty:
        bridge_rank_df = pd.DataFrame(columns=["phone_id", "node_preview", "weighted_degree", "betweenness", "is_seed"])

    focus_member_df["weighted_degree"] = focus_member_df["phone_id"].map(lambda x: round(weighted_degree.get(str(x), 0.0), 2))
    focus_member_df["signal_score"] = focus_member_df.apply(
        lambda r: round(
            safe_float(r["weighted_degree"]) * 0.45
            + safe_float(r["shared_device_count"]) * 8
            + safe_float(r["counterparties"]) * 0.08
            + safe_float(r["call_record_count"]) * 0.03
            + safe_float(r["night_call_ratio"]) * 20,
            2,
        ),
        axis=1,
    )
    focus_member_df["cluster_id"] = focus_member_df["phone_id"].map(lambda x: f"cluster_{component_map.get(str(x), 0)}")
    focus_member_df = focus_member_df.sort_values(["signal_score", "weighted_degree", "shared_device_count", "counterparties"], ascending=[False, False, False, False])

    cluster_devices_df = conn.execute(
        f"""
        WITH cluster_ids AS ({values_sql('phone_id', focus_members)}),
        device_usage AS (
            SELECT e.imei,
                   COUNT(DISTINCT e.user_id) AS total_phone_count,
                   SUM(CASE WHEN COALESCE(u.label, 0) = 1 THEN 1 ELSE 0 END) AS risk_phone_count,
                   COUNT(DISTINCT COALESCE(u.province, 'unknown')) AS province_count
            FROM read_parquet('{device_edges_sql}') e
            LEFT JOIN read_csv_auto('{user_nodes_sql}') u ON e.user_id = u.user_id
            GROUP BY 1
        )
        SELECT e.imei AS device_id,
               COUNT(DISTINCT e.user_id) AS cluster_member_count,
               MAX(du.total_phone_count) AS total_phone_count,
               MAX(du.risk_phone_count) AS risk_phone_count,
               MAX(du.province_count) AS province_count,
               STRING_AGG(SUBSTRING(e.user_id, 1, 12) || '...', ', ') AS member_preview
        FROM read_parquet('{device_edges_sql}') e
        JOIN device_usage du ON e.imei = du.imei
        WHERE e.user_id IN (SELECT phone_id FROM cluster_ids)
        GROUP BY 1
        HAVING COUNT(DISTINCT e.user_id) >= 2
        ORDER BY cluster_member_count DESC, total_phone_count DESC, device_id
        LIMIT {max(args.top_k, 1)}
        """
    ).df()

    cluster_counterparts_df = conn.execute(
        f"""
        WITH cluster_ids AS ({values_sql('phone_id', focus_members)}),
        cp_calls AS (
            SELECT src_user_id, dst_counterparty_id
            FROM read_csv_auto('{call_edges_sql}')
            WHERE src_user_id IN (SELECT phone_id FROM cluster_ids)
        ),
        cp_counts AS (
            SELECT dst_counterparty_id AS counterparty_id,
                   COUNT(DISTINCT src_user_id) AS cluster_member_count,
                   COUNT(*) AS total_calls
            FROM cp_calls
            GROUP BY 1
            HAVING COUNT(DISTINCT src_user_id) >= 2
        ),
        cp_members AS (
            SELECT dst_counterparty_id AS counterparty_id,
                   STRING_AGG(phone_preview, ', ') AS member_preview
            FROM (
                SELECT DISTINCT dst_counterparty_id, SUBSTRING(src_user_id, 1, 12) || '...' AS phone_preview
                FROM cp_calls
            ) t
            GROUP BY 1
        )
        SELECT c.counterparty_id,
               c.cluster_member_count,
               c.total_calls,
               m.member_preview
        FROM cp_counts c
        LEFT JOIN cp_members m ON c.counterparty_id = m.counterparty_id
        ORDER BY c.cluster_member_count DESC, c.total_calls DESC, c.counterparty_id
        LIMIT {max(args.top_k, 1)}
        """
    ).df()

    top_pairs_df = focus_pairs_df.head(max(args.top_k, 1)).copy()
    top_core_df = focus_member_df.head(max(args.top_k, 1)).copy()
    top_bridge_df = bridge_rank_df.sort_values(["betweenness", "weighted_degree"], ascending=[False, False]).head(max(args.top_k, 1)).copy()
    bridge_signal_available = (not top_bridge_df.empty) and (safe_float(top_bridge_df["betweenness"].max()) > 0)

    shared_device_cluster = not cluster_devices_df.empty and safe_int(cluster_devices_df["cluster_member_count"].max()) >= 3
    common_counterparty_cluster = not cluster_counterparts_df.empty and safe_int(cluster_counterparts_df["cluster_member_count"].max()) >= 3
    dense_pair_cluster = not focus_pairs_df.empty and safe_float(focus_pairs_df["relation_score"].mean()) >= 15
    if shared_device_cluster and common_counterparty_cluster:
        cluster_shape = "共享设备 + 共同对端混合团伙簇"
    elif shared_device_cluster:
        cluster_shape = "共享设备驱动团伙簇"
    elif common_counterparty_cluster:
        cluster_shape = "共同对端耦合团伙簇"
    elif dense_pair_cluster:
        cluster_shape = "多跳弱耦合团伙簇"
    else:
        cluster_shape = "候选松散关联群组"

    completeness_checks = {
        "profile_available": not focus_member_df.empty,
        "pair_relation_evidence_available": not top_pairs_df.empty,
        "shared_device_evidence_available": not cluster_devices_df.empty,
        "common_counterparty_evidence_available": not cluster_counterparts_df.empty,
        "subgroup_structure_available": bool(clusters),
        "core_node_ranking_available": not top_core_df.empty,
        "bridge_node_ranking_available": bridge_signal_available,
    }
    completeness_score = round(sum(1 for v in completeness_checks.values() if v) / len(completeness_checks), 4)

    result = {
        "seed_summary": {
            "seed_count": len(seed_ids),
            "candidate_count": len(candidate_ids),
            "candidate_scope": args.candidate_scope,
            "expanded_candidate_count": max(len(candidate_ids) - len(seed_ids), 0),
            "seed_preview": [preview(x) for x in seed_ids[:10]],
        },
        "focus_cluster_summary": {
            "cluster_shape": cluster_shape,
            "cluster_id": focus_cluster["cluster_id"] if focus_cluster else None,
            "cluster_size": focus_cluster["size"] if focus_cluster else len(focus_members),
            "seed_member_count": focus_cluster["seed_member_count"] if focus_cluster else len(seed_ids),
            "edge_count": focus_cluster["edge_count"] if focus_cluster else len(focus_pairs_df),
            "density": focus_cluster["density"] if focus_cluster else 0.0,
            "member_preview": [preview(x) for x in focus_members[:10]],
            "human_summary": f"识别到 1 个重点候选团伙簇，规模 {len(focus_members)} 人，形态判断为“{cluster_shape}”。",
        },
        "cluster_list": clusters,
        "core_nodes": top_core_df[[
            "phone_id", "is_seed", "cluster_id", "signal_score", "weighted_degree", "call_record_count", "counterparties", "shared_device_count", "night_call_ratio"
        ]].to_dict(orient="records") if not top_core_df.empty else [],
        "bridge_nodes": top_bridge_df.to_dict(orient="records") if not top_bridge_df.empty else [],
        "pair_evidence_top": top_pairs_df.to_dict(orient="records") if not top_pairs_df.empty else [],
        "shared_device_evidence_top": cluster_devices_df.to_dict(orient="records") if not cluster_devices_df.empty else [],
        "common_counterparty_evidence_top": cluster_counterparts_df.to_dict(orient="records") if not cluster_counterparts_df.empty else [],
        "investigation_next_steps": [
            "优先围绕 Top 设备继续做 shared-device-analysis，确认是否存在更大的设备池。",
            "对关系分最高的号码对继续做 association-path-analysis，验证多跳路径和桥接节点。",
            "对 Top 核心节点逐个做 single-number-analysis，补全单点画像与局部关系圈。",
            "若需要进一步确认同圈关系，可对 Top 核心节点两两做 overlap-analysis。",
        ],
        "yigraph_meta": {
            "recommended_query_types": ["subgraph_by_nodes", "common_neighbor", "relationship_filter", "path_query"],
            "explanation": "gang-cluster-analysis 主要是把 YiGraph 风格的关系过滤、共同邻居、局部子图和路径分析组合成团伙簇识别。",
        },
        "evidence_completeness": {**completeness_checks, "completeness_score": completeness_score},
        "artifact_roles": {
            "markdown_report": "给人直接看的完整结论版报告",
            "clusters_csv": "所有候选团伙簇的总览表",
            "core_nodes_csv": "核心成员明细表",
            "pairs_csv": "号码对关系证据表",
            "devices_csv": "共享设备证据表",
            "counterparts_csv": "共同对端证据表",
            "xlsx_workbook": "单文件交付版证据工作簿（多 sheet）",
        },
        "bridge_interpretation": (
            "当前重点簇内部高度紧密，没有明显单一桥接点，应优先关注核心节点与共享设备证据。"
            if (not bridge_signal_available and not top_bridge_df.empty) else "检测到可解释的桥接排序信号。"
        ),
    }

    group_slug = args.group_name.replace(" ", "_")
    member_count = len(focus_members)
    md_path = output_root / f"gang_cluster_report_{group_slug}_{member_count}members.md"
    cluster_csv_path = output_root / f"gang_cluster_report_{group_slug}_{member_count}members_clusters.csv"
    members_csv_path = output_root / f"gang_cluster_report_{group_slug}_{member_count}members_core_nodes.csv"
    pairs_csv_path = output_root / f"gang_cluster_report_{group_slug}_{member_count}members_pairs.csv"
    devices_csv_path = output_root / f"gang_cluster_report_{group_slug}_{member_count}members_devices.csv"
    counterparts_csv_path = output_root / f"gang_cluster_report_{group_slug}_{member_count}members_counterparts.csv"
    workbook_path = output_root / f"gang_cluster_report_{group_slug}_{member_count}members_evidence.xlsx"

    ensure_parent(md_path)
    pd.DataFrame(clusters).to_csv(cluster_csv_path, index=False, encoding="utf-8-sig")
    focus_member_df.to_csv(members_csv_path, index=False, encoding="utf-8-sig")
    focus_pairs_df.to_csv(pairs_csv_path, index=False, encoding="utf-8-sig")
    cluster_devices_df.to_csv(devices_csv_path, index=False, encoding="utf-8-sig")
    cluster_counterparts_df.to_csv(counterparts_csv_path, index=False, encoding="utf-8-sig")
    try:
        with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
            pd.DataFrame(clusters).to_excel(writer, sheet_name="clusters", index=False)
            focus_member_df.to_excel(writer, sheet_name="core_nodes", index=False)
            focus_pairs_df.to_excel(writer, sheet_name="pairs", index=False)
            cluster_devices_df.to_excel(writer, sheet_name="devices", index=False)
            cluster_counterparts_df.to_excel(writer, sheet_name="counterparts", index=False)
    except Exception:
        workbook_path = None

    lines: list[str] = []
    lines.append(f"# 团伙簇分析报告：{args.group_name}")
    lines.append("")
    lines.append("## 一、分析对象与总体结论")
    lines.append("")
    lines.append(f"- 输入种子号码数：{len(seed_ids)}")
    lines.append(f"- 候选号码数：{len(candidate_ids)}")
    lines.append(f"- 候选扩展方式：`{args.candidate_scope}`")
    lines.append(f"- 重点候选团伙簇规模：{len(focus_members)}")
    lines.append(f"- 团伙簇形态判断：{cluster_shape}")
    lines.append(f"- 证据完整度：{completeness_score}")
    lines.append(f"- 总体结论：{result['focus_cluster_summary']['human_summary']}")
    lines.append("")
    lines.append("## 二、候选扩展说明")
    lines.append("")
    for pid in seed_ids[:10]:
        lines.append(f"- 种子 `{preview(pid)}`：{candidate_reason.get(pid, ['seed_input'])[0]}")
    non_seed_preview = [pid for pid in candidate_ids if pid not in seed_set][:10]
    if non_seed_preview:
        lines.append(f"- 扩展候选预览：{', '.join([preview(x) for x in non_seed_preview])}")
    else:
        lines.append("- 本次没有扩展到新增候选号码，分析对象仅为输入号码本身。")
    lines.append("")
    lines.append("## 三、识别到的群组 / 团伙簇")
    lines.append("")
    if clusters:
        for cluster in clusters[: max(args.top_k, 3)]:
            lines.append(f"- {cluster['cluster_id']}：规模={cluster['size']}，种子成员数={cluster['seed_member_count']}，边数={cluster['edge_count']}，密度={cluster['density']}，设备预览={', '.join(cluster['top_devices_preview'][:3]) if cluster['top_devices_preview'] else '无'}")
    else:
        lines.append("- 未形成满足阈值的稳定簇，仅识别到若干弱关联候选关系。")
    lines.append("")
    lines.append("## 四、核心节点（Top）")
    lines.append("")
    if not top_core_df.empty:
        for rank, (_, row) in enumerate(top_core_df.iterrows(), start=1):
            lines.append(
                f"- Rank {rank}: `{preview(row['phone_id'])}` | core_score={row['signal_score']} | weighted_degree={row['weighted_degree']} | 通话={safe_int(row['call_record_count'])} | 联系人={safe_int(row['counterparties'])} | 共享设备={safe_int(row['shared_device_count'])}"
            )
    else:
        lines.append("- 无")
    lines.append("")
    lines.append("## 五、桥接点 / 关键连接节点")
    lines.append("")
    if not top_bridge_df.empty and bridge_signal_available:
        for rank, (_, row) in enumerate(top_bridge_df.iterrows(), start=1):
            lines.append(
                f"- Rank {rank}: `{preview(row['phone_id'])}` | betweenness={row['betweenness']} | weighted_degree={row['weighted_degree']} | seed={bool(row['is_seed'])}"
            )
    elif not top_bridge_df.empty:
        lines.append("- 当前重点团伙簇内部连通非常紧密，桥接中心性均为 0。")
        lines.append("- 这通常表示该簇更像一个高密度设备池型小团体，而不是依赖单一中间人串联起来的链式结构。")
        lines.append("- 因此本次更应该优先关注：核心节点、共享设备证据、以及高分号码对关系。")
    else:
        lines.append("- 无")
    lines.append("")
    lines.append("## 六、号码对证据链（Top）")
    lines.append("")
    if not top_pairs_df.empty:
        for _, row in top_pairs_df.iterrows():
            lines.append(
                f"- `{preview(row['phone_a'])}` ↔ `{preview(row['phone_b'])}`：relation_types={row['relation_types']}，shared_device_count={safe_int(row['shared_device_count'])}，common_counterparty_count={safe_int(row['common_counterparty_count'])}，internal_call_records={safe_int(row['internal_call_records'])}，neighbor_overlap_ratio={row['neighbor_overlap_ratio']}，relation_score={row['relation_score']}"
            )
    else:
        lines.append("- 无")
    lines.append("")
    lines.append("## 七、共享设备证据（Top）")
    lines.append("")
    if not cluster_devices_df.empty:
        for _, row in cluster_devices_df.iterrows():
            lines.append(
                f"- 设备 `{preview(row['device_id'])}`：群体内挂载成员数={safe_int(row['cluster_member_count'])}，设备总挂载号码数={safe_int(row['total_phone_count'])}，风险号码数={safe_int(row['risk_phone_count'])}，省份数={safe_int(row['province_count'])}"
            )
    else:
        lines.append("- 无")
    lines.append("")
    lines.append("## 八、共同对端证据（Top）")
    lines.append("")
    if not cluster_counterparts_df.empty:
        for _, row in cluster_counterparts_df.iterrows():
            lines.append(
                f"- 共同对端 `{preview(row['counterparty_id'])}`：被 {safe_int(row['cluster_member_count'])} 个成员共同接触，累计通话 {safe_int(row['total_calls'])} 次"
            )
    else:
        lines.append("- 无")
    lines.append("")
    lines.append("## 九、后续建议")
    lines.append("")
    for item in result["investigation_next_steps"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 十、实现说明（基础算子组合对齐）")
    lines.append("")
    lines.append("- 候选扩展 = neighbor_query + common_neighbor + relationship_filter")
    lines.append("- 团伙边构建 = common_neighbor + query_shared_device + relationship_filter")
    lines.append("- 团伙簇识别 = subgraph_by_nodes + aggregation_query")
    lines.append("- 核心节点/桥接点排序 = aggregation_query + 局部图排序")
    lines.append("- 关系复核建议 = association-path-analysis + overlap-analysis + single-number-analysis")
    lines.append("")
    lines.append("## 十一、生成文件")
    lines.append("")
    lines.append(f"1. Markdown 报告：`{md_path.name}`（给人直接阅读的总报告）")
    lines.append(f"2. 团伙簇 CSV：`{cluster_csv_path.name}`（cluster 级总览）")
    lines.append(f"3. 核心节点 CSV：`{members_csv_path.name}`（成员级优先排查表）")
    lines.append(f"4. 号码对证据 CSV：`{pairs_csv_path.name}`（pair 级关系链表）")
    lines.append(f"5. 共享设备证据 CSV：`{devices_csv_path.name}`（device 级证据表）")
    lines.append(f"6. 共同对端证据 CSV：`{counterparts_csv_path.name}`（counterparty 级证据表）")
    if workbook_path:
        lines.append(f"7. Excel 证据工作簿：`{workbook_path.name}`（把 5 张 csv 分 sheet 汇总到一个文件里，便于单文件交付）")
    lines.append("")
    lines.append("说明：不建议把这 5 张 csv 硬合并成 1 张大表，因为它们分别是 cluster / member / pair / device / counterparty 不同粒度。")
    lines.append("如果需要单文件交付，本次额外生成了 Excel 工作簿，每类证据各放一个 sheet。")
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    payload = {
        "ok": True,
        "skill": "gang-cluster-analysis",
        "query_type": "subgraph_by_nodes",
        "input_summary": {
            "script_version": SCRIPT_VERSION,
            "group_name": args.group_name,
            "seed_count": len(seed_ids),
            "candidate_count": len(candidate_ids),
            "candidate_scope": args.candidate_scope,
            "dataset_root": str(dataset_root),
            "dataset": args.dataset,
            "artifact_mode": args.artifact_mode,
            "top_k": args.top_k,
            "min_shared_device_count": args.min_shared_device_count,
            "min_common_counterparty_count": args.min_common_counterparty_count,
            "min_neighbor_overlap": args.min_neighbor_overlap,
            "min_edge_score": args.min_edge_score,
            "focus_min_cluster_size": args.focus_min_cluster_size,
        },
        "result": result,
        "notes": [
            "本技能把 shared-device-analysis、overlap-analysis、association-path-analysis、subgraph-extraction-analysis 的证据思路组合成团伙簇识别。",
            "如果输入成员很少或关系很弱，可能只会得到候选弱关联群组，而不是紧密团伙簇。",
            "若重点团伙簇密度接近 1，则桥接中心性可能全部为 0，这不表示没有价值，而是说明该簇内部过于紧密，更应关注核心成员和共享设备。",
        ],
        "report_path": str(md_path),
        "artifacts": (
            [
                {"type": "markdown_report", "path": str(md_path), "title": md_path.name},
            ]
            if args.artifact_mode == "markdown_only"
            else [
                {"type": "markdown_report", "path": str(md_path), "title": md_path.name},
                {"type": "csv", "path": str(cluster_csv_path), "title": cluster_csv_path.name},
                {"type": "csv", "path": str(members_csv_path), "title": members_csv_path.name},
                {"type": "csv", "path": str(pairs_csv_path), "title": pairs_csv_path.name},
                {"type": "csv", "path": str(devices_csv_path), "title": devices_csv_path.name},
                {"type": "csv", "path": str(counterparts_csv_path), "title": counterparts_csv_path.name},
            ] + ([{"type": "xlsx", "path": str(workbook_path), "title": workbook_path.name}] if workbook_path else [])
        ),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
