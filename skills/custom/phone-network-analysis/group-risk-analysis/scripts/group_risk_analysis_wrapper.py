#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional

import duckdb
import pandas as pd

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


def read_csv_columns(path: Path) -> list[str]:
    return list(pd.read_csv(path, nrows=0).columns)


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
    checked = [str(x) for x in candidates]
    raise FileNotFoundError(
        "Could not resolve phone-network dataset root. Checked: " + "; ".join(checked)
    )


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


def parse_phone_ids(args: argparse.Namespace) -> list[str]:
    ids: list[str] = []
    if args.phone_ids:
        ids.extend([x.strip() for x in args.phone_ids.split(",") if x.strip()])
    if args.phone_id_file:
        ids.extend(
            [
                line.strip()
                for line in Path(args.phone_id_file).expanduser().read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        )
    if args.input_csv:
        df = pd.read_csv(Path(args.input_csv).expanduser())
        if args.phone_id_column not in df.columns:
            raise ValueError(f"phone-id column '{args.phone_id_column}' not found in input csv")
        ids.extend([str(x).strip() for x in df[args.phone_id_column].dropna().tolist() if str(x).strip()])

    seen: set[str] = set()
    deduped: list[str] = []
    for pid in ids:
        if pid not in seen:
            seen.add(pid)
            deduped.append(pid)
    if not deduped:
        raise ValueError("No phone ids provided. Use --phone-ids, --phone-id-file, or --input-csv.")
    return deduped


def values_sql(column_name: str, values: Iterable[str]) -> str:
    escaped = [v.replace("'", "''") for v in values]
    return "SELECT * FROM (VALUES " + ", ".join([f"('{v}')" for v in escaped]) + f") AS t({column_name})"


def quantile_threshold(series: pd.Series, floor_value: float) -> int:
    non_null = series.dropna()
    if non_null.empty:
        return int(floor_value)
    q = float(non_null.quantile(0.75))
    return int(max(floor_value, math.ceil(q)))


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


def detect_time_column(columns: list[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for candidate in TIME_CANDIDATES:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def parse_list_arg(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    return [x.strip().lower() for x in raw.split(",") if x.strip()]


def classify_group_shape(focus_signal: str, pair_relation_matrix: pd.DataFrame, subgroups: list[dict[str, Any]]) -> str:
    if focus_signal == "shared_device_pool_group":
        if subgroups and subgroups[0]["size"] >= 4:
            return "设备池型紧密群组"
        return "共享设备驱动群组"
    if focus_signal == "night_abnormal_group":
        return "夜间异常活跃群组"
    if focus_signal == "high_call_volume_group":
        return "高通话量扩散型群组"
    if focus_signal == "broad_contact_group":
        return "联系人广度扩张型群组"
    if not pair_relation_matrix.empty and pair_relation_matrix["common_counterparty_count"].max() >= 2:
        return "共同对端耦合型群组"
    return "混合弱耦合群组"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Group-level risk analysis for phone collections")
    parser.add_argument("--phone-ids", type=str, default=None, help="Comma-separated phone ids")
    parser.add_argument("--phone-id-file", type=str, default=None, help="Text file with one phone id per line")
    parser.add_argument("--input-csv", type=str, default=None, help="CSV containing a phone id column")
    parser.add_argument("--phone-id-column", type=str, default="phone_id")
    parser.add_argument("--group-name", type=str, default="group")
    parser.add_argument("--dataset-root", type=str, default=None)
    parser.add_argument("--dataset", type=str, default="unified")
    parser.add_argument("--artifact-mode", choices=["full", "essential", "markdown_only"], default="full")
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--pattern-min-members", type=int, default=2)
    parser.add_argument("--night-start-hour", type=int, default=22)
    parser.add_argument("--night-end-hour", type=int, default=6)
    parser.add_argument("--night-ratio-threshold", type=float, default=0.30)
    parser.add_argument("--night-count-threshold", type=int, default=10)
    parser.add_argument("--high-call-threshold", type=int, default=None)
    parser.add_argument("--broad-contact-threshold", type=int, default=None)
    parser.add_argument("--shared-device-threshold", type=int, default=1)
    parser.add_argument("--shared-peer-threshold", type=int, default=None)
    parser.add_argument("--risk-only", action="store_true")
    parser.add_argument("--province", type=str, default=None)
    parser.add_argument("--include-sub-labels", type=str, default=None, help="Comma-separated sub_label whitelist")
    parser.add_argument("--exclude-sub-labels", type=str, default=None, help="Comma-separated sub_label blacklist")
    parser.add_argument("--min-call-records", type=int, default=0)
    parser.add_argument("--min-counterparties", type=int, default=0)
    parser.add_argument("--min-shared-device-count", type=int, default=0)
    parser.add_argument("--min-shared-peer-total", type=int, default=0)
    parser.add_argument("--min-device-pool-count", type=int, default=0)
    parser.add_argument("--min-common-counterparty-members", type=int, default=2)
    return parser


def main() -> None:
    args = build_parser().parse_args()

    original_phone_ids = parse_phone_ids(args)
    dataset_root = resolve_dataset_root(args.dataset_root, args.dataset)
    output_root = resolve_output_root(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    call_edges_path = dataset_root / "processed" / args.dataset / "call_edges.csv"
    user_nodes_path = dataset_root / "processed" / args.dataset / "user_nodes.csv"
    device_edges_path = dataset_root / "processed" / "graph_views" / args.dataset / "edges_phone_imei.parquet"

    call_columns = read_csv_columns(call_edges_path)
    time_col = detect_time_column(call_columns)

    conn = duckdb.connect(database=":memory:")
    group_ids_sql = values_sql("phone_id", original_phone_ids)

    user_nodes_path_sql = str(user_nodes_path).replace("'", "''")
    call_edges_path_sql = str(call_edges_path).replace("'", "''")
    device_edges_path_sql = str(device_edges_path).replace("'", "''")

    profiles = conn.execute(
        f"""
        WITH group_ids AS ({group_ids_sql})
        SELECT g.phone_id,
               u.province,
               u.dataset_name,
               u.label,
               u.sub_label,
               u.source_table
        FROM group_ids g
        LEFT JOIN read_csv_auto('{user_nodes_path_sql}') u
        ON g.phone_id = u.user_id
        """
    ).df()

    members = pd.DataFrame({"phone_id": original_phone_ids}).merge(profiles, on="phone_id", how="left")
    filter_effect: dict[str, Any] = {
        "input_member_count": len(members),
        "after_label_filter_count": len(members),
        "after_sub_label_filter_count": len(members),
        "after_province_filter_count": len(members),
        "after_metric_filter_count": len(members),
    }

    include_sub_labels = set(parse_list_arg(args.include_sub_labels))
    exclude_sub_labels = set(parse_list_arg(args.exclude_sub_labels))

    if args.risk_only:
        members = members[members["label"].fillna(0).astype(float) == 1.0].copy()
    filter_effect["after_label_filter_count"] = len(members)

    if include_sub_labels:
        members = members[members["sub_label"].fillna("").astype(str).str.lower().isin(include_sub_labels)].copy()
    if exclude_sub_labels:
        members = members[~members["sub_label"].fillna("").astype(str).str.lower().isin(exclude_sub_labels)].copy()
    filter_effect["after_sub_label_filter_count"] = len(members)

    if args.province:
        members = members[members["province"].fillna("").str.lower() == args.province.lower()].copy()
    filter_effect["after_province_filter_count"] = len(members)

    if members.empty:
        result = {
            "ok": False,
            "skill": "group-risk-analysis",
            "error": "No group members remain after label/province/sub_label filtering.",
            "filter_effect": filter_effect,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))
        return

    group_ids_sql = values_sql("phone_id", members["phone_id"].astype(str).tolist())

    call_metrics = conn.execute(
        f"""
        WITH group_ids AS ({group_ids_sql})
        SELECT src_user_id AS phone_id,
               COUNT(*) AS call_record_count,
               COUNT(DISTINCT dst_counterparty_id) AS counterparties,
               SUM(CASE WHEN dst_counterparty_id IN (SELECT phone_id FROM group_ids) THEN 1 ELSE 0 END) AS internal_call_count,
               COUNT(DISTINCT CASE WHEN dst_counterparty_id IN (SELECT phone_id FROM group_ids) THEN dst_counterparty_id END) AS internal_neighbors
        FROM read_csv_auto('{call_edges_path_sql}')
        WHERE src_user_id IN (SELECT phone_id FROM group_ids)
        GROUP BY 1
        """
    ).df()
    members = members.merge(call_metrics, on="phone_id", how="left")

    if time_col:
        night_expr = (
            f"CASE WHEN EXTRACT('hour' FROM TRY_CAST({time_col} AS TIMESTAMP)) >= {args.night_start_hour} "
            f"OR EXTRACT('hour' FROM TRY_CAST({time_col} AS TIMESTAMP)) < {args.night_end_hour} THEN 1 ELSE 0 END"
        )
        night_metrics = conn.execute(
            f"""
            WITH group_ids AS ({group_ids_sql})
            SELECT src_user_id AS phone_id,
                   SUM({night_expr}) AS night_call_count,
                   COUNT(*) AS total_timed_calls
            FROM read_csv_auto('{call_edges_path_sql}')
            WHERE src_user_id IN (SELECT phone_id FROM group_ids)
              AND TRY_CAST({time_col} AS TIMESTAMP) IS NOT NULL
            GROUP BY 1
            """
        ).df()
        members = members.merge(night_metrics, on="phone_id", how="left")
        members["night_time_available"] = True
    else:
        members["night_call_count"] = 0
        members["total_timed_calls"] = 0
        members["night_time_available"] = False

    device_metrics = conn.execute(
        f"""
        WITH group_ids AS ({group_ids_sql}),
        device_usage AS (
            SELECT e.imei,
                   COUNT(DISTINCT e.user_id) AS device_phone_count,
                   SUM(CASE WHEN COALESCE(u.label, 0) = 1 THEN 1 ELSE 0 END) AS device_risk_phone_count,
                   COUNT(DISTINCT COALESCE(u.province, 'unknown')) AS province_count,
                   COUNT(DISTINCT COALESCE(u.sub_label, 'unknown')) AS sub_label_count
            FROM read_parquet('{device_edges_path_sql}') e
            LEFT JOIN read_csv_auto('{user_nodes_path_sql}') u
            ON e.user_id = u.user_id
            GROUP BY 1
        ),
        group_device AS (
            SELECT *
            FROM read_parquet('{device_edges_path_sql}')
            WHERE user_id IN (SELECT phone_id FROM group_ids)
        )
        SELECT gd.user_id AS phone_id,
               COUNT(DISTINCT gd.imei) AS device_count,
               COUNT(DISTINCT CASE WHEN du.device_phone_count >= 2 THEN gd.imei END) AS shared_device_count,
               SUM(CASE WHEN du.device_phone_count >= 2 THEN du.device_phone_count - 1 ELSE 0 END) AS shared_peer_total,
               MAX(CASE WHEN du.device_phone_count >= 2 THEN du.device_phone_count - 1 ELSE 0 END) AS strongest_shared_device_peer_count,
               COUNT(DISTINCT CASE WHEN du.device_phone_count >= 3 THEN gd.imei END) AS shared_device_pool_count,
               COUNT(DISTINCT CASE WHEN du.province_count >= 2 THEN gd.imei END) AS cross_province_shared_device_count,
               COUNT(DISTINCT CASE WHEN du.sub_label_count >= 2 THEN gd.imei END) AS mixed_label_shared_device_count
        FROM group_device gd
        JOIN device_usage du
        ON gd.imei = du.imei
        GROUP BY 1
        """
    ).df()
    members = members.merge(device_metrics, on="phone_id", how="left")

    for col in [
        "call_record_count",
        "counterparties",
        "internal_call_count",
        "internal_neighbors",
        "night_call_count",
        "total_timed_calls",
        "device_count",
        "shared_device_count",
        "shared_peer_total",
        "strongest_shared_device_peer_count",
        "shared_device_pool_count",
        "cross_province_shared_device_count",
        "mixed_label_shared_device_count",
    ]:
        if col not in members.columns:
            members[col] = 0
        members[col] = members[col].fillna(0)

    members["night_call_ratio"] = members.apply(
        lambda r: safe_float(r["night_call_count"]) / safe_float(r["total_timed_calls"])
        if safe_float(r["total_timed_calls"]) > 0
        else 0.0,
        axis=1,
    )

    members = members[
        (members["call_record_count"] >= args.min_call_records)
        & (members["counterparties"] >= args.min_counterparties)
        & (members["shared_device_count"] >= args.min_shared_device_count)
        & (members["shared_peer_total"] >= args.min_shared_peer_total)
        & (members["shared_device_pool_count"] >= args.min_device_pool_count)
    ].copy()
    filter_effect["after_metric_filter_count"] = len(members)

    if members.empty:
        result = {
            "ok": False,
            "skill": "group-risk-analysis",
            "error": "No group members remain after metric thresholds.",
            "filter_effect": filter_effect,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))
        return

    group_ids_sql = values_sql("phone_id", members["phone_id"].astype(str).tolist())

    shared_device_pairs = conn.execute(
        f"""
        WITH group_ids AS ({group_ids_sql}),
        group_device AS (
            SELECT user_id, imei
            FROM read_parquet('{device_edges_path_sql}')
            WHERE user_id IN (SELECT phone_id FROM group_ids)
        ),
        device_usage AS (
            SELECT imei, COUNT(DISTINCT user_id) AS device_phone_count
            FROM read_parquet('{device_edges_path_sql}')
            GROUP BY 1
        )
        SELECT a.user_id AS phone_a,
               b.user_id AS phone_b,
               COUNT(DISTINCT a.imei) AS shared_device_count,
               MAX(du.device_phone_count) AS max_shared_device_phone_count,
               STRING_AGG(DISTINCT a.imei, ' | ' ORDER BY a.imei) AS shared_devices
        FROM group_device a
        JOIN group_device b
          ON a.imei = b.imei AND a.user_id < b.user_id
        JOIN device_usage du
          ON a.imei = du.imei
        GROUP BY 1, 2
        ORDER BY shared_device_count DESC, phone_a, phone_b
        """
    ).df()

    internal_call_pairs = conn.execute(
        f"""
        WITH group_ids AS ({group_ids_sql})
        SELECT LEAST(src_user_id, dst_counterparty_id) AS phone_a,
               GREATEST(src_user_id, dst_counterparty_id) AS phone_b,
               COUNT(*) AS internal_call_records,
               COUNT(DISTINCT src_user_id || '->' || dst_counterparty_id) AS directed_edge_count
        FROM read_csv_auto('{call_edges_path_sql}')
        WHERE src_user_id IN (SELECT phone_id FROM group_ids)
          AND dst_counterparty_id IN (SELECT phone_id FROM group_ids)
          AND src_user_id <> dst_counterparty_id
        GROUP BY 1, 2
        ORDER BY internal_call_records DESC, phone_a, phone_b
        """
    ).df()

    common_counterparty_pairs = conn.execute(
        f"""
        WITH group_ids AS ({group_ids_sql}),
        group_calls AS (
            SELECT src_user_id AS phone_id,
                   dst_counterparty_id AS counterpart,
                   COUNT(*) AS call_count_to_counterpart
            FROM read_csv_auto('{call_edges_path_sql}')
            WHERE src_user_id IN (SELECT phone_id FROM group_ids)
            GROUP BY 1, 2
        )
        SELECT LEAST(a.phone_id, b.phone_id) AS phone_a,
               GREATEST(a.phone_id, b.phone_id) AS phone_b,
               COUNT(DISTINCT a.counterpart) AS common_counterparty_count,
               SUM(a.call_count_to_counterpart + b.call_count_to_counterpart) AS common_counterparty_total_calls,
               STRING_AGG(DISTINCT a.counterpart, ' | ' ORDER BY a.counterpart) AS common_counterparties
        FROM group_calls a
        JOIN group_calls b
          ON a.counterpart = b.counterpart AND a.phone_id < b.phone_id
        GROUP BY 1, 2
        HAVING COUNT(DISTINCT a.counterpart) >= {max(2, args.min_common_counterparty_members)}
        ORDER BY common_counterparty_count DESC, common_counterparty_total_calls DESC, phone_a, phone_b
        """
    ).df()

    top_common_counterparties = conn.execute(
        f"""
        WITH group_ids AS ({group_ids_sql})
        SELECT dst_counterparty_id AS counterpart,
               COUNT(DISTINCT src_user_id) AS member_count,
               COUNT(*) AS total_calls,
               STRING_AGG(DISTINCT src_user_id, ' | ' ORDER BY src_user_id) AS members
        FROM read_csv_auto('{call_edges_path_sql}')
        WHERE src_user_id IN (SELECT phone_id FROM group_ids)
        GROUP BY 1
        HAVING COUNT(DISTINCT src_user_id) >= {max(2, args.min_common_counterparty_members)}
        ORDER BY member_count DESC, total_calls DESC, counterpart
        LIMIT {max(10, args.top_k)}
        """
    ).df()

    top_group_devices = conn.execute(
        f"""
        WITH group_ids AS ({group_ids_sql}),
        group_device AS (
            SELECT *
            FROM read_parquet('{device_edges_path_sql}')
            WHERE user_id IN (SELECT phone_id FROM group_ids)
        ),
        device_usage AS (
            SELECT e.imei,
                   COUNT(DISTINCT e.user_id) AS device_phone_count,
                   SUM(CASE WHEN COALESCE(u.label, 0) = 1 THEN 1 ELSE 0 END) AS risk_phone_count,
                   COUNT(DISTINCT COALESCE(u.province, 'unknown')) AS province_count,
                   COUNT(DISTINCT COALESCE(u.sub_label, 'unknown')) AS sub_label_count
            FROM read_parquet('{device_edges_path_sql}') e
            LEFT JOIN read_csv_auto('{user_nodes_path_sql}') u
            ON e.user_id = u.user_id
            GROUP BY 1
        )
        SELECT gd.imei AS device_id,
               COUNT(DISTINCT gd.user_id) AS group_member_count,
               du.device_phone_count,
               du.risk_phone_count,
               du.province_count,
               du.sub_label_count,
               STRING_AGG(DISTINCT gd.user_id, ' | ' ORDER BY gd.user_id) AS group_members
        FROM group_device gd
        JOIN device_usage du ON gd.imei = du.imei
        GROUP BY 1, 3, 4, 5, 6
        ORDER BY group_member_count DESC, device_phone_count DESC, device_id
        LIMIT {max(10, args.top_k)}
        """
    ).df()

    high_call_threshold = args.high_call_threshold or quantile_threshold(members["call_record_count"], 100)
    broad_contact_threshold = args.broad_contact_threshold or quantile_threshold(members["counterparties"], 50)
    shared_peer_threshold = args.shared_peer_threshold or quantile_threshold(members["shared_peer_total"], 3)

    members["flag_high_call"] = members["call_record_count"] >= high_call_threshold
    members["flag_broad_contact"] = members["counterparties"] >= broad_contact_threshold
    members["flag_shared_device"] = (
        (members["shared_device_count"] >= args.shared_device_threshold)
        | (members["shared_peer_total"] >= shared_peer_threshold)
    )
    members["flag_night_abnormal"] = (
        members["night_time_available"].astype(bool)
        & (members["night_call_count"] >= args.night_count_threshold)
        & (members["night_call_ratio"] >= args.night_ratio_threshold)
    )

    pattern_fields = [
        "flag_high_call",
        "flag_night_abnormal",
        "flag_broad_contact",
        "flag_shared_device",
    ]
    members["signal_count"] = members[pattern_fields].sum(axis=1)
    members["driver_types"] = members.apply(
        lambda r: [
            name
            for name, field in [
                ("high_call_volume", "flag_high_call"),
                ("night_abnormal", "flag_night_abnormal"),
                ("broad_contact", "flag_broad_contact"),
                ("shared_device", "flag_shared_device"),
            ]
            if bool(r[field])
        ],
        axis=1,
    )

    members["group_core_score"] = (
        members["signal_count"] * 20
        + members["shared_peer_total"].clip(upper=200) * 0.4
        + members["call_record_count"].clip(upper=500) * 0.03
        + members["counterparties"].clip(upper=500) * 0.05
        + members["shared_device_pool_count"].clip(upper=20) * 3
        + members["internal_neighbors"].clip(upper=20) * 2
        + (members["label"].fillna(0).astype(float) == 1.0).astype(int) * 8
    )

    members = members.sort_values(
        by=["signal_count", "group_core_score", "shared_peer_total", "call_record_count"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    members["group_rank"] = range(1, len(members) + 1)
    members["driver_type"] = members["driver_types"].apply(lambda xs: ", ".join(xs) if xs else "weak_or_unclear")

    major_patterns: list[str] = []
    pattern_summaries: dict[str, Any] = {}
    pattern_defs = [
        ("high_call_volume", "高通话量型", "flag_high_call", high_call_threshold, "call_record_count"),
        ("night_abnormal", "夜间异常型", "flag_night_abnormal", args.night_ratio_threshold, "night_call_ratio"),
        ("broad_contact", "联系人广度异常型", "flag_broad_contact", broad_contact_threshold, "counterparties"),
        ("shared_device", "共享设备型", "flag_shared_device", args.shared_device_threshold, "shared_device_count"),
    ]

    for key, zh_name, flag_col, threshold, sort_col in pattern_defs:
        subset = members[members[flag_col]].copy()
        triggered = len(subset) >= min(args.pattern_min_members, max(1, len(members)))
        if triggered:
            major_patterns.append(zh_name)
        non_trigger_reason = None
        if not triggered:
            if key == "night_abnormal" and not bool(members["night_time_available"].any()):
                non_trigger_reason = "当前通话边文件未识别到可解析时间列，无法判断夜间异常型。"
            else:
                non_trigger_reason = f"命中成员数仅 {len(subset)}，未达到最小模式成员数 {min(args.pattern_min_members, max(1, len(members)))}。"
        pattern_summaries[key] = {
            "name": zh_name,
            "triggered": triggered,
            "member_count": int(len(subset)),
            "threshold": threshold,
            "non_trigger_reason": non_trigger_reason,
            "top_members": [
                {
                    "phone_id": row.phone_id,
                    "phone_preview": preview(row.phone_id),
                    sort_col: row[sort_col],
                    "signal_count": int(row.signal_count),
                }
                for _, row in subset.sort_values(sort_col, ascending=False).head(args.top_k).iterrows()
            ],
        }

    night_available = bool(members["night_time_available"].any())

    shared_device_pairs_norm = shared_device_pairs.copy()
    internal_call_pairs_norm = internal_call_pairs.copy()
    common_counterparty_pairs_norm = common_counterparty_pairs.copy()
    if shared_device_pairs_norm.empty:
        shared_device_pairs_norm = pd.DataFrame(columns=["phone_a", "phone_b", "shared_device_count", "max_shared_device_phone_count", "shared_devices"])
    if internal_call_pairs_norm.empty:
        internal_call_pairs_norm = pd.DataFrame(columns=["phone_a", "phone_b", "internal_call_records", "directed_edge_count"])
    if common_counterparty_pairs_norm.empty:
        common_counterparty_pairs_norm = pd.DataFrame(columns=["phone_a", "phone_b", "common_counterparty_count", "common_counterparty_total_calls", "common_counterparties"])

    pair_relation_matrix = shared_device_pairs_norm.merge(internal_call_pairs_norm, on=["phone_a", "phone_b"], how="outer")
    pair_relation_matrix = pair_relation_matrix.merge(common_counterparty_pairs_norm, on=["phone_a", "phone_b"], how="outer")
    for col in ["shared_device_count", "max_shared_device_phone_count", "internal_call_records", "directed_edge_count", "common_counterparty_count", "common_counterparty_total_calls"]:
        if col not in pair_relation_matrix.columns:
            pair_relation_matrix[col] = 0
        pair_relation_matrix[col] = pair_relation_matrix[col].fillna(0)
    for col in ["shared_devices", "common_counterparties"]:
        if col not in pair_relation_matrix.columns:
            pair_relation_matrix[col] = ""
        pair_relation_matrix[col] = pair_relation_matrix[col].fillna("")

    pair_relation_matrix["relation_types"] = pair_relation_matrix.apply(
        lambda r: ", ".join(
            [
                x
                for x, ok in [
                    ("shared_device", safe_int(r["shared_device_count"]) > 0),
                    ("internal_call", safe_int(r["internal_call_records"]) > 0),
                    ("common_counterparty", safe_int(r["common_counterparty_count"]) > 0),
                ]
                if ok
            ]
        ) or "weak_or_none",
        axis=1,
    )
    pair_relation_matrix["relation_score"] = (
        pair_relation_matrix["shared_device_count"].apply(safe_int) * 8
        + pair_relation_matrix["internal_call_records"].apply(safe_int) * 2
        + pair_relation_matrix["common_counterparty_count"].apply(safe_int) * 3
    )
    pair_relation_matrix = pair_relation_matrix.sort_values(
        by=["relation_score", "shared_device_count", "common_counterparty_count", "internal_call_records"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    shared_device_edges = [(str(r.phone_a), str(r.phone_b)) for _, r in pair_relation_matrix.iterrows() if safe_int(r.shared_device_count) >= 1]
    internal_edges_undirected = {(str(r.phone_a), str(r.phone_b)) for _, r in pair_relation_matrix.iterrows() if safe_int(r.internal_call_records) > 0}
    common_counterparty_edges = {(str(r.phone_a), str(r.phone_b)) for _, r in pair_relation_matrix.iterrows() if safe_int(r.common_counterparty_count) > 0}
    all_component_edges = set(shared_device_edges) | internal_edges_undirected | common_counterparty_edges
    components = union_find_components(members["phone_id"].astype(str).tolist(), list(all_component_edges))
    component_summaries = [
        {
            "component_id": idx + 1,
            "size": len(comp),
            "members": comp,
            "member_previews": [preview(x) for x in comp],
        }
        for idx, comp in enumerate(components)
        if len(comp) >= 2
    ]

    internal_call_density = 0.0
    n = len(members)
    if n >= 2:
        possible_undirected = n * (n - 1) / 2
        internal_call_density = len(internal_edges_undirected) / possible_undirected if possible_undirected else 0.0

    label_counts = Counter(members["sub_label"].fillna("unknown").astype(str).tolist())
    province_counts = Counter(members["province"].fillna("unknown").astype(str).tolist())

    focus_signal = "mixed_or_unclear_group"
    if pattern_summaries["shared_device"]["triggered"] and top_group_devices.shape[0] > 0 and safe_int(top_group_devices.iloc[0]["device_phone_count"]) >= 3:
        focus_signal = "shared_device_pool_group"
    elif pattern_summaries["night_abnormal"]["triggered"]:
        focus_signal = "night_abnormal_group"
    elif pattern_summaries["high_call_volume"]["triggered"]:
        focus_signal = "high_call_volume_group"
    elif pattern_summaries["broad_contact"]["triggered"]:
        focus_signal = "broad_contact_group"

    group_shape = classify_group_shape(focus_signal, pair_relation_matrix, component_summaries)

    evidence_completeness = {
        "profile_available": bool(members[["province", "sub_label"]].notna().any().any()),
        "time_evidence_available": night_available,
        "shared_device_evidence_available": not top_group_devices.empty,
        "pair_relation_evidence_available": not pair_relation_matrix.empty,
        "common_counterparty_evidence_available": not top_common_counterparties.empty,
        "subgroup_structure_available": bool(component_summaries),
    }
    evidence_completeness["completeness_score"] = round(
        sum(1 for v in evidence_completeness.values() if isinstance(v, bool) and v) / 6.0,
        3,
    )

    filter_notes: list[str] = []
    if args.risk_only and filter_effect["after_label_filter_count"] == filter_effect["input_member_count"]:
        filter_notes.append("risk_only 过滤未改变样本，因为当前输入号码全部都是风险标签号码。")
    if args.province and filter_effect["after_province_filter_count"] == filter_effect["after_sub_label_filter_count"]:
        filter_notes.append(f"province={args.province} 过滤未改变样本，因为当前保留号码全部来自该省份。")
    if args.min_shared_device_count > 0 and filter_effect["after_metric_filter_count"] == filter_effect["after_province_filter_count"]:
        filter_notes.append("当前共享设备阈值过滤未改变样本，因为保留号码全部满足共享设备阈值。")
    if include_sub_labels or exclude_sub_labels:
        filter_notes.append("已按 sub_label 进行集合过滤，用于形成更聚焦的群体分析对象。")
    if not filter_notes:
        filter_notes.append("本次过滤条件对群体样本产生了有限影响，输出可视为该号码集合的总体画像。")

    confidence_score = round(
        min(
            1.0,
            0.2 * len(major_patterns)
            + 0.2 * min(1.0, len(shared_device_pairs) / max(1, len(members)))
            + 0.2 * min(1.0, len(top_common_counterparties) / 5)
            + 0.2 * evidence_completeness["completeness_score"]
            + 0.2 * (1.0 if component_summaries else 0.0),
        ),
        3,
    )

    core_members = members.head(args.top_k).copy()
    core_members["evidence_brief"] = core_members.apply(
        lambda r: "; ".join(
            [
                f"通话={safe_int(r.call_record_count)}",
                f"联系人={safe_int(r.counterparties)}",
                f"共享设备={safe_int(r.shared_device_count)}",
                f"设备池={safe_int(r.shared_device_pool_count)}",
                f"夜间占比={round(safe_float(r.night_call_ratio), 4)}",
            ]
        ),
        axis=1,
    )
    core_members_export = core_members[[
        "group_rank",
        "phone_id",
        "province",
        "label",
        "sub_label",
        "call_record_count",
        "counterparties",
        "night_call_count",
        "night_call_ratio",
        "device_count",
        "shared_device_count",
        "shared_peer_total",
        "strongest_shared_device_peer_count",
        "shared_device_pool_count",
        "signal_count",
        "driver_type",
        "group_core_score",
        "evidence_brief",
    ]].copy()

    group_name_slug = args.group_name.strip().replace(" ", "_") or "group"
    report_base = f"group_risk_report_{group_name_slug}_{len(members)}members"
    report_path = output_root / f"{report_base}.md"
    member_csv_path = output_root / f"{report_base}_members.csv"
    pair_csv_path = output_root / f"{report_base}_pairs.csv"
    device_csv_path = output_root / f"{report_base}_devices.csv"
    counterpart_csv_path = output_root / f"{report_base}_counterparts.csv"

    ensure_parent(report_path)
    core_members_export.to_csv(member_csv_path, index=False)
    pair_relation_matrix.to_csv(pair_csv_path, index=False)
    top_group_devices.to_csv(device_csv_path, index=False)
    top_common_counterparties.to_csv(counterpart_csv_path, index=False)

    markdown_lines: list[str] = []
    markdown_lines.append(f"# 群体风险分析报告：{args.group_name}")
    markdown_lines.append("")
    markdown_lines.append("## 一、分析对象与总体结论")
    markdown_lines.append("")
    markdown_lines.append(f"- 群体号码数：{len(members)}")
    markdown_lines.append(f"- 数据集根目录：`{dataset_root}`")
    markdown_lines.append(f"- 重点信号：`{focus_signal}`")
    markdown_lines.append(f"- 群体形态判断：{group_shape}")
    markdown_lines.append(f"- 已识别主要风险群体特征：{', '.join(major_patterns) if major_patterns else '暂未触发明确主特征'}")
    markdown_lines.append(f"- 省份分布：{dict(province_counts)}")
    markdown_lines.append(f"- 标签分布：{dict(label_counts)}")
    markdown_lines.append(f"- 证据完整度：{evidence_completeness['completeness_score']} / 1.0")
    markdown_lines.append(f"- 分析置信度：{confidence_score} / 1.0")
    markdown_lines.append("")
    markdown_lines.append("## 二、过滤与样本影响说明")
    markdown_lines.append("")
    markdown_lines.append(f"- 初始输入成员数：{filter_effect['input_member_count']}")
    markdown_lines.append(f"- 标签过滤后成员数：{filter_effect['after_label_filter_count']}")
    markdown_lines.append(f"- sub_label 过滤后成员数：{filter_effect['after_sub_label_filter_count']}")
    markdown_lines.append(f"- 省份过滤后成员数：{filter_effect['after_province_filter_count']}")
    markdown_lines.append(f"- 指标阈值过滤后成员数：{filter_effect['after_metric_filter_count']}")
    for note in filter_notes:
        markdown_lines.append(f"- 说明：{note}")
    markdown_lines.append("")
    markdown_lines.append("## 三、群体级统计")
    markdown_lines.append("")
    markdown_lines.append(f"- 总通话记录数（按号码汇总）：{int(members['call_record_count'].sum())}")
    markdown_lines.append(f"- 平均通话记录数：{round(float(members['call_record_count'].mean()), 2)}")
    markdown_lines.append(f"- 平均联系人广度：{round(float(members['counterparties'].mean()), 2)}")
    markdown_lines.append(f"- 总共享设备数（按号码汇总）：{int(members['shared_device_count'].sum())}")
    markdown_lines.append(f"- 总共享设备牵出号码数：{int(members['shared_peer_total'].sum())}")
    markdown_lines.append(f"- 群体内部关系边数量（含内部通话/共用设备/共同对端）：{len(pair_relation_matrix)}")
    markdown_lines.append(f"- 群体内部通话密度：{round(internal_call_density, 4)}")
    markdown_lines.append(f"- 共用设备号码对数量：{len(shared_device_pairs)}")
    markdown_lines.append(f"- 共同对端数量（至少 {max(2, args.min_common_counterparty_members)} 个成员共同接触）：{len(top_common_counterparties)}")
    if night_available:
        markdown_lines.append(f"- 夜间通话总量：{int(members['night_call_count'].sum())}")
        markdown_lines.append(f"- 夜间通话平均占比：{round(float(members['night_call_ratio'].mean()), 4)}")
    else:
        markdown_lines.append("- 夜间通话分析：当前通话边文件未识别到可解析时间列，因此无法判断夜间异常型。")
    markdown_lines.append("")
    markdown_lines.append("## 四、主要群体特征识别")
    markdown_lines.append("")
    for key in ["high_call_volume", "night_abnormal", "broad_contact", "shared_device"]:
        info = pattern_summaries[key]
        markdown_lines.append(f"### {info['name']}")
        markdown_lines.append(f"- 是否触发：{'是' if info['triggered'] else '否'}")
        markdown_lines.append(f"- 命中成员数：{info['member_count']}")
        markdown_lines.append(f"- 参考阈值：{info['threshold']}")
        if info.get("non_trigger_reason"):
            markdown_lines.append(f"- 未触发原因：{info['non_trigger_reason']}")
        if info["top_members"]:
            markdown_lines.append("- 代表成员：")
            for item in info["top_members"][: min(5, len(info["top_members"]))]:
                metric_items = [(k, v) for k, v in item.items() if k not in {"phone_id", "phone_preview", "signal_count"}]
                metric_text = ", ".join([f"{k}={v}" for k, v in metric_items])
                markdown_lines.append(f"  - `{item['phone_preview']}`，{metric_text}，signal_count={item['signal_count']}")
        markdown_lines.append("")
    markdown_lines.append("## 五、群体核心成员（Top）")
    markdown_lines.append("")
    for _, row in core_members_export.head(min(args.top_k, 10)).iterrows():
        markdown_lines.append(
            f"- Rank {int(row['group_rank'])}: `{preview(row['phone_id'])}` | driver={row['driver_type']} | signal_count={int(row['signal_count'])} | core_score={round(float(row['group_core_score']), 2)} | {row['evidence_brief']}"
        )
    markdown_lines.append("")
    markdown_lines.append("## 六、号码对关系证据链")
    markdown_lines.append("")
    if not pair_relation_matrix.empty:
        for _, row in pair_relation_matrix.head(min(args.top_k, 10)).iterrows():
            markdown_lines.append(
                f"- `{preview(row['phone_a'])}` ↔ `{preview(row['phone_b'])}`：relation_types={row['relation_types']}，shared_device_count={safe_int(row['shared_device_count'])}，internal_call_records={safe_int(row['internal_call_records'])}，common_counterparty_count={safe_int(row['common_counterparty_count'])}，relation_score={safe_int(row['relation_score'])}"
            )
    else:
        markdown_lines.append("- 当前群体内部未形成明显的号码对关系证据。")
    markdown_lines.append("")
    markdown_lines.append("## 七、共享设备证据链")
    markdown_lines.append("")
    if not top_group_devices.empty:
        for _, row in top_group_devices.head(min(args.top_k, 10)).iterrows():
            markdown_lines.append(
                f"- 设备 `{preview(row['device_id'])}`：群体内挂载成员数={int(row['group_member_count'])}，设备总挂载号码数={int(row['device_phone_count'])}，风险号码数={int(row['risk_phone_count'])}，省份数={int(row['province_count'])}，标签种类数={int(row['sub_label_count'])}"
            )
    else:
        markdown_lines.append("- 当前群体内未发现有效共享设备证据。")
    markdown_lines.append("")
    markdown_lines.append("## 八、内部关系结构与子群")
    markdown_lines.append("")
    if component_summaries:
        markdown_lines.append("- 基于“共享设备 + 群体内部通话 + 共同对端”联合关系识别到以下子群：")
        for comp in component_summaries[: min(10, len(component_summaries))]:
            markdown_lines.append(f"  - 子群 {comp['component_id']}：规模={comp['size']}，成员={', '.join(comp['member_previews'])}")
    else:
        markdown_lines.append("- 当前群体未形成明显的多成员子群结构。")
    markdown_lines.append("")
    markdown_lines.append("## 九、共同对端证据")
    markdown_lines.append("")
    if not top_common_counterparties.empty:
        for _, row in top_common_counterparties.head(min(args.top_k, 10)).iterrows():
            markdown_lines.append(f"- 共同对端 `{preview(row['counterpart'])}`：被 {int(row['member_count'])} 个成员共同接触，累计通话 {int(row['total_calls'])} 次")
    else:
        markdown_lines.append("- 当前群体未发现明显的共同对端集中迹象。")
    markdown_lines.append("")
    markdown_lines.append("## 十、证据完整度与链路说明")
    markdown_lines.append("")
    for k, v in evidence_completeness.items():
        if k == "completeness_score":
            continue
        markdown_lines.append(f"- {k}: {'是' if v else '否'}")
    markdown_lines.append(f"- completeness_score: {evidence_completeness['completeness_score']}")
    markdown_lines.append(f"- confidence_score: {confidence_score}")
    markdown_lines.append("")
    markdown_lines.append("## 十一、后续建议")
    markdown_lines.append("")
    next_steps: list[str] = []
    if pattern_summaries["shared_device"]["triggered"]:
        next_steps.append("优先围绕 Top 共享设备继续做 shared-device-analysis 或设备池排查。")
    if pattern_summaries["high_call_volume"]["triggered"] or pattern_summaries["broad_contact"]["triggered"]:
        next_steps.append("围绕群体核心成员继续做 single-number-analysis，确认单点画像和局部关系圈。")
    if not pair_relation_matrix.empty:
        next_steps.append("选取关系分最高的号码对，继续做 association-path-analysis 和 overlap-analysis，确认群体内部关系强度。")
    if not next_steps:
        next_steps.append("当前群体信号较弱，建议扩大号码集合后再次进行 group-risk-analysis。")
    for step in next_steps:
        markdown_lines.append(f"- {step}")
    markdown_lines.append("- 命令模板：single-number-analysis <Top成员> / shared-device-analysis <Top设备> / association-path-analysis <Top号码对>")
    markdown_lines.append("")
    markdown_lines.append("## 十二、实现说明（基础算子组合对齐）")
    markdown_lines.append("")
    markdown_lines.append("- group_profile_summary = node_lookup + aggregation_query")
    markdown_lines.append("- high_call_volume_pattern = aggregation_query + relationship_filter")
    markdown_lines.append("- night_abnormal_pattern = relationship_filter")
    markdown_lines.append("- broad_contact_pattern = neighbor_query + aggregation_query")
    markdown_lines.append("- shared_device_pattern = query_shared_device + common_device + subgraph_by_nodes")
    markdown_lines.append("- internal_link_pattern = relationship_filter + subgraph_by_nodes")
    markdown_lines.append("- common_counterparty_pattern = common_neighbor + aggregation_query")

    report_path.write_text("\n".join(markdown_lines), encoding="utf-8")

    summary_text = (
        f"群体规模 {len(members)} 个号码。"
        f"已识别主要特征：{', '.join(major_patterns) if major_patterns else '暂未识别明确主特征'}。"
        f"共享设备号码对 {len(shared_device_pairs)} 组，群体内部关系对 {len(pair_relation_matrix)} 组。"
        f"重点信号为 {focus_signal}，群体形态判断为 {group_shape}。"
    )

    result = {
        "ok": True,
        "skill": "group-risk-analysis",
        "query_type": "group_risk_profile",
        "input_summary": {
            "group_name": args.group_name,
            "input_member_count": len(original_phone_ids),
            "group_member_count": len(members),
            "top_k": args.top_k,
            "pattern_min_members": args.pattern_min_members,
            "risk_only": args.risk_only,
            "province": args.province,
            "include_sub_labels": sorted(include_sub_labels),
            "exclude_sub_labels": sorted(exclude_sub_labels),
            "night_time_available": night_available,
            "night_time_column": time_col,
            "high_call_threshold": high_call_threshold,
            "broad_contact_threshold": broad_contact_threshold,
            "shared_peer_threshold": shared_peer_threshold,
            "dataset_root": str(dataset_root),
            "dataset": args.dataset,
            "artifact_mode": args.artifact_mode,
        },
        "filter_effect": filter_effect,
        "filter_notes": filter_notes,
        "result": {
            "summary": summary_text,
            "focus_signal": focus_signal,
            "group_shape": group_shape,
            "major_patterns": major_patterns,
            "group_statistics": {
                "group_size": len(members),
                "total_call_records": int(members["call_record_count"].sum()),
                "avg_call_records": round(float(members["call_record_count"].mean()), 2),
                "avg_counterparties": round(float(members["counterparties"].mean()), 2),
                "total_shared_devices": int(members["shared_device_count"].sum()),
                "total_shared_peer_total": int(members["shared_peer_total"].sum()),
                "internal_call_edge_count": len(internal_call_pairs),
                "internal_call_density": round(float(internal_call_density), 6),
                "shared_device_pair_count": len(shared_device_pairs),
                "common_counterparty_count": len(top_common_counterparties),
                "province_distribution": dict(province_counts),
                "label_distribution": dict(label_counts),
            },
            "pattern_summaries": pattern_summaries,
            "core_members": core_members_export.head(args.top_k).to_dict(orient="records"),
            "pair_relation_evidence": pair_relation_matrix.head(args.top_k).to_dict(orient="records"),
            "shared_device_evidence": top_group_devices.head(args.top_k).to_dict(orient="records"),
            "common_counterparty_evidence": top_common_counterparties.head(args.top_k).to_dict(orient="records"),
            "subgroups": component_summaries[: args.top_k],
            "evidence_completeness": evidence_completeness,
            "confidence_score": confidence_score,
            "next_steps": next_steps,
        },
        "base_operator_alignment": {
            "group_profile_summary": ["node_lookup", "aggregation_query"],
            "high_call_volume_pattern": ["aggregation_query", "relationship_filter"],
            "night_abnormal_pattern": ["relationship_filter"],
            "broad_contact_pattern": ["neighbor_query", "aggregation_query"],
            "shared_device_pattern": ["query_shared_device", "common_device", "subgraph_by_nodes"],
            "internal_link_pattern": ["relationship_filter", "subgraph_by_nodes"],
            "common_counterparty_pattern": ["common_neighbor", "aggregation_query"],
        },
        "report_path": str(report_path),
        "member_csv_path": str(member_csv_path),
        "pair_csv_path": str(pair_csv_path),
        "device_csv_path": str(device_csv_path),
        "counterpart_csv_path": str(counterpart_csv_path),
        "artifacts": (
            [
                {"type": "markdown", "title": report_path.name, "path": str(report_path)},
            ]
            if args.artifact_mode == "markdown_only"
            else [
                {"type": "markdown", "title": report_path.name, "path": str(report_path)},
                {"type": "csv", "title": member_csv_path.name, "path": str(member_csv_path)},
                {"type": "csv", "title": pair_csv_path.name, "path": str(pair_csv_path)},
                {"type": "csv", "title": device_csv_path.name, "path": str(device_csv_path)},
                {"type": "csv", "title": counterpart_csv_path.name, "path": str(counterpart_csv_path)},
            ]
        ),
        "notes": [
            "本技能通过基础算子组合实现群体级统计与模式归纳，不是独立底层图引擎。",
            "夜间异常型仅在通话边文件存在可解析时间列时生效。",
            "若过滤条件未改变样本，脚本会在 filter_notes 与报告中明确提示样本同质性。",
        ],
    }

    print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
