#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sichuan-shaanxi-comparison

Province-level comparison skill for unified phone-network data.  It compares
Sichuan and Shaanxi along object distribution, risk labels, call behavior,
shared-device signals, and group-structure indicators.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

SCRIPT_VERSION = "sichuan-shaanxi-comparison-release-v1.3"
DEFAULT_DATASET = "unified"
DEFAULT_PROVINCE_A = "sichuan"
DEFAULT_PROVINCE_B = "shaanxi"


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def preview_id(value: Any, n: int = 12) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    s = str(value)
    return s if len(s) <= n else s[:n] + "..."


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return default
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0, ndigits: int = 6) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return round(f, ndigits)
    except Exception:
        return default


def pct(n: Any, d: Any, ndigits: int = 4) -> float:
    n = safe_float(n, 0.0, ndigits=10)
    d = safe_float(d, 0.0, ndigits=10)
    if d == 0:
        return 0.0
    return round(n / d, ndigits)


def json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [json_safe(v) for v in obj]
    if isinstance(obj, (pd.Timestamp,)):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if pd.isna(obj) if not isinstance(obj, (list, tuple, dict)) else False:
        return None
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    return obj


def ensure_output_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def dataframe_records(df: pd.DataFrame, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    if limit is not None:
        df = df.head(limit)
    return json_safe(df.to_dict(orient="records"))


# -----------------------------------------------------------------------------
# Path and schema helpers
# -----------------------------------------------------------------------------

def find_dataset_root(user_root: Optional[str]) -> Path:
    candidates = []
    if user_root:
        candidates.append(Path(user_root))
    env_root = os.environ.get("PHONE_NETWORK_DATASET_ROOT")
    if env_root:
        candidates.append(Path(env_root))
    candidates.extend([
        Path("/workspace/imiss-deer-flow-main/datasets/phone-network"),
        Path("/mnt/datasets/phone-network"),
        Path("/mnt/data/phone-network"),
    ])
    for c in candidates:
        if c.exists():
            return c
    return candidates[0]


def resolve_paths(dataset_root: Path, dataset: str) -> Dict[str, Optional[Path]]:
    processed = dataset_root / "processed"
    user_nodes = processed / dataset / "user_nodes.csv"
    call_edges = processed / dataset / "call_edges.csv"
    device_parquet = processed / "graph_views" / dataset / "edges_phone_imei.parquet"
    device_csv = processed / "graph_views" / dataset / "edges_phone_imei.csv"
    return {
        "user_nodes": user_nodes if user_nodes.exists() else None,
        "call_edges": call_edges if call_edges.exists() else None,
        "device_edges": device_parquet if device_parquet.exists() else (device_csv if device_csv.exists() else None),
    }


def sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def get_columns_for_file(conn: duckdb.DuckDBPyConnection, path: Path) -> List[str]:
    p = sql_path(path)
    if path.suffix.lower() == ".parquet":
        query = f"DESCRIBE SELECT * FROM read_parquet('{p}')"
    else:
        query = f"DESCRIBE SELECT * FROM read_csv_auto('{p}', ignore_errors=true)"
    return [str(x).lower() for x in conn.execute(query).fetchdf()["column_name"].tolist()]


def choose_col(columns: List[str], candidates: List[str]) -> Optional[str]:
    lower_to_orig = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower_to_orig:
            return lower_to_orig[cand.lower()]
    return None


def read_relation_expr(path: Path) -> str:
    p = sql_path(path)
    if path.suffix.lower() == ".parquet":
        return f"read_parquet('{p}')"
    return f"read_csv_auto('{p}', ignore_errors=true)"


def setup_views(
    conn: duckdb.DuckDBPyConnection,
    paths: Dict[str, Optional[Path]],
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {"missing": [], "warnings": [], "columns": {}}
    if not paths["user_nodes"]:
        meta["missing"].append("user_nodes")
        return meta
    if not paths["call_edges"]:
        meta["missing"].append("call_edges")
    if not paths["device_edges"]:
        meta["warnings"].append("device_edges missing; shared-device comparison unavailable")

    # User nodes
    node_cols = get_columns_for_file(conn, paths["user_nodes"])
    user_col = choose_col(node_cols, ["user_id", "phone_id", "phone", "src_user_id"])
    province_col = choose_col(node_cols, ["province", "prov", "area", "region"])
    label_col = choose_col(node_cols, ["label", "risk_label", "is_risk"])
    sub_label_col = choose_col(node_cols, ["sub_label", "sublabel", "type", "category"])
    meta["columns"]["nodes"] = {
        "user_col": user_col,
        "province_col": province_col,
        "label_col": label_col,
        "sub_label_col": sub_label_col,
    }
    if not user_col:
        meta["missing"].append("node_id_column")
        return meta
    if not province_col:
        meta["warnings"].append("province column missing; comparison cannot be province-specific")

    n_path = sql_path(paths["user_nodes"])
    province_expr = f"LOWER(COALESCE(CAST({province_col} AS VARCHAR), 'unknown'))" if province_col else "'unknown'"
    label_expr = f"CAST(COALESCE(CAST({label_col} AS VARCHAR), 'unknown') AS VARCHAR)" if label_col else "'unknown'"
    sub_label_expr = f"LOWER(COALESCE(CAST({sub_label_col} AS VARCHAR), 'unknown'))" if sub_label_col else "'unknown'"
    conn.execute(f"""
        CREATE OR REPLACE VIEW nodes AS
        SELECT
            CAST({user_col} AS VARCHAR) AS user_id,
            {province_expr} AS province,
            {label_expr} AS label,
            {sub_label_expr} AS sub_label
        FROM read_csv_auto('{n_path}', ignore_errors=true)
        WHERE {user_col} IS NOT NULL
    """)

    # Calls
    if paths["call_edges"]:
        call_cols = get_columns_for_file(conn, paths["call_edges"])
        src_col = choose_col(call_cols, ["src_user_id", "source", "src", "caller", "user_id", "phone_id"])
        dst_col = choose_col(call_cols, ["dst_counterparty_id", "target", "dst", "callee", "counterparty_id", "peer_id"])
        time_col = choose_col(call_cols, ["event_time", "call_time", "start_time", "timestamp", "time"])
        hour_col = choose_col(call_cols, ["event_hour", "hour", "call_hour"])
        duration_col = choose_col(call_cols, ["duration", "call_duration", "duration_sec", "call_duration_sec"])
        weight_col = choose_col(call_cols, ["call_count", "weight", "record_count", "cnt"])
        meta["columns"]["calls"] = {
            "src_col": src_col,
            "dst_col": dst_col,
            "time_col": time_col,
            "hour_col": hour_col,
            "duration_col": duration_col,
            "weight_col": weight_col,
        }
        if not src_col or not dst_col:
            meta["missing"].append("call_source_target_columns")
        else:
            if time_col and hour_col:
                hour_expr = f"COALESCE(EXTRACT('hour' FROM TRY_CAST({time_col} AS TIMESTAMP)), TRY_CAST({hour_col} AS DOUBLE))"
            elif time_col:
                hour_expr = f"EXTRACT('hour' FROM TRY_CAST({time_col} AS TIMESTAMP))"
            elif hour_col:
                hour_expr = f"TRY_CAST({hour_col} AS DOUBLE)"
            else:
                hour_expr = "NULL"
                meta["warnings"].append("no time/hour column; night behavior comparison unavailable")
            event_ts_expr = f"TRY_CAST({time_col} AS TIMESTAMP)" if time_col else "NULL"
            duration_expr = f"TRY_CAST({duration_col} AS DOUBLE)" if duration_col else "0.0"
            weight_expr = f"TRY_CAST({weight_col} AS DOUBLE)" if weight_col else "1.0"
            c_path = sql_path(paths["call_edges"])
            conn.execute(f"""
                CREATE OR REPLACE VIEW calls_raw AS
                SELECT
                    CAST({src_col} AS VARCHAR) AS src_user_id,
                    CAST({dst_col} AS VARCHAR) AS dst_counterparty_id,
                    {event_ts_expr} AS event_ts,
                    CAST({hour_expr} AS DOUBLE) AS hour_value,
                    {duration_expr} AS duration_value,
                    COALESCE({weight_expr}, 1.0) AS weight_value
                FROM read_csv_auto('{c_path}', ignore_errors=true)
                WHERE {src_col} IS NOT NULL AND {dst_col} IS NOT NULL
            """)
            conn.execute("""
                CREATE OR REPLACE VIEW calls_enriched AS
                SELECT
                    c.*,
                    n.province AS province,
                    n.label AS label,
                    n.sub_label AS sub_label,
                    CASE
                        WHEN c.hour_value >= 22 OR c.hour_value < 6 THEN 1
                        ELSE 0
                    END AS is_night,
                    CAST(c.event_ts AS DATE) AS event_date
                FROM calls_raw c
                LEFT JOIN nodes n ON c.src_user_id = n.user_id
            """)

    # Device edges
    if paths["device_edges"]:
        dev_cols = get_columns_for_file(conn, paths["device_edges"])
        phone_col = choose_col(dev_cols, ["user_id", "phone_id", "phone", "src_user_id"])
        device_col = choose_col(dev_cols, ["imei", "device_id", "device", "terminal_id"])
        meta["columns"]["devices"] = {"phone_col": phone_col, "device_col": device_col}
        if not phone_col or not device_col:
            meta["warnings"].append("device phone/device columns missing")
        else:
            expr = read_relation_expr(paths["device_edges"])
            conn.execute(f"""
                CREATE OR REPLACE VIEW devices_raw AS
                SELECT DISTINCT
                    CAST({phone_col} AS VARCHAR) AS user_id,
                    CAST({device_col} AS VARCHAR) AS imei
                FROM {expr}
                WHERE {phone_col} IS NOT NULL AND {device_col} IS NOT NULL
            """)
            conn.execute("""
                CREATE OR REPLACE VIEW devices_enriched AS
                SELECT
                    d.user_id,
                    d.imei,
                    n.province AS province,
                    n.label AS label,
                    n.sub_label AS sub_label
                FROM devices_raw d
                LEFT JOIN nodes n ON d.user_id = n.user_id
            """)
            conn.execute("""
                CREATE OR REPLACE VIEW device_global_stats AS
                SELECT
                    imei,
                    COUNT(DISTINCT user_id) AS global_phone_count,
                    COUNT(DISTINCT CASE WHEN label = '1' THEN user_id END) AS global_risk_phone_count,
                    COUNT(DISTINCT province) AS province_count
                FROM devices_enriched
                GROUP BY imei
            """)
    return meta


# -----------------------------------------------------------------------------
# Analysis queries
# -----------------------------------------------------------------------------

def get_node_summary(conn: duckdb.DuckDBPyConnection, provinces: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    province_list = ",".join([f"'{p}'" for p in provinces])
    overview = conn.execute(f"""
        SELECT
            province,
            COUNT(*) AS node_rows,
            COUNT(DISTINCT user_id) AS distinct_users,
            COUNT(*) - COUNT(DISTINCT user_id) AS duplicate_rows,
            COUNT(DISTINCT CASE WHEN label = '1' THEN user_id END) AS risk_users,
            COUNT(DISTINCT CASE WHEN label != '1' OR label IS NULL THEN user_id END) AS non_risk_users,
            ROUND(COUNT(DISTINCT CASE WHEN label = '1' THEN user_id END) * 1.0 / NULLIF(COUNT(DISTINCT user_id), 0), 6) AS risk_ratio
        FROM nodes
        WHERE province IN ({province_list})
        GROUP BY province
        ORDER BY province
    """).fetchdf()
    sub_label = conn.execute(f"""
        SELECT
            province,
            sub_label,
            COUNT(DISTINCT user_id) AS user_count,
            ROUND(COUNT(DISTINCT user_id) * 100.0 / NULLIF(SUM(COUNT(DISTINCT user_id)) OVER (PARTITION BY province), 0), 4) AS pct_in_province
        FROM nodes
        WHERE province IN ({province_list})
        GROUP BY province, sub_label
        ORDER BY province, user_count DESC
    """).fetchdf()
    label = conn.execute(f"""
        SELECT
            province,
            label,
            COUNT(DISTINCT user_id) AS user_count,
            ROUND(COUNT(DISTINCT user_id) * 100.0 / NULLIF(SUM(COUNT(DISTINCT user_id)) OVER (PARTITION BY province), 0), 4) AS pct_in_province
        FROM nodes
        WHERE province IN ({province_list})
        GROUP BY province, label
        ORDER BY province, label
    """).fetchdf()
    return overview, label, sub_label


def get_call_behavior(conn: duckdb.DuckDBPyConnection, provinces: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    province_list = ",".join([f"'{p}'" for p in provinces])
    exists = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='calls_enriched'").fetchone()[0]
    if not exists:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    behavior = conn.execute(f"""
        SELECT
            province,
            COUNT(*) AS call_record_count,
            COUNT(DISTINCT src_user_id) AS active_source_count,
            COUNT(DISTINCT dst_counterparty_id) AS unique_counterparty_count,
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT src_user_id), 0), 4) AS avg_records_per_active_source,
            ROUND(COUNT(DISTINCT dst_counterparty_id) * 1.0 / NULLIF(COUNT(DISTINCT src_user_id), 0), 4) AS avg_counterparties_per_active_source,
            SUM(is_night) AS night_record_count,
            ROUND(SUM(is_night) * 1.0 / NULLIF(COUNT(*), 0), 6) AS night_ratio,
            MIN(event_ts) AS time_min,
            MAX(event_ts) AS time_max,
            COUNT(DISTINCT event_date) AS active_days,
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT event_date), 0), 4) AS avg_records_per_active_day
        FROM calls_enriched
        WHERE province IN ({province_list})
        GROUP BY province
        ORDER BY province
    """).fetchdf()
    if behavior is not None and not behavior.empty:
        # Calendar-span and normalized indicators make cross-province comparison safer when
        # the two province datasets cover different time ranges.
        tmin = pd.to_datetime(behavior["time_min"], errors="coerce")
        tmax = pd.to_datetime(behavior["time_max"], errors="coerce")
        behavior["calendar_span_days"] = (tmax.dt.normalize() - tmin.dt.normalize()).dt.days + 1
        behavior["calendar_span_days"] = behavior["calendar_span_days"].fillna(0).astype(int)
        behavior["records_per_calendar_day"] = behavior.apply(
            lambda r: safe_float(r.get("call_record_count", 0) / r.get("calendar_span_days", 0), 0, 4)
            if safe_float(r.get("calendar_span_days", 0), 0) > 0 else 0.0,
            axis=1,
        )
        behavior["records_per_active_source_per_active_day"] = behavior.apply(
            lambda r: safe_float(r.get("avg_records_per_active_source", 0) / r.get("active_days", 0), 0, 6)
            if safe_float(r.get("active_days", 0), 0) > 0 else 0.0,
            axis=1,
        )
        behavior["counterparties_per_active_source_per_active_day"] = behavior.apply(
            lambda r: safe_float(r.get("avg_counterparties_per_active_source", 0) / r.get("active_days", 0), 0, 6)
            if safe_float(r.get("active_days", 0), 0) > 0 else 0.0,
            axis=1,
        )

    phone_call_stats = conn.execute(f"""
        SELECT
            c.province,
            c.src_user_id AS user_id,
            MAX(c.label) AS label,
            MAX(c.sub_label) AS sub_label,
            COUNT(*) AS record_count,
            COUNT(DISTINCT c.dst_counterparty_id) AS counterparty_count,
            SUM(c.is_night) AS night_record_count,
            ROUND(SUM(c.is_night) * 1.0 / NULLIF(COUNT(*), 0), 6) AS night_ratio
        FROM calls_enriched c
        WHERE c.province IN ({province_list})
        GROUP BY c.province, c.src_user_id
    """).fetchdf()

    hourly = conn.execute(f"""
        SELECT
            province,
            CAST(hour_value AS INTEGER) AS hour,
            COUNT(*) AS record_count,
            ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY province), 0), 4) AS pct_in_province
        FROM calls_enriched
        WHERE province IN ({province_list}) AND hour_value IS NOT NULL
        GROUP BY province, CAST(hour_value AS INTEGER)
        ORDER BY province, hour
    """).fetchdf()

    top_callers = conn.execute(f"""
        SELECT * FROM (
            SELECT
                c.province,
                c.src_user_id AS user_id,
                SUBSTRING(c.src_user_id, 1, 12) || '...' AS user_preview,
                MAX(c.label) AS label,
                MAX(c.sub_label) AS sub_label,
                COUNT(*) AS record_count,
                COUNT(DISTINCT c.dst_counterparty_id) AS counterparty_count,
                ROUND(SUM(c.is_night) * 1.0 / NULLIF(COUNT(*), 0), 6) AS night_ratio,
                ROW_NUMBER() OVER (PARTITION BY c.province ORDER BY COUNT(*) DESC, COUNT(DISTINCT c.dst_counterparty_id) DESC) AS rank
            FROM calls_enriched c
            WHERE c.province IN ({province_list})
            GROUP BY c.province, c.src_user_id
        ) t WHERE rank <= 20
        ORDER BY province, rank
    """).fetchdf()
    return behavior, phone_call_stats, hourly, top_callers


def get_device_summary(conn: duckdb.DuckDBPyConnection, provinces: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    province_list = ",".join([f"'{p}'" for p in provinces])
    exists = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='devices_enriched'").fetchone()[0]
    if not exists:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW province_device_stats AS
        SELECT
            d.province,
            d.imei,
            COUNT(DISTINCT d.user_id) AS province_phone_count,
            COUNT(DISTINCT CASE WHEN d.label = '1' THEN d.user_id END) AS province_risk_phone_count,
            MAX(g.global_phone_count) AS global_phone_count,
            MAX(g.global_risk_phone_count) AS global_risk_phone_count,
            MAX(g.province_count) AS province_count
        FROM devices_enriched d
        LEFT JOIN device_global_stats g ON d.imei = g.imei
        WHERE d.province IN ({province_list})
        GROUP BY d.province, d.imei
    """)
    device_summary = conn.execute("""
        SELECT
            province,
            COUNT(*) AS device_count,
            SUM(province_phone_count) AS device_edge_count,
            COUNT(CASE WHEN province_phone_count >= 2 THEN 1 END) AS shared_device_count_within_province,
            COUNT(CASE WHEN province_phone_count >= 5 THEN 1 END) AS device_5plus_within_province,
            COUNT(CASE WHEN province_phone_count >= 10 THEN 1 END) AS device_10plus_within_province,
            COUNT(CASE WHEN province_count >= 2 THEN 1 END) AS cross_province_device_count,
            ROUND(COUNT(CASE WHEN province_phone_count >= 2 THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0), 6) AS shared_device_ratio,
            ROUND(AVG(province_phone_count), 4) AS avg_phones_per_device,
            ROUND(AVG(province_risk_phone_count * 1.0 / NULLIF(province_phone_count, 0)), 6) AS avg_device_risk_ratio
        FROM province_device_stats
        GROUP BY province
        ORDER BY province
    """).fetchdf()
    top_devices = conn.execute("""
        SELECT * FROM (
            SELECT
                province,
                imei,
                SUBSTRING(imei, 1, 12) || '...' AS imei_preview,
                province_phone_count,
                province_risk_phone_count,
                global_phone_count,
                global_risk_phone_count,
                province_count,
                ROUND(province_risk_phone_count * 1.0 / NULLIF(province_phone_count, 0), 6) AS province_risk_ratio,
                ROW_NUMBER() OVER (PARTITION BY province ORDER BY province_phone_count DESC, province_risk_phone_count DESC, imei) AS rank
            FROM province_device_stats
        ) t WHERE rank <= 20
        ORDER BY province, rank
    """).fetchdf()
    phone_device_stats = conn.execute(f"""
        SELECT
            d.province,
            d.user_id,
            COUNT(DISTINCT d.imei) AS device_count,
            COUNT(DISTINCT CASE WHEN g.global_phone_count >= 2 THEN d.imei END) AS shared_device_count,
            COUNT(DISTINCT CASE WHEN g.global_phone_count >= 5 THEN d.imei END) AS high_shared_device_count
        FROM devices_enriched d
        LEFT JOIN device_global_stats g ON d.imei = g.imei
        WHERE d.province IN ({province_list})
        GROUP BY d.province, d.user_id
    """).fetchdf()
    cross_device_examples = conn.execute("""
        SELECT
            imei,
            SUBSTRING(imei, 1, 12) || '...' AS imei_preview,
            MAX(global_phone_count) AS global_phone_count,
            MAX(global_risk_phone_count) AS global_risk_phone_count,
            COUNT(DISTINCT province) AS province_count,
            SUM(CASE WHEN province = 'sichuan' THEN province_phone_count ELSE 0 END) AS sichuan_phone_count,
            SUM(CASE WHEN province = 'shaanxi' THEN province_phone_count ELSE 0 END) AS shaanxi_phone_count
        FROM province_device_stats
        GROUP BY imei
        HAVING COUNT(DISTINCT province) >= 2
        ORDER BY global_phone_count DESC, global_risk_phone_count DESC
        LIMIT 20
    """).fetchdf()
    return device_summary, top_devices, phone_device_stats, cross_device_examples


def build_structure_metrics(
    node_overview: pd.DataFrame,
    phone_call_stats: pd.DataFrame,
    phone_device_stats: pd.DataFrame,
    provinces: List[str],
) -> pd.DataFrame:
    if node_overview is None:
        node_overview = pd.DataFrame()
    call_df = phone_call_stats.copy() if phone_call_stats is not None and not phone_call_stats.empty else pd.DataFrame()
    dev_df = phone_device_stats.copy() if phone_device_stats is not None and not phone_device_stats.empty else pd.DataFrame()
    rows = []

    call_q90 = safe_float(call_df["record_count"].quantile(0.9), 0) if not call_df.empty and "record_count" in call_df else 0
    cp_q90 = safe_float(call_df["counterparty_count"].quantile(0.9), 0) if not call_df.empty and "counterparty_count" in call_df else 0

    for province in provinces:
        base = node_overview[node_overview["province"] == province] if not node_overview.empty else pd.DataFrame()
        c = call_df[call_df["province"] == province] if not call_df.empty else pd.DataFrame()
        d = dev_df[dev_df["province"] == province] if not dev_df.empty else pd.DataFrame()
        distinct_users = safe_int(base["distinct_users"].iloc[0]) if not base.empty else 0
        risk_users = safe_int(base["risk_users"].iloc[0]) if not base.empty else 0
        active_source_count = len(c) if not c.empty else 0
        users_with_shared_device = int((d["shared_device_count"] > 0).sum()) if not d.empty and "shared_device_count" in d else 0
        users_with_high_shared_device = int((d["high_shared_device_count"] > 0).sum()) if not d.empty and "high_shared_device_count" in d else 0
        high_call_volume_users = int((c["record_count"] >= call_q90).sum()) if not c.empty and call_q90 > 0 else 0
        high_breadth_users = int((c["counterparty_count"] >= cp_q90).sum()) if not c.empty and cp_q90 > 0 else 0
        rows.append({
            "province": province,
            "distinct_users": distinct_users,
            "risk_users": risk_users,
            "risk_ratio": pct(risk_users, distinct_users),
            "active_source_count": active_source_count,
            "active_source_ratio": pct(active_source_count, distinct_users),
            "avg_call_records_per_active_user": safe_float(c["record_count"].mean(), 0, 4) if not c.empty else 0.0,
            "median_call_records_per_active_user": safe_float(c["record_count"].median(), 0, 4) if not c.empty else 0.0,
            "p90_call_records_global_threshold": call_q90,
            "high_call_volume_user_count": high_call_volume_users,
            "high_call_volume_ratio": pct(high_call_volume_users, distinct_users),
            "avg_counterparties_per_active_user": safe_float(c["counterparty_count"].mean(), 0, 4) if not c.empty else 0.0,
            "median_counterparties_per_active_user": safe_float(c["counterparty_count"].median(), 0, 4) if not c.empty else 0.0,
            "p90_counterparty_global_threshold": cp_q90,
            "high_breadth_user_count": high_breadth_users,
            "high_breadth_ratio": pct(high_breadth_users, distinct_users),
            "avg_night_ratio_per_active_user": safe_float(c["night_ratio"].mean(), 0, 6) if not c.empty else 0.0,
            "users_with_shared_device": users_with_shared_device,
            "shared_device_user_ratio": pct(users_with_shared_device, distinct_users),
            "users_with_high_shared_device": users_with_high_shared_device,
            "high_shared_device_user_ratio": pct(users_with_high_shared_device, distinct_users),
        })
    return pd.DataFrame(rows)


def add_ratio_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if out[c].dtype.kind in "biufc":
            out[c] = out[c].apply(lambda x: safe_float(x, x if not pd.isna(x) else 0, 6))
    return out


def build_metric_contrast(
    node_overview: pd.DataFrame,
    sub_label: pd.DataFrame,
    call_behavior: pd.DataFrame,
    device_summary: pd.DataFrame,
    structure: pd.DataFrame,
    province_a: str,
    province_b: str,
) -> pd.DataFrame:
    def get(df: pd.DataFrame, province: str, col: str, default=0):
        if df is None or df.empty or col not in df.columns:
            return default
        row = df[df["province"] == province]
        if row.empty:
            return default
        return row[col].iloc[0]

    def get_sub(province: str, label: str) -> float:
        if sub_label is None or sub_label.empty:
            return 0.0
        row = sub_label[(sub_label["province"] == province) & (sub_label["sub_label"] == label)]
        if row.empty:
            return 0.0
        return safe_float(row["pct_in_province"].iloc[0] / 100.0, 0, 6)

    specs = [
        ("风险对象占比", node_overview, "risk_ratio", "风险对象分布"),
        ("risk标签占比", None, "risk_sub", "风险对象分布"),
        ("purefraud标签占比", None, "purefraud_sub", "风险对象分布"),
        ("mutation标签占比", None, "mutation_sub", "风险对象分布"),
        ("时间覆盖天数", call_behavior, "calendar_span_days", "数据口径"),
        ("通话记录总量", call_behavior, "call_record_count", "行为模式"),
        ("按活跃日平均通话记录", call_behavior, "avg_records_per_active_day", "行为模式"),
        ("平均每活跃号码通话记录", call_behavior, "avg_records_per_active_source", "行为模式"),
        ("每活跃号码日均通话记录", call_behavior, "records_per_active_source_per_active_day", "行为模式"),
        ("平均每活跃号码联系人规模", call_behavior, "avg_counterparties_per_active_source", "行为模式"),
        ("每活跃号码日均联系人规模", call_behavior, "counterparties_per_active_source_per_active_day", "行为模式"),
        ("夜间通话占比", call_behavior, "night_ratio", "行为模式"),
        ("共享设备数量", device_summary, "shared_device_count_within_province", "共享设备"),
        ("5+挂载设备数量", device_summary, "device_5plus_within_province", "共享设备"),
        ("10+挂载设备数量", device_summary, "device_10plus_within_province", "共享设备"),
        ("共享设备用户占比", structure, "shared_device_user_ratio", "群体结构"),
        ("高通话量用户占比", structure, "high_call_volume_ratio", "群体结构"),
        ("联系人广度异常用户占比", structure, "high_breadth_ratio", "群体结构"),
    ]
    rows = []
    for metric, df, col, group in specs:
        if col.endswith("_sub"):
            label = col.replace("_sub", "")
            a = get_sub(province_a, label)
            b = get_sub(province_b, label)
        else:
            a = safe_float(get(df, province_a, col, 0), 0, 6)
            b = safe_float(get(df, province_b, col, 0), 0, 6)
        diff = safe_float(a - b, 0, 6)
        if abs(diff) < 1e-9:
            higher = "tie"
        else:
            higher = province_a if diff > 0 else province_b
        rows.append({
            "feature_group": group,
            "metric": metric,
            province_a: a,
            province_b: b,
            "difference_a_minus_b": diff,
            "higher_side": higher,
            "interpretation": interpret_metric(metric, higher, province_a, province_b, diff),
        })
    return pd.DataFrame(rows)


def interpret_metric(metric: str, higher: str, province_a: str, province_b: str, diff: float) -> str:
    if higher == "tie":
        return "两地差异很小，可作为背景特征，不作为主要差异证据。"
    side = "四川" if higher == "sichuan" else "陕西" if higher == "shaanxi" else higher
    if "风险" in metric or "purefraud" in metric or "mutation" in metric:
        return f"{side}在该风险标签/风险比例上更高，说明该地区风险对象占比更突出。"
    if "时间覆盖" in metric:
        return f"{side}时间覆盖更长；该项只说明数据口径差异，不直接代表风险更高。"
    if "夜间" in metric:
        return f"{side}夜间行为占比更高，适合后续用 time-series-anomaly-analysis 或 condition-based-screening 下钻。"
    if "共享设备" in metric or "挂载" in metric:
        return f"{side}共享设备或设备池信号更强，适合后续用 shared-device-analysis 和 gang-cluster-analysis 下钻。"
    if "联系人" in metric:
        return f"{side}联系人广度更突出，可能存在更强的外联扩散或公共对端特征。"
    if "日均" in metric:
        return f"{side}按时间归一化后的行为强度更高，比原始总量更适合跨省比较。"
    if "通话" in metric:
        return f"{side}通话活跃度更高，但若两地时间覆盖不同，需结合日均和人均指标解释。"
    return f"{side}在该指标上更高。"


def select_top_objects(
    phone_call_stats: pd.DataFrame,
    phone_device_stats: pd.DataFrame,
    provinces: List[str],
    top_k: int,
) -> pd.DataFrame:
    if phone_call_stats is None or phone_call_stats.empty:
        return pd.DataFrame()
    df = phone_call_stats.copy()
    if phone_device_stats is not None and not phone_device_stats.empty:
        df = df.merge(phone_device_stats[["user_id", "device_count", "shared_device_count", "high_shared_device_count"]], on="user_id", how="left")
    for col in ["device_count", "shared_device_count", "high_shared_device_count"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].fillna(0)
    # Score is only for prioritizing examples, not a legal/definitive risk conclusion.
    df["risk_flag_score"] = df["label"].astype(str).eq("1").astype(int) * 30
    df["call_score"] = df["record_count"].rank(pct=True) * 25
    df["breadth_score"] = df["counterparty_count"].rank(pct=True) * 25
    df["device_score"] = df["shared_device_count"].rank(pct=True) * 15
    df["night_score"] = df["night_ratio"].fillna(0).rank(pct=True) * 5
    df["comparison_signal_score"] = (df["risk_flag_score"] + df["call_score"] + df["breadth_score"] + df["device_score"] + df["night_score"]).round(4)
    rows = []
    for p in provinces:
        part = df[df["province"] == p].copy()
        if part.empty:
            continue
        part = part.sort_values(["comparison_signal_score", "record_count", "counterparty_count"], ascending=False).head(top_k)
        part["rank_in_province"] = range(1, len(part) + 1)
        part["user_preview"] = part["user_id"].apply(preview_id)
        part["reason"] = part.apply(lambda r: build_top_object_reason(r), axis=1)
        rows.append(part)
    if not rows:
        return pd.DataFrame()
    result = pd.concat(rows, ignore_index=True)
    keep = [
        "province", "rank_in_province", "user_id", "user_preview", "label", "sub_label",
        "record_count", "counterparty_count", "night_ratio", "device_count", "shared_device_count",
        "high_shared_device_count", "comparison_signal_score", "reason",
    ]
    return result[[c for c in keep if c in result.columns]]


def build_top_object_reason(row: pd.Series) -> str:
    reasons = []
    if str(row.get("label", "")) == "1":
        reasons.append("风险标签")
    if safe_int(row.get("record_count")) > 0:
        reasons.append(f"通话{safe_int(row.get('record_count'))}条")
    if safe_int(row.get("counterparty_count")) > 0:
        reasons.append(f"联系人{safe_int(row.get('counterparty_count'))}个")
    if safe_int(row.get("shared_device_count")) > 0:
        reasons.append(f"共享设备{safe_int(row.get('shared_device_count'))}台")
    return "；".join(reasons[:4]) if reasons else "缺少明显信号"


# -----------------------------------------------------------------------------
# Report generation
# -----------------------------------------------------------------------------

def build_time_comparability_note(call_behavior: pd.DataFrame, province_a: str, province_b: str) -> str:
    if call_behavior is None or call_behavior.empty or "calendar_span_days" not in call_behavior.columns:
        return "未识别到完整时间覆盖信息，时间维度仅作辅助参考。"
    a = get_row(call_behavior, province_a)
    b = get_row(call_behavior, province_b)
    a_span = safe_int(a.get("calendar_span_days"))
    b_span = safe_int(b.get("calendar_span_days"))
    a_min = str(a.get("time_min", ""))[:10]
    a_max = str(a.get("time_max", ""))[:10]
    b_min = str(b.get("time_min", ""))[:10]
    b_max = str(b.get("time_max", ""))[:10]
    if a_span <= 0 or b_span <= 0:
        return "未识别到完整时间覆盖信息，时间维度仅作辅助参考。"
    ratio = max(a_span, b_span) / max(min(a_span, b_span), 1)
    if ratio >= 2:
        return (
            f"两省数据时间覆盖不一致：{province_zh(province_a)}约 {a_span} 天({a_min}至{a_max})，"
            f"{province_zh(province_b)}约 {b_span} 天({b_min}至{b_max})。"
            "因此原始通话总量只能作为规模背景，跨省行为差异应优先看人均、日均和比例类指标。"
        )
    return (
        f"两省时间覆盖相对可比：{province_zh(province_a)}约 {a_span} 天，"
        f"{province_zh(province_b)}约 {b_span} 天。"
    )


def province_zh(p: str) -> str:
    mapping = {"sichuan": "四川", "shaanxi": "陕西"}
    return mapping.get(p, p)


def get_row(df: pd.DataFrame, province: str) -> Dict[str, Any]:
    if df is None or df.empty:
        return {}
    row = df[df["province"] == province]
    if row.empty:
        return {}
    return row.iloc[0].to_dict()


def md_table(df: pd.DataFrame, cols: Optional[List[str]] = None, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "暂无数据。\n"
    out = df.copy()
    if cols:
        out = out[[c for c in cols if c in out.columns]]
    out = out.head(max_rows)
    return out.to_markdown(index=False) + "\n"


def build_key_findings(metric_contrast: pd.DataFrame, provinces: List[str]) -> List[str]:
    findings = []
    if metric_contrast is None or metric_contrast.empty:
        return ["未形成可用差异指标。"]
    # Avoid letting raw totals dominate the conclusions when time ranges differ.
    # Prefer ratios, normalized behavioral metrics, and structural indicators.
    tmp = metric_contrast.copy()
    raw_background_metrics = {"通话记录总量", "时间覆盖天数"}
    tmp = tmp[~tmp["metric"].isin(raw_background_metrics)].copy()
    if tmp.empty:
        return ["两地核心指标差异不大，建议结合具体对象进一步下钻。"]
    def rel_gap(row):
        vals = []
        for c in provinces:
            vals.append(abs(safe_float(row.get(c), 0, 10)))
        scale = max(vals + [1.0])
        return abs(safe_float(row.get("difference_a_minus_b"), 0, 10)) / scale
    tmp["relative_gap"] = tmp.apply(rel_gap, axis=1)
    for _, row in tmp.sort_values(["relative_gap", "feature_group"], ascending=[False, True]).head(6).iterrows():
        higher = row.get("higher_side")
        metric = row.get("metric")
        if higher == "tie":
            continue
        side = province_zh(str(higher))
        findings.append(f"{side}在 `{metric}` 上更突出：{row.get('interpretation')}")
    return findings[:5] or ["两地核心指标差异不大，建议结合具体对象进一步下钻。"]


def write_reports(
    output_dir: Path,
    prefix: str,
    dataset: str,
    province_a: str,
    province_b: str,
    summary: Dict[str, Any],
    dfs: Dict[str, pd.DataFrame],
) -> Tuple[Path, Path]:
    report_path = output_dir / f"{prefix}.md"
    presentation_path = output_dir / f"{prefix}_presentation.md"

    a = province_a
    b = province_b
    a_node = get_row(dfs["node_overview"], a)
    b_node = get_row(dfs["node_overview"], b)
    a_call = get_row(dfs["call_behavior"], a)
    b_call = get_row(dfs["call_behavior"], b)
    a_dev = get_row(dfs["device_summary"], a)
    b_dev = get_row(dfs["device_summary"], b)
    findings = summary.get("key_findings", [])
    time_note = summary.get("time_comparability_note", "")

    report = []
    report.append(f"# 四川-陕西电话网络地域对比分析报告：{dataset}\n")
    report.append("## 一、核心结论\n")
    report.append("- 分析口径：**全量地域对比**，基于 unified 数据中省份字段对四川与陕西进行整体比较。\n")
    report.append("- 不是条件筛选结果：本报告不是 `condition-based-screening` 的条件切片，也不只代表某个筛选子集。\n")
    report.append(f"- 对比对象：`{province_zh(a)} ({a})` vs `{province_zh(b)} ({b})`。\n")
    for item in findings:
        report.append(f"- {item}\n")
    report.append("- 本报告用于地域差异研判和后续分析入口选择，不代表最终定性结论。\n")
    report.append("\n## 二、口径说明与时间可比性提醒\n")
    report.append(f"- 时间可比性提醒：{time_note}\n")
    report.append("- 原始总量类指标适合看数据规模背景；跨省行为强弱更建议看比例、人均、日均和每活跃号码日均指标。\n")
    report.append("- 如需比较某类特定对象，例如‘联系人广度高且共享设备数高’的目标，应另行使用 `condition-based-screening` 输出条件切片结果。\n")

    report.append("\n## 三、对象规模与风险分布对比\n")
    report.append(md_table(dfs["node_overview"], ["province", "distinct_users", "risk_users", "risk_ratio", "duplicate_rows"], 10))
    report.append("\n### sub_label 分布\n")
    report.append(md_table(dfs["sub_label_distribution"], ["province", "sub_label", "user_count", "pct_in_province"], 20))

    report.append("\n## 四、通话行为模式对比\n")
    report.append(md_table(dfs["call_behavior"], ["province", "call_record_count", "active_source_count", "unique_counterparty_count", "avg_records_per_active_source", "avg_counterparties_per_active_source", "night_record_count", "night_ratio", "active_days"], 10))
    report.append("\n### 归一化行为强度对比\n")
    report.append(md_table(dfs["call_behavior"], ["province", "calendar_span_days", "records_per_calendar_day", "records_per_active_source_per_active_day", "counterparties_per_active_source_per_active_day"], 10))
    report.append("\n### 小时分布说明\n")
    report.append("小时分布明细已输出到 `hourly_distribution.csv`，可用于继续查看高峰时段与夜间行为差异。\n")

    report.append("\n## 五、共享设备与群体结构对比\n")
    report.append(md_table(dfs["device_summary"], ["province", "device_count", "device_edge_count", "shared_device_count_within_province", "device_5plus_within_province", "device_10plus_within_province", "cross_province_device_count", "shared_device_ratio", "avg_device_risk_ratio"], 10))
    report.append("\n### 群体结构指标\n")
    report.append(md_table(dfs["structure_metrics"], ["province", "active_source_ratio", "avg_call_records_per_active_user", "high_call_volume_ratio", "avg_counterparties_per_active_user", "high_breadth_ratio", "shared_device_user_ratio", "high_shared_device_user_ratio"], 10))

    report.append("\n## 六、关键差异指标\n")
    report.append(md_table(dfs["metric_contrast"], ["feature_group", "metric", a, b, "difference_a_minus_b", "higher_side", "interpretation"], 30))

    report.append("\n## 七、代表性对象与设备证据\n")
    report.append("### Top 对象样例\n")
    report.append(md_table(dfs["top_objects"], ["province", "rank_in_province", "user_preview", "label", "sub_label", "record_count", "counterparty_count", "shared_device_count", "comparison_signal_score", "reason"], 20))
    report.append("\n### Top 共享设备样例\n")
    report.append(md_table(dfs["top_shared_devices"], ["province", "rank", "imei_preview", "province_phone_count", "province_risk_phone_count", "global_phone_count", "province_count", "province_risk_ratio"], 20))
    if dfs.get("cross_device_examples") is not None and not dfs["cross_device_examples"].empty:
        report.append("\n### 跨省共享设备样例（用于后续 cross-province-linkage-analysis 下钻）\n")
        report.append(md_table(dfs["cross_device_examples"], ["imei_preview", "global_phone_count", "global_risk_phone_count", "sichuan_phone_count", "shaanxi_phone_count"], 20))

    report.append("\n## 八、建议的后续分析\n")
    report.append("- 若要继续寻找跨省共享设备、跨省共同对端和跨省强关联对象，建议使用 `cross-province-linkage-analysis`。\n")
    report.append("- 若要对某个重点号码生成解释型证据，建议使用 `risk-evidence-pack` 或 `single-number-analysis`。\n")
    report.append("- 若要分析设备池或疑似团伙，建议使用 `shared-device-analysis`、`group-risk-analysis`、`gang-cluster-analysis`。\n")
    report.append("- 若要比较阶段性变化，建议继续使用 `time-series-anomaly-analysis`。\n")

    report.append("\n## 九、基础算子对齐\n")
    report.append("- 对象规模与标签分布 = `node_lookup + aggregation_query`\n")
    report.append("- 通话行为与夜间比例 = `relationship_filter + aggregation_query`\n")
    report.append("- 共享设备与设备池 = `query_shared_device + aggregation_query`\n")
    report.append("- 群体结构指标 = `subgraph_by_nodes / aggregation_query + scoring_layer`\n")

    report.append("\n## 十、生成文件\n")
    for name, path in summary.get("files", {}).items():
        report.append(f"- `{name}`：`{Path(path).name}`\n")
    report_path.write_text("".join(report), encoding="utf-8")

    pres = []
    pres.append(f"# 四川-陕西电话网络地域对比报告：{dataset}\n\n")
    pres.append("> 面向汇报展示，用于快速说明两地风险特征、行为模式和群体结构差异。\n\n")
    pres.append("## 一、分析口径\n\n")
    pres.append("- 本报告是 **全量地域对比**，不是 condition-based-screening 的条件切片。\n")
    pres.append(f"- 时间可比性提醒：{time_note}\n")
    pres.append("- 汇报解读时优先看比例、人均、日均和每活跃号码日均指标。\n\n")
    pres.append("## 二、对比总览\n\n")
    pres.append("| 维度 | 四川 | 陕西 | 说明 |\n|---|---:|---:|---|\n")
    pres.append(f"| 号码对象 | {safe_int(a_node.get('distinct_users'))} | {safe_int(b_node.get('distinct_users'))} | 唯一号码数 |\n")
    pres.append(f"| 风险对象 | {safe_int(a_node.get('risk_users'))} | {safe_int(b_node.get('risk_users'))} | label=1 |\n")
    pres.append(f"| 风险占比 | {safe_float(a_node.get('risk_ratio')):.2%} | {safe_float(b_node.get('risk_ratio')):.2%} | 风险对象/唯一号码 |\n")
    pres.append(f"| 通话记录 | {safe_int(a_call.get('call_record_count'))} | {safe_int(b_call.get('call_record_count'))} | 源号码通话记录，只作规模背景 |\n")
    pres.append(f"| 每活跃号码日均通话 | {safe_float(a_call.get('records_per_active_source_per_active_day')):.2f} | {safe_float(b_call.get('records_per_active_source_per_active_day')):.2f} | 归一化行为强度 |\n")
    pres.append(f"| 每活跃号码日均联系人 | {safe_float(a_call.get('counterparties_per_active_source_per_active_day')):.2f} | {safe_float(b_call.get('counterparties_per_active_source_per_active_day')):.2f} | 归一化联系人广度 |\n")
    pres.append(f"| 夜间占比 | {safe_float(a_call.get('night_ratio')):.2%} | {safe_float(b_call.get('night_ratio')):.2%} | 22:00-06:00 |\n")
    pres.append(f"| 共享设备 | {safe_int(a_dev.get('shared_device_count_within_province'))} | {safe_int(b_dev.get('shared_device_count_within_province'))} | 省内至少2号码共用 |\n")

    pres.append("\n## 三、主要发现\n")
    for item in findings[:5]:
        pres.append(f"- {item}\n")

    pres.append("\n## 四、重点证据样例\n")
    top_obj = dfs.get("top_objects", pd.DataFrame()).head(6)
    if not top_obj.empty:
        pres.append(md_table(top_obj, ["province", "rank_in_province", "user_preview", "sub_label", "record_count", "counterparty_count", "shared_device_count", "reason"], 6))
    else:
        pres.append("暂无代表性对象样例。\n")

    pres.append("\n## 五、后续建议\n")
    pres.append("1. 使用 `cross-province-linkage-analysis` 继续核查跨省共享设备和共同对端。\n")
    pres.append("2. 对 Top 对象使用 `risk-evidence-pack` 生成风险证据包。\n")
    pres.append("3. 对共享设备密集区域使用 `shared-device-analysis` 和 `gang-cluster-analysis` 下钻。\n")
    pres.append("\n## 六、交付文件\n")
    pres.append("- 技术报告：`sichuan_shaanxi_comparison_<dataset>.md`\n")
    pres.append("- 演示报告：`sichuan_shaanxi_comparison_<dataset>_presentation.md`\n")
    pres.append("- 结构化摘要：`sichuan_shaanxi_comparison_<dataset>_summary.json`\n")
    pres.append("- 证据工作簿：`sichuan_shaanxi_comparison_<dataset>_evidence.xlsx`\n")
    presentation_path.write_text("".join(pres), encoding="utf-8")

    return report_path, presentation_path


def write_outputs(
    output_dir: Path,
    prefix: str,
    dfs: Dict[str, pd.DataFrame],
    summary: Dict[str, Any],
) -> Dict[str, str]:
    files: Dict[str, str] = {}
    csv_map = {
        "node_overview": "province_overview_csv",
        "label_distribution": "label_distribution_csv",
        "sub_label_distribution": "sub_label_distribution_csv",
        "call_behavior": "call_behavior_csv",
        "hourly_distribution": "hourly_distribution_csv",
        "device_summary": "device_summary_csv",
        "top_shared_devices": "top_shared_devices_csv",
        "cross_device_examples": "cross_device_examples_csv",
        "structure_metrics": "structure_metrics_csv",
        "metric_contrast": "metric_contrast_csv",
        "top_objects": "top_objects_csv",
        "top_callers": "top_callers_csv",
    }
    for df_key, file_key in csv_map.items():
        df = dfs.get(df_key)
        if df is not None and not df.empty:
            path = output_dir / f"{prefix}_{df_key}.csv"
            df.to_csv(path, index=False, encoding="utf-8-sig")
            files[file_key] = str(path)
    summary_json = output_dir / f"{prefix}_summary.json"
    # summary will be written after files are finalized by main
    files["summary_json"] = str(summary_json)
    xlsx_path = output_dir / f"{prefix}_evidence.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        for sheet_name, df in dfs.items():
            if df is not None and not df.empty:
                safe_sheet = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_sheet, index=False)
    files["evidence_xlsx"] = str(xlsx_path)
    return files


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def build_error_result(args, dataset_root: Path, paths: Dict[str, Optional[Path]], meta: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
    prefix = f"sichuan_shaanxi_comparison_{args.dataset}"
    report_path = output_dir / f"{prefix}.md"
    report_path.write_text(
        "# 四川-陕西电话网络地域对比分析失败\n\n"
        f"- 数据集：`{args.dataset}`\n"
        f"- 数据根目录：`{dataset_root}`\n"
        f"- 缺失项：`{meta.get('missing', [])}`\n"
        "\n请检查是否存在统一图结构文件：user_nodes.csv、call_edges.csv、edges_phone_imei.parquet。\n",
        encoding="utf-8",
    )
    return {
        "ok": False,
        "skill": "sichuan-shaanxi-comparison",
        "query_type": "province_comparison",
        "script_version": SCRIPT_VERSION,
        "dataset": args.dataset,
        "status": "missing_required_data",
        "missing": meta.get("missing", []),
        "paths": {k: str(v) if v else None for k, v in paths.items()},
        "artifacts": [{"type": "markdown_report", "path": str(report_path), "title": report_path.name}],
        "report_path": str(report_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare Sichuan and Shaanxi phone-network data.")
    parser.add_argument("--dataset-root", default=None, help="Phone-network dataset root. Defaults to common project paths.")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Dataset name, default: unified")
    parser.add_argument("--province-a", default=DEFAULT_PROVINCE_A, help="First province, default: sichuan")
    parser.add_argument("--province-b", default=DEFAULT_PROVINCE_B, help="Second province, default: shaanxi")
    parser.add_argument("--top-k", type=int, default=10, help="Top K examples per province")
    parser.add_argument("--output-dir", default="/mnt/user-data/outputs", help="Output directory")
    parser.add_argument("--artifact-mode", choices=["full", "markdown_only", "essential"], default="full", help="Control which generated files are exposed in JSON artifacts: full exposes all evidence files; markdown_only exposes only Markdown reports; essential exposes Markdown + summary JSON + evidence XLSX.")
    args = parser.parse_args()

    dataset_root = find_dataset_root(args.dataset_root)
    output_dir = ensure_output_dir(args.output_dir)
    province_a = args.province_a.lower()
    province_b = args.province_b.lower()
    provinces = [province_a, province_b]
    prefix = f"sichuan_shaanxi_comparison_{args.dataset}"

    conn = duckdb.connect(database=":memory:")
    paths = resolve_paths(dataset_root, args.dataset)
    meta = setup_views(conn, paths)
    if meta.get("missing"):
        result = build_error_result(args, dataset_root, paths, meta, output_dir)
        print(json.dumps(json_safe(result), ensure_ascii=False, indent=2))
        return

    node_overview, label_dist, sub_label_dist = get_node_summary(conn, provinces)
    call_behavior, phone_call_stats, hourly_dist, top_callers = get_call_behavior(conn, provinces)
    device_summary, top_devices, phone_device_stats, cross_device_examples = get_device_summary(conn, provinces)
    structure_metrics = build_structure_metrics(node_overview, phone_call_stats, phone_device_stats, provinces)
    metric_contrast = build_metric_contrast(node_overview, sub_label_dist, call_behavior, device_summary, structure_metrics, province_a, province_b)
    top_objects = select_top_objects(phone_call_stats, phone_device_stats, provinces, args.top_k)

    dfs = {
        "node_overview": add_ratio_fields(node_overview),
        "label_distribution": add_ratio_fields(label_dist),
        "sub_label_distribution": add_ratio_fields(sub_label_dist),
        "call_behavior": add_ratio_fields(call_behavior),
        "hourly_distribution": add_ratio_fields(hourly_dist),
        "top_callers": add_ratio_fields(top_callers),
        "device_summary": add_ratio_fields(device_summary),
        "top_shared_devices": add_ratio_fields(top_devices),
        "cross_device_examples": add_ratio_fields(cross_device_examples),
        "structure_metrics": add_ratio_fields(structure_metrics),
        "metric_contrast": add_ratio_fields(metric_contrast),
        "top_objects": add_ratio_fields(top_objects),
    }

    key_findings = build_key_findings(metric_contrast, provinces)
    time_comparability_note = build_time_comparability_note(dfs["call_behavior"], province_a, province_b)
    files = write_outputs(output_dir, prefix, dfs, {})

    summary: Dict[str, Any] = {
        "ok": True,
        "skill": "sichuan-shaanxi-comparison",
        "query_type": "province_comparison",
        "script_version": SCRIPT_VERSION,
        "dataset": args.dataset,
        "dataset_root": str(dataset_root),
        "status": "ok",
        "analysis_scope": "full_province_comparison_not_condition_screening",
        "time_comparability_note": time_comparability_note,
        "input_summary": {
            "dataset_root": str(dataset_root),
            "dataset": args.dataset,
            "province_a": province_a,
            "province_b": province_b,
            "top_k": args.top_k,
            "paths": {k: str(v) if v else None for k, v in paths.items()},
        },
        "result": {
            "province_overview": dataframe_records(dfs["node_overview"]),
            "call_behavior": dataframe_records(dfs["call_behavior"]),
            "device_summary": dataframe_records(dfs["device_summary"]),
            "structure_metrics": dataframe_records(dfs["structure_metrics"]),
            "metric_contrast_top": dataframe_records(dfs["metric_contrast"], 20),
            "top_objects": dataframe_records(dfs["top_objects"], 20),
            "key_findings": key_findings,
            "time_comparability_note": time_comparability_note,
            "recommended_next_steps": [
                "cross-province-linkage-analysis",
                "risk-evidence-pack",
                "shared-device-analysis",
                "gang-cluster-analysis",
                "time-series-anomaly-analysis",
            ],
        },
        "base_operator_alignment": {
            "node_distribution": "node_lookup + aggregation_query",
            "call_behavior": "relationship_filter + aggregation_query",
            "device_pool": "query_shared_device + aggregation_query",
            "group_structure": "subgraph_by_nodes / aggregation_query + scoring_layer",
            "comparison_layer": "province filter + metric contrast + evidence ranking",
        },
        "notes": [time_comparability_note] + meta.get("warnings", []),
        "files": files,
    }
    report_path, presentation_path = write_reports(output_dir, prefix, args.dataset, province_a, province_b, summary, dfs)
    files["report_md"] = str(report_path)
    files["presentation_md"] = str(presentation_path)
    # rewrite final summary json after report files are known
    summary["files"] = files
    # Artifact exposure policy:
    # - The script always writes all evidence files for reproducibility.
    # - But front-end download cards are usually driven by the JSON `artifacts` field.
    # - Therefore `--artifact-mode` controls what gets advertised to the front end.
    #   This solves the common case where the user asks for Markdown only and too many
    #   CSV/XLSX/JSON cards would make the front-end output noisy.
    summary["artifact_mode"] = args.artifact_mode
    if args.artifact_mode == "markdown_only":
        summary["files"] = {
            "report_md": str(report_path),
            "presentation_md": str(presentation_path),
        }
        artifacts = [
            {"type": "markdown_report", "path": str(report_path), "title": report_path.name},
            {"type": "markdown_report", "path": str(presentation_path), "title": presentation_path.name},
        ]
    elif args.artifact_mode == "essential":
        keep = {"report_md", "presentation_md", "summary_json", "evidence_xlsx"}
        summary["files"] = {k: v for k, v in files.items() if k in keep}
        artifacts = []
        for key in ["report_md", "presentation_md", "summary_json", "evidence_xlsx"]:
            if key not in files:
                continue
            p = Path(files[key])
            if key in {"report_md", "presentation_md"}:
                typ = "markdown_report"
            else:
                typ = p.suffix.lower().replace(".", "") or "file"
            artifacts.append({"type": typ, "path": str(p), "title": p.name})
    else:
        summary["files"] = files
        artifacts = [
            {"type": "markdown_report", "path": str(report_path), "title": report_path.name},
            {"type": "markdown_report", "path": str(presentation_path), "title": presentation_path.name},
        ]
        for key, path in files.items():
            p = Path(path)
            if key in {"report_md", "presentation_md"}:
                continue
            suffix = p.suffix.lower().replace(".", "") or "file"
            artifacts.append({"type": suffix, "path": str(p), "title": p.name})
    summary["artifacts"] = artifacts
    summary["report_path"] = str(report_path)

    # Always write the full summary JSON to the path that was generated before the
    # optional artifact filtering. This keeps command-line evidence complete even
    # when the front-end display mode is markdown_only.
    Path(files["summary_json"]).write_text(json.dumps(json_safe(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(json_safe(summary), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
