#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

SCRIPT_VERSION = "time-series-anomaly-analysis-release-v1.4"

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
HOUR_CANDIDATES = ["call_hour", "hour", "start_hour"]
DATASET_ROOT_CANDIDATES = [
    os.environ.get("PHONE_NETWORK_DATASET_ROOT", ""),
    "/mnt/datasets/phone-network",
    "/workspace/imiss-deer-flow-main/datasets/phone-network",
]


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or pd.isna(v):
            return default
        return int(v)
    except Exception:
        return default


def preview(value: Any, n: int = 12) -> str:
    if value is None:
        return ""
    s = str(value)
    return s if len(s) <= n else s[:n] + "..."


def fmt_date(value: Any) -> str:
    if value is None:
        return ""
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return str(value)
        return str(pd.Timestamp(ts).date())
    except Exception:
        return str(value)


def fmt_pct(value: Any) -> str:
    if value is None:
        return "无基线"
    try:
        if pd.isna(value):
            return "无基线"
    except Exception:
        pass
    return f"{round(float(value), 2)}%"


def stage_zh(stage: str) -> str:
    mapping = {
        "spike_rising": "阶段性活跃上升",
        "night_shift_rising": "夜间行为上升",
        "cooling_down": "活跃度下降",
        "volatile": "波动型异常",
        "stable_or_mild": "整体平稳或轻微波动",
        "unknown": "证据不足",
    }
    return mapping.get(stage or "", stage or "unknown")


def ensure_output_dir() -> Path:
    for p in [Path("/mnt/user-data/outputs"), Path("/workspace/imiss-deer-flow-main/outputs")]:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            pass
    local = Path(__file__).resolve().parent.parent / "outputs"
    local.mkdir(parents=True, exist_ok=True)
    return local


def detect_candidates(columns: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def find_dataset_root(explicit: Optional[str]) -> str:
    if explicit:
        p = Path(explicit)
        if p.exists():
            return str(p)
        raise FileNotFoundError(f"dataset root not found: {explicit}")
    for c in DATASET_ROOT_CANDIDATES:
        if c and Path(c).exists():
            return c
    raise FileNotFoundError("无法找到电话网络数据集根目录，请通过 --dataset-root 指定。")


def resolve_paths(dataset_root: str, dataset: str) -> Dict[str, str]:
    root = Path(dataset_root)
    user_nodes = root / "processed" / dataset / "user_nodes.csv"
    call_edges = root / "processed" / dataset / "call_edges.csv"
    device_parquet = root / "processed" / "graph_views" / dataset / "edges_phone_imei.parquet"
    device_csv = root / "processed" / "graph_views" / dataset / "edges_phone_imei.csv"
    if device_parquet.exists():
        device_edges = device_parquet
    elif device_csv.exists():
        device_edges = device_csv
    else:
        device_edges = None
    for p in [user_nodes, call_edges]:
        if not p.exists():
            raise FileNotFoundError(f"required file not found: {p}")
    return {
        "user_nodes": str(user_nodes),
        "call_edges": str(call_edges),
        "device_edges": str(device_edges) if device_edges else "",
    }


def connect(paths: Dict[str, str]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(database=":memory:")
    conn.execute("PRAGMA threads=4")
    conn.execute(f"CREATE VIEW user_nodes AS SELECT * FROM read_csv_auto('{paths['user_nodes']}', HEADER=TRUE)")
    conn.execute(f"CREATE VIEW call_edges AS SELECT * FROM read_csv_auto('{paths['call_edges']}', HEADER=TRUE)")
    if paths.get("device_edges"):
        if paths["device_edges"].endswith(".parquet"):
            conn.execute(f"CREATE VIEW device_edges AS SELECT * FROM read_parquet('{paths['device_edges']}')")
        else:
            conn.execute(f"CREATE VIEW device_edges AS SELECT * FROM read_csv_auto('{paths['device_edges']}', HEADER=TRUE)")
    return conn


def detect_columns(conn: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]


def setup_views(conn: duckdb.DuckDBPyConnection) -> Dict[str, Optional[str]]:
    user_cols = detect_columns(conn, "user_nodes")
    call_cols = detect_columns(conn, "call_edges")

    user_id_col = detect_candidates(user_cols, ["user_id", "id", "phone_id"])
    label_col = detect_candidates(user_cols, ["label"])
    sub_label_col = detect_candidates(user_cols, ["sub_label", "risk_sub_label"])
    province_col = detect_candidates(user_cols, ["province"])

    src_col = detect_candidates(call_cols, ["src_user_id", "src", "source", "caller_id", "user_id"])
    dst_col = detect_candidates(call_cols, ["dst_counterparty_id", "dst", "target", "callee_id", "counterparty_id"])
    weight_col = detect_candidates(call_cols, ["call_count", "cnt", "weight", "times", "freq", "count"])
    duration_col = detect_candidates(call_cols, ["duration", "duration_sec", "call_duration", "duration_seconds"])
    hour_col = detect_candidates(call_cols, HOUR_CANDIDATES)
    time_col = detect_candidates(call_cols, TIME_CANDIDATES)

    missing = [n for n, v in {"user_id": user_id_col, "src": src_col, "dst": dst_col}.items() if not v]
    if missing:
        raise RuntimeError(f"failed to detect required columns: {missing}")

    ts_expr = f"TRY_CAST({time_col} AS TIMESTAMP)" if time_col else "NULL"
    if time_col and hour_col:
        hour_expr = (
            f"CASE WHEN {ts_expr} IS NOT NULL THEN EXTRACT('hour' FROM {ts_expr}) "
            f"ELSE TRY_CAST({hour_col} AS DOUBLE) END"
        )
    elif time_col:
        hour_expr = f"CASE WHEN {ts_expr} IS NOT NULL THEN EXTRACT('hour' FROM {ts_expr}) ELSE NULL END"
    elif hour_col:
        hour_expr = f"TRY_CAST({hour_col} AS DOUBLE)"
    else:
        hour_expr = "NULL"
    date_expr = f"CAST({ts_expr} AS DATE)" if time_col else "NULL"

    conn.execute(f"""
        CREATE OR REPLACE VIEW user_nodes_std AS
        SELECT
            CAST({user_id_col} AS VARCHAR) AS user_id,
            {label_col if label_col else 'NULL'} AS label,
            {sub_label_col if sub_label_col else 'NULL'} AS sub_label,
            {province_col if province_col else 'NULL'} AS province,
            *
        FROM user_nodes
    """)

    conn.execute(f"""
        CREATE OR REPLACE VIEW call_edges_std AS
        SELECT
            CAST({src_col} AS VARCHAR) AS src_user_id,
            CAST({dst_col} AS VARCHAR) AS dst_counterparty_id,
            CAST({weight_col if weight_col else '1'} AS DOUBLE) AS edge_weight,
            CAST({duration_col if duration_col else 'NULL'} AS DOUBLE) AS duration_value,
            {ts_expr} AS event_ts,
            {date_expr} AS event_date,
            {hour_expr} AS hour_value
        FROM call_edges
        WHERE {src_col} IS NOT NULL AND {dst_col} IS NOT NULL
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW undirected_contact AS
        SELECT
            src_user_id AS user_id,
            dst_counterparty_id AS counterparty_id,
            edge_weight,
            duration_value,
            event_ts,
            event_date,
            hour_value
        FROM call_edges_std
        UNION ALL
        SELECT
            dst_counterparty_id AS user_id,
            src_user_id AS counterparty_id,
            edge_weight,
            duration_value,
            event_ts,
            event_date,
            hour_value
        FROM call_edges_std
    """)

    return {
        "time_col": time_col,
        "hour_col": hour_col,
    }


def load_scope_ids(args: argparse.Namespace) -> List[str]:
    ids: List[str] = []
    if args.phone_id:
        ids.append(args.phone_id.strip())
    if args.phone_ids:
        ids.extend([x.strip() for x in args.phone_ids.split(",") if x.strip()])
    if args.phone_id_file:
        f = Path(args.phone_id_file)
        if not f.exists():
            skill_local = Path(__file__).resolve().parent / args.phone_id_file
            if skill_local.exists():
                f = skill_local
        if not f.exists():
            raise FileNotFoundError(f"scope file not found: {args.phone_id_file}")
        ids.extend([line.strip() for line in f.read_text(encoding="utf-8").splitlines() if line.strip()])
    dedup = []
    seen = set()
    for x in ids:
        if x not in seen:
            seen.add(x)
            dedup.append(x)
    return dedup


def register_scope(conn: duckdb.DuckDBPyConnection, ids: List[str]) -> None:
    conn.register("scope_df", pd.DataFrame({"user_id": ids}))
    conn.execute("CREATE OR REPLACE VIEW scope_ids AS SELECT CAST(user_id AS VARCHAR) AS user_id FROM scope_df")


def get_profile(conn: duckdb.DuckDBPyConnection, phone_id: str) -> Dict[str, Any]:
    df = conn.execute("SELECT * FROM user_nodes_std WHERE user_id = ? LIMIT 1", [phone_id]).df()
    if df.empty:
        return {"node_found": False, "phone_id": phone_id}
    row = df.iloc[0].to_dict()
    return {
        "node_found": True,
        "phone_id": phone_id,
        "province": row.get("province"),
        "label": safe_int(row.get("label"), None),
        "sub_label": row.get("sub_label"),
    }


def recent_baseline_bounds(max_date: pd.Timestamp, recent_days: int, baseline_days: int) -> Dict[str, pd.Timestamp]:
    recent_end = pd.Timestamp(max_date)
    recent_start = recent_end - pd.Timedelta(days=recent_days - 1)
    baseline_end = recent_start - pd.Timedelta(days=1)
    baseline_start = baseline_end - pd.Timedelta(days=baseline_days - 1)
    return {
        "recent_start": recent_start,
        "recent_end": recent_end,
        "baseline_start": baseline_start,
        "baseline_end": baseline_end,
    }


def _numeric_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a numeric Series even when duplicate column names exist.

    Pandas returns a DataFrame rather than a Series if a DataFrame has duplicate
    column names. That can happen after an unsafe rename. This helper keeps the
    downstream JSON result clean and avoids accidental pandas Series values.
    """
    if df.empty or col not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
    values = df[col]
    if isinstance(values, pd.DataFrame):
        values = values.iloc[:, 0]
    return pd.to_numeric(values, errors="coerce").fillna(0.0)


def _mean_col(df: pd.DataFrame, col: str, digits: int = 2) -> float:
    if df.empty:
        return 0.0
    return round(float(_numeric_col(df, col).mean()), digits)


def complete_daily_calendar(df: pd.DataFrame, start: Optional[pd.Timestamp] = None, end: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """Fill missing calendar days with zero-valued rows.

    The v1.3 implementation averaged only days with records. That made phrases
    like "近7天日均" misleading when one or more days had no activity. This
    function makes the evidence table and window statistics calendar-day based.
    """
    base_cols = list(df.columns) if not df.empty else ["event_date", "record_count", "weighted_count", "counterparty_count", "night_ratio"]
    if df.empty and (start is None or end is None):
        return pd.DataFrame(columns=base_cols)
    work = df.copy()
    if not work.empty:
        work["event_date"] = pd.to_datetime(work["event_date"], errors="coerce").dt.normalize()
        work = work.dropna(subset=["event_date"])
    min_date = pd.Timestamp(start).normalize() if start is not None else work["event_date"].min()
    max_date = pd.Timestamp(end).normalize() if end is not None else work["event_date"].max()
    if pd.isna(min_date) or pd.isna(max_date):
        return pd.DataFrame(columns=base_cols)
    calendar = pd.DataFrame({"event_date": pd.date_range(min_date, max_date, freq="D")})
    merged = calendar.merge(work, on="event_date", how="left")
    numeric_zero_cols = [
        "record_count", "weighted_count", "counterparty_count", "night_ratio", "total_duration", "active_member_count"
    ]
    for c in numeric_zero_cols:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)
    return merged


def summarize_period(
    df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    counterparty_col: str = "counterparty_count",
) -> Dict[str, float]:
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    expected_days = int((end_ts - start_ts).days) + 1
    if df.empty:
        return {
            "days": expected_days,
            "active_days": 0,
            "avg_records": 0.0,
            "avg_weighted": 0.0,
            "avg_counterparties": 0.0,
            "avg_night_ratio": 0.0,
        }
    event_dates = pd.to_datetime(df["event_date"], errors="coerce").dt.normalize()
    mask = (event_dates >= start_ts) & (event_dates <= end_ts)
    part = df[mask].copy()
    part = complete_daily_calendar(part, start_ts, end_ts)
    active_days = int((_numeric_col(part, "record_count") > 0).sum()) if not part.empty else 0
    return {
        "days": expected_days,
        "active_days": active_days,
        "avg_records": _mean_col(part, "record_count", 2),
        "avg_weighted": _mean_col(part, "weighted_count", 2),
        "avg_counterparties": _mean_col(part, counterparty_col, 2),
        "avg_night_ratio": _mean_col(part, "night_ratio", 4),
    }


def calc_change(recent: float, baseline: float) -> Dict[str, float]:
    delta = recent - baseline
    pct = None if baseline == 0 else (delta / baseline) * 100.0
    return {"delta": round(delta, 2), "pct_change": None if pct is None else round(pct, 2)}


def classify_stage(recent_records: float, baseline_records: float, recent_night: float, baseline_night: float, anomaly_days: int) -> str:
    change = calc_change(recent_records, baseline_records)
    pct = change["pct_change"]
    if pct is not None and pct >= 50 and anomaly_days >= 1:
        return "spike_rising"
    if recent_night - baseline_night >= 0.15 and recent_records >= baseline_records:
        return "night_shift_rising"
    if pct is not None and pct <= -30:
        return "cooling_down"
    if anomaly_days >= 2:
        return "volatile"
    return "stable_or_mild"


def compute_anomaly_days(df: pd.DataFrame, top_k: int) -> pd.DataFrame:
    """Return truly notable days, not every low-volume negative-z day.

    v1.3 used abs(z) >= 1, which put very low-count historical days into
    "重点异常日期". This version prioritizes upward spikes and meaningful
    night-shift days. If no strong anomaly exists, it returns only top active
    days and marks them as top_active, so the report does not overclaim.
    """
    cols = ["event_date", "record_count", "weighted_count", "counterparty_count", "night_ratio", "zscore_records", "anomaly_type"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    work = complete_daily_calendar(df)
    if work.empty:
        return pd.DataFrame(columns=cols)
    work = work.copy()
    for c in ["record_count", "weighted_count", "counterparty_count", "night_ratio"]:
        if c not in work.columns:
            work[c] = 0
        work[c] = pd.to_numeric(work[c], errors="coerce").fillna(0)
    mean = work["record_count"].mean()
    std = work["record_count"].std(ddof=0)
    if std == 0 or pd.isna(std):
        work["zscore_records"] = 0.0
    else:
        work["zscore_records"] = (work["record_count"] - mean) / std
    min_records_for_night = max(2, int(math.ceil(float(work["record_count"].mean()) * 0.2)))

    def tag(row: pd.Series) -> str:
        z = safe_float(row.get("zscore_records"))
        rc = safe_float(row.get("record_count"))
        nr = safe_float(row.get("night_ratio"))
        if z >= 2.0:
            return "spike"
        if z >= 1.5:
            return "mild_spike"
        if nr >= 0.5 and rc >= min_records_for_night:
            return "night_shift"
        if z <= -2.0 and rc == 0:
            return "drop"
        return "normal"

    work["anomaly_type"] = work.apply(tag, axis=1)
    display = work[work["anomaly_type"].isin(["spike", "mild_spike", "night_shift", "drop"])].copy()
    # Avoid flooding the report with zero-activity drops. Keep them only when no upward evidence exists.
    upward = display[display["anomaly_type"].isin(["spike", "mild_spike", "night_shift"])].copy()
    if not upward.empty:
        display = upward
    if display.empty:
        display = work.sort_values(["record_count", "weighted_count"], ascending=[False, False]).head(top_k).copy()
        display["anomaly_type"] = "top_active"
    display["anomaly_score"] = display["zscore_records"].abs() + display["record_count"].rank(pct=True)
    return display.sort_values(["anomaly_score", "record_count"], ascending=[False, False]).head(top_k)


def get_phone_timeseries(conn: duckdb.DuckDBPyConnection, phone_id: str, top_k: int, night_start: int, night_end: int) -> Dict[str, Any]:
    daily_df = conn.execute(
        """
        SELECT
            event_date,
            COUNT(*) AS record_count,
            COALESCE(SUM(edge_weight), 0) AS weighted_count,
            COUNT(DISTINCT counterparty_id) AS counterparty_count,
            COALESCE(AVG(CASE WHEN hour_value >= ? OR hour_value < ? THEN 1.0 ELSE 0.0 END), 0) AS night_ratio,
            COALESCE(SUM(duration_value), 0) AS total_duration
        FROM undirected_contact
        WHERE user_id = ? AND event_date IS NOT NULL
        GROUP BY event_date
        ORDER BY event_date
        """,
        [night_start, night_end, phone_id],
    ).df()
    hourly_df = conn.execute(
        """
        SELECT
            CAST(hour_value AS INTEGER) AS hour_value,
            COUNT(*) AS record_count,
            COALESCE(SUM(edge_weight), 0) AS weighted_count
        FROM undirected_contact
        WHERE user_id = ? AND hour_value IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """,
        [phone_id],
    ).df()
    recent_counterparties = conn.execute(
        """
        SELECT
            counterparty_id,
            COUNT(*) AS record_count,
            COALESCE(SUM(edge_weight), 0) AS weighted_count,
            COALESCE(AVG(CASE WHEN hour_value >= ? OR hour_value < ? THEN 1.0 ELSE 0.0 END), 0) AS night_ratio
        FROM undirected_contact
        WHERE user_id = ?
        GROUP BY 1
        ORDER BY weighted_count DESC, record_count DESC, counterparty_id ASC
        LIMIT ?
        """,
        [night_start, night_end, phone_id, max(top_k, 20)],
    ).df()
    if not recent_counterparties.empty:
        recent_counterparties["counterparty_preview"] = recent_counterparties["counterparty_id"].map(preview)
    return {"daily": daily_df, "hourly": hourly_df, "contributors": recent_counterparties}


def get_group_timeseries(conn: duckdb.DuckDBPyConnection, ids: List[str], top_k: int, night_start: int, night_end: int) -> Dict[str, Any]:
    register_scope(conn, ids)
    daily_df = conn.execute(
        """
        SELECT
            u.event_date,
            COUNT(*) AS record_count,
            COALESCE(SUM(u.edge_weight), 0) AS weighted_count,
            COUNT(DISTINCT u.user_id) AS active_member_count,
            COUNT(DISTINCT u.counterparty_id) AS counterparty_count,
            COALESCE(AVG(CASE WHEN u.hour_value >= ? OR u.hour_value < ? THEN 1.0 ELSE 0.0 END), 0) AS night_ratio
        FROM undirected_contact u
        JOIN scope_ids s ON u.user_id = s.user_id
        WHERE u.event_date IS NOT NULL
        GROUP BY u.event_date
        ORDER BY u.event_date
        """,
        [night_start, night_end],
    ).df()
    hourly_df = conn.execute(
        """
        SELECT
            CAST(u.hour_value AS INTEGER) AS hour_value,
            COUNT(*) AS record_count,
            COALESCE(SUM(u.edge_weight), 0) AS weighted_count,
            COUNT(DISTINCT u.user_id) AS active_member_count
        FROM undirected_contact u
        JOIN scope_ids s ON u.user_id = s.user_id
        WHERE u.hour_value IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """
    ).df()
    contributors_df = conn.execute(
        """
        SELECT
            u.user_id,
            COUNT(*) AS record_count,
            COALESCE(SUM(u.edge_weight), 0) AS weighted_count,
            COUNT(DISTINCT u.counterparty_id) AS counterparty_count,
            COALESCE(AVG(CASE WHEN u.hour_value >= ? OR u.hour_value < ? THEN 1.0 ELSE 0.0 END), 0) AS night_ratio,
            MAX(n.label) AS label,
            MAX(n.sub_label) AS sub_label,
            MAX(n.province) AS province
        FROM undirected_contact u
        JOIN scope_ids s ON u.user_id = s.user_id
        LEFT JOIN user_nodes_std n ON u.user_id = n.user_id
        GROUP BY 1
        ORDER BY weighted_count DESC, record_count DESC, user_id ASC
        LIMIT ?
        """,
        [night_start, night_end, max(top_k, 20)],
    ).df()
    if not contributors_df.empty:
        contributors_df["user_preview"] = contributors_df["user_id"].map(preview)
    return {"daily": daily_df, "hourly": hourly_df, "contributors": contributors_df}


def build_phone_summary(profile: Dict[str, Any], ts: Dict[str, Any], recent_days: int, baseline_days: int, top_k: int) -> Dict[str, Any]:
    daily_df = ts["daily"].copy()
    if daily_df.empty:
        return {
            "status": "time_evidence_unavailable",
            "notes": ["当前号码缺少可解析的日期级通话时间，无法形成时间趋势证据。"],
            "stage": "unknown",
            "bounds": None,
            "recent": {},
            "baseline": {},
            "changes": {},
            "anomaly_days": pd.DataFrame(),
            "top_signal_summary": ["当前无法形成有效时间序列证据。"],
        }
    max_date = pd.to_datetime(daily_df["event_date"]).max()
    bounds = recent_baseline_bounds(max_date, recent_days, baseline_days)
    # Complete the full baseline+recent calendar before computing averages and anomalies.
    daily_df = complete_daily_calendar(daily_df, bounds["baseline_start"], bounds["recent_end"])
    ts["daily"] = daily_df
    recent = summarize_period(daily_df, bounds["recent_start"], bounds["recent_end"])
    baseline = summarize_period(daily_df, bounds["baseline_start"], bounds["baseline_end"])
    changes = {
        "records": calc_change(recent["avg_records"], baseline["avg_records"]),
        "counterparties": calc_change(recent["avg_counterparties"], baseline["avg_counterparties"]),
        "night_ratio": calc_change(recent["avg_night_ratio"], baseline["avg_night_ratio"]),
    }
    anomaly_days = compute_anomaly_days(daily_df, top_k)
    stage = classify_stage(recent["avg_records"], baseline["avg_records"], recent["avg_night_ratio"], baseline["avg_night_ratio"], len(anomaly_days))
    summary_lines = [
        f"近{recent_days}天日均通话 {recent['avg_records']}，基线{baseline_days}天日均通话 {baseline['avg_records']}。",
        f"近{recent_days}天活跃日 {recent['active_days']} 天，基线活跃日 {baseline['active_days']} 天。",
        f"近{recent_days}天夜间占比 {recent['avg_night_ratio']:.2f}，基线夜间占比 {baseline['avg_night_ratio']:.2f}。",
        f"共识别 {len(anomaly_days)} 个重点异常日期。",
    ]
    return {
        "status": "ok",
        "notes": [],
        "stage": stage,
        "bounds": {k: str(v.date()) for k, v in bounds.items()},
        "recent": recent,
        "baseline": baseline,
        "changes": changes,
        "data_coverage": {"recent_active_days": recent.get("active_days", 0), "baseline_active_days": baseline.get("active_days", 0)},
        "anomaly_days": anomaly_days,
        "top_signal_summary": summary_lines,
    }


def build_group_summary(ts: Dict[str, Any], recent_days: int, baseline_days: int, top_k: int) -> Dict[str, Any]:
    daily_df = ts["daily"].copy()
    if daily_df.empty:
        return {
            "status": "time_evidence_unavailable",
            "notes": ["当前群体缺少可解析的日期级通话时间，无法形成时间趋势证据。"],
            "stage": "unknown",
            "bounds": None,
            "recent": {},
            "baseline": {},
            "changes": {},
            "anomaly_days": pd.DataFrame(),
            "top_signal_summary": ["当前无法形成有效群体时间序列证据。"],
        }
    max_date = pd.to_datetime(daily_df["event_date"]).max()
    bounds = recent_baseline_bounds(max_date, recent_days, baseline_days)
    # Complete the full baseline+recent calendar before computing averages and anomalies.
    daily_df = complete_daily_calendar(daily_df, bounds["baseline_start"], bounds["recent_end"])
    ts["daily"] = daily_df
    recent = summarize_period(daily_df, bounds["recent_start"], bounds["recent_end"], counterparty_col="counterparty_count")
    # restore active_member metrics separately
    event_dates = pd.to_datetime(daily_df["event_date"], errors="coerce").dt.normalize()
    rmask = (event_dates >= pd.Timestamp(bounds["recent_start"]).normalize()) & (event_dates <= pd.Timestamp(bounds["recent_end"]).normalize())
    bmask = (event_dates >= pd.Timestamp(bounds["baseline_start"]).normalize()) & (event_dates <= pd.Timestamp(bounds["baseline_end"]).normalize())
    recent_active = round(daily_df[rmask]["active_member_count"].mean(), 2) if not daily_df[rmask].empty else 0.0
    baseline_active = round(daily_df[bmask]["active_member_count"].mean(), 2) if not daily_df[bmask].empty else 0.0
    baseline = summarize_period(daily_df, bounds["baseline_start"], bounds["baseline_end"], counterparty_col="counterparty_count")
    changes = {
        "records": calc_change(recent["avg_records"], baseline["avg_records"]),
        "active_members": calc_change(recent_active, baseline_active),
        "night_ratio": calc_change(recent["avg_night_ratio"], baseline["avg_night_ratio"]),
    }
    anomaly_days = compute_anomaly_days(daily_df, top_k)
    stage = classify_stage(recent["avg_records"], baseline["avg_records"], recent["avg_night_ratio"], baseline["avg_night_ratio"], len(anomaly_days))
    active_scope_size = safe_int(ts.get("contributors", pd.DataFrame()).shape[0]) if isinstance(ts.get("contributors"), pd.DataFrame) else 0
    summary_lines = [
        f"近{recent_days}天群体日均通话 {recent['avg_records']}，基线{baseline_days}天为 {baseline['avg_records']}。",
        f"近{recent_days}天群体日均活跃成员 {recent_active}，基线为 {baseline_active}。",
        f"输入群体中有时间行为记录的成员数为 {active_scope_size}。",
        f"共识别 {len(anomaly_days)} 个重点异常日期。",
    ]
    recent["avg_active_members"] = recent_active
    baseline["avg_active_members"] = baseline_active
    return {
        "status": "ok",
        "notes": [],
        "stage": stage,
        "bounds": {k: str(v.date()) for k, v in bounds.items()},
        "recent": recent,
        "baseline": baseline,
        "changes": changes,
        "data_coverage": {"recent_active_days": recent.get("active_days", 0), "baseline_active_days": baseline.get("active_days", 0), "active_scope_size": active_scope_size},
        "anomaly_days": anomaly_days,
        "top_signal_summary": summary_lines,
    }


def build_followups(mode: str, summary: Dict[str, Any]) -> List[Dict[str, str]]:
    if summary.get("status") != "ok":
        return [
            {"skill": "single-number-analysis" if mode == "phone" else "group-risk-analysis", "reason": "先确认对象基础画像和当前数据覆盖情况。"},
            {"skill": "condition-based-screening", "reason": "若时间证据不足，可先用规则筛选缩小候选范围。"},
        ]
    if mode == "phone":
        return [
            {"skill": "single-number-analysis", "reason": "继续查看该号码当前局部关系圈和共享设备结构。"},
            {"skill": "shared-device-analysis", "reason": "若时间异常伴随设备共用，可继续查看同设备号码。"},
            {"skill": "risk-evidence-pack", "reason": "将时间异常与画像、设备、同圈证据合并成单号证据包。"},
        ]
    return [
        {"skill": "group-risk-analysis", "reason": "继续判断异常阶段中的群体类型和群体级模式。"},
        {"skill": "gang-cluster-analysis", "reason": "若异常阶段集中爆发，可继续验证是否存在更紧密团伙簇。"},
        {"skill": "condition-based-screening", "reason": "可对异常日期附近对象再做条件筛选，锁定阶段性上升成员。"},
    ]


def render_markdown(mode: str, target_desc: str, dataset: str, profile: Dict[str, Any], ids: List[str], ts: Dict[str, Any], summary: Dict[str, Any], paths: Dict[str, Path], top_k: int) -> str:
    lines: List[str] = []
    title = "号码级" if mode == "phone" else "群体级"
    lines.append(f"# 时间序列异常分析：{title}{target_desc}")
    lines.append("")
    lines.append("## 一、核心结论")
    lines.append("")
    lines.append(f"- 数据集：`{dataset}`")
    lines.append(f"- 分析模式：`{mode}`")
    lines.append(f"- 处理状态：`{summary.get('status')}`")
    if mode == "phone":
        lines.append(f"- 目标号码：`{target_desc}`")
        lines.append(f"- 省份：`{profile.get('province')}` | label=`{profile.get('label')}` | sub_label=`{profile.get('sub_label')}`")
    else:
        lines.append(f"- 输入成员数：`{len(ids)}`")
        lines.append(f"- 成员预览：{', '.join(preview(x) for x in ids[:10])}{' ...' if len(ids) > 10 else ''}")
    if summary.get("status") != "ok":
        for n in summary.get("notes", []):
            lines.append(f"- 说明：{n}")
    else:
        lines.append(f"- 阶段判断：`{summary.get('stage')}`（{stage_zh(summary.get('stage'))}）")
        for s in summary.get("top_signal_summary", []):
            lines.append(f"- {s}")

        lines.append("")
        lines.append("## 二、窗口对比")
        lines.append("")
        b = summary["bounds"]
        lines.append(f"- recent：`{b['recent_start']}` ~ `{b['recent_end']}`")
        lines.append(f"- baseline：`{b['baseline_start']}` ~ `{b['baseline_end']}`")
        if mode == "phone":
            lines.append(f"- recent 日均通话：`{summary['recent']['avg_records']}` | baseline：`{summary['baseline']['avg_records']}` | 变化：`{fmt_pct(summary['changes']['records']['pct_change'])}`")
            lines.append(f"- recent 日均联系人：`{summary['recent']['avg_counterparties']}` | baseline：`{summary['baseline']['avg_counterparties']}` | 变化：`{fmt_pct(summary['changes']['counterparties']['pct_change'])}`")
            lines.append(f"- recent 活跃日：`{summary['recent'].get('active_days', 0)}/{summary['recent'].get('days', 0)}` | baseline 活跃日：`{summary['baseline'].get('active_days', 0)}/{summary['baseline'].get('days', 0)}`")
        else:
            lines.append(f"- recent 日均通话：`{summary['recent']['avg_records']}` | baseline：`{summary['baseline']['avg_records']}` | 变化：`{fmt_pct(summary['changes']['records']['pct_change'])}`")
            lines.append(f"- recent 日均活跃成员：`{summary['recent']['avg_active_members']}` | baseline：`{summary['baseline']['avg_active_members']}` | 变化：`{fmt_pct(summary['changes']['active_members']['pct_change'])}`")
            lines.append(f"- recent 活跃日：`{summary['recent'].get('active_days', 0)}/{summary['recent'].get('days', 0)}` | baseline 活跃日：`{summary['baseline'].get('active_days', 0)}/{summary['baseline'].get('days', 0)}`")
        lines.append(f"- recent 夜间占比：`{summary['recent']['avg_night_ratio']}` | baseline：`{summary['baseline']['avg_night_ratio']}` | 变化：`{fmt_pct(summary['changes']['night_ratio']['pct_change'])}`")

        lines.append("")
        lines.append("## 三、重点异常日期")
        lines.append("")
        adf = summary["anomaly_days"]
        if adf.empty:
            lines.append("- 当前没有识别到显著异常日期。")
        else:
            for _, r in adf.head(top_k).iterrows():
                lines.append(
                    f"- 日期 `{fmt_date(r['event_date'])}` | 记录数={safe_int(r['record_count'])} | 加权量={round(safe_float(r['weighted_count']), 2)} | 夜间占比={round(safe_float(r.get('night_ratio')), 2)} | zscore={round(safe_float(r.get('zscore_records')), 2)} | 类型={r.get('anomaly_type')}"
                )

        lines.append("")
        lines.append("## 四、小时分布")
        lines.append("")
        hdf = ts["hourly"]
        if hdf.empty:
            lines.append("- 当前没有可解释的小时分布证据。")
        else:
            for _, r in hdf.sort_values(["record_count", "weighted_count"], ascending=[False, False]).head(top_k).iterrows():
                extra = f" | 活跃成员={safe_int(r['active_member_count'])}" if 'active_member_count' in hdf.columns else ''
                lines.append(f"- 小时 `{safe_int(r['hour_value'])}` | 记录数={safe_int(r['record_count'])} | 加权量={round(safe_float(r['weighted_count']), 2)}{extra}")

        lines.append("")
        lines.append("## 五、关键贡献对象")
        lines.append("")
        cdf = ts["contributors"]
        if cdf.empty:
            lines.append("- 当前没有可解释的关键贡献对象。")
        else:
            for _, r in cdf.head(top_k).iterrows():
                if mode == 'phone':
                    lines.append(f"- 对端 `{r['counterparty_preview']}` | 记录数={safe_int(r['record_count'])} | 加权量={round(safe_float(r['weighted_count']), 2)} | 夜间占比={round(safe_float(r['night_ratio']), 2)}")
                else:
                    lines.append(f"- 成员 `{r['user_preview']}` | 记录数={safe_int(r['record_count'])} | 加权量={round(safe_float(r['weighted_count']), 2)} | 联系人={safe_int(r['counterparty_count'])} | 夜间占比={round(safe_float(r['night_ratio']), 2)} | label={r.get('label')} | sub_label={r.get('sub_label')}")

    lines.append("")
    lines.append("## 六、后续建议")
    lines.append("")
    for item in build_followups(mode, summary):
        lines.append(f"- `{item['skill']}`：{item['reason']}")

    lines.append("")
    lines.append("## 七、基础算子对齐")
    lines.append("")
    lines.append("- 时间窗口切片 = `relationship_filter(time window)`")
    lines.append("- 日级 / 小时级聚合 = `aggregation_query`")
    lines.append("- 单号或群体范围限定 = `node_lookup / subgraph_by_nodes`")
    lines.append("- 异常日期排序 = `aggregation_query + scoring_layer`")

    lines.append("")
    lines.append("## 八、生成文件")
    lines.append("")
    for k, p in paths.items():
        if k == 'report_md':
            continue
        lines.append(f"- `{k}`：`{p.name}`")
    return "\n".join(lines) + "\n"


def write_excel(path: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            out = df if not df.empty else pd.DataFrame({"note": ["no rows"]})
            out.to_excel(writer, sheet_name=sheet_name[:31], index=False)




def json_ready(value: Any) -> Any:
    """Convert pandas/numpy objects to plain JSON-serializable Python objects."""
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="records")
    if isinstance(value, pd.Series):
        return value.to_dict()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [json_ready(v) for v in value]
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value

def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Analyze phone-level or group-level time-series anomalies.")
    p.add_argument("--mode", choices=["phone", "group"], default="phone")
    p.add_argument("--phone-id", default=None)
    p.add_argument("--phone-ids", default=None)
    p.add_argument("--phone-id-file", default=None)
    p.add_argument("--dataset-root", default=None)
    p.add_argument("--dataset", default="unified")
    p.add_argument("--recent-days", type=int, default=7)
    p.add_argument("--baseline-days", type=int, default=30)
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--evidence-limit", type=int, default=100)
    p.add_argument("--night-start-hour", type=int, default=22)
    p.add_argument("--night-end-hour", type=int, default=6)
    return p


def main() -> None:
    args = build_argparser().parse_args()
    ids = load_scope_ids(args)
    if args.mode == "phone":
        if len(ids) != 1:
            raise SystemExit("phone 模式下必须提供且仅提供一个号码（--phone-id）。")
    else:
        if len(ids) < 2:
            raise SystemExit("group 模式下至少需要 2 个号码（--phone-ids 或 --phone-id-file）。")

    dataset_root = find_dataset_root(args.dataset_root)
    paths = resolve_paths(dataset_root, args.dataset)
    conn = connect(paths)
    setup = setup_views(conn)

    if not setup.get("time_col"):
        status = "time_evidence_unavailable"
    else:
        status = "ok"

    outdir = ensure_output_dir()
    prefix = f"time_series_anomaly_{preview(ids[0], 8).replace('...', '')}_{args.mode}_{args.dataset}"
    report_md = outdir / f"{prefix}.md"
    daily_csv = outdir / f"{prefix}_daily.csv"
    anomalies_csv = outdir / f"{prefix}_anomaly_days.csv"
    hourly_csv = outdir / f"{prefix}_hourly.csv"
    contributors_csv = outdir / f"{prefix}_contributors.csv"
    summary_json = outdir / f"{prefix}_summary.json"
    evidence_xlsx = outdir / f"{prefix}_evidence.xlsx"

    if args.mode == "phone":
        profile = get_profile(conn, ids[0])
        if not profile.get("node_found"):
            summary = {
                "status": "target_not_found",
                "notes": ["当前数据集中未找到该号码。"],
                "stage": "unknown",
                "bounds": None,
                "recent": {},
                "baseline": {},
                "changes": {},
                "anomaly_days": pd.DataFrame(),
                "top_signal_summary": ["当前目标号码不存在，无法形成时间序列证据。"],
            }
            ts = {"daily": pd.DataFrame(), "hourly": pd.DataFrame(), "contributors": pd.DataFrame()}
        elif status != "ok":
            ts = {"daily": pd.DataFrame(), "hourly": pd.DataFrame(), "contributors": pd.DataFrame()}
            summary = {
                "status": "time_evidence_unavailable",
                "notes": ["当前通话边表中未检测到可解析时间列，无法做日期级趋势分析。"],
                "stage": "unknown",
                "bounds": None,
                "recent": {},
                "baseline": {},
                "changes": {},
                "anomaly_days": pd.DataFrame(),
                "top_signal_summary": ["当前数据缺少可解析时间列。"],
            }
        else:
            ts = get_phone_timeseries(conn, ids[0], args.evidence_limit, args.night_start_hour, args.night_end_hour)
            summary = build_phone_summary(profile, ts, args.recent_days, args.baseline_days, args.top_k)
        target_desc = ids[0]
    else:
        profile = {}
        if status != "ok":
            ts = {"daily": pd.DataFrame(), "hourly": pd.DataFrame(), "contributors": pd.DataFrame()}
            summary = {
                "status": "time_evidence_unavailable",
                "notes": ["当前通话边表中未检测到可解析时间列，无法做群体时间趋势分析。"],
                "stage": "unknown",
                "bounds": None,
                "recent": {},
                "baseline": {},
                "changes": {},
                "anomaly_days": pd.DataFrame(),
                "top_signal_summary": ["当前数据缺少可解析时间列。"],
            }
        else:
            ts = get_group_timeseries(conn, ids, args.evidence_limit, args.night_start_hour, args.night_end_hour)
            summary = build_group_summary(ts, args.recent_days, args.baseline_days, args.top_k)
        target_desc = f"{len(ids)}个号码"

    evidence_ok = summary.get("status") == "ok"
    if evidence_ok:
        ts["daily"].head(args.evidence_limit).to_csv(daily_csv, index=False)
        summary["anomaly_days"].head(args.evidence_limit).to_csv(anomalies_csv, index=False)
        ts["hourly"].head(args.evidence_limit).to_csv(hourly_csv, index=False)
        ts["contributors"].head(args.evidence_limit).to_csv(contributors_csv, index=False)
        output_paths = {
            "report_md": report_md,
            "daily_csv": daily_csv,
            "anomaly_days_csv": anomalies_csv,
            "hourly_csv": hourly_csv,
            "contributors_csv": contributors_csv,
            "summary_json": summary_json,
            "evidence_xlsx": evidence_xlsx,
        }
    else:
        output_paths = {
            "report_md": report_md,
            "summary_json": summary_json,
        }

    report_text = render_markdown(
        mode=args.mode,
        target_desc=target_desc,
        dataset=args.dataset,
        profile=profile,
        ids=ids,
        ts=ts,
        summary=summary,
        paths=output_paths,
        top_k=args.top_k,
    )
    report_md.write_text(report_text, encoding="utf-8")

    if evidence_ok:
        write_excel(evidence_xlsx, {
            "summary": pd.DataFrame([{
                "mode": args.mode,
                "status": summary.get("status"),
                "dataset": args.dataset,
                "recent_days": args.recent_days,
                "baseline_days": args.baseline_days,
                "stage": summary.get("stage"),
                "target_desc": target_desc,
                "recent_active_days": summary.get("data_coverage", {}).get("recent_active_days"),
                "baseline_active_days": summary.get("data_coverage", {}).get("baseline_active_days"),
            }]),
            "daily": ts["daily"].head(args.evidence_limit),
            "anomaly_days": summary["anomaly_days"].head(args.evidence_limit),
            "hourly": ts["hourly"].head(args.evidence_limit),
            "contributors": ts["contributors"].head(args.evidence_limit),
        })

    result = {
        "ok": True,
        "skill": "time-series-anomaly-analysis",
        "query_type": "time_series_anomaly",
        "script_version": SCRIPT_VERSION,
        "mode": args.mode,
        "dataset": args.dataset,
        "status": summary.get("status"),
        "scope_size": len(ids),
        "target": ids[0] if args.mode == "phone" else None,
        "stage": summary.get("stage"),
        "notes": summary.get("notes", []),
        "time_column": setup.get("time_col"),
        "hour_column": setup.get("hour_col"),
        "recent_window": summary.get("bounds", {}).get("recent_start") if summary.get("bounds") else None,
        "baseline_window": summary.get("bounds", {}).get("baseline_start") if summary.get("bounds") else None,
        "recent_metrics": summary.get("recent", {}),
        "baseline_metrics": summary.get("baseline", {}),
        "changes": summary.get("changes", {}),
        "data_coverage": summary.get("data_coverage", {}),
        "top_signal_summary": summary.get("top_signal_summary", []),
        "next_step_suggestions": [x["skill"] for x in build_followups(args.mode, summary)],
        "base_operator_alignment": {
            "time_window_filter": "relationship_filter(time window)",
            "daily_hourly_aggregation": "aggregation_query",
            "scope_limiting": "node_lookup / subgraph_by_nodes",
            "anomaly_scoring": "aggregation_query + scoring_layer",
        },
        "artifacts": [
            {"type": ("markdown_report" if k == "report_md" else ("xlsx" if str(p).endswith(".xlsx") else ("json" if str(p).endswith(".json") else "csv"))), "path": str(p), "title": p.name}
            for k, p in output_paths.items()
        ],
        "report_path": str(report_md),
    }
    result = json_ready(result)
    summary_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
