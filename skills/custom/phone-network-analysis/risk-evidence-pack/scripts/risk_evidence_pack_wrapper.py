#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd

SCRIPT_VERSION = "risk-evidence-pack-release-v3"

DEFAULT_DATASET_ROOT_CANDIDATES = [
    os.environ.get("PHONE_NETWORK_DATASET_ROOT", ""),
    "/mnt/datasets/phone-network",
    "/workspace/imiss-deer-flow-main/datasets/phone-network",
    "/workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/condition-based-screening/scripts/condition_based_screening_testdata/datasets/phone-network",
]

RISK_SUBLABELS = {"risk", "purefraud", "mutation"}


def _safe_float(v, default=0.0):
    try:
        if v is None or pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v, default=0):
    try:
        if v is None or pd.isna(v):
            return default
        return int(v)
    except Exception:
        return default


def preview(x: Optional[str], n: int = 12) -> str:
    if not x:
        return ""
    x = str(x)
    return x if len(x) <= n else f"{x[:n]}..."


def compress_preview_string(value: object, max_items: int = 5) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    parts = [p.strip() for p in s.split(',') if p.strip()]
    if len(parts) <= max_items:
        return ', '.join(parts)
    return ', '.join(parts[:max_items]) + f" 等{len(parts) - max_items}项"


def find_dataset_root(explicit: Optional[str]) -> str:
    if explicit:
        root = Path(explicit)
        if root.exists():
            return str(root)
        raise FileNotFoundError(f"dataset root not found: {explicit}")
    for candidate in DEFAULT_DATASET_ROOT_CANDIDATES:
        if candidate and Path(candidate).exists():
            return candidate
    raise FileNotFoundError("No dataset root found. Please pass --dataset-root or set PHONE_NETWORK_DATASET_ROOT.")


def resolve_data_paths(dataset_root: str, dataset: str) -> Dict[str, str]:
    root = Path(dataset_root)
    user_nodes = root / 'processed' / dataset / 'user_nodes.csv'
    call_edges = root / 'processed' / dataset / 'call_edges.csv'
    parquet_device = root / 'processed' / 'graph_views' / dataset / 'edges_phone_imei.parquet'
    csv_device = root / 'processed' / 'graph_views' / dataset / 'edges_phone_imei.csv'
    if parquet_device.exists():
        device_edges = parquet_device
    elif csv_device.exists():
        device_edges = csv_device
    else:
        raise FileNotFoundError(f"device edges not found under {parquet_device} or {csv_device}")
    for p in (user_nodes, call_edges):
        if not p.exists():
            raise FileNotFoundError(f"required file not found: {p}")
    return {
        'dataset_root': str(root),
        'user_nodes': str(user_nodes),
        'call_edges': str(call_edges),
        'device_edges': str(device_edges),
    }


def output_dir() -> Path:
    for c in (Path('/mnt/user-data/outputs'), Path('/workspace/imiss-deer-flow-main/outputs')):
        try:
            c.mkdir(parents=True, exist_ok=True)
            return c
        except Exception:
            pass
    local = Path(__file__).resolve().parent.parent / 'outputs'
    local.mkdir(parents=True, exist_ok=True)
    return local


def output_basename(phone_id: str, dataset: str) -> str:
    return f"risk_evidence_pack_{preview(phone_id, 8).replace('...', '')}_{dataset}"


def connect_and_register(paths: Dict[str, str]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(database=':memory:')
    conn.execute('PRAGMA threads=4')
    conn.execute(f"CREATE VIEW user_nodes AS SELECT * FROM read_csv_auto('{paths['user_nodes']}', HEADER=TRUE)")
    conn.execute(f"CREATE VIEW call_edges AS SELECT * FROM read_csv_auto('{paths['call_edges']}', HEADER=TRUE)")
    if paths['device_edges'].endswith('.parquet'):
        conn.execute(f"CREATE VIEW device_edges AS SELECT * FROM read_parquet('{paths['device_edges']}')")
    else:
        conn.execute(f"CREATE VIEW device_edges AS SELECT * FROM read_csv_auto('{paths['device_edges']}', HEADER=TRUE)")
    return conn


def detect_columns(conn: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]


def choose_first(existing: List[str], candidates: List[str]) -> Optional[str]:
    existing_lower = {c.lower(): c for c in existing}
    for c in candidates:
        if c.lower() in existing_lower:
            return existing_lower[c.lower()]
    return None


def setup_views(conn: duckdb.DuckDBPyConnection) -> Dict[str, Optional[str]]:
    user_cols = detect_columns(conn, 'user_nodes')
    call_cols = detect_columns(conn, 'call_edges')
    dev_cols = detect_columns(conn, 'device_edges')

    user_id_col = choose_first(user_cols, ['user_id', 'id', 'phone_id'])
    label_col = choose_first(user_cols, ['label'])
    sub_label_col = choose_first(user_cols, ['sub_label', 'risk_sub_label'])
    province_col = choose_first(user_cols, ['province'])

    src_col = choose_first(call_cols, ['src_user_id', 'src', 'source', 'caller_id', 'user_id'])
    dst_col = choose_first(call_cols, ['dst_counterparty_id', 'dst', 'target', 'callee_id', 'counterparty_id'])
    weight_col = choose_first(call_cols, ['call_count', 'cnt', 'weight', 'times', 'freq', 'count'])
    duration_col = choose_first(call_cols, ['duration', 'duration_sec', 'call_duration', 'duration_seconds'])
    hour_col = choose_first(call_cols, ['call_hour', 'hour', 'start_hour'])

    dev_user_col = choose_first(dev_cols, ['user_id', 'phone_id', 'src_user_id'])
    dev_imei_col = choose_first(dev_cols, ['imei', 'device_id'])

    missing = [
        name for name, value in {
            'user_id_col': user_id_col,
            'src_col': src_col,
            'dst_col': dst_col,
            'dev_user_col': dev_user_col,
            'dev_imei_col': dev_imei_col,
        }.items() if not value
    ]
    if missing:
        raise RuntimeError(f"failed to detect required columns: {missing}")

    conn.execute(f"""
        CREATE OR REPLACE VIEW call_edges_std AS
        SELECT
            CAST({src_col} AS VARCHAR) AS src_user_id,
            CAST({dst_col} AS VARCHAR) AS dst_counterparty_id,
            {weight_col if weight_col else '1'} AS edge_weight,
            {duration_col if duration_col else 'NULL'} AS duration_value,
            {hour_col if hour_col else 'NULL'} AS hour_value
        FROM call_edges
        WHERE {src_col} IS NOT NULL AND {dst_col} IS NOT NULL
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW undirected_contact AS
        SELECT CAST(src_user_id AS VARCHAR) AS user_id,
               CAST(dst_counterparty_id AS VARCHAR) AS counterparty_id,
               CAST(edge_weight AS DOUBLE) AS edge_weight,
               duration_value,
               hour_value
        FROM call_edges_std
        UNION ALL
        SELECT CAST(dst_counterparty_id AS VARCHAR) AS user_id,
               CAST(src_user_id AS VARCHAR) AS counterparty_id,
               CAST(edge_weight AS DOUBLE) AS edge_weight,
               duration_value,
               hour_value
        FROM call_edges_std
    """)

    conn.execute(f"""
        CREATE OR REPLACE VIEW device_edges_std AS
        SELECT
            CAST({dev_user_col} AS VARCHAR) AS user_id,
            CAST({dev_imei_col} AS VARCHAR) AS imei
        FROM device_edges
        WHERE {dev_user_col} IS NOT NULL AND {dev_imei_col} IS NOT NULL
    """)

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

    return {
        'label_col': label_col,
        'sub_label_col': sub_label_col,
        'province_col': province_col,
        'hour_col': hour_col,
        'duration_col': duration_col,
        'weight_col': weight_col,
    }


def get_phone_profile(conn: duckdb.DuckDBPyConnection, phone_id: str) -> Dict:
    df = conn.execute('SELECT * FROM user_nodes_std WHERE user_id = ? LIMIT 1', [phone_id]).df()
    if df.empty:
        return {'phone_id': phone_id, 'node_found': False}
    row = df.iloc[0].to_dict()
    return {
        'phone_id': phone_id,
        'node_found': True,
        'label': _safe_int(row.get('label'), None),
        'sub_label': row.get('sub_label'),
        'province': row.get('province'),
        'raw_node_attrs': row,
    }


def get_call_metrics(conn: duckdb.DuckDBPyConnection, phone_id: str) -> Dict:
    metrics = conn.execute(
        """
        SELECT
            COUNT(*) AS call_record_count,
            COUNT(DISTINCT counterparty_id) AS counterparty_count,
            COALESCE(SUM(edge_weight), 0) AS weighted_call_sum,
            COALESCE(SUM(CASE WHEN CAST(hour_value AS DOUBLE) >= 22 OR CAST(hour_value AS DOUBLE) < 6 THEN 1 ELSE 0 END), 0) AS night_record_count,
            COALESCE(AVG(CASE WHEN CAST(hour_value AS DOUBLE) >= 22 OR CAST(hour_value AS DOUBLE) < 6 THEN 1.0 ELSE 0.0 END), NULL) AS night_ratio,
            COALESCE(SUM(duration_value), NULL) AS total_duration
        FROM undirected_contact
        WHERE user_id = ?
        """,
        [phone_id],
    ).df().iloc[0].to_dict()

    top_counterparties = conn.execute(
        """
        SELECT
            counterparty_id,
            COUNT(*) AS record_count,
            COALESCE(SUM(edge_weight), 0) AS weighted_count,
            COALESCE(SUM(duration_value), 0) AS total_duration
        FROM undirected_contact
        WHERE user_id = ?
        GROUP BY 1
        ORDER BY weighted_count DESC, record_count DESC, counterparty_id ASC
        LIMIT 50
        """,
        [phone_id],
    ).df()
    if not top_counterparties.empty:
        top_counterparties['counterparty_preview'] = top_counterparties['counterparty_id'].map(preview)
    return {
        **{k: (_safe_float(v) if isinstance(v, (float, int)) or v is None else v) for k, v in metrics.items()},
        'top_counterparties': top_counterparties,
    }


def get_shared_device_metrics(conn: duckdb.DuckDBPyConnection, phone_id: str) -> Dict:
    phone_devices = conn.execute('SELECT DISTINCT imei FROM device_edges_std WHERE user_id = ? ORDER BY imei', [phone_id]).df()
    device_count = len(phone_devices)

    top_devices = conn.execute(
        """
        WITH phone_devices AS (
            SELECT DISTINCT imei FROM device_edges_std WHERE user_id = ?
        )
        SELECT
            d.imei,
            COUNT(DISTINCT d.user_id) - 1 AS shared_phone_count,
            SUM(CASE WHEN u.label = 1 OR LOWER(COALESCE(CAST(u.sub_label AS VARCHAR), '')) IN ('risk','purefraud','mutation') THEN 1 ELSE 0 END)
                - MAX(CASE WHEN d.user_id = ? AND (u.label = 1 OR LOWER(COALESCE(CAST(u.sub_label AS VARCHAR), '')) IN ('risk','purefraud','mutation')) THEN 1 ELSE 0 END) AS risk_peer_count,
            STRING_AGG(SUBSTRING(d.user_id, 1, 12) || '...', ', ' ORDER BY d.user_id) AS peer_preview
        FROM device_edges_std d
        JOIN phone_devices pd ON d.imei = pd.imei
        LEFT JOIN user_nodes_std u ON d.user_id = u.user_id
        GROUP BY d.imei
        ORDER BY shared_phone_count DESC, risk_peer_count DESC, d.imei ASC
        LIMIT 50
        """,
        [phone_id, phone_id],
    ).df()
    if not top_devices.empty:
        top_devices['device_preview'] = top_devices['imei'].map(preview)
        top_devices['peer_preview'] = top_devices['peer_preview'].map(lambda x: compress_preview_string(x, 6))

    top_shared_peers = conn.execute(
        """
        WITH phone_devices AS (
            SELECT DISTINCT imei FROM device_edges_std WHERE user_id = ?
        ), peer_device_rows AS (
            SELECT DISTINCT
                d.user_id AS peer_id,
                d.imei AS imei,
                u.label AS peer_label,
                u.sub_label AS peer_sub_label,
                CASE WHEN u.label = 1 OR LOWER(COALESCE(CAST(u.sub_label AS VARCHAR), '')) IN ('risk','purefraud','mutation') THEN 1 ELSE 0 END AS peer_is_risk
            FROM device_edges_std d
            JOIN phone_devices pd ON d.imei = pd.imei
            LEFT JOIN user_nodes_std u ON d.user_id = u.user_id
            WHERE d.user_id <> ?
        )
        SELECT
            peer_id,
            COUNT(imei) AS shared_device_count,
            STRING_AGG(SUBSTRING(imei, 1, 12) || '...', ', ' ORDER BY imei) AS shared_devices_preview,
            MAX(peer_is_risk) AS peer_is_risk,
            MAX(peer_label) AS peer_label,
            MAX(peer_sub_label) AS peer_sub_label
        FROM peer_device_rows
        GROUP BY peer_id
        ORDER BY shared_device_count DESC, peer_is_risk DESC, peer_id ASC
        LIMIT 100
        """,
        [phone_id, phone_id],
    ).df()
    if not top_shared_peers.empty:
        top_shared_peers['peer_preview'] = top_shared_peers['peer_id'].map(preview)
        top_shared_peers['shared_devices_preview'] = top_shared_peers['shared_devices_preview'].map(lambda x: compress_preview_string(x, 5))

    shared_peer_total = _safe_int(top_shared_peers['peer_id'].nunique() if not top_shared_peers.empty else 0)
    shared_device_count = _safe_int((top_devices['shared_phone_count'] > 0).sum() if not top_devices.empty else 0)
    return {
        'device_count': device_count,
        'shared_device_count': shared_device_count,
        'shared_peer_total': shared_peer_total,
        'top_devices': top_devices,
        'top_shared_peers': top_shared_peers,
    }


def get_overlap_metrics(conn: duckdb.DuckDBPyConnection, phone_id: str) -> Dict:
    peer_df = conn.execute(
        """
        WITH phone_cps AS (
            SELECT DISTINCT counterparty_id
            FROM undirected_contact
            WHERE user_id = ?
        ), peer_counterparty_rows AS (
            SELECT DISTINCT
                u.user_id AS peer_id,
                u.counterparty_id AS counterparty_id,
                u.edge_weight AS edge_weight,
                p.label AS peer_label,
                p.sub_label AS peer_sub_label
            FROM undirected_contact u
            JOIN phone_cps pc ON u.counterparty_id = pc.counterparty_id
            LEFT JOIN user_nodes_std p ON u.user_id = p.user_id
            WHERE u.user_id <> ?
        )
        SELECT
            peer_id,
            COUNT(counterparty_id) AS common_counterparty_count,
            COALESCE(SUM(edge_weight), 0) AS overlap_weighted_calls,
            STRING_AGG(SUBSTRING(counterparty_id, 1, 12) || '...', ', ' ORDER BY counterparty_id) AS common_counterparties_preview,
            MAX(peer_label) AS peer_label,
            MAX(peer_sub_label) AS peer_sub_label
        FROM peer_counterparty_rows
        GROUP BY peer_id
        HAVING COUNT(counterparty_id) >= 1
        ORDER BY common_counterparty_count DESC, overlap_weighted_calls DESC, peer_id ASC
        LIMIT 100
        """,
        [phone_id, phone_id],
    ).df()
    if not peer_df.empty:
        peer_df['peer_preview'] = peer_df['peer_id'].map(preview)
        peer_df['common_counterparties_preview'] = peer_df['common_counterparties_preview'].map(lambda x: compress_preview_string(x, 5))

    cp_df = conn.execute(
        """
        WITH phone_cps AS (
            SELECT DISTINCT counterparty_id
            FROM undirected_contact
            WHERE user_id = ?
        ), counterparty_peer_rows AS (
            SELECT DISTINCT
                u.counterparty_id AS counterparty_id,
                u.user_id AS peer_id,
                u.edge_weight AS edge_weight
            FROM undirected_contact u
            JOIN phone_cps pc ON u.counterparty_id = pc.counterparty_id
            WHERE u.user_id <> ?
        )
        SELECT
            counterparty_id,
            COUNT(peer_id) AS touching_peer_count,
            COALESCE(SUM(edge_weight), 0) AS total_weighted_calls,
            STRING_AGG(SUBSTRING(peer_id, 1, 12) || '...', ', ' ORDER BY peer_id) AS peer_preview
        FROM counterparty_peer_rows
        GROUP BY counterparty_id
        ORDER BY touching_peer_count DESC, total_weighted_calls DESC, counterparty_id ASC
        LIMIT 100
        """,
        [phone_id, phone_id],
    ).df()
    if not cp_df.empty:
        cp_df['counterparty_preview'] = cp_df['counterparty_id'].map(preview)
        cp_df['peer_preview'] = cp_df['peer_preview'].map(lambda x: compress_preview_string(x, 5))
        cp_df['hub_like'] = cp_df['touching_peer_count'].map(lambda x: 'yes' if _safe_int(x) >= 50 else 'no')

    return {
        'top_overlap_peers': peer_df,
        'top_common_counterparties': cp_df,
    }


def classify_evidence_strength(risk_score: float) -> str:
    if risk_score >= 75:
        return 'very_high'
    if risk_score >= 55:
        return 'high'
    if risk_score >= 35:
        return 'medium'
    return 'low'


def build_evidence_summary(profile: Dict, call_metrics: Dict, shared_metrics: Dict, overlap_metrics: Dict) -> Dict:
    label = profile.get('label')
    sub_label = str(profile.get('sub_label') or '').lower()
    counterparty_count = _safe_int(call_metrics.get('counterparty_count'))
    call_record_count = _safe_int(call_metrics.get('call_record_count'))
    weighted_sum = _safe_float(call_metrics.get('weighted_call_sum'))
    night_ratio = call_metrics.get('night_ratio')
    night_ratio = None if pd.isna(night_ratio) else _safe_float(night_ratio, None)
    shared_device_count = _safe_int(shared_metrics.get('shared_device_count'))
    shared_peer_total = _safe_int(shared_metrics.get('shared_peer_total'))
    max_common_counterparty = 0
    if not overlap_metrics['top_overlap_peers'].empty:
        max_common_counterparty = _safe_int(overlap_metrics['top_overlap_peers'].iloc[0]['common_counterparty_count'])

    factors: List[str] = []
    score = 0.0
    label_component = 0.0
    if label == 1 or sub_label in RISK_SUBLABELS:
        label_component = 28.0
        score += label_component
        factors.append('画像标签中已出现风险信号')
    elif sub_label == 'whitelist':
        label_component = -10.0
        score += label_component
        factors.append('画像标签为白名单，需谨慎解读')
    else:
        factors.append('未显式标注为风险对象')

    breadth_score = min(20.0, math.log1p(counterparty_count) * 3.2)
    volume_score = min(15.0, math.log1p(max(weighted_sum, call_record_count)) * 1.6)
    device_score = min(20.0, shared_device_count * 2.5 + math.log1p(shared_peer_total) * 2.2)
    overlap_score = min(12.0, math.log1p(max_common_counterparty) * 4.0)
    night_score = 0.0
    if night_ratio is not None:
        night_score = min(10.0, max(0.0, (night_ratio - 0.15) * 20))
        if night_ratio >= 0.35:
            factors.append(f'夜间行为偏高（夜间占比={night_ratio:.2f}）')
    if counterparty_count >= 80:
        factors.append(f'联系人广度较大（{counterparty_count} 个）')
    if shared_device_count >= 1:
        factors.append(f'存在共享设备证据（{shared_device_count} 台）')
    if max_common_counterparty >= 3:
        factors.append(f'与其他号码存在同圈重叠（最大共同对端={max_common_counterparty}）')

    score += breadth_score + volume_score + device_score + overlap_score + night_score
    score = max(0.0, min(100.0, score))

    strongest_device = None
    if not shared_metrics['top_devices'].empty:
        nonzero_devices = shared_metrics['top_devices'][shared_metrics['top_devices']['shared_phone_count'].fillna(0) > 0]
        if not nonzero_devices.empty:
            r = nonzero_devices.iloc[0]
            strongest_device = {
                'device_id': r['imei'],
                'device_preview': preview(r['imei']),
                'shared_phone_count': _safe_int(r['shared_phone_count']),
                'risk_peer_count': _safe_int(r.get('risk_peer_count', 0)),
            }

    strongest_overlap_peer = None
    if not overlap_metrics['top_overlap_peers'].empty:
        r = overlap_metrics['top_overlap_peers'].iloc[0]
        strongest_overlap_peer = {
            'peer_id': r['peer_id'],
            'peer_preview': preview(r['peer_id']),
            'common_counterparty_count': _safe_int(r['common_counterparty_count']),
            'peer_label': _safe_int(r.get('peer_label'), None),
            'peer_sub_label': r.get('peer_sub_label'),
        }

    return {
        'risk_score': round(score, 2),
        'evidence_strength': classify_evidence_strength(score),
        'evidence_factors': factors,
        'component_scores': {
            'label_score': round(label_component, 2),
            'breadth_score': round(breadth_score, 2),
            'volume_score': round(volume_score, 2),
            'device_score': round(device_score, 2),
            'overlap_score': round(overlap_score, 2),
            'night_score': round(night_score, 2),
        },
        'strongest_device': strongest_device,
        'strongest_overlap_peer': strongest_overlap_peer,
    }


def build_followups(profile: Dict, shared_metrics: Dict, overlap_metrics: Dict, status: str) -> List[Dict[str, str]]:
    if status == 'target_not_found':
        return [
            {'skill': 'single-number-analysis', 'reason': '请先确认号码是否真实存在于当前数据集，或检查是否用了正确的数据集。'},
            {'skill': 'condition-based-screening', 'reason': '若你是在找某类对象而不是确定号码，可先从条件筛选或名单发现入手。'},
        ]
    items: List[Dict[str, str]] = []
    if shared_metrics.get('shared_device_count', 0) > 0:
        items.append({'skill': 'shared-device-analysis', 'reason': '已有共享设备证据，建议继续核查同设备下其他号码。'})
    if not overlap_metrics['top_overlap_peers'].empty:
        items.append({'skill': 'overlap-analysis', 'reason': '已发现共同对端重叠，可进一步判断是否处于同一联系圈。'})
        items.append({'skill': 'association-path-analysis', 'reason': '可继续验证该号码与可疑同圈号码之间是否存在路径型间接关系。'})
    items.append({'skill': 'single-number-analysis', 'reason': '可对该号码做更完整的单号深挖与局部关系圈分析。'})
    return items


def rank_counterparties_for_display(df: pd.DataFrame, top_k: int) -> pd.DataFrame:
    if df.empty:
        return df
    hub_mask = df['hub_like'].astype(str).str.lower().eq('yes') if 'hub_like' in df.columns else pd.Series(False, index=df.index)
    non_hub = df[~hub_mask]
    hub = df[hub_mask]
    display_df = pd.concat([non_hub.head(top_k), hub.head(max(0, top_k - len(non_hub.head(top_k))))], ignore_index=True)
    return display_df.head(top_k)


def write_excel(path: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        for sheet_name, df in sheets.items():
            (df if not df.empty else pd.DataFrame({'note': ['no rows']})).to_excel(writer, sheet_name=sheet_name[:31], index=False)


def markdown_report(phone_id: str, dataset: str, profile: Dict, call_metrics: Dict, shared_metrics: Dict, overlap_metrics: Dict, evidence_summary: Dict, paths: Dict[str, Path], top_k: int, status: str, notes: List[str]) -> str:
    province = profile.get('province')
    sub_label = profile.get('sub_label')
    label = profile.get('label')
    strongest_device = evidence_summary.get('strongest_device')
    strongest_overlap_peer = evidence_summary.get('strongest_overlap_peer')
    lines: List[str] = []
    lines.append(f"# 风险证据包：{preview(phone_id, 16)}")
    lines.append('')
    lines.append('## 一、核心结论')
    lines.append('')
    lines.append(f"- 数据集：`{dataset}`")
    lines.append(f"- 目标号码：`{phone_id}`")
    lines.append(f"- 处理状态：`{status}`")
    if status == 'target_not_found':
        lines.append('- 说明：当前数据集中未定位到该号码，无法形成有效证据包。')
        lines.append('- 建议：请先核对号码是否存在于当前数据集，或切换正确的数据集后重试。')
    else:
        lines.append(f"- 风险证据强度：`{evidence_summary['evidence_strength']}`")
        lines.append(f"- 风险证据分：`{evidence_summary['risk_score']}`")
        lines.append(f"- 结论摘要：该号码当前最主要的风险驱动来自 {', '.join(evidence_summary['evidence_factors'][:3]) if evidence_summary['evidence_factors'] else '证据较弱'}。")
        if strongest_device:
            lines.append(f"- 最强共享设备证据：设备 `{strongest_device['device_preview']}` 挂载相关号码 {strongest_device['shared_phone_count']} 个。")
        if strongest_overlap_peer:
            lines.append(f"- 最强同圈证据：与 `{strongest_overlap_peer['peer_preview']}` 最大共同对端数为 {strongest_overlap_peer['common_counterparty_count']}。")
    if notes:
        lines.append(f"- 备注：{'；'.join(notes)}")

    lines.append('')
    lines.append('## 二、号码画像')
    lines.append('')
    lines.append(f"- 省份：`{province}`")
    lines.append(f"- 标签：label=`{label}`，sub_label=`{sub_label}`")
    lines.append(f"- 通话记录数：`{_safe_int(call_metrics.get('call_record_count'))}`")
    lines.append(f"- 联系人广度：`{_safe_int(call_metrics.get('counterparty_count'))}`")
    lines.append(f"- 加权通话量：`{round(_safe_float(call_metrics.get('weighted_call_sum')), 2)}`")
    if call_metrics.get('night_ratio') is not None and not pd.isna(call_metrics.get('night_ratio')):
        lines.append(f"- 夜间占比：`{_safe_float(call_metrics['night_ratio']):.2f}`")
    lines.append(f"- 设备数量：`{_safe_int(shared_metrics.get('device_count'))}`")
    lines.append(f"- 共享设备数量：`{_safe_int(shared_metrics.get('shared_device_count'))}`")
    lines.append(f"- 共享设备牵出号码数：`{_safe_int(shared_metrics.get('shared_peer_total'))}`")

    if status != 'target_not_found':
        lines.append('')
        lines.append('## 三、证据评分拆解')
        lines.append('')
        for k, v in evidence_summary['component_scores'].items():
            lines.append(f"- `{k}`：`{v}`")
        lines.append('')
        lines.append('### 风险因子')
        for item in evidence_summary['evidence_factors']:
            lines.append(f"- {item}")

        lines.append('')
        lines.append('## 四、Top 直接联系对端')
        lines.append('')
        top_counterparties = call_metrics['top_counterparties'].head(top_k)
        if top_counterparties.empty:
            lines.append('- 当前没有直接联系对端证据。')
        else:
            for _, r in top_counterparties.iterrows():
                lines.append(f"- 对端 `{r['counterparty_preview']}` | 记录数={_safe_int(r['record_count'])} | 加权通话量={round(_safe_float(r['weighted_count']), 2)} | 总时长={round(_safe_float(r['total_duration']), 2)}")

        lines.append('')
        lines.append('## 五、Top 共享设备证据')
        lines.append('')
        display_devices = shared_metrics['top_devices']
        if not display_devices.empty:
            display_devices = display_devices[display_devices['shared_phone_count'].fillna(0) > 0]
        if display_devices.empty:
            lines.append('- 当前未发现显著共享设备证据。')
        else:
            for _, r in display_devices.head(top_k).iterrows():
                lines.append(f"- 设备 `{r['device_preview']}` | 命中成员数={_safe_int(r['shared_phone_count'])} | 风险同挂载数={_safe_int(r.get('risk_peer_count', 0))} | 号码示例={r.get('peer_preview', '')}")

        lines.append('')
        lines.append('## 六、Top 共享设备关联号码')
        lines.append('')
        if shared_metrics['top_shared_peers'].empty:
            lines.append('- 当前没有共享设备关联号码。')
        else:
            for _, r in shared_metrics['top_shared_peers'].head(top_k).iterrows():
                lines.append(f"- 号码 `{r['peer_preview']}` | 共享设备数={_safe_int(r['shared_device_count'])} | peer_label={r.get('peer_label')} | peer_sub_label={r.get('peer_sub_label')} | 设备示例={r.get('shared_devices_preview', '')}")

        lines.append('')
        lines.append('## 七、Top 共同对端证据')
        lines.append('')
        display_counterparties = rank_counterparties_for_display(overlap_metrics['top_common_counterparties'], top_k)
        if display_counterparties.empty:
            lines.append('- 当前没有足够显著的共同对端证据。')
        else:
            if not overlap_metrics['top_common_counterparties'].empty and 'hub_like' in overlap_metrics['top_common_counterparties'].columns:
                non_hub_count = int((overlap_metrics['top_common_counterparties']['hub_like'].astype(str).str.lower() != 'yes').sum())
                hub_count = int((overlap_metrics['top_common_counterparties']['hub_like'].astype(str).str.lower() == 'yes').sum())
                lines.append(f'- 说明：正文优先展示更有辨识度的非公共共同对端；高频公共对端共 {hub_count} 个，非公共共同对端共 {non_hub_count} 个。')
            for _, r in display_counterparties.iterrows():
                hub_note = '（高频公共对端，需谨慎解读）' if str(r.get('hub_like')) == 'yes' else ''
                lines.append(f"- 对端 `{r['counterparty_preview']}` | 共接触号码数={_safe_int(r['touching_peer_count'])} | 累计加权通话={round(_safe_float(r['total_weighted_calls']), 2)} | 号码示例={r.get('peer_preview', '')} {hub_note}")

        lines.append('')
        lines.append('## 八、Top 同圈重叠号码')
        lines.append('')
        if overlap_metrics['top_overlap_peers'].empty:
            lines.append('- 当前没有足够显著的同圈重叠号码。')
        else:
            for _, r in overlap_metrics['top_overlap_peers'].head(top_k).iterrows():
                lines.append(f"- 号码 `{r['peer_preview']}` | 最大共同对端数={_safe_int(r['common_counterparty_count'])} | 重叠通话量={round(_safe_float(r['overlap_weighted_calls']), 2)} | peer_label={r.get('peer_label')} | peer_sub_label={r.get('peer_sub_label')} | 证据示例={r.get('common_counterparties_preview', '')}")

    lines.append('')
    lines.append('## 九、后续建议')
    lines.append('')
    for item in build_followups(profile, shared_metrics, overlap_metrics, status):
        lines.append(f"- `{item['skill']}`：{item['reason']}")

    lines.append('')
    lines.append('## 十、基础算子对齐')
    lines.append('')
    lines.append('- 号码画像 = `node_lookup`')
    lines.append('- 直接联系对端与通话广度 = `neighbor_query + aggregation_query`')
    lines.append('- 共享设备证据 = `query_shared_device + aggregation_query`')
    lines.append('- 同圈重叠证据 = `common_neighbor + aggregation_query`')
    lines.append('- 证据评分与打包 = `aggregation_query + scoring_layer`')

    lines.append('')
    lines.append('## 十一、生成文件')
    lines.append('')
    for name, path in paths.items():
        if name == 'report_md':
            continue
        lines.append(f"- `{name}`：`{path.name}`")
    return '\n'.join(lines) + '\n'


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='Build a structured risk evidence pack for one phone number.')
    p.add_argument('--phone-id', required=True)
    p.add_argument('--dataset-root', default=None)
    p.add_argument('--dataset', default='unified')
    p.add_argument('--top-k', type=int, default=10)
    p.add_argument('--evidence-limit', type=int, default=50)
    p.add_argument('--artifact-mode', choices=['full', 'essential', 'markdown_only'], default='full')
    return p


def main() -> None:
    args = build_argparser().parse_args()
    dataset_root = find_dataset_root(args.dataset_root)
    paths = resolve_data_paths(dataset_root, args.dataset)
    conn = connect_and_register(paths)
    setup_views(conn)

    phone_id = args.phone_id.strip()
    profile = get_phone_profile(conn, phone_id)
    notes: List[str] = []
    status = 'ok'

    if profile.get('node_found'):
        call_metrics = get_call_metrics(conn, phone_id)
        shared_metrics = get_shared_device_metrics(conn, phone_id)
        overlap_metrics = get_overlap_metrics(conn, phone_id)
    else:
        status = 'target_not_found'
        notes.append('当前数据集中未找到该号码，因此未生成有效证据。')
        call_metrics = {
            'call_record_count': 0.0,
            'counterparty_count': 0.0,
            'weighted_call_sum': 0.0,
            'night_record_count': 0.0,
            'night_ratio': 0.0,
            'total_duration': 0.0,
            'top_counterparties': pd.DataFrame(columns=['counterparty_id', 'record_count', 'weighted_count', 'total_duration', 'counterparty_preview'])
        }
        shared_metrics = {
            'device_count': 0,
            'shared_device_count': 0,
            'shared_peer_total': 0,
            'top_devices': pd.DataFrame(columns=['imei', 'shared_phone_count', 'risk_peer_count', 'peer_preview', 'device_preview']),
            'top_shared_peers': pd.DataFrame(columns=['peer_id', 'shared_device_count', 'shared_devices_preview', 'peer_is_risk', 'peer_label', 'peer_sub_label', 'peer_preview'])
        }
        overlap_metrics = {
            'top_overlap_peers': pd.DataFrame(columns=['peer_id', 'common_counterparty_count', 'overlap_weighted_calls', 'common_counterparties_preview', 'peer_label', 'peer_sub_label', 'peer_preview']),
            'top_common_counterparties': pd.DataFrame(columns=['counterparty_id', 'touching_peer_count', 'total_weighted_calls', 'peer_preview', 'counterparty_preview', 'hub_like'])
        }

    evidence_summary = build_evidence_summary(profile, call_metrics, shared_metrics, overlap_metrics)

    base = output_basename(phone_id, args.dataset)
    outdir = output_dir()
    report_md = outdir / f'{base}.md'
    devices_csv = outdir / f'{base}_devices.csv'
    shared_peers_csv = outdir / f'{base}_shared_peers.csv'
    counterparties_csv = outdir / f'{base}_counterparties.csv'
    overlap_peers_csv = outdir / f'{base}_overlap_peers.csv'
    direct_counterparties_csv = outdir / f'{base}_direct_counterparties.csv'
    summary_json = outdir / f'{base}_summary.json'
    evidence_xlsx = outdir / f'{base}_evidence.xlsx'

    if status != 'target_not_found':
        shared_metrics['top_devices'].head(args.evidence_limit).to_csv(devices_csv, index=False)
        shared_metrics['top_shared_peers'].head(args.evidence_limit).to_csv(shared_peers_csv, index=False)
        overlap_metrics['top_common_counterparties'].head(args.evidence_limit).to_csv(counterparties_csv, index=False)
        overlap_metrics['top_overlap_peers'].head(args.evidence_limit).to_csv(overlap_peers_csv, index=False)
        call_metrics['top_counterparties'].head(args.evidence_limit).to_csv(direct_counterparties_csv, index=False)

    report_text = markdown_report(
        phone_id=phone_id,
        dataset=args.dataset,
        profile=profile,
        call_metrics=call_metrics,
        shared_metrics=shared_metrics,
        overlap_metrics=overlap_metrics,
        evidence_summary=evidence_summary,
        paths={
            'report_md': report_md,
            'devices_csv': devices_csv,
            'shared_peers_csv': shared_peers_csv,
            'counterparts_csv': counterparties_csv,
            'overlap_peers_csv': overlap_peers_csv,
            'direct_counterparties_csv': direct_counterparties_csv,
            'summary_json': summary_json,
            'evidence_xlsx': evidence_xlsx,
        },
        top_k=args.top_k,
        status=status,
        notes=notes,
    )
    report_md.write_text(report_text, encoding='utf-8')

    followups = build_followups(profile, shared_metrics, overlap_metrics, status)
    summary = {
        'ok': True,
        'status': status,
        'skill': 'risk-evidence-pack',
        'query_type': 'evidence_pack',
        'script_version': SCRIPT_VERSION,
        'phone_id': phone_id,
        'dataset': args.dataset,
        'artifact_mode': args.artifact_mode,
        'notes': notes,
        'profile': {
            'node_found': profile.get('node_found', False),
            'province': profile.get('province'),
            'label': profile.get('label'),
            'sub_label': profile.get('sub_label'),
        },
        'call_metrics': {k: call_metrics.get(k) for k in ['call_record_count', 'counterparty_count', 'weighted_call_sum', 'night_record_count', 'night_ratio', 'total_duration']},
        'shared_device_metrics': {
            'device_count': shared_metrics.get('device_count'),
            'shared_device_count': shared_metrics.get('shared_device_count'),
            'shared_peer_total': shared_metrics.get('shared_peer_total'),
        },
        'overlap_metrics': {
            'overlap_peer_count': int(overlap_metrics['top_overlap_peers']['peer_id'].nunique()) if not overlap_metrics['top_overlap_peers'].empty else 0,
            'common_counterparty_evidence_count': int(overlap_metrics['top_common_counterparties']['counterparty_id'].nunique()) if not overlap_metrics['top_common_counterparties'].empty else 0,
        },
        'evidence_summary': evidence_summary,
        'top_signal_summary': [
            f"风险证据分 {evidence_summary['risk_score']}，等级 {evidence_summary['evidence_strength']}。",
            f"联系人广度 {_safe_int(call_metrics.get('counterparty_count'))}，加权通话量 {round(_safe_float(call_metrics.get('weighted_call_sum')), 2)}。",
            f"共享设备 {shared_metrics.get('shared_device_count', 0)} 台，牵出 {shared_metrics.get('shared_peer_total', 0)} 个关联号码。",
        ],
        'next_step_suggestions': [x['skill'] for x in followups],
        'base_operator_alignment': {
            'profile_lookup': 'node_lookup',
            'neighbor_evidence': 'neighbor_query + aggregation_query',
            'shared_device_evidence': 'query_shared_device + aggregation_query',
            'overlap_evidence': 'common_neighbor + aggregation_query',
            'evidence_pack_scoring': 'aggregation_query + scoring_layer',
        },
        'artifacts': (
            [
                {'type': 'markdown_report', 'path': str(report_md), 'title': report_md.name},
            ]
            if args.artifact_mode == 'markdown_only'
            else (
                [
                    {'type': 'markdown_report', 'path': str(report_md), 'title': report_md.name},
                    {'type': 'json', 'path': str(summary_json), 'title': summary_json.name},
                ]
                if status == 'target_not_found'
                else (
                    [
                        {'type': 'markdown_report', 'path': str(report_md), 'title': report_md.name},
                        {'type': 'xlsx', 'path': str(evidence_xlsx), 'title': evidence_xlsx.name},
                        {'type': 'json', 'path': str(summary_json), 'title': summary_json.name},
                    ]
                    if args.artifact_mode == 'essential'
                    else [
                        {'type': 'markdown_report', 'path': str(report_md), 'title': report_md.name},
                        {'type': 'csv', 'path': str(devices_csv), 'title': devices_csv.name},
                        {'type': 'csv', 'path': str(shared_peers_csv), 'title': shared_peers_csv.name},
                        {'type': 'csv', 'path': str(counterparties_csv), 'title': counterparties_csv.name},
                        {'type': 'csv', 'path': str(overlap_peers_csv), 'title': overlap_peers_csv.name},
                        {'type': 'csv', 'path': str(direct_counterparties_csv), 'title': direct_counterparties_csv.name},
                        {'type': 'xlsx', 'path': str(evidence_xlsx), 'title': evidence_xlsx.name},
                        {'type': 'json', 'path': str(summary_json), 'title': summary_json.name},
                    ]
                )
            )
        ),
        'report_path': str(report_md),
    }

    if status != 'target_not_found':
        write_excel(evidence_xlsx, {
            'summary': pd.DataFrame([{
                'phone_id': phone_id,
                'status': status,
                'province': profile.get('province'),
                'label': profile.get('label'),
                'sub_label': profile.get('sub_label'),
                'risk_score': evidence_summary['risk_score'],
                'evidence_strength': evidence_summary['evidence_strength'],
                'call_record_count': call_metrics.get('call_record_count'),
                'counterparty_count': call_metrics.get('counterparty_count'),
                'weighted_call_sum': call_metrics.get('weighted_call_sum'),
                'device_count': shared_metrics.get('device_count'),
                'shared_device_count': shared_metrics.get('shared_device_count'),
                'shared_peer_total': shared_metrics.get('shared_peer_total'),
            }]),
            'direct_counterparties': call_metrics['top_counterparties'].head(args.evidence_limit),
            'devices': shared_metrics['top_devices'].head(args.evidence_limit),
            'shared_peers': shared_metrics['top_shared_peers'].head(args.evidence_limit),
            'counterparts': overlap_metrics['top_common_counterparties'].head(args.evidence_limit),
            'overlap_peers': overlap_metrics['top_overlap_peers'].head(args.evidence_limit),
        })
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
