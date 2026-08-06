#!/usr/bin/env python3
"""topn-high-risk-discovery final

升级目标：
1. 报告模板按模式自适应，减少 Q2/Q3 重复榜单。
2. 增强解释层：驱动类型、分数组件 Top3、Top3 证据包、与下一名差异解释。
3. 增强过滤能力：支持 include/exclude sub_label、最小设备数、最小共享牵出数。
4. 保持 artifacts 导出：markdown + csv。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

DEFAULT_ROOT = Path("/workspace/imiss-deer-flow-main")
DEFAULT_USER_NODE_CANDIDATES = [
    DEFAULT_ROOT / "datasets/phone-network/processed/unified/user_nodes.csv",
    Path("/mnt/datasets/phone-network/processed/unified/user_nodes.csv"),
]
DEFAULT_CALL_GRAPH_CANDIDATES = [
    DEFAULT_ROOT / "datasets/phone-network/processed/unified/call_edges.csv",
    Path("/mnt/datasets/phone-network/processed/unified/call_edges.csv"),
]
DEFAULT_DEVICE_GRAPH_CANDIDATES = [
    DEFAULT_ROOT / "datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet",
    Path("/mnt/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet"),
]
DEFAULT_OUTPUT_DIR_CANDIDATES = [
    Path("/mnt/user-data/outputs"),
    DEFAULT_ROOT / "outputs",
]
DEFAULT_DATASET_ROOT_CANDIDATES = [
    DEFAULT_ROOT / "datasets/phone-network",
    Path("/mnt/datasets/phone-network"),
    Path("/workspace/imiss-deer-flow-main/datasets/phone-network"),
]

RISK_SUBLABEL_HINTS = {"risk", "purefraud", "fraud", "suspect", "spam", "black"}
BASE_COMPONENT_NAMES = [
    "label_risk_flag",
    "call_record_score",
    "counterparty_score",
    "device_count_score",
    "shared_device_score",
    "shared_peer_score",
]
RAW_TO_PCT_FIELDS = {
    "call_record_count": "call_record_pct",
    "counterparty_count": "counterparty_pct",
    "device_count": "device_count_pct",
    "shared_device_count": "shared_device_pct",
    "shared_device_peer_total": "shared_peer_pct",
}
COMPONENT_LABELS = {
    "label_risk_flag": "标签风险信号",
    "call_record_score": "通话活跃度",
    "counterparty_score": "联系人广度",
    "device_count_score": "设备规模",
    "shared_device_score": "共享设备强度",
    "shared_peer_score": "设备扩散强度",
}


def short_id(value: Optional[str], n: int = 12) -> str:
    if not value:
        return ""
    text = str(value)
    return text if len(text) <= n else f"{text[:n]}..."


def resolve_first_existing(candidates: Sequence[Path], description: str) -> Path:
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"cannot_resolve_{description}: {[str(x) for x in candidates]}")


def resolve_output_dir() -> Path:
    for path in DEFAULT_OUTPUT_DIR_CANDIDATES:
        try:
            path.mkdir(parents=True, exist_ok=True)
            return path
        except Exception:
            continue
    raise RuntimeError("cannot_resolve_output_dir")


def resolve_dataset_paths(dataset_root: Optional[str], dataset: str) -> Tuple[Path, Path, Path]:
    candidates: List[Path] = []
    if dataset_root:
        candidates.append(Path(dataset_root).expanduser())
    candidates.extend(DEFAULT_DATASET_ROOT_CANDIDATES)

    checked: List[str] = []
    for root in candidates:
        user_node_path = root / "processed" / dataset / "user_nodes.csv"
        call_graph_path = root / "processed" / dataset / "call_edges.csv"
        parquet_device_path = root / "processed" / "graph_views" / dataset / "edges_phone_imei.parquet"
        csv_device_path = root / "processed" / "graph_views" / dataset / "edges_phone_imei.csv"
        device_graph_path = parquet_device_path if parquet_device_path.exists() else csv_device_path
        checked.append(
            f"{root}: user={user_node_path.exists()}, call={call_graph_path.exists()}, device={device_graph_path.exists()}"
        )
        if user_node_path.exists() and call_graph_path.exists() and device_graph_path.exists():
            return user_node_path, call_graph_path, device_graph_path

    raise FileNotFoundError(
        f"cannot_resolve_dataset_paths: dataset={dataset}, checked={checked}"
    )


def safe_read_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"file_not_found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, low_memory=False)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"unsupported_file_type: {path}")


def normalize_phone_columns(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for col in cols:
        if col not in df.columns:
            raise ValueError(f"missing_required_column: {col}")
        df[col] = df[col].astype(str)
    return df


def percentile_score(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=float, index=series.index)
    series = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if float(series.max()) == float(series.min()):
        return pd.Series([0.0] * len(series), index=series.index)
    return series.rank(method="average", pct=True).fillna(0.0)


def score_to_level(score: float) -> str:
    if score >= 75:
        return "very_high"
    if score >= 55:
        return "high"
    if score >= 35:
        return "medium"
    return "attention"


def level_from_percentile(pct: float) -> str:
    if pct >= 0.95:
        return "top_5_percent"
    if pct >= 0.90:
        return "top_10_percent"
    if pct >= 0.75:
        return "top_25_percent"
    if pct >= 0.50:
        return "middle_upper"
    return "long_tail"


def component_field(component_name: str) -> str:
    return f"score_{component_name}_component"


def percentile_human(pct: float) -> str:
    pct = max(0.0, min(1.0, float(pct)))
    top_pct = max(1, int(round((1.0 - pct) * 100)))
    return f"候选池前 {top_pct}%"


def normalize_label_flag(label_value: object, sub_label: object) -> float:
    label_num = pd.to_numeric(pd.Series([label_value]), errors="coerce").fillna(0).iloc[0]
    if float(label_num) >= 1:
        return 1.0
    sub = str(sub_label or "").strip().lower()
    return 1.0 if sub in RISK_SUBLABEL_HINTS else 0.0


def format_pct(value: float) -> str:
    return f"{100.0 * float(value):.1f}%"


def markdown_table(headers: List[str], rows: List[List[object]]) -> List[str]:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(x) for x in row) + " |")
    return lines


def parse_csv_like_text(value: Optional[str]) -> List[str]:
    if not value:
        return []
    parts = [x.strip().lower() for x in str(value).split(",")]
    return [x for x in parts if x]


class TopNRiskDiscovery:
    def __init__(
        self,
        user_node_path: Path,
        call_graph_path: Path,
        device_graph_path: Path,
        source_col: str,
        target_col: str,
        device_source_col: str,
        device_target_col: str,
    ) -> None:
        self.user_node_path = user_node_path
        self.call_graph_path = call_graph_path
        self.device_graph_path = device_graph_path
        self.source_col = source_col
        self.target_col = target_col
        self.device_source_col = device_source_col
        self.device_target_col = device_target_col

        self.user_df = safe_read_table(user_node_path)
        self.call_df = safe_read_table(call_graph_path)
        self.device_df = safe_read_table(device_graph_path)
        self._normalize_inputs()

    def _normalize_inputs(self) -> None:
        self.call_df = normalize_phone_columns(self.call_df, [self.source_col, self.target_col])
        self.device_df = normalize_phone_columns(self.device_df, [self.device_source_col, self.device_target_col])
        if "user_id" not in self.user_df.columns:
            raise ValueError("user_nodes_missing_user_id")
        self.user_df["user_id"] = self.user_df["user_id"].astype(str)
        if "province" not in self.user_df.columns:
            self.user_df["province"] = ""
        if "sub_label" not in self.user_df.columns:
            self.user_df["sub_label"] = ""
        if "label" not in self.user_df.columns:
            self.user_df["label"] = 0.0
        self.user_df["province"] = self.user_df["province"].fillna("").astype(str)
        self.user_df["sub_label"] = self.user_df["sub_label"].fillna("").astype(str)
        self.user_df["label"] = pd.to_numeric(self.user_df["label"], errors="coerce").fillna(0.0)
        self.call_df = self.call_df[[self.source_col, self.target_col]].copy()
        self.device_df = self.device_df[[self.device_source_col, self.device_target_col]].drop_duplicates().copy()

    def build_feature_table(
        self,
        province: Optional[str],
        candidate_scope: str,
        min_call_records: int,
        min_counterparties: int,
        min_shared_device_count: int,
        min_device_count: int,
        min_shared_peer_total: int,
        include_sub_labels: List[str],
        exclude_sub_labels: List[str],
    ) -> Tuple[pd.DataFrame, Dict[str, object]]:
        user_base = self.user_df.copy()
        if province:
            user_base = user_base[user_base["province"].astype(str).str.lower() == province.lower()].copy()
        if include_sub_labels:
            user_base = user_base[user_base["sub_label"].astype(str).str.lower().isin(include_sub_labels)].copy()
        if exclude_sub_labels:
            user_base = user_base[~user_base["sub_label"].astype(str).str.lower().isin(exclude_sub_labels)].copy()

        all_call_records = pd.concat(
            [
                self.call_df[[self.source_col]].rename(columns={self.source_col: "phone_id"}),
                self.call_df[[self.target_col]].rename(columns={self.target_col: "phone_id"}),
            ],
            ignore_index=True,
        )
        call_record_count = all_call_records.groupby("phone_id").size().rename("call_record_count")

        undirected_pairs = self.call_df.rename(columns={self.source_col: "a", self.target_col: "b"}).copy()
        undirected_pairs["u"] = undirected_pairs[["a", "b"]].min(axis=1)
        undirected_pairs["v"] = undirected_pairs[["a", "b"]].max(axis=1)
        undirected_pairs = undirected_pairs[["u", "v"]].drop_duplicates()
        counterparties = pd.concat(
            [
                undirected_pairs[["u", "v"]].rename(columns={"u": "phone_id", "v": "neighbor"}),
                undirected_pairs[["u", "v"]].rename(columns={"v": "phone_id", "u": "neighbor"}),
            ],
            ignore_index=True,
        )
        counterparty_count = counterparties.groupby("phone_id")["neighbor"].nunique().rename("counterparty_count")

        phone_device_count = self.device_df.groupby(self.device_source_col)[self.device_target_col].nunique().rename("device_count")
        device_phone_count = self.device_df.groupby(self.device_target_col)[self.device_source_col].nunique().rename("device_phone_count")
        device_edges = self.device_df.merge(
            device_phone_count.rename("device_phone_count"),
            left_on=self.device_target_col,
            right_index=True,
            how="left",
        )
        device_edges["shared_device_flag"] = (device_edges["device_phone_count"] > 1).astype(int)
        device_edges["shared_peer_incidence"] = (device_edges["device_phone_count"] - 1).clip(lower=0)

        shared_device_count = (
            device_edges[device_edges["shared_device_flag"] == 1]
            .groupby(self.device_source_col)[self.device_target_col]
            .nunique()
            .rename("shared_device_count")
        )
        shared_device_peer_total = device_edges.groupby(self.device_source_col)["shared_peer_incidence"].sum().rename("shared_device_peer_total")

        strongest_device_rows = device_edges.sort_values(
            by=[self.device_source_col, "device_phone_count", self.device_target_col],
            ascending=[True, False, True],
        ).drop_duplicates(subset=[self.device_source_col], keep="first")
        strongest_device_rows = strongest_device_rows.rename(
            columns={
                self.device_source_col: "phone_id",
                self.device_target_col: "strongest_shared_device",
                "device_phone_count": "strongest_shared_device_peer_count",
            }
        )[["phone_id", "strongest_shared_device", "strongest_shared_device_peer_count"]]
        strongest_device_rows["strongest_shared_device_peer_count"] = (
            strongest_device_rows["strongest_shared_device_peer_count"] - 1
        ).clip(lower=0)

        feat = user_base[["user_id"]].rename(columns={"user_id": "phone_id"}).copy()
        for extra_col in [c for c in ["province", "dataset_name", "label", "sub_label"] if c in user_base.columns]:
            feat[extra_col] = user_base.set_index("user_id")[extra_col].reindex(feat["phone_id"]).values

        feat = feat.merge(call_record_count, on="phone_id", how="left")
        feat = feat.merge(counterparty_count, on="phone_id", how="left")
        feat = feat.merge(phone_device_count.rename_axis("phone_id"), on="phone_id", how="left")
        feat = feat.merge(shared_device_count.rename_axis("phone_id"), on="phone_id", how="left")
        feat = feat.merge(shared_device_peer_total.rename_axis("phone_id"), on="phone_id", how="left")
        feat = feat.merge(strongest_device_rows, on="phone_id", how="left")

        numeric_cols = [
            "call_record_count",
            "counterparty_count",
            "device_count",
            "shared_device_count",
            "shared_device_peer_total",
            "strongest_shared_device_peer_count",
        ]
        for col in numeric_cols:
            feat[col] = pd.to_numeric(feat.get(col), errors="coerce").fillna(0)

        feat["label_risk_flag"] = [normalize_label_flag(x, y) for x, y in zip(feat["label"], feat["sub_label"])]
        feat["is_labeled_risk"] = feat["label_risk_flag"] > 0
        feat["is_unlabeled_candidate"] = ~feat["is_labeled_risk"]
        feat["sub_label_norm"] = feat["sub_label"].astype(str).str.lower()

        for raw_col, pct_col in RAW_TO_PCT_FIELDS.items():
            feat[pct_col] = percentile_score(feat[raw_col])

        pool_before_scope = len(feat)
        if candidate_scope == "labeled_only":
            feat = feat[feat["is_labeled_risk"]].copy()
        elif candidate_scope == "unlabeled_only":
            feat = feat[feat["is_unlabeled_candidate"]].copy()

        feat = feat[
            (feat["call_record_count"] >= min_call_records)
            & (feat["counterparty_count"] >= min_counterparties)
            & (feat["shared_device_count"] >= min_shared_device_count)
            & (feat["device_count"] >= min_device_count)
            & (feat["shared_device_peer_total"] >= min_shared_peer_total)
        ].copy()

        meta = {
            "total_phone_count": int(len(self.user_df)),
            "filtered_phone_count_before_scope": int(pool_before_scope),
            "candidate_count_after_filter": int(len(feat)),
            "province_filter": province,
            "candidate_scope": candidate_scope,
            "labeled_risk_phone_count": int((self.user_df["label"].fillna(0) >= 1).sum()),
            "include_sub_labels": include_sub_labels,
            "exclude_sub_labels": exclude_sub_labels,
        }
        return feat, meta


def weights_for_view(analysis_mode: str, ranking_view: str) -> Dict[str, float]:
    if ranking_view == "overall":
        if analysis_mode == "mixed":
            return {
                "label_risk_flag": 0.20,
                "call_record_score": 0.22,
                "counterparty_score": 0.22,
                "device_count_score": 0.08,
                "shared_device_score": 0.15,
                "shared_peer_score": 0.13,
            }
        if analysis_mode == "call_only":
            return {
                "label_risk_flag": 0.25,
                "call_record_score": 0.38,
                "counterparty_score": 0.32,
                "device_count_score": 0.03,
                "shared_device_score": 0.01,
                "shared_peer_score": 0.01,
            }
        return {
            "label_risk_flag": 0.25,
            "call_record_score": 0.00,
            "counterparty_score": 0.00,
            "device_count_score": 0.10,
            "shared_device_score": 0.35,
            "shared_peer_score": 0.30,
        }
    if ranking_view == "unlabeled_only":
        if analysis_mode == "mixed":
            return {
                "label_risk_flag": 0.00,
                "call_record_score": 0.20,
                "counterparty_score": 0.20,
                "device_count_score": 0.10,
                "shared_device_score": 0.25,
                "shared_peer_score": 0.25,
            }
        if analysis_mode == "call_only":
            return {
                "label_risk_flag": 0.00,
                "call_record_score": 0.45,
                "counterparty_score": 0.45,
                "device_count_score": 0.05,
                "shared_device_score": 0.03,
                "shared_peer_score": 0.02,
            }
        return {
            "label_risk_flag": 0.00,
            "call_record_score": 0.02,
            "counterparty_score": 0.03,
            "device_count_score": 0.12,
            "shared_device_score": 0.43,
            "shared_peer_score": 0.40,
        }
    if analysis_mode == "mixed":
        return {
            "label_risk_flag": 0.10,
            "call_record_score": 0.05,
            "counterparty_score": 0.05,
            "device_count_score": 0.15,
            "shared_device_score": 0.35,
            "shared_peer_score": 0.30,
        }
    if analysis_mode == "call_only":
        return {
            "label_risk_flag": 0.10,
            "call_record_score": 0.45,
            "counterparty_score": 0.35,
            "device_count_score": 0.05,
            "shared_device_score": 0.03,
            "shared_peer_score": 0.02,
        }
    return {
        "label_risk_flag": 0.10,
        "call_record_score": 0.00,
        "counterparty_score": 0.00,
        "device_count_score": 0.15,
        "shared_device_score": 0.40,
        "shared_peer_score": 0.35,
    }


def apply_view_scores(df: pd.DataFrame, weights: Dict[str, float], ranking_view: str) -> pd.DataFrame:
    feat = df.copy()
    component_source = {
        "label_risk_flag": feat["label_risk_flag"],
        "call_record_score": feat["call_record_pct"],
        "counterparty_score": feat["counterparty_pct"],
        "device_count_score": feat["device_count_pct"],
        "shared_device_score": feat["shared_device_pct"],
        "shared_peer_score": feat["shared_peer_pct"],
    }
    weighted_sum = pd.Series([0.0] * len(feat), index=feat.index)
    for name, series in component_source.items():
        component_value = 100.0 * float(weights.get(name, 0.0)) * series.fillna(0.0)
        feat[component_field(name)] = component_value.round(6)
        weighted_sum = weighted_sum + component_value
    feat["risk_score"] = weighted_sum.round(6)
    feat["risk_level"] = feat["risk_score"].apply(score_to_level)
    feat["ranking_view"] = ranking_view
    feat["risk_percentile"] = percentile_score(feat["risk_score"]).round(6)
    feat["rank_bucket"] = feat["risk_percentile"].apply(level_from_percentile)
    return feat


def sort_columns_for_view(ranking_view: str) -> List[str]:
    if ranking_view == "device_priority":
        return ["risk_score", "shared_device_peer_total", "shared_device_count", "device_count", "phone_id"]
    return ["risk_score", "counterparty_count", "call_record_count", "shared_device_peer_total", "phone_id"]


def filter_df_for_view(df: pd.DataFrame, ranking_view: str) -> pd.DataFrame:
    feat = df.copy()
    if ranking_view == "unlabeled_only":
        feat = feat[feat["is_unlabeled_candidate"]].copy()
    if ranking_view == "device_priority":
        feat = feat[(feat["shared_device_count"] > 0) | (feat["shared_device_peer_total"] > 0)].copy()
    return feat


def compute_view_top(df: pd.DataFrame, ranking_view: str, top_n: int, analysis_mode: str) -> Tuple[pd.DataFrame, Dict[str, float]]:
    filtered = filter_df_for_view(df, ranking_view)
    weights = weights_for_view(analysis_mode, ranking_view)
    if filtered.empty:
        return filtered.copy(), weights
    scored = apply_view_scores(filtered, weights, ranking_view)
    ascending = [False] * (len(sort_columns_for_view(ranking_view)) - 1) + [True]
    scored = scored.sort_values(by=sort_columns_for_view(ranking_view), ascending=ascending).head(top_n).copy()
    scored["rank"] = range(1, len(scored) + 1)
    return scored, weights


def component_reason_text(row: pd.Series, base_name: str) -> str:
    if base_name == "label_risk_flag":
        return f"标签提示：sub_label={row.get('sub_label') or 'risk'}"
    if base_name == "call_record_score":
        return f"通话记录 {int(row.get('call_record_count', 0))} 条，{percentile_human(row.get('call_record_pct', 0.0))}"
    if base_name == "counterparty_score":
        return f"对端数 {int(row.get('counterparty_count', 0))}，{percentile_human(row.get('counterparty_pct', 0.0))}"
    if base_name == "device_count_score":
        return f"设备数 {int(row.get('device_count', 0))} 台，{percentile_human(row.get('device_count_pct', 0.0))}"
    if base_name == "shared_device_score":
        return f"共享设备 {int(row.get('shared_device_count', 0))} 台"
    return f"共享设备额外牵出 {int(row.get('shared_device_peer_total', 0))} 个号码"


def build_reason_candidates(row: pd.Series) -> List[Tuple[str, float, str]]:
    candidates: List[Tuple[str, float, str]] = []
    sub_label = str(row.get("sub_label") or "")
    if float(row.get(component_field("label_risk_flag"), 0.0)) > 0:
        candidates.append((
            f"画像标签已包含风险信号（sub_label={sub_label or 'risk'}）",
            float(row.get(component_field("label_risk_flag"), 0.0)),
            "label_risk_flag",
        ))
    if float(row.get(component_field("counterparty_score"), 0.0)) > 0:
        candidates.append((
            f"联系人广度异常（对端数 {int(row.get('counterparty_count', 0))}，{percentile_human(row.get('counterparty_pct', 0.0))}）",
            float(row.get(component_field("counterparty_score"), 0.0)),
            "counterparty_score",
        ))
    if float(row.get(component_field("call_record_score"), 0.0)) > 0:
        candidates.append((
            f"通话活跃度较高（通话记录 {int(row.get('call_record_count', 0))} 条，{percentile_human(row.get('call_record_pct', 0.0))}）",
            float(row.get(component_field("call_record_score"), 0.0)),
            "call_record_score",
        ))
    if float(row.get(component_field("shared_device_score"), 0.0)) > 0 and int(row.get("shared_device_count", 0)) > 0:
        candidates.append((
            f"共享设备线索突出（共享设备 {int(row.get('shared_device_count', 0))} 台）",
            float(row.get(component_field("shared_device_score"), 0.0)),
            "shared_device_score",
        ))
    if float(row.get(component_field("shared_peer_score"), 0.0)) > 0 and int(row.get("shared_device_peer_total", 0)) > 0:
        candidates.append((
            f"设备扩散性较强（共享设备额外牵出 {int(row.get('shared_device_peer_total', 0))} 个号码）",
            float(row.get(component_field("shared_peer_score"), 0.0)),
            "shared_peer_score",
        ))
    if float(row.get(component_field("device_count_score"), 0.0)) > 0 and int(row.get("device_count", 0)) > 0:
        candidates.append((
            f"设备使用规模较大（设备数 {int(row.get('device_count', 0))} 台，{percentile_human(row.get('device_count_pct', 0.0))}）",
            float(row.get(component_field("device_count_score"), 0.0)),
            "device_count_score",
        ))
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates


def classify_driver_type(row: pd.Series) -> str:
    pairs = [(name, float(row.get(component_field(name), 0.0))) for name in BASE_COMPONENT_NAMES]
    pairs.sort(key=lambda x: x[1], reverse=True)
    top_name, top_score = pairs[0]
    if top_score <= 0:
        return "弱特征型"
    if top_name == "label_risk_flag":
        secondary = pairs[1][0] if len(pairs) > 1 else ""
        if secondary in {"shared_device_score", "shared_peer_score", "device_count_score"}:
            return "标签+设备混合驱动型"
        if secondary in {"counterparty_score", "call_record_score"}:
            return "标签+通话混合驱动型"
        return "标签驱动型"
    if top_name in {"shared_device_score", "shared_peer_score", "device_count_score"}:
        return "设备关联驱动型"
    if top_name == "counterparty_score":
        return "联系人广度驱动型"
    return "通话活跃驱动型"


def row_to_item(row: pd.Series) -> Dict[str, object]:
    reason_candidates = build_reason_candidates(row)
    top_components = []
    for text, score, base_name in reason_candidates[:3]:
        top_components.append({
            "component": base_name,
            "component_zh": COMPONENT_LABELS.get(base_name, base_name),
            "score": round(float(score), 4),
            "explanation": text,
        })

    unlabeled_warning = None
    if float(row.get("label") or 0.0) < 1 and str(row.get("sub_label") or "").lower() not in RISK_SUBLABEL_HINTS:
        unlabeled_warning = "该号码属于未标注/非风险标签对象，当前结果表示其行为结构可疑，不等于最终认定风险。"

    item = {
        "ranking_view": row.get("ranking_view"),
        "rank": int(row.get("rank", 0)),
        "phone_id": row["phone_id"],
        "phone_preview": short_id(row["phone_id"]),
        "province": row.get("province"),
        "label": None if pd.isna(row.get("label")) else float(row.get("label")),
        "sub_label": row.get("sub_label"),
        "risk_score": round(float(row.get("risk_score", 0.0)), 4),
        "risk_level": row.get("risk_level"),
        "risk_percentile": round(float(row.get("risk_percentile", 0.0)), 6),
        "rank_bucket": row.get("rank_bucket"),
        "call_record_count": int(row.get("call_record_count", 0)),
        "counterparty_count": int(row.get("counterparty_count", 0)),
        "device_count": int(row.get("device_count", 0)),
        "shared_device_count": int(row.get("shared_device_count", 0)),
        "shared_device_peer_total": int(row.get("shared_device_peer_total", 0)),
        "strongest_shared_device": row.get("strongest_shared_device"),
        "strongest_shared_device_preview": short_id(row.get("strongest_shared_device")),
        "strongest_shared_device_peer_count": int(row.get("strongest_shared_device_peer_count", 0)),
        "reasons": [text for text, _, _ in reason_candidates[:3]],
        "top_score_components": top_components,
        "driver_type": classify_driver_type(row),
        "unlabeled_warning": unlabeled_warning,
        "recommended_followups": [
            "single-number-analysis",
            "association-path-analysis",
            "overlap-analysis",
        ],
    }
    for base_name in BASE_COMPONENT_NAMES:
        field = component_field(base_name)
        item[field] = round(float(row.get(field, 0.0)), 4)
    return item


def summarize_view(items: List[Dict[str, object]], ranking_view: str) -> Dict[str, object]:
    if not items:
        return {
            "ranking_view": ranking_view,
            "count": 0,
            "score_range": None,
            "province_distribution": {},
            "sub_label_distribution": {},
            "labeled_risk_count": 0,
            "unlabeled_count": 0,
        }
    provinces: Dict[str, int] = {}
    sublabels: Dict[str, int] = {}
    labeled_count = 0
    unlabeled_count = 0
    scores: List[float] = []
    for item in items:
        province = str(item.get("province") or "unknown")
        provinces[province] = provinces.get(province, 0) + 1
        sub = str(item.get("sub_label") or "none")
        sublabels[sub] = sublabels.get(sub, 0) + 1
        if float(item.get("label") or 0.0) >= 1 or str(item.get("sub_label") or "").lower() in RISK_SUBLABEL_HINTS:
            labeled_count += 1
        else:
            unlabeled_count += 1
        scores.append(float(item.get("risk_score") or 0.0))
    return {
        "ranking_view": ranking_view,
        "count": len(items),
        "score_range": {"min": round(min(scores), 4), "max": round(max(scores), 4)},
        "province_distribution": provinces,
        "sub_label_distribution": sublabels,
        "labeled_risk_count": labeled_count,
        "unlabeled_count": unlabeled_count,
    }


def overall_structural_insights(
    overall_items: List[Dict[str, object]],
    unlabeled_items: List[Dict[str, object]],
    device_items: List[Dict[str, object]],
) -> Dict[str, object]:
    overall_set = {item["phone_id"] for item in overall_items}
    unlabeled_set = {item["phone_id"] for item in unlabeled_items}
    device_set = {item["phone_id"] for item in device_items}
    return {
        "overall_and_device_overlap_count": len(overall_set & device_set),
        "overall_and_unlabeled_overlap_count": len(overall_set & unlabeled_set),
        "device_only_candidates_preview": [short_id(x) for x in list(device_set - overall_set)[:5]],
        "unlabeled_discovery_preview": [item["phone_preview"] for item in unlabeled_items[:5]],
    }


def enrich_rank_context(items: List[Dict[str, object]]) -> List[Dict[str, object]]:
    if not items:
        return items
    for i, item in enumerate(items):
        next_item = items[i + 1] if i + 1 < len(items) else None
        if not next_item:
            item["score_gap_vs_next"] = None
            item["why_ahead_of_next"] = "已是当前榜单末位，后面无可比对象。"
            continue
        gap = round(float(item["risk_score"]) - float(next_item["risk_score"]), 4)
        item["score_gap_vs_next"] = gap
        item_components = {x["component"]: x["score"] for x in item.get("top_score_components", [])}
        next_components = {x["component"]: x["score"] for x in next_item.get("top_score_components", [])}
        biggest_adv = None
        biggest_delta = -1.0
        for base_name in BASE_COMPONENT_NAMES:
            delta = float(item.get(component_field(base_name), 0.0)) - float(next_item.get(component_field(base_name), 0.0))
            if delta > biggest_delta:
                biggest_delta = delta
                biggest_adv = base_name
        if biggest_adv and biggest_delta > 0:
            item["why_ahead_of_next"] = (
                f"比下一名高 {gap:.2f} 分，主要优势来自{COMPONENT_LABELS.get(biggest_adv, biggest_adv)}"
                f"（高出 {biggest_delta:.2f} 分）。"
            )
        else:
            item["why_ahead_of_next"] = f"比下一名高 {gap:.2f} 分，整体为多项特征小幅累积优势。"
    return items


def build_followup_templates(item: Dict[str, object], second_item: Optional[Dict[str, object]]) -> Dict[str, str]:
    phone_id = item["phone_id"]
    phone_preview = item["phone_preview"]
    result = {
        "single_number_analysis": (
            f"python3 single_number_analysis_wrapper.py --phone-id '{phone_id}' --hops 2 --max-nodes 200 --top-k 10 --analysis-mode mixed"
        )
    }
    if second_item:
        result["association_path_analysis"] = (
            f"python3 association_path_wrapper.py --phone-a '{phone_id}' --phone-b '{second_item['phone_id']}' --analysis-mode both --max-hops 3 --top-k 3"
        )
        result["overlap_analysis"] = (
            f"python3 overlap_analysis_wrapper.py --phone-a '{phone_id}' --phone-b '{second_item['phone_id']}' --top-k 10 --min-common-counterparty 1"
        )
    return result


def build_evidence_pack(items: List[Dict[str, object]]) -> List[Dict[str, object]]:
    evidence: List[Dict[str, object]] = []
    for idx, item in enumerate(items[:3]):
        second = items[idx + 1] if idx + 1 < len(items) else None
        pack = {
            "rank": item["rank"],
            "phone_id": item["phone_id"],
            "phone_preview": item["phone_preview"],
            "risk_score": item["risk_score"],
            "risk_level": item["risk_level"],
            "driver_type": item["driver_type"],
            "province": item.get("province"),
            "sub_label": item.get("sub_label"),
            "key_metrics": {
                "call_record_count": item["call_record_count"],
                "counterparty_count": item["counterparty_count"],
                "device_count": item["device_count"],
                "shared_device_count": item["shared_device_count"],
                "shared_device_peer_total": item["shared_device_peer_total"],
                "strongest_shared_device_preview": item["strongest_shared_device_preview"],
                "strongest_shared_device_peer_count": item["strongest_shared_device_peer_count"],
            },
            "top_score_components": item.get("top_score_components", []),
            "main_reasons": item.get("reasons", []),
            "unlabeled_warning": item.get("unlabeled_warning"),
            "why_ahead_of_next": item.get("why_ahead_of_next"),
            "followup_templates": build_followup_templates(item, second),
        }
        evidence.append(pack)
    return evidence


def section_title_context(candidate_scope: str, analysis_mode: str, ranking_view: str, insights: Dict[str, object], overall_count: int, unlabeled_count: int, device_count: int) -> Dict[str, object]:
    hide_unlabeled_section = False
    hide_device_section = False
    overall_title = "综合风险总榜"
    context_notes: List[str] = []

    if candidate_scope == "unlabeled_only":
        overall_title = "未标注综合总榜"
        if overall_count > 0 and overall_count == insights.get("overall_and_unlabeled_overlap_count", -1) and unlabeled_count == overall_count:
            hide_unlabeled_section = True
            context_notes.append("当前候选范围已限定为未标注对象，因此“未标注高可疑榜”与综合总榜等价，报告中已省略重复榜单。")
            context_notes.append("这里的结果表示“高可疑线索”，不等于最终认定风险；尤其当 sub_label=whitelist/normal 时，更应结合后续核查判断。")

    if analysis_mode == "device_only" and ranking_view == "device_priority":
        overall_title = "设备优先总榜"
        if overall_count > 0 and overall_count == insights.get("overall_and_device_overlap_count", -1) and device_count == overall_count:
            hide_device_section = True
            context_notes.append("当前分析模式为 device_only，且主视图为 device_priority，因此综合总榜本身已等价于设备优先榜，报告中已省略重复设备榜。")

    return {
        "overall_title": overall_title,
        "hide_unlabeled_section": hide_unlabeled_section,
        "hide_device_section": hide_device_section,
        "context_notes": context_notes,
    }


def markdown_report(
    *,
    overall_items: List[Dict[str, object]],
    unlabeled_items: List[Dict[str, object]],
    device_items: List[Dict[str, object]],
    meta: Dict[str, object],
    analysis_mode: str,
    top_n: int,
    discovery_top_n: int,
    province: Optional[str],
    candidate_scope: str,
    ranking_view: str,
    insights: Dict[str, object],
    score_breakdown_weights: Dict[str, Dict[str, float]],
    evidence_pack: List[Dict[str, object]],
) -> str:
    context = section_title_context(candidate_scope, analysis_mode, ranking_view, insights, len(overall_items), len(unlabeled_items), len(device_items))
    overall_title = context["overall_title"]
    lines: List[str] = [f"# TopN 高风险号码发现报告（Top {top_n}）", ""]
    lines.extend([
        "## 1. 分析设置",
        f"- 排名视图：{ranking_view}",
        f"- 分析模式：{analysis_mode}",
        f"- 候选范围：{candidate_scope}",
        f"- 省份过滤：{province or '无'}",
        f"- 综合总榜 TopN：{top_n}",
        f"- 发现榜 / 设备榜 TopN：{discovery_top_n}",
        f"- 全量号码数：{meta.get('total_phone_count')}",
        f"- 省份过滤前候选数：{meta.get('filtered_phone_count_before_scope')}",
        f"- 过滤后候选数：{meta.get('candidate_count_after_filter')}",
        f"- 数据中已有风险标签号码数：{meta.get('labeled_risk_phone_count')}",
        f"- include_sub_labels：{', '.join(meta.get('include_sub_labels') or []) or '无'}",
        f"- exclude_sub_labels：{', '.join(meta.get('exclude_sub_labels') or []) or '无'}",
        "",
    ])
    if context["context_notes"]:
        lines.append("## 2. 解读提醒")
        for note in context["context_notes"]:
            lines.append(f"- {note}")
        lines.append("")
        section_no = 3
    else:
        section_no = 2

    lines.append(f"## {section_no}. {overall_title}")
    if overall_items:
        overall_rows = []
        for item in overall_items:
            overall_rows.append([
                item["rank"],
                item["phone_preview"],
                f"{item['risk_score']:.2f}",
                item["risk_level"],
                item.get("province") or "-",
                item["driver_type"],
                "；".join(item["reasons"]),
            ])
        lines.extend(markdown_table(["排名", "号码预览", "风险分", "等级", "省份", "驱动类型", "入榜原因"], overall_rows))
    else:
        lines.append("- 当前没有满足条件的综合高风险号码。")
    lines.append("")
    section_no += 1

    if not context["hide_unlabeled_section"]:
        lines.append(f"## {section_no}. 未标注高可疑榜")
        if unlabeled_items:
            unlabeled_rows = []
            for item in unlabeled_items:
                unlabeled_rows.append([
                    item["rank"],
                    item["phone_preview"],
                    f"{item['risk_score']:.2f}",
                    item["rank_bucket"],
                    item["shared_device_count"],
                    item["shared_device_peer_total"],
                    item["driver_type"],
                    "；".join(item["reasons"]),
                ])
            lines.extend(markdown_table(["排名", "号码预览", "发现分", "分位桶", "共享设备数", "牵出号码数", "驱动类型", "发现原因"], unlabeled_rows))
            lines.append("")
            lines.append("> 说明：本节展示的是未标注/非风险标签对象中的高可疑线索，用于“优先复核”，不等于最终认定风险。")
        else:
            lines.append("- 当前没有筛到未标注但结构上高可疑的号码。")
        lines.append("")
        section_no += 1

    if not context["hide_device_section"]:
        lines.append(f"## {section_no}. 设备驱动高可疑榜")
        if device_items:
            device_rows = []
            for item in device_items:
                device_rows.append([
                    item["rank"],
                    item["phone_preview"],
                    f"{item['risk_score']:.2f}",
                    item["shared_device_count"],
                    item["shared_device_peer_total"],
                    item["strongest_shared_device_peer_count"],
                    item["driver_type"],
                    "；".join(item["reasons"]),
                ])
            lines.extend(markdown_table(["排名", "号码预览", "设备驱动分", "共享设备数", "牵出号码数", "最强设备额外牵出数", "驱动类型", "设备原因"], device_rows))
        else:
            lines.append("- 当前没有明显的设备驱动高可疑号码。")
        lines.append("")
        section_no += 1

    lines.append(f"## {section_no}. Top3 证据包与解释层")
    if evidence_pack:
        for pack in evidence_pack:
            lines.append(f"### Rank {pack['rank']} · {pack['phone_preview']} · 风险分 {pack['risk_score']:.2f}")
            lines.append(f"- 驱动类型：{pack['driver_type']}")
            lines.append(f"- 省份 / 标签：{pack.get('province') or '-'} / {pack.get('sub_label') or '-'}")
            metrics = pack["key_metrics"]
            lines.append(
                f"- 关键结构特征：通话 {metrics['call_record_count']} 条；对端 {metrics['counterparty_count']} 个；设备 {metrics['device_count']} 台；共享设备 {metrics['shared_device_count']} 台；共享设备额外牵出 {metrics['shared_device_peer_total']} 个号码。"
            )
            if metrics.get("strongest_shared_device_preview"):
                lines.append(
                    f"- 最强共享设备：{metrics['strongest_shared_device_preview']}，除当前号码外还额外牵出 {metrics['strongest_shared_device_peer_count']} 个号码。"
                )
            if pack.get("unlabeled_warning"):
                lines.append(f"- 风险解释边界：{pack['unlabeled_warning']}")
            lines.append(f"- 与下一名差异：{pack.get('why_ahead_of_next')}")
            comp_rows = []
            for comp in pack.get("top_score_components", []):
                comp_rows.append([
                    comp["component_zh"],
                    f"{comp['score']:.2f}",
                    comp["explanation"],
                ])
            if comp_rows:
                lines.extend(markdown_table(["核心分数组件", "贡献分", "解释"], comp_rows))
            lines.append("- 推荐后续联动：")
            for skill_name, cmd in pack.get("followup_templates", {}).items():
                lines.append(f"  - {skill_name}: `{cmd}`")
            lines.append("")
    else:
        lines.append("- 当前没有可展示的 Top3 证据包。")
        lines.append("")
    section_no += 1

    overall_summary = summarize_view(overall_items, "overall")
    unlabeled_summary = summarize_view(unlabeled_items, "unlabeled_only")
    device_summary = summarize_view(device_items, "device_priority")
    lines.append(f"## {section_no}. 榜单结构分析")
    if overall_items:
        lines.append(
            f"- 综合总榜中，已标注风险号码 {overall_summary['labeled_risk_count']} 个，未标注号码 {overall_summary['unlabeled_count']} 个。"
        )
        province_parts = [f"{k}:{v}" for k, v in sorted(overall_summary["province_distribution"].items())]
        sublabel_parts = [f"{k}:{v}" for k, v in sorted(overall_summary["sub_label_distribution"].items())]
        lines.append(f"- 综合总榜省份分布：{'，'.join(province_parts) if province_parts else '无'}")
        lines.append(f"- 综合总榜子标签分布：{'，'.join(sublabel_parts) if sublabel_parts else '无'}")
        lines.append(
            f"- 综合总榜风险分范围：{overall_summary['score_range']['min']:.2f} - {overall_summary['score_range']['max']:.2f}"
        )
    lines.append(f"- 未标注高可疑榜数量：{unlabeled_summary['count']} 个")
    lines.append(f"- 设备驱动高可疑榜数量：{device_summary['count']} 个")
    lines.append(f"- 综合总榜与设备榜重合数：{insights.get('overall_and_device_overlap_count', 0)} 个")
    lines.append(f"- 综合总榜与未标注榜重合数：{insights.get('overall_and_unlabeled_overlap_count', 0)} 个")
    device_only_preview = insights.get("device_only_candidates_preview") or []
    if device_only_preview:
        lines.append(f"- 仅在设备榜中突出的号码预览：{', '.join(device_only_preview)}")
    if score_breakdown_weights:
        lines.append("- 当前权重策略概览：")
        for view_name, weights in score_breakdown_weights.items():
            weight_text = "，".join(f"{COMPONENT_LABELS[k]}={v:.2f}" for k, v in weights.items())
            lines.append(f"  - {view_name}: {weight_text}")
    lines.append("")
    section_no += 1

    lines.append(f"## {section_no}. 重点对象与下一步建议")
    if overall_items:
        top_overall = ", ".join(item["phone_preview"] for item in overall_items[:3])
        lines.append(f"- 先对综合总榜前 3 个号码做 single-number-analysis：{top_overall}")
    if unlabeled_items:
        top_unlabeled = ", ".join(item["phone_preview"] for item in unlabeled_items[:3])
        lines.append(f"- 对未标注高可疑对象优先做人工复核：{top_unlabeled}")
    if device_items:
        top_device = ", ".join(item["phone_preview"] for item in device_items[:3])
        lines.append(f"- 对设备驱动榜前 3 个号码优先复查共享设备链路：{top_device}")
    lines.append("- 若要比较两个入榜号码之间如何连起来，调用 association-path-analysis")
    lines.append("- 若要比较两个入榜号码是否位于同一联系圈，调用 overlap-analysis")
    lines.append("- 若要解释某个号码为什么上榜，继续调用 single-number-analysis 下钻其局部关系圈")
    lines.append("")

    return "\n".join(lines) + "\n"


def export_csv(
    *,
    overall_items: List[Dict[str, object]],
    unlabeled_items: List[Dict[str, object]],
    device_items: List[Dict[str, object]],
    csv_path: Path,
) -> None:
    rows: List[Dict[str, object]] = []
    for items, view_name in [
        (overall_items, "overall"),
        (unlabeled_items, "unlabeled_only"),
        (device_items, "device_priority"),
    ]:
        for item in items:
            row = {
                "ranking_view": view_name,
                "rank": item["rank"],
                "phone_id": item["phone_id"],
                "phone_preview": item["phone_preview"],
                "province": item.get("province"),
                "label": item.get("label"),
                "sub_label": item.get("sub_label"),
                "risk_score": item.get("risk_score"),
                "risk_level": item.get("risk_level"),
                "risk_percentile": item.get("risk_percentile"),
                "rank_bucket": item.get("rank_bucket"),
                "driver_type": item.get("driver_type"),
                "call_record_count": item.get("call_record_count"),
                "counterparty_count": item.get("counterparty_count"),
                "device_count": item.get("device_count"),
                "shared_device_count": item.get("shared_device_count"),
                "shared_device_peer_total": item.get("shared_device_peer_total"),
                "strongest_shared_device": item.get("strongest_shared_device"),
                "strongest_shared_device_peer_count": item.get("strongest_shared_device_peer_count"),
                "reason_1": item["reasons"][0] if len(item["reasons"]) > 0 else "",
                "reason_2": item["reasons"][1] if len(item["reasons"]) > 1 else "",
                "reason_3": item["reasons"][2] if len(item["reasons"]) > 2 else "",
                "top_component_1": item.get("top_score_components", [{}])[0].get("component_zh", "") if item.get("top_score_components") else "",
                "top_component_2": item.get("top_score_components", [{}, {}])[1].get("component_zh", "") if len(item.get("top_score_components", [])) > 1 else "",
                "top_component_3": item.get("top_score_components", [{}, {}, {}])[2].get("component_zh", "") if len(item.get("top_score_components", [])) > 2 else "",
                "score_gap_vs_next": item.get("score_gap_vs_next"),
                "why_ahead_of_next": item.get("why_ahead_of_next"),
                "unlabeled_warning": item.get("unlabeled_warning") or "",
            }
            for base_name in BASE_COMPONENT_NAMES:
                row[component_field(base_name)] = item.get(component_field(base_name), 0.0)
            rows.append(row)
    pd.DataFrame(rows).to_csv(csv_path, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TopN 高风险号码识别与排序 final")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--discovery-top-n", type=int, default=10)
    parser.add_argument("--analysis-mode", choices=["mixed", "call_only", "device_only"], default="mixed")
    parser.add_argument("--ranking-view", choices=["all_views", "overall", "unlabeled_only", "device_priority"], default="all_views")
    parser.add_argument("--candidate-scope", choices=["all", "labeled_only", "unlabeled_only"], default="all")
    parser.add_argument("--province", type=str, default=None)
    parser.add_argument("--min-call-records", type=int, default=1)
    parser.add_argument("--min-counterparties", type=int, default=1)
    parser.add_argument("--min-shared-device-count", type=int, default=0)
    parser.add_argument("--min-device-count", type=int, default=0)
    parser.add_argument("--min-shared-peer-total", type=int, default=0)
    parser.add_argument("--include-sub-labels", type=str, default=None)
    parser.add_argument("--exclude-sub-labels", type=str, default=None)
    parser.add_argument("--user-node-path", type=str, default=None)
    parser.add_argument("--call-graph-path", type=str, default=None)
    parser.add_argument("--device-graph-path", type=str, default=None)
    parser.add_argument("--source-col", type=str, default="src_user_id")
    parser.add_argument("--target-col", type=str, default="dst_counterparty_id")
    parser.add_argument("--device-source-col", type=str, default="user_id")
    parser.add_argument("--device-target-col", type=str, default="imei")
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--dataset-root", type=str, default=None)
    parser.add_argument("--dataset", type=str, default="unified")
    parser.add_argument("--artifact-mode", choices=["full", "essential", "markdown_only"], default="full")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    auto_user_node_path: Optional[Path] = None
    auto_call_graph_path: Optional[Path] = None
    auto_device_graph_path: Optional[Path] = None

    if not (args.user_node_path and args.call_graph_path and args.device_graph_path):
        auto_user_node_path, auto_call_graph_path, auto_device_graph_path = resolve_dataset_paths(
            args.dataset_root,
            args.dataset,
        )

    user_node_path = (
        Path(args.user_node_path)
        if args.user_node_path
        else auto_user_node_path
        if auto_user_node_path is not None
        else resolve_first_existing(DEFAULT_USER_NODE_CANDIDATES, "user_node_path")
    )
    call_graph_path = (
        Path(args.call_graph_path)
        if args.call_graph_path
        else auto_call_graph_path
        if auto_call_graph_path is not None
        else resolve_first_existing(DEFAULT_CALL_GRAPH_CANDIDATES, "call_graph_path")
    )
    device_graph_path = (
        Path(args.device_graph_path)
        if args.device_graph_path
        else auto_device_graph_path
        if auto_device_graph_path is not None
        else resolve_first_existing(DEFAULT_DEVICE_GRAPH_CANDIDATES, "device_graph_path")
    )
    output_dir = Path(args.output_dir) if args.output_dir else resolve_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    include_sub_labels = parse_csv_like_text(args.include_sub_labels)
    exclude_sub_labels = parse_csv_like_text(args.exclude_sub_labels)

    engine = TopNRiskDiscovery(
        user_node_path=user_node_path,
        call_graph_path=call_graph_path,
        device_graph_path=device_graph_path,
        source_col=args.source_col,
        target_col=args.target_col,
        device_source_col=args.device_source_col,
        device_target_col=args.device_target_col,
    )

    feat, meta = engine.build_feature_table(
        province=args.province,
        candidate_scope=args.candidate_scope,
        min_call_records=args.min_call_records,
        min_counterparties=args.min_counterparties,
        min_shared_device_count=args.min_shared_device_count,
        min_device_count=args.min_device_count,
        min_shared_peer_total=args.min_shared_peer_total,
        include_sub_labels=include_sub_labels,
        exclude_sub_labels=exclude_sub_labels,
    )

    overall_df, overall_weights = compute_view_top(feat, "overall", args.top_n, args.analysis_mode)
    unlabeled_df, unlabeled_weights = compute_view_top(feat, "unlabeled_only", args.discovery_top_n, args.analysis_mode)
    device_df, device_weights = compute_view_top(feat, "device_priority", args.discovery_top_n, args.analysis_mode)

    overall_items = enrich_rank_context([row_to_item(row) for _, row in overall_df.iterrows()])
    unlabeled_items = enrich_rank_context([row_to_item(row) for _, row in unlabeled_df.iterrows()])
    device_items = enrich_rank_context([row_to_item(row) for _, row in device_df.iterrows()])

    insights = overall_structural_insights(overall_items, unlabeled_items, device_items)
    evidence_source = overall_items or unlabeled_items or device_items
    evidence_pack = build_evidence_pack(evidence_source)

    province_tag = (args.province or "all").replace(" ", "_")
    stem = f"topn_high_risk_{province_tag}_{args.analysis_mode}_top{args.top_n}"
    if args.ranking_view != "all_views":
        stem += f"_{args.ranking_view}"
    md_path = output_dir / f"{stem}.md"
    csv_path = output_dir / f"{stem}.csv"

    export_csv(overall_items=overall_items, unlabeled_items=unlabeled_items, device_items=device_items, csv_path=csv_path)
    report_text = markdown_report(
        overall_items=overall_items,
        unlabeled_items=unlabeled_items,
        device_items=device_items,
        meta=meta,
        analysis_mode=args.analysis_mode,
        top_n=args.top_n,
        discovery_top_n=args.discovery_top_n,
        province=args.province,
        candidate_scope=args.candidate_scope,
        ranking_view=args.ranking_view,
        insights=insights,
        score_breakdown_weights={
            "overall": overall_weights,
            "unlabeled_only": unlabeled_weights,
            "device_priority": device_weights,
        },
        evidence_pack=evidence_pack,
    )
    md_path.write_text(report_text, encoding="utf-8")

    result = {
        "dataset_overview": {
            "total_phone_count": meta.get("total_phone_count"),
            "labeled_risk_phone_count": meta.get("labeled_risk_phone_count"),
            "province_filter": args.province,
        },
        "candidate_pool_summary": {
            "candidate_scope": args.candidate_scope,
            "candidate_count_after_filter": meta.get("candidate_count_after_filter"),
            "analysis_mode": args.analysis_mode,
            "ranking_view": args.ranking_view,
            "min_call_records": args.min_call_records,
            "min_counterparties": args.min_counterparties,
            "min_shared_device_count": args.min_shared_device_count,
            "min_device_count": args.min_device_count,
            "min_shared_peer_total": args.min_shared_peer_total,
            "include_sub_labels": include_sub_labels,
            "exclude_sub_labels": exclude_sub_labels,
        },
        "score_breakdown_weights": {
            "overall": overall_weights,
            "unlabeled_only": unlabeled_weights,
            "device_priority": device_weights,
        },
        "top_risk_numbers": overall_items,
        "top_overall_numbers": overall_items,
        "top_unlabeled_numbers": unlabeled_items,
        "top_device_driven_numbers": device_items,
        "view_summaries": {
            "overall": summarize_view(overall_items, "overall"),
            "unlabeled_only": summarize_view(unlabeled_items, "unlabeled_only"),
            "device_priority": summarize_view(device_items, "device_priority"),
        },
        "top3_evidence_pack": evidence_pack,
        "report_context_flags": section_title_context(
            args.candidate_scope,
            args.analysis_mode,
            args.ranking_view,
            insights,
            len(overall_items),
            len(unlabeled_items),
            len(device_items),
        ),
        "discovery_insights": insights,
        "discovery_summary": (
            f"已生成综合总榜 Top {len(overall_items)}、未标注发现榜 Top {len(unlabeled_items)}、设备驱动榜 Top {len(device_items)}。"
            + (
                f" 综合总榜第 1 名是 {overall_items[0]['phone_preview']}，风险分 {overall_items[0]['risk_score']:.2f}。"
                if overall_items
                else " 当前没有满足条件的号码。"
            )
        ),
        "recommended_followups": [
            {
                "skill": "single-number-analysis",
                "reason": "对综合总榜或未标注发现榜中的号码做单号码深挖，查看其局部关系圈和共享设备细节。",
            },
            {
                "skill": "association-path-analysis",
                "reason": "若要比较两个入榜号码之间如何形成关联，可继续做路径型联合核查。",
            },
            {
                "skill": "overlap-analysis",
                "reason": "若要比较两个入榜号码是否位于同一联系圈，可继续做重叠关系核查。",
            },
        ],
    }

    payload = {
        "ok": True,
        "skill": "topn-high-risk-discovery",
        "query_type": "topn_risk_discovery",
        "input_summary": {
            "top_n": args.top_n,
            "discovery_top_n": args.discovery_top_n,
            "analysis_mode": args.analysis_mode,
            "ranking_view": args.ranking_view,
            "candidate_scope": args.candidate_scope,
            "province": args.province,
            "min_call_records": args.min_call_records,
            "min_counterparties": args.min_counterparties,
            "min_shared_device_count": args.min_shared_device_count,
            "min_device_count": args.min_device_count,
            "min_shared_peer_total": args.min_shared_peer_total,
            "include_sub_labels": include_sub_labels,
            "exclude_sub_labels": exclude_sub_labels,
            "user_node_path": str(user_node_path),
            "call_graph_path": str(call_graph_path),
            "device_graph_path": str(device_graph_path),
            "source_col": args.source_col,
            "target_col": args.target_col,
            "device_source_col": args.device_source_col,
            "device_target_col": args.device_target_col,
            "dataset_root": str(Path(args.dataset_root).expanduser()) if args.dataset_root else None,
            "dataset": args.dataset,
            "artifact_mode": args.artifact_mode,
        },
        "result": result,
        "notes": [
            "该技能通过基础算子组合实现：节点画像读取 + 通话聚合 + 共享设备扩展 + TopN 排序。",
            "当前输出同时生成 Markdown 报告和 CSV 风险名单，前端下载完全依赖 artifacts，不再在报告正文里重复伪附件。",
            "已增强解释层：驱动类型、Top3 分数组件、Top3 证据包、与下一名差异说明。",
            "已增强报告模板：在 unlabeled_only / device_only 等模式下会自动收缩重复榜单。",
        ],
        "base_operator_alignment": {
            "profile_lookup": "node_lookup",
            "call_aggregation": "aggregation_query + neighbor_query",
            "shared_device_expansion": "neighbor_query + subgraph_by_nodes",
            "risk_filter_and_ranking": "relationship_filter + aggregation_query",
        },
        "report_path": str(md_path),
        "risk_list_csv_path": str(csv_path),
        "report_exists": md_path.exists(),
        "artifacts": (
            [
                {"type": "markdown", "path": str(md_path), "title": md_path.name},
            ]
            if args.artifact_mode == "markdown_only"
            else [
                {"type": "markdown", "path": str(md_path), "title": md_path.name},
                {"type": "csv", "path": str(csv_path), "title": csv_path.name},
            ]
        ),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
