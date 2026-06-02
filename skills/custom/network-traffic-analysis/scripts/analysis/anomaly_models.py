from __future__ import annotations

import hashlib
import math
from typing import Any

numpy = None
rrcf = None


def ensure_ml_dependencies() -> Any:
    global numpy
    if numpy is not None:
        return numpy
    try:
        import numpy as np  # type: ignore
        from sklearn.ensemble import IsolationForest  # noqa: F401
        from sklearn.neighbors import LocalOutlierFactor  # noqa: F401
        from sklearn.preprocessing import RobustScaler  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "Missing ML dependencies 'numpy' and/or 'scikit-learn'. Install them before using ML anomaly engines: pip install numpy scikit-learn"
        ) from exc
    numpy = np
    return numpy


def ensure_rrcf_dependencies() -> tuple[Any, Any]:
    global rrcf
    np = ensure_ml_dependencies()
    if rrcf is not None:
        return np, rrcf
    try:
        import rrcf as rrcf_module  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "Missing optional dependency 'rrcf'. Install it before using the RCF anomaly engine: pip install rrcf"
        ) from exc
    rrcf = rrcf_module
    return np, rrcf


def _safe_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _hash_bucket(value: Any, buckets: int = 257) -> float:
    if value in (None, ""):
        return 0.0
    digest = hashlib.sha1(str(value).encode("utf-8")).hexdigest()
    return float(int(digest[:8], 16) % buckets)


def _normalize_scores(values: list[float]) -> list[float]:
    if not values:
        return []
    minimum = min(values)
    maximum = max(values)
    if math.isclose(minimum, maximum):
        # All values identical -> no anomaly variance, not "medium" risk
        return [0.0 for _ in values]
    return [(item - minimum) / (maximum - minimum) for item in values]


def _severity(score: float) -> str:
    if score >= 0.85:
        return "critical"
    if score >= 0.65:
        return "high"
    if score >= 0.45:
        return "medium"
    return "low"


def _compose_score(rule_score: float, iforest_score: float, lof_score: float, rcf_score: float, engine: str) -> float:
    """
    [修复 Issue 5] 提供纯模型评分模式。
    当指定 engine 时，不再混合规则分数，直接返回模型结果，
    以便用户能对比不同算法引擎的真实差异。
    """
    if engine == "iforest":
        return min(1.0, round(iforest_score, 4))
    if engine == "lof":
        return min(1.0, round(lof_score, 4))
    if engine == "rcf":
        return min(1.0, round(rcf_score, 4))
    # 默认混合模式 (Hybrid)
    return min(1.0, round(0.3 * rule_score + 0.25 * iforest_score + 0.2 * lof_score + 0.25 * rcf_score, 4))


def _fit_outlier_models(feature_rows: list[list[float]], contamination: float) -> tuple[list[float], list[float]]:
    np = ensure_ml_dependencies()
    from sklearn.ensemble import IsolationForest  # type: ignore
    from sklearn.neighbors import LocalOutlierFactor  # type: ignore
    from sklearn.preprocessing import RobustScaler  # type: ignore

    X = np.asarray(feature_rows, dtype=float)
    X_scaled = RobustScaler().fit_transform(X)

    iforest = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        max_samples="auto",
    )
    iforest.fit(X_scaled)
    iforest_scores = _normalize_scores((-iforest.score_samples(X_scaled)).tolist())

    neighbor_count = min(max(5, len(feature_rows) // 10), max(len(feature_rows) - 1, 1))
    if neighbor_count >= 2:
        lof = LocalOutlierFactor(n_neighbors=neighbor_count, contamination=contamination)
        lof.fit_predict(X_scaled)
        lof_scores = _normalize_scores((-lof.negative_outlier_factor_).tolist())
    else:
        lof_scores = [0.5 for _ in feature_rows]

    return iforest_scores, lof_scores


def _fit_rcf_outlier_scores(feature_rows: list[list[float]], *, num_trees: int = 40, tree_size: int = 256) -> list[float]:
    np, rrcf_module = ensure_rrcf_dependencies()
    from sklearn.preprocessing import RobustScaler  # type: ignore

    X = np.asarray(feature_rows, dtype=float)
    if len(X) <= 1:
        return [0.0 for _ in feature_rows]

    X_scaled = RobustScaler().fit_transform(X)
    effective_tree_size = min(tree_size, len(feature_rows))
    rng = np.random.default_rng(42)
    avg_codisp = np.zeros(len(X_scaled), dtype=float)
    counts = np.zeros(len(X_scaled), dtype=float)

    for _ in range(num_trees):
        tree = rrcf_module.RCTree()
        order = rng.permutation(len(X_scaled))
        for sequence_index, point_index in enumerate(order):
            point_id = int(point_index)
            tree.insert_point(X_scaled[point_id], index=point_id)
            if len(tree.leaves) > effective_tree_size:
                forget_id = int(order[sequence_index - effective_tree_size])
                if forget_id in tree.leaves:
                    tree.forget_point(forget_id)
            if point_id in tree.leaves:
                avg_codisp[point_id] += float(tree.codisp(point_id))
                counts[point_id] += 1.0

    scores: list[float] = []
    for index in range(len(X_scaled)):
        if counts[index] <= 0:
            scores.append(0.0)
        else:
            scores.append(avg_codisp[index] / counts[index])
    return _normalize_scores(scores)


def _likely_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
    protocol = str(row.get("protocol", "")).upper()
    session_state = str(row.get("session_state", "")).upper()
    dst_port = str(row.get("dst_port", ""))
    bytes_value = _safe_float(row.get("bytes"))
    packets = _safe_float(row.get("packets"))
    duration_ms = _safe_float(row.get("duration_ms"))
    fanout = _safe_float(row.get("src_unique_dst_ip"))

    if protocol == "TCP" and session_state in {"RST", "SYN", "SYN_ONLY"} and fanout >= 5:
        return "tcp_failure_or_scan_like_pattern"
    if dst_port == "443" and session_state in {"ACK", "ESTABLISHED"} and packets <= 2 and duration_ms <= 10:
        return "likely_tls_micro_transaction"
    if bytes_value <= 128 and packets <= 2 and duration_ms <= 5:
        return "very_small_microflow"
    if rule_score >= 0.7 and final_score >= 0.7:
        return "rule_and_model_agree_on_anomaly"
    return "model_flagged_short_flow_outlier"


def score_short_connection_candidates(rows: list[dict[str, Any]], *, engine: str = "hybrid") -> list[dict[str, Any]]:
    if not rows:
        return []

    feature_rows: list[list[float]] = []
    for row in rows:
        protocol = str(row.get("protocol", "")).upper()
        bytes_value = _safe_float(row.get("bytes"))
        packets = _safe_float(row.get("packets"))
        duration_ms = _safe_float(row.get("duration_ms"))
        payload_bytes = _safe_float(row.get("payload_bytes"))
        src_flow_count = _safe_float(row.get("src_flow_count"))
        src_unique_dst_ip = _safe_float(row.get("src_unique_dst_ip"))
        src_unique_dst_port = _safe_float(row.get("src_unique_dst_port"))
        dst_flow_count = _safe_float(row.get("dst_flow_count"))
        dst_unique_src_ip = _safe_float(row.get("dst_unique_src_ip"))
        bytes_per_packet = bytes_value / max(packets, 1.0)
        packets_per_second = packets / max(duration_ms / 1000.0, 0.001)
        row["_rule_score"] = min(
            1.0,
            (
                (0.30 if duration_ms <= 10 else 0.0)
                + (0.20 if bytes_value <= 300 else 0.0)
                + (0.15 if packets <= 2 else 0.0)
                + (0.20 if protocol == "TCP" and str(row.get("session_state", "")).upper() in {"RST", "SYN", "SYN_ONLY"} else 0.0)
                + (0.15 if src_unique_dst_ip >= 5 or src_unique_dst_port >= 10 else 0.0)
            ),
        )
        feature_rows.append(
            [
                math.log1p(bytes_value),
                math.log1p(packets),
                math.log1p(duration_ms),
                math.log1p(payload_bytes),
                math.log1p(bytes_per_packet),
                math.log1p(packets_per_second),
                math.log1p(src_flow_count),
                math.log1p(src_unique_dst_ip),
                math.log1p(src_unique_dst_port),
                math.log1p(dst_flow_count),
                math.log1p(dst_unique_src_ip),
            ]
        )

    contamination = min(0.25, max(0.03, 30 / max(len(rows), 100)))
    iforest_scores, lof_scores = _fit_outlier_models(feature_rows, contamination)
    rcf_scores = _fit_rcf_outlier_scores(feature_rows)

    enriched: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        rule_score = float(row.get("_rule_score", 0.0))
        final_score = _compose_score(rule_score, iforest_scores[index], lof_scores[index], rcf_scores[index], engine)
        enriched_row = dict(row)
        enriched_row["rule_score"] = round(rule_score, 4)
        enriched_row["iforest_score"] = round(iforest_scores[index], 4)
        enriched_row["lof_score"] = round(lof_scores[index], 4)
        enriched_row["rcf_score"] = round(rcf_scores[index], 4)
        enriched_row["anomaly_score"] = final_score
        enriched_row["severity"] = _severity(final_score)
        enriched_row["likely_reason"] = _likely_reason(enriched_row, final_score, rule_score)
        enriched.append(enriched_row)

    enriched.sort(
        key=lambda item: (
            float(item.get("anomaly_score", 0.0)),
            float(item.get("rule_score", 0.0)),
            -_safe_float(item.get("bytes")),
        ),
        reverse=True,
    )
    return enriched


def score_scan_candidates(rows: list[dict[str, Any]], *, packet_view: bool, engine: str = "hybrid") -> list[dict[str, Any]]:
    if not rows:
        return []

    feature_rows: list[list[float]] = []
    for row in rows:
        count_key = "packets" if packet_view else "flows"
        count_value = _safe_float(row.get(count_key))
        unique_dst_ip = _safe_float(row.get("unique_dst_ip"))
        unique_dst_port = _safe_float(row.get("unique_dst_port"))
        total_bytes = _safe_float(row.get("total_bytes"))
        avg_bytes = _safe_float(row.get("avg_bytes"))
        unique_protocols = _safe_float(row.get("unique_protocols"))
        unique_app_protocols = _safe_float(row.get("unique_app_protocols"))
        syn_only_packets = _safe_float(row.get("syn_only_packets"))
        rst_packets = _safe_float(row.get("rst_packets"))
        syn_only_pct = _safe_float(row.get("syn_only_pct"))
        rst_pct = _safe_float(row.get("rst_pct"))

        row["_rule_score"] = min(
            1.0,
            (
                (0.30 if unique_dst_ip >= 10 else 0.15 if unique_dst_ip >= 5 else 0.0)
                + (0.25 if unique_dst_port >= 20 else 0.15 if unique_dst_port >= 10 else 0.0)
                + (0.20 if syn_only_packets >= 10 or syn_only_pct >= 40 else 0.0)
                + (0.10 if rst_packets >= 10 or rst_pct >= 30 else 0.0)
                + (0.15 if unique_protocols >= 3 or unique_app_protocols >= 3 else 0.0)
            ),
        )

        feature_rows.append(
            [
                math.log1p(count_value),
                math.log1p(unique_dst_ip),
                math.log1p(unique_dst_port),
                math.log1p(total_bytes),
                math.log1p(max(avg_bytes, 0.0)),
                math.log1p(unique_protocols),
                math.log1p(unique_app_protocols),
                math.log1p(syn_only_packets),
                math.log1p(rst_packets),
                syn_only_pct,
                rst_pct,
            ]
        )

    contamination = min(0.2, max(0.03, 20 / max(len(rows), 100)))
    iforest_scores, lof_scores = _fit_outlier_models(feature_rows, contamination)
    rcf_scores = _fit_rcf_outlier_scores(feature_rows)

    enriched: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        rule_score = float(row.get("_rule_score", 0.0))
        final_score = _compose_score(rule_score, iforest_scores[index], lof_scores[index], rcf_scores[index], engine)
        enriched_row = dict(row)
        enriched_row["rule_score"] = round(rule_score, 4)
        enriched_row["iforest_score"] = round(iforest_scores[index], 4)
        enriched_row["lof_score"] = round(lof_scores[index], 4)
        enriched_row["rcf_score"] = round(rcf_scores[index], 4)
        enriched_row["scan_risk_score"] = final_score
        enriched_row["severity"] = _severity(final_score)
        if _safe_float(row.get("syn_only_pct")) >= 40 and _safe_float(row.get("unique_dst_ip")) >= 5:
            reason = "syn_heavy_broad_targeting"
        elif _safe_float(row.get("unique_dst_port")) >= 20:
            reason = "broad_port_coverage"
        elif _safe_float(row.get("rst_pct")) >= 30:
            reason = "reset_heavy_scan_or_rejection"
        else:
            reason = "source_behavior_outlier"
        enriched_row["likely_reason"] = reason
        enriched.append(enriched_row)

    enriched.sort(
        key=lambda item: (
            float(item.get("scan_risk_score", 0.0)),
            float(item.get("rule_score", 0.0)),
            _safe_float(item.get("unique_dst_ip")),
            _safe_float(item.get("unique_dst_port")),
        ),
        reverse=True,
    )
    return enriched


def score_session_candidates(rows: list[dict[str, Any]], *, engine: str = "hybrid") -> list[dict[str, Any]]:
    if not rows:
        return []

    feature_rows: list[list[float]] = []
    for row in rows:
        flows = _safe_float(row.get("flows"))
        unique_dst_ip = _safe_float(row.get("unique_dst_ip"))
        unique_dst_port = _safe_float(row.get("unique_dst_port"))
        total_bytes = _safe_float(row.get("total_bytes"))
        avg_bytes = _safe_float(row.get("avg_bytes"))
        avg_packets = _safe_float(row.get("avg_packets"))
        avg_duration_ms = _safe_float(row.get("avg_duration_ms"))
        negative_outcomes = _safe_float(row.get("negative_outcomes"))
        negative_pct = _safe_float(row.get("negative_pct"))
        risky_states = _safe_float(row.get("risky_states"))
        risky_state_pct = _safe_float(row.get("risky_state_pct"))
        short_low_byte_pct = _safe_float(row.get("short_low_byte_pct"))

        row["_rule_score"] = min(
            1.0,
            (
                (0.35 if negative_pct >= 40 else 0.20 if negative_pct >= 20 else 0.0)
                + (0.25 if risky_state_pct >= 40 else 0.15 if risky_state_pct >= 20 else 0.0)
                + (0.15 if short_low_byte_pct >= 40 else 0.0)
                + (0.10 if unique_dst_ip >= 10 or unique_dst_port >= 10 else 0.0)
                + (0.15 if flows >= 20 and negative_outcomes >= 5 else 0.0)
            ),
        )

        feature_rows.append(
            [
                math.log1p(flows),
                math.log1p(unique_dst_ip),
                math.log1p(unique_dst_port),
                math.log1p(total_bytes),
                math.log1p(max(avg_bytes, 0.0)),
                math.log1p(max(avg_packets, 0.0)),
                math.log1p(max(avg_duration_ms, 0.0)),
                math.log1p(negative_outcomes),
                math.log1p(risky_states),
                negative_pct,
                risky_state_pct,
                short_low_byte_pct,
            ]
        )

    contamination = min(0.2, max(0.03, 20 / max(len(rows), 100)))
    iforest_scores, lof_scores = _fit_outlier_models(feature_rows, contamination)
    rcf_scores = _fit_rcf_outlier_scores(feature_rows)

    enriched: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        rule_score = float(row.get("_rule_score", 0.0))
        final_score = _compose_score(rule_score, iforest_scores[index], lof_scores[index], rcf_scores[index], engine)
        enriched_row = dict(row)
        enriched_row["rule_score"] = round(rule_score, 4)
        enriched_row["iforest_score"] = round(iforest_scores[index], 4)
        enriched_row["lof_score"] = round(lof_scores[index], 4)
        enriched_row["rcf_score"] = round(rcf_scores[index], 4)
        enriched_row["session_risk_score"] = final_score
        enriched_row["severity"] = _severity(final_score)
        if _safe_float(row.get("negative_pct")) >= 40:
            reason = "failure_heavy_source"
        elif _safe_float(row.get("risky_state_pct")) >= 40:
            reason = "risky_session_state_mix"
        elif _safe_float(row.get("short_low_byte_pct")) >= 40:
            reason = "short_low_byte_session_pattern"
        else:
            reason = "session_behavior_outlier"
        enriched_row["likely_reason"] = reason
        enriched.append(enriched_row)

    enriched.sort(
        key=lambda item: (
            float(item.get("session_risk_score", 0.0)),
            float(item.get("rule_score", 0.0)),
            _safe_float(item.get("negative_pct")),
            _safe_float(item.get("risky_state_pct")),
        ),
        reverse=True,
    )
    return enriched


def score_generic_candidates(
    rows: list[dict[str, Any]],
    *,
    numeric_fields: list[str],
    categorical_fields: list[str],
    rule_score_fn: Any,
    reason_fn: Any,
    output_field: str,
    contamination: float = 0.15,
    engine: str = "hybrid",
) -> list[dict[str, Any]]:
    if not rows:
        return []

    feature_rows: list[list[float]] = []
    for row in rows:
        rule_score = float(max(0.0, min(1.0, rule_score_fn(row))))
        row["_rule_score"] = rule_score
        # Use only numeric features for distance calculation to avoid fake distance artifacts
        # caused by hash bucketing (e.g., bucket 12 being "closer" to 13 than 240).
        # Categorical fields are still used by the rule_score_fn logic.
        encoded = [math.log1p(max(_safe_float(row.get(field)), 0.0)) for field in numeric_fields]
        feature_rows.append(encoded)

    iforest_scores, lof_scores = _fit_outlier_models(feature_rows, contamination)
    rcf_scores = _fit_rcf_outlier_scores(feature_rows)

    enriched: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        rule_score = float(row.get("_rule_score", 0.0))
        final_score = _compose_score(rule_score, iforest_scores[index], lof_scores[index], rcf_scores[index], engine)
        enriched_row = dict(row)
        enriched_row["rule_score"] = round(rule_score, 4)
        enriched_row["iforest_score"] = round(iforest_scores[index], 4)
        enriched_row["lof_score"] = round(lof_scores[index], 4)
        enriched_row["rcf_score"] = round(rcf_scores[index], 4)
        enriched_row[output_field] = final_score
        enriched_row["severity"] = _severity(final_score)
        enriched_row["likely_reason"] = reason_fn(enriched_row, final_score, rule_score)
        enriched.append(enriched_row)

    enriched.sort(
        key=lambda item: (
            float(item.get(output_field, 0.0)),
            float(item.get("rule_score", 0.0)),
        ),
        reverse=True,
    )
    return enriched


def score_timeseries_rcf(
    rows: list[dict[str, Any]],
    *,
    numeric_fields: list[str],
    num_trees: int = 40,
    tree_size: int = 256,
) -> list[float]:
    if not rows:
        return []

    np, rrcf_module = ensure_rrcf_dependencies()
    points = np.asarray(
        [
            [max(_safe_float(row.get(field)), 0.0) for field in numeric_fields]
            for row in rows
        ],
        dtype=float,
    )
    if len(points) == 1:
        # A single point cannot be anomalous by itself
        return [0.0]

    forests = [rrcf_module.RCTree() for _ in range(num_trees)]
    avg_codisp = np.zeros(len(points), dtype=float)
    counts = np.zeros(len(points), dtype=float)

    for tree in forests:
        for index, point in enumerate(points):
            tree.insert_point(point, index=index)
            if len(tree.leaves) > tree_size:
                forget_index = index - tree_size
                if forget_index in tree.leaves:
                    tree.forget_point(forget_index)
            if index in tree.leaves:
                avg_codisp[index] += float(tree.codisp(index))
                counts[index] += 1.0

    scores = []
    for index in range(len(points)):
        if counts[index] <= 0:
            scores.append(0.0)
        else:
            scores.append(avg_codisp[index] / counts[index])
    return _normalize_scores(scores)
