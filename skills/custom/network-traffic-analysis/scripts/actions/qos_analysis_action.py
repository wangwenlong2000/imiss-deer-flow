"""
QoS Analysis Action

Action handler for service-quality analysis using directly measured QoS fields
when present and transparent flow/session proxies when they are not.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import append_file_errors, fetch_rows, format_dict_rows, present_fields, scoped_where


def _duration_expr(available: set[str]) -> str:
    if "duration_ms" in available:
        return "COALESCE(duration_ms, flow_duration * 1000.0, 0.0)"
    if "flow_duration" in available:
        return "COALESCE(flow_duration * 1000.0, 0.0)"
    return "0.0"


def _throughput_kbps_expr(available: set[str]) -> str:
    duration_expr = _duration_expr(available)
    if "duration_ms" in available or "flow_duration" in available:
        return (
            f"CASE WHEN {duration_expr} > 0 "
            f"THEN (COALESCE(bytes, 0) * 8.0) / NULLIF({duration_expr}, 0) "
            "ELSE NULL END"
        )
    return "NULL"


def _service_label_expr(available: set[str]) -> str:
    app_protocol_expr = "NULLIF(TRIM(COALESCE(app_protocol, '')), '')" if "app_protocol" in available else "NULL"
    service_expr = "NULLIF(TRIM(COALESCE(service, '')), '')" if "service" in available else "NULL"
    protocol_expr = "NULLIF(TRIM(COALESCE(protocol, '')), '')" if "protocol" in available else "NULL"
    port_expr = "CASE WHEN dst_port IS NOT NULL THEN CONCAT('port:', CAST(CAST(dst_port AS BIGINT) AS VARCHAR)) ELSE NULL END" if "dst_port" in available else "NULL"
    return f"COALESCE({app_protocol_expr}, {service_expr}, {port_expr}, {protocol_expr}, 'UNKNOWN')"


def _negative_action_expr(available: set[str]) -> str:
    if "action" not in available:
        return "0.0"
    return (
        "CASE WHEN LOWER(COALESCE(action, '')) IN "
        "('deny', 'drop', 'block', 'reset', 'reject', 'timeout', 'failed') "
        "THEN 1.0 ELSE 0.0 END"
    )


def _risky_state_expr(available: set[str]) -> str:
    predicates: list[str] = []
    if "session_state" in available:
        predicates.append(
            "LOWER(COALESCE(session_state, '')) IN "
            "('syn_only', 'syn_sent', 'reset', 'rst', 'failed', 'reject', 'timeout', 's0', 'rej', 'rsto', 'rstr', 'rstos0', 'shr', 'sh')"
        )
    if "conn_state" in available:
        predicates.append(
            "UPPER(COALESCE(conn_state, '')) IN "
            "('S0', 'REJ', 'RSTO', 'RSTR', 'RSTOS0', 'SH', 'SHR', 'OTH')"
        )
    if not predicates:
        return "0.0"
    return "CASE WHEN " + " OR ".join(predicates) + " THEN 1.0 ELSE 0.0 END"


def _loss_pct_expr(available: set[str]) -> str:
    if "packet_loss_pct" in available:
        return "packet_loss_pct"
    return "NULL"


def _jitter_ms_expr(available: set[str]) -> str:
    if "jitter_ms" in available:
        return "jitter_ms"
    return "NULL"


def _rtt_ms_expr(available: set[str]) -> str:
    if "rtt_ms" in available:
        return "rtt_ms"
    return "NULL"


def _retransmission_rate_expr(available: set[str]) -> str:
    if "retransmission_rate" in available:
        return "retransmission_rate"
    if "retransmission_count" in available:
        return "CASE WHEN retransmission_count IS NOT NULL AND COALESCE(packets, 0.0) > 0 THEN retransmission_count * 1.0 / NULLIF(packets, 0.0) END"
    return "NULL"


def _asymmetry_expr(available: set[str]) -> str:
    candidates: list[str] = []
    if "byte_asymmetry" in available:
        candidates.append("COALESCE(byte_asymmetry, 0.0)")
    if "packet_asymmetry" in available:
        candidates.append("COALESCE(packet_asymmetry, 0.0)")
    if not candidates:
        return "0.0"
    if len(candidates) == 1:
        return candidates[0]
    return "GREATEST(" + ", ".join(candidates) + ")"


def _missed_byte_ratio_expr(available: set[str]) -> str:
    if "missed_bytes" not in available:
        return "0.0"
    return "COALESCE(SUM(COALESCE(missed_bytes, 0.0)) / NULLIF(SUM(COALESCE(bytes, 0.0) + COALESCE(missed_bytes, 0.0)), 0.0), 0.0)"


def _coverage_expr(field: str, available: set[str], *, nonzero_only: bool = False) -> str:
    if field not in available:
        return "0"
    if nonzero_only:
        return f"SUM(CASE WHEN {field} IS NOT NULL AND {field} <> 0 THEN 1 ELSE 0 END)"
    return f"SUM(CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END)"


def _build_measurement_profile(con: Any, available: set[str], where_clause: str) -> dict[str, Any]:
    coverage_sql = f"""
        SELECT
            COUNT(*) AS flow_count,
            {_coverage_expr("rtt_ms", available)} AS rtt_populated,
            {_coverage_expr("jitter_ms", available)} AS jitter_populated,
            {_coverage_expr("packet_loss_pct", available)} AS loss_populated,
            CASE
                WHEN {"'retransmission_rate'" if "retransmission_rate" in available else "NULL"} IS NOT NULL
                    THEN {_coverage_expr("retransmission_rate", available)}
                WHEN {"'retransmission_count'" if "retransmission_count" in available else "NULL"} IS NOT NULL
                    THEN {_coverage_expr("retransmission_count", available)}
                ELSE 0
            END AS retransmission_populated
        FROM flows
        {scoped_where(where_clause, "1=1")}
    """
    rows = fetch_rows(con, coverage_sql)
    profile = rows[0] if rows else {}
    flow_count = int(_safe_float(profile.get("flow_count")))

    def ratio(key: str) -> float:
        populated = int(_safe_float(profile.get(key)))
        if flow_count <= 0:
            return 0.0
        return round(populated / flow_count, 4)

    evidence_sources: list[str] = []
    if any(ratio(key) > 0 for key in ("rtt_populated", "jitter_populated", "loss_populated", "retransmission_populated")):
        evidence_sources.append("packet-level tshark tcp.analysis enrichment")
    if "missed_bytes" in available:
        evidence_sources.append("zeek conn.log missed_bytes")
    if {"byte_asymmetry", "packet_asymmetry"} & available:
        evidence_sources.append("flow asymmetry features")
    if {"timestamp", "relative_time_s", "start_relative_time_s"} & available:
        evidence_sources.append("ordered timing gaps")

    direct_ratios = [ratio("rtt_populated"), ratio("jitter_populated"), ratio("loss_populated"), ratio("retransmission_populated")]
    direct_present = sum(1 for item in direct_ratios if item > 0)
    if direct_present >= 3:
        mode = "direct_packet_enriched"
        confidence = "high"
    elif direct_present >= 1:
        mode = "mixed_direct_and_proxy"
        confidence = "medium"
    else:
        mode = "proxy_only"
        confidence = "low"

    return {
        "mode": mode,
        "confidence_floor": confidence,
        "flow_count": flow_count,
        "rtt_coverage_ratio": ratio("rtt_populated"),
        "jitter_coverage_ratio": ratio("jitter_populated"),
        "loss_coverage_ratio": ratio("loss_populated"),
        "retransmission_coverage_ratio": ratio("retransmission_populated"),
        "evidence_sources": ", ".join(evidence_sources) if evidence_sources else "flow/session proxies only",
    }


def _build_signal_rows(available: set[str], measurement_profile: dict[str, Any]) -> list[dict[str, str]]:
    timing_available = bool({"timestamp", "relative_time_s", "start_relative_time_s"} & available)
    rtt_available = _safe_float(measurement_profile.get("rtt_coverage_ratio")) > 0
    jitter_available = _safe_float(measurement_profile.get("jitter_coverage_ratio")) > 0
    loss_available = _safe_float(measurement_profile.get("loss_coverage_ratio")) > 0
    retrans_available = _safe_float(measurement_profile.get("retransmission_coverage_ratio")) > 0
    return [
        {
            "signal": "duration_and_throughput",
            "status": "available" if {"duration_ms", "flow_duration"} & available else "missing",
            "detail": "Uses flow duration to estimate throughput and sustained low-rate transfers.",
        },
        {
            "signal": "latency_rtt",
            "status": "available" if rtt_available else "proxy_only",
            "detail": "Uses direct RTT when mapped; otherwise latency conclusions are not asserted.",
        },
        {
            "signal": "jitter",
            "status": "available" if jitter_available else "proxy_only",
            "detail": "Uses direct jitter when mapped; otherwise timing instability is reported as a proxy, not packet-delay variation.",
        },
        {
            "signal": "packet_loss",
            "status": "available" if loss_available else "proxy_only",
            "detail": "Uses direct loss percentage when mapped; otherwise missed_bytes/session failures are treated as softer evidence.",
        },
        {
            "signal": "retransmissions",
            "status": "available" if retrans_available else "missing",
            "detail": "Uses retransmission rate directly or derives it from retransmission count and packets.",
        },
        {
            "signal": "directional_asymmetry",
            "status": "available" if {"byte_asymmetry", "packet_asymmetry"} & available else "missing",
            "detail": "Uses byte or packet asymmetry to highlight one-sided transfers and delivery imbalance.",
        },
        {
            "signal": "timing_instability_proxy",
            "status": "available" if timing_available else "missing",
            "detail": "Uses inter-event gap variability as a burstiness proxy when ordered time exists.",
        },
    ]


def _clamp01(value: float) -> float:
    if value <= 0:
        return 0.0
    if value >= 1:
        return 1.0
    return value


def _safe_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _score_qos_row(row: dict[str, Any]) -> dict[str, Any]:
    low_throughput = _safe_float(row.get("low_throughput_ratio"))
    negative_action = _safe_float(row.get("negative_action_ratio"))
    risky_state = _safe_float(row.get("risky_state_ratio"))
    loss_pct = _safe_float(row.get("avg_packet_loss_pct"))
    jitter_ms = _safe_float(row.get("avg_jitter_ms"))
    rtt_ms = _safe_float(row.get("avg_rtt_ms"))
    retrans_rate = _safe_float(row.get("avg_retransmission_rate"))
    asymmetry = _safe_float(row.get("avg_asymmetry"))
    missed_ratio = _safe_float(row.get("missed_byte_ratio"))

    degradation_score = (
        0.18 * _clamp01(low_throughput / 0.40)
        + 0.16 * _clamp01(max(negative_action, risky_state) / 0.25)
        + 0.14 * _clamp01(loss_pct / 5.0)
        + 0.12 * _clamp01(missed_ratio / 0.10)
        + 0.12 * _clamp01(retrans_rate / 0.05)
        + 0.10 * _clamp01(jitter_ms / 50.0)
        + 0.10 * _clamp01(rtt_ms / 200.0)
        + 0.08 * _clamp01(asymmetry / 0.75)
    )

    reasons: list[str] = []
    if low_throughput >= 0.30:
        reasons.append("persistent_low_throughput")
    if max(negative_action, risky_state) >= 0.20:
        reasons.append("delivery_failures")
    if loss_pct >= 1.0 or missed_ratio >= 0.03:
        reasons.append("loss_or_observability_gap")
    if retrans_rate >= 0.03:
        reasons.append("retransmission_pressure")
    if jitter_ms >= 30.0:
        reasons.append("high_jitter")
    if rtt_ms >= 150.0:
        reasons.append("high_rtt")
    if asymmetry >= 0.60:
        reasons.append("directional_imbalance")
    if not reasons and degradation_score >= 0.45:
        reasons.append("multi_signal_quality_degradation")

    if degradation_score >= 0.70:
        status = "poor"
    elif degradation_score >= 0.40:
        status = "degraded"
    else:
        status = "healthy"

    direct_signals = sum(
        1
        for field in ("avg_packet_loss_pct", "avg_jitter_ms", "avg_rtt_ms", "avg_retransmission_rate")
        if row.get(field) not in (None, "")
    )
    if direct_signals >= 3:
        confidence = "high"
        evidence_profile = "direct_packet_qos"
    elif direct_signals >= 1:
        confidence = "medium"
        evidence_profile = "mixed_direct_and_proxy"
    else:
        confidence = "low"
        evidence_profile = "proxy_only"

    row = dict(row)
    row["qos_degradation_score"] = round(degradation_score, 4)
    row["status"] = status
    row["confidence"] = confidence
    row["evidence_profile"] = evidence_profile
    row["dominant_reason"] = ", ".join(reasons) if reasons and status in {"degraded", "poor"} else "no_strong_qos_degradation_signal"
    return row


def _sorted_hotspots(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    scored = [_score_qos_row(row) for row in rows]
    scored.sort(
        key=lambda row: (
            _safe_float(row.get("qos_degradation_score")),
            _safe_float(row.get("total_bytes")),
            _safe_float(row.get("flow_count")),
        ),
        reverse=True,
    )
    return scored[:limit]


def execute_qos_analysis(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    **kwargs,
) -> dict[str, Any]:
    limit = kwargs.get("limit", 20)
    available = present_fields(mappings)

    results: dict[str, Any] = {
        "action": "qos-analysis",
        "files_analyzed": [],
        "summary": {
            "signals_available": 0,
            "degraded_services": 0,
            "degraded_paths": 0,
            "timing_hotspots": 0,
        },
    }

    file_result: dict[str, Any] = {
        "file": files[0] if files else "selected scope",
        "measurement_profile": {},
        "signals": [],
        "service_hotspots": [],
        "conversation_hotspots": [],
        "timing_hotspots": [],
        "recommendations": [],
        "notes": [],
    }

    try:
        if not {"src_ip", "dst_ip", "bytes", "packets"}.issubset(available):
            file_result["error"] = "QoS analysis requires src_ip, dst_ip, bytes, and packets in the canonical flow view."
            results["files_analyzed"].append(file_result)
            return results

        file_result["measurement_profile"] = _build_measurement_profile(con, available, where_clause)
        file_result["signals"] = _build_signal_rows(available, file_result["measurement_profile"])
        results["summary"]["signals_available"] = sum(1 for row in file_result["signals"] if row["status"] == "available")

        duration_expr = _duration_expr(available)
        throughput_expr = _throughput_kbps_expr(available)
        service_label_expr = _service_label_expr(available)
        negative_action_expr = _negative_action_expr(available)
        risky_state_expr = _risky_state_expr(available)
        loss_pct_expr = _loss_pct_expr(available)
        jitter_ms_expr = _jitter_ms_expr(available)
        rtt_ms_expr = _rtt_ms_expr(available)
        retrans_rate_expr = _retransmission_rate_expr(available)
        asymmetry_expr = _asymmetry_expr(available)
        missed_ratio_expr = _missed_byte_ratio_expr(available)

        service_sql = f"""
            SELECT
                {service_label_expr} AS service_label,
                COUNT(*) AS flow_count,
                ROUND(SUM(COALESCE(bytes, 0.0)), 2) AS total_bytes,
                ROUND(AVG(COALESCE(packets, 0.0)), 2) AS avg_packets,
                ROUND(AVG(COALESCE({duration_expr}, 0.0)), 2) AS avg_duration_ms,
                ROUND(AVG({throughput_expr}), 2) AS avg_throughput_kbps,
                ROUND(SUM(CASE WHEN {throughput_expr} IS NOT NULL AND {duration_expr} >= 1000 AND {throughput_expr} < 128 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS low_throughput_ratio,
                ROUND(AVG({negative_action_expr}), 4) AS negative_action_ratio,
                ROUND(AVG({risky_state_expr}), 4) AS risky_state_ratio,
                ROUND(AVG({loss_pct_expr}), 4) AS avg_packet_loss_pct,
                ROUND(AVG({jitter_ms_expr}), 2) AS avg_jitter_ms,
                ROUND(AVG({rtt_ms_expr}), 2) AS avg_rtt_ms,
                ROUND(AVG(COALESCE({retrans_rate_expr}, 0.0)), 4) AS avg_retransmission_rate,
                ROUND(AVG(COALESCE({asymmetry_expr}, 0.0)), 4) AS avg_asymmetry,
                ROUND({missed_ratio_expr}, 4) AS missed_byte_ratio
            FROM flows
            {scoped_where(where_clause, "src_ip IS NOT NULL AND dst_ip IS NOT NULL")}
            GROUP BY 1
            HAVING COUNT(*) >= 2
            ORDER BY total_bytes DESC, flow_count DESC, service_label ASC
            LIMIT {max(limit * 15, 200)}
        """
        service_rows = fetch_rows(con, service_sql)
        file_result["service_hotspots"] = _sorted_hotspots(service_rows, limit=limit)
        results["summary"]["degraded_services"] = sum(1 for row in file_result["service_hotspots"] if row.get("status") in {"degraded", "poor"})

        conversation_sql = f"""
            SELECT
                src_ip,
                dst_ip,
                {service_label_expr} AS service_label,
                COUNT(*) AS flow_count,
                ROUND(SUM(COALESCE(bytes, 0.0)), 2) AS total_bytes,
                ROUND(AVG(COALESCE({duration_expr}, 0.0)), 2) AS avg_duration_ms,
                ROUND(AVG({throughput_expr}), 2) AS avg_throughput_kbps,
                ROUND(SUM(CASE WHEN {throughput_expr} IS NOT NULL AND {duration_expr} >= 1000 AND {throughput_expr} < 128 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS low_throughput_ratio,
                ROUND(AVG({negative_action_expr}), 4) AS negative_action_ratio,
                ROUND(AVG({risky_state_expr}), 4) AS risky_state_ratio,
                ROUND(AVG({loss_pct_expr}), 4) AS avg_packet_loss_pct,
                ROUND(AVG({jitter_ms_expr}), 2) AS avg_jitter_ms,
                ROUND(AVG({rtt_ms_expr}), 2) AS avg_rtt_ms,
                ROUND(AVG(COALESCE({retrans_rate_expr}, 0.0)), 4) AS avg_retransmission_rate,
                ROUND(AVG(COALESCE({asymmetry_expr}, 0.0)), 4) AS avg_asymmetry,
                ROUND({missed_ratio_expr}, 4) AS missed_byte_ratio
            FROM flows
            {scoped_where(where_clause, "src_ip IS NOT NULL AND dst_ip IS NOT NULL")}
            GROUP BY 1, 2, 3
            HAVING COUNT(*) >= 2
            ORDER BY total_bytes DESC, flow_count DESC, src_ip ASC, dst_ip ASC
            LIMIT {max(limit * 20, 300)}
        """
        conversation_rows = fetch_rows(con, conversation_sql)
        file_result["conversation_hotspots"] = _sorted_hotspots(conversation_rows, limit=limit)
        results["summary"]["degraded_paths"] = sum(1 for row in file_result["conversation_hotspots"] if row.get("status") in {"degraded", "poor"})

        if {"timestamp", "relative_time_s", "start_relative_time_s"} & available:
            timing_sql = f"""
                WITH ordered AS (
                    SELECT
                        src_ip,
                        COALESCE(analysis_time_relative_s, EXTRACT(EPOCH FROM analysis_time_ts)) AS event_time_s
                    FROM flows
                    {scoped_where(where_clause, "src_ip IS NOT NULL AND (analysis_time_relative_s IS NOT NULL OR analysis_time_ts IS NOT NULL)")}
                ),
                gaps AS (
                    SELECT
                        src_ip,
                        event_time_s - LAG(event_time_s) OVER (PARTITION BY src_ip ORDER BY event_time_s) AS gap_s
                    FROM ordered
                )
                SELECT
                    src_ip,
                    COUNT(*) AS event_count,
                    ROUND(AVG(gap_s), 4) AS avg_gap_s,
                    ROUND(STDDEV_SAMP(gap_s), 4) AS gap_std_s,
                    ROUND(COALESCE(STDDEV_SAMP(gap_s) / NULLIF(AVG(gap_s), 0), 0), 4) AS gap_cv
                FROM gaps
                WHERE gap_s IS NOT NULL AND gap_s >= 0
                GROUP BY 1
                HAVING COUNT(*) >= 3
                ORDER BY gap_cv DESC, event_count DESC, src_ip ASC
                LIMIT {limit}
            """
            file_result["timing_hotspots"] = fetch_rows(con, timing_sql)
            results["summary"]["timing_hotspots"] = len(file_result["timing_hotspots"])
            if file_result["timing_hotspots"]:
                file_result["notes"].append(
                    "Timing hotspots use inter-event gap variability as a burstiness proxy. This is not the same as direct packet jitter unless jitter_ms is present."
                )
        else:
            file_result["notes"].append(
                "Ordered time fields are not available, so timing-instability hotspots could not be computed."
            )

        if file_result["measurement_profile"].get("mode") == "proxy_only":
            file_result["notes"].append(
                "Direct RTT/jitter/loss fields are absent. The action falls back to throughput, retransmission, asymmetry, missed_bytes, and failure-state evidence."
            )
        elif file_result["measurement_profile"].get("mode") == "mixed_direct_and_proxy":
            file_result["notes"].append(
                "Direct QoS measurements are only partially populated in the selected scope. Results combine packet-derived evidence with flow/session proxies."
            )

        if _safe_float(file_result["measurement_profile"].get("loss_coverage_ratio")) > 0:
            file_result["notes"].append(
                "packet_loss_pct is treated as packet-derived loss-like evidence when populated by preprocessing, not as a guaranteed ground-truth end-to-end loss measurement."
            )

        if results["summary"]["degraded_services"] > 0:
            file_result["recommendations"].append(
                "Use qos-analysis results to isolate degraded services first, then drill into session-review or packet-review for the worst paths."
            )
        if any(_safe_float(row.get("avg_retransmission_rate")) >= 0.03 for row in file_result["conversation_hotspots"]):
            file_result["recommendations"].append(
                "Retransmission pressure is elevated on at least one path; inspect packet-review for resets, out-of-order behavior, or handshake failure."
            )
        if any(_safe_float(row.get("avg_packet_loss_pct")) >= 1.0 or _safe_float(row.get("missed_byte_ratio")) >= 0.03 for row in file_result["conversation_hotspots"]):
            file_result["recommendations"].append(
                "Loss-like indicators are present. Validate whether this is actual delivery loss, sensor visibility loss, or selective blocking."
            )
        if any(_safe_float(row.get("low_throughput_ratio")) >= 0.30 for row in file_result["service_hotspots"]):
            file_result["recommendations"].append(
                "Sustained low-throughput flows are concentrated in at least one service bucket; compare with timeseries to distinguish congestion from background trickle traffic."
            )
        if not file_result["recommendations"]:
            file_result["recommendations"].append(
                "No strong QoS degradation surfaced in the selected scope. Use timeseries or protocol-review if the question is more workload-specific than quality-specific."
            )

        results["files_analyzed"].append(file_result)
    except Exception as exc:
        file_result["error"] = str(exc)
        results["files_analyzed"].append(file_result)

    return results


def format_results(results: dict[str, Any]) -> str:
    output: list[str] = []
    output.append("# QoS Analysis Results\n")

    summary = results["summary"]
    output.append("## Summary\n")
    output.append(f"- **Signals Available**: {summary['signals_available']}")
    output.append(f"- **Degraded Services**: {summary['degraded_services']}")
    output.append(f"- **Degraded Paths**: {summary['degraded_paths']}")
    output.append(f"- **Timing Hotspots**: {summary['timing_hotspots']}\n")

    for file_result in results["files_analyzed"]:
        output.append(f"\n## File: {file_result['file']}\n")

        if file_result.get("measurement_profile"):
            output.append("### Measurement Profile\n")
            output.append(
                format_dict_rows(
                    [file_result["measurement_profile"]],
                    [
                        "mode",
                        "confidence_floor",
                        "flow_count",
                        "rtt_coverage_ratio",
                        "jitter_coverage_ratio",
                        "loss_coverage_ratio",
                        "retransmission_coverage_ratio",
                        "evidence_sources",
                    ],
                )
            )

        if file_result.get("signals"):
            output.append("### Signal Availability\n")
            output.append(format_dict_rows(file_result["signals"], ["signal", "status", "detail"]))

        if file_result.get("service_hotspots"):
            output.append("\n### Service Quality Hotspots\n")
            output.append(
                format_dict_rows(
                    file_result["service_hotspots"],
                    [
                        "service_label",
                        "status",
                        "confidence",
                        "qos_degradation_score",
                        "avg_throughput_kbps",
                        "low_throughput_ratio",
                        "avg_packet_loss_pct",
                        "avg_retransmission_rate",
                        "avg_rtt_ms",
                        "avg_jitter_ms",
                        "dominant_reason",
                    ],
                )
            )

        if file_result.get("conversation_hotspots"):
            output.append("\n### Conversation Degradation Hotspots\n")
            output.append(
                format_dict_rows(
                    file_result["conversation_hotspots"],
                    [
                        "src_ip",
                        "dst_ip",
                        "service_label",
                        "status",
                        "confidence",
                        "qos_degradation_score",
                        "avg_throughput_kbps",
                        "negative_action_ratio",
                        "risky_state_ratio",
                        "missed_byte_ratio",
                        "dominant_reason",
                    ],
                )
            )

        if file_result.get("timing_hotspots"):
            output.append("\n### Timing Instability Proxies\n")
            output.append(
                format_dict_rows(
                    file_result["timing_hotspots"],
                    ["src_ip", "event_count", "avg_gap_s", "gap_std_s", "gap_cv"],
                )
            )

        if file_result.get("notes"):
            output.append("\n### Notes\n")
            for note in file_result["notes"]:
                output.append(f"- {note}")

        if file_result.get("recommendations"):
            output.append("\n### Recommendations\n")
            for recommendation in file_result["recommendations"]:
                output.append(f"- {recommendation}")

    append_file_errors(output, results)
    return "\n".join(output)


def build_skill_result_parts(results: dict[str, Any], raw_output: str) -> dict[str, Any]:
    summary = results.get("summary", {})
    files = results.get("files_analyzed", [])
    errors = [
        {"file": item.get("file", "selected scope"), "error": item["error"]}
        for item in files
        if item.get("error")
    ]

    service_hotspots: list[dict[str, Any]] = []
    conversation_hotspots: list[dict[str, Any]] = []
    timing_hotspots: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    measurement_profiles: list[dict[str, Any]] = []
    warnings: list[str] = []
    recommendations: list[str] = []

    for file_result in files:
        service_hotspots.extend(file_result.get("service_hotspots") or [])
        conversation_hotspots.extend(file_result.get("conversation_hotspots") or [])
        timing_hotspots.extend(file_result.get("timing_hotspots") or [])
        signals.extend(file_result.get("signals") or [])
        if file_result.get("measurement_profile"):
            measurement_profiles.append(file_result["measurement_profile"])
        warnings.extend(file_result.get("notes") or [])
        recommendations.extend(file_result.get("recommendations") or [])

    findings: list[dict[str, Any]] = []
    for index, row in enumerate(service_hotspots[:20], 1):
        status = row.get("status")
        if status not in {"degraded", "poor"}:
            continue
        severity = "high" if status == "poor" else "medium"
        findings.append(
            {
                "finding_id": f"f-qos-service-{index:03d}",
                "type": "service_qos_degradation",
                "severity": severity,
                "confidence": float(row.get("confidence") or 0.6),
                "title": f"QoS degradation for {row.get('service_label', 'unknown service')}",
                "description": row.get("dominant_reason", "Service bucket shows QoS degradation indicators."),
                "entities": [{"type": "service", "value": row.get("service_label", "")}],
                "evidence_refs": ["e-service-hotspots", "e-measurement-profile"],
                "recommended_actions": recommendations[:3],
            }
        )

    for index, row in enumerate(conversation_hotspots[:20], len(findings) + 1):
        status = row.get("status")
        if status not in {"degraded", "poor"}:
            continue
        severity = "high" if status == "poor" else "medium"
        findings.append(
            {
                "finding_id": f"f-qos-path-{index:03d}",
                "type": "conversation_qos_degradation",
                "severity": severity,
                "confidence": float(row.get("confidence") or 0.6),
                "title": f"QoS degradation on {row.get('src_ip', '')} -> {row.get('dst_ip', '')}",
                "description": row.get("dominant_reason", "Conversation shows QoS degradation indicators."),
                "entities": [
                    {"type": "src_ip", "value": row.get("src_ip", "")},
                    {"type": "dst_ip", "value": row.get("dst_ip", "")},
                    {"type": "service", "value": row.get("service_label", "")},
                ],
                "evidence_refs": ["e-conversation-hotspots", "e-measurement-profile"],
                "recommended_actions": recommendations[:3],
            }
        )

    evidence: list[dict[str, Any]] = []
    if measurement_profiles:
        evidence.append(
            {
                "evidence_id": "e-measurement-profile",
                "type": "table",
                "title": "Measurement Profile",
                "columns": list(measurement_profiles[0].keys()),
                "rows": measurement_profiles,
            }
        )
    if signals:
        evidence.append(
            {
                "evidence_id": "e-signal-availability",
                "type": "table",
                "title": "Signal Availability",
                "columns": list(signals[0].keys()),
                "rows": signals,
            }
        )
    if service_hotspots:
        evidence.append(
            {
                "evidence_id": "e-service-hotspots",
                "type": "table",
                "title": "Service Quality Hotspots",
                "columns": list(service_hotspots[0].keys()),
                "rows": service_hotspots,
            }
        )
    if conversation_hotspots:
        evidence.append(
            {
                "evidence_id": "e-conversation-hotspots",
                "type": "table",
                "title": "Conversation Degradation Hotspots",
                "columns": list(conversation_hotspots[0].keys()),
                "rows": conversation_hotspots,
            }
        )
    if timing_hotspots:
        evidence.append(
            {
                "evidence_id": "e-timing-hotspots",
                "type": "table",
                "title": "Timing Instability Proxies",
                "columns": list(timing_hotspots[0].keys()),
                "rows": timing_hotspots,
            }
        )
    evidence.append(
        {
            "evidence_id": "e-raw-report",
            "type": "text",
            "title": "Raw QoS Analysis Report",
            "content": raw_output,
        }
    )

    return {
        "summary": {
            "title": "QoS Analysis",
            "overview": (
                f"Found {summary.get('degraded_services', 0)} degraded service bucket(s), "
                f"{summary.get('degraded_paths', 0)} degraded path(s), and "
                f"{summary.get('timing_hotspots', 0)} timing hotspot(s)."
            ),
            "severity": "high" if summary.get("degraded_paths", 0) else "medium" if summary.get("degraded_services", 0) else "info",
            "confidence": 0.75 if any(profile.get("mode") != "proxy_only" for profile in measurement_profiles) else 0.6,
            "key_metrics": [
                {"name": "signals_available", "value": summary.get("signals_available", 0)},
                {"name": "degraded_services", "value": summary.get("degraded_services", 0)},
                {"name": "degraded_paths", "value": summary.get("degraded_paths", 0)},
                {"name": "timing_hotspots", "value": summary.get("timing_hotspots", 0)},
            ],
        },
        "findings": findings,
        "evidence": evidence,
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "files_with_errors": len(errors),
                "measurement_profiles": len(measurement_profiles),
                "service_hotspots_returned": len(service_hotspots),
                "conversation_hotspots_returned": len(conversation_hotspots),
            },
            "errors": errors,
        },
    }
