from __future__ import annotations

import json
import math
import statistics
from collections import defaultdict
from contextlib import suppress
from pathlib import Path
from typing import Any

from analysis.anomaly_models import score_generic_candidates
from analysis.feature_engineering import rows_from_query
from analysis.signature_matching import scan_signature_hits
from core.schema_mapping import available_canonical_fields, ensure_required, quote_identifier
from utils.formatter import render_rows_section
from utils.math import _safe_float_local, _text_entropy_local, _coerce_event_seconds, _safe_ratio_local
from utils.io import load_json
from utils.path import _metadata_candidates_for_file, repo_root, to_repo_relative_display
from utils.zeek import _discover_zeek_logs, _load_zeek_json_rows, _zeek_value, _zeek_semantic_candidates, _signature_source_candidates
def dns_tunnel_review_action(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    limit: int,
) -> str:
    available = available_canonical_fields(mappings)
    ensure_required(mappings, ["src_ip"])

    has_flow_dns = any(field in available for field in {"dns_query", "dns_query_length", "dns_label_count", "dns_query_entropy"})
    _, grouped_logs = _discover_zeek_logs(files)
    dns_log_rows = [row for path in grouped_logs.get("dns.log", []) for row in _load_zeek_json_rows(path)]

    if not has_flow_dns and not dns_log_rows:
        return (
            "DNS tunnel review\n"
            "No DNS lexical fields or Zeek dns.log artifacts were found for the selected scope. "
            "Re-run prepare_pcap.py with DNS-aware preprocessing or choose a dataset containing DNS evidence."
        )

    sections: list[str] = []
    flow_candidate_map: dict[str, dict[str, Any]] = {}
    if has_flow_dns:
        timestamp_available = "analysis_time_relative_s" in available or "analysis_time_ts" in available
        event_time_expr = "COALESCE(analysis_time_relative_s, EXTRACT(EPOCH FROM analysis_time_ts))" if timestamp_available else "NULL"
        lexical_predicates: list[str] = []
        if "dns_query" in available:
            lexical_predicates.append("(dns_query IS NOT NULL AND dns_query != '')")
        if "dns_query_length" in available:
            lexical_predicates.append("COALESCE(dns_query_length, 0) > 0")
        if "dns_query_entropy" in available:
            lexical_predicates.append("COALESCE(dns_query_entropy, 0) > 0")
        flow_scope_predicate = " OR ".join(lexical_predicates) if lexical_predicates else "FALSE"
        flow_sql = f"""
            WITH dns_base AS (
                SELECT
                    src_ip,
                    COALESCE(dns_query, '') AS dns_query,
                    COALESCE(dns_query_length, LENGTH(COALESCE(dns_query, ''))) AS dns_query_length,
                    COALESCE(dns_label_count, CASE WHEN COALESCE(dns_query, '') = '' THEN 0 ELSE LENGTH(COALESCE(dns_query, '')) - LENGTH(REPLACE(COALESCE(dns_query, ''), '.', '')) + 1 END) AS dns_label_count,
                    COALESCE(dns_query_entropy, 0) AS dns_query_entropy,
                    {event_time_expr} AS event_time_s
                FROM flows
                {where_clause}
                {"AND" if where_clause else "WHERE"} src_ip IS NOT NULL
                  AND ({flow_scope_predicate})
            ),
            dns_intervals AS (
                SELECT
                    src_ip,
                    event_time_s - LAG(event_time_s) OVER (PARTITION BY src_ip ORDER BY event_time_s) AS delta_s
                FROM dns_base
                WHERE event_time_s IS NOT NULL
            )
            SELECT
                b.src_ip,
                COUNT(*) AS dns_records,
                COUNT(DISTINCT NULLIF(b.dns_query, '')) AS unique_queries,
                ROUND(AVG(NULLIF(b.dns_query_length, 0)), 2) AS avg_query_length,
                MAX(b.dns_query_length) AS max_query_length,
                ROUND(AVG(NULLIF(b.dns_label_count, 0)), 2) AS avg_label_count,
                ROUND(AVG(NULLIF(b.dns_query_entropy, 0)), 4) AS avg_query_entropy,
                ROUND(SUM(CASE WHEN b.dns_query_length >= 35 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS long_query_ratio,
                ROUND(SUM(CASE WHEN b.dns_query_entropy >= 3.6 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS high_entropy_ratio,
                ROUND(STDDEV_SAMP(i.delta_s), 4) AS interval_std,
                ROUND(AVG(i.delta_s), 4) AS interval_mean,
                ROUND(COALESCE(STDDEV_SAMP(i.delta_s) / NULLIF(AVG(i.delta_s), 0), 0), 4) AS interval_cv
            FROM dns_base b
            LEFT JOIN dns_intervals i
              ON b.src_ip = i.src_ip
            GROUP BY 1
            HAVING COUNT(*) >= 2
            ORDER BY dns_records DESC, unique_queries DESC, avg_query_entropy DESC, avg_query_length DESC, src_ip ASC
            LIMIT {max(limit * 50, 1000)}
        """
        _, flow_rows = rows_from_query(con, flow_sql)
        if flow_rows:
            sections.append(
                render_rows_section(
                    "Flow DNS lexical candidates",
                    ["src_ip", "dns_records", "unique_queries", "avg_query_length", "avg_label_count", "avg_query_entropy", "long_query_ratio", "high_entropy_ratio", "interval_cv"],
                    [
                        (
                            row.get("src_ip"),
                            row.get("dns_records"),
                            row.get("unique_queries"),
                            row.get("avg_query_length"),
                            row.get("avg_label_count"),
                            row.get("avg_query_entropy"),
                            row.get("long_query_ratio"),
                            row.get("high_entropy_ratio"),
                            row.get("interval_cv"),
                        )
                        for row in flow_rows[:limit]
                    ],
                )
            )
        flow_candidate_map = {str(row.get("src_ip")): row for row in flow_rows}

    zeek_candidate_map: dict[str, dict[str, Any]] = {}
    if dns_log_rows:
        zeek_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "src_ip": "UNKNOWN",
                "dns_queries": 0.0,
                "nxdomain_count": 0.0,
                "_queries": set(),
                "_query_lengths": [],
                "_label_counts": [],
                "_entropies": [],
                "_timestamps": [],
            }
        )
        for row in dns_log_rows:
            src_ip = _zeek_value(row, "id.orig_h", default="UNKNOWN")
            stats = zeek_stats[src_ip]
            stats["src_ip"] = src_ip
            stats["dns_queries"] += 1.0
            query = _zeek_value(row, "query", default="")
            if query:
                normalized = str(query).strip().lower().rstrip(".")
                stats["_queries"].add(normalized)
                stats["_query_lengths"].append(float(len(normalized)))
                stats["_label_counts"].append(float(normalized.count(".") + 1))
                stats["_entropies"].append(_text_entropy_local(normalized))
            rcode = _zeek_value(row, "rcode_name", default="UNKNOWN").upper()
            if rcode == "NXDOMAIN":
                stats["nxdomain_count"] += 1.0
            ts_value = _coerce_event_seconds(row.get("ts"))
            if ts_value is not None:
                stats["_timestamps"].append(ts_value)

        zeek_rows: list[dict[str, Any]] = []
        for stats in zeek_stats.values():
            timestamps = sorted(stats["_timestamps"])
            deltas = [timestamps[idx] - timestamps[idx - 1] for idx in range(1, len(timestamps)) if timestamps[idx] >= timestamps[idx - 1]]
            interval_mean = statistics.fmean(deltas) if deltas else 0.0
            interval_std = statistics.pstdev(deltas) if len(deltas) >= 2 else 0.0
            zeek_rows.append(
                {
                    "src_ip": stats["src_ip"],
                    "dns_queries": int(stats["dns_queries"]),
                    "unique_queries": len(stats["_queries"]),
                    "avg_query_length": round(statistics.fmean(stats["_query_lengths"]), 2) if stats["_query_lengths"] else 0.0,
                    "avg_label_count": round(statistics.fmean(stats["_label_counts"]), 2) if stats["_label_counts"] else 0.0,
                    "avg_query_entropy": round(statistics.fmean(stats["_entropies"]), 4) if stats["_entropies"] else 0.0,
                    "long_query_ratio": round(_safe_ratio_local(sum(1 for item in stats["_query_lengths"] if item >= 35), len(stats["_query_lengths"])), 4),
                    "high_entropy_ratio": round(_safe_ratio_local(sum(1 for item in stats["_entropies"] if item >= 3.6), len(stats["_entropies"])), 4),
                    "nxdomain_ratio": round(_safe_ratio_local(stats["nxdomain_count"], stats["dns_queries"]), 4),
                    "interval_mean": round(interval_mean, 4),
                    "interval_std": round(interval_std, 4),
                    "interval_cv": round(_safe_ratio_local(interval_std, interval_mean), 4) if interval_mean > 0 else 0.0,
                }
            )
        zeek_candidate_map = {str(row.get("src_ip")): row for row in zeek_rows}
        if zeek_rows:
            sections.append(
                render_rows_section(
                    "Zeek DNS semantic candidates",
                    ["src_ip", "dns_queries", "unique_queries", "avg_query_length", "avg_query_entropy", "long_query_ratio", "high_entropy_ratio", "nxdomain_ratio", "interval_cv"],
                    [
                        (
                            row.get("src_ip"),
                            row.get("dns_queries"),
                            row.get("unique_queries"),
                            row.get("avg_query_length"),
                            row.get("avg_query_entropy"),
                            row.get("long_query_ratio"),
                            row.get("high_entropy_ratio"),
                            row.get("nxdomain_ratio"),
                            row.get("interval_cv"),
                        )
                        for row in zeek_rows[:limit]
                    ],
                )
            )

    merged: list[dict[str, Any]] = []
    for src_ip in sorted(set(flow_candidate_map) | set(zeek_candidate_map)):
        flow_row = flow_candidate_map.get(src_ip, {})
        zeek_row = zeek_candidate_map.get(src_ip, {})
        merged.append(
            {
                "src_ip": src_ip,
                "flow_dns_records": int(_safe_float_local(flow_row.get("dns_records"))),
                "flow_unique_queries": int(_safe_float_local(flow_row.get("unique_queries"))),
                "flow_avg_query_length": round(_safe_float_local(flow_row.get("avg_query_length")), 2),
                "flow_max_query_length": round(_safe_float_local(flow_row.get("max_query_length")), 2),
                "flow_avg_label_count": round(_safe_float_local(flow_row.get("avg_label_count")), 2),
                "flow_avg_query_entropy": round(_safe_float_local(flow_row.get("avg_query_entropy")), 4),
                "flow_long_query_ratio": round(_safe_float_local(flow_row.get("long_query_ratio")), 4),
                "flow_high_entropy_ratio": round(_safe_float_local(flow_row.get("high_entropy_ratio")), 4),
                "flow_interval_cv": round(_safe_float_local(flow_row.get("interval_cv")), 4),
                "zeek_dns_queries": int(_safe_float_local(zeek_row.get("dns_queries"))),
                "zeek_unique_queries": int(_safe_float_local(zeek_row.get("unique_queries"))),
                "zeek_avg_query_length": round(_safe_float_local(zeek_row.get("avg_query_length")), 2),
                "zeek_avg_label_count": round(_safe_float_local(zeek_row.get("avg_label_count")), 2),
                "zeek_avg_query_entropy": round(_safe_float_local(zeek_row.get("avg_query_entropy")), 4),
                "zeek_long_query_ratio": round(_safe_float_local(zeek_row.get("long_query_ratio")), 4),
                "zeek_high_entropy_ratio": round(_safe_float_local(zeek_row.get("high_entropy_ratio")), 4),
                "zeek_nxdomain_ratio": round(_safe_float_local(zeek_row.get("nxdomain_ratio")), 4),
                "zeek_interval_cv": round(_safe_float_local(zeek_row.get("interval_cv")), 4),
            }
        )

    def dns_tunnel_rule_score(row: dict[str, Any]) -> float:
        score = 0.0
        if _safe_float_local(row.get("flow_avg_query_length")) >= 35 or _safe_float_local(row.get("zeek_avg_query_length")) >= 35:
            score += 0.2
        if _safe_float_local(row.get("flow_avg_query_entropy")) >= 3.6 or _safe_float_local(row.get("zeek_avg_query_entropy")) >= 3.6:
            score += 0.2
        if _safe_float_local(row.get("flow_long_query_ratio")) >= 0.3 or _safe_float_local(row.get("zeek_long_query_ratio")) >= 0.3:
            score += 0.15
        if _safe_float_local(row.get("flow_high_entropy_ratio")) >= 0.3 or _safe_float_local(row.get("zeek_high_entropy_ratio")) >= 0.3:
            score += 0.15
        if _safe_float_local(row.get("zeek_nxdomain_ratio")) >= 0.3 and _safe_float_local(row.get("zeek_dns_queries")) >= 5:
            score += 0.15
        regularity_candidates = [item for item in [_safe_float_local(row.get("flow_interval_cv")), _safe_float_local(row.get("zeek_interval_cv"))] if item > 0]
        regularity = min(regularity_candidates) if regularity_candidates else None
        if regularity is not None and regularity <= 0.4 and (_safe_float_local(row.get("flow_dns_records")) >= 5 or _safe_float_local(row.get("zeek_dns_queries")) >= 5):
            score += 0.15
        return min(1.0, score)

    def dns_tunnel_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
        reasons: list[str] = []
        if _safe_float_local(row.get("flow_avg_query_entropy")) >= 3.6 or _safe_float_local(row.get("zeek_avg_query_entropy")) >= 3.6:
            reasons.append("high_entropy_dns_queries")
        if _safe_float_local(row.get("flow_avg_query_length")) >= 35 or _safe_float_local(row.get("zeek_avg_query_length")) >= 35:
            reasons.append("long_dns_queries")
        if _safe_float_local(row.get("zeek_nxdomain_ratio")) >= 0.3:
            reasons.append("nxdomain_heavy_dns_pattern")
        regularity_candidates = [item for item in [_safe_float_local(row.get("flow_interval_cv")), _safe_float_local(row.get("zeek_interval_cv"))] if item > 0]
        regularity = min(regularity_candidates) if regularity_candidates else None
        if regularity is not None and regularity <= 0.4:
            reasons.append("regular_dns_intervals")
        if _safe_float_local(row.get("flow_unique_queries")) >= 10 or _safe_float_local(row.get("zeek_unique_queries")) >= 10:
            reasons.append("high_dns_query_cardinality")
        if not reasons and final_score >= 0.65:
            reasons.append("model_ranked_dns_tunnel_candidate")
        if not reasons and rule_score >= 0.35:
            reasons.append("rule_ranked_dns_tunnel_candidate")
        return ",".join(reasons) if reasons else "mixed_low_signal_dns_activity"

    scored = score_generic_candidates(
        merged,
        numeric_fields=["flow_dns_records", "flow_unique_queries", "flow_avg_query_length", "flow_max_query_length", "flow_avg_label_count", "flow_avg_query_entropy", "flow_long_query_ratio", "flow_high_entropy_ratio", "flow_interval_cv", "zeek_dns_queries", "zeek_unique_queries", "zeek_avg_query_length", "zeek_avg_label_count", "zeek_avg_query_entropy", "zeek_long_query_ratio", "zeek_high_entropy_ratio", "zeek_nxdomain_ratio", "zeek_interval_cv"],
        categorical_fields=[],
        rule_score_fn=dns_tunnel_rule_score,
        reason_fn=dns_tunnel_reason,
        output_field="dns_tunnel_risk_score",
        contamination=0.15,
        engine="hybrid",
    )

    sections.append(
        render_rows_section(
            "DNS tunnel hotspots",
            ["src_ip", "dns_tunnel_risk_score", "severity", "flow_unique_queries", "flow_avg_query_entropy", "flow_long_query_ratio", "zeek_nxdomain_ratio", "zeek_interval_cv", "likely_reason"],
            [(row.get("src_ip"), row.get("dns_tunnel_risk_score"), row.get("severity"), row.get("flow_unique_queries"), row.get("flow_avg_query_entropy"), row.get("flow_long_query_ratio"), row.get("zeek_nxdomain_ratio"), row.get("zeek_interval_cv"), row.get("likely_reason")) for row in scored[:limit]],
        )
    )

    if not dns_log_rows:
        sections.append("Zeek dns.log was not available for this scope, so DNS tunnel scoring currently relies on flow-level lexical and timing features only.")
    if not has_flow_dns:
        sections.append("Flow-level DNS lexical fields were not available, so DNS tunnel scoring is currently relying on Zeek DNS semantics only.")

    return "\n\n".join(section for section in sections if section)


def zeek_review_action(files: list[str], limit: int) -> str:
    artifacts, grouped_logs = _discover_zeek_logs(files)
    if not grouped_logs:
        return (
            "Zeek review\n"
            "No Zeek logs were found for the selected dataset. Re-run prepare_pcap.py on a machine with Zeek installed "
            "or choose a processed dataset that already contains zeek/ artifacts."
        )

    sections: list[str] = []
    inventory_rows = [
        (
            artifact.get("pcap_name", "UNKNOWN"),
            artifact.get("pcap_source", "UNKNOWN"),
            artifact.get("zeek_dir", ""),
            len(artifact.get("logs", [])),
        )
        for artifact in artifacts[:limit]
    ]
    sections.append(render_rows_section("Zeek artifact inventory", ["pcap_name", "pcap_source", "zeek_dir", "log_count"], inventory_rows))

    conn_rows = [row for path in grouped_logs.get("conn.log", []) for row in _load_zeek_json_rows(path)]
    if conn_rows:
        service_counts: dict[str, int] = defaultdict(int)
        state_counts: dict[str, int] = defaultdict(int)
        pair_counts: dict[tuple[str, str, str], dict[str, float]] = {}
        for row in conn_rows:
            service = _zeek_value(row, "service", default="UNKNOWN")
            state = _zeek_value(row, "conn_state", default="UNKNOWN")
            service_counts[service] += 1
            state_counts[state] += 1
            key = (
                _zeek_value(row, "id.orig_h", default="UNKNOWN"),
                _zeek_value(row, "id.resp_h", default="UNKNOWN"),
                service,
            )
            pair = pair_counts.setdefault(key, {"count": 0.0, "orig_bytes": 0.0, "resp_bytes": 0.0})
            pair["count"] += 1
            pair["orig_bytes"] += _safe_float_local(row.get("orig_bytes"))
            pair["resp_bytes"] += _safe_float_local(row.get("resp_bytes"))
        sections.append(
            render_rows_section(
                "Zeek conn.log top services",
                ["service", "records"],
                sorted(((service, count) for service, count in service_counts.items()), key=lambda item: (-item[1], item[0]))[:limit],
            )
        )
        sections.append(
            render_rows_section(
                "Zeek conn.log top connection states",
                ["conn_state", "records"],
                sorted(((state, count) for state, count in state_counts.items()), key=lambda item: (-item[1], item[0]))[:limit],
            )
        )
        sections.append(
            render_rows_section(
                "Zeek conn.log top talker pairs",
                ["src_ip", "dst_ip", "service", "records", "orig_bytes", "resp_bytes"],
                [
                    (src, dst, service, int(values["count"]), int(values["orig_bytes"]), int(values["resp_bytes"]))
                    for (src, dst, service), values in sorted(
                        pair_counts.items(),
                        key=lambda item: (-item[1]["count"], -(item[1]["orig_bytes"] + item[1]["resp_bytes"]), item[0][0], item[0][1]),
                    )[:limit]
                ],
            )
        )

    dns_rows = [row for path in grouped_logs.get("dns.log", []) for row in _load_zeek_json_rows(path)]
    if dns_rows:
        query_counts: dict[str, int] = defaultdict(int)
        rcode_counts: dict[str, int] = defaultdict(int)
        nxdomain_sources: dict[str, int] = defaultdict(int)
        for row in dns_rows:
            query = _zeek_value(row, "query", default="UNKNOWN")
            rcode = _zeek_value(row, "rcode_name", default="UNKNOWN")
            query_counts[query] += 1
            rcode_counts[rcode] += 1
            if rcode == "NXDOMAIN":
                nxdomain_sources[_zeek_value(row, "id.orig_h", default="UNKNOWN")] += 1
        sections.append(render_rows_section("Zeek dns.log top queries", ["query", "records"], sorted(((query, count) for query, count in query_counts.items()), key=lambda item: (-item[1], item[0]))[:limit]))
        sections.append(render_rows_section("Zeek dns.log response codes", ["rcode_name", "records"], sorted(((rcode, count) for rcode, count in rcode_counts.items()), key=lambda item: (-item[1], item[0]))[:limit]))
        if nxdomain_sources:
            sections.append(render_rows_section("Zeek dns.log NXDOMAIN-heavy sources", ["src_ip", "nxdomain_queries"], sorted(((src, count) for src, count in nxdomain_sources.items()), key=lambda item: (-item[1], item[0]))[:limit]))

    http_rows = [row for path in grouped_logs.get("http.log", []) for row in _load_zeek_json_rows(path)]
    if http_rows:
        host_counts: dict[str, int] = defaultdict(int)
        method_counts: dict[str, int] = defaultdict(int)
        status_counts: dict[str, int] = defaultdict(int)
        for row in http_rows:
            host_counts[_zeek_value(row, "host", default="UNKNOWN")] += 1
            method_counts[_zeek_value(row, "method", default="UNKNOWN")] += 1
            status_counts[_zeek_value(row, "status_code", default="UNKNOWN")] += 1
        sections.append(render_rows_section("Zeek http.log top hosts", ["host", "requests"], sorted(((host, count) for host, count in host_counts.items()), key=lambda item: (-item[1], item[0]))[:limit]))
        sections.append(render_rows_section("Zeek http.log methods", ["method", "records"], sorted(((method, count) for method, count in method_counts.items()), key=lambda item: (-item[1], item[0]))[:limit]))
        sections.append(render_rows_section("Zeek http.log status codes", ["status_code", "records"], sorted(((status, count) for status, count in status_counts.items()), key=lambda item: (-item[1], item[0]))[:limit]))

    ssl_log_name = "ssl.log" if "ssl.log" in grouped_logs else "tls.log" if "tls.log" in grouped_logs else None
    ssl_rows: list[dict[str, Any]] = []
    if ssl_log_name:
        ssl_rows = [row for path in grouped_logs.get(ssl_log_name, []) for row in _load_zeek_json_rows(path)]
        server_counts: dict[str, int] = defaultdict(int)
        version_counts: dict[str, int] = defaultdict(int)
        issuer_counts: dict[str, int] = defaultdict(int)
        for row in ssl_rows:
            server_counts[_zeek_value(row, "server_name", default="UNKNOWN")] += 1
            version_counts[_zeek_value(row, "version", default="UNKNOWN")] += 1
            issuer_counts[_zeek_value(row, "issuer", default="UNKNOWN")] += 1
        sections.append(render_rows_section(f"Zeek {ssl_log_name} top server names", ["server_name", "records"], sorted(((name, count) for name, count in server_counts.items()), key=lambda item: (-item[1], item[0]))[:limit]))
        sections.append(render_rows_section(f"Zeek {ssl_log_name} versions", ["version", "records"], sorted(((version, count) for version, count in version_counts.items()), key=lambda item: (-item[1], item[0]))[:limit]))
        if any(name != "UNKNOWN" for name in issuer_counts):
            sections.append(render_rows_section(f"Zeek {ssl_log_name} issuers", ["issuer", "records"], sorted(((issuer, count) for issuer, count in issuer_counts.items()), key=lambda item: (-item[1], item[0]))[:limit]))

    weird_rows = [row for path in grouped_logs.get("weird.log", []) for row in _load_zeek_json_rows(path)]
    if weird_rows:
        weird_counts: dict[str, int] = defaultdict(int)
        weird_sources: dict[tuple[str, str], int] = defaultdict(int)
        for row in weird_rows:
            name = _zeek_value(row, "name", default="UNKNOWN")
            weird_counts[name] += 1
            weird_sources[(_zeek_value(row, "id.orig_h", default="UNKNOWN"), name)] += 1
        sections.append(render_rows_section("Zeek weird.log top event names", ["name", "records"], sorted(((name, count) for name, count in weird_counts.items()), key=lambda item: (-item[1], item[0]))[:limit]))
        sections.append(render_rows_section("Zeek weird.log source hotspots", ["src_ip", "name", "records"], [(src, name, count) for (src, name), count in sorted(weird_sources.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))[:limit]]))

    semantic_candidates = _zeek_semantic_candidates(conn_rows=conn_rows, dns_rows=dns_rows, http_rows=http_rows, ssl_rows=ssl_rows if ssl_log_name else [], weird_rows=weird_rows)
    if semantic_candidates:
        def semantic_rule_score(row: dict[str, Any]) -> float:
            score = 0.0
            if _safe_float_local(row.get("failed_conn_ratio")) >= 0.35 and _safe_float_local(row.get("conn_count")) >= 5:
                score += 0.25
            if _safe_float_local(row.get("nxdomain_ratio")) >= 0.4 and _safe_float_local(row.get("dns_queries")) >= 3:
                score += 0.25
            if _safe_float_local(row.get("weird_density")) >= 0.15 and _safe_float_local(row.get("weird_events")) >= 2:
                score += 0.2
            if _safe_float_local(row.get("http_error_ratio")) >= 0.5 and _safe_float_local(row.get("http_requests")) >= 3:
                score += 0.15
            if _safe_float_local(row.get("unique_dst_ip")) >= 10:
                score += 0.15
            if _safe_float_local(row.get("tls_records")) >= 5 and _safe_float_local(row.get("known_sni_count")) >= 5 and _safe_float_local(row.get("unique_server_names")) <= 1:
                score += 0.1
            return min(1.0, score)

        def semantic_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
            reasons: list[str] = []
            if _safe_float_local(row.get("nxdomain_ratio")) >= 0.4 and _safe_float_local(row.get("dns_queries")) >= 3:
                reasons.append("dns_nxdomain_skew")
            if _safe_float_local(row.get("weird_events")) >= 2:
                reasons.append("zeek_weird_events_present")
            if _safe_float_local(row.get("failed_conn_ratio")) >= 0.35:
                reasons.append("high_failed_connection_ratio")
            if _safe_float_local(row.get("tls_records")) >= 5 and _safe_float_local(row.get("known_sni_count")) >= 5 and _safe_float_local(row.get("unique_server_names")) <= 1:
                reasons.append("repetitive_tls_destination_pattern")
            if _safe_float_local(row.get("http_error_ratio")) >= 0.5 and _safe_float_local(row.get("http_requests")) >= 3:
                reasons.append("http_error_skew")
            if not reasons and rule_score >= 0.45:
                reasons.append("rule_level_semantic_outlier")
            if not reasons and final_score >= 0.65:
                reasons.append("model_flagged_semantic_hotspot")
            return ",".join(reasons) if reasons else "mixed_low_signal_semantic_activity"

        scored_semantic = score_generic_candidates(
            semantic_candidates,
            numeric_fields=["semantic_event_count", "conn_count", "dns_queries", "http_requests", "tls_records", "weird_events", "unique_dst_ip", "unique_services", "unique_queries", "unique_hosts", "unique_server_names", "unique_weird_names", "failed_conn_ratio", "nxdomain_ratio", "http_error_ratio", "weird_density", "avg_orig_bytes", "avg_resp_bytes"],
            categorical_fields=["dominant_service", "dominant_weird", "dominant_dns_rcode"],
            rule_score_fn=semantic_rule_score,
            reason_fn=semantic_reason,
            output_field="zeek_risk_score",
            contamination=0.2,
            engine="hybrid",
        )
        sections.append(
            render_rows_section(
                "Zeek semantic hotspots (hybrid scoring)",
                ["src_ip", "zeek_risk_score", "severity", "semantic_event_count", "conn_count", "dns_queries", "weird_events", "nxdomain_ratio", "failed_conn_ratio", "likely_reason"],
                [(row.get("src_ip"), row.get("zeek_risk_score"), row.get("severity"), row.get("semantic_event_count"), row.get("conn_count"), row.get("dns_queries"), row.get("weird_events"), row.get("nxdomain_ratio"), row.get("failed_conn_ratio"), row.get("likely_reason")) for row in scored_semantic[:limit]],
            )
        )

    return "\n\n".join(sections)


def _signature_review_data(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> dict[str, Any]:
    """Return structured signature review data (not text)."""
    available = available_canonical_fields(mappings)
    candidate_fields = [field for field in ["dns_query", "tls_sni", "http_host", "service", "rule_name", "app_protocol"] if field in available]
    if not candidate_fields:
        return {
            "error": "No signature-capable semantic fields were found. Expected at least one of dns_query, tls_sni, http_host, service, rule_name, or app_protocol.",
        }

    select_columns = ["src_ip", "dst_ip", "dst_port", "protocol", "bytes", "packets"]
    select_columns.extend(field for field in candidate_fields if field not in select_columns)
    max_scan_rows = max(5000, limit * 1000)
    select_sql = ",\n                    ".join(select_columns)
    populated_predicate = " OR ".join(
        f"(COALESCE(CAST({quote_identifier(field)} AS VARCHAR), '') != '')" for field in candidate_fields
    )
    sql = f"""
        WITH base AS (
            SELECT * FROM flows
            {where_clause}
        )
        SELECT
            {select_sql}
        FROM base
        WHERE {populated_predicate}
        LIMIT {max_scan_rows}
    """
    result = con.execute(sql)
    columns = [item[0] for item in result.description]
    records = [{columns[idx]: row[idx] for idx in range(len(columns))} for row in result.fetchall()]
    if not records:
        return {
            "error": "No rows contained populated dns_query / tls_sni / http_host / service / rule_name / app_protocol fields in the selected scope.",
        }

    hits = scan_signature_hits(records, candidate_fields=candidate_fields)
    if not hits:
        return {
            "candidate_fields": candidate_fields,
            "scanned_rows": len(records),
            "hit_count": 0,
            "indicator_rows": [],
            "value_rows": [],
            "hotspot_rows": [],
        }

    rule_summary: dict[tuple[str, str, str], dict[str, Any]] = {}
    value_summary: dict[tuple[str, str, str], dict[str, Any]] = {}
    for hit in hits:
        rule_key = (
            str(hit.get("signature_rule_id", "UNKNOWN")),
            str(hit.get("signature_severity", "low")),
            str(hit.get("signature_category", "unknown")),
        )
        rule_bucket = rule_summary.setdefault(
            rule_key,
            {"count": 0, "src_ips": set(), "values": set(), "description": str(hit.get("signature_description", ""))},
        )
        rule_bucket["count"] += 1
        rule_bucket["src_ips"].add(str(hit.get("src_ip", "UNKNOWN")))
        rule_bucket["values"].add(str(hit.get("matched_value", "")))

        value_key = (
            str(hit.get("signature_rule_id", "UNKNOWN")),
            str(hit.get("matched_field", "UNKNOWN")),
            str(hit.get("matched_value", "")),
        )
        value_bucket = value_summary.setdefault(value_key, {"count": 0, "src_ips": set()})
        value_bucket["count"] += 1
        value_bucket["src_ips"].add(str(hit.get("src_ip", "UNKNOWN")))

    indicator_rows = [
        {
            "rule_id": rule_id,
            "severity": severity,
            "category": category,
            "hits": bucket["count"],
            "src_ips": len(bucket["src_ips"]),
            "distinct_values": len(bucket["values"]),
            "description": bucket["description"],
        }
        for (rule_id, severity, category), bucket in sorted(rule_summary.items(), key=lambda item: (-item[1]["count"], item[0][0]))[:limit]
    ]
    value_rows = [
        {
            "rule_id": rule_id,
            "field": field,
            "matched_value": matched_value,
            "hits": bucket["count"],
            "src_ips": len(bucket["src_ips"]),
        }
        for (rule_id, field, matched_value), bucket in sorted(
            value_summary.items(),
            key=lambda item: (-item[1]["count"], -len(item[1]["src_ips"]), item[0][0], item[0][1]),
        )[:limit]
    ]

    source_candidates, _ = _signature_source_candidates(records, candidate_fields=candidate_fields)

    def signature_rule_score(row: dict[str, Any]) -> float:
        score = 0.0
        if _safe_float_local(row.get("critical_hits")) >= 1:
            score += 0.35
        if _safe_float_local(row.get("high_hits")) >= 2:
            score += 0.25
        if _safe_float_local(row.get("unique_rules")) >= 2:
            score += 0.2
        if _safe_float_local(row.get("unique_values")) >= 3:
            score += 0.1
        if _safe_float_local(row.get("matched_fields")) >= 2:
            score += 0.1
        return min(1.0, score)

    def signature_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
        reasons: list[str] = []
        if _safe_float_local(row.get("critical_hits")) >= 1:
            reasons.append("critical_signature_match")
        if _safe_float_local(row.get("high_hits")) >= 2:
            reasons.append("multiple_high_confidence_indicators")
        if _safe_float_local(row.get("unique_rules")) >= 2:
            reasons.append("multi_rule_semantic_overlap")
        if row.get("dominant_category"):
            reasons.append(f"dominant_{row.get('dominant_category')}")
        if not reasons and final_score >= 0.65:
            reasons.append("model_ranked_signature_hotspot")
        if not reasons and rule_score >= 0.35:
            reasons.append("rule_ranked_signature_hotspot")
        return ",".join(reasons) if reasons else "low_signal_signature_activity"

    scored_sources = score_generic_candidates(
        source_candidates,
        numeric_fields=["total_hits", "critical_hits", "high_hits", "medium_hits", "unique_rules", "unique_values", "matched_fields"],
        categorical_fields=["dominant_category"],
        rule_score_fn=signature_rule_score,
        reason_fn=signature_reason,
        output_field="signature_risk_score",
        contamination=0.2,
        engine="hybrid",
    )

    return {
        "candidate_fields": candidate_fields,
        "scanned_rows": len(records),
        "hit_count": len(hits),
        "indicator_rows": indicator_rows,
        "value_rows": value_rows,
        "hotspot_rows": scored_sources[:limit],
    }


def signature_review_action(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> dict[str, Any]:
    """Return structured signature review data."""
    return _signature_review_data(con, mappings, where_clause, limit)


def _risk_fusion_review_data(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    limit: int,
) -> dict[str, Any]:
    """Return structured risk fusion review data (not text)."""
    available = available_canonical_fields(mappings)
    ensure_required(mappings, ["src_ip", "dst_ip", "dst_port", "bytes", "packets"])

    duration_expr = "COALESCE(duration_ms, flow_duration, 0)" if "duration_ms" in available else "COALESCE(flow_duration, 0)" if "flow_duration" in available else "0"
    action_negative_expr = (
        "CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END"
        if "action" in available
        else "0"
    )
    session_risky_expr = (
        "CASE WHEN UPPER(COALESCE(session_state, '')) IN ('RST', 'S0', 'REJ', 'SYN_ONLY') THEN 1 ELSE 0 END"
        if "session_state" in available
        else "0"
    )
    app_protocol_cardinality = "COUNT(DISTINCT app_protocol)" if "app_protocol" in available else "0"
    avg_byte_ratio_expr = (
        "AVG(COALESCE(src_to_dst_byte_ratio, 0)) AS avg_src_to_dst_byte_ratio,"
        if "src_to_dst_byte_ratio" in available
        else "CAST(NULL AS DOUBLE) AS avg_src_to_dst_byte_ratio,"
    )
    avg_packet_ratio_expr = (
        "AVG(COALESCE(src_to_dst_packet_ratio, 0)) AS avg_src_to_dst_packet_ratio,"
        if "src_to_dst_packet_ratio" in available
        else "CAST(NULL AS DOUBLE) AS avg_src_to_dst_packet_ratio,"
    )
    avg_byte_asymmetry_expr = (
        "AVG(COALESCE(byte_asymmetry, 0)) AS avg_byte_asymmetry,"
        if "byte_asymmetry" in available
        else "CAST(NULL AS DOUBLE) AS avg_byte_asymmetry,"
    )
    avg_packet_asymmetry_expr = (
        "AVG(COALESCE(packet_asymmetry, 0)) AS avg_packet_asymmetry,"
        if "packet_asymmetry" in available
        else "CAST(NULL AS DOUBLE) AS avg_packet_asymmetry,"
    )
    max_ttl_range_expr = (
        "MAX(COALESCE(ttl_range, 0)) AS max_ttl_range,"
        if "ttl_range" in available
        else "CAST(NULL AS DOUBLE) AS max_ttl_range,"
    )
    avg_dns_query_length_expr = (
        "AVG(CASE WHEN COALESCE(dns_query_length, 0) > 0 THEN COALESCE(dns_query_length, 0) END) AS avg_dns_query_length,"
        if "dns_query_length" in available
        else "CAST(NULL AS DOUBLE) AS avg_dns_query_length,"
    )
    avg_dns_label_count_expr = (
        "AVG(CASE WHEN COALESCE(dns_label_count, 0) > 0 THEN COALESCE(dns_label_count, 0) END) AS avg_dns_label_count,"
        if "dns_label_count" in available
        else "CAST(NULL AS DOUBLE) AS avg_dns_label_count,"
    )
    avg_dns_entropy_expr = (
        "AVG(CASE WHEN COALESCE(dns_query_entropy, 0) > 0 THEN COALESCE(dns_query_entropy, 0) END) AS avg_dns_query_entropy,"
        if "dns_query_entropy" in available
        else "CAST(NULL AS DOUBLE) AS avg_dns_query_entropy,"
    )

    flow_sql = f"""
        SELECT
            src_ip,
            COUNT(*) AS flow_count,
            SUM(COALESCE(bytes, 0)) AS total_bytes,
            AVG(COALESCE(bytes, 0)) AS avg_bytes,
            AVG(COALESCE(packets, 0)) AS avg_packets,
            COUNT(DISTINCT dst_ip) AS unique_dst_ip,
            COUNT(DISTINCT dst_port) AS unique_dst_port,
            {app_protocol_cardinality} AS unique_app_protocol,
            SUM(CASE WHEN COALESCE(bytes, 0) <= 300 AND COALESCE(packets, 0) <= 2 AND {duration_expr} <= 10 THEN 1 ELSE 0 END) AS short_like_flows,
            ROUND(SUM(CASE WHEN COALESCE(bytes, 0) <= 300 AND COALESCE(packets, 0) <= 2 AND {duration_expr} <= 10 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS short_like_ratio,
            ROUND(SUM({action_negative_expr}) * 1.0 / NULLIF(COUNT(*), 0), 4) AS negative_action_ratio,
            ROUND(SUM({session_risky_expr}) * 1.0 / NULLIF(COUNT(*), 0), 4) AS risky_state_ratio,
            {avg_byte_ratio_expr}
            {avg_packet_ratio_expr}
            {avg_byte_asymmetry_expr}
            {avg_packet_asymmetry_expr}
            {max_ttl_range_expr}
            {avg_dns_query_length_expr}
            {avg_dns_label_count_expr}
            {avg_dns_entropy_expr}
            SUM(CASE WHEN COALESCE(dns_query_length, 0) >= 35 OR COALESCE(dns_query_entropy, 0) >= 3.6 THEN 1 ELSE 0 END) AS dns_lexical_alert_flows
        FROM flows
        {where_clause}
        WHERE src_ip IS NOT NULL
        GROUP BY 1
        ORDER BY flow_count DESC, total_bytes DESC, src_ip ASC
        LIMIT {max(limit * 50, 2000)}
    """
    _, flow_rows = rows_from_query(con, flow_sql)

    artifacts, grouped_logs = _discover_zeek_logs(files)
    conn_rows = [row for path in grouped_logs.get("conn.log", []) for row in _load_zeek_json_rows(path)]
    dns_rows = [row for path in grouped_logs.get("dns.log", []) for row in _load_zeek_json_rows(path)]
    http_rows = [row for path in grouped_logs.get("http.log", []) for row in _load_zeek_json_rows(path)]
    ssl_log_name = "ssl.log" if "ssl.log" in grouped_logs else "tls.log" if "tls.log" in grouped_logs else None
    ssl_rows = [row for path in grouped_logs.get(ssl_log_name, []) for row in _load_zeek_json_rows(path)] if ssl_log_name else []
    weird_rows = [row for path in grouped_logs.get("weird.log", []) for row in _load_zeek_json_rows(path)]
    zeek_semantic_rows = _zeek_semantic_candidates(conn_rows, dns_rows, http_rows, ssl_rows, weird_rows)

    candidate_fields = [field for field in ["dns_query", "tls_sni", "http_host", "service", "rule_name", "app_protocol"] if field in available]
    signature_records: list[dict[str, Any]] = []
    signature_candidates_list: list[dict[str, Any]] = []
    signature_hit_count = 0
    if candidate_fields:
        select_columns = ["src_ip", "dst_ip", "dst_port", "protocol", "bytes", "packets"]
        select_columns.extend(field for field in candidate_fields if field not in select_columns)
        select_sql = ",\n                    ".join(select_columns)
        populated_predicate = " OR ".join(
            f"(COALESCE(CAST({quote_identifier(field)} AS VARCHAR), '') != '')" for field in candidate_fields
        )
        signature_query = f"""
            WITH base AS (
                SELECT * FROM flows
                {where_clause}
            )
            SELECT
                {select_sql}
            FROM base
            WHERE {populated_predicate}
            LIMIT {max(limit * 1000, 5000)}
        """
        result = con.execute(signature_query)
        columns = [item[0] for item in result.description]
        signature_records = [{columns[idx]: row[idx] for idx in range(len(columns))} for row in result.fetchall()]
        signature_candidates_list, signature_hit_count = _signature_source_candidates(signature_records, candidate_fields=candidate_fields)

    zeek_scored_map: dict[str, dict[str, Any]] = {}
    if zeek_semantic_rows:
        def semantic_rule_score(row: dict[str, Any]) -> float:
            score = 0.0
            if _safe_float_local(row.get("failed_conn_ratio")) >= 0.35 and _safe_float_local(row.get("conn_count")) >= 5:
                score += 0.25
            if _safe_float_local(row.get("nxdomain_ratio")) >= 0.4 and _safe_float_local(row.get("dns_queries")) >= 3:
                score += 0.25
            if _safe_float_local(row.get("weird_density")) >= 0.15 and _safe_float_local(row.get("weird_events")) >= 2:
                score += 0.2
            if _safe_float_local(row.get("http_error_ratio")) >= 0.5 and _safe_float_local(row.get("http_requests")) >= 3:
                score += 0.15
            if _safe_float_local(row.get("unique_dst_ip")) >= 10:
                score += 0.15
            if _safe_float_local(row.get("tls_records")) >= 5 and _safe_float_local(row.get("known_sni_count")) >= 5 and _safe_float_local(row.get("unique_server_names")) <= 1:
                score += 0.1
            return min(1.0, score)

        def semantic_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
            reasons: list[str] = []
            if _safe_float_local(row.get("nxdomain_ratio")) >= 0.4 and _safe_float_local(row.get("dns_queries")) >= 3:
                reasons.append("dns_nxdomain_skew")
            if _safe_float_local(row.get("weird_events")) >= 2:
                reasons.append("zeek_weird_events_present")
            if _safe_float_local(row.get("failed_conn_ratio")) >= 0.35:
                reasons.append("high_failed_connection_ratio")
            if _safe_float_local(row.get("tls_records")) >= 5 and _safe_float_local(row.get("known_sni_count")) >= 5 and _safe_float_local(row.get("unique_server_names")) <= 1:
                reasons.append("repetitive_tls_destination_pattern")
            if _safe_float_local(row.get("http_error_ratio")) >= 0.5 and _safe_float_local(row.get("http_requests")) >= 3:
                reasons.append("http_error_skew")
            if not reasons and rule_score >= 0.45:
                reasons.append("rule_level_semantic_outlier")
            if not reasons and final_score >= 0.65:
                reasons.append("model_flagged_semantic_hotspot")
            return ",".join(reasons) if reasons else "mixed_low_signal_semantic_activity"

        zeek_scored = score_generic_candidates(
            zeek_semantic_rows,
            numeric_fields=[
                "semantic_event_count",
                "conn_count",
                "dns_queries",
                "http_requests",
                "tls_records",
                "weird_events",
                "unique_dst_ip",
                "unique_services",
                "unique_queries",
                "unique_hosts",
                "unique_server_names",
                "unique_weird_names",
                "failed_conn_ratio",
                "nxdomain_ratio",
                "http_error_ratio",
                "weird_density",
                "avg_orig_bytes",
                "avg_resp_bytes",
            ],
            categorical_fields=["dominant_service", "dominant_weird", "dominant_dns_rcode"],
            rule_score_fn=semantic_rule_score,
            reason_fn=semantic_reason,
            output_field="zeek_risk_score",
            contamination=0.2,
            engine="hybrid",
        )
        zeek_scored_map = {str(row.get("src_ip")): row for row in zeek_scored}

    signature_scored_map: dict[str, dict[str, Any]] = {}
    if signature_candidates_list:
        def signature_rule_score(row: dict[str, Any]) -> float:
            score = 0.0
            if _safe_float_local(row.get("critical_hits")) >= 1:
                score += 0.35
            if _safe_float_local(row.get("high_hits")) >= 2:
                score += 0.25
            if _safe_float_local(row.get("unique_rules")) >= 2:
                score += 0.2
            if _safe_float_local(row.get("unique_values")) >= 3:
                score += 0.1
            if _safe_float_local(row.get("matched_fields")) >= 2:
                score += 0.1
            return min(1.0, score)

        def signature_reason_fn(row: dict[str, Any], final_score: float, rule_score: float) -> str:
            reasons: list[str] = []
            if _safe_float_local(row.get("critical_hits")) >= 1:
                reasons.append("critical_signature_match")
            if _safe_float_local(row.get("high_hits")) >= 2:
                reasons.append("multiple_high_confidence_indicators")
            if _safe_float_local(row.get("unique_rules")) >= 2:
                reasons.append("multi_rule_semantic_overlap")
            if row.get("dominant_category"):
                reasons.append(f"dominant_{row.get('dominant_category')}")
            if not reasons and final_score >= 0.65:
                reasons.append("model_ranked_signature_hotspot")
            if not reasons and rule_score >= 0.35:
                reasons.append("rule_ranked_signature_hotspot")
            return ",".join(reasons) if reasons else "low_signal_signature_activity"

        signature_scored = score_generic_candidates(
            signature_candidates_list,
            numeric_fields=["total_hits", "critical_hits", "high_hits", "medium_hits", "unique_rules", "unique_values", "matched_fields"],
            categorical_fields=["dominant_category"],
            rule_score_fn=signature_rule_score,
            reason_fn=signature_reason_fn,
            output_field="signature_risk_score",
            contamination=0.2,
            engine="hybrid",
        )
        signature_scored_map = {str(row.get("src_ip")): row for row in signature_scored}

    merged_candidates: list[dict[str, Any]] = []
    for flow_row in flow_rows:
        src_ip = str(flow_row.get("src_ip", "UNKNOWN"))
        zeek_row = zeek_scored_map.get(src_ip, {})
        signature_row = signature_scored_map.get(src_ip, {})
        merged_candidates.append(
            {
                "src_ip": src_ip,
                "flow_count": int(_safe_float_local(flow_row.get("flow_count"))),
                "total_bytes": round(_safe_float_local(flow_row.get("total_bytes")), 2),
                "avg_bytes": round(_safe_float_local(flow_row.get("avg_bytes")), 2),
                "avg_packets": round(_safe_float_local(flow_row.get("avg_packets")), 2),
                "unique_dst_ip": int(_safe_float_local(flow_row.get("unique_dst_ip"))),
                "unique_dst_port": int(_safe_float_local(flow_row.get("unique_dst_port"))),
                "unique_app_protocol": int(_safe_float_local(flow_row.get("unique_app_protocol"))),
                "short_like_ratio": round(_safe_float_local(flow_row.get("short_like_ratio")), 4),
                "negative_action_ratio": round(_safe_float_local(flow_row.get("negative_action_ratio")), 4),
                "risky_state_ratio": round(_safe_float_local(flow_row.get("risky_state_ratio")), 4),
                "avg_src_to_dst_byte_ratio": round(_safe_float_local(flow_row.get("avg_src_to_dst_byte_ratio")), 4),
                "avg_src_to_dst_packet_ratio": round(_safe_float_local(flow_row.get("avg_src_to_dst_packet_ratio")), 4),
                "avg_byte_asymmetry": round(_safe_float_local(flow_row.get("avg_byte_asymmetry")), 4),
                "avg_packet_asymmetry": round(_safe_float_local(flow_row.get("avg_packet_asymmetry")), 4),
                "max_ttl_range": round(_safe_float_local(flow_row.get("max_ttl_range")), 4),
                "avg_dns_query_length": round(_safe_float_local(flow_row.get("avg_dns_query_length")), 4),
                "avg_dns_label_count": round(_safe_float_local(flow_row.get("avg_dns_label_count")), 4),
                "avg_dns_query_entropy": round(_safe_float_local(flow_row.get("avg_dns_query_entropy")), 4),
                "dns_lexical_alert_flows": int(_safe_float_local(flow_row.get("dns_lexical_alert_flows"))),
                "zeek_risk_score": round(_safe_float_local(zeek_row.get("zeek_risk_score")), 4),
                "zeek_event_count": int(_safe_float_local(zeek_row.get("semantic_event_count"))),
                "zeek_weird_events": int(_safe_float_local(zeek_row.get("weird_events"))),
                "zeek_nxdomain_ratio": round(_safe_float_local(zeek_row.get("nxdomain_ratio")), 4),
                "zeek_failed_conn_ratio": round(_safe_float_local(zeek_row.get("failed_conn_ratio")), 4),
                "signature_risk_score": round(_safe_float_local(signature_row.get("signature_risk_score")), 4),
                "signature_total_hits": round(_safe_float_local(signature_row.get("total_hits")), 3),
                "signature_critical_hits": int(_safe_float_local(signature_row.get("critical_hits"))),
                "signature_high_hits": int(_safe_float_local(signature_row.get("high_hits"))),
                "signature_unique_rules": int(_safe_float_local(signature_row.get("unique_rules"))),
                "signature_unique_values": int(_safe_float_local(signature_row.get("unique_values"))),
                "signature_category": signature_row.get("dominant_category", "none"),
            }
        )

    def fusion_rule_score(row: dict[str, Any]) -> float:
        score = 0.0
        if _safe_float_local(row.get("unique_dst_ip")) >= 10 or _safe_float_local(row.get("unique_dst_port")) >= 10:
            score += 0.15
        if _safe_float_local(row.get("short_like_ratio")) >= 0.35 and _safe_float_local(row.get("flow_count")) >= 10:
            score += 0.15
        if _safe_float_local(row.get("negative_action_ratio")) >= 0.3:
            score += 0.1
        if _safe_float_local(row.get("risky_state_ratio")) >= 0.3:
            score += 0.1
        if _safe_float_local(row.get("avg_byte_asymmetry")) >= 0.75:
            score += 0.12
        if _safe_float_local(row.get("avg_packet_asymmetry")) >= 0.75:
            score += 0.08
        if _safe_float_local(row.get("max_ttl_range")) >= 16:
            score += 0.07
        if _safe_float_local(row.get("avg_dns_query_entropy")) >= 3.6 and _safe_float_local(row.get("avg_dns_query_length")) >= 30:
            score += 0.1
        if _safe_float_local(row.get("dns_lexical_alert_flows")) >= 3:
            score += 0.08
        if _safe_float_local(row.get("zeek_risk_score")) >= 0.65:
            score += 0.25
        if _safe_float_local(row.get("signature_risk_score")) >= 0.65:
            score += 0.25
        if _safe_float_local(row.get("signature_critical_hits")) >= 1:
            score += 0.15
        if _safe_float_local(row.get("zeek_risk_score")) >= 0.5 and _safe_float_local(row.get("signature_risk_score")) >= 0.5:
            score += 0.1
        return min(1.0, score)

    def fusion_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
        reasons: list[str] = []
        if _safe_float_local(row.get("signature_critical_hits")) >= 1:
            reasons.append("critical_signature_indicator")
        if _safe_float_local(row.get("signature_risk_score")) >= 0.65:
            reasons.append("signature_hotspot")
        if _safe_float_local(row.get("zeek_risk_score")) >= 0.65:
            reasons.append("zeek_semantic_hotspot")
        if _safe_float_local(row.get("unique_dst_ip")) >= 10 or _safe_float_local(row.get("unique_dst_port")) >= 10:
            reasons.append("broad_destination_or_port_spread")
        if _safe_float_local(row.get("short_like_ratio")) >= 0.35:
            reasons.append("short_flow_heavy_pattern")
        if _safe_float_local(row.get("negative_action_ratio")) >= 0.3 or _safe_float_local(row.get("risky_state_ratio")) >= 0.3:
            reasons.append("flow_level_failure_pattern")
        if _safe_float_local(row.get("avg_byte_asymmetry")) >= 0.75 or _safe_float_local(row.get("avg_packet_asymmetry")) >= 0.75:
            reasons.append("directional_flow_asymmetry")
        if _safe_float_local(row.get("avg_dns_query_entropy")) >= 3.6 and _safe_float_local(row.get("avg_dns_query_length")) >= 30:
            reasons.append("dns_lexical_outlier")
        if _safe_float_local(row.get("max_ttl_range")) >= 16:
            reasons.append("ttl_variation_outlier")
        if not reasons and final_score >= 0.65:
            reasons.append("fused_model_ranked_source_hotspot")
        if not reasons and rule_score >= 0.35:
            reasons.append("fused_rule_ranked_source_hotspot")
        return ",".join(reasons) if reasons else "mixed_low_signal_fused_activity"

    scored_fusion = score_generic_candidates(
        merged_candidates,
        numeric_fields=[
            "flow_count",
            "total_bytes",
            "avg_bytes",
            "avg_packets",
            "unique_dst_ip",
            "unique_dst_port",
            "unique_app_protocol",
            "short_like_ratio",
            "negative_action_ratio",
            "risky_state_ratio",
            "avg_src_to_dst_byte_ratio",
            "avg_src_to_dst_packet_ratio",
            "avg_byte_asymmetry",
            "avg_packet_asymmetry",
            "max_ttl_range",
            "avg_dns_query_length",
            "avg_dns_label_count",
            "avg_dns_query_entropy",
            "dns_lexical_alert_flows",
            "zeek_risk_score",
            "zeek_event_count",
            "zeek_weird_events",
            "zeek_nxdomain_ratio",
            "zeek_failed_conn_ratio",
            "signature_risk_score",
            "signature_total_hits",
            "signature_critical_hits",
            "signature_high_hits",
            "signature_unique_rules",
            "signature_unique_values",
        ],
        categorical_fields=["signature_category"],
        rule_score_fn=fusion_rule_score,
        reason_fn=fusion_reason,
        output_field="final_risk_score",
        contamination=0.18,
        engine="hybrid",
    )

    risk_rows = scored_fusion[:limit]
    evidence_mix_rows = [
        {
            "src_ip": row.get("src_ip"),
            "flow_failure_pattern": round(max(_safe_float_local(row.get("negative_action_ratio")), _safe_float_local(row.get("risky_state_ratio"))), 4),
            "zeek_event_count": row.get("zeek_event_count"),
            "zeek_weird_events": row.get("zeek_weird_events"),
            "signature_total_hits": row.get("signature_total_hits"),
            "signature_category": row.get("signature_category"),
        }
        for row in risk_rows
    ]

    notes = []
    if not zeek_scored_map:
        notes.append("Zeek semantic evidence was not available for this scope, so the fused risk view is currently combining flow-level and signature-level evidence only.")
    if candidate_fields and signature_hit_count == 0:
        notes.append("No built-in signature indicators matched the selected flow scope, so signature contribution is neutral in the fused ranking.")

    return {
        "flow_sources": len(flow_rows),
        "zeek_sources": len(zeek_scored_map),
        "signature_sources": len(signature_scored_map),
        "signature_hits": signature_hit_count,
        "zeek_artifact_sets": len(artifacts),
        "risk_rows": risk_rows,
        "evidence_mix_rows": evidence_mix_rows,
        "notes": notes,
    }


def risk_fusion_review_action(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    limit: int,
) -> dict[str, Any]:
    """Fuse flow-level metrics, Zeek semantic evidence, and signature hits into a ranked risk view."""
    return _risk_fusion_review_data(con, mappings, where_clause, files, limit)
