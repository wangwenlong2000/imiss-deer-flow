"""Structured SkillResult adapter for zeek-review text reports."""

from __future__ import annotations

import json
from collections import defaultdict
from contextlib import suppress
from pathlib import Path
from typing import Any

from analysis.anomaly_models import score_generic_candidates
from analysis.review import zeek_review_action
from utils.math import _safe_float_local, _safe_ratio_local
from utils.io import load_json
from utils.path import _metadata_candidates_for_file, repo_root, to_repo_relative_display


SEVERITY_ORDER = {
    "critical": 4,
    "high": 3,
    "medium": 2,
    "low": 1,
    "info": 0,
}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _zeek_value(row: dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return default


def _load_zeek_json_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            with suppress(json.JSONDecodeError):
                payload = json.loads(stripped)
                if isinstance(payload, dict):
                    rows.append(payload)
    return rows


def _discover_zeek_logs(files: list[str]) -> tuple[list[dict[str, Any]], dict[str, list[Path]]]:
    artifacts: list[dict[str, Any]] = []
    grouped_logs: dict[str, list[Path]] = defaultdict(list)
    seen_logs: set[str] = set()

    for file_name in files:
        path = Path(file_name)
        metadata = None
        for candidate in _metadata_candidates_for_file(path):
            metadata = load_json(candidate)
            if metadata:
                break

        if metadata and isinstance(metadata.get("zeek_artifacts"), list):
            for artifact in metadata.get("zeek_artifacts", []):
                if not isinstance(artifact, dict):
                    continue
                artifacts.append(artifact)
                for log_path_str in artifact.get("logs", []):
                    log_path = repo_root() / log_path_str
                    if log_path.exists():
                        resolved = str(log_path.resolve())
                        if resolved not in seen_logs:
                            grouped_logs[log_path.name].append(log_path)
                            seen_logs.add(resolved)
            continue

        zeek_root = path.parent / "zeek"
        if zeek_root.exists():
            discovered = sorted(p for p in zeek_root.rglob("*") if p.is_file() and p.suffix in {".log", ".json"})
            if discovered:
                artifacts.append(
                    {
                        "pcap_name": path.name,
                        "pcap_source": to_repo_relative_display(path),
                        "zeek_dir": to_repo_relative_display(zeek_root),
                        "logs": [to_repo_relative_display(p) for p in discovered],
                    }
                )
            for log_path in discovered:
                resolved = str(log_path.resolve())
                if resolved not in seen_logs:
                    grouped_logs[log_path.name].append(log_path)
                    seen_logs.add(resolved)

    return artifacts, grouped_logs


def execute_zeek_review(files: list[str], limit: int) -> dict[str, Any]:
    """Execute zeek-review analysis and return structured data dict."""
    artifacts, grouped_logs = _discover_zeek_logs(files)

    result: dict[str, Any] = {
        "artifacts": artifacts[:limit],
        "conn_rows": [],
        "dns_rows": [],
        "http_rows": [],
        "ssl_rows": [],
        "ssl_log_name": None,
        "weird_rows": [],
        "has_zeek_data": bool(grouped_logs),
    }

    if not grouped_logs:
        return result

    result["conn_rows"] = [row for path in grouped_logs.get("conn.log", []) for row in _load_zeek_json_rows(path)]
    result["dns_rows"] = [row for path in grouped_logs.get("dns.log", []) for row in _load_zeek_json_rows(path)]
    result["http_rows"] = [row for path in grouped_logs.get("http.log", []) for row in _load_zeek_json_rows(path)]

    ssl_log_name = "ssl.log" if "ssl.log" in grouped_logs else "tls.log" if "tls.log" in grouped_logs else None
    result["ssl_log_name"] = ssl_log_name
    if ssl_log_name:
        result["ssl_rows"] = [row for path in grouped_logs.get(ssl_log_name, []) for row in _load_zeek_json_rows(path)]

    result["weird_rows"] = [row for path in grouped_logs.get("weird.log", []) for row in _load_zeek_json_rows(path)]

    return result


def _top_n(items: dict[str, int], n: int) -> list[tuple[str, int]]:
    return sorted(items.items(), key=lambda item: (-item[1], item[0]))[:n]


def _compute_row_counts(data: dict[str, Any]) -> dict[str, int]:
    """Count rows per log type."""
    return {
        "conn_rows": len(data.get("conn_rows", [])),
        "dns_rows": len(data.get("dns_rows", [])),
        "http_rows": len(data.get("http_rows", [])),
        "ssl_rows": len(data.get("ssl_rows", [])),
        "weird_rows": len(data.get("weird_rows", [])),
    }


def _compute_coverage_metrics(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Compute which Zeek log types are present."""
    key_map = {
        "conn.log": "conn_rows",
        "dns.log": "dns_rows",
        "http.log": "http_rows",
        "ssl.log": "ssl_rows",
        "tls.log": "ssl_rows",
        "weird.log": "weird_rows",
    }
    metrics = []
    for log_type in ["conn.log", "dns.log", "http.log", "ssl.log", "tls.log", "weird.log"]:
        actual_key = key_map[log_type]
        present = bool(data.get(actual_key))
        metrics.append({"name": log_type, "value": 1 if present else 0})

    metrics.append({"name": "total_zeek_artifacts", "value": len(data.get("artifacts", []))})
    total_rows = sum(len(data.get(k, [])) for k in ["conn_rows", "dns_rows", "http_rows", "ssl_rows", "weird_rows"])
    metrics.append({"name": "total_zeek_rows", "value": total_rows})
    return metrics


def _zeek_semantic_candidates(
    conn_rows: list[dict[str, Any]],
    dns_rows: list[dict[str, Any]],
    http_rows: list[dict[str, Any]],
    ssl_rows: list[dict[str, Any]],
    weird_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    suspicious_states = {"S0", "REJ", "RSTO", "RSTR", "RSTOS0", "RSTRH", "SH", "SHR"}
    source_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "src_ip": "UNKNOWN",
            "conn_count": 0.0,
            "dns_queries": 0.0,
            "http_requests": 0.0,
            "tls_records": 0.0,
            "weird_events": 0.0,
            "failed_conn_count": 0.0,
            "nxdomain_count": 0.0,
            "http_error_count": 0.0,
            "_dst_ips": set(),
            "_services": set(),
            "_queries": set(),
            "_hosts": set(),
            "_server_names": set(),
            "_weird_names": set(),
            "_service_counts": defaultdict(int),
            "_rcode_counts": defaultdict(int),
            "_weird_counts": defaultdict(int),
            "_orig_bytes_total": 0.0,
            "_resp_bytes_total": 0.0,
        }
    )

    def ensure_src(row: dict[str, Any]) -> dict[str, Any]:
        src_ip = _zeek_value(row, "id.orig_h", default="UNKNOWN")
        stats = source_stats[src_ip]
        stats["src_ip"] = src_ip
        return stats

    for row in conn_rows:
        stats = ensure_src(row)
        stats["conn_count"] += 1.0
        stats["_dst_ips"].add(_zeek_value(row, "id.resp_h", default="UNKNOWN"))
        service = _zeek_value(row, "service", default="UNKNOWN")
        stats["_services"].add(service)
        stats["_service_counts"][service] += 1
        state = _zeek_value(row, "conn_state", default="UNKNOWN")
        if state in suspicious_states:
            stats["failed_conn_count"] += 1.0
        stats["_orig_bytes_total"] += _safe_float_local(row.get("orig_bytes"))
        stats["_resp_bytes_total"] += _safe_float_local(row.get("resp_bytes"))

    for row in dns_rows:
        stats = ensure_src(row)
        stats["dns_queries"] += 1.0
        query = _zeek_value(row, "query", default="UNKNOWN")
        stats["_queries"].add(query)
        rcode = _zeek_value(row, "rcode_name", default="UNKNOWN")
        stats["_rcode_counts"][rcode] += 1
        if rcode == "NXDOMAIN":
            stats["nxdomain_count"] += 1.0

    for row in http_rows:
        stats = ensure_src(row)
        stats["http_requests"] += 1.0
        host = _zeek_value(row, "host", default="UNKNOWN")
        stats["_hosts"].add(host)
        status_code = _zeek_value(row, "status_code", default="UNKNOWN")
        with suppress(ValueError):
            if int(status_code) >= 400:
                stats["http_error_count"] += 1.0

    for row in ssl_rows:
        stats = ensure_src(row)
        stats["tls_records"] += 1.0
        server_name = _zeek_value(row, "server_name", default="UNKNOWN")
        stats["_server_names"].add(server_name)

    for row in weird_rows:
        stats = ensure_src(row)
        stats["weird_events"] += 1.0
        name = _zeek_value(row, "name", default="UNKNOWN")
        stats["_weird_names"].add(name)
        stats["_weird_counts"][name] += 1

    candidate_rows: list[dict[str, Any]] = []
    for stats in source_stats.values():
        total_semantic_events = (
            float(stats["conn_count"])
            + float(stats["dns_queries"])
            + float(stats["http_requests"])
            + float(stats["tls_records"])
            + float(stats["weird_events"])
        )
        dominant_service = max(stats["_service_counts"].items(), key=lambda item: item[1])[0] if stats["_service_counts"] else "UNKNOWN"
        dominant_weird = max(stats["_weird_counts"].items(), key=lambda item: item[1])[0] if stats["_weird_counts"] else "NONE"
        dominant_dns_rcode = max(stats["_rcode_counts"].items(), key=lambda item: item[1])[0] if stats["_rcode_counts"] else "NONE"

        candidate_rows.append(
            {
                "src_ip": stats["src_ip"],
                "semantic_event_count": int(total_semantic_events),
                "conn_count": int(stats["conn_count"]),
                "dns_queries": int(stats["dns_queries"]),
                "http_requests": int(stats["http_requests"]),
                "tls_records": int(stats["tls_records"]),
                "weird_events": int(stats["weird_events"]),
                "unique_dst_ip": len(stats["_dst_ips"]),
                "unique_services": len(stats["_services"]),
                "unique_queries": len(stats["_queries"]),
                "unique_hosts": len(stats["_hosts"]),
                "unique_server_names": len(stats["_server_names"]),
                "unique_weird_names": len(stats["_weird_names"]),
                "failed_conn_ratio": round(_safe_ratio_local(float(stats["failed_conn_count"]), float(stats["conn_count"])), 4),
                "nxdomain_ratio": round(_safe_ratio_local(float(stats["nxdomain_count"]), float(stats["dns_queries"])), 4),
                "http_error_ratio": round(_safe_ratio_local(float(stats["http_error_count"]), float(stats["http_requests"])), 4),
                "weird_density": round(_safe_ratio_local(float(stats["weird_events"]), total_semantic_events), 4),
                "avg_orig_bytes": round(_safe_ratio_local(float(stats["_orig_bytes_total"]), float(stats["conn_count"])), 2),
                "avg_resp_bytes": round(_safe_ratio_local(float(stats["_resp_bytes_total"]), float(stats["conn_count"])), 2),
                "dominant_service": dominant_service,
                "dominant_weird": dominant_weird,
                "dominant_dns_rcode": dominant_dns_rcode,
            }
        )

    return candidate_rows


def build_skill_result_parts(data: dict[str, Any], report: str, limit: int) -> dict[str, Any]:
    """Build structured SkillResult for zeek-review action."""
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []

    has_zeek = data.get("has_zeek_data", False)
    row_counts = _compute_row_counts(data)

    # Coverage metrics
    coverage_metrics = _compute_coverage_metrics(data)
    metrics.extend(coverage_metrics)

    evidence.append({
        "evidence_id": "e-zeek-coverage",
        "type": "metric",
        "title": "Zeek Coverage Status",
        "metrics": metrics,
    })

    if not has_zeek:
        warnings.append({
            "code": "no_zeek_artifacts",
            "message": "No Zeek logs were found for the selected dataset. Re-run prepare_pcap.py on a machine with Zeek installed or choose a processed dataset that already contains zeek/ artifacts.",
            "severity": "warning",
        })
    else:
        present_logs = [k for k, v in row_counts.items() if v > 0]
        expected_logs = ["conn_rows", "dns_rows", "http_rows", "ssl_rows", "weird_rows"]
        missing_logs = [k for k in expected_logs if k not in present_logs]
        if missing_logs:
            warnings.append({
                "code": "partial_zeek_artifacts",
                "message": f"Zeek data is partial. Present: {', '.join(present_logs)}. Missing: {', '.join(missing_logs)}. Some review sections may be incomplete.",
                "severity": "info",
            })

    # Conn data
    if row_counts["conn_rows"] > 0:
        conn_rows = data["conn_rows"]
        state_counts: dict[str, int] = defaultdict(int)
        service_counts: dict[str, int] = defaultdict(int)
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

        state_columns = ["conn_state", "records"]
        state_table_rows = [[s, c] for s, c in _top_n(state_counts, limit)]
        evidence.append({
            "evidence_id": "e-zeek-conn-states",
            "type": "table",
            "title": "Zeek Connection State Distribution",
            "columns": state_columns,
            "rows": state_table_rows,
        })

        svc_columns = ["service", "records"]
        svc_table_rows = [[s, c] for s, c in _top_n(service_counts, limit)]
        evidence.append({
            "evidence_id": "e-zeek-services",
            "type": "table",
            "title": "Zeek Top Services",
            "columns": svc_columns,
            "rows": svc_table_rows,
        })

        pair_sorted = sorted(
            pair_counts.items(),
            key=lambda item: (-item[1]["count"], -(item[1]["orig_bytes"] + item[1]["resp_bytes"]), item[0][0], item[0][1]),
        )[:limit]
        pair_columns = ["src_ip", "dst_ip", "service", "records", "orig_bytes", "resp_bytes"]
        pair_table_rows = [
            [src, dst, svc, int(v["count"]), int(v["orig_bytes"]), int(v["resp_bytes"])]
            for (src, dst, svc), v in pair_sorted
        ]
        evidence.append({
            "evidence_id": "e-zeek-talker-pairs",
            "type": "table",
            "title": "Zeek Top Talker Pairs",
            "columns": pair_columns,
            "rows": pair_table_rows,
        })

    # DNS review
    if row_counts["dns_rows"] > 0:
        dns_rows = data["dns_rows"]
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

        dns_columns = ["query", "records"]
        dns_table_rows = [[q, c] for q, c in _top_n(query_counts, limit)]
        evidence.append({
            "evidence_id": "e-zeek-dns-review",
            "type": "table",
            "title": "Zeek DNS Top Queries",
            "columns": dns_columns,
            "rows": dns_table_rows,
        })

        rcode_columns = ["rcode_name", "records"]
        rcode_table_rows = [[s, c] for s, c in _top_n(rcode_counts, limit)]
        evidence.append({
            "evidence_id": "e-zeek-dns-rcodes",
            "type": "table",
            "title": "Zeek DNS Response Codes",
            "columns": rcode_columns,
            "rows": rcode_table_rows,
        })

        if nxdomain_sources:
            total_dns = len(dns_rows)
            total_nxdomain = sum(nxdomain_sources.values())
            nx_ratio = total_nxdomain / total_dns if total_dns > 0 else 0.0
            if nx_ratio >= 0.3:
                findings.append({
                    "finding_id": "f-zeek-high-nxdomain",
                    "type": "high_nxdomain_ratio",
                    "severity": "medium",
                    "confidence": 0.6,
                    "title": f"High DNS NXDOMAIN ratio: {nx_ratio:.1%}",
                    "description": f"{total_nxdomain} of {total_dns} DNS queries returned NXDOMAIN ({nx_ratio:.1%}). This may indicate DNS tunneling, domain generation algorithms, or misconfigured resolvers.",
                    "entities": [{"type": "metric", "value": f"nxdomain_ratio={nx_ratio:.4f}"}],
                    "evidence_refs": ["e-zeek-dns-rcodes", "e-zeek-dns-review"],
                    "recommended_actions": ["Review NXDOMAIN-heavy sources in diagnostics for potential tunneling or DGA activity"],
                })

            nx_columns = ["src_ip", "nxdomain_queries"]
            nx_table_rows = [[src, c] for src, c in _top_n(nxdomain_sources, limit)]
            evidence.append({
                "evidence_id": "e-zeek-nxdomain-sources",
                "type": "table",
                "title": "Zeek NXDOMAIN-Heavy Sources",
                "columns": nx_columns,
                "rows": nx_table_rows,
            })

    # HTTP review
    if row_counts["http_rows"] > 0:
        http_rows = data["http_rows"]
        host_counts: dict[str, int] = defaultdict(int)
        status_counts: dict[str, int] = defaultdict(int)
        for row in http_rows:
            host_counts[_zeek_value(row, "host", default="UNKNOWN")] += 1
            status_counts[_zeek_value(row, "status_code", default="UNKNOWN")] += 1

        http_columns = ["host", "requests"]
        http_table_rows = [[s, c] for s, c in _top_n(host_counts, limit)]
        evidence.append({
            "evidence_id": "e-zeek-http-review",
            "type": "table",
            "title": "Zeek HTTP Top Hosts",
            "columns": http_columns,
            "rows": http_table_rows,
        })

        total_http = len(http_rows)
        error_count = sum(c for s, c in status_counts.items() if s.isdigit() and int(s) >= 400)
        error_ratio = error_count / total_http if total_http > 0 else 0.0
        if error_ratio >= 0.3:
            findings.append({
                "finding_id": "f-zeek-high-http-errors",
                "type": "high_http_error_ratio",
                "severity": "low",
                "confidence": 0.5,
                "title": f"High HTTP error ratio: {error_ratio:.1%}",
                "description": f"{error_count} of {total_http} HTTP requests resulted in error status codes (4xx/5xx).",
                "entities": [{"type": "metric", "value": f"http_error_ratio={error_ratio:.4f}"}],
                "evidence_refs": ["e-zeek-http-review"],
                "recommended_actions": ["Review HTTP hosts returning errors for potential C2 or scanning activity"],
            })

    # TLS review
    if row_counts["ssl_rows"] > 0:
        ssl_rows_data = data["ssl_rows"]
        server_counts: dict[str, int] = defaultdict(int)
        version_counts: dict[str, int] = defaultdict(int)
        for row in ssl_rows_data:
            server_counts[_zeek_value(row, "server_name", default="UNKNOWN")] += 1
            version_counts[_zeek_value(row, "version", default="UNKNOWN")] += 1

        tls_columns = ["server_name", "records"]
        tls_table_rows = [[s, c] for s, c in _top_n(server_counts, limit)]
        evidence.append({
            "evidence_id": "e-zeek-tls-review",
            "type": "table",
            "title": "Zeek TLS Top Server Names",
            "columns": tls_columns,
            "rows": tls_table_rows,
        })

        for version, count in version_counts.items():
            if version in ("SSLv2", "SSLv3", "TLSv1.0", "TLSv1.1"):
                findings.append({
                    "finding_id": f"f-zeek-deprecated-tls-{version}",
                    "type": "deprecated_tls_version",
                    "severity": "medium",
                    "confidence": 0.7,
                    "title": f"Deprecated TLS version detected: {version}",
                    "description": f"{count} TLS record(s) using {version}. This version is deprecated and may indicate legacy or misconfigured systems.",
                    "entities": [{"type": "protocol", "value": version}],
                    "evidence_refs": ["e-zeek-tls-review"],
                    "recommended_actions": [f"Upgrade clients and servers to use TLSv1.2 or higher; {version} is no longer considered secure"],
                })
                break

    # Weird events
    if row_counts["weird_rows"] > 0:
        weird_rows = data["weird_rows"]
        weird_counts: dict[str, int] = defaultdict(int)
        for row in weird_rows:
            name = _zeek_value(row, "name", default="UNKNOWN")
            weird_counts[name] += 1

        weird_columns = ["name", "records"]
        weird_table_rows = [[s, c] for s, c in _top_n(weird_counts, limit)]
        evidence.append({
            "evidence_id": "e-zeek-weird-events",
            "type": "table",
            "title": "Zeek Weird Event Names",
            "columns": weird_columns,
            "rows": weird_table_rows,
        })

        total_weird = len(weird_rows)
        if total_weird >= 5:
            findings.append({
                "finding_id": "f-zeek-weird-events",
                "type": "zeek_weird_activity",
                "severity": "low",
                "confidence": 0.5,
                "title": f"Zeek weird events detected: {total_weird}",
                "description": f"{total_weird} Zeek weird log event(s) observed. Weird events indicate protocol anomalies or unexpected network behavior.",
                "entities": [{"type": "metric", "value": f"weird_events={total_weird}"}],
                "evidence_refs": ["e-zeek-weird-events"],
                "recommended_actions": ["Review weird event types and source IPs in the evidence table"],
            })

    # Semantic hotspots
    semantic_candidates = _zeek_semantic_candidates(
        conn_rows=data.get("conn_rows", []),
        dns_rows=data.get("dns_rows", []),
        http_rows=data.get("http_rows", []),
        ssl_rows=data.get("ssl_rows", []),
        weird_rows=data.get("weird_rows", []),
    )
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
            if _safe_float_local(row.get("tls_records")) >= 5 and _safe_float_local(row.get("unique_server_names")) <= 1:
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
            if _safe_float_local(row.get("tls_records")) >= 5 and _safe_float_local(row.get("unique_server_names")) <= 1:
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
        hotspots = scored_semantic[:limit]
        if hotspots:
            hs_columns = ["src_ip", "zeek_risk_score", "severity", "semantic_event_count", "conn_count", "dns_queries", "weird_events", "nxdomain_ratio", "failed_conn_ratio", "likely_reason"]
            hs_table_rows = [
                [r.get("src_ip"), r.get("zeek_risk_score"), r.get("severity"), r.get("semantic_event_count"), r.get("conn_count"), r.get("dns_queries"), r.get("weird_events"), r.get("nxdomain_ratio"), r.get("failed_conn_ratio"), r.get("likely_reason")]
                for r in hotspots
            ]
            evidence.append({
                "evidence_id": "e-zeek-semantic-hotspots",
                "type": "table",
                "title": "Zeek Semantic Hotspots",
                "columns": hs_columns,
                "rows": hs_table_rows,
            })

            for index, row in enumerate(hotspots, start=1):
                score = _safe_float_local(row.get("zeek_risk_score"))
                severity = str(row.get("severity") or "info").lower()
                if score >= 0.35:
                    findings.append({
                        "finding_id": f"f-zeek-semantic-{index:03d}",
                        "type": "zeek_semantic_hotspot",
                        "severity": severity,
                        "confidence": score,
                        "title": f"Zeek semantic hotspot: {row.get('src_ip')}",
                        "description": row.get("likely_reason") or "Source flagged by Zeek semantic analysis.",
                        "entities": [{"type": "src_ip", "value": row.get("src_ip")}],
                        "evidence_refs": ["e-zeek-semantic-hotspots", "e-zeek-coverage"],
                    })

    # Raw report
    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Zeek Review Output",
        "content": report,
    })

    # Fix evidence_refs
    existing_ids = {e["evidence_id"] for e in evidence}
    for finding in findings:
        finding["evidence_refs"] = [
            ref for ref in finding["evidence_refs"] if ref in existing_ids
        ]

    highest_severity = "info"
    for f in findings:
        if SEVERITY_ORDER.get(f.get("severity", "info"), 0) > SEVERITY_ORDER.get(highest_severity, 0):
            highest_severity = f["severity"]

    total_rows = sum(row_counts.values())
    overview_text = f"Zeek review of {len(data.get('artifacts', []))} artifact(s), {total_rows} total rows."
    if findings:
        overview_text += f" {len(findings)} finding(s) identified."

    return {
        "summary": {
            "title": "Zeek Review",
            "overview": overview_text,
            "severity": highest_severity,
            "confidence": None,
            "key_metrics": metrics,
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "has_zeek_data": has_zeek,
                "row_counts": row_counts,
            },
        },
    }
