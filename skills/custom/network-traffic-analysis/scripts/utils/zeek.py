from __future__ import annotations

import json
import math
from collections import defaultdict
from contextlib import suppress
from pathlib import Path
from typing import Any

from utils.io import load_json
from utils.math import _safe_float_local, _shannon_entropy
from utils.path import _metadata_candidates_for_file, repo_root, to_repo_relative_display
from analysis.signature_matching import scan_signature_hits


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


def _safe_ratio_local(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _private_ip_predicate(field_name: str) -> str:
    cidrs = [
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "100.64.0.0/10",
        "fc00::/7",
    ]
    return "(" + " OR ".join(f"ip_in_cidr(CAST({field_name} AS VARCHAR), '{cidr}')" for cidr in cidrs) + ")"


def _text_entropy_local(value: str) -> float:
    text = str(value or "").strip().lower()
    if not text:
        return 0.0
    counts = defaultdict(int)
    for char in text:
        counts[char] += 1
    return _shannon_entropy([float(item) for item in counts.values()])


def _coerce_event_seconds(value: Any) -> float | None:
    if value in (None, ""):
        return None
    with suppress(TypeError, ValueError):
        return float(value)
    return None


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
            "tls_missing_sni_count": 0.0,
            "_dst_ips": set(),
            "_services": set(),
            "_queries": set(),
            "_hosts": set(),
            "_server_names": set(),
            "_known_sni_count": 0,
            "_weird_names": set(),
            "_service_counts": defaultdict(int),
            "_rcode_counts": defaultdict(int),
            "_weird_counts": defaultdict(int),
            "_orig_bytes_total": 0.0,
            "_resp_bytes_total": 0.0,
            "tls_missing_sni_count": 0.0,
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
        if server_name and str(server_name).strip() not in ("", "-", "UNKNOWN"):
            stats["_server_names"].add(server_name)
            stats["_known_sni_count"] += 1
        else:
            stats["tls_missing_sni_count"] += 1

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
                "known_sni_count": stats["_known_sni_count"],
                "tls_missing_sni_count": int(stats["tls_missing_sni_count"]),
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


def _signature_source_candidates(
    records: list[dict[str, Any]],
    *,
    candidate_fields: list[str],
) -> tuple[list[dict[str, Any]], int]:
    hits = scan_signature_hits(records, candidate_fields=candidate_fields)
    source_summary: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "src_ip": "UNKNOWN",
            "total_hits": 0.0,
            "critical_hits": 0.0,
            "high_hits": 0.0,
            "medium_hits": 0.0,
            "_rules": set(),
            "_values": set(),
            "_fields": set(),
            "_categories": defaultdict(int),
        }
    )
    severity_weight = {"critical": 1.0, "high": 0.85, "medium": 0.55, "low": 0.3}
    for hit in hits:
        src_ip = str(hit.get("src_ip", "UNKNOWN"))
        bucket = source_summary[src_ip]
        bucket["src_ip"] = src_ip
        bucket["total_hits"] += severity_weight.get(str(hit.get("signature_severity", "low")), 0.3)
        if hit.get("signature_severity") == "critical":
            bucket["critical_hits"] += 1.0
        elif hit.get("signature_severity") == "high":
            bucket["high_hits"] += 1.0
        elif hit.get("signature_severity") == "medium":
            bucket["medium_hits"] += 1.0
        bucket["_rules"].add(str(hit.get("signature_rule_id", "UNKNOWN")))
        bucket["_values"].add(str(hit.get("matched_value", "")))
        bucket["_fields"].add(str(hit.get("matched_field", "UNKNOWN")))
        bucket["_categories"][str(hit.get("signature_category", "unknown"))] += 1

    candidates: list[dict[str, Any]] = []
    for bucket in source_summary.values():
        dominant_category = max(bucket["_categories"].items(), key=lambda item: item[1])[0] if bucket["_categories"] else "unknown"
        candidates.append(
            {
                "src_ip": bucket["src_ip"],
                "total_hits": round(float(bucket["total_hits"]), 3),
                "critical_hits": int(bucket["critical_hits"]),
                "high_hits": int(bucket["high_hits"]),
                "medium_hits": int(bucket["medium_hits"]),
                "unique_rules": len(bucket["_rules"]),
                "unique_values": len(bucket["_values"]),
                "matched_fields": len(bucket["_fields"]),
                "dominant_category": dominant_category,
            }
        )
    return candidates, len(hits)


def _zeek_value(row: dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return default
