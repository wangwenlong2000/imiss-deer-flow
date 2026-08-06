#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from file_resolution import is_explicit_path_reference, resolve_reference
from utils.path import repo_root, to_repo_relative_display, network_traffic_workspace_root

import os

# ---------------------------------------------------------------------------
# RAG v2 schema constants
# ---------------------------------------------------------------------------
RAG_DOC_SCHEMA_VERSION = "rag_doc_v2"

# Actions whose SkillResult output should be converted into action taxonomy docs
EVIDENCE_ACTIONS = [
    "inspect",
    "overview-report",
    "signature-review",
    "zeek-review",
    "dns-tunnel-review",
    "data-exfiltration-review",
    "lateral-movement-review",
    "risk-fusion-review",
]


def _processed_root_for_rag() -> Path:
    """Resolved processed root for RAG artifacts.

    Local: repo `datasets/network-traffic/processed`
    AIO: network_traffic_workspace_root / processed
    """
    env_path = os.environ.get("NETWORK_TRAFFIC_PROCESSED_ROOT")
    if env_path:
        return Path(env_path)
    ws = network_traffic_workspace_root()
    if ws.exists():
        return ws / "processed"
    return repo_root() / "datasets" / "network-traffic" / "processed"


MICROFLOW_DURATION_MS = 10
COMPACT_MICROFLOW_BYTES = 300
COMPACT_MICROFLOW_PACKETS = 2


def discover_files(values: list[str]) -> list[str]:
    files: list[str] = []
    for value in values:
        path = Path(value)
        if path.is_dir():
            files.extend(str(p) for p in sorted(path.rglob("*.flow.csv")))
        elif path.exists():
            files.append(str(path))
        elif is_explicit_path_reference(value):
            raise ValueError(f"Flow file path '{value}' does not exist.")
        else:
            files.extend(resolve_file_reference(value))
    deduped: list[str] = []
    seen: set[str] = set()
    for item in files:
        normalized = str(Path(item).resolve())
        if normalized not in seen:
            deduped.append(normalized)
            seen.add(normalized)
    return deduped


def resolve_file_reference(reference: str) -> list[str]:
    result = resolve_reference(reference)
    if result.status == "resolved":
        return result.matches
    if result.status == "ambiguous":
        sample = "\n".join(f"  - {to_repo_relative_display(path)}" for path in result.matches[:10])
        raise ValueError(
            f"Flow reference '{reference}' matched multiple datasets. Use a more specific path.\nCandidates:\n{sample}"
        )
    raise ValueError(result.message)


def sanitize_name(value: str) -> str:
    filtered = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in value.strip())
    return filtered.strip("-._") or "dataset"


def dataset_name_from_file(path: Path) -> str:
    name = path.name
    if name.endswith(".flow.csv"):
        return sanitize_name(name[:-9])
    return sanitize_name(path.stem)


def default_output_dir(dataset_name: str, files: list[str]) -> Path:
    if len(files) == 1:
        return Path(files[0]).resolve().parent / "rag"
    return _processed_root_for_rag() / dataset_name / "rag"


def parse_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def iso_hour_bucket(value: str) -> str:
    timestamp = parse_timestamp(value)
    if timestamp is None:
        return ""
    bucket = timestamp.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return bucket.isoformat()


def parse_boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def relative_hour_bucket(value: Any) -> str:
    seconds = parse_float(value)
    if seconds < 0:
        return ""
    bucket_start = int(seconds // 3600) * 3600
    return f"t+{bucket_start}s"


def row_time_value_seconds(row: dict[str, Any]) -> float | None:
    relative_value = row.get("start_relative_time_s") or row.get("relative_time_s")
    if parse_boolish(row.get("time_is_relative")) or (not row.get("timestamp") and relative_value not in (None, "")):
        seconds = parse_float(relative_value)
        return seconds if seconds >= 0 else None
    timestamp = parse_timestamp(row.get("timestamp", ""))
    if timestamp is None:
        return None
    return timestamp.timestamp()


def choose_time_bucket_seconds(rows: list[dict[str, Any]]) -> float:
    values = [value for value in (row_time_value_seconds(row) for row in rows) if value is not None]
    if len(values) < 2:
        return 1.0
    span_seconds = max(values) - min(values)
    if span_seconds <= 1:
        return 0.1
    if span_seconds <= 10:
        return 1.0
    if span_seconds <= 600:
        return 60.0
    return 3600.0


def adaptive_time_bucket_label(row: dict[str, Any], bucket_seconds: float) -> str:
    relative_value = row.get("start_relative_time_s") or row.get("relative_time_s")
    if parse_boolish(row.get("time_is_relative")) or (not row.get("timestamp") and relative_value not in (None, "")):
        seconds = parse_float(relative_value)
        if seconds < 0:
            return ""
        bucket_start = int(seconds // bucket_seconds) * bucket_seconds
        if bucket_seconds >= 1:
            return f"t+{int(bucket_start)}s"
        return f"t+{bucket_start:.1f}s"
    timestamp = parse_timestamp(row.get("timestamp", ""))
    if timestamp is None:
        return ""
    bucket_epoch = int(timestamp.timestamp() // bucket_seconds) * bucket_seconds
    bucket_dt = datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)
    if bucket_seconds >= 3600:
        bucket_dt = bucket_dt.replace(minute=0, second=0, microsecond=0)
    elif bucket_seconds >= 60:
        bucket_dt = bucket_dt.replace(second=0, microsecond=0)
    else:
        bucket_dt = bucket_dt.replace(microsecond=0)
    return bucket_dt.isoformat()


def row_time_bucket(row: dict[str, Any]) -> str:
    timestamp_bucket = iso_hour_bucket(row.get("timestamp", ""))
    if timestamp_bucket:
        return timestamp_bucket
    relative_value = row.get("start_relative_time_s") or row.get("relative_time_s")
    if parse_boolish(row.get("time_is_relative")) or (not row.get("timestamp") and relative_value not in (None, "")):
        return relative_hour_bucket(relative_value)
    return ""


def stable_id(*parts: str) -> str:
    payload = "|".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def compact_join(values: Iterable[str], separator: str = ", ") -> str:
    normalized = [value for value in values if value]
    return separator.join(normalized)


def top_values(counter: Counter[str], limit: int = 5) -> list[str]:
    return [item for item, _ in counter.most_common(limit) if item]


def top_ports(counter: Counter[int], limit: int = 5) -> list[int]:
    return [item for item, _ in counter.most_common(limit) if item]


def first_top_value(counter: Counter[str]) -> str:
    values = top_values(counter, 1)
    return values[0] if values else ""


def first_top_port(counter: Counter[int]) -> int:
    ports = top_ports(counter, 1)
    return ports[0] if ports else 0


def make_keywords(*groups: Iterable[str] | str) -> list[str]:
    values: list[str] = []
    for group in groups:
        if isinstance(group, str):
            candidates = [group]
        else:
            candidates = list(group)
        for candidate in candidates:
            value = str(candidate).strip()
            if value and value not in values:
                values.append(value)
    return values


def safe_metadata_value(value: Any) -> Any:
    if value in (None, ""):
        return ""
    return value


def ensure_required_columns(fieldnames: list[str] | None) -> None:
    if not fieldnames:
        raise ValueError("The flow CSV has no header row.")
    present = set(fieldnames)
    required = {"src_ip", "dst_ip", "protocol", "bytes", "packets"}
    missing = sorted(required - present)
    if missing:
        raise ValueError(f"Flow CSV is missing required columns: {', '.join(missing)}")
    if "timestamp" not in present and "start_relative_time_s" not in present:
        raise ValueError("Flow CSV must include either 'timestamp' or 'start_relative_time_s' for time-aware summaries.")


def load_flow_rows(files: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file_path in files:
        with open(file_path, encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            ensure_required_columns(reader.fieldnames)
            for row_index, row in enumerate(reader, start=1):
                row = dict(row)
                row["_raw_source_file"] = row.get("source_file", "")
                row["_input_file"] = str(Path(file_path).resolve())
                row["_row_index"] = str(row_index)
                row["_time_bucket"] = row_time_bucket(row)
                rows.append(row)
    return rows


def choose_risk_level(*scores: float) -> str:
    maximum = max(scores) if scores else 0.0
    if maximum >= 0.8:
        return "high"
    if maximum >= 0.4:
        return "medium"
    return "low"


def _sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    import hashlib
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compute_dataset_id(dataset_name: str, source_files: list[str]) -> str:
    """Compute a stable dataset_id from dataset_name + sorted source file hashes."""
    parts = [RAG_DOC_SCHEMA_VERSION, dataset_name]
    for sf in sorted(source_files):
        p = Path(sf)
        if p.exists():
            parts.append(_sha256_file(p))
    return stable_id(*parts)


def build_doc(
    *,
    doc_id: str,
    dataset_name: str,
    dataset_id: str = "",
    source_file: str,
    doc_type: str,
    title: str,
    content: str,
    summary: str,
    keywords: list[str],
    metadata: dict[str, Any],
    raw_source_file: str = "",
    source_sha256: str = "",
    artifact_generation_id: str = "",
) -> dict[str, Any]:
    source_file_display = to_repo_relative_display(source_file) if source_file else ""
    raw_source_file_display = to_repo_relative_display(raw_source_file) if raw_source_file else ""
    metadata = dict(metadata)
    if source_file_display:
        metadata["source_file"] = source_file_display
    if raw_source_file_display:
        metadata["raw_source_file"] = raw_source_file_display
    doc = {
        "doc_id": doc_id,
        "dataset_name": dataset_name,
        "dataset_id": dataset_id,
        "source_file": source_file_display,
        "doc_type": doc_type,
        "title": title,
        "content": content,
        "summary": summary,
        "keywords": keywords,
        "metadata": metadata,
    }
    if raw_source_file_display:
        doc["raw_source_file"] = raw_source_file_display
    if source_sha256:
        doc["source_sha256"] = source_sha256
    if artifact_generation_id:
        doc["artifact_generation_id"] = artifact_generation_id
    if metadata.get("provenance_type"):
        doc["provenance_type"] = metadata["provenance_type"]
    return doc


def flow_summary_doc_id(
    row: dict[str, Any],
    dataset_name: str,
    src_ip: str,
    dst_ip: str,
    src_port: Any,
    dst_port: Any,
    protocol: str,
    session_state: str,
) -> str:
    """Generate a unique doc_id for a flow_summary row.

    Priority: explicit flow_id > (_input_file + _row_index) > heuristic tuple.
    """
    flow_id = str(row.get("flow_id") or "").strip()
    if flow_id:
        return stable_id(RAG_DOC_SCHEMA_VERSION, dataset_name, "flow_summary", flow_id)

    source_file_raw = row.get("_input_file") or row.get("source_file") or ""
    source_file = to_repo_relative_display(source_file_raw) if source_file_raw else ""
    row_index = str(row.get("_row_index") or "").strip()
    if row_index:
        return stable_id(
            RAG_DOC_SCHEMA_VERSION,
            dataset_name,
            "flow_summary",
            source_file,
            row_index,
        )

    return stable_id(
        RAG_DOC_SCHEMA_VERSION,
        dataset_name,
        "flow_summary",
        source_file,
        row.get("_time_bucket", ""),
        src_ip,
        dst_ip,
        str(src_port),
        str(dst_port),
        protocol,
        session_state,
    )


def flow_summary_doc(row: dict[str, Any], dataset_name: str, dataset_id: str = "", source_sha256: str = "", artifact_generation_id: str = "") -> dict[str, Any]:
    src_ip = row.get("src_ip", "")
    dst_ip = row.get("dst_ip", "")
    src_port = row.get("src_port", "")
    dst_port = row.get("dst_port", "")
    protocol = row.get("protocol", "") or "UNKNOWN"
    app_protocol = row.get("app_protocol", "")
    service = row.get("service", "")
    traffic_family = row.get("traffic_family", "")
    session_state = row.get("session_state", "") or "unknown"
    bytes_total = parse_int(row.get("bytes") or row.get("bytes_total"))
    packets_total = parse_int(row.get("packets") or row.get("packet_count"))
    duration_ms = parse_int(row.get("duration_ms") or row.get("flow_duration"))
    dns_query = row.get("dns_query", "")
    tls_sni = row.get("tls_sni", "")
    http_host = row.get("http_host", "")
    tcp_flags_seen = row.get("tcp_flags_seen") or row.get("tcp_flags", "")
    direction = row.get("direction", "") or "unknown"
    title = f"{protocol} flow {src_ip}:{src_port} -> {dst_ip}:{dst_port}"
    content = (
        f"This network flow is a bidirectional session between {src_ip}:{src_port} and {dst_ip}:{dst_port} over {protocol}. "
        f"The session transferred {bytes_total} bytes in {packets_total} packets over {duration_ms} ms. "
        f"Session state is {session_state}. Observed traffic family is {traffic_family or 'unknown'}. "
        f"Application hints include DNS query {dns_query or 'none'}, TLS SNI {tls_sni or 'none'}, and HTTP host {http_host or 'none'}. "
        f"Direction roles are first_seen and reverse_seen. "
        f"App protocol is {app_protocol or 'unknown'} and service hint is {service or 'unknown'}. "
        f"TCP flags seen are {tcp_flags_seen or 'none'}."
    )
    summary = (
        f"{protocol} session {src_ip}:{src_port} to {dst_ip}:{dst_port}, "
        f"{bytes_total} bytes, {packets_total} packets, state {session_state}."
    )
    score = 0.0
    if session_state in {"RST", "SYN_ONLY"}:
        score += 0.7
    wide_short_connection = duration_ms <= MICROFLOW_DURATION_MS
    narrow_short_connection = (
        wide_short_connection
        and bytes_total <= COMPACT_MICROFLOW_BYTES
        and packets_total <= COMPACT_MICROFLOW_PACKETS
    )
    app_or_service = (app_protocol or service or "").upper()
    looks_like_normal_microflow = (
        session_state == "ACK"
        and packets_total <= 2
        and (app_or_service in {"TLS", "HTTPS", "HTTP", "DNS"} or dst_port in {"443", "80", "53"})
    )
    if narrow_short_connection and session_state in {"RST", "SYN_ONLY", "FIN"}:
        score += 0.3
    if dns_query or tls_sni or http_host or looks_like_normal_microflow:
        score = max(score - 0.2, 0.0)

    source_file = row.get("_input_file", row.get("source_file", ""))
    raw_source_file = row.get("_raw_source_file", "")
    metadata = {
        "protocol": protocol,
        "app_protocol": app_protocol or service,
        "traffic_family": traffic_family,
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "dst_port": parse_int(dst_port),
        "time_bucket": row.get("_time_bucket", ""),
        "risk_level": choose_risk_level(score),
        "is_short_connection": narrow_short_connection,
        "is_wide_short_connection": wide_short_connection,
        "is_narrow_short_connection": narrow_short_connection,
        "is_scan_like": session_state in {"RST", "SYN_ONLY"},
        "has_dns": bool(dns_query),
        "has_tls": bool(tls_sni),
        "has_http": bool(http_host),
        "state_bucket": session_state,
        "risk_bucket": choose_risk_level(score),
        "tags": make_keywords(
            [protocol.lower(), app_protocol.lower(), service.lower(), traffic_family.lower(), direction.lower()],
            [session_state.lower()],
            [value for value in [dns_query, tls_sni, http_host] if value],
            ["microflow"] if wide_short_connection else [],
        ),
        "source_file": to_repo_relative_display(source_file) if source_file else "",
        "raw_source_file": to_repo_relative_display(raw_source_file) if raw_source_file else "",
        "row_index": row.get("_row_index", ""),
        "flow_id": row.get("flow_id", ""),
        "provenance_type": "flow_row",
    }
    return build_doc(
        doc_id=flow_summary_doc_id(
            row,
            dataset_name,
            src_ip,
            dst_ip,
            src_port,
            dst_port,
            protocol,
            session_state,
        ),
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        raw_source_file=raw_source_file,
        source_file=to_repo_relative_display(source_file) if source_file else "",
        doc_type="flow_summary",
        title=title,
        content=content,
        summary=summary,
        keywords=make_keywords(
            protocol,
            app_protocol,
            service,
            traffic_family,
            session_state,
            dns_query,
            tls_sni,
            http_host,
            src_ip,
            dst_ip,
            dst_port,
        ),
        metadata=metadata,
        source_sha256=source_sha256,
        artifact_generation_id=artifact_generation_id,
    )


def is_wide_short_connection_row(row: dict[str, Any]) -> bool:
    return parse_int(row.get("duration_ms") or row.get("flow_duration")) <= MICROFLOW_DURATION_MS


def is_narrow_short_connection_row(row: dict[str, Any]) -> bool:
    return (
        is_wide_short_connection_row(row)
        and parse_int(row.get("bytes") or row.get("bytes_total")) <= COMPACT_MICROFLOW_BYTES
        and parse_int(row.get("packets") or row.get("packet_count")) <= COMPACT_MICROFLOW_PACKETS
    )


def endpoint_summary_docs(rows: list[dict[str, Any]], dataset_name: str, dataset_id: str = "", source_sha256: str = "", artifact_generation_id: str = "") -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "flows": 0,
            "bytes": 0,
            "packets": 0,
            "dst_ips": Counter(),
            "dst_ports": Counter(),
            "protocols": Counter(),
            "app_protocols": Counter(),
            "source_files": Counter(),
            "raw_source_files": Counter(),
            "traffic_family": Counter(),
            "time_buckets": Counter(),
            "wide_short_connection_count": 0,
            "narrow_short_connection_count": 0,
            "scan_like_rows": 0,
        }
    )
    for row in rows:
        src_ip = row.get("src_ip", "")
        if not src_ip:
            continue
        group = grouped[src_ip]
        group["flows"] += 1
        group["bytes"] += parse_int(row.get("bytes") or row.get("bytes_total"))
        group["packets"] += parse_int(row.get("packets") or row.get("packet_count"))
        if row.get("dst_ip"):
            group["dst_ips"][row["dst_ip"]] += 1
        dst_port = parse_int(row.get("dst_port"))
        if dst_port:
            group["dst_ports"][dst_port] += 1
        if row.get("protocol"):
            group["protocols"][row["protocol"]] += 1
        app_protocol = row.get("app_protocol", "") or row.get("service", "")
        if app_protocol:
            group["app_protocols"][app_protocol] += 1
        source_file = row.get("_input_file", row.get("source_file", ""))
        if source_file:
            group["source_files"][source_file] += 1
        raw_source_file = row.get("_raw_source_file", "")
        if raw_source_file:
            group["raw_source_files"][raw_source_file] += 1
        traffic_family = row.get("traffic_family", "")
        if traffic_family:
            group["traffic_family"][traffic_family] += 1
        if row.get("_time_bucket"):
            group["time_buckets"][row["_time_bucket"]] += 1
        if is_wide_short_connection_row(row):
            group["wide_short_connection_count"] += 1
        if is_narrow_short_connection_row(row):
            group["narrow_short_connection_count"] += 1
        if parse_int(row.get("dst_port")) >= 0 and row.get("dst_ip"):
            group["scan_like_rows"] += 1

    documents: list[dict[str, Any]] = []
    for host_ip, group in grouped.items():
        if group["flows"] < 3:
            continue
        unique_dst_ip = len(group["dst_ips"])
        unique_dst_port = len(group["dst_ports"])
        wide_short_pct = round(group["wide_short_connection_count"] * 100.0 / group["flows"], 2) if group["flows"] else 0.0
        narrow_short_pct = round(group["narrow_short_connection_count"] * 100.0 / group["flows"], 2) if group["flows"] else 0.0
        scan_like = unique_dst_ip >= 5 or unique_dst_port >= 10
        top_peers = top_values(group["dst_ips"])
        top_ports_list = top_ports(group["dst_ports"])
        top_protocols = top_values(group["protocols"])
        top_app_protocols = top_values(group["app_protocols"])
        dominant_bucket = first_top_value(group["time_buckets"])
        traffic_family = first_top_value(group["traffic_family"])
        source_file = first_top_value(group["source_files"])
        raw_source_file = first_top_value(group["raw_source_files"])
        title = f"Endpoint summary for {host_ip}"
        content = (
            f"Endpoint {host_ip} appears as a source host in {group['flows']} flows, "
            f"transferring {group['bytes']} bytes across {group['packets']} packets. "
            f"It contacts {unique_dst_ip} unique destination hosts and {unique_dst_port} unique destination ports. "
            f"Top peers are {compact_join(top_peers) or 'none'}. "
            f"Top destination ports are {compact_join(str(port) for port in top_ports_list) or 'none'}. "
            f"Top transport protocols are {compact_join(top_protocols) or 'unknown'}. "
            f"Application hints are {compact_join(top_app_protocols) or 'unknown'}. "
            f"Microflows (duration <= {MICROFLOW_DURATION_MS} ms) account for {group['wide_short_connection_count']} flows ({wide_short_pct:.2f}%). "
            f"Compact microflows (duration <= {MICROFLOW_DURATION_MS} ms, bytes <= {COMPACT_MICROFLOW_BYTES}, packets <= {COMPACT_MICROFLOW_PACKETS}) account for {group['narrow_short_connection_count']} flows ({narrow_short_pct:.2f}%). "
            f"Scan-like posture is {'present' if scan_like else 'not obvious'}. "
            f"Dominant time bucket is {dominant_bucket or 'unknown'}."
        )
        summary = (
            f"Endpoint {host_ip}: {group['flows']} flows, {group['bytes']} bytes, "
            f"{unique_dst_ip} unique destinations, microflow {wide_short_pct:.2f}%, compact-microflow {narrow_short_pct:.2f}%."
        )
        risk_score = max(
            min(unique_dst_ip / 20.0, 1.0),
            min(unique_dst_port / 20.0, 1.0),
        )
        documents.append(
            build_doc(
                doc_id=stable_id(dataset_id or dataset_name, "endpoint_summary", host_ip),
                dataset_name=dataset_name,
                dataset_id=dataset_id,
                source_file=source_file,
                raw_source_file=raw_source_file,
                doc_type="endpoint_summary",
                title=title,
                content=content,
                summary=summary,
                keywords=make_keywords(
                    host_ip,
                    top_peers,
                    [str(port) for port in top_ports_list],
                    top_protocols,
                    top_app_protocols,
                    ["endpoint", "host", "communication-profile"],
                ),
                metadata={
                    "protocol": top_protocols[0] if top_protocols else "",
                    "app_protocol": top_app_protocols[0] if top_app_protocols else "",
                    "traffic_family": traffic_family,
                    "src_ip": host_ip,
                    "dst_ip": top_peers[0] if top_peers else "",
                    "dst_port": top_ports_list[0] if top_ports_list else 0,
                    "time_bucket": dominant_bucket,
                    "risk_level": choose_risk_level(risk_score),
                    "is_short_connection": group["narrow_short_connection_count"] > 0,
                    "is_wide_short_connection": group["wide_short_connection_count"] > 0,
                    "is_narrow_short_connection": group["narrow_short_connection_count"] > 0,
                    "is_scan_like": scan_like,
                    "has_dns": "DNS" in top_app_protocols,
                    "has_tls": any("TLS" in value.upper() for value in top_app_protocols),
                    "has_http": any("HTTP" in value.upper() for value in top_app_protocols),
                    "state_bucket": "",
                    "risk_bucket": choose_risk_level(risk_score),
                    "tags": make_keywords(
                        "endpoint-summary",
                        "host-profile",
                        "scan-like" if scan_like else "",
                        "microflow" if group["narrow_short_connection_count"] else "",
                    ),
                    "source_file": to_repo_relative_display(source_file) if source_file else "",
                    "raw_source_file": to_repo_relative_display(raw_source_file) if raw_source_file else "",
                    "provenance_type": "flow_aggregate",
                    "row_count": group["flows"],
                },
                source_sha256=source_sha256,
                artifact_generation_id=artifact_generation_id,
            )
        )
    return documents


def port_summary_docs(rows: list[dict[str, Any]], dataset_name: str, dataset_id: str = "", source_sha256: str = "", artifact_generation_id: str = "") -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], dict[str, Any]] = defaultdict(
        lambda: {
            "flows": 0,
            "bytes": 0,
            "packets": 0,
            "src_ips": Counter(),
            "dst_ips": Counter(),
            "app_protocols": Counter(),
            "traffic_family": Counter(),
            "source_files": Counter(),
            "raw_source_files": Counter(),
            "time_buckets": Counter(),
            "wide_short_connection_count": 0,
            "narrow_short_connection_count": 0,
        }
    )
    for row in rows:
        dst_port = parse_int(row.get("dst_port"))
        if not dst_port:
            continue
        protocol = row.get("protocol", "") or "UNKNOWN"
        key = (protocol, dst_port)
        group = grouped[key]
        group["flows"] += 1
        group["bytes"] += parse_int(row.get("bytes") or row.get("bytes_total"))
        group["packets"] += parse_int(row.get("packets") or row.get("packet_count"))
        if row.get("src_ip"):
            group["src_ips"][row["src_ip"]] += 1
        if row.get("dst_ip"):
            group["dst_ips"][row["dst_ip"]] += 1
        app_protocol = row.get("app_protocol", "") or row.get("service", "")
        if app_protocol:
            group["app_protocols"][app_protocol] += 1
        traffic_family = row.get("traffic_family", "")
        if traffic_family:
            group["traffic_family"][traffic_family] += 1
        source_file = row.get("_input_file", row.get("source_file", ""))
        if source_file:
            group["source_files"][source_file] += 1
        raw_source_file = row.get("_raw_source_file", "")
        if raw_source_file:
            group["raw_source_files"][raw_source_file] += 1
        if row.get("_time_bucket"):
            group["time_buckets"][row["_time_bucket"]] += 1
        if is_wide_short_connection_row(row):
            group["wide_short_connection_count"] += 1
        if is_narrow_short_connection_row(row):
            group["narrow_short_connection_count"] += 1

    documents: list[dict[str, Any]] = []
    for (protocol, dst_port), group in grouped.items():
        if group["flows"] < 3:
            continue
        top_sources = top_values(group["src_ips"])
        top_destinations = top_values(group["dst_ips"])
        top_app_protocols = top_values(group["app_protocols"])
        traffic_family = first_top_value(group["traffic_family"])
        source_file = first_top_value(group["source_files"])
        raw_source_file = first_top_value(group["raw_source_files"])
        dominant_bucket = first_top_value(group["time_buckets"])
        wide_short_pct = round(group["wide_short_connection_count"] * 100.0 / group["flows"], 2) if group["flows"] else 0.0
        narrow_short_pct = round(group["narrow_short_connection_count"] * 100.0 / group["flows"], 2) if group["flows"] else 0.0
        title = f"Port summary for {protocol}/{dst_port}"
        content = (
            f"Destination port {dst_port} over {protocol} appears in {group['flows']} flows, "
            f"with {group['bytes']} bytes and {group['packets']} packets in total. "
            f"It is accessed by {len(group['src_ips'])} unique source hosts and targets {len(group['dst_ips'])} unique destination hosts. "
            f"Top source hosts are {compact_join(top_sources) or 'none'}. "
            f"Top destination hosts are {compact_join(top_destinations) or 'none'}. "
            f"Application protocol hints are {compact_join(top_app_protocols) or 'unknown'}. "
            f"Microflows (duration <= {MICROFLOW_DURATION_MS} ms) account for {group['wide_short_connection_count']} flows ({wide_short_pct:.2f}%). "
            f"Compact microflows account for {group['narrow_short_connection_count']} flows ({narrow_short_pct:.2f}%). "
            f"Dominant time bucket is {dominant_bucket or 'unknown'}."
        )
        summary = (
            f"Port {dst_port}/{protocol}: {group['flows']} flows, {group['bytes']} bytes, "
            f"{len(group['src_ips'])} unique sources, microflow {wide_short_pct:.2f}%, compact-microflow {narrow_short_pct:.2f}%."
        )
        risk_score = max(
            min(group["flows"] / 50.0, 1.0),
            min(len(group["src_ips"]) / 20.0, 1.0),
        )
        documents.append(
            build_doc(
                doc_id=stable_id(dataset_id or dataset_name, "port_summary", protocol, str(dst_port)),
                dataset_name=dataset_name,
                dataset_id=dataset_id,
                source_file=source_file,
                raw_source_file=raw_source_file,
                doc_type="port_summary",
                title=title,
                content=content,
                summary=summary,
                keywords=make_keywords(
                    protocol,
                    str(dst_port),
                    top_sources,
                    top_destinations,
                    top_app_protocols,
                    ["port-summary", "port-profile"],
                ),
                metadata={
                    "protocol": protocol,
                    "app_protocol": top_app_protocols[0] if top_app_protocols else "",
                    "traffic_family": traffic_family,
                    "src_ip": top_sources[0] if top_sources else "",
                    "dst_ip": top_destinations[0] if top_destinations else "",
                    "dst_port": dst_port,
                    "time_bucket": dominant_bucket,
                    "risk_level": choose_risk_level(risk_score),
                    "is_short_connection": group["narrow_short_connection_count"] > 0,
                    "is_wide_short_connection": group["wide_short_connection_count"] > 0,
                    "is_narrow_short_connection": group["narrow_short_connection_count"] > 0,
                    "is_scan_like": len(group["src_ips"]) >= 5,
                    "has_dns": "DNS" in top_app_protocols,
                    "has_tls": any("TLS" in value.upper() for value in top_app_protocols),
                    "has_http": any("HTTP" in value.upper() for value in top_app_protocols),
                    "state_bucket": "",
                    "risk_bucket": choose_risk_level(risk_score),
                    "tags": make_keywords("port-summary", "port-profile", protocol.lower(), str(dst_port)),
                    "source_file": to_repo_relative_display(source_file) if source_file else "",
                    "raw_source_file": to_repo_relative_display(raw_source_file) if raw_source_file else "",
                    "provenance_type": "flow_aggregate",
                    "row_count": group["flows"],
                },
                source_sha256=source_sha256,
                artifact_generation_id=artifact_generation_id,
            )
        )
    return documents


def protocol_summary_docs(rows: list[dict[str, Any]], dataset_name: str, dataset_id: str = "", source_sha256: str = "", artifact_generation_id: str = "") -> list[dict[str, Any]]:
    definitions = [
        ("dns_query", "DNS", "dns", "query"),
        ("tls_sni", "TLS", "tls", "sni"),
        ("http_host", "HTTP", "http", "host"),
    ]
    documents: list[dict[str, Any]] = []
    for field_name, label, tag_name, noun in definitions:
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            value = (row.get(field_name) or "").strip()
            if not value:
                continue
            group = grouped.setdefault(
                value,
                {
                    "flows": 0,
                    "bytes": 0,
                    "src_hosts": Counter(),
                    "dst_hosts": Counter(),
                    "dst_ports": Counter(),
                    "protocols": Counter(),
                    "app_protocols": Counter(),
                    "buckets": Counter(),
                    "source_files": Counter(),
                    "raw_source_files": Counter(),
                    "traffic_family": Counter(),
                    "wide_short_connections": 0,
                    "narrow_short_connections": 0,
                    "states": Counter(),
                },
            )
            group["flows"] += 1
            group["bytes"] += parse_int(row.get("bytes") or row.get("bytes_total"))
            group["src_hosts"][row.get("src_ip", "")] += 1
            group["dst_hosts"][row.get("dst_ip", "")] += 1
            group["dst_ports"][parse_int(row.get("dst_port"))] += 1
            group["protocols"][row.get("protocol", "")] += 1
            group["app_protocols"][row.get("app_protocol", "") or row.get("service", "")] += 1
            group["buckets"][row.get("_time_bucket", "")] += 1
            group["source_files"][row.get("_input_file", row.get("source_file", ""))] += 1
            if row.get("_raw_source_file"):
                group["raw_source_files"][row["_raw_source_file"]] += 1
            group["traffic_family"][row.get("traffic_family", "")] += 1
            if is_wide_short_connection_row(row):
                group["wide_short_connections"] += 1
            if is_narrow_short_connection_row(row):
                group["narrow_short_connections"] += 1
            if row.get("session_state"):
                group["states"][row["session_state"]] += 1

        for value, group in grouped.items():
            top_sources = top_values(group["src_hosts"])
            top_destinations = top_values(group["dst_hosts"])
            top_ports_list = top_ports(group["dst_ports"])
            top_protocols = top_values(group["protocols"])
            top_app_protocols = top_values(group["app_protocols"])
            source_file = first_top_value(group["source_files"])
            raw_source_file = first_top_value(group["raw_source_files"])
            traffic_family = first_top_value(group["traffic_family"])
            dominant_bucket = first_top_value(group["buckets"])
            top_states = top_values(group["states"])
            wide_short_pct = round(group["wide_short_connections"] * 100.0 / group["flows"], 2) if group["flows"] else 0.0
            narrow_short_pct = round(group["narrow_short_connections"] * 100.0 / group["flows"], 2) if group["flows"] else 0.0
            protocol_word = "DNS query" if field_name == "dns_query" else ("TLS SNI" if field_name == "tls_sni" else "HTTP host")
            title = f"{label} communication summary for {value}"
            content = (
                f"{label}-related traffic in this dataset frequently contacts {protocol_word.lower()} {value}. "
                f"Total flows: {group['flows']}. Total bytes: {group['bytes']}. "
                f"Main source hosts: {compact_join(top_sources) or 'none'}. "
                f"Main destination hosts: {compact_join(top_destinations) or 'none'}. "
                f"Main destination ports: {compact_join(str(port) for port in top_ports_list) or 'none'}. "
                f"Transport protocols seen: {compact_join(top_protocols) or 'unknown'}. "
                f"Application protocol hints: {compact_join(top_app_protocols) or 'unknown'}. "
                f"Dominant session states: {compact_join(top_states) or 'unknown'}. "
                f"Microflows (duration <= {MICROFLOW_DURATION_MS} ms) account for {group['wide_short_connections']} flows ({wide_short_pct:.2f}%). "
                f"Compact microflows account for {group['narrow_short_connections']} flows ({narrow_short_pct:.2f}%). "
                f"Dominant time bucket: {dominant_bucket or 'unknown'}."
            )
            summary = (
                f"{label} feature {value} appears in {group['flows']} flows with {group['bytes']} total bytes; "
                f"microflow {wide_short_pct:.2f}%, compact-microflow {narrow_short_pct:.2f}%."
            )
            flow_score = min(group["flows"] / 25.0, 1.0)
            metadata = {
                "protocol": top_protocols[0] if top_protocols else "",
                "app_protocol": label,
                "traffic_family": traffic_family,
                "src_ip": top_sources[0] if top_sources else "",
                "dst_ip": top_destinations[0] if top_destinations else "",
                "dst_port": top_ports_list[0] if top_ports_list else 0,
                "time_bucket": dominant_bucket,
                "risk_level": choose_risk_level(flow_score if field_name == "dns_query" else 0.2),
                "is_short_connection": group["narrow_short_connections"] > 0,
                "is_wide_short_connection": group["wide_short_connections"] > 0,
                "is_narrow_short_connection": group["narrow_short_connections"] > 0,
                "is_scan_like": False,
                "has_dns": field_name == "dns_query",
                "has_tls": field_name == "tls_sni",
                "has_http": field_name == "http_host",
                "state_bucket": top_states[0] if top_states else "",
                "risk_bucket": choose_risk_level(flow_score if field_name == "dns_query" else 0.2),
                "tags": make_keywords([tag_name, noun, label.lower(), "feature"], [value]),
                "source_file": to_repo_relative_display(source_file) if source_file else "",
                "raw_source_file": to_repo_relative_display(raw_source_file) if raw_source_file else "",
                "provenance_type": "flow_aggregate",
                "row_count": group["flows"],
            }
            documents.append(
                build_doc(
                    doc_id=stable_id(dataset_id or dataset_name, "protocol_summary", field_name, value),
                    dataset_name=dataset_name,
                    dataset_id=dataset_id,
                    source_file=to_repo_relative_display(source_file) if source_file else "",
                    raw_source_file=raw_source_file,
                    doc_type="protocol_summary",
                    title=title,
                    content=content,
                    summary=summary,
                    keywords=make_keywords(
                        value,
                        label,
                        top_protocols,
                        top_app_protocols,
                        top_states,
                        top_sources,
                        top_destinations,
                        [str(port) for port in top_ports_list],
                    ),
                    metadata=metadata,
                    source_sha256=source_sha256,
                    artifact_generation_id=artifact_generation_id,
                )
            )
    return documents


def short_connection_doc(rows: list[dict[str, Any]], dataset_name: str, dataset_id: str = "", source_sha256: str = "", artifact_generation_id: str = "") -> dict[str, Any]:
    total_flows = len(rows)
    wide_matched = [row for row in rows if is_wide_short_connection_row(row)]
    narrow_matched = [row for row in rows if is_narrow_short_connection_row(row)]
    top_sources = Counter(row.get("src_ip", "") for row in narrow_matched if row.get("src_ip"))
    protocols = Counter(row.get("protocol", "") for row in narrow_matched if row.get("protocol"))
    app_protocols = Counter((row.get("app_protocol", "") or row.get("service", "")) for row in wide_matched if (row.get("app_protocol", "") or row.get("service", "")))
    states = Counter(row.get("session_state", "") for row in wide_matched if row.get("session_state"))
    ports = Counter(parse_int(row.get("dst_port")) for row in narrow_matched if parse_int(row.get("dst_port")))
    source_files = Counter(row.get("_input_file", row.get("source_file", "")) for row in rows if row.get("_input_file", row.get("source_file", "")))
    raw_source_files = Counter(row.get("_raw_source_file", "") for row in rows if row.get("_raw_source_file", ""))
    traffic_families = Counter(row.get("traffic_family", "") for row in narrow_matched if row.get("traffic_family"))
    wide_short_pct = round((len(wide_matched) * 100.0 / total_flows), 2) if total_flows else 0.0
    narrow_short_pct = round((len(narrow_matched) * 100.0 / total_flows), 2) if total_flows else 0.0
    title = "Short-flow behavior summary"
    dominant_app = first_top_value(app_protocols)
    dominant_state = first_top_value(states)
    dominant_port = first_top_port(ports)
    tls_like = dominant_app.upper() in {"TLS", "HTTPS", "SSLV3", "SSL"} or dominant_port == 443
    content = (
        f"Short-flow behavior is {'prominent' if wide_matched else 'not prominent'} in this dataset. "
        f"{len(wide_matched)} of {total_flows} flows match the microflow heuristic (duration at most {MICROFLOW_DURATION_MS} ms), "
        f"which corresponds to {wide_short_pct:.2f}% of all flows. "
        f"{len(narrow_matched)} of {total_flows} flows match the compact microflow heuristic (duration at most {MICROFLOW_DURATION_MS} ms, bytes at most {COMPACT_MICROFLOW_BYTES}, packets at most {COMPACT_MICROFLOW_PACKETS}), "
        f"which corresponds to {narrow_short_pct:.2f}% of all flows. "
        f"Top compact-microflow source hosts include {compact_join(top_values(top_sources)) or 'none'}. "
        f"These flows are mainly associated with protocols {compact_join(top_values(protocols)) or 'none'}, "
        f"application hints {compact_join(top_values(app_protocols)) or 'none'}, "
        f"states {compact_join(top_values(states)) or 'none'}, "
        f"and destination ports {compact_join(str(port) for port in top_ports(ports)) or 'none'}. "
        f"{'The dominant pattern is compatible with normal TLS/web micro-transactions and should not be treated as an anomaly by itself. ' if tls_like and dominant_state == 'ACK' else ''}"
        "Use scan/failure evidence, port spread, and source concentration before treating short-flow prevalence as suspicious."
    )
    summary = (
        f"Microflow: {len(wide_matched)} / {total_flows} flows ({wide_short_pct:.2f}%). "
        f"Compact microflow: {len(narrow_matched)} / {total_flows} flows ({narrow_short_pct:.2f}%)."
    )
    risk_score = 0.2
    if dominant_state in {"RST", "SYN_ONLY", "FIN"}:
        risk_score = max(risk_score, 0.6)
    if not tls_like and wide_short_pct >= 80.0:
        risk_score = max(risk_score, 0.5)
    return build_doc(
        doc_id=stable_id(dataset_id or dataset_name, "behavior_summary", "short_connections"),
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        source_file=first_top_value(source_files) if source_files else rows[0].get("_input_file", rows[0].get("source_file", "")) if rows else "",
        raw_source_file=first_top_value(raw_source_files) if raw_source_files else "",
        doc_type="behavior_summary",
        title=title,
        content=content,
        summary=summary,
        keywords=make_keywords("microflow", "compact-microflow", top_values(top_sources), top_values(protocols), top_values(app_protocols), [str(port) for port in top_ports(ports)]),
        metadata={
            "protocol": first_top_value(protocols),
            "app_protocol": dominant_app,
            "traffic_family": first_top_value(traffic_families) if narrow_matched else "",
            "src_ip": first_top_value(top_sources),
            "dst_ip": "",
            "dst_port": first_top_port(ports),
            "time_bucket": "",
            "risk_level": choose_risk_level(risk_score),
            "is_short_connection": bool(narrow_matched),
            "is_wide_short_connection": bool(wide_matched),
            "is_narrow_short_connection": bool(narrow_matched),
            "is_scan_like": False,
            "has_dns": False,
            "has_tls": tls_like,
            "has_http": dominant_app.upper() == "HTTP",
            "state_bucket": dominant_state,
            "risk_bucket": choose_risk_level(risk_score),
            "tags": ["behavior-summary", "microflow", "compact-microflow"],
            "source_file": to_repo_relative_display(source_files.most_common(1)[0][0]) if source_files else "",
            "raw_source_file": to_repo_relative_display(first_top_value(raw_source_files)) if raw_source_files else "",
            "provenance_type": "flow_aggregate",
            "row_count": total_flows,
        },
        source_sha256=source_sha256,
        artifact_generation_id=artifact_generation_id,
    )


def scan_summary_doc(rows: list[dict[str, Any]], dataset_name: str, dataset_id: str = "", source_sha256: str = "", artifact_generation_id: str = "") -> dict[str, Any]:
    by_source: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "flows": 0,
            "dst_ips": set(),
            "dst_ports": set(),
            "protocols": Counter(),
            "total_bytes": 0,
        }
    )
    for row in rows:
        src_ip = row.get("src_ip", "")
        if not src_ip:
            continue
        record = by_source[src_ip]
        record["flows"] += 1
        if row.get("dst_ip"):
            record["dst_ips"].add(row["dst_ip"])
        port = parse_int(row.get("dst_port"))
        if port:
            record["dst_ports"].add(port)
        if row.get("protocol"):
            record["protocols"][row["protocol"]] += 1
        record["total_bytes"] += parse_int(row.get("bytes") or row.get("bytes_total"))

    suspicious: list[tuple[str, dict[str, Any], int, int]] = []
    for src_ip, record in by_source.items():
        distinct_ips = len(record["dst_ips"])
        distinct_ports = len(record["dst_ports"])
        if distinct_ips >= 5 or distinct_ports >= 10:
            suspicious.append((src_ip, record, distinct_ips, distinct_ports))
    suspicious.sort(key=lambda item: (item[2], item[3], item[1]["flows"], item[1]["total_bytes"]), reverse=True)
    top_sources = [item[0] for item in suspicious[:5]]
    protocols = Counter()
    for _, record, _, _ in suspicious:
        protocols.update(record["protocols"])
    suspicious_details = []
    for src_ip, record, distinct_ips, distinct_ports in suspicious[:5]:
        suspicious_details.append(
            f"{src_ip} ({record['flows']} flows, {distinct_ips} dst_ip, {distinct_ports} dst_port, {record['total_bytes']} bytes)"
        )
    title = "Scan-source anomaly summary"
    content = (
        f"Potential scan-source behavior is {'present' if suspicious else 'not obvious'} in this dataset. "
        f"{len(suspicious)} source hosts matched the flow-level scan heuristic requiring at least 5 distinct destination IPs "
        f"or at least 10 distinct destination ports. "
        f"Top suspicious sources include {compact_join(suspicious_details) or 'none'}. "
        f"Observed protocols among suspicious sources are {compact_join(top_values(protocols)) or 'none'}."
    )
    summary = f"{len(suspicious)} source hosts matched the scan heuristic."
    risk_score = 0.0
    if suspicious:
        max_ips = max(item[2] for item in suspicious)
        max_ports = max(item[3] for item in suspicious)
        max_flows = max(item[1]["flows"] for item in suspicious)
        risk_score = max(
            min(max_ips / 20.0, 1.0),
            min(max_ports / 20.0, 1.0),
            min(max_flows / 50.0, 1.0),
        )
    return build_doc(
        doc_id=stable_id(dataset_id or dataset_name, "anomaly_summary", "scan_sources"),
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        source_file=rows[0].get("_input_file", rows[0].get("source_file", "")) if rows else "",
        raw_source_file=rows[0].get("_raw_source_file", "") if rows else "",
        doc_type="anomaly_summary",
        title=title,
        content=content,
        summary=summary,
        keywords=make_keywords("scan", "probe", "broad-destination", top_sources, top_values(protocols)),
        metadata={
            "protocol": first_top_value(protocols),
            "app_protocol": "",
            "traffic_family": first_top_value(Counter(row.get("traffic_family", "") for row in rows if row.get("traffic_family"))) if rows else "",
            "src_ip": top_sources[0] if top_sources else "",
            "dst_ip": "",
            "dst_port": 0,
            "time_bucket": "",
            "risk_level": choose_risk_level(risk_score),
            "is_short_connection": False,
            "is_scan_like": bool(suspicious),
            "has_dns": False,
            "has_tls": False,
            "has_http": False,
            "state_bucket": "",
            "risk_bucket": choose_risk_level(risk_score),
            "tags": ["anomaly", "scan", "source"],
            "source_file": to_repo_relative_display(rows[0].get("_input_file", "")) if rows else "",
            "raw_source_file": to_repo_relative_display(rows[0].get("_raw_source_file", "")) if rows else "",
            "provenance_type": "flow_aggregate",
            "row_count": len(rows),
        },
        source_sha256=source_sha256,
        artifact_generation_id=artifact_generation_id,
    )


def peak_summary_doc(rows: list[dict[str, Any]], dataset_name: str, dataset_id: str = "", source_sha256: str = "", artifact_generation_id: str = "") -> dict[str, Any]:
    bucket_seconds = choose_time_bucket_seconds(rows)
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"flows": 0, "bytes": 0, "protocols": Counter()})
    for row in rows:
        bucket = adaptive_time_bucket_label(row, bucket_seconds)
        if not bucket:
            continue
        record = buckets[bucket]
        record["flows"] += 1
        record["bytes"] += parse_int(row.get("bytes") or row.get("bytes_total"))
        if row.get("protocol"):
            record["protocols"][row["protocol"]] += 1

    if buckets:
        ranked = sorted(buckets.items(), key=lambda item: (item[1]["bytes"], item[1]["flows"]), reverse=True)
        top_bucket, top_record = ranked[0]
        mean_bytes = sum(item["bytes"] for item in buckets.values()) / len(buckets)
        spike_ratio = (top_record["bytes"] / mean_bytes) if mean_bytes > 0 else 0.0
        spike_status = "spike" if len(buckets) > 1 and mean_bytes > 0 and top_record["bytes"] > mean_bytes * 2 else "normal"
        protocols = top_values(top_record["protocols"])
    else:
        top_bucket = ""
        top_record = {"flows": 0, "bytes": 0, "protocols": Counter()}
        mean_bytes = 0.0
        spike_ratio = 0.0
        spike_status = "normal"
        protocols = []

    title = "Traffic concentration summary"
    if len(buckets) <= 1:
        content = (
            f"Traffic concentration analysis used an adaptive time bucket of {bucket_seconds:g} seconds. "
            f"All observed traffic falls into a single bucket ({top_bucket or 'unknown'}), containing {top_record['flows']} flows and {top_record['bytes']} bytes. "
            "Because the capture spans only one effective time bucket, this result is descriptive and should not be treated as a traffic-spike anomaly by itself. "
            f"Dominant protocols in the bucket are {compact_join(protocols) or 'none'}."
        )
        summary = (
            f"Single-bucket traffic concentration at {top_bucket or 'unknown'} "
            f"with {top_record['bytes']} bytes; insufficient temporal spread for spike judgement."
        )
    else:
        content = (
            f"Traffic concentration analysis used an adaptive time bucket of {bucket_seconds:g} seconds. "
            f"The most active time bucket is {top_bucket or 'unknown'}, containing {top_record['flows']} flows and {top_record['bytes']} bytes. "
            f"The mean bucket volume is {mean_bytes:.2f} bytes and the concentration ratio is {spike_ratio:.2f}. "
            f"Concentration status is {spike_status}. "
            f"Dominant protocols in the peak bucket are {compact_join(protocols) or 'none'}."
        )
        summary = (
            f"Peak bucket {top_bucket or 'unknown'} with {top_record['bytes']} bytes, "
            f"ratio {spike_ratio:.2f}, status {spike_status}."
        )
    return build_doc(
        doc_id=stable_id(dataset_id or dataset_name, "behavior_summary", "traffic_peak"),
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        source_file=rows[0].get("_input_file", rows[0].get("source_file", "")) if rows else "",
        raw_source_file=rows[0].get("_raw_source_file", "") if rows else "",
        doc_type="behavior_summary",
        title=title,
        content=content,
        summary=summary,
        keywords=make_keywords("peak", "timeseries", "traffic-concentration", protocols, top_bucket),
        metadata={
            "protocol": protocols[0] if protocols else "",
            "app_protocol": "",
            "traffic_family": first_top_value(Counter(row.get("traffic_family", "") for row in rows if row.get("traffic_family"))) if rows else "",
            "src_ip": "",
            "dst_ip": "",
            "dst_port": 0,
            "time_bucket": top_bucket,
            "risk_level": choose_risk_level(min(spike_ratio / 2.0, 1.0) if spike_status == "spike" else 0.2 if len(buckets) > 1 else 0.0),
            "is_short_connection": False,
            "is_scan_like": False,
            "has_dns": False,
            "has_tls": False,
            "has_http": False,
            "state_bucket": spike_status,
            "risk_bucket": choose_risk_level(min(spike_ratio / 2.0, 1.0) if spike_status == "spike" else 0.2 if len(buckets) > 1 else 0.0),
            "tags": ["behavior-summary", "peak", "timeseries"],
            "source_file": to_repo_relative_display(rows[0].get("_input_file", "")) if rows else "",
            "raw_source_file": to_repo_relative_display(rows[0].get("_raw_source_file", "")) if rows else "",
            "provenance_type": "flow_aggregate",
            "row_count": len(rows),
        },
        source_sha256=source_sha256,
        artifact_generation_id=artifact_generation_id,
    )


def anomaly_summary_docs(rows: list[dict[str, Any]], dataset_name: str, dataset_id: str = "", source_sha256: str = "", artifact_generation_id: str = "") -> list[dict[str, Any]]:
    if not rows:
        return []
    return [
        short_connection_doc(rows, dataset_name, dataset_id, source_sha256, artifact_generation_id),
        scan_summary_doc(rows, dataset_name, dataset_id, source_sha256, artifact_generation_id),
        peak_summary_doc(rows, dataset_name, dataset_id, source_sha256, artifact_generation_id),
    ]


# ---------------------------------------------------------------------------
# RAG v2: normalize SkillResult → action_finding, action_evidence, action_diagnostic documents
# ---------------------------------------------------------------------------

_IP_RE = re.compile(r"(?:(?:\d{1,3}\.){3}\d{1,3})")
_DOMAIN_RE = re.compile(r"(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+(?:com|net|org|io|edu|gov|vps|mydyndns|no-ip|dyn|biz|info|mobi|asia|eu|ru|cn|tw|jp|de|uk|fr|br|br|in|us|co|cc|tk|ml|ga|cf|top|xyz|online|site|store|tech|space|fun|website|pro|name|tel|aero|coop|int|museum|travel|jobs|mobi|cat|asia|tel|post|xxx|arpa|mil|govt|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cu|cv|cx|cy|cz|de|dj|dk|dm|do|dz|ec|ee|eg|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|sk|sl|sm|sn|so|sr|st|su|sv|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|um|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)(?:\b|$)")
_PORT_RE = re.compile(r"port[:\s/]*([0-9]{1,5})")


def _extract_ips(text: str) -> list[str]:
    return _IP_RE.findall(text)


def _extract_domains(text: str) -> list[str]:
    return _DOMAIN_RE.findall(text)


def _extract_ports(text: str) -> list[int]:
    return [int(m) for m in _PORT_RE.findall(text)]


def _collect_text_from_skill_result(result: dict[str, Any]) -> str:
    """Collect all human-readable text from a SkillResult for embedding."""
    parts: list[str] = []
    summary = result.get("result", {}).get("summary", {})
    for key in ("title", "overview", "description"):
        val = summary.get(key)
        if val:
            parts.append(str(val))
    for metric in summary.get("key_metrics", []):
        parts.append(f"{metric.get('name', '')}={metric.get('value', '')}")
    for finding in result.get("result", {}).get("findings", []):
        for key in ("title", "description", "summary", "type"):
            val = finding.get(key)
            if val:
                parts.append(str(val))
        entities_field = finding.get("entities", [])
        if isinstance(entities_field, list):
            for entity in entities_field:
                if isinstance(entity, dict):
                    parts.append(f"{entity.get('type', '')}={entity.get('value', '')}")
                elif isinstance(entity, str):
                    parts.append(entity)
        elif isinstance(entities_field, dict):
            for ek, ev in entities_field.items():
                parts.append(f"{ek}={ev}")
    for evidence in result.get("result", {}).get("evidence", []):
        parts.append(evidence.get("title", ""))
        if evidence.get("content"):
            parts.append(evidence["content"])
        for metric in evidence.get("metrics", []):
            parts.append(f"{metric.get('name', '')}={metric.get('value', '')}")
        for row in evidence.get("rows", []):
            parts.append(" ".join(str(v) for v in row))
    for warning in result.get("diagnostics", {}).get("warnings", []):
        parts.append(warning.get("message", ""))
    return " ".join(p for p in parts if p)


def _extract_entities_from_result(result: dict[str, Any]) -> dict[str, list]:
    """Lightweight entity extraction from a SkillResult."""
    entities: dict[str, set] = {
        "src_ips": set(), "dst_ips": set(), "domains": set(),
        "ports": set(), "protocols": set(), "urls": set(),
        "services": set(), "users": set(), "assets": set(),
    }
    text_blob = _collect_text_from_skill_result(result)
    for ip in _extract_ips(text_blob):
        # Heuristic: private IPs likely src, but keep in src_ips for now
        entities["src_ips"].add(ip)
    for domain in _extract_domains(text_blob):
        entities["domains"].add(domain)
    for port in _extract_ports(text_blob):
        entities["ports"].add(port)
    # Try structured entity extraction from findings
    for finding in result.get("result", {}).get("findings", []):
        entities_field = finding.get("entities", [])
        if isinstance(entities_field, list):
            for entity in entities_field:
                if not isinstance(entity, dict):
                    continue
                etype = entity.get("type", "")
                evalue = entity.get("value", "")
                if etype == "src_ip":
                    entities["src_ips"].add(evalue)
                elif etype == "dst_ip":
                    entities["dst_ips"].add(evalue)
                elif etype in ("domain", "dns_query", "tls_sni", "http_host"):
                    if evalue:
                        entities["domains"].add(evalue)
                elif etype in ("port", "dst_port"):
                    try:
                        entities["ports"].add(int(evalue))
                    except (ValueError, TypeError):
                        pass
                elif etype in ("service", "app_protocol", "protocol"):
                    if evalue:
                        entities["services"].add(evalue)
                elif etype == "user":
                    entities["users"].add(evalue)
        elif isinstance(entities_field, dict):
            for ek, ev in entities_field.items():
                if ek in ("src_ip",):
                    entities["src_ips"].add(str(ev))
                elif ek in ("dst_ip",):
                    entities["dst_ips"].add(str(ev))
                elif ek in ("domain", "dns_query", "tls_sni", "http_host"):
                    entities["domains"].add(str(ev))
                elif ek in ("port", "dst_port"):
                    try:
                        entities["ports"].add(int(ev))
                    except (ValueError, TypeError):
                        pass
                elif ek in ("service", "app_protocol", "protocol"):
                    entities["services"].add(str(ev))
                elif ek == "user":
                    entities["users"].add(str(ev))
    return {k: sorted(v)[:20] for k, v in entities.items()}


def _extract_metrics_from_result(result: dict[str, Any]) -> dict[str, Any]:
    """Extract numeric metrics from SkillResult summary + evidence."""
    metrics: dict[str, Any] = {}
    summary = result.get("result", {}).get("summary", {})
    for m in summary.get("key_metrics", []):
        name = m.get("name", "")
        if name:
            metrics[name] = m.get("value")
    for evidence in result.get("result", {}).get("evidence", []):
        for m in evidence.get("metrics", []):
            name = m.get("name", "")
            if name and name not in metrics:
                metrics[name] = m.get("value")
    return metrics


def _extract_evidence_tables(result: dict[str, Any], max_rows: int = 5) -> list[dict[str, Any]]:
    """Extract table evidence, truncating rows to avoid oversized docs."""
    tables: list[dict[str, Any]] = []
    for evidence in result.get("result", {}).get("evidence", []):
        if evidence.get("type") == "table" and evidence.get("columns") and evidence.get("rows"):
            tables.append({
                "title": evidence.get("title", ""),
                "columns": evidence["columns"],
                "rows": evidence["rows"][:max_rows],
                "total_rows": len(evidence["rows"]),
                "truncated": len(evidence["rows"]) > max_rows,
            })
    return tables


def _build_flow_filter(
    dataset_name: str,
    source_file: str,
    entities: dict[str, list],
    hotspot_entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a flow_query filter dict from extracted entities.

    Used by rag_search to do a second-hop lookup of matching flow_summary docs.
    Priority: src_ip > dst_ip > domain/dns_query > service > port.
    """
    flow_filter: dict[str, Any] = {
        "dataset_name": dataset_name,
        "source_file": to_repo_relative_display(source_file) if source_file else "",
    }
    src_ips = entities.get("src_ips", [])
    dst_ips = entities.get("dst_ips", [])
    domains = entities.get("domains", [])
    ports = entities.get("ports", [])
    services = entities.get("services", [])
    if len(src_ips) == 1:
        flow_filter["primary_entity"] = {"type": "src_ip", "value": src_ips[0]}
    elif src_ips:
        flow_filter["primary_entity"] = {"type": "src_ips", "values": src_ips[:5]}
    if dst_ips:
        flow_filter["dst_ips"] = dst_ips[:5]
    if domains:
        flow_filter["domains"] = domains[:5]
    if ports:
        flow_filter["ports"] = [str(p) for p in ports[:10]]
    if services:
        flow_filter["services"] = services[:5]

    # If entities are empty, try to infer from hotspot_entities or table rows
    has_entities = bool(src_ips or dst_ips or domains or ports or services)
    if not has_entities and hotspot_entities:
        inferred: dict[str, list] = {"src_ips": [], "domains": [], "services": [], "ports": []}
        for h in hotspot_entities[:10]:
            rd = h.get("row_data", {})
            for k in ("src_ip", "query", "host", "server_name", "service", "conn_state"):
                v = rd.get(k)
                if v and str(v):
                    if k == "src_ip":
                        inferred["src_ips"].append(str(v))
                    elif k in ("query", "host", "server_name"):
                        inferred["domains"].append(str(v))
                    elif k == "service":
                        inferred["services"].append(str(v))
                    break
        if inferred["src_ips"]:
            flow_filter["primary_entity"] = {"type": "src_ips", "values": list(dict.fromkeys(inferred["src_ips"]))[:5]}
        if inferred["domains"]:
            flow_filter["domains"] = list(dict.fromkeys(inferred["domains"]))[:5]
        if inferred["services"]:
            flow_filter["services"] = list(dict.fromkeys(inferred["services"]))[:5]

    return flow_filter


def _extract_hotspot_entities(
    tables: list[dict[str, Any]],
    top_k: int = 50,
) -> list[dict[str, Any]]:
    """Extract top-K hotspot rows from evidence tables as linkable entities.

    Returns list of dicts with entity_type, entity_value, and table context.
    """
    hotspots: list[dict[str, Any]] = []
    for table in tables:
        title = table.get("title", "").lower()
        if not any(kw in title for kw in ["hotspot", "top talker", "top quer", "nxdomain", "top host", "top service"]):
            continue
        for row in table.get("rows", [])[:top_k]:
            if isinstance(row, list):
                cols = table.get("columns", [])
                row_dict = dict(zip(cols, row))
            else:
                row_dict = row
            entry: dict[str, Any] = {
                "table_title": table.get("title", ""),
                "row_data": row_dict,
            }
            # Extract key entity fields
            for k in ("src_ip", "query", "host", "server_name", "service", "conn_state"):
                if k in row_dict:
                    entry["entity_type"] = k
                    entry["entity_value"] = str(row_dict[k])
                    break
            hotspots.append(entry)
    return hotspots


def _build_action_finding_doc(
    *,
    finding: dict[str, Any],
    result: dict[str, Any],
    dataset_name: str,
    dataset_id: str,
    source_file: str,
    source_sha256: str,
    action_name: str,
    action_category: str = "",
    index: int = 0,
    raw_source_file: str = "",
) -> dict[str, Any]:
    """Build an action_finding RAG document from one finding."""
    finding_id = finding.get("finding_id") or finding.get("id") or f"{action_name}:{index}"
    title = finding.get("title") or finding.get("name") or action_name.replace("-", " ").title()
    summary_text = finding.get("description") or finding.get("summary") or ""
    severity = finding.get("severity") or result.get("result", {}).get("summary", {}).get("severity", "info")
    confidence = finding.get("confidence")
    risk_score = finding.get("risk_score")
    evidence_refs = finding.get("evidence_refs", [])
    entities = _extract_entities_from_result(result)

    keywords = [action_name, severity] + entities.get("src_ips", [])[:5] + entities.get("domains", [])[:5]
    canonical_text = _collect_text_from_skill_result(result)

    payload = {
        "schema_version": RAG_DOC_SCHEMA_VERSION,
        "doc_type": "action_finding",
        "dataset": {
            "dataset_name": dataset_name,
            "source_file": to_repo_relative_display(source_file) if source_file else "",
            "input_type": "flow_csv",
            "traffic_family": "",
        },
        "action": {
            "name": action_name,
            "category": action_category,
            "view": "flow",
            "engine": "hybrid",
        },
        "finding": {
            "id": finding_id,
            "title": title,
            "summary": summary_text,
            "severity": severity,
            "confidence": confidence,
            "risk_score": risk_score,
            "status": "observed",
        },
        "security_context": {
            "threat_tags": [],
            "attack_stages": [],
            "mitre_techniques": [],
            "ioc_candidates": [],
        },
        "evidence_refs": evidence_refs,
        "retrieval_text": {
            "title": title,
            "summary": summary_text,
            "keywords": keywords[:30],
            "canonical_text": canonical_text,
        },
    }

    flow_filter = _build_flow_filter(dataset_name, source_file, entities)
    payload["flow_filter"] = flow_filter

    doc_id = stable_id(
        RAG_DOC_SCHEMA_VERSION, dataset_id, action_name, finding_id, str(index)
    )
    doc = build_doc(
        doc_id=doc_id,
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        source_file=to_repo_relative_display(source_file) if source_file else "",
        doc_type="action_finding",
        title=title,
        content=json.dumps(payload, ensure_ascii=False),
        summary=summary_text[:200] if summary_text else title,
        keywords=keywords[:30],
        metadata={
            "finding_id": finding_id,
            "action_name": action_name,
            "severity": severity,
            "risk_score": risk_score,
            "confidence": confidence,
            "schema_version": RAG_DOC_SCHEMA_VERSION,
        },
        raw_source_file=raw_source_file,
        source_sha256=source_sha256,
    )
    doc["schema_version"] = RAG_DOC_SCHEMA_VERSION
    doc["payload"] = payload
    doc["provenance_type"] = "finding"
    doc["source_file"] = to_repo_relative_display(source_file) if source_file else ""
    doc["action_name"] = action_name
    doc["finding_id"] = finding_id
    doc["evidence_refs"] = evidence_refs
    return doc


def _build_action_evidence_doc(
    *,
    result: dict[str, Any],
    dataset_name: str,
    dataset_id: str,
    source_file: str,
    source_sha256: str,
    action_name: str,
    action_category: str = "",
    finding: dict[str, Any] | None = None,
    raw_source_file: str = "",
) -> dict[str, Any] | None:
    """Build an action_evidence RAG document from one finding (or action-level if no finding)."""
    result_data = result.get("result", {})
    summary = result_data.get("summary", {})

    if finding:
        finding_id = finding.get("finding_id") or finding.get("id") or f"{action_name}:0"
        title = finding.get("title") or finding.get("name") or action_name.replace("-", " ").title()
        summary_text = finding.get("description") or finding.get("summary") or ""
        severity = finding.get("severity") or summary.get("severity", "info")
    else:
        finding_id = f"{action_name}:action-level"
        title = summary.get("title") or action_name.replace("-", " ").title()
        summary_text = summary.get("overview", "")
        severity = summary.get("severity", "info")

    entities = _extract_entities_from_result(result)
    metrics = _extract_metrics_from_result(result)
    tables = _extract_evidence_tables(result)

    keywords = [action_name, severity]
    for vals in entities.values():
        if isinstance(vals, list):
            keywords.extend(str(v) for v in vals)
    canonical_text = _collect_text_from_skill_result(result)

    payload = {
        "schema_version": RAG_DOC_SCHEMA_VERSION,
        "doc_type": "action_evidence",
        "dataset": {
            "dataset_name": dataset_name,
            "source_file": to_repo_relative_display(source_file) if source_file else "",
            "input_type": "flow_csv",
            "traffic_family": "",
        },
        "action": {
            "name": action_name,
            "category": action_category,
            "view": "flow",
            "engine": "hybrid",
        },
        "finding": {
            "id": finding_id,
            "title": title,
            "summary": summary_text,
            "severity": severity,
        },
        "entities": entities,
        "metrics": metrics,
        "evidence": {
            "refs": finding.get("evidence_refs", []) if finding else [],
            "tables": tables,
            "sample_rows": tables[0].get("rows", [])[:3] if tables else [],
            "time_range": {"start": None, "end": None},
        },
        "retrieval_text": {
            "title": title,
            "summary": summary_text,
            "keywords": keywords[:30],
            "canonical_text": canonical_text,
        },
    }

    hotspot_entities = _extract_hotspot_entities(tables)
    flow_filter = _build_flow_filter(dataset_name, source_file, entities, hotspot_entities)
    payload["flow_filter"] = flow_filter
    payload["hotspot_entities"] = hotspot_entities

    if finding:
        doc_id = stable_id(
            RAG_DOC_SCHEMA_VERSION, dataset_id, action_name, finding_id, "evidence"
        )
    else:
        doc_id = stable_id(RAG_DOC_SCHEMA_VERSION, dataset_name, action_name, "action-level", "evidence")

    doc = build_doc(
        doc_id=doc_id,
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        source_file=to_repo_relative_display(source_file) if source_file else "",
        doc_type="action_evidence",
        title=title,
        content=json.dumps(payload, ensure_ascii=False),
        summary=summary_text[:200] if summary_text else title,
        keywords=keywords[:30],
        metadata={
            "finding_id": finding_id,
            "action_name": action_name,
            "severity": severity,
            "schema_version": RAG_DOC_SCHEMA_VERSION,
        },
        raw_source_file=raw_source_file,
        source_sha256=source_sha256,
    )
    doc["schema_version"] = RAG_DOC_SCHEMA_VERSION
    doc["payload"] = payload
    doc["provenance_type"] = "evidence"
    doc["source_file"] = to_repo_relative_display(source_file) if source_file else ""
    doc["action_name"] = action_name
    doc["finding_id"] = finding_id
    return doc


def _build_action_diagnostic_doc(
    *,
    result: dict[str, Any],
    dataset_name: str,
    dataset_id: str,
    source_file: str,
    source_sha256: str,
    action_name: str,
    action_category: str = "",
    raw_source_file: str = "",
) -> dict[str, Any] | None:
    """Build one action_diagnostic document per action run."""
    diagnostics = result.get("diagnostics", {})
    warning_count = len(diagnostics.get("warnings", diagnostics.get("errors", [])))

    keywords = [action_name, "diagnostic"]
    if diagnostics.get("model_version"):
        keywords.append(diagnostics["model_version"])
    canonical_text = json.dumps(diagnostics, ensure_ascii=False)

    title = f"Action diagnostic: {action_name}"
    summary_text = f"Diagnostic for action \'{action_name}\' on dataset \'{dataset_name}\'. "
    if diagnostics:
        summary_text += f"Warnings/errors: {warning_count}. "

    payload = {
        "schema_version": RAG_DOC_SCHEMA_VERSION,
        "doc_type": "action_diagnostic",
        "dataset": {
            "dataset_name": dataset_name,
            "source_file": to_repo_relative_display(source_file) if source_file else "",
            "input_type": "flow_csv",
        },
        "action": {
            "name": action_name,
            "category": action_category,
            "view": "flow",
            "engine": "hybrid",
        },
        "processing": {
            "source_sha256": source_sha256,
            "warning_count": warning_count,
        },
        "diagnostics": diagnostics,
        "retrieval_text": {
            "title": title,
            "summary": summary_text,
            "keywords": keywords[:30],
            "canonical_text": canonical_text,
        },
    }

    doc_id = stable_id(RAG_DOC_SCHEMA_VERSION, dataset_id, action_name, "diagnostic")
    doc = build_doc(
        doc_id=doc_id,
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        source_file=to_repo_relative_display(source_file) if source_file else "",
        doc_type="action_diagnostic",
        title=title,
        content=json.dumps(payload, ensure_ascii=False),
        summary=summary_text[:200],
        keywords=keywords[:30],
        metadata={
            "action_name": action_name,
            "source_sha256": source_sha256,
            "warning_count": warning_count,
            "schema_version": RAG_DOC_SCHEMA_VERSION,
        },
        raw_source_file=raw_source_file,
        source_sha256=source_sha256,
    )
    doc["schema_version"] = RAG_DOC_SCHEMA_VERSION
    doc["payload"] = payload
    doc["provenance_type"] = "diagnostic"
    doc["source_file"] = to_repo_relative_display(source_file) if source_file else ""
    doc["action_name"] = action_name
    return doc


def normalize_skill_result_to_evidence_docs(
    result: dict[str, Any],
    *,
    dataset_name: str,
    dataset_id: str,
    source_file: str,
    source_sha256: str = "",
    action_name: str,
    raw_source_file: str = "",
) -> list[dict[str, Any]]:
    """Convert a SkillResult dict into action_finding, action_evidence, and action_diagnostic docs.

    - If the result has findings: one action_finding + one action_evidence per finding.
    - If no findings but the action ran successfully: one action_evidence (action-level).
    - One action_diagnostic per action run.
    - If the result status is 'failed', skip (no doc).
    """
    if result.get("status") == "failed":
        return []

    findings = result.get("result", {}).get("findings", [])
    docs: list[dict[str, Any]] = []

    if findings:
        for idx, finding in enumerate(findings):
            doc = _build_action_finding_doc(
                finding=finding,
                result=result,
                dataset_name=dataset_name,
                dataset_id=dataset_id,
                source_file=source_file,
                source_sha256=source_sha256,
                action_name=action_name,
                index=idx,
                raw_source_file=raw_source_file,
            )
            docs.append(doc)

    if findings:
        for finding in findings:
            doc = _build_action_evidence_doc(
                result=result,
                dataset_name=dataset_name,
                dataset_id=dataset_id,
                source_file=source_file,
                source_sha256=source_sha256,
                action_name=action_name,
                finding=finding,
                raw_source_file=raw_source_file,
            )
            if doc:
                docs.append(doc)
    else:
        action_doc = _build_action_evidence_doc(
            result=result,
            dataset_name=dataset_name,
            dataset_id=dataset_id,
            source_file=source_file,
            source_sha256=source_sha256,
            action_name=action_name,
            raw_source_file=raw_source_file,
        )
        if action_doc:
            docs.append(action_doc)

    diagnostic_doc = _build_action_diagnostic_doc(
        result=result,
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        source_file=source_file,
        source_sha256=source_sha256,
        action_name=action_name,
        raw_source_file=raw_source_file,
    )
    if diagnostic_doc:
        docs.append(diagnostic_doc)

    return docs


def build_documents(rows: list[dict[str, Any]], dataset_name: str, dataset_id: str = "", source_sha256: str = "", artifact_generation_id: str = "") -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    documents.extend(endpoint_summary_docs(rows, dataset_name, dataset_id, source_sha256, artifact_generation_id))
    documents.extend(port_summary_docs(rows, dataset_name, dataset_id, source_sha256, artifact_generation_id))
    documents.extend(flow_summary_doc(row, dataset_name, dataset_id, source_sha256, artifact_generation_id) for row in rows)
    documents.extend(protocol_summary_docs(rows, dataset_name, dataset_id, source_sha256, artifact_generation_id))
    documents.extend(anomaly_summary_docs(rows, dataset_name, dataset_id, source_sha256, artifact_generation_id))
    return documents


def write_jsonl(path: Path, documents: list[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        for document in documents:
            handle.write(json.dumps(document, ensure_ascii=False) + "\n")


def build_manifest(*, dataset_name: str, files: list[str], docs_path: Path, documents: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(doc["doc_type"] for doc in documents)
    sample_titles = [doc["title"] for doc in documents[:10]]

    id_counts = Counter(doc["doc_id"] for doc in documents)
    duplicate_doc_ids = [doc_id for doc_id, count in id_counts.items() if count > 1]

    provenance_coverage = {
        "docs_with_source_file": sum(1 for d in documents if d.get("source_file") or d.get("metadata", {}).get("source_file")),
        "docs_with_row_index": sum(1 for d in documents if d.get("row_index") or d.get("metadata", {}).get("row_index")),
        "docs_with_flow_id": sum(1 for d in documents if d.get("flow_id") or d.get("metadata", {}).get("flow_id")),
        "docs_with_evidence_refs": sum(1 for d in documents if d.get("evidence_refs") or d.get("metadata", {}).get("evidence_refs")),
    }

    return {
        "dataset_name": dataset_name,
        "source_files": [to_repo_relative_display(item) for item in files],
        "document_count": len(documents),
        "document_types": dict(counts),
        "unique_doc_id_count": len(id_counts),
        "duplicate_doc_id_count": len(duplicate_doc_ids),
        "duplicate_doc_id_samples": duplicate_doc_ids[:10],
        "provenance_coverage": provenance_coverage,
        "output_file": to_repo_relative_display(docs_path),
        "samples": sample_titles,
    }


def build_dataset_profile_doc(
    *,
    dataset_name: str,
    dataset_id: str,
    files: list[str],
    row_count: int,
    evidence_doc_count: int,
    action_names: list[str],
    source_sha256: str = "",
    raw_source_files: list[str] | None = None,
) -> dict[str, Any]:
    """Single dataset-level profile document (Layer 3)."""
    source_files = [to_repo_relative_display(f) for f in files]
    raw_files = [to_repo_relative_display(f) for f in raw_source_files] if raw_source_files else []
    time_range: dict[str, str | None] = {"start": None, "end": None}

    if files:
        try:
            with open(files[0], encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                timestamps = []
                for row in reader:
                    ts = row.get("timestamp", "")
                    if ts:
                        parsed = parse_timestamp(ts)
                        if parsed:
                            timestamps.append(parsed)
                if timestamps:
                    timestamps.sort()
                    time_range = {
                        "start": timestamps[0].isoformat(),
                        "end": timestamps[-1].isoformat(),
                    }
        except Exception:
            pass

    content = (
        f"Dataset '{dataset_name}' contains {row_count} flow rows from {len(files)} source file(s). "
        f"RAG index produced {evidence_doc_count} action taxonomy documents across {len(action_names)} actions: "
        f"{', '.join(action_names) or 'none'}. "
        f"Time range: {time_range['start'] or 'unknown'} to {time_range['end'] or 'unknown'}. "
        f"Schema version: {RAG_DOC_SCHEMA_VERSION}. "
        f"Dataset ID: {dataset_id}."
    )
    summary = f"Dataset profile: {dataset_name}, {row_count} rows, {evidence_doc_count} evidence docs."

    doc = build_doc(
        doc_id=stable_id(dataset_id, "dataset_profile"),
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        source_file=source_files[0] if source_files else "",
        doc_type="dataset_profile",
        title=f"Dataset profile: {dataset_name}",
        content=content,
        summary=summary,
        keywords=["dataset-profile", dataset_name] + action_names,
        metadata={
            "row_count": row_count,
            "source_files": source_files,
            "raw_source_files": raw_files,
            "time_range": time_range,
            "action_count": len(action_names),
            "action_names": action_names,
            "evidence_doc_count": evidence_doc_count,
            "rag_schema_version": RAG_DOC_SCHEMA_VERSION,
            "builder_version": "rag-v2",
            "provenance_type": "dataset_profile",
        },
        raw_source_file=raw_files[0] if raw_files else "",
    )
    doc["schema_version"] = RAG_DOC_SCHEMA_VERSION
    doc["provenance_type"] = "dataset_profile"
    return doc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build RAG documents from network flow CSV files.")
    parser.add_argument("--files", nargs="+", required=True, help="Input flow CSV files, directories, or shorthand references")
    parser.add_argument("--dataset-name", default=None, help="Override dataset name used in rag_docs.jsonl")
    parser.add_argument("--output-dir", default=None, help="Directory for rag_docs.jsonl and rag_manifest.json")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--skip-skill-result-analysis", action="store_true", help="Do not run analyze.py actions for structured evidence")
    parser.add_argument("--actions", nargs="*", default=None, help="Override the default set of actions for evidence generation")
    return parser


def _ensure_schema_version(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Assign rag_doc_v2 schema_version to documents that lack one."""
    for doc in documents:
        doc["schema_version"] = RAG_DOC_SCHEMA_VERSION
    return documents


def _run_action_skill_result(
    flow_csv: str,
    action_name: str,
) -> dict[str, Any] | None:
    """Run analyze.py for a single action with --format skill-result-json.

    Returns the parsed JSON dict, or None on failure.
    """
    analyze_py = Path(__file__).parent / "analyze.py"
    try:
        proc = subprocess.run(
            [sys.executable, str(analyze_py), "--files", flow_csv, "--action", action_name, "--format", "skill-result-json"],
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(Path(__file__).parent),
        )
        if proc.returncode != 0:
            return None
        return json.loads(proc.stdout)
    except Exception:
        return None


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        files = discover_files(args.files)
        if not files:
            parser.error("No flow CSV files were found from --files")
        for file_path in files:
            if not str(file_path).lower().endswith(".flow.csv"):
                raise ValueError(f"Expected a .flow.csv input, got '{to_repo_relative_display(file_path)}'.")

        dataset_name = sanitize_name(args.dataset_name or dataset_name_from_file(Path(files[0])))
        output_dir = Path(args.output_dir) if args.output_dir else default_output_dir(dataset_name, files)
        output_dir.mkdir(parents=True, exist_ok=True)

        rows = load_flow_rows(files)
        if rows and rows[0].get("dataset_label") and not args.dataset_name:
            dataset_name = sanitize_name(rows[0]["dataset_label"])
            if not args.output_dir:
                output_dir = default_output_dir(dataset_name, files)
                output_dir.mkdir(parents=True, exist_ok=True)

        # Compute shared index fields
        dataset_id = compute_dataset_id(dataset_name, files)
        source_sha256 = _sha256_file(Path(files[0])) if len(files) == 1 else ""

        # Layer 1: flow-level documents
        documents = _ensure_schema_version(build_documents(rows, dataset_name, dataset_id, source_sha256))

        # Layer 2: structured analysis evidence from SkillResult outputs
        evidence_docs: list[dict[str, Any]] = []
        actions_to_run = list(EVIDENCE_ACTIONS)
        if args.actions is not None:
            actions_to_run = args.actions

        flow_csv = files[0]
        skipped_actions: list[str] = []
        successful_actions: list[str] = []
        if not args.skip_skill_result_analysis:
            for action_name in actions_to_run:
                skill_result = _run_action_skill_result(flow_csv, action_name)
                if skill_result is None:
                    skipped_actions.append(action_name)
                    continue
                successful_actions.append(action_name)
                docs = normalize_skill_result_to_evidence_docs(
                    skill_result,
                    dataset_name=dataset_name,
                    dataset_id=dataset_id,
                    source_file=flow_csv,
                    source_sha256=source_sha256,
                    action_name=action_name,
                    raw_source_file=rows[0].get("_raw_source_file", "") if rows else "",
                )
                evidence_docs.extend(docs)

        documents.extend(evidence_docs)

        # Layer 3: dataset profile
        raw_source_files = sorted({row["_raw_source_file"] for row in rows if row.get("_raw_source_file")})
        dataset_profile = build_dataset_profile_doc(
            dataset_name=dataset_name,
            dataset_id=dataset_id,
            files=files,
            row_count=len(rows),
            evidence_doc_count=len(evidence_docs),
            action_names=[a for a in actions_to_run],
            source_sha256=source_sha256,
            raw_source_files=raw_source_files if raw_source_files else None,
        )
        documents.append(dataset_profile)

        docs_path = output_dir / "rag_docs.jsonl"
        manifest_path = output_dir / "rag_manifest.json"
        write_jsonl(docs_path, documents)
        manifest = build_manifest(dataset_name=dataset_name, files=files, docs_path=docs_path, documents=documents)
        manifest["builder_version"] = "rag-v2"
        manifest["schema_versions"] = sorted({doc.get("schema_version", "") for doc in documents if doc.get("schema_version")})
        manifest["skipped_actions"] = skipped_actions
        manifest["successful_actions"] = successful_actions
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

        # Build doc_counts for json output
        doc_counts = Counter(doc["doc_type"] for doc in documents)

        if args.format == "json":
            output = {
                "status": "success",
                "doc_counts": dict(doc_counts),
                "schema_versions": manifest["schema_versions"],
                "manifest": manifest,
            }
            print(json.dumps(output, ensure_ascii=False))
        else:
            lines = [
                f"Built RAG docs for dataset: {dataset_name}",
                f"Source flow files: {len(files)}",
                f"Document count: {len(documents)}",
                f"  flow_summary: {doc_counts.get('flow_summary', 0)}",
                f"  endpoint_summary: {doc_counts.get('endpoint_summary', 0)}",
                f"  port_summary: {doc_counts.get('port_summary', 0)}",
                f"  protocol_summary: {doc_counts.get('protocol_summary', 0)}",
                f"  behavior_summary: {doc_counts.get('behavior_summary', 0)}",
                f"  anomaly_summary: {doc_counts.get('anomaly_summary', 0)}",
                f"  action_finding: {doc_counts.get('action_finding', 0)}",
                f"  action_evidence: {doc_counts.get('action_evidence', 0)}",
                f"  action_diagnostic: {doc_counts.get('action_diagnostic', 0)}",
                f"  dataset_profile: {doc_counts.get('dataset_profile', 0)}",
                f"rag_docs: {to_repo_relative_display(docs_path)}",
                f"manifest: {to_repo_relative_display(manifest_path)}",
            ]
            print("\n".join(lines))
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
