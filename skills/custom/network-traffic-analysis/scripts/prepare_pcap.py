#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from contextlib import suppress
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from file_resolution import normalize_name

try:
    from scapy.all import DNS, DNSQR, ICMP, IP, Raw, TCP, UDP, Ether, IPv6, PcapReader, RawPcapReader  # type: ignore
except ImportError:
    import os

    os.system(f"{sys.executable} -m pip install scapy -q")
    from scapy.all import DNS, DNSQR, ICMP, IP, Raw, TCP, UDP, Ether, IPv6, PcapReader, RawPcapReader  # type: ignore


PCAP_PATTERNS = ("*.pcap", "*.pcapng", "*.cap")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def get_search_roots() -> list[Path]:
    base = repo_root() / "datasets" / "network-traffic"
    return [base / "raw", base / "processed"]


def sanitize_name(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return sanitized or "dataset"


def resolve_pcap_reference(reference: str) -> list[str]:
    normalized_reference = normalize_name(reference)
    suffix_matches: list[Path] = []
    name_matches: list[Path] = []
    normalized_matches: list[Path] = []

    for root in get_search_roots():
        if not root.exists():
            continue
        for pattern in PCAP_PATTERNS:
            for candidate in root.rglob(pattern):
                candidate_path = candidate.resolve()
                candidate_name = candidate_path.name
                candidate_suffix = candidate_path.as_posix()
                if candidate_name == reference or candidate_suffix.endswith(reference.replace("\\", "/")):
                    suffix_matches.append(candidate_path)
                    continue
                if candidate_path.stem == Path(reference).stem:
                    name_matches.append(candidate_path)
                    continue
                if normalize_name(candidate_name) == normalized_reference or normalize_name(candidate_path.stem) == normalized_reference:
                    normalized_matches.append(candidate_path)

    matches = suffix_matches or name_matches or normalized_matches
    deduped: list[str] = []
    seen: set[str] = set()
    for match in matches:
        as_posix = match.as_posix()
        if as_posix not in seen:
            deduped.append(as_posix)
            seen.add(as_posix)
    if not deduped:
        raise ValueError(f"PCAP reference '{reference}' was not found under datasets/network-traffic/raw or processed.")
    if len(deduped) > 1:
        sample = "\n".join(f"  - {item}" for item in deduped[:10])
        raise ValueError(
            f"PCAP reference '{reference}' matched multiple files. Use a more specific path.\nCandidates:\n{sample}"
        )
    return deduped


def discover_pcaps(values: list[str]) -> list[str]:
    files: list[str] = []
    for value in values:
        path = Path(value).expanduser()
        if path.is_dir():
            for pattern in PCAP_PATTERNS:
                files.extend(str(item.resolve()) for item in sorted(path.rglob(pattern)))
        elif path.exists():
            files.append(str(path.resolve()))
        else:
            files.extend(resolve_pcap_reference(value))
    deduped: list[str] = []
    seen: set[str] = set()
    for file_path in files:
        if file_path not in seen:
            deduped.append(file_path)
            seen.add(file_path)
    return deduped


def iso_timestamp(epoch_value: float | int | str | None) -> str:
    if epoch_value in (None, ""):
        return ""
    try:
        value = float(epoch_value)
    except (TypeError, ValueError):
        return str(epoch_value)
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def detect_protocol(packet: Any) -> str:
    if packet.haslayer(TCP):
        return "TCP"
    if packet.haslayer(UDP):
        return "UDP"
    if packet.haslayer(ICMP):
        return "ICMP"
    if packet.haslayer(IPv6):
        return "IPV6"
    if packet.haslayer(IP):
        return "IP"
    return packet.lastlayer().name.upper() if getattr(packet, "lastlayer", None) else "UNKNOWN"


def format_tcp_flags(tcp_segment: Any | None) -> str:
    if tcp_segment is None:
        return ""
    try:
        return str(tcp_segment.sprintf("%TCP.flags%"))
    except Exception:
        return str(getattr(tcp_segment, "flags", ""))


def safe_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def infer_payload_bytes(packet: Any) -> int:
    try:
        payload = bytes(packet.payload.payload.payload) if getattr(packet.payload, "payload", None) else b""
        if payload:
            return len(payload)
    except Exception:
        pass

    if packet.haslayer(TCP):
        try:
            return len(bytes(packet[TCP].payload))
        except Exception:
            return 0
    if packet.haslayer(UDP):
        try:
            return len(bytes(packet[UDP].payload))
        except Exception:
            return 0
    if packet.haslayer(ICMP):
        try:
            return len(bytes(packet[ICMP].payload))
        except Exception:
            return 0
    return 0


def decode_text(value: bytes) -> str:
    return value.decode("utf-8", errors="ignore").strip()


def extract_dns_query(packet: Any) -> str:
    if not packet.haslayer(DNS) or not packet.haslayer(DNSQR):
        return ""
    with suppress(Exception):
        qname = packet[DNSQR].qname
        if isinstance(qname, bytes):
            return qname.decode("utf-8", errors="ignore").rstrip(".")
        return str(qname).rstrip(".")
    return ""


def extract_http_host(packet: Any) -> str:
    if not packet.haslayer(Raw):
        return ""
    with suppress(Exception):
        payload = bytes(packet[Raw].load)
        if not payload:
            return ""
        text = payload.decode("utf-8", errors="ignore")
        if not text.startswith(("GET ", "POST ", "HEAD ", "PUT ", "DELETE ", "OPTIONS ", "PATCH ")):
            return ""
        for line in text.splitlines():
            if line.lower().startswith("host:"):
                return line.split(":", 1)[1].strip()
    return ""


def extract_tls_sni(packet: Any) -> str:
    if not packet.haslayer(TCP):
        return ""
    with suppress(Exception):
        payload = bytes(packet[TCP].payload)
        if len(payload) < 5 or payload[0] != 0x16:
            return ""
        record_len = int.from_bytes(payload[3:5], "big")
        if len(payload) < 5 + record_len or payload[5] != 0x01:
            return ""
        idx = 5 + 4
        idx += 2 + 32
        if idx >= len(payload):
            return ""
        session_id_len = payload[idx]
        idx += 1 + session_id_len
        cipher_len = int.from_bytes(payload[idx : idx + 2], "big")
        idx += 2 + cipher_len
        comp_len = payload[idx]
        idx += 1 + comp_len
        ext_total_len = int.from_bytes(payload[idx : idx + 2], "big")
        idx += 2
        end = min(idx + ext_total_len, len(payload))
        while idx + 4 <= end:
            ext_type = int.from_bytes(payload[idx : idx + 2], "big")
            ext_len = int.from_bytes(payload[idx + 2 : idx + 4], "big")
            idx += 4
            ext_data = payload[idx : idx + ext_len]
            idx += ext_len
            if ext_type != 0x0000 or len(ext_data) < 5:
                continue
            list_len = int.from_bytes(ext_data[0:2], "big")
            pos = 2
            list_end = min(2 + list_len, len(ext_data))
            while pos + 3 <= list_end:
                name_type = ext_data[pos]
                name_len = int.from_bytes(ext_data[pos + 1 : pos + 3], "big")
                pos += 3
                name = ext_data[pos : pos + name_len]
                pos += name_len
                if name_type == 0:
                    return decode_text(name)
    return ""


def infer_service(dst_port: str, protocol: str, dns_query: str, tls_sni: str, http_host: str) -> str:
    if dns_query:
        return "dns"
    if tls_sni:
        return "tls"
    if http_host:
        return "http"
    common = {
        ("TCP", "80"): "http",
        ("TCP", "443"): "https",
        ("TCP", "25"): "smtp",
        ("TCP", "465"): "smtps",
        ("TCP", "587"): "submission",
        ("TCP", "993"): "imaps",
        ("TCP", "995"): "pop3s",
        ("TCP", "143"): "imap",
        ("UDP", "53"): "dns",
        ("TCP", "53"): "dns",
    }
    return common.get((protocol, dst_port), "")


def infer_app_protocol(dst_port: str, protocol: str, dns_query: str, tls_sni: str, http_host: str) -> str:
    if dns_query:
        return "DNS"
    if http_host:
        return "HTTP"
    if tls_sni or (protocol == "TCP" and dst_port == "443"):
        return "TLS"
    return infer_service(dst_port, protocol, dns_query, tls_sni, http_host).upper()


def infer_session_state(tcp_flags: str, protocol: str) -> str:
    if protocol != "TCP" or not tcp_flags:
        return ""
    normalized = tcp_flags.upper()
    if "R" in normalized:
        return "RST"
    if "F" in normalized:
        return "FIN"
    if "S" in normalized and "A" in normalized:
        return "SYN_ACK"
    if "S" in normalized:
        return "SYN"
    if "A" in normalized:
        return "ACK"
    return normalized


def infer_rule_name(app_protocol: str, dns_query: str, tls_sni: str, http_host: str) -> str:
    if dns_query:
        return "dns_query"
    if tls_sni:
        return "tls_client_hello"
    if http_host:
        return "http_request"
    if app_protocol:
        return f"{app_protocol.lower()}_traffic"
    return ""


def infer_traffic_family(dataset_name: str, source_file: str, app_protocol: str, service: str) -> str:
    joined = f"{dataset_name} {Path(source_file).name} {app_protocol} {service}".lower()
    if any(token in joined for token in ["gmail", "outlook", "mail", "smtp", "imap", "pop3"]):
        return "email"
    if "dns" in joined:
        return "dns"
    if any(token in joined for token in ["http", "https", "tls"]):
        return "web"
    return "network"


def make_flow_id(source_file: str, src_ip: str, dst_ip: str, src_port: str, dst_port: str, protocol: str) -> str:
    raw = "|".join([source_file, src_ip, dst_ip, src_port, dst_port, protocol])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def extract_packet_fields(packet: Any, source_file: str, packet_number: int) -> dict[str, Any]:
    timestamp = iso_timestamp(getattr(packet, "time", None))
    src_ip = ""
    dst_ip = ""
    src_port = ""
    dst_port = ""
    ip_version = ""
    ttl = None
    mac_src = ""
    mac_dst = ""
    icmp_type = None
    icmp_code = None

    if packet.haslayer(Ether):
        mac_src = str(packet[Ether].src)
        mac_dst = str(packet[Ether].dst)

    if packet.haslayer(IP):
        src_ip = str(packet[IP].src)
        dst_ip = str(packet[IP].dst)
        ip_version = "IPv4"
        ttl = safe_int(getattr(packet[IP], "ttl", None))
    elif packet.haslayer(IPv6):
        src_ip = str(packet[IPv6].src)
        dst_ip = str(packet[IPv6].dst)
        ip_version = "IPv6"
        ttl = safe_int(getattr(packet[IPv6], "hlim", None))

    if packet.haslayer(TCP):
        src_port = str(packet[TCP].sport)
        dst_port = str(packet[TCP].dport)
    elif packet.haslayer(UDP):
        src_port = str(packet[UDP].sport)
        dst_port = str(packet[UDP].dport)
    elif packet.haslayer(ICMP):
        icmp_type = safe_int(getattr(packet[ICMP], "type", None))
        icmp_code = safe_int(getattr(packet[ICMP], "code", None))

    protocol = detect_protocol(packet)
    tcp_flags = format_tcp_flags(packet[TCP]) if packet.haslayer(TCP) else ""
    payload_bytes = infer_payload_bytes(packet)
    flow_id = make_flow_id(source_file, src_ip, dst_ip, src_port, dst_port, protocol)
    dns_query = extract_dns_query(packet)
    http_host = extract_http_host(packet)
    tls_sni = extract_tls_sni(packet)
    service = infer_service(dst_port, protocol, dns_query, tls_sni, http_host)
    app_protocol = infer_app_protocol(dst_port, protocol, dns_query, tls_sni, http_host)
    session_state = infer_session_state(tcp_flags, protocol)
    rule_name = infer_rule_name(app_protocol, dns_query, tls_sni, http_host)
    packet_len = int(len(packet))

    return {
        "timestamp": timestamp,
        "packet_number": packet_number,
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "src_port": src_port,
        "dst_port": dst_port,
        "protocol": protocol,
        "ip_version": ip_version,
        "ttl": ttl,
        "payload_bytes": payload_bytes,
        "tcp_flags": tcp_flags,
        "icmp_type": icmp_type,
        "icmp_code": icmp_code,
        "mac_src": mac_src,
        "mac_dst": mac_dst,
        "flow_id": flow_id,
        "bytes": packet_len,
        "frame_len": packet_len,
        "packet_count": 1,
        "byte_count": packet_len,
        "bytes_total": packet_len,
        "src_bytes": packet_len,
        "dst_bytes": 0,
        "src_packets": 1,
        "dst_packets": 0,
        "duration_ms": 0,
        "app_protocol": app_protocol,
        "service": service,
        "direction": "unknown",
        "action": "observed",
        "session_state": session_state,
        "rule_name": rule_name,
        "dns_query": dns_query,
        "tls_sni": tls_sni,
        "http_host": http_host,
        "device_id": "",
        "sensor_id": "",
        "vlan_id": "",
        "src_zone": "",
        "dst_zone": "",
        "src_asset_group": "",
        "dst_asset_group": "",
        "nat_src_ip": "",
        "nat_dst_ip": "",
        "dst_asn": "",
        "dst_country": "",
        "asset_id": "",
        "user_id": "",
        "dataset_label": "",
        "traffic_family": "",
        "pcap_name": Path(source_file).name,
        "source_file": source_file,
    }


def parse_packet_rows(pcap_file: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    packet_number = 0
    try:
        with PcapReader(pcap_file) as reader:
            for packet in reader:
                packet_number += 1
                rows.append(extract_packet_fields(packet, pcap_file, packet_number))
    except Exception:
        # Fallback for edge-case captures where RawPcapReader is more tolerant.
        rows.clear()
        packet_number = 0
        with RawPcapReader(pcap_file) as reader:
            for raw_packet, metadata in reader:
                packet_number += 1
                try:
                    packet = IP(raw_packet)
                except Exception:
                    try:
                        packet = IPv6(raw_packet)
                    except Exception:
                        continue
                packet.time = getattr(metadata, "sec", 0) + getattr(metadata, "usec", 0) / 1_000_000
                rows.append(extract_packet_fields(packet, pcap_file, packet_number))
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def build_flow_rows(packet_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    for row in packet_rows:
        key = (
            row["source_file"],
            row["src_ip"],
            row["dst_ip"],
            row["src_port"],
            row["dst_port"],
            row["protocol"],
        )
        timestamp = row["timestamp"]
        bytes_value = int(row["bytes"])
        if key not in grouped:
            grouped[key] = {
                "timestamp": timestamp,
                "end_time": timestamp,
                "src_ip": row["src_ip"],
                "dst_ip": row["dst_ip"],
                "src_port": row["src_port"],
                "dst_port": row["dst_port"],
                "protocol": row["protocol"],
                "ip_version": row["ip_version"],
                "bytes": bytes_value,
                "bytes_total": bytes_value,
                "packets": 1,
                "packet_count": 1,
                "payload_bytes": int(row.get("payload_bytes") or 0),
                "flow_duration": 0.0,
                "duration": 0.0,
                "duration_ms": 0,
                "flow_id": row["flow_id"],
                "app_protocol": row.get("app_protocol", ""),
                "service": row.get("service", ""),
                "tcp_flags": row.get("tcp_flags", ""),
                "tcp_flags_seen": row.get("tcp_flags", ""),
                "ttl_min": row.get("ttl"),
                "ttl_max": row.get("ttl"),
                "ttl_avg": float(row["ttl"]) if row.get("ttl") not in (None, "") else None,
                "icmp_type": row.get("icmp_type"),
                "icmp_code": row.get("icmp_code"),
                "src_bytes": bytes_value,
                "dst_bytes": 0,
                "src_packets": 1,
                "dst_packets": 0,
                "direction": "unknown",
                "action": "observed",
                "session_state": row.get("session_state", ""),
                "rule_name": row.get("rule_name", ""),
                "dns_query": row.get("dns_query", ""),
                "tls_sni": row.get("tls_sni", ""),
                "http_host": row.get("http_host", ""),
                "device_id": row.get("device_id", ""),
                "sensor_id": row.get("sensor_id", ""),
                "vlan_id": row.get("vlan_id", ""),
                "src_zone": row.get("src_zone", ""),
                "dst_zone": row.get("dst_zone", ""),
                "src_asset_group": row.get("src_asset_group", ""),
                "dst_asset_group": row.get("dst_asset_group", ""),
                "nat_src_ip": row.get("nat_src_ip", ""),
                "nat_dst_ip": row.get("nat_dst_ip", ""),
                "dst_asn": row.get("dst_asn", ""),
                "dst_country": row.get("dst_country", ""),
                "asset_id": row.get("asset_id", ""),
                "user_id": row.get("user_id", ""),
                "source_file": row["source_file"],
            }
            continue
        current = grouped[key]
        current["bytes"] += bytes_value
        current["bytes_total"] += bytes_value
        current["packets"] += 1
        current["packet_count"] += 1
        current["payload_bytes"] += int(row.get("payload_bytes") or 0)
        current["src_bytes"] += bytes_value
        current["src_packets"] += 1
        if timestamp and (not current["timestamp"] or timestamp < current["timestamp"]):
            current["timestamp"] = timestamp
        if timestamp and (not current["end_time"] or timestamp > current["end_time"]):
            current["end_time"] = timestamp
        if row.get("tcp_flags"):
            existing = set(filter(None, str(current.get("tcp_flags_seen", "")).split(",")))
            existing.add(str(row["tcp_flags"]))
            current["tcp_flags_seen"] = ",".join(sorted(existing))
            current["tcp_flags"] = row["tcp_flags"]
        if not current.get("app_protocol") and row.get("app_protocol"):
            current["app_protocol"] = row["app_protocol"]
        if not current.get("service") and row.get("service"):
            current["service"] = row["service"]
        if not current.get("session_state") and row.get("session_state"):
            current["session_state"] = row["session_state"]
        if not current.get("rule_name") and row.get("rule_name"):
            current["rule_name"] = row["rule_name"]
        if not current.get("dns_query") and row.get("dns_query"):
            current["dns_query"] = row["dns_query"]
        if not current.get("tls_sni") and row.get("tls_sni"):
            current["tls_sni"] = row["tls_sni"]
        if not current.get("http_host") and row.get("http_host"):
            current["http_host"] = row["http_host"]
        ttl_value = row.get("ttl")
        if ttl_value not in (None, ""):
            ttl_int = int(ttl_value)
            if current.get("ttl_min") in (None, "") or ttl_int < int(current["ttl_min"]):
                current["ttl_min"] = ttl_int
            if current.get("ttl_max") in (None, "") or ttl_int > int(current["ttl_max"]):
                current["ttl_max"] = ttl_int
            if current.get("ttl_avg") in (None, ""):
                current["ttl_avg"] = float(ttl_int)
            else:
                packet_count = current["packets"]
                previous_sum = float(current["ttl_avg"]) * (packet_count - 1)
                current["ttl_avg"] = (previous_sum + ttl_int) / packet_count

    flow_rows: list[dict[str, Any]] = []
    for flow in grouped.values():
        if flow["timestamp"] and flow["end_time"]:
            start_dt = datetime.fromisoformat(flow["timestamp"])
            end_dt = datetime.fromisoformat(flow["end_time"])
            flow["flow_duration"] = max((end_dt - start_dt).total_seconds(), 0.0)
            flow["duration"] = flow["flow_duration"]
            flow["duration_ms"] = int(round(flow["flow_duration"] * 1000))
        flow_rows.append(flow)
    flow_rows.sort(key=lambda item: (item["timestamp"], item["src_ip"], item["dst_ip"], item["src_port"], item["dst_port"]))
    return flow_rows


def default_dataset_name(pcap_files: list[str]) -> str:
    if len(pcap_files) == 1:
        return sanitize_name(Path(pcap_files[0]).stem)
    digest = hashlib.sha1("|".join(sorted(pcap_files)).encode("utf-8")).hexdigest()[:8]
    return f"batch-{digest}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preprocess PCAP files into standardized packet.csv and flow.csv files.")
    parser.add_argument("--files", nargs="+", required=True, help="PCAP files, directories, or shorthand references")
    parser.add_argument("--dataset-name", default=None, help="Output dataset directory name under datasets/network-traffic/processed")
    parser.add_argument("--output-dir", default=None, help="Explicit output directory. Defaults to datasets/network-traffic/processed/<dataset-name>")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        pcap_files = discover_pcaps(args.files)
        if not pcap_files:
            parser.error("No PCAP files were found from --files")

        dataset_name = sanitize_name(args.dataset_name or default_dataset_name(pcap_files))
        output_dir = Path(args.output_dir) if args.output_dir else repo_root() / "datasets" / "network-traffic" / "processed" / dataset_name
        output_dir.mkdir(parents=True, exist_ok=True)

        packet_rows: list[dict[str, Any]] = []
        packet_counts: dict[str, int] = defaultdict(int)
        for pcap_file in pcap_files:
            rows = parse_packet_rows(pcap_file)
            packet_rows.extend(rows)
            packet_counts[pcap_file] = len(rows)

        for row in packet_rows:
            row["dataset_label"] = dataset_name
            row["traffic_family"] = infer_traffic_family(dataset_name, row["source_file"], row.get("app_protocol", ""), row.get("service", ""))

        packet_rows.sort(key=lambda item: (item["timestamp"], item["source_file"], item["packet_number"]))
        flow_rows = build_flow_rows(packet_rows)
        for row in flow_rows:
            row["dataset_label"] = dataset_name
            row["traffic_family"] = infer_traffic_family(dataset_name, row["source_file"], row.get("app_protocol", ""), row.get("service", ""))

        packet_path = output_dir / f"{dataset_name}.packet.csv"
        flow_path = output_dir / f"{dataset_name}.flow.csv"
        metadata_path = output_dir / "metadata.json"

        write_csv(
            packet_path,
            packet_rows,
            [
                "timestamp",
                "packet_number",
                "src_ip",
                "dst_ip",
                "src_port",
                "dst_port",
                "protocol",
                "app_protocol",
                "service",
                "ip_version",
                "frame_len",
                "ttl",
                "payload_bytes",
                "tcp_flags",
                "dns_query",
                "tls_sni",
                "http_host",
                "icmp_type",
                "icmp_code",
                "flow_id",
                "device_id",
                "sensor_id",
                "packet_count",
                "byte_count",
                "bytes_total",
                "src_bytes",
                "dst_bytes",
                "src_packets",
                "dst_packets",
                "duration_ms",
                "direction",
                "action",
                "session_state",
                "rule_name",
                "vlan_id",
                "src_zone",
                "dst_zone",
                "src_asset_group",
                "dst_asset_group",
                "nat_src_ip",
                "nat_dst_ip",
                "dst_asn",
                "dst_country",
                "asset_id",
                "user_id",
                "dataset_label",
                "traffic_family",
                "pcap_name",
                "mac_src",
                "mac_dst",
                "bytes",
                "source_file",
            ],
        )
        write_csv(
            flow_path,
            flow_rows,
            [
                "timestamp",
                "end_time",
                "src_ip",
                "dst_ip",
                "src_port",
                "dst_port",
                "protocol",
                "app_protocol",
                "service",
                "direction",
                "action",
                "session_state",
                "rule_name",
                "tcp_flags",
                "ip_version",
                "bytes",
                "bytes_total",
                "packets",
                "packet_count",
                "payload_bytes",
                "src_bytes",
                "dst_bytes",
                "src_packets",
                "dst_packets",
                "vlan_id",
                "src_zone",
                "dst_zone",
                "src_asset_group",
                "dst_asset_group",
                "nat_src_ip",
                "nat_dst_ip",
                "dst_asn",
                "dst_country",
                "dns_query",
                "tls_sni",
                "http_host",
                "asset_id",
                "user_id",
                "flow_duration",
                "duration",
                "duration_ms",
                "flow_id",
                "tcp_flags_seen",
                "ttl_min",
                "ttl_max",
                "ttl_avg",
                "icmp_type",
                "icmp_code",
                "device_id",
                "sensor_id",
                "dataset_label",
                "traffic_family",
                "source_file",
            ],
        )

        metadata = {
            "dataset_name": dataset_name,
            "engine": "scapy",
            "source_files": pcap_files,
            "packet_rows": len(packet_rows),
            "flow_rows": len(flow_rows),
            "per_file_packet_rows": packet_counts,
            "packet_csv": str(packet_path),
            "flow_csv": str(flow_path),
        }
        metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

        if args.format == "json":
            print(json.dumps(metadata, ensure_ascii=False))
        else:
            print(
                "\n".join(
                    [
                        f"Prepared dataset: {dataset_name}",
                        f"Source PCAP files: {len(pcap_files)}",
                        f"Packet rows: {len(packet_rows)}",
                        f"Flow rows: {len(flow_rows)}",
                        f"packet.csv: {packet_path}",
                        f"flow.csv: {flow_path}",
                        f"metadata: {metadata_path}",
                    ]
                )
            )
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
