#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
from contextlib import suppress
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from file_resolution import is_explicit_path_reference, normalize_name
from utils.path import dataset_root, repo_root, skill_root, to_repo_relative_display, network_traffic_workspace_root
from utils.io import load_json, save_json

BOOTP = DHCP = DNS = DNSQR = ICMP = IP = Raw = TCP = UDP = Ether = IPv6 = PcapReader = RawPcapReader = None
_SCAPY_LOADED = False


def ensure_scapy() -> None:
    global BOOTP, DHCP, DNS, DNSQR, ICMP, IP, Raw, TCP, UDP, Ether, IPv6, PcapReader, RawPcapReader, _SCAPY_LOADED
    if _SCAPY_LOADED:
        return
    try:
        from scapy.all import BOOTP, DHCP, DNS, DNSQR, ICMP, IP, Raw, TCP, UDP, Ether, IPv6, PcapReader, RawPcapReader  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "Missing optional dependency 'scapy'. Install it before running PCAP fallback parsing: pip install scapy"
        ) from exc
    _SCAPY_LOADED = True


PCAP_PATTERNS = ("*.pcap", "*.pcapng", "*.cap")
RELATIVE_TIME_EPOCH_THRESHOLD = 946684800  # 2000-01-01T00:00:00Z
TCP_IDLE_TIMEOUT_SECONDS = 60
NON_TCP_IDLE_TIMEOUT_SECONDS = 30
GREASE_VALUES = {
    0x0A0A,
    0x1A1A,
    0x2A2A,
    0x3A3A,
    0x4A4A,
    0x5A5A,
    0x6A6A,
    0x7A7A,
    0x8A8A,
    0x9A9A,
    0xAAAA,
    0xBABA,
    0xCACA,
    0xDADA,
    0xEAEA,
    0xFAFA,
}


def get_search_roots() -> list[Path]:
    ds = dataset_root()
    repo_base = repo_root() / "datasets" / "network-traffic"
    candidates = [
        ds / "raw",
        ds / "processed",
        repo_base / "raw",
        repo_base / "processed",
    ]
    roots: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key not in seen:
            roots.append(candidate)
            seen.add(key)
    return roots


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
        sample = "\n".join(f"  - {to_repo_relative_display(item)}" for item in deduped[:10])
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
        elif is_explicit_path_reference(value):
            raise ValueError(f"PCAP path '{value}' does not exist.")
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


def coerce_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def lexical_entropy(value: str) -> float:
    if not value:
        return 0.0
    counts: dict[str, int] = {}
    for char in value:
        counts[char] = counts.get(char, 0) + 1
    total = len(value)
    entropy = 0.0
    for count in counts.values():
        probability = count / total
        entropy -= probability * math.log2(probability)
    return round(entropy, 4)


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


def safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def resolve_tshark_binary() -> str | None:
    explicit = os.environ.get("TSHARK_BIN") if "os" in globals() else None
    if explicit:
        return explicit
    return shutil.which("tshark")


def resolve_zeek_binary() -> str | None:
    explicit = os.environ.get("ZEEK_BIN") if "os" in globals() else None
    if explicit:
        return explicit
    return shutil.which("zeek") or shutil.which("bro")


def infer_payload_bytes(packet: Any) -> int:
    if packet.haslayer(TCP):
        with suppress(Exception):
            if packet.haslayer(IP):
                ip_total_len = safe_int(getattr(packet[IP], "len", None))
                ip_header_len = int(getattr(packet[IP], "ihl", 5)) * 4
                tcp_header_len = int(getattr(packet[TCP], "dataofs", 5)) * 4
                if ip_total_len is not None:
                    return max(ip_total_len - ip_header_len - tcp_header_len, 0)
            if packet.haslayer(IPv6):
                ipv6_payload_len = safe_int(getattr(packet[IPv6], "plen", None))
                tcp_header_len = int(getattr(packet[TCP], "dataofs", 5)) * 4
                if ipv6_payload_len is not None:
                    return max(ipv6_payload_len - tcp_header_len, 0)
        with suppress(Exception):
            return len(bytes(packet[TCP].payload))
        return 0
    if packet.haslayer(UDP):
        with suppress(Exception):
            udp_len = safe_int(getattr(packet[UDP], "len", None))
            if udp_len is not None:
                return max(udp_len - 8, 0)
        with suppress(Exception):
            return len(bytes(packet[UDP].payload))
        return 0
    if packet.haslayer(ICMP):
        with suppress(Exception):
            return len(bytes(packet[ICMP].payload))
        return 0
    return 0


def normalize_capture_time(epoch_value: Any, base_epoch: float | None) -> Any:
    if epoch_value in (None, ""):
        return epoch_value
    try:
        value = float(epoch_value)
    except (TypeError, ValueError):
        return epoch_value
    if base_epoch is None:
        return value
    return max(value - base_epoch, 0.0)


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


def extract_http_user_agent(packet: Any) -> str:
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
            if line.lower().startswith("user-agent:"):
                return line.split(":", 1)[1].strip()
    return ""


def extract_dhcp_metadata(packet: Any) -> dict[str, str]:
    metadata = {
        "dhcp_fingerprint": "",
        "dhcp_vendor": "",
        "dhcp_hostname": "",
    }
    if not packet.haslayer(DHCP):
        return metadata
    with suppress(Exception):
        for option in packet[DHCP].options:
            if not isinstance(option, tuple) or len(option) < 2:
                continue
            name = str(option[0])
            value = option[1]
            if name == "param_req_list":
                if isinstance(value, (list, tuple)):
                    metadata["dhcp_fingerprint"] = ",".join(str(int(item)) for item in value if str(item).isdigit())
                else:
                    metadata["dhcp_fingerprint"] = str(value)
            elif name == "vendor_class_id":
                metadata["dhcp_vendor"] = decode_text(value) if isinstance(value, bytes) else str(value)
            elif name == "hostname":
                metadata["dhcp_hostname"] = decode_text(value) if isinstance(value, bytes) else str(value)
    return metadata


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


TSHARK_FIELDS = [
    "frame.number",
    "frame.time_epoch",
    "frame.len",
    "frame.protocols",
    "eth.src",
    "eth.dst",
    "ip.src",
    "ipv6.src",
    "ip.dst",
    "ipv6.dst",
    "tcp.srcport",
    "tcp.dstport",
    "udp.srcport",
    "udp.dstport",
    "ip.ttl",
    "ipv6.hlim",
    "ip.len",
    "ipv6.plen",
    "tcp.hdr_len",
    "tcp.len",
    "tcp.flags.str",
    "tls.handshake.type",
    "tls.handshake.version",
    "tls.handshake.ciphersuite",
    "tls.handshake.extension.type",
    "tls.handshake.extensions_supported_group",
    "tls.handshake.extensions_ec_point_format",
    "icmp.type",
    "icmp.code",
    "dns.qry.name",
    "tls.handshake.extensions_server_name",
    "http.host",
    "http.user_agent",
    "dhcp.option.request_list_item",
    "dhcp.option.vendor_class_id",
    "dhcp.option.hostname",
    "vlan.id",
    "tcp.analysis.ack_rtt",
    "tcp.analysis.retransmission",
    "tcp.analysis.fast_retransmission",
    "tcp.analysis.spurious_retransmission",
    "tcp.analysis.duplicate_ack",
    "tcp.analysis.lost_segment",
    "tcp.analysis.out_of_order",
]


def normalize_tshark_value(value: str) -> str:
    return value.strip()


def first_tshark_value(value: Any) -> str:
    text = normalize_tshark_value(str(value or ""))
    if "," in text:
        return text.split(",", 1)[0].strip()
    return text


def tshark_values(value: Any) -> list[str]:
    text = normalize_tshark_value(str(value or ""))
    if not text:
        return []
    values: list[str] = []
    for part in re.split(r"[,;]", text):
        item = part.strip()
        if item and item not in values:
            values.append(item)
    return values


def tls_decimal_values(value: Any) -> list[str]:
    decimal_values: list[str] = []
    for item in tshark_values(value):
        try:
            number = int(item, 16) if item.lower().startswith("0x") else int(item)
        except ValueError:
            continue
        if number in GREASE_VALUES:
            continue
        decimal_values.append(str(number))
    return decimal_values


def ja3_hash_from_parts(version: str, ciphers: list[str], extensions: list[str], groups: list[str], point_formats: list[str]) -> tuple[str, str]:
    ja3_string = ",".join(
        [
            version,
            "-".join(ciphers),
            "-".join(extensions),
            "-".join(groups),
            "-".join(point_formats),
        ]
    )
    return ja3_string, hashlib.md5(ja3_string.encode("utf-8")).hexdigest()


def ja3s_hash_from_parts(version: str, selected_cipher: str, extensions: list[str]) -> tuple[str, str]:
    ja3s_string = ",".join([version, selected_cipher, "-".join(extensions)])
    return ja3s_string, hashlib.md5(ja3s_string.encode("utf-8")).hexdigest()


def tls_metadata_from_tshark_row(row_values: dict[str, str]) -> dict[str, Any]:
    handshake_types = set(tls_decimal_values(row_values.get("tls.handshake.type", "")))
    version_values = tls_decimal_values(row_values.get("tls.handshake.version", ""))
    cipher_values = tls_decimal_values(row_values.get("tls.handshake.ciphersuite", ""))
    extension_values = tls_decimal_values(row_values.get("tls.handshake.extension.type", ""))
    group_values = tls_decimal_values(row_values.get("tls.handshake.extensions_supported_group", ""))
    point_format_values = tls_decimal_values(row_values.get("tls.handshake.extensions_ec_point_format", ""))

    metadata: dict[str, Any] = {
        "tls_handshake_type": "-".join(sorted(handshake_types)),
        "tls_version": version_values[0] if version_values else "",
        "tls_ciphers": "",
        "tls_extensions": "",
        "tls_supported_groups": "",
        "tls_point_formats": "",
        "tls_server_cipher": "",
        "tls_server_extensions": "",
        "ja3_string": "",
        "ja3_hash": "",
        "ja3s_string": "",
        "ja3s_hash": "",
        "tls_metadata_source": "",
    }

    if "1" in handshake_types and version_values:
        metadata["tls_ciphers"] = "-".join(cipher_values)
        metadata["tls_extensions"] = "-".join(extension_values)
        metadata["tls_supported_groups"] = "-".join(group_values)
        metadata["tls_point_formats"] = "-".join(point_format_values)
        ja3_string, ja3_hash = ja3_hash_from_parts(
            version_values[0],
            cipher_values,
            extension_values,
            group_values,
            point_format_values,
        )
        metadata["ja3_string"] = ja3_string
        metadata["ja3_hash"] = ja3_hash
        metadata["tls_metadata_source"] = "tshark_clienthello"

    if "2" in handshake_types and version_values:
        selected_cipher = cipher_values[0] if cipher_values else ""
        metadata["tls_server_cipher"] = selected_cipher
        metadata["tls_server_extensions"] = "-".join(extension_values)
        ja3s_string, ja3s_hash = ja3s_hash_from_parts(version_values[0], selected_cipher, extension_values)
        metadata["ja3s_string"] = ja3s_string
        metadata["ja3s_hash"] = ja3s_hash
        metadata["tls_metadata_source"] = metadata["tls_metadata_source"] or "tshark_serverhello"

    return metadata


def dhcp_fingerprint_from_tshark_value(value: Any) -> str:
    items: list[str] = []
    for item in tshark_values(value):
        text = item.lower()
        try:
            number = int(text, 16) if text.startswith("0x") else int(text)
        except ValueError:
            continue
        items.append(str(number))
    return ",".join(items)


def tshark_flag_present(value: str) -> int:
    return 1 if normalize_tshark_value(value) else 0


def tshark_protocol(frame_protocols: str, tcp_src: str, udp_src: str, icmp_type: str, src_ip: str, src_ip_v6: str) -> str:
    protocols = frame_protocols.lower()
    if tcp_src:
        return "TCP"
    if udp_src:
        return "UDP"
    if icmp_type:
        return "ICMP"
    if "icmpv6" in protocols:
        return "ICMPV6"
    if src_ip_v6:
        return "IPV6"
    if src_ip:
        return "IP"
    return "UNKNOWN"


def tshark_payload_bytes(protocol: str, frame_len: int, ip_len: int | None, ipv6_plen: int | None, tcp_hdr_len: int | None, tcp_len: int | None) -> int:
    if protocol == "TCP":
        if tcp_len is not None:
            return max(tcp_len, 0)
        if ip_len is not None and tcp_hdr_len is not None:
            return max(ip_len - 20 - tcp_hdr_len, 0)
        if ipv6_plen is not None and tcp_hdr_len is not None:
            return max(ipv6_plen - tcp_hdr_len, 0)
    if protocol == "UDP":
        if ip_len is not None:
            return max(ip_len - 20 - 8, 0)
        if ipv6_plen is not None:
            return max(ipv6_plen - 8, 0)
    return max(frame_len, 0)


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


def make_conversation_key(row: dict[str, Any]) -> tuple[str, str, tuple[str, str], tuple[str, str]]:
    endpoint_a = (row["src_ip"], row["src_port"])
    endpoint_b = (row["dst_ip"], row["dst_port"])
    ordered = tuple(sorted((endpoint_a, endpoint_b)))
    return (row["source_file"], row["protocol"], ordered[0], ordered[1])


def make_conversation_flow_id(key: tuple[str, str, tuple[str, str], tuple[str, str]]) -> str:
    source_file, protocol, endpoint_a, endpoint_b = key
    raw = "|".join(
        [
            source_file,
            protocol,
            endpoint_a[0],
            endpoint_a[1],
            endpoint_b[0],
            endpoint_b[1],
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def make_session_flow_id(
    key: tuple[str, str, tuple[str, str], tuple[str, str]],
    session_index: int,
    start_timestamp: str,
) -> str:
    source_file, protocol, endpoint_a, endpoint_b = key
    raw = "|".join(
        [
            source_file,
            protocol,
            endpoint_a[0],
            endpoint_a[1],
            endpoint_b[0],
            endpoint_b[1],
            str(session_index),
            start_timestamp,
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def flow_source_key(value: str) -> str:
    return to_repo_relative_display(value)


def normalized_conversation_key(row: dict[str, Any]) -> tuple[str, str, tuple[str, str], tuple[str, str]]:
    endpoints = tuple(
        sorted(
            (
                (str(row.get("src_ip", "")), str(row.get("src_port", ""))),
                (str(row.get("dst_ip", "")), str(row.get("dst_port", ""))),
            )
        )
    )
    return (
        flow_source_key(str(row.get("source_file", ""))),
        str(row.get("protocol", "")),
        endpoints[0],
        endpoints[1],
    )


def flow_time_bounds(row: dict[str, Any]) -> tuple[str | None, float | None, float | None]:
    if row.get("time_is_relative") == "true":
        start_rel = coerce_float(row.get("start_relative_time_s"))
        if start_rel is None:
            start_rel = coerce_float(row.get("relative_time_s"))
        end_rel = coerce_float(row.get("end_relative_time_s"))
        if end_rel is None:
            end_rel = start_rel
        if start_rel is None:
            return (None, None, None)
        return ("relative", start_rel, end_rel if end_rel is not None else start_rel)

    start_ts = parse_iso_datetime(str(row.get("timestamp") or ""))
    end_ts = parse_iso_datetime(str(row.get("end_time") or ""))
    if start_ts is None:
        return (None, None, None)
    start_epoch = start_ts.timestamp()
    end_epoch = end_ts.timestamp() if end_ts is not None else start_epoch
    return ("absolute", start_epoch, end_epoch)


def same_flow_direction(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (
        str(left.get("src_ip", "")) == str(right.get("src_ip", ""))
        and str(left.get("dst_ip", "")) == str(right.get("dst_ip", ""))
        and str(left.get("src_port", "")) == str(right.get("src_port", ""))
        and str(left.get("dst_port", "")) == str(right.get("dst_port", ""))
    )


def flow_match_score(target: dict[str, Any], candidate: dict[str, Any]) -> tuple[float, float, int]:
    target_kind, target_start, target_end = flow_time_bounds(target)
    candidate_kind, candidate_start, candidate_end = flow_time_bounds(candidate)

    overlap_seconds = 0.0
    start_delta = float("inf")
    if (
        target_kind is not None
        and candidate_kind is not None
        and target_kind == candidate_kind
        and target_start is not None
        and candidate_start is not None
    ):
        resolved_target_end = target_end if target_end is not None else target_start
        resolved_candidate_end = candidate_end if candidate_end is not None else candidate_start
        overlap_seconds = max(0.0, min(resolved_target_end, resolved_candidate_end) - max(target_start, candidate_start))
        start_delta = abs(target_start - candidate_start)

    return (overlap_seconds, -start_delta, 1 if same_flow_direction(target, candidate) else 0)


def apply_packet_qos_signal(flow: dict[str, Any], row: dict[str, Any]) -> None:
    if row.get("_packet_qos_supported") == "true":
        flow["_packet_qos_supported"] = True

    ack_rtt_ms = coerce_float(row.get("_ack_rtt_ms"))
    if ack_rtt_ms is not None:
        flow["_rtt_sample_count"] += 1
        flow["_rtt_sum_ms"] += ack_rtt_ms
        if flow.get("_rtt_last_ms") is not None:
            flow["_rtt_delta_sum_ms"] += abs(ack_rtt_ms - float(flow["_rtt_last_ms"]))
            flow["_rtt_delta_count"] += 1
        flow["_rtt_last_ms"] = ack_rtt_ms

    flow["_retransmission_count"] += safe_int(row.get("_retransmission_flag")) or 0
    flow["_duplicate_ack_count"] += safe_int(row.get("_duplicate_ack_flag")) or 0
    flow["_lost_segment_count"] += safe_int(row.get("_lost_segment_flag")) or 0
    flow["_out_of_order_count"] += safe_int(row.get("_out_of_order_flag")) or 0


def summarize_session_state(protocol: str, tcp_flags_seen: str) -> str:
    if protocol != "TCP" or not tcp_flags_seen:
        return ""
    flags = {flag.strip().upper() for flag in tcp_flags_seen.split(",") if flag.strip()}
    if any("R" in flag for flag in flags):
        return "RST"
    if any("F" in flag for flag in flags):
        return "FIN"
    saw_syn = any("S" in flag and "A" not in flag for flag in flags)
    saw_syn_ack = any("S" in flag and "A" in flag for flag in flags)
    saw_ack = any(flag == "A" or ("A" in flag and "S" not in flag and "F" not in flag and "R" not in flag) for flag in flags)
    if saw_syn and saw_syn_ack and saw_ack:
        return "ESTABLISHED"
    if saw_syn and not saw_syn_ack and not saw_ack:
        return "SYN_ONLY"
    if saw_syn_ack:
        return "SYN_ACK"
    if saw_syn:
        return "SYN"
    if saw_ack:
        return "ACK"
    return sorted(flags)[-1] if flags else ""


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    with suppress(Exception):
        return datetime.fromisoformat(value)
    return None


def is_tcp_syn_start(tcp_flags: str) -> bool:
    normalized = (tcp_flags or "").upper()
    return "S" in normalized and "A" not in normalized and "R" not in normalized and "F" not in normalized


def is_tcp_terminator(tcp_flags: str) -> bool:
    normalized = (tcp_flags or "").upper()
    return "R" in normalized or "F" in normalized


def session_idle_timeout_seconds(protocol: str) -> int:
    if protocol == "TCP":
        return TCP_IDLE_TIMEOUT_SECONDS
    return NON_TCP_IDLE_TIMEOUT_SECONDS


def derive_flow_start_reason(row: dict[str, Any], matched_session: dict[str, Any] | None = None) -> str:
    if matched_session is None:
        if row["protocol"] == "TCP" and is_tcp_syn_start(str(row.get("tcp_flags", ""))):
            return "syn"
        return "first_packet"
    return "continued"


def initialize_flow_session(
    row: dict[str, Any],
    key: tuple[str, str, tuple[str, str], tuple[str, str]],
    session_index: int,
) -> dict[str, Any]:
    timestamp = row["timestamp"]
    bytes_value = int(row["bytes"])
    rel_time = row.get("relative_time_s")

    session = {
        "timestamp": timestamp,
        "end_time": timestamp,
        "start_relative_time_s": rel_time,
        "end_relative_time_s": rel_time,
        "time_is_relative": row.get("time_is_relative", "false"),
        "src_ip": row["src_ip"],
        "dst_ip": row["dst_ip"],
        "src_port": row["src_port"],
        "dst_port": row["dst_port"],
        "protocol": row["protocol"],
        "origin_src_ip": row["src_ip"],
        "origin_dst_ip": row["dst_ip"],
        "origin_src_port": row["src_port"],
        "origin_dst_port": row["dst_port"],
        "ip_version": row["ip_version"],
        "bytes": bytes_value,
        "bytes_total": bytes_value,
        "packets": 1,
        "packet_count": 1,
        "payload_bytes": int(row.get("payload_bytes") or 0),
        "flow_duration": 0.0,
        "duration": 0.0,
        "duration_ms": 0,
        "rtt_ms": "",
        "jitter_ms": "",
        "packet_loss_pct": "",
        "retransmission_count": "",
        "retransmission_rate": "",
        "flow_id": make_session_flow_id(key, session_index, timestamp or ""),
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
        "direction": "unidirectional",
        "src_role": "first_seen",
        "dst_role": "reverse_seen",
        "action": "observed",
        "session_state": row.get("session_state", ""),
        "flow_start_reason": derive_flow_start_reason(row),
        "flow_end_reason": "",
        "rule_name": row.get("rule_name", ""),
        "dns_query": row.get("dns_query", ""),
        "tls_sni": row.get("tls_sni", ""),
        "tls_handshake_type": row.get("tls_handshake_type", ""),
        "tls_version": row.get("tls_version", ""),
        "tls_ciphers": row.get("tls_ciphers", ""),
        "tls_extensions": row.get("tls_extensions", ""),
        "tls_supported_groups": row.get("tls_supported_groups", ""),
        "tls_point_formats": row.get("tls_point_formats", ""),
        "tls_server_cipher": row.get("tls_server_cipher", ""),
        "tls_server_extensions": row.get("tls_server_extensions", ""),
        "ja3_string": row.get("ja3_string", ""),
        "ja3_hash": row.get("ja3_hash", ""),
        "ja3s_string": row.get("ja3s_string", ""),
        "ja3s_hash": row.get("ja3s_hash", ""),
        "tls_metadata_source": row.get("tls_metadata_source", ""),
        "http_host": row.get("http_host", ""),
        "http_user_agent": row.get("http_user_agent", ""),
        "dhcp_fingerprint": row.get("dhcp_fingerprint", ""),
        "dhcp_vendor": row.get("dhcp_vendor", ""),
        "dhcp_hostname": row.get("dhcp_hostname", ""),
        "ssh_hassh": row.get("ssh_hassh", ""),
        "p0f_os": row.get("p0f_os", ""),
        "tcp_syn_signature": row.get("tcp_syn_signature", ""),
        "mac_src": row.get("mac_src", ""),
        "mac_dst": row.get("mac_dst", ""),
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
        "_last_seen_dt": parse_iso_datetime(timestamp),
        "_last_relative_time": float(rel_time) if rel_time not in (None, "") else None,
        "_closed": row["protocol"] == "TCP" and is_tcp_terminator(str(row.get("tcp_flags", ""))),
        "_packet_qos_supported": False,
        "_rtt_sample_count": 0,
        "_rtt_sum_ms": 0.0,
        "_rtt_last_ms": None,
        "_rtt_delta_sum_ms": 0.0,
        "_rtt_delta_count": 0,
        "_retransmission_count": 0,
        "_duplicate_ack_count": 0,
        "_lost_segment_count": 0,
        "_out_of_order_count": 0,
    }

    apply_packet_qos_signal(session, row)
    return session


def update_flow_session(current: dict[str, Any], row: dict[str, Any]) -> None:
    timestamp = row["timestamp"]
    bytes_value = int(row["bytes"])
    current["bytes"] += bytes_value
    current["bytes_total"] += bytes_value
    current["packets"] += 1
    current["packet_count"] += 1
    current["payload_bytes"] += int(row.get("payload_bytes") or 0)
    same_direction = (
        row["src_ip"] == current["origin_src_ip"]
        and row["dst_ip"] == current["origin_dst_ip"]
        and row["src_port"] == current["origin_src_port"]
        and row["dst_port"] == current["origin_dst_port"]
    )
    if same_direction:
        current["src_bytes"] += bytes_value
        current["src_packets"] += 1
    else:
        current["dst_bytes"] += bytes_value
        current["dst_packets"] += 1
        current["direction"] = "bidirectional"
    if timestamp and (not current["timestamp"] or timestamp < current["timestamp"]):
        current["timestamp"] = timestamp
    if timestamp and (not current["end_time"] or timestamp > current["end_time"]):
        current["end_time"] = timestamp
    row_relative_time = row.get("relative_time_s")
    if row_relative_time not in (None, ""):
        if current.get("start_relative_time_s") in (None, "") or float(row_relative_time) < float(current["start_relative_time_s"]):
            current["start_relative_time_s"] = row_relative_time
        if current.get("end_relative_time_s") in (None, "") or float(row_relative_time) > float(current["end_relative_time_s"]):
            current["end_relative_time_s"] = row_relative_time
    if row.get("tcp_flags"):
        existing = set(filter(None, str(current.get("tcp_flags_seen", "")).split(",")))
        existing.add(str(row["tcp_flags"]))
        current["tcp_flags_seen"] = ",".join(sorted(existing))
        current["tcp_flags"] = row["tcp_flags"]
    if not current.get("app_protocol") and row.get("app_protocol"):
        current["app_protocol"] = row["app_protocol"]
    if not current.get("service") and row.get("service"):
        current["service"] = row["service"]
    if not current.get("rule_name") and row.get("rule_name"):
        current["rule_name"] = row["rule_name"]
    if not current.get("dns_query") and row.get("dns_query"):
        current["dns_query"] = row["dns_query"]
    if not current.get("tls_sni") and row.get("tls_sni"):
        current["tls_sni"] = row["tls_sni"]
    for field in (
        "tls_handshake_type",
        "tls_version",
        "tls_ciphers",
        "tls_extensions",
        "tls_supported_groups",
        "tls_point_formats",
        "tls_server_cipher",
        "tls_server_extensions",
        "ja3_string",
        "ja3_hash",
        "ja3s_string",
        "ja3s_hash",
        "tls_metadata_source",
    ):
        if not current.get(field) and row.get(field):
            current[field] = row[field]
    if not current.get("http_host") and row.get("http_host"):
        current["http_host"] = row["http_host"]
    for field in (
        "http_user_agent",
        "dhcp_fingerprint",
        "dhcp_vendor",
        "dhcp_hostname",
        "ssh_hassh",
        "p0f_os",
        "tcp_syn_signature",
        "mac_src",
        "mac_dst",
    ):
        if not current.get(field) and row.get(field):
            current[field] = row[field]
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
    current["_last_seen_dt"] = parse_iso_datetime(timestamp)
    # Update relative time tracking
    row_relative_time = row.get("relative_time_s")
    if row_relative_time not in (None, ""):
        current["_last_relative_time"] = float(row_relative_time)
        if current.get("end_relative_time_s") in (None, "") or float(row_relative_time) > float(current["end_relative_time_s"]):
            current["end_relative_time_s"] = row_relative_time
    if row["protocol"] == "TCP" and is_tcp_terminator(str(row.get("tcp_flags", ""))):
        current["_closed"] = True
        current["flow_end_reason"] = "tcp_terminator"
    apply_packet_qos_signal(current, row)


def finalize_flow_session(flow: dict[str, Any]) -> dict[str, Any]:
    if flow.get("time_is_relative") == "true":
        start_rel = coerce_float(flow.get("start_relative_time_s"))
        end_rel = coerce_float(flow.get("end_relative_time_s"))
        if start_rel is not None and end_rel is not None:
            flow["flow_duration"] = max(end_rel - start_rel, 0.0)
            flow["duration"] = flow["flow_duration"]
            flow["duration_ms"] = int(max(1, round(flow["flow_duration"] * 1000))) if flow["flow_duration"] > 0 else 0
    elif flow["timestamp"] and flow["end_time"]:
        start_dt = datetime.fromisoformat(flow["timestamp"])
        end_dt = datetime.fromisoformat(flow["end_time"])
        flow["flow_duration"] = max((end_dt - start_dt).total_seconds(), 0.0)
        flow["duration"] = flow["flow_duration"]
        flow["duration_ms"] = int(max(1, round(flow["flow_duration"] * 1000))) if flow["flow_duration"] > 0 else 0
    flow["session_state"] = summarize_session_state(flow["protocol"], str(flow.get("tcp_flags_seen", "")))
    src_bytes = int(flow.get("src_bytes") or 0)
    dst_bytes = int(flow.get("dst_bytes") or 0)
    src_packets = int(flow.get("src_packets") or 0)
    dst_packets = int(flow.get("dst_packets") or 0)
    total_bytes = max(int(flow.get("bytes_total") or flow.get("bytes") or 0), 0)
    total_packets = max(int(flow.get("packet_count") or flow.get("packets") or 0), 0)
    flow["src_to_dst_byte_ratio"] = round(src_bytes / max(dst_bytes, 1), 4)
    flow["src_to_dst_packet_ratio"] = round(src_packets / max(dst_packets, 1), 4)
    flow["byte_asymmetry"] = round(abs(src_bytes - dst_bytes) / max(total_bytes, 1), 4)
    flow["packet_asymmetry"] = round(abs(src_packets - dst_packets) / max(total_packets, 1), 4)
    ttl_min = flow.get("ttl_min")
    ttl_max = flow.get("ttl_max")
    if ttl_min not in (None, "") and ttl_max not in (None, ""):
        flow["ttl_range"] = max(int(ttl_max) - int(ttl_min), 0)
    else:
        flow["ttl_range"] = ""
    dns_query = str(flow.get("dns_query") or "").strip().lower().rstrip(".")
    flow["dns_query_length"] = len(dns_query) if dns_query else 0
    flow["dns_label_count"] = dns_query.count(".") + 1 if dns_query else 0
    flow["dns_query_entropy"] = lexical_entropy(dns_query) if dns_query else 0.0
    if flow.get("_packet_qos_supported") and flow["protocol"] == "TCP":
        total_tcp_packets = max(int(flow.get("packet_count") or flow.get("packets") or 0), 1)
        retransmission_count = int(flow.get("_retransmission_count") or 0)
        lost_segment_count = int(flow.get("_lost_segment_count") or 0)
        flow["retransmission_count"] = retransmission_count
        flow["retransmission_rate"] = round(retransmission_count / total_tcp_packets, 4)
        flow["packet_loss_pct"] = round((lost_segment_count * 100.0) / total_tcp_packets, 4)
        if int(flow.get("_rtt_sample_count") or 0) > 0:
            flow["rtt_ms"] = round(float(flow["_rtt_sum_ms"]) / int(flow["_rtt_sample_count"]), 3)
        if int(flow.get("_rtt_delta_count") or 0) > 0:
            flow["jitter_ms"] = round(float(flow["_rtt_delta_sum_ms"]) / int(flow["_rtt_delta_count"]), 3)
    if not flow.get("flow_end_reason"):
        flow["flow_end_reason"] = "idle_timeout_or_end_of_capture" if flow.get("_closed") else "end_of_capture"
    flow.pop("origin_src_ip", None)
    flow.pop("origin_dst_ip", None)
    flow.pop("origin_src_port", None)
    flow.pop("origin_dst_port", None)
    flow.pop("_last_seen_dt", None)
    flow.pop("_last_relative_time", None)
    flow.pop("_closed", None)
    flow.pop("_packet_qos_supported", None)
    flow.pop("_rtt_sample_count", None)
    flow.pop("_rtt_sum_ms", None)
    flow.pop("_rtt_last_ms", None)
    flow.pop("_rtt_delta_sum_ms", None)
    flow.pop("_rtt_delta_count", None)
    flow.pop("_retransmission_count", None)
    flow.pop("_duplicate_ack_count", None)
    flow.pop("_lost_segment_count", None)
    flow.pop("_out_of_order_count", None)
    return flow


def expire_inactive_sessions(sessions: list[dict[str, Any]], packet_dt: datetime | None, protocol: str,
                            packet_relative_time: float | None = None) -> None:
    timeout_seconds = session_idle_timeout_seconds(protocol)

    # Priority 1: Use relative time (works for both absolute and relative time modes)
    if packet_relative_time is not None:
        for session in sessions:
            if session.get("_closed"):
                continue
            last_rel_time = session.get("_last_relative_time")
            if last_rel_time is not None:
                gap = packet_relative_time - float(last_rel_time)
                if gap > timeout_seconds:
                    session["_closed"] = True
                    session["flow_end_reason"] = "idle_timeout"
        return

    # Priority 2: Fallback to absolute time (legacy behavior)
    if packet_dt is None:
        return
    for session in sessions:
        if session.get("_closed"):
            continue
        last_seen = session.get("_last_seen_dt")
        if last_seen and (packet_dt - last_seen).total_seconds() > timeout_seconds:
            session["_closed"] = True
            session["flow_end_reason"] = "idle_timeout"


def score_session_match(session: dict[str, Any], row: dict[str, Any]) -> tuple[int, int, str]:
    same_direction = (
        row["src_ip"] == session["origin_src_ip"]
        and row["dst_ip"] == session["origin_dst_ip"]
        and row["src_port"] == session["origin_src_port"]
        and row["dst_port"] == session["origin_dst_port"]
    )
    reverse_direction = (
        row["src_ip"] == session["origin_dst_ip"]
        and row["dst_ip"] == session["origin_src_ip"]
        and row["src_port"] == session["origin_dst_port"]
        and row["dst_port"] == session["origin_src_port"]
    )
    direction_score = 3 if same_direction else 2 if reverse_direction else 0
    state = str(session.get("session_state", "")).upper()
    maturity_score = 2 if state == "ESTABLISHED" else 1 if state in {"SYN_ACK", "ACK", "SYN", "SYN_ONLY"} else 0
    return (direction_score, maturity_score, session.get("timestamp", ""))


def find_matching_session(
    sessions: list[dict[str, Any]],
    row: dict[str, Any],
    packet_dt: datetime | None,
    packet_relative_time: float | None = None,
) -> dict[str, Any] | None:
    protocol = row["protocol"]
    expire_inactive_sessions(sessions, packet_dt, protocol, packet_relative_time)
    active_candidates: list[dict[str, Any]] = []
    for session in sessions:
        if session.get("_closed"):
            continue
        active_candidates.append(session)

    if not active_candidates:
        return None

    tcp_flags = str(row.get("tcp_flags", ""))
    if protocol == "TCP" and is_tcp_syn_start(tcp_flags):
        for session in reversed(active_candidates):
            same_direction = (
                row["src_ip"] == session["origin_src_ip"]
                and row["dst_ip"] == session["origin_dst_ip"]
                and row["src_port"] == session["origin_src_port"]
                and row["dst_port"] == session["origin_dst_port"]
            )
            if same_direction and session.get("dst_packets", 0) == 0 and session.get("packets", 0) <= 3:
                return session
        return None

    ranked = sorted(active_candidates, key=lambda session: score_session_match(session, row))
    return ranked[-1]


def extract_packet_fields(
    packet: Any,
    source_file: str,
    packet_number: int,
    packet_time: Any | None = None,
    *,
    use_relative_time: bool = False,
) -> dict[str, Any]:
    ensure_scapy()
    normalized_time = packet_time if packet_time is not None else getattr(packet, "time", None)
    relative_time_s = coerce_float(normalized_time) if use_relative_time else None
    timestamp = "" if use_relative_time else iso_timestamp(normalized_time)
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
    http_user_agent = extract_http_user_agent(packet)
    dhcp_metadata = extract_dhcp_metadata(packet)
    tls_sni = extract_tls_sni(packet)
    service = infer_service(dst_port, protocol, dns_query, tls_sni, http_host)
    app_protocol = infer_app_protocol(dst_port, protocol, dns_query, tls_sni, http_host)
    session_state = infer_session_state(tcp_flags, protocol)
    rule_name = infer_rule_name(app_protocol, dns_query, tls_sni, http_host)
    packet_len = int(len(packet))

    return {
        "timestamp": timestamp,
        "relative_time_s": relative_time_s if relative_time_s is not None else "",
        "time_is_relative": "true" if use_relative_time else "false",
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
        "tls_handshake_type": "",
        "tls_version": "",
        "tls_ciphers": "",
        "tls_extensions": "",
        "tls_supported_groups": "",
        "tls_point_formats": "",
        "tls_server_cipher": "",
        "tls_server_extensions": "",
        "ja3_string": "",
        "ja3_hash": "",
        "ja3s_string": "",
        "ja3s_hash": "",
        "tls_metadata_source": "",
        "http_host": http_host,
        "http_user_agent": http_user_agent,
        "dhcp_fingerprint": dhcp_metadata["dhcp_fingerprint"],
        "dhcp_vendor": dhcp_metadata["dhcp_vendor"],
        "dhcp_hostname": dhcp_metadata["dhcp_hostname"],
        "ssh_hassh": "",
        "p0f_os": "",
        "tcp_syn_signature": "",
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
        "_packet_qos_supported": "false",
        "_ack_rtt_ms": "",
        "_retransmission_flag": "",
        "_duplicate_ack_flag": "",
        "_lost_segment_flag": "",
        "_out_of_order_flag": "",
    }


def extract_packet_fields_from_tshark_row(
    row_values: dict[str, str],
    source_file: str,
    *,
    base_epoch: float | None = None,
    use_relative_time: bool = False,
) -> dict[str, Any]:
    raw_time = safe_float(row_values.get("frame.time_epoch"))
    packet_number = safe_int(row_values.get("frame.number")) or 0
    frame_len = safe_int(row_values.get("frame.len")) or 0
    packet_time = normalize_capture_time(raw_time, base_epoch) if use_relative_time else raw_time
    timestamp = "" if use_relative_time else iso_timestamp(packet_time)
    relative_time_s = safe_float(packet_time) if use_relative_time else None
    src_ip_v4 = first_tshark_value(row_values.get("ip.src", ""))
    dst_ip_v4 = first_tshark_value(row_values.get("ip.dst", ""))
    src_ip_v6 = first_tshark_value(row_values.get("ipv6.src", ""))
    dst_ip_v6 = first_tshark_value(row_values.get("ipv6.dst", ""))
    src_ip = src_ip_v4 or src_ip_v6
    dst_ip = dst_ip_v4 or dst_ip_v6
    tcp_src = first_tshark_value(row_values.get("tcp.srcport", ""))
    tcp_dst = first_tshark_value(row_values.get("tcp.dstport", ""))
    udp_src = first_tshark_value(row_values.get("udp.srcport", ""))
    udp_dst = first_tshark_value(row_values.get("udp.dstport", ""))
    src_port = tcp_src or udp_src
    dst_port = tcp_dst or udp_dst
    protocol = tshark_protocol(
        normalize_tshark_value(row_values.get("frame.protocols", "")),
        tcp_src,
        udp_src,
        first_tshark_value(row_values.get("icmp.type", "")),
        src_ip_v4,
        src_ip_v6,
    )
    ttl = safe_int(row_values.get("ip.ttl")) or safe_int(row_values.get("ipv6.hlim"))
    ip_version = "IPv4" if src_ip_v4 else "IPv6" if src_ip_v6 else ""
    tcp_flags = first_tshark_value(row_values.get("tcp.flags.str", ""))
    icmp_type = safe_int(first_tshark_value(row_values.get("icmp.type")))
    icmp_code = safe_int(first_tshark_value(row_values.get("icmp.code")))
    dns_query = first_tshark_value(row_values.get("dns.qry.name", "")).rstrip(".")
    tls_sni = first_tshark_value(row_values.get("tls.handshake.extensions_server_name", ""))
    tls_metadata = tls_metadata_from_tshark_row(row_values)
    http_host = first_tshark_value(row_values.get("http.host", ""))
    http_user_agent = first_tshark_value(row_values.get("http.user_agent", ""))
    dhcp_fingerprint = dhcp_fingerprint_from_tshark_value(row_values.get("dhcp.option.request_list_item", ""))
    dhcp_vendor = first_tshark_value(row_values.get("dhcp.option.vendor_class_id", ""))
    dhcp_hostname = first_tshark_value(row_values.get("dhcp.option.hostname", ""))
    payload_bytes = tshark_payload_bytes(
        protocol,
        frame_len,
        safe_int(row_values.get("ip.len")),
        safe_int(row_values.get("ipv6.plen")),
        safe_int(row_values.get("tcp.hdr_len")),
        safe_int(row_values.get("tcp.len")),
    )
    service = infer_service(dst_port, protocol, dns_query, tls_sni, http_host)
    app_protocol = infer_app_protocol(dst_port, protocol, dns_query, tls_sni, http_host)
    session_state = infer_session_state(tcp_flags, protocol)
    rule_name = infer_rule_name(app_protocol, dns_query, tls_sni, http_host)
    flow_id = make_flow_id(source_file, src_ip, dst_ip, src_port, dst_port, protocol)
    ack_rtt_s = safe_float(row_values.get("tcp.analysis.ack_rtt"))
    retransmission_flag = 1 if any(
        tshark_flag_present(row_values.get(field, ""))
        for field in (
            "tcp.analysis.retransmission",
            "tcp.analysis.fast_retransmission",
            "tcp.analysis.spurious_retransmission",
        )
    ) else 0
    duplicate_ack_flag = tshark_flag_present(row_values.get("tcp.analysis.duplicate_ack", ""))
    lost_segment_flag = tshark_flag_present(row_values.get("tcp.analysis.lost_segment", ""))
    out_of_order_flag = tshark_flag_present(row_values.get("tcp.analysis.out_of_order", ""))

    return {
        "timestamp": timestamp,
        "relative_time_s": relative_time_s if relative_time_s is not None else "",
        "time_is_relative": "true" if use_relative_time else "false",
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
        "mac_src": normalize_tshark_value(row_values.get("eth.src", "")),
        "mac_dst": normalize_tshark_value(row_values.get("eth.dst", "")),
        "flow_id": flow_id,
        "bytes": frame_len,
        "frame_len": frame_len,
        "packet_count": 1,
        "byte_count": frame_len,
        "bytes_total": frame_len,
        "src_bytes": frame_len,
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
        **tls_metadata,
        "http_host": http_host,
        "http_user_agent": http_user_agent,
        "dhcp_fingerprint": dhcp_fingerprint,
        "dhcp_vendor": dhcp_vendor,
        "dhcp_hostname": dhcp_hostname,
        "ssh_hassh": "",
        "p0f_os": "",
        "tcp_syn_signature": "",
        "device_id": "",
        "sensor_id": "",
        "vlan_id": normalize_tshark_value(row_values.get("vlan.id", "")),
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
        "_packet_qos_supported": "true" if protocol == "TCP" else "false",
        "_ack_rtt_ms": round(ack_rtt_s * 1000.0, 4) if ack_rtt_s is not None else "",
        "_retransmission_flag": retransmission_flag if protocol == "TCP" else "",
        "_duplicate_ack_flag": duplicate_ack_flag if protocol == "TCP" else "",
        "_lost_segment_flag": lost_segment_flag if protocol == "TCP" else "",
        "_out_of_order_flag": out_of_order_flag if protocol == "TCP" else "",
    }


def parse_packet_rows_with_tshark(pcap_file: str) -> list[dict[str, Any]]:
    tshark_bin = resolve_tshark_binary()
    if not tshark_bin:
        raise RuntimeError("tshark is not available in PATH.")

    cmd = [
        tshark_bin,
        "-r",
        pcap_file,
        "-T",
        "fields",
        "-E",
        "header=n",
        "-E",
        "separator=\t",
        "-E",
        "quote=n",
        "-E",
        "occurrence=a",
        "-E",
        "aggregator=,",
    ]
    for field in TSHARK_FIELDS:
        cmd.extend(["-e", field])

    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="ignore", check=False)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(f"tshark failed for {pcap_file}: {stderr or f'exit code {result.returncode}'}")

    rows: list[dict[str, Any]] = []
    display_source = to_repo_relative_display(pcap_file)
    base_epoch: float | None = None
    use_relative_time = False

    for line_index, line in enumerate(result.stdout.splitlines(), start=1):
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < len(TSHARK_FIELDS):
            parts.extend([""] * (len(TSHARK_FIELDS) - len(parts)))
        row_values = dict(zip(TSHARK_FIELDS, parts[: len(TSHARK_FIELDS)]))
        raw_time = safe_float(row_values.get("frame.time_epoch"))
        if line_index == 1 and raw_time is not None and raw_time < RELATIVE_TIME_EPOCH_THRESHOLD:
            base_epoch = raw_time
            use_relative_time = True
        rows.append(
            extract_packet_fields_from_tshark_row(
                row_values,
                display_source,
                base_epoch=base_epoch,
                use_relative_time=use_relative_time,
            )
        )
    return rows


def parse_packet_rows_with_scapy(pcap_file: str) -> list[dict[str, Any]]:
    ensure_scapy()
    rows: list[dict[str, Any]] = []
    packet_number = 0
    display_source = to_repo_relative_display(pcap_file)
    base_epoch: float | None = None
    use_relative_time = False
    try:
        with PcapReader(pcap_file) as reader:
            for packet in reader:
                packet_number += 1
                raw_time = getattr(packet, "time", None)
                if packet_number == 1:
                    try:
                        first_time = float(raw_time)
                        if first_time < RELATIVE_TIME_EPOCH_THRESHOLD:
                            base_epoch = first_time
                            use_relative_time = True
                    except (TypeError, ValueError):
                        base_epoch = None
                packet_time = normalize_capture_time(raw_time, base_epoch) if use_relative_time else raw_time
                rows.append(
                    extract_packet_fields(
                        packet,
                        display_source,
                        packet_number,
                        packet_time=packet_time,
                        use_relative_time=use_relative_time,
                    )
                )
    except Exception:
        # Fallback for edge-case captures where RawPcapReader is more tolerant.
        rows.clear()
        packet_number = 0
        base_epoch = None
        use_relative_time = False
        with RawPcapReader(pcap_file) as reader:
            for raw_packet, metadata in reader:
                packet_number += 1
                try:
                    packet = Ether(raw_packet)
                except Exception:
                    try:
                        packet = IP(raw_packet)
                    except Exception:
                        try:
                            packet = IPv6(raw_packet)
                        except Exception:
                            continue
                raw_time = getattr(metadata, "sec", 0) + getattr(metadata, "usec", 0) / 1_000_000
                packet.time = raw_time
                if packet_number == 1:
                    if raw_time < RELATIVE_TIME_EPOCH_THRESHOLD:
                        base_epoch = float(raw_time)
                        use_relative_time = True
                packet_time = normalize_capture_time(raw_time, base_epoch) if use_relative_time else raw_time
                rows.append(
                    extract_packet_fields(
                        packet,
                        display_source,
                        packet_number,
                        packet_time=packet_time,
                        use_relative_time=use_relative_time,
                    )
                )
    return rows


def parse_packet_rows(pcap_file: str) -> tuple[list[dict[str, Any]], str]:
    tshark_error: Exception | None = None
    if resolve_tshark_binary():
        try:
            print(f"[prepare_pcap] Using tshark for {to_repo_relative_display(pcap_file)}", file=sys.stderr)
            return parse_packet_rows_with_tshark(pcap_file), "tshark"
        except Exception as exc:
            tshark_error = exc
            print(
                f"[prepare_pcap] tshark failed for {to_repo_relative_display(pcap_file)}: {exc}. Falling back to scapy.",
                file=sys.stderr,
            )
    try:
        print(f"[prepare_pcap] Using scapy for {to_repo_relative_display(pcap_file)}", file=sys.stderr)
        return parse_packet_rows_with_scapy(pcap_file), "scapy"
    except Exception:
        if tshark_error is not None:
            raise RuntimeError(f"Both tshark and scapy preprocessing failed. tshark error: {tshark_error}")
        raise


def run_zeek_logs(pcap_file: str, dataset_output_dir: Path) -> dict[str, Any]:
    zeek_bin = resolve_zeek_binary()
    if not zeek_bin:
        raise RuntimeError("zeek is not available in PATH.")

    pcap_path = Path(pcap_file)
    zeek_root = dataset_output_dir / "zeek"
    zeek_dir = zeek_root / sanitize_name(pcap_path.stem)
    zeek_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        zeek_bin,
        "-C",
        "-r",
        str(pcap_path),
        "LogAscii::use_json=T",
    ]
    result = subprocess.run(
        cmd,
        cwd=str(zeek_dir),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
        check=False,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(f"zeek failed for {pcap_file}: {stderr or f'exit code {result.returncode}'}")

    discovered_logs = sorted(path for path in zeek_dir.iterdir() if path.is_file() and path.suffix in {".log", ".json"})
    return {
        "pcap_name": pcap_path.name,
        "pcap_source": to_repo_relative_display(pcap_path),
        "zeek_dir": to_repo_relative_display(zeek_dir),
        "logs": [to_repo_relative_display(path) for path in discovered_logs],
    }


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_flow_rows(packet_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sorted_rows = sorted(packet_rows, key=lambda row: (row.get("timestamp", ""), row.get("packet_number", 0)))
    grouped_sessions: dict[tuple[str, str, tuple[str, str], tuple[str, str]], list[dict[str, Any]]] = defaultdict(list)
    session_counters: dict[tuple[str, str, tuple[str, str], tuple[str, str]], int] = defaultdict(int)

    for row in sorted_rows:
        key = make_conversation_key(row)
        packet_dt = parse_iso_datetime(row.get("timestamp"))
        packet_relative_time = coerce_float(row.get("relative_time_s"))
        current = find_matching_session(grouped_sessions[key], row, packet_dt, packet_relative_time)
        if current is None:
            session_counters[key] += 1
            current = initialize_flow_session(row, key, session_counters[key])
            grouped_sessions[key].append(current)
            continue
        update_flow_session(current, row)

    flow_rows: list[dict[str, Any]] = []
    for sessions in grouped_sessions.values():
        for flow in sessions:
            flow_rows.append(finalize_flow_session(flow))
    flow_rows.sort(key=lambda item: (item["timestamp"], item["src_ip"], item["dst_ip"], item["src_port"], item["dst_port"]))
    return flow_rows


def copy_packet_qos_metrics(target: dict[str, Any], source: dict[str, Any]) -> None:
    for field in ("rtt_ms", "jitter_ms", "packet_loss_pct", "retransmission_count", "retransmission_rate"):
        value = source.get(field)
        if value not in (None, ""):
            target[field] = value


def copy_packet_tls_metadata(target: dict[str, Any], source: dict[str, Any]) -> None:
    for field in (
        "tls_handshake_type",
        "tls_version",
        "tls_ciphers",
        "tls_extensions",
        "tls_supported_groups",
        "tls_point_formats",
        "tls_server_cipher",
        "tls_server_extensions",
        "ja3_string",
        "ja3_hash",
        "ja3s_string",
        "ja3s_hash",
        "tls_metadata_source",
    ):
        value = source.get(field)
        if value not in (None, "") and target.get(field) in (None, ""):
            target[field] = value


def copy_packet_fingerprint_fields(target: dict[str, Any], source: dict[str, Any]) -> None:
    for field in (
        "http_user_agent",
        "dhcp_fingerprint",
        "dhcp_vendor",
        "dhcp_hostname",
        "ssh_hassh",
        "p0f_os",
        "tcp_syn_signature",
        "mac_src",
        "mac_dst",
    ):
        value = source.get(field)
        if value not in (None, "") and target.get(field) in (None, ""):
            target[field] = value


def enrich_flows_with_packet_qos(flow_rows: list[dict[str, Any]], packet_flow_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not flow_rows or not packet_flow_rows:
        return flow_rows

    indexed_candidates: dict[tuple[str, str, tuple[str, str], tuple[str, str]], list[dict[str, Any]]] = defaultdict(list)
    for row in packet_flow_rows:
        indexed_candidates[normalized_conversation_key(row)].append(dict(row))

    enriched: list[dict[str, Any]] = []
    for row in flow_rows:
        enriched_row = dict(row)
        key = normalized_conversation_key(enriched_row)
        candidates = indexed_candidates.get(key, [])
        if candidates:
            best_index = max(range(len(candidates)), key=lambda idx: flow_match_score(enriched_row, candidates[idx]))
            matched = candidates.pop(best_index)
            copy_packet_qos_metrics(enriched_row, matched)
            copy_packet_tls_metadata(enriched_row, matched)
            copy_packet_fingerprint_fields(enriched_row, matched)
        enriched.append(enriched_row)
    return enriched


def summarize_qos_population(flow_rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_rows = len(flow_rows)
    if total_rows == 0:
        return {
            "mode": "empty",
            "flow_rows": 0,
            "rtt_populated": 0,
            "jitter_populated": 0,
            "loss_populated": 0,
            "retransmission_populated": 0,
        }

    rtt_populated = sum(1 for row in flow_rows if row.get("rtt_ms") not in (None, ""))
    jitter_populated = sum(1 for row in flow_rows if row.get("jitter_ms") not in (None, ""))
    loss_populated = sum(1 for row in flow_rows if row.get("packet_loss_pct") not in (None, ""))
    retrans_populated = sum(1 for row in flow_rows if row.get("retransmission_count") not in (None, "") or row.get("retransmission_rate") not in (None, ""))

    direct_signals = sum(1 for count in (rtt_populated, jitter_populated, loss_populated, retrans_populated) if count > 0)
    if direct_signals >= 3:
        mode = "direct_packet_enriched"
    elif direct_signals >= 1:
        mode = "mixed_direct_and_proxy"
    else:
        mode = "proxy_only"

    return {
        "mode": mode,
        "flow_rows": total_rows,
        "rtt_populated": rtt_populated,
        "jitter_populated": jitter_populated,
        "loss_populated": loss_populated,
        "retransmission_populated": retrans_populated,
    }


def summarize_tls_population(flow_rows: list[dict[str, Any]]) -> dict[str, Any]:
    source_counts: dict[str, int] = defaultdict(int)
    for row in flow_rows:
        source_counts[str(row.get("tls_metadata_source") or "missing")] += 1
    return {
        "flow_rows": len(flow_rows),
        "tls_metadata_rows": sum(
            1
            for row in flow_rows
            if row.get("tls_version") or row.get("tls_ciphers") or row.get("tls_server_cipher") or row.get("tls_sni")
        ),
        "ja3_populated": sum(1 for row in flow_rows if row.get("ja3_hash")),
        "ja3s_populated": sum(1 for row in flow_rows if row.get("ja3s_hash")),
        "source_counts": dict(sorted(source_counts.items())),
    }


def zeek_conn_state_to_session_state(conn_state: str) -> str:
    """Map Zeek conn_state to session_state."""
    mapping = {
        "SF": "ESTABLISHED",  # Normal SYN/FIN completion
        "S0": "SYN_ONLY",      # SYN seen, no reply
        "S1": "SYN_ACK",       # SYN-ACK seen, no ACK
        "S2": "SYN_ACK",       # SYN-ACK seen, no ACK
        "S3": "ESTABLISHED",   # SYN-ACK and ACK seen
        "RSTO": "RST",         # RST from originator
        "RSTR": "RST",         # RST from responder
        "RSTOS0": "RST",       # RST from originator early
        "RSTRH": "RST",        # RST from responder after handshake
        "SH": "SYN_ACK",       # SYN + SYN-ACK, no ACK
        "SHR": "SYN_ACK",      # SYN + SYN-ACK from responder
        "REJ": "RST",          # Connection rejected
        "OTH": "ACK",          # No SYN seen
    }
    return mapping.get(conn_state or "", "")


def zeek_history_to_tcp_flags(history: str) -> str:
    """Decode Zeek history field to TCP flags string."""
    if not history:
        return ""
    flag_map = {
        "S": "SYN",
        "s": "SYN",
        "h": "SYN_ACK",
        "H": "SYN_ACK",
        "A": "ACK",
        "a": "ACK",
        "d": "FIN",
        "f": "FIN",
        "r": "RST",
        "R": "RST",
        "D": "FIN",
        "F": "FIN",
    }
    flags = set()
    for char in history:
        if char in flag_map:
            flags.add(flag_map[char])
    return ",".join(sorted(flags)) if flags else ""


def build_flows_from_zeek(zeek_dir: Path, pcap_file: str, dataset_name: str) -> list[dict[str, Any]]:
    """
    Build flow.csv from Zeek conn.log, enriched with DNS and SSL data.

    This is the industrial-strength alternative to Python session aggregation.
    Zeek's conn.log has correct timeout handling, protocol detection, and
    bidirectional byte counting.

    Args:
        zeek_dir: Directory containing Zeek logs (conn.log, dns.log, ssl.log)
        pcap_file: Original PCAP file path
        dataset_name: Dataset name for labeling

    Returns:
        List of flow dictionaries in standard CANONICAL_COLUMNS format
    """
    conn_log = zeek_dir / "conn.log"
    dns_log = zeek_dir / "dns.log"
    ssl_log = zeek_dir / "ssl.log"

    # Load DNS queries (uid → dns_query mapping)
    dns_map: dict[str, str] = {}
    if dns_log.exists():
        try:
            with open(dns_log) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    try:
                        record = json.loads(line)
                        uid = record.get("uid", "")
                        query = record.get("query", "")
                        if uid and query:
                            dns_map[uid] = query.rstrip(".")
                    except json.JSONDecodeError:
                        continue
        except Exception:
            pass

    # Load TLS SNI (uid → sni mapping)
    sni_map: dict[str, str] = {}
    ssl_metadata_map: dict[str, dict[str, str]] = {}
    if ssl_log.exists():
        try:
            with open(ssl_log) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    try:
                        record = json.loads(line)
                        uid = record.get("uid", "")
                        server_name = record.get("server_name", "")
                        if uid and server_name:
                            sni_map[uid] = server_name
                        if uid:
                            ssl_metadata_map[uid] = {
                                "tls_version": str(record.get("version", "") or ""),
                                "tls_server_cipher": str(record.get("cipher", "") or ""),
                                "ja3_hash": str(record.get("ja3", "") or ""),
                                "ja3s_hash": str(record.get("ja3s", "") or ""),
                                "tls_metadata_source": "zeek_ja3"
                                if record.get("ja3") or record.get("ja3s")
                                else "zeek_ssl_partial",
                            }
                    except json.JSONDecodeError:
                        continue
        except Exception:
            pass

    # Process conn.log
    flows: list[dict[str, Any]] = []
    pcap_name = Path(pcap_file).name

    with open(conn_log) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Extract Zeek fields
            uid = record.get("uid", "")
            ts = record.get("ts", 0)
            id_orig_h = record.get("id.orig_h", "")
            id_orig_p = record.get("id.orig_p", 0)
            id_resp_h = record.get("id.resp_h", "")
            id_resp_p = record.get("id.resp_p", 0)
            proto = record.get("proto", "").upper()
            service = record.get("service", "")
            duration = record.get("duration", 0.0)
            orig_bytes = record.get("orig_bytes", 0)
            resp_bytes = record.get("resp_bytes", 0)
            orig_pkts = record.get("orig_pkts", 0)
            resp_pkts = record.get("resp_pkts", 0)
            conn_state = record.get("conn_state", "")
            history = record.get("history", "")
            local_orig = record.get("local_orig", None)
            local_resp = record.get("local_resp", None)
            missed_bytes = record.get("missed_bytes", 0)
            orig_ip_bytes = record.get("orig_ip_bytes", 0)
            resp_ip_bytes = record.get("resp_ip_bytes", 0)
            ip_proto = record.get("ip_proto", 0)

            # Map to CANONICAL_COLUMNS
            # Handle relative vs absolute timestamps
            is_relative = ts < RELATIVE_TIME_EPOCH_THRESHOLD
            if is_relative:
                timestamp = ""
                start_rel = ts
                end_rel = ts + duration if duration else ts
                time_is_relative = "true"
            else:
                timestamp = iso_timestamp(ts)
                start_rel = ""
                end_rel = ""
                time_is_relative = "false"

            total_bytes = orig_bytes + resp_bytes
            total_packets = orig_pkts + resp_pkts
            session_state = zeek_conn_state_to_session_state(conn_state)
            tcp_flags = zeek_history_to_tcp_flags(history)

            # Determine direction
            if local_orig and not local_resp:
                direction = "outbound"
            elif local_resp and not local_orig:
                direction = "inbound"
            else:
                direction = "internal"

            # Determine action
            if conn_state in ("RSTO", "RSTR", "RSTOS0", "RSTRH", "REJ"):
                action = "rejected"
            elif conn_state == "S0":
                action = "attempted"
            else:
                action = "allowed"

            # Determine app_protocol
            dns_query = dns_map.get(uid, "")
            tls_sni = sni_map.get(uid, "")
            ssl_metadata = ssl_metadata_map.get(uid, {})

            if dns_query:
                app_protocol = "DNS"
            elif tls_sni:
                app_protocol = "TLS"
            elif service:
                app_protocol = service.upper()
            elif proto == "TCP" and id_resp_p == "443":
                app_protocol = "TLS"
            else:
                app_protocol = proto

            flow = {
                # Time fields
                "timestamp": timestamp,
                "end_time": iso_timestamp(ts + duration) if duration and not is_relative else "",
                "start_relative_time_s": start_rel if is_relative else "",
                "end_relative_time_s": end_rel if is_relative else "",
                "time_is_relative": time_is_relative,
                # 5-tuple
                "src_ip": id_orig_h,
                "dst_ip": id_resp_h,
                "src_port": str(id_orig_p),
                "dst_port": str(id_resp_p),
                "protocol": proto,
                "ip_proto": ip_proto,
                # Volume
                "bytes": total_bytes,
                "bytes_total": total_bytes,
                "packets": total_packets,
                "packet_count": total_packets,
                "src_bytes": orig_bytes,
                "dst_bytes": resp_bytes,
                "src_packets": orig_pkts,
                "dst_packets": resp_pkts,
                "payload_bytes": 0,  # Zeek doesn't directly provide this
                # Duration
                "flow_duration": duration if duration else 0.0,
                "duration": duration if duration else 0.0,
                "duration_ms": int(duration * 1000) if duration else 0,
                # Protocol/App
                "app_protocol": app_protocol,
                "service": service if service else "",
                "session_state": session_state,
                "tcp_flags": tcp_flags,
                "tcp_flags_seen": tcp_flags,
                "dns_query": dns_query,
                "tls_sni": tls_sni,
                "tls_handshake_type": "",
                "tls_version": ssl_metadata.get("tls_version", ""),
                "tls_ciphers": "",
                "tls_extensions": "",
                "tls_supported_groups": "",
                "tls_point_formats": "",
                "tls_server_cipher": ssl_metadata.get("tls_server_cipher", ""),
                "tls_server_extensions": "",
                "ja3_string": "",
                "ja3_hash": ssl_metadata.get("ja3_hash", ""),
                "ja3s_string": "",
                "ja3s_hash": ssl_metadata.get("ja3s_hash", ""),
                "tls_metadata_source": ssl_metadata.get("tls_metadata_source", ""),
                "http_host": "",
                "http_user_agent": "",
                "dhcp_fingerprint": "",
                "dhcp_vendor": "",
                "dhcp_hostname": "",
                "ssh_hassh": "",
                "p0f_os": "",
                "tcp_syn_signature": "",
                # Connection state
                "direction": direction,
                "action": action,
                "flow_start_reason": "syn" if proto == "TCP" and "S" in (history or "") else "first_packet",
                "flow_end_reason": conn_state if conn_state else "end_of_capture",
                "conn_state": conn_state,
                "history": history,
                # Zeek-specific
                "uid": uid,
                "local_orig": str(local_orig) if local_orig is not None else "",
                "local_resp": str(local_resp) if local_resp is not None else "",
                "missed_bytes": missed_bytes,
                "orig_ip_bytes": orig_ip_bytes,
                "resp_ip_bytes": resp_ip_bytes,
                "orig_pkts": orig_pkts,
                "resp_pkts": resp_pkts,
                # Empty/placeholder fields (filled by other tools if available)
                "rule_name": "",
                "ip_version": "IPv6" if ":" in id_orig_h else "IPv4",
                "frame_len": 0,
                "ttl": "",
                "ttl_min": "",
                "ttl_max": "",
                "ttl_avg": "",
                "ttl_range": "",
                "icmp_type": "",
                "icmp_code": "",
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
                "device_id": "",
                "sensor_id": "",
                "pcap_name": pcap_name,
                "mac_src": "",
                "mac_dst": "",
                "packet_count": total_packets,
                "byte_count": total_bytes,
                "src_to_dst_byte_ratio": round(orig_bytes / max(resp_bytes, 1), 4),
                "src_to_dst_packet_ratio": round(orig_pkts / max(resp_pkts, 1), 4),
                "byte_asymmetry": round(abs(orig_bytes - resp_bytes) / max(total_bytes, 1), 4),
                "packet_asymmetry": round(abs(orig_pkts - resp_pkts) / max(total_packets, 1), 4),
                "ttl_min": "",
                "ttl_max": "",
                "ttl_avg": "",
                "ttl_range": "",
                "dns_query_length": len(dns_query) if dns_query else 0,
                "dns_label_count": dns_query.count(".") + 1 if dns_query else 0,
                "dns_query_entropy": lexical_entropy(dns_query) if dns_query else 0.0,
                "dataset_label": dataset_name,
                "traffic_family": infer_traffic_family(dataset_name, pcap_file, app_protocol, service),
                "source_file": pcap_file,
            }
            flows.append(flow)

    # Sort by timestamp
    flows.sort(key=lambda x: x.get("timestamp", "") or x.get("start_relative_time_s", ""))

    return flows


def default_dataset_name(pcap_files: list[str]) -> str:
    if len(pcap_files) == 1:
        return sanitize_name(Path(pcap_files[0]).stem)
    digest = hashlib.sha1("|".join(sorted(pcap_files)).encode("utf-8")).hexdigest()[:8]
    return f"batch-{digest}"


def input_source_type(paths: list[str]) -> str:
    normalized = [Path(item).as_posix() for item in paths]
    if normalized and all(path.startswith("/mnt/user-data/uploads/") for path in normalized):
        return "uploads"
    if normalized and all("/datasets/network-traffic/" in path.replace("\\", "/") for path in normalized):
        return "local-dataset"
    return "mixed"


def default_output_dir_for_inputs(dataset_name: str, pcap_files: list[str]) -> Path:
    from utils.path import is_relative_to_path, uploads_root, processed_dataset_root
    source_type = input_source_type(pcap_files)
    first = Path(pcap_files[0]).resolve()
    # uploads always go to workspace — never pollute datasets with user uploads
    if is_relative_to_path(first, uploads_root()):
        return network_traffic_workspace_root() / "processed" / dataset_name
    # dataset raw uses processed_dataset_root() (env override only applies here)
    if is_relative_to_path(first, dataset_root() / "raw"):
        if os.environ.get("NETWORK_TRAFFIC_PROCESSED_ROOT"):
            return Path(os.environ["NETWORK_TRAFFIC_PROCESSED_ROOT"]) / dataset_name
        return processed_dataset_root() / dataset_name
    # other paths default to workspace
    return network_traffic_workspace_root() / "processed" / dataset_name


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preprocess PCAP files into standardized packet.csv and flow.csv files.")
    parser.add_argument("--files", nargs="+", required=True, help="PCAP files, directories, or shorthand references")
    parser.add_argument("--dataset-name", default=None, help="Output dataset directory name under datasets/network-traffic/processed")
    parser.add_argument("--output-dir", default=None, help="Explicit output directory. Defaults to datasets/network-traffic/processed/<dataset-name>")
    parser.add_argument("--force", action="store_true", help="Rebuild processed files even if they already exist")
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
        source_type = input_source_type(pcap_files)
        output_dir = Path(args.output_dir) if args.output_dir else default_output_dir_for_inputs(dataset_name, pcap_files)

        # Reuse existing processed results if available (unless --force)
        flow_path = output_dir / f"{dataset_name}.flow.csv"
        metadata_path = output_dir / "metadata.json"
        if not args.force and flow_path.exists() and metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata["reused"] = True
            if args.format == "json":
                print(json.dumps(metadata, ensure_ascii=False))
            else:
                print(
                    "\n".join(
                        [
                            f"Reusing existing processed dataset: {dataset_name}",
                            f"flow.csv: {to_repo_relative_display(flow_path)}",
                            f"metadata: {to_repo_relative_display(metadata_path)}",
                        ]
                    )
                )
            return 0

        output_dir.mkdir(parents=True, exist_ok=True)

        packet_rows: list[dict[str, Any]] = []
        packet_counts: dict[str, int] = defaultdict(int)
        packet_engines_used: list[str] = []
        zeek_artifacts: list[dict[str, Any]] = []
        for pcap_file in pcap_files:
            rows, engine = parse_packet_rows(pcap_file)
            packet_rows.extend(rows)
            packet_counts[pcap_file] = len(rows)
            packet_engines_used.append(engine)
            if resolve_zeek_binary():
                try:
                    print(f"[prepare_pcap] Using zeek for {to_repo_relative_display(pcap_file)}", file=sys.stderr)
                    zeek_artifacts.append(run_zeek_logs(pcap_file, output_dir))
                except Exception as exc:
                    print(
                        f"[prepare_pcap] zeek failed for {to_repo_relative_display(pcap_file)}: {exc}. Continuing without zeek logs.",
                        file=sys.stderr,
                    )

        for row in packet_rows:
            row["dataset_label"] = dataset_name
            row["traffic_family"] = infer_traffic_family(dataset_name, row["source_file"], row.get("app_protocol", ""), row.get("service", ""))

        packet_rows.sort(key=lambda item: (item["source_file"], item["packet_number"]))

        packet_derived_flow_rows = build_flow_rows(packet_rows)

        # Use Zeek flows if available, otherwise fallback to Python session aggregation
        if zeek_artifacts:
            zeek_dir_path = zeek_artifacts[0]["zeek_dir"]
            zeek_dir = Path(zeek_dir_path)
            if not zeek_dir.is_absolute():
                zeek_dir = repo_root() / zeek_dir_path
            if (zeek_dir / "conn.log").exists():
                print(f"[prepare_pcap] Using Zeek conn.log for flow generation (industrial-strength)", file=sys.stderr)
                flow_rows = build_flows_from_zeek(zeek_dir, pcap_files[0], dataset_name)
                flow_rows = enrich_flows_with_packet_qos(flow_rows, packet_derived_flow_rows)
            else:
                flow_rows = packet_derived_flow_rows
        else:
            flow_rows = packet_derived_flow_rows
        for row in flow_rows:
            row["dataset_label"] = dataset_name
            row["traffic_family"] = infer_traffic_family(dataset_name, row["source_file"], row.get("app_protocol", ""), row.get("service", ""))

        qos_population = summarize_qos_population(flow_rows)
        tls_population = summarize_tls_population(flow_rows)

        packet_path = output_dir / f"{dataset_name}.packet.csv"
        flow_path = output_dir / f"{dataset_name}.flow.csv"
        metadata_path = output_dir / "metadata.json"

        write_csv(
            packet_path,
            packet_rows,
            [
                "timestamp",
                "relative_time_s",
                "time_is_relative",
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
                "tls_handshake_type",
                "tls_version",
                "tls_ciphers",
                "tls_extensions",
                "tls_supported_groups",
                "tls_point_formats",
                "tls_server_cipher",
                "tls_server_extensions",
                "ja3_string",
                "ja3_hash",
                "ja3s_string",
                "ja3s_hash",
                "tls_metadata_source",
                "http_host",
                "http_user_agent",
                "dhcp_fingerprint",
                "dhcp_vendor",
                "dhcp_hostname",
                "ssh_hassh",
                "p0f_os",
                "tcp_syn_signature",
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
                "src_to_dst_byte_ratio",
                "src_to_dst_packet_ratio",
                "byte_asymmetry",
                "packet_asymmetry",
                "duration_ms",
                "direction",
                "action",
                "session_state",
                "flow_start_reason",
                "flow_end_reason",
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
                "start_relative_time_s",
                "end_relative_time_s",
                "time_is_relative",
                "src_ip",
                "dst_ip",
                "src_port",
                "dst_port",
                "protocol",
                "app_protocol",
                "service",
                "direction",
                "src_role",
                "dst_role",
                "action",
                "session_state",
                "flow_start_reason",
                "flow_end_reason",
                "rule_name",
                "tcp_flags",
                "ip_version",
                "frame_len",
                "ttl",
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
                "tls_handshake_type",
                "tls_version",
                "tls_ciphers",
                "tls_extensions",
                "tls_supported_groups",
                "tls_point_formats",
                "tls_server_cipher",
                "tls_server_extensions",
                "ja3_string",
                "ja3_hash",
                "ja3s_string",
                "ja3s_hash",
                "tls_metadata_source",
                "http_host",
                "http_user_agent",
                "dhcp_fingerprint",
                "dhcp_vendor",
                "dhcp_hostname",
                "ssh_hassh",
                "p0f_os",
                "tcp_syn_signature",
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
                "ttl_range",
                "rtt_ms",
                "jitter_ms",
                "packet_loss_pct",
                "retransmission_count",
                "retransmission_rate",
                "icmp_type",
                "icmp_code",
                "device_id",
                "sensor_id",
                "dns_query_length",
                "dns_label_count",
                "dns_query_entropy",
                "dataset_label",
                "traffic_family",
                "source_file",
                "src_to_dst_byte_ratio",
                "src_to_dst_packet_ratio",
                "byte_asymmetry",
                "packet_asymmetry",
                "packet_count",
                "byte_count",
                "pcap_name",
                "mac_src",
                "mac_dst",
                # Zeek-specific fields
                "uid",
                "conn_state",
                "history",
                "local_orig",
                "local_resp",
                "missed_bytes",
                "orig_ip_bytes",
                "resp_ip_bytes",
                "orig_pkts",
                "resp_pkts",
                "ip_proto",
            ],
        )

        metadata = {
            "dataset_name": dataset_name,
            "reused": False,
            "engine": (
                "tshark"
                if packet_engines_used and all(item == "tshark" for item in packet_engines_used)
                else "scapy"
                if packet_engines_used and all(item == "scapy" for item in packet_engines_used)
                else "hybrid"
            ),
            "engines_used": packet_engines_used + (["zeek"] if zeek_artifacts else []),
            "input_source_type": source_type,
            "source_files": [to_repo_relative_display(item) for item in pcap_files],
            "packet_rows": len(packet_rows),
            "flow_rows": len(flow_rows),
            "per_file_packet_rows": {to_repo_relative_display(key): value for key, value in packet_counts.items()},
            "packet_csv": to_repo_relative_display(packet_path),
            "flow_csv": to_repo_relative_display(flow_path),
            "zeek_enabled": bool(zeek_artifacts),
            "zeek_dir": to_repo_relative_display(output_dir / "zeek") if zeek_artifacts else "",
            "zeek_artifacts": zeek_artifacts,
            "qos_measurement": qos_population,
            "tls_metadata": tls_population,
            "metadata": to_repo_relative_display(metadata_path),
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
                        f"packet.csv: {to_repo_relative_display(packet_path)}",
                        f"flow.csv: {to_repo_relative_display(flow_path)}",
                        f"metadata: {to_repo_relative_display(metadata_path)}",
                    ]
                )
            )
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
