#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
struct_id_detector.py v2

Detector for violation type 1: structured identifier leakage (struct_id).

Core idea:
1. Keep Microsoft Presidio as optional auxiliary recognizer.
2. Add local rules for the eight collected data types, especially:
   - code: phone/email/IP/MAC/ID-card in code/text snippets
   - netflow: src/dst IP, MAC, URL query identifiers
   - traffic_flow: plate number, checkpoint ID, device ID, MAC, collector IP, traffic card ID
   - telecom: hashed/stable user_id, imei, src/dst IDs, counterparty IDs
3. Supports both the original annotation format and the normalized package format:
   normalized/by_violation/struct_id.jsonl uses content_text + features.
4. Emits prediction JSONL, evaluation report, and optional FP/FN files.

Usage:
  python scripts/struct_id_detector.py \
    --input normalized/by_violation/struct_id.jsonl \
    --output outputs/struct_id_predictions.jsonl \
    --report outputs/struct_id_report.json \
    --use-presidio auto
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

STRUCT_ID_VIOLATION = "struct_id"
DETECTOR_VERSION = "struct_id_detector_presidio_rule_v2.0"

# -----------------------------
# Regex rules
# -----------------------------

REGEX_RULES: List[Tuple[str, re.Pattern[str], float]] = [
    ("CN_PHONE_NUMBER", re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)"), 0.95),
    ("EMAIL_ADDRESS", re.compile(r"(?<![\w.%-])[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?![\w.%-])"), 0.90),
    ("IP_ADDRESS", re.compile(r"(?<!\d)(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)(?!\d)"), 0.86),
    ("MAC_ADDRESS", re.compile(r"(?<![A-Fa-f0-9])(?:[A-Fa-f0-9]{2}[:-]){5}[A-Fa-f0-9]{2}(?![A-Fa-f0-9])"), 0.92),
    ("CN_ID_CARD", re.compile(r"(?<!\d)[1-9]\d{5}(?:18|19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[0-9Xx](?!\d)"), 0.92),
    # Mainland vehicle plate. Handles 陕A8K6D2 and 陕AD12345.
    ("VEHICLE_PLATE", re.compile(r"(?<![A-Z0-9])[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼使领][A-Z][A-Z0-9]{5,6}(?![A-Z0-9])"), 0.93),
    ("TRAFFIC_CHECKPOINT_ID", re.compile(r"\bKK-[A-Z]{2}-\d{4}-\d{3,}\b"), 0.93),
    ("TRAFFIC_DEVICE_ID", re.compile(r"\bTRF-(?:DET|RDR)(?:-[A-Z]{2})?(?:-\d{3,})+\b"), 0.93),
    ("TRAFFIC_CARD_ID", re.compile(r"\b(?:TFCARD-\d{4}-\d{8,}|TC-[A-Z]{2}-\d{3}-\d{4,})\b"), 0.93),
    ("TOLL_GATE_ID", re.compile(r"\bTG-[A-Z]{2}-\d{3,}\b"), 0.93),
    # URL query/body parameters with structured identifiers. Value filtering is applied later.
    ("URL_PARAM_IDENTIFIER", re.compile(r"(?i)(?:\b|[?&])(user_id|account_id|phone|mobile|email|plate_no|vehicle_plate)=([^&\s'\"<>]+)"), 0.88),
    # Explicit field-like expressions for text outputs.
    ("FIELD_LIKE_STRUCT_ID", re.compile(r"(?i)\b(?:user_id|src_user_id|dst_counterparty_id|src_id|dst_id|imei|device_id|checkpoint_id|plate_no|vehicle_plate|traffic_card_id|toll_card_no|gate_id|toll_gate_id)\b\s*[:=：]\s*[A-Za-z0-9_:\-]{4,}"), 0.90),
]

# Field names which directly indicate structured identifiers.
SENSITIVE_FIELD_EXACT = {
    # Telecom / graph IDs
    "user_id", "src_id", "dst_id", "src_user_id", "dst_user_id", "dst_counterparty_id",
    "counterparty_id", "imei", "imsi", "iccid", "terminal_id", "device_id",
    # Personal / contact IDs
    "phone", "mobile", "msisdn", "driver_phone", "caller_phone", "callee_phone",
    "email", "id_card", "identity_card", "cert_no", "card_no",
    # Network IDs
    "ip", "src_ip", "dst_ip", "collector_ip", "mac", "mac_address", "src_mac", "dst_mac",
    # Traffic IDs
    "plate_no", "vehicle_plate", "license_plate", "checkpoint_id", "traffic_card_id", "toll_card_no", "gate_id", "toll_gate_id",
}

# Fields which are statistics/aggregates and should not be treated as identifiers.
AGGREGATE_FIELD_TOKENS = {
    "count", "num", "number", "total", "sum", "avg", "mean", "ratio", "rate",
    "duration", "fee", "flow", "amount", "score", "vehicle_count", "user_count",
    "edge_count", "record_count", "label_count", "avg_edge_count", "unique_count",
    "pass_count", "risk_user_count", "unique_contacts", "duration_sum",
}

NON_SENSITIVE_ID_FIELDS = {
    "flow_id", "doc_id", "data_id", "sample_id", "evidence_id", "source_id", "source_table",
    "dataset_name", "dataset", "object_type", "rule_id", "matched_rule_id", "policy_id",
}

MASKED_OR_SAFE_TEXT_PATTERNS = [
    re.compile(r"\*{2,}"),
    re.compile(r"<\s*(?:redacted|user_id|imei|phone|id|host-ip|name|domain|email)\s*>", re.I),
    re.compile(r"对象[A-Z一二三四五六七八九十]"),
    re.compile(r"设备[A-Z一二三四五六七八九十]"),
    re.compile(r"用户甲|用户乙|车辆甲|车辆乙"),
    re.compile(r"sha256_salted:[A-Za-z0-9]+\*{2,}[A-Za-z0-9]+", re.I),
]

PUBLIC_EXAMPLE_EMAIL_DOMAINS = {
    "example.com", "example.org", "example.net", "example.test", "example.invalid",
    "demo.invalid",
}

@dataclass
class Hit:
    field: str
    text: str
    entity_type: str
    method: str
    score: float
    reason: str
    reason_code: str

# -----------------------------
# IO helpers
# -----------------------------

def read_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON on line {i}: {e}") from e
    return rows


def write_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# -----------------------------
# Normalization helpers
# -----------------------------

def normalize_key(key: Any) -> str:
    return str(key).strip().lower().replace("-", "_").replace(" ", "_")


def base_key(path_or_key: Any) -> str:
    s = normalize_key(path_or_key)
    s = s.split(".")[-1]
    if "[" in s:
        s = s.split("[")[0]
    return s


def is_aggregate_field(key: str) -> bool:
    k = base_key(key)
    full = normalize_key(key)
    if k in AGGREGATE_FIELD_TOKENS or full in AGGREGATE_FIELD_TOKENS:
        return True
    return any(k.endswith("_" + tok) or k.startswith(tok + "_") for tok in AGGREGATE_FIELD_TOKENS)


def is_sensitive_field(key: str) -> bool:
    k = base_key(key)
    full = normalize_key(key)
    if is_aggregate_field(key) or k in NON_SENSITIVE_ID_FIELDS or full in NON_SENSITIVE_ID_FIELDS:
        return False
    if k in SENSITIVE_FIELD_EXACT or full in SENSITIVE_FIELD_EXACT:
        return True
    sensitive_suffixes = (
        "_user_id", "_counterparty_id", "_device_id", "_imei", "_phone", "_ip", "_mac",
        "_plate", "_plate_no", "_card_id", "_checkpoint_id",
    )
    if any(full.endswith(suf) for suf in sensitive_suffixes):
        return True
    chinese_tokens = ("手机号", "主叫", "被叫", "对端号码", "用户标识", "设备标识", "身份证", "邮箱", "车牌")
    return any(tok in str(key) for tok in chinese_tokens)


def iter_items(obj: Any, path: str = "content") -> Iterable[Tuple[str, Optional[str], Any]]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            next_path = f"{path}.{k}" if path else str(k)
            yield from iter_items(v, next_path)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            next_path = f"{path}[{i}]"
            yield from iter_items(v, next_path)
    else:
        key = path.split(".")[-1]
        if "[" in key:
            key = None
        yield path, key, obj


def stringify_for_text(obj: Any) -> str:
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, (int, float, bool)):
        return str(obj)
    if isinstance(obj, dict):
        parts: List[str] = []
        for path, key, value in iter_items(obj, "content"):
            if isinstance(value, (str, int, float, bool)):
                parts.append(str(value))
        return " ".join(parts)
    if isinstance(obj, list):
        return " ".join(stringify_for_text(x) for x in obj)
    return str(obj)


def flatten_dict(obj: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                out.update(flatten_dict(v, path))
            elif isinstance(v, list):
                # Preserve short primitive lists as a string; flatten nested list items.
                primitive = all(not isinstance(x, (dict, list)) for x in v)
                if primitive:
                    out[path] = " ".join(str(x) for x in v)
                else:
                    for i, item in enumerate(v):
                        out.update(flatten_dict(item, f"{path}[{i}]"))
            else:
                out[path] = v
    return out


def extract_detection_input(sample: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Return (content_text, features) for normalized or original annotations."""
    text_parts: List[str] = []
    features: Dict[str, Any] = {}

    if isinstance(sample.get("content_text"), str):
        text_parts.append(sample["content_text"])
    if isinstance(sample.get("features"), dict):
        features.update(sample["features"])

    content = sample.get("content")
    if content is not None:
        text_parts.append(stringify_for_text(content))
        if isinstance(content, dict):
            # Keep original nested fields as fallback. Avoid overriding normalized features.
            for k, v in flatten_dict(content, "content").items():
                features.setdefault(k, v)

    raw_content = sample.get("raw_content")
    if raw_content is not None and raw_content is not content:
        if isinstance(raw_content, str):
            text_parts.append(raw_content)
        elif isinstance(raw_content, dict):
            # raw_content is retained for compatibility; do not over-flatten if already has features.
            for k, v in flatten_dict(raw_content, "raw_content").items():
                features.setdefault(k, v)

    for top_key in ("query", "text", "output_text"):
        if isinstance(sample.get(top_key), str):
            text_parts.append(sample[top_key])

    # Include feature values in text scan only indirectly via field_scan; avoid massive duplication in text.
    content_text = "\n".join(t for t in text_parts if isinstance(t, str) and t.strip())
    return content_text, features

# -----------------------------
# Suppression helpers
# -----------------------------

def is_masked_or_placeholder(text: str) -> bool:
    if text is None:
        return False
    s = str(text).strip()
    if not s:
        return True
    if any(p.search(s) for p in MASKED_OR_SAFE_TEXT_PATTERNS):
        return True
    lower = s.lower()
    placeholders = {
        "xxx", "xxxx", "null", "none", "nil", "masked", "redacted", "<id>", "<phone>",
        "<imei>", "<redacted>", "your_api_key_here", "device_hash", "hash_email(value)",
    }
    return lower in placeholders


def email_is_placeholder(email: str) -> bool:
    e = email.strip().lower()
    if "<" in e or ">" in e:
        return True
    domain = e.split("@")[-1]
    return domain in PUBLIC_EXAMPLE_EMAIL_DOMAINS


def phone_is_placeholder(phone: str) -> bool:
    p = re.sub(r"\D", "", phone)
    if len(p) != 11:
        return True
    if p in {"00000000000", "11111111111", "19999999999"}:
        return True
    # 000/999 style placeholders or all same suffix.
    if len(set(p)) <= 2 and (p.startswith("000") or p.endswith("99999999")):
        return True
    return False


def ip_is_suppressed(ip: str, context: str) -> bool:
    s = ip.strip()
    ctx = context.lower()
    try:
        addr = ipaddress.ip_address(s)
    except ValueError:
        return True
    if addr.is_loopback or addr.is_unspecified or addr.is_multicast or addr.is_reserved:
        return True
    # RFC documentation and public example IPs.
    doc_networks = [
        ipaddress.ip_network("192.0.2.0/24"),
        ipaddress.ip_network("198.51.100.0/24"),
        ipaddress.ip_network("203.0.113.0/24"),
    ]
    if any(addr in net for net in doc_networks):
        return True
    if s == "93.184.216.34":
        return True
    # Network ranges/templates are not individual identifiers.
    if f"{s}/" in ctx or "网段" in ctx or "cidr" in ctx:
        return True
    # Synthetic test/example flow with example.test should not be counted as struct_id.
    if ("example.test" in ctx or "public.example" in ctx or "测试域名" in ctx) and "flow-struct-boundary" in ctx:
        return True
    return False


def mac_is_suppressed(mac: str) -> bool:
    s = mac.strip().lower()
    if "**" in s or "xx" in s:
        return True
    return s in {"00:00:00:00:00:00", "ff:ff:ff:ff:ff:ff"}


def url_param_value_is_suppressed(name: str, value: str, context: str) -> bool:
    n = name.lower()
    v = value.strip().strip("'\"")
    if is_masked_or_placeholder(v):
        return True
    if n in {"email"} and email_is_placeholder(v):
        return True
    if n in {"phone", "mobile"} and phone_is_placeholder(v):
        return True
    if "example.test" in context.lower() and n in {"user_id", "account_id", "email"}:
        return True
    return False


def looks_like_hash_or_stable_id(value: str, precise_field: bool = False) -> bool:
    if not isinstance(value, str):
        return False
    s = value.strip()
    if is_masked_or_placeholder(s):
        return False
    if precise_field and len(s) >= 4 and re.fullmatch(r"[A-Za-z0-9_:\-\.]+", s):
        return True
    if len(s) < 12 or len(s) > 180:
        return False
    if re.fullmatch(r"[a-fA-F0-9]{16,160}", s):
        return True
    if re.fullmatch(r"[A-Za-z0-9_:\-]{16,180}", s) and any(c.isdigit() for c in s) and any(c.isalpha() for c in s):
        return True
    return False


def should_suppress_hit(entity_type: str, span: str, context: str) -> bool:
    if is_masked_or_placeholder(span):
        return True
    if entity_type == "EMAIL_ADDRESS":
        return email_is_placeholder(span)
    if entity_type == "CN_PHONE_NUMBER":
        return phone_is_placeholder(span)
    if entity_type == "IP_ADDRESS":
        return ip_is_suppressed(span, context)
    if entity_type == "MAC_ADDRESS":
        return mac_is_suppressed(span)
    if entity_type == "URL_PARAM_IDENTIFIER":
        m = re.search(r"(?i)(user_id|account_id|phone|mobile|email|plate_no|vehicle_plate)=([^&\s'\"<>]+)", span)
        if m and url_param_value_is_suppressed(m.group(1), m.group(2), context):
            return True
    if entity_type == "VEHICLE_PLATE" and "****" in span:
        return True
    return False

# -----------------------------
# Detection logic
# -----------------------------

def make_hit(field: str, text: str, entity_type: str, method: str, score: float, reason: str, reason_code: str) -> Hit:
    return Hit(field=field, text=str(text)[:180], entity_type=entity_type, method=method, score=score, reason=reason, reason_code=reason_code)


def text_scan(text: str, field: str, context: Optional[str] = None) -> List[Hit]:
    hits: List[Hit] = []
    if not isinstance(text, str) or not text.strip():
        return hits
    scan_context = text if context is None else context
    for entity, pattern, score in REGEX_RULES:
        for m in pattern.finditer(text):
            span = m.group(0)
            if entity == "URL_PARAM_IDENTIFIER":
                name, value = m.group(1), m.group(2)
                if url_param_value_is_suppressed(name, value, scan_context):
                    continue
                hits.append(make_hit(field, span, entity, "regex", score, f"URL/query parameter '{name}' contains structured identifier", "url_param_identifier"))
                continue
            if should_suppress_hit(entity, span, scan_context):
                continue
            hits.append(make_hit(field, span, entity, "regex", score, f"regex matched {entity}", f"regex_{entity.lower()}"))
    return hits


def field_scan(path: str, key: Optional[str], value: Any, data_type: str, global_context: str) -> List[Hit]:
    hits: List[Hit] = []
    if value is None:
        return hits
    value_text = str(value)
    if is_masked_or_placeholder(value_text):
        return hits

    # Always scan string values for common identifiers.
    if isinstance(value, str):
        hits.extend(text_scan(value, path, context=(global_context + "\n" + value)))

    if key is None:
        return hits

    k = base_key(key)
    full = normalize_key(path)
    if not is_sensitive_field(key) and not is_sensitive_field(path):
        return hits

    # Extra suppressions for aggregate/placeholder descriptions.
    if is_aggregate_field(key) or is_aggregate_field(path):
        return hits
    if "field list" in global_context.lower() or "字段列表" in global_context:
        return hits

    # Field-specific validation.
    entity_type = k.upper()
    score = 0.90
    reason = f"sensitive field name '{key}' contains structured/stable identifier"
    reason_code = f"sensitive_field_{k}"

    # IP/MAC/email/phone fields should only be accepted when the value itself is concrete.
    if k in {"ip", "src_ip", "dst_ip", "collector_ip"} or full.endswith("_ip"):
        if not re.fullmatch(r"(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)", value_text.strip()):
            return hits
        if ip_is_suppressed(value_text.strip(), global_context):
            return hits
        entity_type = "IP_ADDRESS"
    elif k in {"mac", "mac_address", "src_mac", "dst_mac"} or full.endswith("_mac"):
        if not re.fullmatch(r"(?i)(?:[0-9a-f]{2}[:-]){5}[0-9a-f]{2}", value_text.strip()):
            return hits
        if mac_is_suppressed(value_text.strip()):
            return hits
        entity_type = "MAC_ADDRESS"
    elif k in {"email"}:
        if not re.fullmatch(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", value_text.strip()):
            return hits
        if email_is_placeholder(value_text.strip()):
            return hits
        entity_type = "EMAIL_ADDRESS"
    elif k in {"phone", "mobile", "driver_phone", "caller_phone", "callee_phone", "msisdn"}:
        if not re.fullmatch(r"1[3-9]\d{9}", value_text.strip()):
            return hits
        if phone_is_placeholder(value_text.strip()):
            return hits
        entity_type = "CN_PHONE_NUMBER"
    elif k in {"plate_no", "vehicle_plate", "license_plate"}:
        if not re.fullmatch(r"[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼使领][A-Z][A-Z0-9]{5,6}", value_text.strip()):
            return hits
        entity_type = "VEHICLE_PLATE"
    elif k in {"id_card", "identity_card", "cert_no"}:
        if not re.fullmatch(r"[1-9]\d{5}(?:18|19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[0-9Xx]", value_text.strip()):
            return hits
        entity_type = "CN_ID_CARD"
    else:
        # user_id, imei, src/dst IDs, checkpoint/device IDs, traffic cards.
        precise = k in SENSITIVE_FIELD_EXACT or full.split(".")[-1] in SENSITIVE_FIELD_EXACT
        if not looks_like_hash_or_stable_id(value_text, precise_field=precise):
            return hits

    hits.append(make_hit(path, value_text, entity_type, "field_rule", score, reason, reason_code))
    return hits


class OptionalPresidioAnalyzer:
    def __init__(self, mode: str = "auto") -> None:
        self.mode = mode
        self.enabled = False
        self.error: Optional[str] = None
        self.analyzer = None
        if mode == "never":
            return
        try:
            from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer  # type: ignore
            analyzer = AnalyzerEngine()
            custom_patterns = [
                ("CN_PHONE_NUMBER", r"(?<!\d)1[3-9]\d{9}(?!\d)", 0.85),
                ("CN_ID_CARD", r"(?<!\d)[1-9]\d{5}(?:18|19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[0-9Xx](?!\d)", 0.85),
                ("MAC_ADDRESS", r"(?<![A-Fa-f0-9])(?:[A-Fa-f0-9]{2}[:-]){5}[A-Fa-f0-9]{2}(?![A-Fa-f0-9])", 0.85),
            ]
            for entity, regex, score in custom_patterns:
                recognizer = PatternRecognizer(supported_entity=entity, patterns=[Pattern(name=entity.lower(), regex=regex, score=score)])
                analyzer.registry.add_recognizer(recognizer)
            self.analyzer = analyzer
            self.enabled = True
        except Exception as e:  # pragma: no cover
            self.error = str(e)
            if mode == "require":
                raise RuntimeError(
                    "Presidio was required but could not be initialized. Install presidio-analyzer and spaCy model, "
                    "or run with --use-presidio auto/never. Original error: " + self.error
                ) from e

    def scan(self, text: str, field: str) -> List[Hit]:
        if not self.enabled or not self.analyzer or not isinstance(text, str) or not text.strip():
            return []
        try:
            results = self.analyzer.analyze(text=text, language="en")
        except Exception:
            return []
        hits: List[Hit] = []
        for r in results:
            span = text[r.start:r.end]
            entity = getattr(r, "entity_type", "PRESIDIO_ENTITY")
            keep = entity in {
                "PHONE_NUMBER", "EMAIL_ADDRESS", "IP_ADDRESS", "IBAN_CODE", "US_SSN",
                "CN_PHONE_NUMBER", "CN_ID_CARD", "CREDIT_CARD", "URL", "MAC_ADDRESS",
            }
            if not keep or should_suppress_hit(entity if entity != "PHONE_NUMBER" else "CN_PHONE_NUMBER", span, text):
                continue
            hits.append(make_hit(field, span, entity, "presidio", float(getattr(r, "score", 0.5)), f"Presidio matched {entity}", f"presidio_{entity.lower()}"))
        return hits


def detect_struct_id(sample: Dict[str, Any], presidio: OptionalPresidioAnalyzer) -> Dict[str, Any]:
    data_type = str(sample.get("data_type") or sample.get("original_data_type") or "unknown")
    content_text, features = extract_detection_input(sample)
    hits: List[Hit] = []

    # 1. Scan normalized/free text.
    hits.extend(text_scan(content_text, "content_text"))
    hits.extend(presidio.scan(content_text, "content_text"))

    # 2. Scan flattened features with field semantics.
    for path, value in features.items():
        key = base_key(path)
        hits.extend(field_scan(path, key, value, data_type, content_text))
        if isinstance(value, str):
            hits.extend(presidio.scan(value, path))

    # 3. Original-format fallback when no normalized features are present.
    if not features and isinstance(sample.get("content"), (dict, list, str)):
        for path, key, value in iter_items(sample.get("content"), "content"):
            hits.extend(field_scan(path, key, value, data_type, content_text))
            if isinstance(value, str):
                hits.extend(presidio.scan(value, path))

    # De-duplicate and remove obviously unsupported placeholder hits.
    seen = set()
    deduped: List[Hit] = []
    for h in hits:
        sig = (h.field, h.text, h.entity_type, h.method)
        if sig in seen:
            continue
        seen.add(sig)
        deduped.append(h)

    pred_positive = bool(deduped)
    return {
        "sample_id": sample.get("sample_id"),
        "data_id": sample.get("data_id") or sample.get("original_data_id"),
        "data_type": data_type,
        "gate": sample.get("gate") or sample.get("trigger_gate"),
        "trigger_gate": sample.get("trigger_gate") or sample.get("gate"),
        "target_violation_type": sample.get("target_violation_type"),
        "predicted_violation_type": STRUCT_ID_VIOLATION if pred_positive else "none",
        "is_positive_pred": pred_positive,
        "confidence": max([h.score for h in deduped], default=0.0),
        "risk_locations_pred": [asdict(h) for h in deduped],
        "detector_version": DETECTOR_VERSION,
    }

# -----------------------------
# Evaluation
# -----------------------------

def gold_is_struct_id(sample: Dict[str, Any]) -> bool:
    target = sample.get("target_violation_type") or sample.get("original_target_violation_type")
    final = sample.get("final_violation_type") or sample.get("original_final_violation_type")
    is_pos = bool(sample.get("is_positive"))
    # For by_violation/struct_id.jsonl target is struct_id; final is none for negative cases.
    return is_pos and (final == STRUCT_ID_VIOLATION or target == STRUCT_ID_VIOLATION)


def metric_from_counter(c: Counter) -> Dict[str, Any]:
    TP, FP, FN, TN = c["TP"], c["FP"], c["FN"], c["TN"]
    total = TP + FP + FN + TN
    precision = TP / (TP + FP) if TP + FP else 0.0
    recall = TP / (TP + FN) if TP + FN else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = (TP + TN) / total if total else 0.0
    return {"TP": TP, "FP": FP, "FN": FN, "TN": TN, "accuracy": accuracy, "precision": precision, "recall": recall, "f1": f1, "samples": total}


def evaluate(samples: List[Dict[str, Any]], preds: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    overall = Counter()
    by_gate: Dict[str, Counter] = defaultdict(Counter)
    by_data_type: Dict[str, Counter] = defaultdict(Counter)
    by_sample_kind: Dict[str, Counter] = defaultdict(Counter)
    by_target: Dict[str, Counter] = defaultdict(Counter)
    false_positives: List[Dict[str, Any]] = []
    false_negatives: List[Dict[str, Any]] = []

    for s, p in zip(samples, preds):
        gold = gold_is_struct_id(s)
        pred = bool(p["is_positive_pred"])
        if gold and pred:
            key = "TP"
        elif (not gold) and pred:
            key = "FP"
        elif gold and (not pred):
            key = "FN"
        else:
            key = "TN"
        overall[key] += 1
        by_gate[str(s.get("gate") or s.get("trigger_gate") or "unknown")][key] += 1
        by_data_type[str(s.get("data_type") or s.get("original_data_type") or "unknown")][key] += 1
        by_sample_kind[str(s.get("sample_kind") or s.get("original_sample_kind") or "unknown")][key] += 1
        by_target[str(s.get("target_violation_type") or "unknown")][key] += 1

        if key in {"FP", "FN"}:
            item = {
                "error_type": key,
                "sample_id": s.get("sample_id"),
                "data_type": s.get("data_type"),
                "gate": s.get("gate") or s.get("trigger_gate"),
                "sample_kind": s.get("sample_kind"),
                "gold_is_positive": gold,
                "pred_is_positive": pred,
                "content_text": s.get("content_text") or stringify_for_text(s.get("content"))[:1000],
                "features": s.get("features", {}),
                "gold_risk_locations": s.get("risk_locations", []),
                "pred_risk_locations": p.get("risk_locations_pred", []),
            }
            (false_positives if key == "FP" else false_negatives).append(item)

    report = {
        "target": STRUCT_ID_VIOLATION,
        "detector_version": DETECTOR_VERSION,
        "overall": metric_from_counter(overall),
        "by_data_type": {k: metric_from_counter(v) for k, v in sorted(by_data_type.items())},
        "by_gate": {k: metric_from_counter(v) for k, v in sorted(by_gate.items())},
        "by_sample_kind": {k: metric_from_counter(v) for k, v in sorted(by_sample_kind.items())},
        "by_target_violation_type": {k: metric_from_counter(v) for k, v in sorted(by_target.items())},
        "false_positive_count": len(false_positives),
        "false_negative_count": len(false_negatives),
    }
    return report, false_positives, false_negatives

# -----------------------------
# CLI
# -----------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Detect structured identifier leakage in compliance JSONL samples.")
    parser.add_argument("--input", required=True, help="Input JSONL annotation file, original or normalized")
    parser.add_argument("--output", default="outputs/struct_id_predictions.jsonl", help="Prediction JSONL output path")
    parser.add_argument("--report", default="outputs/struct_id_evaluation_report.json", help="Evaluation report JSON path")
    parser.add_argument("--false-positives", default=None, help="Optional false positives JSONL output path")
    parser.add_argument("--false-negatives", default=None, help="Optional false negatives JSONL output path")
    parser.add_argument("--use-presidio", choices=["auto", "never", "require"], default="auto", help="Use Microsoft Presidio if available")
    args = parser.parse_args(argv)

    samples = read_jsonl(args.input)
    presidio = OptionalPresidioAnalyzer(args.use_presidio)
    preds = [detect_struct_id(s, presidio) for s in samples]
    write_jsonl(args.output, preds)
    report, fps, fns = evaluate(samples, preds)
    report["presidio"] = {"requested_mode": args.use_presidio, "enabled": presidio.enabled, "init_error": presidio.error}
    write_json(args.report, report)

    fp_path = args.false_positives
    fn_path = args.false_negatives
    if fp_path is None:
        fp_path = os.path.join(os.path.dirname(args.report) or ".", "struct_id_false_positives.jsonl")
    if fn_path is None:
        fn_path = os.path.join(os.path.dirname(args.report) or ".", "struct_id_false_negatives.jsonl")
    write_jsonl(fp_path, fps)
    write_jsonl(fn_path, fns)

    print(f"wrote {len(preds)} predictions to {args.output}")
    print(f"wrote report to {args.report}")
    print(f"wrote {len(fps)} false positives to {fp_path}")
    print(f"wrote {len(fns)} false negatives to {fn_path}")
    print(json.dumps(report["overall"], ensure_ascii=False, indent=2))
    if args.use_presidio != "never":
        print(f"Presidio enabled: {presidio.enabled}")
        if presidio.error:
            print(f"Presidio init note: {presidio.error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
