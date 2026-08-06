"""Feature extraction for trainable compliance detectors.

The extractor intentionally avoids hand-maintained business lexicons. It uses
character/token n-grams learned from training data plus generic structural
signals such as URL, time, coordinate, long identifier and small-count formats.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any, Iterable


ACTIVE_LABELS = ("video_meta_leak", "re_identify", "domain")
ALL_LABELS = ("none",) + ACTIVE_LABELS

LABEL_FIELDS = {
    "final_violation_type",
    "original_final_violation_type",
    "is_positive",
    "target_violation_type",
    "original_target_violation_type",
    "sample_kind",
    "original_sample_kind",
    "risk_locations",
    "original_risk_locations",
    "judgement_reason",
    "violation_name",
    "quality_flags",
}

STRUCTURAL_PATTERNS = {
    "has_url": re.compile(r"\b[a-z][a-z0-9+.-]*://|(?:^|[\s/])www\.", re.I),
    "has_ipv4": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "has_datetime": re.compile(r"\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}(?:[ t]\d{1,2}:\d{2}(?::\d{2})?)?\b", re.I),
    "has_clock": re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\b"),
    "has_coordinate_pair": re.compile(r"\b-?\d{1,3}\.\d{4,}\s*[,，]\s*-?\d{1,3}\.\d{4,}\b"),
    "has_long_digit": re.compile(r"\b\d{12,}\b"),
    "has_long_hex": re.compile(r"\b[0-9a-f]{24,}\b", re.I),
    "has_small_assignment": re.compile(r"\b[a-z_]*(?:count|size|k|users|candidates?)\s*[=:]\s*[1-5]\b", re.I),
    "has_route_arrow": re.compile(r"\S+\s*(?:->|→)\s*\S+"),
}


def map_label(row: dict[str, Any]) -> str:
    """Map repository labels into the three delivered detector labels plus none."""
    label = str(row.get("final_violation_type") or "none")
    return label if label in ACTIVE_LABELS else "none"


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        raw = value
    elif isinstance(value, (int, float, bool)):
        raw = str(value)
    else:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True)
    raw = raw.lower()
    raw = re.sub(r"\s+", " ", raw)
    return raw.strip()


def char_ngrams(text: str, min_n: int = 2, max_n: int = 5) -> Iterable[str]:
    compact = re.sub(r"\s+", "", text)
    for n in range(min_n, max_n + 1):
        if len(compact) < n:
            continue
        for i in range(len(compact) - n + 1):
            yield f"c{n}:{compact[i:i+n]}"


def token_ngrams(text: str, max_n: int = 2) -> Iterable[str]:
    tokens = re.findall(r"[a-z0-9_:/.-]{2,}|[\u4e00-\u9fff]", text, flags=re.I)
    for token in tokens:
        yield f"w1:{token}"
    if max_n >= 2:
        for left, right in zip(tokens, tokens[1:]):
            yield f"w2:{left}_{right}"


def path_features(path: str) -> Iterable[str]:
    normalized = normalize_text(path)
    if not normalized:
        return
    yield f"path:{normalized}"
    parts = [part for part in re.split(r"[^a-z0-9_]+", normalized) if part]
    for part in parts:
        yield f"path_part:{part}"
    for left, right in zip(parts, parts[1:]):
        yield f"path_bigram:{left}.{right}"


def iter_feature_parts(row: dict[str, Any]) -> Iterable[tuple[str, str]]:
    content_text = normalize_text(row.get("content_text"))
    if content_text:
        yield "content_text", content_text

    features = row.get("features") if isinstance(row.get("features"), dict) else {}
    for path, value in sorted(features.items()):
        text = normalize_text(value)
        if text:
            yield str(path), text


def extract_features(row: dict[str, Any]) -> Counter[str]:
    feats: Counter[str] = Counter()

    data_type = normalize_text(row.get("data_type"))
    gate = normalize_text(row.get("gate") or row.get("trigger_gate"))
    if data_type:
        feats[f"meta:data_type={data_type}"] += 1
    if gate:
        feats[f"meta:gate={gate}"] += 1

    all_text_parts: list[str] = []
    field_count = 0
    for path, text in iter_feature_parts(row):
        field_count += 1
        all_text_parts.append(text)
        for feat in path_features(path):
            feats[feat] += 1
        for feat in char_ngrams(text):
            feats[feat] += 1
        for feat in token_ngrams(text):
            feats[feat] += 1

    combined_text = " ".join(all_text_parts)
    for name, pattern in STRUCTURAL_PATTERNS.items():
        if pattern.search(combined_text):
            feats[f"struct:{name}"] += 1

    if field_count:
        bucket = "1" if field_count == 1 else "2_5" if field_count <= 5 else "6_20" if field_count <= 20 else "20_plus"
        feats[f"struct:field_count={bucket}"] += 1
    if combined_text:
        length = len(combined_text)
        bucket = "short" if length < 120 else "medium" if length < 600 else "long"
        feats[f"struct:text_length={bucket}"] += 1
        digit_ratio = sum(ch.isdigit() for ch in combined_text) / max(1, len(combined_text))
        ratio_bucket = "none" if digit_ratio == 0 else "low" if digit_ratio < 0.08 else "mid" if digit_ratio < 0.2 else "high"
        feats[f"struct:digit_ratio={ratio_bucket}"] += 1

    return feats


def strip_label_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return a copy with labels removed for prediction-only workflows."""
    return {key: value for key, value in row.items() if key not in LABEL_FIELDS}

