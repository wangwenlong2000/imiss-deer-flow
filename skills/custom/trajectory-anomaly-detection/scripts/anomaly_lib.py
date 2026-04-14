#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


MOBILITY_METRIC_HINTS = (
    "checkin_count",
    "unique_users",
    "duration_minutes",
    "distance_km",
    "distance",
    "point_count",
    "wow_change_pct",
    "speed",
    "speed_kmh",
    "flow",
    "volume",
    "count",
)

TIME_CANDIDATES = (
    "timestamp",
    "start_time",
    "time",
    "datetime",
    "meta.time_range.start",
    "time_range.start",
)

GROUP_CANDIDATES = (
    "meta.geo_scope.geohash",
    "geo_scope.geohash",
    "geohash",
    "user_id",
    "trajectory_id",
    "trip_id",
    "city",
    "meta.geo_scope.city",
)


def flatten(record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in record.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            flat.update(flatten(value, name))
        else:
            flat[name] = value
    return flat


def read_records(input_path: Path) -> list[dict[str, Any]]:
    suffix = input_path.suffix.lower()
    if suffix in {".jsonl", ".ndjson"}:
        records = []
        with input_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records
    if suffix == ".json":
        data = json.loads(input_path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("results"), list):
                return [item.get("source", item) if isinstance(item, dict) else item for item in data["results"]]
            if isinstance(data.get("records"), list):
                return data["records"]
        return []

    delimiter = "\t" if suffix == ".tsv" else ","
    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        if suffix not in {".csv", ".tsv"}:
            try:
                delimiter = csv.Sniffer().sniff(sample).delimiter
            except csv.Error:
                delimiter = ","
        return list(csv.DictReader(handle, delimiter=delimiter))


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        text = str(value).strip()
        if text == "" or text.lower() in {"nan", "none", "null"}:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.replace(".", "", 1).isdigit():
        try:
            number = float(text)
            if number > 1_000_000_000_000:
                number /= 1000
            return datetime.fromtimestamp(number, tz=timezone.utc).replace(tzinfo=None)
        except (ValueError, OSError):
            return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        return None


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    fraction = rank - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def detect_time_col(flat_records: list[dict[str, Any]], explicit: str | None = None) -> str | None:
    if explicit:
        return explicit
    keys = {key for record in flat_records for key in record.keys()}
    for candidate in TIME_CANDIDATES:
        if candidate in keys:
            return candidate
    return None


def detect_group_col(flat_records: list[dict[str, Any]], explicit: str | None = None) -> str | None:
    if explicit:
        return explicit
    keys = {key for record in flat_records for key in record.keys()}
    for candidate in GROUP_CANDIDATES:
        if candidate in keys:
            return candidate
    return None


def detect_metrics(flat_records: list[dict[str, Any]], explicit: list[str] | None = None) -> list[str]:
    if explicit:
        return explicit
    numeric_counts: dict[str, int] = defaultdict(int)
    for record in flat_records:
        for key, value in record.items():
            if parse_float(value) is not None:
                numeric_counts[key] += 1
    candidates = []
    for key, count in numeric_counts.items():
        lowered = key.lower()
        if count >= 2 and any(hint in lowered for hint in MOBILITY_METRIC_HINTS):
            candidates.append(key)
    if candidates:
        return sorted(candidates)
    return sorted(key for key, count in numeric_counts.items() if count >= 2)


def zscore_flags(values: list[float], threshold: float) -> dict[int, float]:
    if len(values) < 2:
        return {}
    mean = statistics.fmean(values)
    std = statistics.pstdev(values)
    if std == 0:
        return {}
    return {
        idx: (value - mean) / std
        for idx, value in enumerate(values)
        if abs((value - mean) / std) >= threshold
    }


def mad_flags(values: list[float], threshold: float) -> dict[int, float]:
    if len(values) < 3:
        return {}
    median = statistics.median(values)
    deviations = [abs(value - median) for value in values]
    mad = statistics.median(deviations)
    if mad == 0:
        return {}
    return {
        idx: 0.6745 * (value - median) / mad
        for idx, value in enumerate(values)
        if abs(0.6745 * (value - median) / mad) >= threshold
    }


def iqr_flags(values: list[float], multiplier: float) -> dict[int, float]:
    if len(values) < 4:
        return {}
    q1 = percentile(values, 0.25)
    q3 = percentile(values, 0.75)
    iqr = q3 - q1
    if iqr == 0:
        return {}
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    flags = {}
    for idx, value in enumerate(values):
        if value < lower:
            flags[idx] = (lower - value) / iqr
        elif value > upper:
            flags[idx] = (value - upper) / iqr
    return flags


def esd_flags(values: list[float], threshold: float, max_fraction: float) -> dict[int, float]:
    remaining = list(enumerate(values))
    flags: dict[int, float] = {}
    max_outliers = max(1, int(len(values) * max_fraction))
    for _ in range(max_outliers):
        if len(remaining) < 3:
            break
        current_values = [value for _, value in remaining]
        mean = statistics.fmean(current_values)
        std = statistics.pstdev(current_values)
        if std == 0:
            break
        scored = [(idx, (value - mean) / std) for idx, value in remaining]
        idx, score = max(scored, key=lambda item: abs(item[1]))
        if abs(score) < threshold:
            break
        flags[idx] = score
        remaining = [(ridx, value) for ridx, value in remaining if ridx != idx]
    return flags


def rolling_z_flags(items: list[tuple[int, datetime | None, float]], threshold: float, window: int) -> dict[int, float]:
    dated = [(idx, ts, value) for idx, ts, value in items if ts is not None]
    if len(dated) <= window:
        return {}
    dated.sort(key=lambda item: item[1])
    flags = {}
    history: list[float] = []
    for idx, _, value in dated:
        if len(history) >= window:
            recent = history[-window:]
            mean = statistics.fmean(recent)
            std = statistics.pstdev(recent)
            if std > 0:
                score = (value - mean) / std
                if abs(score) >= threshold:
                    flags[idx] = score
        history.append(value)
    return flags


def euclidean(a: list[float], b: list[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def lof_flags(vectors: list[tuple[int, list[float]]], threshold: float, k: int) -> dict[int, float]:
    if len(vectors) < max(k + 2, 5):
        return {}
    indexes = [idx for idx, _ in vectors]
    values = [vec for _, vec in vectors]

    columns = list(zip(*values))
    means = [statistics.fmean(col) for col in columns]
    stds = [statistics.pstdev(col) or 1.0 for col in columns]
    scaled = [
        [(value - means[pos]) / stds[pos] for pos, value in enumerate(vec)]
        for vec in values
    ]

    distances: list[list[tuple[int, float]]] = []
    for i, vec in enumerate(scaled):
        row = []
        for j, other in enumerate(scaled):
            if i != j:
                row.append((j, euclidean(vec, other)))
        row.sort(key=lambda item: item[1])
        distances.append(row)

    k = min(k, len(vectors) - 1)
    k_distance = [distances[i][k - 1][1] for i in range(len(vectors))]
    neighbors = [[j for j, _ in distances[i][:k]] for i in range(len(vectors))]

    lrd = []
    for i, neigh in enumerate(neighbors):
        reachability = [max(k_distance[j], euclidean(scaled[i], scaled[j])) for j in neigh]
        avg = statistics.fmean(reachability) if reachability else 0
        lrd.append(1 / avg if avg > 0 else 0)

    flags = {}
    for i, neigh in enumerate(neighbors):
        if lrd[i] == 0:
            continue
        lof = statistics.fmean([lrd[j] for j in neigh]) / lrd[i]
        if lof >= threshold:
            flags[indexes[i]] = lof
    return flags


def isolation_forest_flags(vectors: list[tuple[int, list[float]]], contamination: float) -> tuple[dict[int, float], str | None]:
    try:
        from sklearn.ensemble import IsolationForest  # type: ignore
    except Exception:
        return {}, "scikit-learn is not installed; isolation-forest was skipped."

    if len(vectors) < 5:
        return {}, "Not enough records for isolation-forest."
    indexes = [idx for idx, _ in vectors]
    data = [vec for _, vec in vectors]
    model = IsolationForest(contamination=contamination, random_state=42)
    labels = model.fit_predict(data)
    scores = -model.score_samples(data)
    return {
        indexes[i]: float(scores[i])
        for i, label in enumerate(labels)
        if label == -1
    }, None


def add_flag(
    flags_by_record: dict[int, list[dict[str, Any]]],
    record_idx: int,
    metric: str,
    method: str,
    score: float,
    value: float,
) -> None:
    direction = "high" if score > 0 else "low"
    flags_by_record[record_idx].append({
        "metric": metric,
        "method": method,
        "score": round(float(abs(score)), 6),
        "signed_score": round(float(score), 6),
        "direction": direction,
        "value": value,
    })


def write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count

