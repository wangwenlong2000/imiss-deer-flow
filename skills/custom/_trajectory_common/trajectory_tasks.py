#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def read_records(input_path: Path) -> list[dict[str, Any]]:
    suffix = input_path.suffix.lower()
    if suffix in {".jsonl", ".ndjson"}:
        records: list[dict[str, Any]] = []
        with input_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records
    if suffix == ".json":
        data = json.loads(input_path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [record for record in data if isinstance(record, dict)]
        if isinstance(data, dict):
            if isinstance(data.get("records"), list):
                return [record for record in data["records"] if isinstance(record, dict)]
            if isinstance(data.get("results"), list):
                rows = []
                for item in data["results"]:
                    if isinstance(item, dict):
                        source = item.get("source", item)
                        if isinstance(source, dict):
                            rows.append(source)
                return rows
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


def write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def flatten(record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in record.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            flat.update(flatten(value, name))
        else:
            flat[name] = value
    return flat


def get_path(record: dict[str, Any], path: str | None, default: Any = None) -> Any:
    if not path:
        return default
    current: Any = record
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return record.get(path, default) if isinstance(record, dict) else default
    return current


def parse_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
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
        except (OSError, ValueError):
            return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def detect_field(records: list[dict[str, Any]], candidates: list[str]) -> str | None:
    keys = {key for record in records for key in flatten(record).keys()}
    normalized = {key.lower().replace("_", "").replace(".", ""): key for key in keys}
    for candidate in candidates:
        compact = candidate.lower().replace("_", "").replace(".", "")
        if compact in normalized:
            return normalized[compact]
    return None


def detect_lat_lon(records: list[dict[str, Any]], lat_col: str | None = None, lon_col: str | None = None) -> tuple[str | None, str | None]:
    return (
        lat_col or detect_field(records, ["lat", "latitude", "centroid_lat", "start_lat", "meta.geo_scope.lat"]),
        lon_col or detect_field(records, ["lon", "lng", "longitude", "centroid_lon", "start_lon", "meta.geo_scope.lon"]),
    )


def detect_time(records: list[dict[str, Any]], time_col: str | None = None) -> str | None:
    return time_col or detect_field(records, ["timestamp", "start_time", "time", "datetime", "time_bucket", "meta.time_range.start", "date", "utc", "UTC Time"])


def detect_user(records: list[dict[str, Any]], user_col: str | None = None) -> str | None:
    return user_col or detect_field(records, ["user_id", "uid", "user", "device_id"])


def detect_geohash(records: list[dict[str, Any]], geohash_col: str | None = None) -> str | None:
    return geohash_col or detect_field(records, ["geohash", "start_geohash", "meta.geo_scope.geohash"])


def encode_geohash(lat: float, lon: float, precision: int = 6) -> str:
    lat_interval = [-90.0, 90.0]
    lon_interval = [-180.0, 180.0]
    geohash = []
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    even = True
    while len(geohash) < precision:
        if even:
            mid = sum(lon_interval) / 2
            if lon > mid:
                ch |= bits[bit]
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:
            mid = sum(lat_interval) / 2
            if lat > mid:
                ch |= bits[bit]
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(BASE32[ch])
            bit = 0
            ch = 0
    return "".join(geohash)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius_km * math.asin(math.sqrt(a))


def in_time_window(record: dict[str, Any], time_col: str | None, start: datetime | None, end: datetime | None, hour_start: int | None, hour_end: int | None) -> bool:
    if not any(value is not None for value in (start, end, hour_start, hour_end)):
        return True
    ts = parse_time(get_path(record, time_col)) if time_col else None
    if ts is None:
        return False
    if start and ts < start:
        return False
    if end and ts > end:
        return False
    if hour_start is not None or hour_end is not None:
        hs = 0 if hour_start is None else hour_start
        he = 24 if hour_end is None else hour_end
        if hs <= he:
            return hs <= ts.hour < he
        return ts.hour >= hs or ts.hour < he
    return True


def in_bbox(record: dict[str, Any], lat_col: str | None, lon_col: str | None, bbox: tuple[float, float, float, float] | None) -> bool:
    if bbox is None:
        return True
    if not lat_col or not lon_col:
        return False
    lat = parse_float(get_path(record, lat_col))
    lon = parse_float(get_path(record, lon_col))
    if lat is None or lon is None:
        return False
    min_lon, min_lat, max_lon, max_lat = bbox
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


def parse_bbox(value: str | None) -> tuple[float, float, float, float] | None:
    if not value:
        return None
    parts = [float(item.strip()) for item in value.split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be min_lon,min_lat,max_lon,max_lat")
    return parts[0], parts[1], parts[2], parts[3]


def record_text(record: dict[str, Any]) -> str:
    flat = flatten(record)
    return " ".join(str(value) for value in flat.values() if value not in (None, ""))


def filter_records(
    records: list[dict[str, Any]],
    *,
    query: str | None = None,
    city: str | None = None,
    geohash: str | None = None,
    geohash_col: str | None = None,
    time_col: str | None = None,
    time_start: str | None = None,
    time_end: str | None = None,
    hour_start: int | None = None,
    hour_end: int | None = None,
    bbox: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
) -> list[dict[str, Any]]:
    start = parse_time(time_start)
    end = parse_time(time_end)
    parsed_bbox = parse_bbox(bbox)
    lat_field, lon_field = detect_lat_lon(records, lat_col, lon_col)
    time_field = detect_time(records, time_col)
    geohash_field = detect_geohash(records, geohash_col)
    query_terms = [term.lower() for term in (query or "").split() if term.strip()]
    out = []
    for record in records:
        flat = flatten(record)
        if city:
            city_values = [str(value).lower() for key, value in flat.items() if "city" in key.lower()]
            if city.lower() not in city_values and city.lower() not in record_text(record).lower():
                continue
        if geohash:
            gh = str(get_path(record, geohash_field, "") or "")
            if not gh.startswith(geohash):
                continue
        if query_terms:
            text = record_text(record).lower()
            if not all(term in text for term in query_terms):
                continue
        if not in_time_window(record, time_field, start, end, hour_start, hour_end):
            continue
        if not in_bbox(record, lat_field, lon_field, parsed_bbox):
            continue
        out.append(record)
    return out


def add_grid(record: dict[str, Any], precision: int, lat_col: str | None, lon_col: str | None) -> dict[str, Any] | None:
    lat = parse_float(get_path(record, lat_col))
    lon = parse_float(get_path(record, lon_col))
    if lat is None or lon is None:
        return None
    enriched = dict(record)
    enriched["geohash"] = encode_geohash(lat, lon, precision)
    enriched["grid_id"] = enriched["geohash"]
    return enriched


def time_bucket(record: dict[str, Any], time_col: str | None, bucket: str) -> str:
    ts = parse_time(get_path(record, time_col)) if time_col else None
    if ts is None:
        return "unknown"
    if bucket == "hour":
        return ts.strftime("%Y-%m-%dT%H:00:00")
    if bucket == "day":
        return ts.strftime("%Y-%m-%d")
    if bucket == "month":
        return ts.strftime("%Y-%m")
    return "all"


def build_grid_records(input_path: Path, output_dir: Path, precision: int, lat_col: str | None, lon_col: str | None) -> dict[str, Any]:
    records = read_records(input_path)
    lat_field, lon_field = detect_lat_lon(records, lat_col, lon_col)
    gridded = []
    skipped = 0
    for record in records:
        item = add_grid(record, precision, lat_field, lon_field)
        if item is None:
            skipped += 1
        else:
            gridded.append(item)
    output_dir.mkdir(parents=True, exist_ok=True)
    points_path = output_dir / "grid_points.jsonl"
    write_jsonl(points_path, gridded)
    counter = Counter(record["geohash"] for record in gridded)
    summary_rows = [{"geohash": gh, "count": count} for gh, count in counter.most_common()]
    grid_summary_path = output_dir / "grid_summary.jsonl"
    write_jsonl(grid_summary_path, summary_rows)
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "detected_columns": {"lat": lat_field, "lon": lon_field},
        "parameters": {"geohash_precision": precision},
        "counts": {"input_records": len(records), "mapped_records": len(gridded), "skipped_records": skipped, "grids": len(counter)},
        "outputs": {"grid_points": str(points_path), "grid_summary": str(grid_summary_path), "summary": str(output_dir / "summary.json")},
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def search_records(input_path: Path, output_dir: Path, **kwargs: Any) -> dict[str, Any]:
    records = read_records(input_path)
    top_k = int(kwargs.get("top_k") or len(records))
    filter_kwargs = dict(kwargs)
    filter_kwargs.pop("top_k", None)
    matches = filter_records(records, **filter_kwargs)
    matches = matches[:top_k]
    output_dir.mkdir(parents=True, exist_ok=True)
    matches_path = output_dir / "matches.jsonl"
    write_jsonl(matches_path, matches)
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "parameters": kwargs,
        "counts": {"input_records": len(records), "matched_records": len(matches)},
        "outputs": {"matches": str(matches_path), "summary": str(output_dir / "summary.json")},
        "preview": matches[:5],
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def region_heat(input_path: Path, output_dir: Path, precision: int, bucket: str, **filters: Any) -> dict[str, Any]:
    lat_override = filters.get("lat_col")
    lon_override = filters.get("lon_col")
    time_override = filters.get("time_col")
    user_override = filters.get("user_col")
    geohash_override = filters.get("geohash_col")
    filter_kwargs = dict(filters)
    filter_kwargs.pop("user_col", None)
    records = filter_records(read_records(input_path), **filter_kwargs)
    lat_col, lon_col = detect_lat_lon(records, lat_override, lon_override)
    time_col = detect_time(records, time_override)
    user_col = detect_user(records, user_override)
    gh_col = detect_geohash(records, geohash_override)
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    skipped = 0
    for record in records:
        gh = str(get_path(record, gh_col, "") or "")
        if not gh and lat_col and lon_col:
            item = add_grid(record, precision, lat_col, lon_col)
            gh = item["geohash"] if item else ""
        if not gh:
            skipped += 1
            continue
        gh = gh[:precision]
        tb = time_bucket(record, time_col, bucket)
        key = (gh, tb)
        row = groups.setdefault(key, {"geohash": gh, "time_bucket": tb, "activity_count": 0, "users": set(), "categories": Counter()})
        row["activity_count"] += 1
        user = get_path(record, user_col)
        if user not in (None, ""):
            row["users"].add(str(user))
        category = get_path(record, "category") or get_path(record, "venue_category") or get_path(record, "top_cat_names")
        if isinstance(category, list):
            for item in category:
                row["categories"][str(item)] += 1
        elif category not in (None, ""):
            row["categories"][str(category)] += 1
    rows = []
    for row in groups.values():
        rows.append({
            "geohash": row["geohash"],
            "time_bucket": row["time_bucket"],
            "activity_count": row["activity_count"],
            "unique_users": len(row["users"]),
            "top_categories": row["categories"].most_common(5),
        })
    rows.sort(key=lambda item: (item["activity_count"], item["unique_users"]), reverse=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    heat_path = output_dir / "region_heat.jsonl"
    write_jsonl(heat_path, rows)
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "parameters": {"geohash_precision": precision, "time_bucket": bucket, **filters},
        "counts": {"filtered_records": len(records), "region_buckets": len(rows), "skipped_records": skipped},
        "outputs": {"region_heat": str(heat_path), "summary": str(output_dir / "summary.json")},
        "top_regions": rows[:10],
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def od_flow(input_path: Path, output_dir: Path, precision: int, top_k: int) -> dict[str, Any]:
    records = read_records(input_path)
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    skipped = 0
    for record in records:
        start = str(get_path(record, "start_geohash", "") or "")
        end = str(get_path(record, "end_geohash", "") or "")
        if not start:
            lat = parse_float(get_path(record, "start_lat"))
            lon = parse_float(get_path(record, "start_lon"))
            start = encode_geohash(lat, lon, precision) if lat is not None and lon is not None else ""
        if not end:
            lat = parse_float(get_path(record, "end_lat"))
            lon = parse_float(get_path(record, "end_lon"))
            end = encode_geohash(lat, lon, precision) if lat is not None and lon is not None else ""
        if not start or not end:
            skipped += 1
            continue
        key = (start[:precision], end[:precision])
        row = groups.setdefault(key, {"origin": key[0], "destination": key[1], "flow_count": 0, "users": set(), "distances": [], "durations": []})
        row["flow_count"] += 1
        user = get_path(record, "user_id")
        if user not in (None, ""):
            row["users"].add(str(user))
        for field, target in (("distance_km", "distances"), ("duration_minutes", "durations")):
            value = parse_float(get_path(record, field))
            if value is not None:
                row[target].append(value)
    rows = []
    for row in groups.values():
        rows.append({
            "origin": row["origin"],
            "destination": row["destination"],
            "od_pair": f"{row['origin']}->{row['destination']}",
            "flow_count": row["flow_count"],
            "unique_users": len(row["users"]),
            "avg_distance_km": round(statistics.fmean(row["distances"]), 6) if row["distances"] else None,
            "avg_duration_minutes": round(statistics.fmean(row["durations"]), 3) if row["durations"] else None,
        })
    rows.sort(key=lambda item: (item["flow_count"], item["unique_users"]), reverse=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    matrix_path = output_dir / "od_matrix.jsonl"
    top_path = output_dir / "top_od_flows.jsonl"
    write_jsonl(matrix_path, rows)
    write_jsonl(top_path, rows[:top_k])
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "parameters": {"geohash_precision": precision, "top_k": top_k},
        "counts": {"input_records": len(records), "od_pairs": len(rows), "skipped_records": skipped},
        "outputs": {"od_matrix": str(matrix_path), "top_od_flows": str(top_path), "summary": str(output_dir / "summary.json")},
        "top_od_flows": rows[:10],
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def trajectory_sequences(records: list[dict[str, Any]], precision: int, geohash_col: str | None = None) -> dict[str, list[str]]:
    user_col = detect_user(records)
    time_col = detect_time(records)
    lat_col, lon_col = detect_lat_lon(records)
    gh_col = detect_geohash(records, geohash_col)
    grouped: dict[str, list[tuple[datetime, str]]] = defaultdict(list)
    for idx, record in enumerate(records):
        user = str(get_path(record, user_col, f"record_{idx}"))
        ts = parse_time(get_path(record, time_col)) or datetime.min
        gh = str(get_path(record, gh_col, "") or "")
        if not gh and lat_col and lon_col:
            lat = parse_float(get_path(record, lat_col))
            lon = parse_float(get_path(record, lon_col))
            gh = encode_geohash(lat, lon, precision) if lat is not None and lon is not None else ""
        if gh:
            grouped[user].append((ts, gh[:precision]))
    return {
        user: [gh for _, gh in sorted(items, key=lambda item: item[0])]
        for user, items in grouped.items()
    }


def mine_patterns(input_path: Path, output_dir: Path, precision: int, max_len: int, min_support: int, top_k: int) -> dict[str, Any]:
    records = read_records(input_path)
    sequences = trajectory_sequences(records, precision)
    pattern_counts: Counter[tuple[str, ...]] = Counter()
    transition_counts: Counter[tuple[str, str]] = Counter()
    for seq in sequences.values():
        compressed = [gh for i, gh in enumerate(seq) if i == 0 or gh != seq[i - 1]]
        for a, b in zip(compressed, compressed[1:]):
            transition_counts[(a, b)] += 1
        for size in range(1, max_len + 1):
            seen = set()
            for i in range(0, max(0, len(compressed) - size + 1)):
                seen.add(tuple(compressed[i:i + size]))
            for pattern in seen:
                pattern_counts[pattern] += 1
    patterns = [
        {"pattern": list(pattern), "pattern_text": "->".join(pattern), "support": count, "length": len(pattern)}
        for pattern, count in pattern_counts.most_common()
        if count >= min_support
    ][:top_k]
    transitions = [
        {"from": a, "to": b, "transition": f"{a}->{b}", "count": count}
        for (a, b), count in transition_counts.most_common(top_k)
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    patterns_path = output_dir / "patterns.jsonl"
    transitions_path = output_dir / "transitions.jsonl"
    write_jsonl(patterns_path, patterns)
    write_jsonl(transitions_path, transitions)
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "parameters": {"geohash_precision": precision, "max_pattern_length": max_len, "min_support": min_support, "top_k": top_k},
        "counts": {"records": len(records), "sequences": len(sequences), "patterns": len(patterns), "transitions": len(transitions)},
        "outputs": {"patterns": str(patterns_path), "transitions": str(transitions_path), "summary": str(output_dir / "summary.json")},
        "top_patterns": patterns[:10],
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def trip_vector(record: dict[str, Any]) -> list[float] | None:
    fields = ["start_lat", "start_lon", "end_lat", "end_lon", "distance_km", "duration_minutes"]
    values = [parse_float(get_path(record, field)) for field in fields]
    if any(value is None for value in values[:4]):
        return None
    return [float(value or 0.0) for value in values]


def similarity_search(input_path: Path, output_dir: Path, target_id: str | None, top_k: int) -> dict[str, Any]:
    records = read_records(input_path)
    vectors = []
    for idx, record in enumerate(records):
        vector = trip_vector(record)
        if vector is not None:
            vectors.append((idx, record, vector))
    if not vectors:
        raise SystemExit("No trip-like vectors were found. Use trips.jsonl or records with start/end coordinates.")
    target = None
    for item in vectors:
        idx, record, vector = item
        rid = str(get_path(record, "trip_id", idx))
        if (target_id and rid == target_id) or (target_id is None and target is None):
            target = item
            break
    if target is None:
        raise SystemExit(f"Target trip_id not found: {target_id}")
    _, target_record, target_vec = target
    cols = list(zip(*(vec for _, _, vec in vectors)))
    means = [statistics.fmean(col) for col in cols]
    stds = [statistics.pstdev(col) or 1.0 for col in cols]

    def scale(vec: list[float]) -> list[float]:
        return [(value - means[i]) / stds[i] for i, value in enumerate(vec)]

    target_scaled = scale(target_vec)
    rows = []
    target_trip_id = str(get_path(target_record, "trip_id", "target"))
    for idx, record, vector in vectors:
        rid = str(get_path(record, "trip_id", idx))
        if rid == target_trip_id:
            continue
        distance = math.sqrt(sum((a - b) ** 2 for a, b in zip(target_scaled, scale(vector))))
        rows.append({
            "rank": 0,
            "trip_id": rid,
            "similarity_score": round(1 / (1 + distance), 6),
            "distance": round(distance, 6),
            "record": record,
        })
    rows.sort(key=lambda item: item["similarity_score"], reverse=True)
    rows = rows[:top_k]
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    output_dir.mkdir(parents=True, exist_ok=True)
    results_path = output_dir / "similar_trajectories.jsonl"
    write_jsonl(results_path, rows)
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "target": target_record,
        "counts": {"candidate_records": len(vectors), "returned": len(rows)},
        "outputs": {"similar_trajectories": str(results_path), "summary": str(output_dir / "summary.json")},
        "top_similar": rows[:10],
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def robust_scores(values: list[float]) -> list[float]:
    if len(values) < 2:
        return [0.0 for _ in values]
    if len(values) < 5:
        std = statistics.pstdev(values)
        if std == 0:
            return [0.0 for _ in values]
        mean = statistics.fmean(values)
        return [(value - mean) / std for value in values]
    median = statistics.median(values)
    deviations = [abs(value - median) for value in values]
    mad = statistics.median(deviations)
    if mad > 0:
        return [0.6745 * (value - median) / mad for value in values]
    std = statistics.pstdev(values)
    if std == 0:
        return [0.0 for _ in values]
    mean = statistics.fmean(values)
    return [(value - mean) / std for value in values]


def route_anomaly(input_path: Path, output_dir: Path, group_col: str | None, threshold: float, top_k: int) -> dict[str, Any]:
    records = read_records(input_path)
    enriched = []
    for idx, record in enumerate(records):
        duration = parse_float(get_path(record, "duration_minutes"))
        distance = parse_float(get_path(record, "distance_km"))
        speed = (distance / (duration / 60)) if distance is not None and duration and duration > 0 else parse_float(get_path(record, "avg_speed_kmh"))
        item = dict(record)
        item["_record_index"] = idx
        item["_avg_speed_kmh"] = speed
        item["_route_group"] = str(get_path(record, group_col, get_path(record, "user_id", "_all"))) if group_col else "_all"
        enriched.append(item)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in enriched:
        groups[item["_route_group"]].append(item)
    anomalies = []
    for group, rows in groups.items():
        for metric in ("distance_km", "duration_minutes", "_avg_speed_kmh"):
            values = [parse_float(get_path(row, metric)) for row in rows]
            valid = [(row, float(value)) for row, value in zip(rows, values) if value is not None]
            scores = robust_scores([value for _, value in valid])
            for (row, value), score in zip(valid, scores):
                if abs(score) >= threshold:
                    anomalies.append({
                        "record_index": row["_record_index"],
                        "group": group,
                        "trip_id": get_path(row, "trip_id", row["_record_index"]),
                        "anomaly_score": round(abs(score), 6),
                        "metric": metric.replace("_avg_speed_kmh", "avg_speed_kmh"),
                        "direction": "high" if score > 0 else "low",
                        "value": value,
                        "record": {k: v for k, v in row.items() if not k.startswith("_")},
                    })
    anomalies.sort(key=lambda item: item["anomaly_score"], reverse=True)
    anomalies = anomalies[:top_k]
    for rank, item in enumerate(anomalies, start=1):
        item["rank"] = rank
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "route_anomalies.jsonl"
    write_jsonl(out_path, anomalies)
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "parameters": {"group_col": group_col, "threshold": threshold, "top_k": top_k},
        "counts": {"records": len(records), "groups": len(groups), "anomalies": len(anomalies)},
        "outputs": {"route_anomalies": str(out_path), "summary": str(output_dir / "summary.json")},
        "top_anomalies": anomalies[:10],
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def flow_anomaly(input_path: Path, output_dir: Path, group_col: str | None, metric_col: str | None, threshold: float, top_k: int) -> dict[str, Any]:
    records = read_records(input_path)
    flat_records = [flatten(record) for record in records]
    if group_col is None:
        group_col = detect_field(records, ["geohash", "od_pair", "origin", "city", "meta.geo_scope.geohash"])
    if metric_col is None:
        metric_col = detect_field(records, ["activity_count", "flow_count", "checkin_count", "unique_users", "count", "volume"])
    time_col = detect_time(records)
    groups: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for idx, flat in enumerate(flat_records):
        value = parse_float(flat.get(metric_col or ""))
        if value is None:
            continue
        group = str(flat.get(group_col or "", "_all")) if group_col else "_all"
        groups[group].append((idx, value))
    anomalies = []
    for group, items in groups.items():
        scores = robust_scores([value for _, value in items])
        for (idx, value), score in zip(items, scores):
            if abs(score) >= threshold:
                anomalies.append({
                    "record_index": idx,
                    "group": group,
                    "timestamp": flat_records[idx].get(time_col) if time_col else None,
                    "anomaly_score": round(abs(score), 6),
                    "metric": metric_col,
                    "direction": "high" if score > 0 else "low",
                    "value": value,
                    "record": records[idx],
                })
    anomalies.sort(key=lambda item: item["anomaly_score"], reverse=True)
    anomalies = anomalies[:top_k]
    for rank, item in enumerate(anomalies, start=1):
        item["rank"] = rank
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "flow_anomalies.jsonl"
    write_jsonl(out_path, anomalies)
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "detected": {"group_col": group_col, "metric_col": metric_col, "time_col": time_col},
        "parameters": {"threshold": threshold, "top_k": top_k},
        "counts": {"records": len(records), "groups": len(groups), "anomalies": len(anomalies)},
        "outputs": {"flow_anomalies": str(out_path), "summary": str(output_dir / "summary.json")},
        "top_anomalies": anomalies[:10],
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def detect_category(records: list[dict[str, Any]], category_col: str | None = None) -> str | None:
    return category_col or detect_field(records, ["category", "venue_category", "Venue Category Name", "top_cat_names", "poi_category", "landuse"])


def parse_categories(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            pass
    for sep in ("|", ";", ",", "/"):
        if sep in text:
            return [item.strip() for item in text.split(sep) if item.strip()]
    return [text]


def parse_bucket_timestamp(value: Any) -> datetime | None:
    ts = parse_time(value)
    if ts is not None:
        return ts
    return None


def bucket_delta(bucket: str) -> timedelta:
    if bucket == "hour":
        return timedelta(hours=1)
    if bucket == "day":
        return timedelta(days=1)
    return timedelta(days=30)


def add_month(ts: datetime, step: int = 1) -> datetime:
    month_index = (ts.month - 1) + step
    year = ts.year + month_index // 12
    month = month_index % 12 + 1
    return ts.replace(year=year, month=month, day=1)


def format_bucket(ts: datetime, bucket: str) -> str:
    if bucket == "hour":
        return ts.strftime("%Y-%m-%dT%H:00:00")
    if bucket == "day":
        return ts.strftime("%Y-%m-%d")
    if bucket == "month":
        return ts.strftime("%Y-%m")
    return ts.isoformat()


def increment_bucket(ts: datetime, bucket: str, step: int = 1) -> datetime:
    if bucket == "month":
        return add_month(ts, step)
    return ts + bucket_delta(bucket) * step


def aggregate_region_rows(
    records: list[dict[str, Any]],
    *,
    precision: int,
    bucket: str,
    query: str | None = None,
    city: str | None = None,
    geohash: str | None = None,
    geohash_col: str | None = None,
    time_col: str | None = None,
    time_start: str | None = None,
    time_end: str | None = None,
    hour_start: int | None = None,
    hour_end: int | None = None,
    bbox: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
    user_col: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str | None], int]:
    lat_field, lon_field = detect_lat_lon(records, lat_col, lon_col)
    time_field = detect_time(records, time_col)
    user_field = detect_user(records, user_col)
    gh_field = detect_geohash(records, geohash_col)
    filtered = filter_records(
        records,
        query=query,
        city=city,
        geohash=geohash,
        geohash_col=gh_field,
        time_col=time_field,
        time_start=time_start,
        time_end=time_end,
        hour_start=hour_start,
        hour_end=hour_end,
        bbox=bbox,
        lat_col=lat_field,
        lon_col=lon_field,
    )
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    skipped = 0
    for record in filtered:
        region = str(get_path(record, gh_field, "") or "")
        if not region and lat_field and lon_field:
            item = add_grid(record, precision, lat_field, lon_field)
            region = item["geohash"] if item else ""
        if not region:
            skipped += 1
            continue
        region = region[:precision]
        tb = time_bucket(record, time_field, bucket)
        row = groups.setdefault((region, tb), {"geohash": region, "time_bucket": tb, "activity_count": 0, "users": set(), "categories": Counter()})
        row["activity_count"] += 1
        user = get_path(record, user_field)
        if user not in (None, ""):
            row["users"].add(str(user))
        category = get_path(record, "category") or get_path(record, "venue_category") or get_path(record, "Venue Category Name") or get_path(record, "top_cat_names")
        for item in parse_categories(category):
            row["categories"][item] += 1
    rows = []
    for row in groups.values():
        rows.append({
            "geohash": row["geohash"],
            "time_bucket": row["time_bucket"],
            "activity_count": row["activity_count"],
            "unique_users": len(row["users"]),
            "top_categories": row["categories"].most_common(5),
        })
    rows.sort(key=lambda item: (item["activity_count"], item["unique_users"]), reverse=True)
    detected = {"lat_col": lat_field, "lon_col": lon_field, "time_col": time_field, "user_col": user_field, "geohash_col": gh_field}
    return rows, detected, skipped


def standardize_metric_rows(
    input_path: Path,
    *,
    precision: int,
    bucket: str,
    group_col: str | None = None,
    metric_col: str | None = None,
    time_col: str | None = None,
    geohash_col: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
    user_col: str | None = None,
    query: str | None = None,
    city: str | None = None,
    geohash: str | None = None,
    time_start: str | None = None,
    time_end: str | None = None,
    hour_start: int | None = None,
    hour_end: int | None = None,
    bbox: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str | None]]:
    records = read_records(input_path)
    detected_group = group_col or detect_field(records, ["geohash", "grid_id", "region_id", "od_pair", "origin"])
    detected_metric = metric_col or detect_field(records, ["activity_count", "flow_count", "checkin_count", "unique_users", "count", "volume"])
    detected_time = time_col or detect_field(records, ["time_bucket", "timestamp", "start_time", "time", "datetime", "date"])
    if detected_group and detected_metric and detected_time:
        rows = []
        for record in records:
            group_value = get_path(record, detected_group)
            metric_value = parse_float(get_path(record, detected_metric))
            bucket_value = get_path(record, detected_time)
            parsed_bucket = parse_bucket_timestamp(bucket_value)
            if group_value in (None, "") or metric_value is None or parsed_bucket is None:
                continue
            rows.append({
                "region": str(group_value),
                "time_bucket": str(bucket_value),
                "metric": float(metric_value),
                "record": record,
            })
        if rows:
            return rows, {"mode": "aggregated", "group_col": detected_group, "metric_col": detected_metric, "time_col": detected_time}
    region_rows, detected, _ = aggregate_region_rows(
        records,
        precision=precision,
        bucket=bucket,
        query=query,
        city=city,
        geohash=geohash,
        geohash_col=geohash_col,
        time_col=time_col,
        time_start=time_start,
        time_end=time_end,
        hour_start=hour_start,
        hour_end=hour_end,
        bbox=bbox,
        lat_col=lat_col,
        lon_col=lon_col,
        user_col=user_col,
    )
    rows = [{"region": row["geohash"], "time_bucket": row["time_bucket"], "metric": float(row["activity_count"]), "record": row} for row in region_rows]
    detected_summary = {"mode": "raw", "group_col": "geohash", "metric_col": "activity_count", "time_col": "time_bucket", **detected}
    return rows, detected_summary


def forecast_region_flow(
    input_path: Path,
    output_dir: Path,
    *,
    precision: int,
    bucket: str,
    forecast_steps: int,
    history_window: int,
    top_k: int,
    group_col: str | None = None,
    metric_col: str | None = None,
    time_col: str | None = None,
    geohash_col: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
    user_col: str | None = None,
    query: str | None = None,
    city: str | None = None,
    geohash: str | None = None,
    time_start: str | None = None,
    time_end: str | None = None,
    hour_start: int | None = None,
    hour_end: int | None = None,
    bbox: str | None = None,
) -> dict[str, Any]:
    rows, detected = standardize_metric_rows(
        input_path,
        precision=precision,
        bucket=bucket,
        group_col=group_col,
        metric_col=metric_col,
        time_col=time_col,
        geohash_col=geohash_col,
        lat_col=lat_col,
        lon_col=lon_col,
        user_col=user_col,
        query=query,
        city=city,
        geohash=geohash,
        time_start=time_start,
        time_end=time_end,
        hour_start=hour_start,
        hour_end=hour_end,
        bbox=bbox,
    )
    grouped: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    for row in rows:
        ts = parse_bucket_timestamp(row["time_bucket"])
        if ts is None:
            continue
        grouped[row["region"]].append((ts, row["metric"]))
    ranked_groups = sorted(grouped.items(), key=lambda item: sum(value for _, value in item[1]), reverse=True)[:top_k]
    forecasts = []
    for region, series in ranked_groups:
        series = sorted(series, key=lambda item: item[0])
        values = [value for _, value in series]
        window = values[-history_window:] if history_window > 0 else values
        if not window:
            continue
        if len(window) == 1:
            baseline = window[-1]
            trend = 0.0
        else:
            baseline = statistics.fmean(window)
            deltas = [b - a for a, b in zip(window, window[1:])]
            trend = statistics.fmean(deltas) if deltas else 0.0
        volatility = statistics.pstdev(window) if len(window) > 1 else 0.0
        last_ts = series[-1][0]
        for step in range(1, forecast_steps + 1):
            target_ts = increment_bucket(last_ts, bucket, step)
            forecast_value = max(0.0, baseline + trend * step)
            forecasts.append({
                "region": region,
                "time_bucket": format_bucket(target_ts, bucket),
                "forecast_value": round(forecast_value, 3),
                "lower_bound": round(max(0.0, forecast_value - volatility), 3),
                "upper_bound": round(forecast_value + volatility, 3),
                "baseline": round(baseline, 3),
                "trend_per_step": round(trend, 3),
                "history_points": len(window),
                "source_points": len(series),
            })
    forecasts.sort(key=lambda item: (item["region"], item["time_bucket"]))
    top_forecasts = sorted(forecasts, key=lambda item: item["forecast_value"], reverse=True)[:top_k]
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "region_forecasts.jsonl"
    write_jsonl(out_path, forecasts)
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "detected": detected,
        "parameters": {
            "geohash_precision": precision,
            "time_bucket": bucket,
            "forecast_steps": forecast_steps,
            "history_window": history_window,
            "top_k_regions": top_k,
        },
        "counts": {
            "input_rows": len(rows),
            "regions": len(grouped),
            "forecast_regions": len(ranked_groups),
            "forecast_rows": len(forecasts),
        },
        "outputs": {"region_forecasts": str(out_path), "summary": str(output_dir / "summary.json")},
        "top_forecasts": top_forecasts,
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def score_category_keywords(categories: Counter[str], keywords: list[str]) -> float:
    if not categories:
        return 0.0
    total = sum(categories.values()) or 1
    score = 0
    for category, count in categories.items():
        text = category.lower()
        if any(keyword in text for keyword in keywords):
            score += count
    return score / total


def profile_urban_region(
    input_path: Path,
    output_dir: Path,
    *,
    precision: int,
    poi_input: Path | None = None,
    poi_lat_col: str | None = None,
    poi_lon_col: str | None = None,
    poi_category_col: str | None = None,
    geohash_col: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
    time_col: str | None = None,
    user_col: str | None = None,
    top_k: int = 20,
) -> dict[str, Any]:
    records = read_records(input_path)
    lat_field, lon_field = detect_lat_lon(records, lat_col, lon_col)
    time_field = detect_time(records, time_col)
    user_field = detect_user(records, user_col)
    gh_field = detect_geohash(records, geohash_col)
    grouped: dict[str, dict[str, Any]] = {}
    for idx, record in enumerate(records):
        region = str(get_path(record, gh_field, "") or "")
        if not region and lat_field and lon_field:
            item = add_grid(record, precision, lat_field, lon_field)
            region = item["geohash"] if item else ""
        if not region:
            continue
        region = region[:precision]
        row = grouped.setdefault(region, {
            "geohash": region,
            "activity_count": 0,
            "users": set(),
            "morning": 0,
            "workday": 0,
            "evening": 0,
            "night": 0,
            "weekend": 0,
            "record_categories": Counter(),
            "poi_categories": Counter(),
        })
        row["activity_count"] += 1
        user = get_path(record, user_field, f"record_{idx}")
        if user not in (None, ""):
            row["users"].add(str(user))
        ts = parse_time(get_path(record, time_field)) if time_field else None
        if ts is not None:
            if 6 <= ts.hour < 10:
                row["morning"] += 1
            elif 10 <= ts.hour < 17:
                row["workday"] += 1
            elif 17 <= ts.hour < 22:
                row["evening"] += 1
            else:
                row["night"] += 1
            if ts.weekday() >= 5:
                row["weekend"] += 1
        for item in parse_categories(get_path(record, "category") or get_path(record, "venue_category") or get_path(record, "Venue Category Name") or get_path(record, "top_cat_names")):
            row["record_categories"][item] += 1
    if poi_input is not None and poi_input.exists():
        poi_records = read_records(poi_input)
        poi_lat_field, poi_lon_field = detect_lat_lon(poi_records, poi_lat_col, poi_lon_col)
        poi_category_field = detect_category(poi_records, poi_category_col)
        for record in poi_records:
            item = add_grid(record, precision, poi_lat_field, poi_lon_field)
            if item is None:
                continue
            region = item["geohash"][:precision]
            row = grouped.setdefault(region, {
                "geohash": region,
                "activity_count": 0,
                "users": set(),
                "morning": 0,
                "workday": 0,
                "evening": 0,
                "night": 0,
                "weekend": 0,
                "record_categories": Counter(),
                "poi_categories": Counter(),
            })
            for item in parse_categories(get_path(record, poi_category_field)):
                row["poi_categories"][item] += 1
    profiles = []
    residential_keywords = ["residential", "apartment", "housing", "community", "neighborhood", "住宅", "小区"]
    commercial_keywords = ["office", "company", "mall", "shop", "store", "bank", "market", "business", "building", "plaza", "商业", "写字楼"]
    transit_keywords = ["station", "subway", "metro", "bus", "airport", "rail", "train", "transport", "地铁", "车站", "机场"]
    leisure_keywords = ["cafe", "restaurant", "bar", "club", "park", "museum", "cinema", "entertainment", "hotel", "shopping", "餐厅", "公园", "酒店"]
    for row in grouped.values():
        total = row["activity_count"] or 1
        morning_ratio = row["morning"] / total
        workday_ratio = row["workday"] / total
        evening_ratio = row["evening"] / total
        night_ratio = row["night"] / total
        weekend_ratio = row["weekend"] / total
        categories = row["record_categories"] + row["poi_categories"]
        scores = {
            "residential": 0.45 * night_ratio + 0.2 * weekend_ratio + 0.35 * score_category_keywords(categories, residential_keywords),
            "commercial": 0.45 * workday_ratio + 0.15 * evening_ratio + 0.4 * score_category_keywords(categories, commercial_keywords),
            "transit": 0.5 * (morning_ratio + evening_ratio) + 0.5 * score_category_keywords(categories, transit_keywords),
            "leisure": 0.35 * (evening_ratio + night_ratio) + 0.15 * weekend_ratio + 0.5 * score_category_keywords(categories, leisure_keywords),
        }
        ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        label = ranked[0][0]
        if len(ranked) > 1 and ranked[0][1] - ranked[1][1] < 0.08:
            label = "mixed"
        profiles.append({
            "geohash": row["geohash"],
            "function_label": label,
            "activity_count": row["activity_count"],
            "unique_users": len(row["users"]),
            "time_signature": {
                "morning_ratio": round(morning_ratio, 3),
                "workday_ratio": round(workday_ratio, 3),
                "evening_ratio": round(evening_ratio, 3),
                "night_ratio": round(night_ratio, 3),
                "weekend_ratio": round(weekend_ratio, 3),
            },
            "function_scores": {key: round(value, 3) for key, value in scores.items()},
            "top_poi_categories": categories.most_common(5),
        })
    profiles.sort(key=lambda item: (item["activity_count"], item["unique_users"]), reverse=True)
    profiles = profiles[:top_k]
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "region_profiles.jsonl"
    write_jsonl(out_path, profiles)
    summary = {
        "ok": True,
        "input": str(input_path),
        "poi_input": str(poi_input) if poi_input else None,
        "output_dir": str(output_dir),
        "parameters": {"geohash_precision": precision, "top_k": top_k},
        "detected": {
            "lat_col": lat_field,
            "lon_col": lon_field,
            "time_col": time_field,
            "user_col": user_field,
            "geohash_col": gh_field,
        },
        "counts": {"records": len(records), "profiled_regions": len(profiles), "all_regions": len(grouped)},
        "outputs": {"region_profiles": str(out_path), "summary": str(output_dir / "summary.json")},
        "top_profiles": profiles[:10],
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def analyze_event_impact(
    input_path: Path,
    output_dir: Path,
    *,
    precision: int,
    bucket: str,
    event_start: str,
    event_end: str,
    before_start: str | None = None,
    before_end: str | None = None,
    after_start: str | None = None,
    after_end: str | None = None,
    group_col: str | None = None,
    metric_col: str | None = None,
    time_col: str | None = None,
    geohash_col: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
    user_col: str | None = None,
    top_k: int = 20,
    query: str | None = None,
    city: str | None = None,
    geohash: str | None = None,
    bbox: str | None = None,
) -> dict[str, Any]:
    event_start_dt = parse_time(event_start)
    event_end_dt = parse_time(event_end)
    if event_start_dt is None or event_end_dt is None:
        raise SystemExit("event_start and event_end must be valid timestamps.")
    window = event_end_dt - event_start_dt
    before_start_dt = parse_time(before_start) if before_start else event_start_dt - window
    before_end_dt = parse_time(before_end) if before_end else event_start_dt
    after_start_dt = parse_time(after_start) if after_start else event_end_dt
    after_end_dt = parse_time(after_end) if after_end else event_end_dt + window
    rows, detected = standardize_metric_rows(
        input_path,
        precision=precision,
        bucket=bucket,
        group_col=group_col,
        metric_col=metric_col,
        time_col=time_col,
        geohash_col=geohash_col,
        lat_col=lat_col,
        lon_col=lon_col,
        user_col=user_col,
        query=query,
        city=city,
        geohash=geohash,
        bbox=bbox,
    )
    grouped: dict[str, dict[str, float]] = defaultdict(lambda: {"before": 0.0, "during": 0.0, "after": 0.0})
    for row in rows:
        ts = parse_bucket_timestamp(row["time_bucket"])
        if ts is None:
            continue
        region = row["region"]
        value = row["metric"]
        if before_start_dt <= ts < before_end_dt:
            grouped[region]["before"] += value
        if event_start_dt <= ts <= event_end_dt:
            grouped[region]["during"] += value
        if after_start_dt <= ts <= after_end_dt:
            grouped[region]["after"] += value
    impacts = []
    for region, values in grouped.items():
        before_value = values["before"]
        during_value = values["during"]
        after_value = values["after"]
        change_ratio = (during_value - before_value) / max(before_value, 1.0)
        recovery_ratio = after_value / max(during_value, 1.0)
        if change_ratio >= 0.3:
            label = "surge"
        elif change_ratio <= -0.3:
            label = "drop"
        elif recovery_ratio >= 0.8:
            label = "recovered"
        else:
            label = "stable"
        impact_score = abs(change_ratio) * math.log1p(before_value + during_value + after_value)
        impacts.append({
            "region": region,
            "before_metric": round(before_value, 3),
            "event_metric": round(during_value, 3),
            "after_metric": round(after_value, 3),
            "event_vs_before_ratio": round(change_ratio, 3),
            "recovery_ratio": round(recovery_ratio, 3),
            "impact_score": round(impact_score, 6),
            "impact_label": label,
        })
    impacts.sort(key=lambda item: item["impact_score"], reverse=True)
    impacts = impacts[:top_k]
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "event_impacts.jsonl"
    write_jsonl(out_path, impacts)
    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "detected": detected,
        "parameters": {
            "geohash_precision": precision,
            "time_bucket": bucket,
            "event_start": event_start,
            "event_end": event_end,
            "before_start": format_bucket(before_start_dt, bucket),
            "before_end": format_bucket(before_end_dt, bucket),
            "after_start": format_bucket(after_start_dt, bucket),
            "after_end": format_bucket(after_end_dt, bucket),
            "top_k": top_k,
        },
        "counts": {"input_rows": len(rows), "impacted_regions": len(impacts), "all_regions": len(grouped)},
        "outputs": {"event_impacts": str(out_path), "summary": str(output_dir / "summary.json")},
        "top_impacts": impacts[:10],
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def extract_evidence_region(record: dict[str, Any]) -> str | None:
    for candidate in ("geohash", "region", "group", "origin", "od_pair", "grid_id"):
        value = get_path(record, candidate)
        if value not in (None, ""):
            return str(value)
    nested = get_path(record, "record")
    if isinstance(nested, dict):
        return extract_evidence_region(nested)
    return None


def extract_evidence_time(record: dict[str, Any]) -> str | None:
    for candidate in ("time_bucket", "timestamp", "forecast_time", "event_start"):
        value = get_path(record, candidate)
        if value not in (None, ""):
            return str(value)
    nested = get_path(record, "record")
    if isinstance(nested, dict):
        return extract_evidence_time(nested)
    return None


def extract_evidence_score(record: dict[str, Any]) -> float:
    for candidate in ("anomaly_score", "forecast_value", "impact_score", "similarity_score", "activity_count", "flow_count", "metric", "value", "score"):
        value = parse_float(get_path(record, candidate))
        if value is not None:
            return float(value)
    return 1.0


def extract_evidence_label(record: dict[str, Any]) -> str | None:
    for candidate in ("function_label", "impact_label", "direction", "metric", "transition", "pattern_text"):
        value = get_path(record, candidate)
        if value not in (None, ""):
            return str(value)
    return None


def fuse_spatial_evidence(input_paths: list[Path], output_dir: Path, top_k: int) -> dict[str, Any]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    evidence_rows = []
    for path in input_paths:
        source_name = path.stem
        records = read_records(path)
        for record in records:
            region = extract_evidence_region(record)
            if not region:
                continue
            time_bucket = extract_evidence_time(record) or "all"
            score = extract_evidence_score(record)
            label = extract_evidence_label(record)
            key = (region, time_bucket)
            row = grouped.setdefault(key, {
                "region": region,
                "time_bucket": time_bucket,
                "fused_score": 0.0,
                "evidence_count": 0,
                "sources": Counter(),
                "labels": Counter(),
                "max_score": 0.0,
            })
            row["fused_score"] += math.log1p(max(score, 0.0))
            row["evidence_count"] += 1
            row["sources"][source_name] += 1
            if label:
                row["labels"][label] += 1
            row["max_score"] = max(row["max_score"], score)
            evidence_rows.append({
                "region": region,
                "time_bucket": time_bucket,
                "source": source_name,
                "score": round(score, 6),
                "label": label,
                "record": record,
            })
    fused = []
    for row in grouped.values():
        fused.append({
            "region": row["region"],
            "time_bucket": row["time_bucket"],
            "fused_score": round(row["fused_score"], 6),
            "max_score": round(row["max_score"], 6),
            "evidence_count": row["evidence_count"],
            "source_breakdown": dict(row["sources"]),
            "dominant_labels": row["labels"].most_common(3),
        })
    fused.sort(key=lambda item: (item["fused_score"], item["evidence_count"]), reverse=True)
    top_rows = fused[:top_k]
    output_dir.mkdir(parents=True, exist_ok=True)
    fused_path = output_dir / "fused_evidence.jsonl"
    detail_path = output_dir / "evidence_details.jsonl"
    write_jsonl(fused_path, top_rows)
    write_jsonl(detail_path, evidence_rows)
    summary = {
        "ok": True,
        "inputs": [str(path) for path in input_paths],
        "output_dir": str(output_dir),
        "parameters": {"top_k": top_k},
        "counts": {"fused_regions": len(fused), "returned": len(top_rows), "evidence_rows": len(evidence_rows)},
        "outputs": {"fused_evidence": str(fused_path), "evidence_details": str(detail_path), "summary": str(output_dir / "summary.json")},
        "top_fused_regions": top_rows,
    }
    write_json(output_dir / "summary.json", summary)
    return summary
