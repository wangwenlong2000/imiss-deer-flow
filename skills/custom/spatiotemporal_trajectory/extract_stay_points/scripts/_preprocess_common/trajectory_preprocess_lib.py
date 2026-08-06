#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


@dataclass
class Point:
    user_id: str
    trajectory_id: str
    timestamp: datetime
    lat: float
    lon: float
    attributes: dict[str, Any] = field(default_factory=dict)
    geohash: str = ""
    sequence: int = 0
    point_id: str = ""


@dataclass
class Stats:
    raw_points: int = 0
    invalid_schema: int = 0
    invalid_time: int = 0
    invalid_coordinate: int = 0
    duplicate_points: int = 0
    speed_filtered_points: int = 0
    cleaned_points: int = 0
    users: int = 0
    staypoints: int = 0
    trips: int = 0
    warnings: list[str] = field(default_factory=list)


COLUMN_CANDIDATES = {
    "user": ["user_id", "userid", "user", "uid", "device_id", "deviceid", "user id"],
    "trajectory": ["trajectory_id", "trajectoryid", "traj_id", "trajid", "trip_id", "tripid", "trace_id", "traceid"],
    "timestamp": ["timestamp", "time", "datetime", "date_time", "local_time", "utc_time", "utc time", "created_at", "checkin_time"],
    "lat": ["lat", "latitude", "y"],
    "lon": ["lon", "lng", "longitude", "x"],
}


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def detect_columns(fieldnames: list[str], overrides: dict[str, str | None]) -> dict[str, str | None]:
    normalized = {normalize_name(name): name for name in fieldnames}
    detected: dict[str, str | None] = {}

    for role, override in overrides.items():
        if override:
            detected[role] = override
            continue
        detected[role] = None
        for candidate in COLUMN_CANDIDATES.get(role, []):
            normalized_candidate = normalize_name(candidate)
            if normalized_candidate in normalized:
                detected[role] = normalized[normalized_candidate]
                break

    return detected


def parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    if re.fullmatch(r"\d+(\.\d+)?", text):
        try:
            number = float(text)
            if number > 1_000_000_000_000:
                number /= 1000.0
            return datetime.fromtimestamp(number, tz=timezone.utc).replace(tzinfo=None)
        except (OSError, ValueError):
            return None

    iso_text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_text)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        pass

    formats = [
        "%a %b %d %H:%M:%S %z %Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except ValueError:
            continue
    return None


def parse_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def is_valid_coordinate(lat: float | None, lon: float | None) -> bool:
    return lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius_km * math.asin(math.sqrt(a))


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


def read_records(input_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    suffix = input_path.suffix.lower()
    if suffix in {".jsonl", ".ndjson"}:
        records = []
        with input_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        fieldnames = sorted({key for record in records for key in record.keys()})
        return records, fieldnames
    if suffix == ".json":
        data = json.loads(input_path.read_text(encoding="utf-8"))
        records = data if isinstance(data, list) else data.get("records", [])
        fieldnames = sorted({key for record in records for key in record.keys()})
        return records, fieldnames

    delimiter = "\t" if suffix == ".tsv" else ","
    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        if suffix not in {".csv", ".tsv"}:
            try:
                delimiter = csv.Sniffer().sniff(sample).delimiter
            except csv.Error:
                delimiter = ","
        reader = csv.DictReader(handle, delimiter=delimiter)
        records = list(reader)
        return records, reader.fieldnames or []


def load_points(
    input_path: Path,
    user_col: str | None = None,
    trajectory_col: str | None = None,
    time_col: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
    geohash_precision: int = 6,
) -> tuple[list[Point], Stats, dict[str, str | None]]:
    records, fieldnames = read_records(input_path)
    stats = Stats(raw_points=len(records))
    columns = detect_columns(
        fieldnames,
        {
            "user": user_col,
            "trajectory": trajectory_col,
            "timestamp": time_col,
            "lat": lat_col,
            "lon": lon_col,
        },
    )

    required = ["user", "timestamp", "lat", "lon"]
    missing = [role for role in required if not columns.get(role)]
    if missing:
        stats.invalid_schema = len(records)
        stats.warnings.append(f"Missing required columns: {', '.join(missing)}")
        return [], stats, columns

    points: list[Point] = []
    for record in records:
        user_id = str(record.get(columns["user"], "")).strip()
        timestamp = parse_timestamp(record.get(columns["timestamp"]))
        lat = parse_float(record.get(columns["lat"]))
        lon = parse_float(record.get(columns["lon"]))
        trajectory_value = record.get(columns["trajectory"]) if columns.get("trajectory") else None
        trajectory_id = str(trajectory_value).strip() if trajectory_value not in (None, "") else user_id

        if not user_id:
            stats.invalid_schema += 1
            continue
        if timestamp is None:
            stats.invalid_time += 1
            continue
        if not is_valid_coordinate(lat, lon):
            stats.invalid_coordinate += 1
            continue

        used_cols = {columns["user"], columns["trajectory"], columns["timestamp"], columns["lat"], columns["lon"]}
        attributes = {
            key: value
            for key, value in record.items()
            if key not in used_cols and value not in (None, "")
        }
        point = Point(
            user_id=user_id,
            trajectory_id=trajectory_id,
            timestamp=timestamp,
            lat=float(lat),
            lon=float(lon),
            attributes=attributes,
            geohash=encode_geohash(float(lat), float(lon), geohash_precision),
        )
        points.append(point)

    points.sort(key=lambda p: (p.user_id, p.trajectory_id, p.timestamp, p.lat, p.lon))
    return points, stats, columns


def clean_points(points: list[Point], stats: Stats, max_speed_kmh: float, geohash_precision: int) -> list[Point]:
    deduped: list[Point] = []
    seen = set()
    for point in points:
        key = (point.user_id, point.trajectory_id, point.timestamp.isoformat(), round(point.lat, 7), round(point.lon, 7))
        if key in seen:
            stats.duplicate_points += 1
            continue
        seen.add(key)
        deduped.append(point)

    grouped: dict[tuple[str, str], list[Point]] = defaultdict(list)
    for point in deduped:
        grouped[(point.user_id, point.trajectory_id)].append(point)

    cleaned: list[Point] = []
    for _, group in grouped.items():
        previous: Point | None = None
        for point in group:
            keep = True
            if previous is not None:
                elapsed_hours = (point.timestamp - previous.timestamp).total_seconds() / 3600
                if elapsed_hours > 0:
                    speed = haversine_km(previous.lat, previous.lon, point.lat, point.lon) / elapsed_hours
                    if speed > max_speed_kmh:
                        stats.speed_filtered_points += 1
                        keep = False
            if keep:
                point.geohash = encode_geohash(point.lat, point.lon, geohash_precision)
                cleaned.append(point)
                previous = point

    cleaned.sort(key=lambda p: (p.user_id, p.trajectory_id, p.timestamp))
    for idx, point in enumerate(cleaned, start=1):
        point.point_id = f"pt_{idx:06d}"
        point.sequence = idx
    stats.cleaned_points = len(cleaned)
    stats.users = len({p.user_id for p in cleaned})
    if stats.cleaned_points == 0:
        stats.warnings.append("No cleaned points were produced.")
    return cleaned


def extract_staypoints(
    points: list[Point],
    stay_radius_m: float,
    stay_min_minutes: float,
    geohash_precision: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Point]] = defaultdict(list)
    for point in points:
        grouped[(point.user_id, point.trajectory_id)].append(point)

    staypoints: list[dict[str, Any]] = []
    stay_id = 1
    radius_km = stay_radius_m / 1000

    for (user_id, trajectory_id), group in grouped.items():
        i = 0
        while i < len(group):
            j = i + 1
            found = False
            while j < len(group):
                dist = haversine_km(group[i].lat, group[i].lon, group[j].lat, group[j].lon)
                if dist > radius_km:
                    dwell = (group[j - 1].timestamp - group[i].timestamp).total_seconds() / 60
                    if dwell >= stay_min_minutes:
                        window = group[i:j]
                        lat = sum(p.lat for p in window) / len(window)
                        lon = sum(p.lon for p in window) / len(window)
                        staypoints.append({
                            "staypoint_id": f"sp_{stay_id:06d}",
                            "user_id": user_id,
                            "trajectory_id": trajectory_id,
                            "start_time": group[i].timestamp.isoformat(),
                            "end_time": group[j - 1].timestamp.isoformat(),
                            "duration_minutes": round(dwell, 3),
                            "centroid_lat": lat,
                            "centroid_lon": lon,
                            "geohash": encode_geohash(lat, lon, geohash_precision),
                            "point_count": len(window),
                        })
                        stay_id += 1
                        i = j
                        found = True
                    break
                j += 1
            if not found:
                i += 1

    return staypoints


def segment_trips(points: list[Point], trip_gap_minutes: float, min_trip_points: int, geohash_precision: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Point]] = defaultdict(list)
    for point in points:
        grouped[(point.user_id, point.trajectory_id)].append(point)

    trips: list[dict[str, Any]] = []
    trip_id = 1

    def emit_trip(segment: list[Point]) -> None:
        nonlocal trip_id
        if len(segment) < min_trip_points:
            return
        distance = sum(
            haversine_km(a.lat, a.lon, b.lat, b.lon)
            for a, b in zip(segment, segment[1:])
        )
        start = segment[0]
        end = segment[-1]
        duration = (end.timestamp - start.timestamp).total_seconds() / 60
        trips.append({
            "trip_id": f"trip_{trip_id:06d}",
            "user_id": start.user_id,
            "trajectory_id": start.trajectory_id,
            "start_time": start.timestamp.isoformat(),
            "end_time": end.timestamp.isoformat(),
            "duration_minutes": round(duration, 3),
            "point_count": len(segment),
            "start_lat": start.lat,
            "start_lon": start.lon,
            "end_lat": end.lat,
            "end_lon": end.lon,
            "start_geohash": encode_geohash(start.lat, start.lon, geohash_precision),
            "end_geohash": encode_geohash(end.lat, end.lon, geohash_precision),
            "distance_km": round(distance, 6),
        })
        trip_id += 1

    for _, group in grouped.items():
        segment: list[Point] = []
        previous: Point | None = None
        for point in group:
            if previous is not None:
                gap = (point.timestamp - previous.timestamp).total_seconds() / 60
                if gap > trip_gap_minutes:
                    emit_trip(segment)
                    segment = []
            segment.append(point)
            previous = point
        emit_trip(segment)

    return trips


def point_to_record(point: Point) -> dict[str, Any]:
    return {
        "point_id": point.point_id,
        "user_id": point.user_id,
        "trajectory_id": point.trajectory_id,
        "timestamp": point.timestamp.isoformat(),
        "lat": point.lat,
        "lon": point.lon,
        "geohash": point.geohash,
        "sequence": point.sequence,
        "attributes": point.attributes,
    }


def write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count


def build_summary(
    stats: Stats,
    input_path: Path,
    output_dir: Path,
    columns: dict[str, str | None],
    parameters: dict[str, Any],
    staypoints: list[dict[str, Any]],
    trips: list[dict[str, Any]],
) -> dict[str, Any]:
    stats.staypoints = len(staypoints)
    stats.trips = len(trips)
    if stats.staypoints == 0:
        stats.warnings.append("No staypoints were detected with the current thresholds.")
    if stats.trips == 0:
        stats.warnings.append("No trips were emitted with the current thresholds.")

    return {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "detected_columns": columns,
        "parameters": parameters,
        "counts": {
            "raw_points": stats.raw_points,
            "cleaned_points": stats.cleaned_points,
            "users": stats.users,
            "staypoints": stats.staypoints,
            "trips": stats.trips,
        },
        "dropped": {
            "invalid_schema": stats.invalid_schema,
            "invalid_time": stats.invalid_time,
            "invalid_coordinate": stats.invalid_coordinate,
            "duplicate_points": stats.duplicate_points,
            "speed_filtered_points": stats.speed_filtered_points,
        },
        "outputs": {
            "cleaned_points": str(output_dir / "cleaned_points.jsonl"),
            "staypoints": str(output_dir / "staypoints.jsonl"),
            "trips": str(output_dir / "trips.jsonl"),
            "summary": str(output_dir / "summary.json"),
        },
        "warnings": sorted(set(stats.warnings)),
    }

