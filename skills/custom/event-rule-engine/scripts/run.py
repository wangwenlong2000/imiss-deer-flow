
from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from hashlib import sha1, sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

try:
    import yaml
except Exception:
    yaml = None

VERSION = "1.0.0"

def load_structured(path: str | None) -> Any:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    if source.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError("PyYAML is required to read YAML files")
        return yaml.safe_load(text) or {}
    return json.loads(text) if text.strip() else {}

def load_config(path: str | None) -> dict[str, Any]:
    data = load_structured(path)
    return data if isinstance(data, dict) else {}

def load_list(path: str | None, key: str) -> list[dict[str, Any]]:
    data = load_structured(path)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        value = data.get(key) or data.get("data", {}).get(key, [])
        return value if isinstance(value, list) else []
    return []

def load_dict(path: str | None, key: str | None = None) -> dict[str, Any]:
    data = load_structured(path)
    if not isinstance(data, dict):
        return {}
    if key and key in data and isinstance(data[key], dict):
        return data[key]
    if key and isinstance(data.get("data"), dict) and isinstance(data["data"].get(key), dict):
        return data["data"][key]
    return data

def parse_json_arg(value: str | None, default: Any) -> Any:
    return json.loads(value) if value else default

def success(skill: str, data: dict[str, Any], confidence: float = 1.0) -> dict[str, Any]:
    return {"skill": skill, "version": VERSION, "status": "success", "confidence": round(float(confidence), 4), "data": data}

def failed(skill: str, code: str, message: str, retryable: bool = False, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"skill": skill, "version": VERSION, "status": "failed", "error_code": code, "message": message, "retryable": retryable, "detail": detail or {}}

def emit(result: dict[str, Any], output: str | None) -> int:
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result.get("status") == "success" else 1

def parse_time(value: str | None, fallback: datetime | None = None) -> datetime:
    if not value:
        return fallback or datetime.now().astimezone()
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def iso_add_seconds(value: str, seconds: int | float) -> str:
    return (parse_time(value) + timedelta(seconds=float(seconds))).isoformat()

def bbox_center(bbox: list[float]) -> tuple[float, float]:
    x1, y1, x2, y2 = bbox
    return ((x1 + x2) / 2, (y1 + y2) / 2)

def bbox_bottom_center(bbox: list[float]) -> tuple[float, float]:
    x1, _, x2, y2 = bbox
    return ((x1 + x2) / 2, y2)

def bbox_area(bbox: list[float]) -> float:
    x1, y1, x2, y2 = bbox
    return max(0.0, x2 - x1) * max(0.0, y2 - y1)

def bbox_overlap(a: list[float], b: list[float]) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    inter = bbox_area([max(ax1, bx1), max(ay1, by1), min(ax2, bx2), min(ay2, by2)])
    base = bbox_area(a)
    return 0.0 if base <= 0 else inter / base

def polygon_bounds(polygon: list[list[float]]) -> list[float]:
    xs = [p[0] for p in polygon]
    ys = [p[1] for p in polygon]
    return [min(xs), min(ys), max(xs), max(ys)] if xs and ys else [0, 0, 0, 0]

def polygon_area(polygon: list[list[float]]) -> float:
    if len(polygon) < 3:
        return 0.0
    total = 0.0
    for i, (x1, y1) in enumerate(polygon):
        x2, y2 = polygon[(i + 1) % len(polygon)]
        total += x1 * y2 - x2 * y1
    return abs(total) / 2

def point_in_polygon(point: tuple[float, float], polygon: list[list[float]]) -> bool:
    x, y = point
    inside = False
    j = len(polygon) - 1
    for i, (xi, yi) in enumerate(polygon):
        xj, yj = polygon[j]
        if (yi > y) != (yj > y):
            x_at_y = (xj - xi) * (y - yi) / ((yj - yi) or 1e-9) + xi
            if x < x_at_y:
                inside = not inside
        j = i
    return inside

def distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])

def subject_id(item: dict[str, Any]) -> str:
    return item.get("track_id") or item.get("object_id") or item.get("id") or "unknown"

def iter_detection_objects(detections: list[dict[str, Any]]):
    for detection in detections:
        for obj in detection.get("objects", []):
            enriched = dict(obj)
            enriched.setdefault("frame_id", detection.get("frame_id"))
            enriched.setdefault("timestamp", detection.get("timestamp"))
            yield enriched

def method_confidence(result: dict[str, Any]) -> float:
    items = result.get("matches") or result.get("clusters") or result.get("groups") or result.get("anomalies") or []
    return sum(float(i.get("confidence", 1.0)) for i in items) / len(items) if items else 0.0

def subject_ids(result: dict[str, Any]) -> list[str]:
    ids = []
    for key in ("matches", "clusters", "groups"):
        for item in result.get(key, []):
            ids.extend(item.get("object_ids", []))
            if item.get("subject_id"):
                ids.append(item["subject_id"])
    return ids

def evidence_ids(result: dict[str, Any]) -> list[str]:
    ids = []
    for item in result.get("matches", []):
        ids.extend(item.get("evidence_frame_ids", []))
    return ids

def build_event(template_name: str, template: dict[str, Any], method_results: list[dict[str, Any]], camera_id: str | None) -> dict[str, Any] | None:
    if not method_results:
        return None
    hits = [r for r in method_results if r.get("matched")]
    join_policy = template.get("join_policy", "all_required")
    if join_policy == "all_required" and len(hits) != len(method_results):
        return None
    if join_policy == "any_required" and not hits:
        return None
    scores = [method_confidence(r) for r in method_results]
    score = sum(scores) / len(scores) if scores else 0.0
    if join_policy == "weighted_score":
        weights = template.get("score_policy", {}).get("weights", {})
        total_weight = 0.0
        total = 0.0
        for result in method_results:
            weight = float(weights.get(result.get("method"), 1.0))
            total_weight += weight
            total += method_confidence(result) * weight
        score = total / total_weight if total_weight else 0.0
    if score < float(template.get("score_policy", {}).get("min_score", 0)):
        return None
    related = sorted({sid for r in method_results for sid in subject_ids(r) if sid})
    starts = [i.get("start_time") for r in method_results for i in r.get("matches", []) if i.get("start_time")]
    ends = [i.get("end_time") for r in method_results for i in r.get("matches", []) if i.get("end_time")]
    roi_ids = [i.get("roi_id") for r in method_results for i in r.get("matches", []) + r.get("clusters", []) + r.get("groups", []) if i.get("roi_id")]
    stable_key = "|".join([str(camera_id), template_name, ",".join(related), ",".join(roi_ids)])
    suffix = sha1(stable_key.encode("utf-8")).hexdigest()[:10]
    rules = []
    for method in template.get("methods", []):
        if method.get("target_labels"):
            rules.append("label:" + ",".join(method["target_labels"]))
        if method.get("roi_types"):
            rules.append("roi:" + ",".join(method["roi_types"]))
        if method.get("min_duration_seconds") is not None:
            rules.append(f"duration>={method['min_duration_seconds']}")
    rules.extend(f"method:{hit.get('method')}" for hit in hits)
    return {"event_id": f"EVT_{template_name}_{suffix}", "event_type": template.get("event_type", template_name), "camera_id": camera_id, "roi_id": roi_ids[0] if roi_ids else None, "start_time": min(starts) if starts else None, "end_time": max(ends) if ends else None, "confidence": round(score, 4), "severity": template.get("severity", "medium"), "related_tracks": related, "reason": f"{template_name} matched by " + ", ".join(h.get("method", "unknown") for h in hits), "method_hits": [h.get("method") for h in hits], "rule_hits": rules, "evidence_frame_ids": sorted({fid for r in method_results for fid in evidence_ids(r)}), "status": "candidate", "created_at": utc_now_iso()}

SKILL = "event-rule-engine"

def spatial_method(input_data, method):
    labels = set(method.get("target_labels", [])); roi_types = set(method.get("roi_types", [])); excluded = set(method.get("excluded_roi_types", [])); min_overlap = float(method.get("min_overlap_ratio", 0)); min_confidence = float(method.get("min_confidence", 0)); tracks = {t.get("track_id"): t for t in input_data.get("tracks", [])}; matches = []
    for match in input_data.get("roi_matches", []):
        if not match.get("matched"): continue
        track = tracks.get(match.get("object_id"), {}); label = track.get("label") or match.get("label")
        if labels and label not in labels: continue
        if roi_types and match.get("roi_type") not in roi_types: continue
        if match.get("roi_type") in excluded: continue
        if float(match.get("overlap_ratio", 0)) < min_overlap: continue
        conf = min(float(track.get("confidence", match.get("confidence", 1.0))), float(match.get("confidence", 1.0)))
        if conf < min_confidence: continue
        matches.append({"subject_id": match.get("object_id"), "subject_type": "track" if match.get("object_id") in tracks else "object", "label": label, "roi_id": match.get("roi_id"), "roi_type": match.get("roi_type"), "overlap_ratio": match.get("overlap_ratio", 0), "confidence": round(conf, 4), "evidence_frame_ids": track.get("evidence_frame_ids", [])})
    conf = sum(float(m["confidence"]) for m in matches) / len(matches) if matches else 0
    return {"method": "spatial_occupancy", "matched": bool(matches), "matches": matches, "confidence": conf}

def temporal_method(input_data, method):
    labels = set(method.get("target_labels", [])); states = set(method.get("movement_states", [])); min_duration = int(method.get("min_duration_seconds", 0)); matches = []
    for track in input_data.get("tracks", []):
        if labels and track.get("label") not in labels: continue
        if states and track.get("movement_state") not in states: continue
        if int(track.get("duration_seconds", 0)) < min_duration: continue
        matches.append({"subject_id": track.get("track_id"), "duration_seconds": track.get("duration_seconds", 0), "movement_state": track.get("movement_state"), "confidence": track.get("confidence", 1.0), "start_time": track.get("start_time"), "end_time": track.get("end_time"), "evidence_frame_ids": track.get("evidence_frame_ids", [])})
    conf = sum(float(m["confidence"]) for m in matches) / len(matches) if matches else 0
    return {"method": "temporal_persistence", "matched": bool(matches), "matches": matches, "confidence": conf}

def density_method(input_data, method):
    labels = set(method.get("target_labels", [])); roi_types = set(method.get("roi_types", [])); min_count = int(method.get("min_count", 1)); min_density = float(method.get("min_density", 0)); min_duration = int(method.get("min_duration_seconds", 0)); tracks = {t.get("track_id"): t for t in input_data.get("tracks", [])}; by_roi = defaultdict(list)
    for match in input_data.get("roi_matches", []):
        if not match.get("matched") or (roi_types and match.get("roi_type") not in roi_types): continue
        track = tracks.get(match.get("object_id"))
        if not track or (labels and track.get("label") not in labels) or int(track.get("duration_seconds", 0)) < min_duration: continue
        by_roi[match.get("roi_id")].append(track)
    areas = {r.get("id"): polygon_area(r.get("polygon", [])) for r in input_data.get("rois", [])}; clusters = []
    for roi_id, items in by_roi.items():
        count = len(items); dens = count / max(areas.get(roi_id, 1.0), 1.0) * 10000
        if count < min_count or dens < min_density: continue
        confs = [float(i.get("confidence", 1.0)) for i in items]
        clusters.append({"cluster_id": f"cluster_{len(clusters)+1:03d}", "count": count, "density": round(dens, 4), "roi_id": roi_id, "duration_seconds": min(int(i.get("duration_seconds", 0)) for i in items), "confidence": round(sum(confs)/max(len(confs),1), 4)})
    conf = sum(c["confidence"] for c in clusters) / len(clusters) if clusters else 0
    return {"method": "density_aggregation", "matched": bool(clusters), "clusters": clusters, "confidence": conf}

def composition_method(input_data, method):
    required = method.get("required", []); group_by = method.get("group_by", "frame"); max_distance = float(method.get("max_distance_pixels", 150)); objects = list(iter_detection_objects(input_data.get("detections", []))) + input_data.get("tracks", []); roi_by = {m.get("object_id"): m.get("roi_id") for m in input_data.get("roi_matches", []) if m.get("matched")}; grouped = defaultdict(list)
    for obj in objects:
        key = roi_by.get(subject_id(obj), "all") if group_by == "roi" else obj.get("frame_id", "all")
        grouped[key].append(obj)
    groups = []
    for key, items in grouped.items():
        counts = Counter(i.get("label") for i in items)
        if any(sum(counts[label] for label in r.get("labels", [])) < int(r.get("min_count", 1)) for r in required): continue
        if method.get("spatial_relation") == "near":
            centers = [bbox_center(i.get("last_bbox") or i.get("bbox")) for i in items if i.get("last_bbox") or i.get("bbox")]
            if len(centers) >= 2 and not any(distance(a,b) <= max_distance for idx,a in enumerate(centers) for b in centers[idx+1:]): continue
        confs = [float(i.get("confidence", 1.0)) for i in items]
        groups.append({"group_id": f"grp_{len(groups)+1:03d}", "object_ids": [subject_id(i) for i in items], "labels": sorted({i.get("label") for i in items if i.get("label")}), "roi_id": key if group_by == "roi" else None, "confidence": round(sum(confs)/max(len(confs),1), 4)})
    conf = sum(g["confidence"] for g in groups) / len(groups) if groups else 0
    return {"method": "object_composition", "matched": bool(groups), "groups": groups, "confidence": conf}

def run_method(input_data, method):
    name = method.get("skill", "")
    if name == "spatial-occupancy-event": return spatial_method(input_data, method)
    if name == "temporal-persistence-event": return temporal_method(input_data, method)
    if name == "density-aggregation-event": return density_method(input_data, method)
    if name == "object-composition-event": return composition_method(input_data, method)
    return {"method": name, "matched": False, "error": "unsupported_method"}

def main() -> int:
    p = argparse.ArgumentParser(description="Run configured event templates over prepared video analytics JSON.")
    p.add_argument("--frames-json"); p.add_argument("--detections-json"); p.add_argument("--tracks-json"); p.add_argument("--roi-matches-json"); p.add_argument("--rois-json"); p.add_argument("--camera-health-json")
    p.add_argument("--camera-id", default="CAM_DEERFLOW_001"); p.add_argument("--templates", help="Comma-separated template names"); p.add_argument("--config", required=True); p.add_argument("--output")
    args = p.parse_args(); config = load_config(args.config); templates = config.get("event_templates", {}); names = [x.strip() for x in args.templates.split(",")] if args.templates else config.get("enabled_event_templates") or list(templates)
    input_data = {"camera_id": args.camera_id, "frames": load_list(args.frames_json, "frames"), "detections": load_list(args.detections_json, "detections"), "tracks": load_list(args.tracks_json, "tracks"), "roi_matches": load_list(args.roi_matches_json, "matches"), "rois": load_list(args.rois_json, "rois"), "camera_health": load_dict(args.camera_health_json)}
    events = []; method_outputs = {}
    for template_name in names:
        template = templates.get(template_name)
        if not template: continue
        results = [run_method(input_data, method) for method in template.get("methods", [])]
        method_outputs[template_name] = results
        event = build_event(template_name, template, results, args.camera_id)
        if event:
            if input_data.get("camera_health", {}).get("health_status") == "degraded":
                event["confidence"] = round(event["confidence"] * 0.85, 4); event["reason"] += "; camera health degraded"
            events.append(event)
    conf = sum(e["confidence"] for e in events) / len(events) if events else 0
    return emit(success(SKILL, {"events": events, "method_outputs": method_outputs}, conf), args.output)

if __name__ == "__main__":
    raise SystemExit(main())
