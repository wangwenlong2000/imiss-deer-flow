from __future__ import annotations

from src.skills.base import SkillContext
from src.skills.registry import SkillRegistry


def run_camera_pipeline(camera_config: dict, context: SkillContext, registry: SkillRegistry) -> dict:
    context.registry = registry
    raw_video = registry.get("video-stream-ingestion").run(camera_config, context)
    if raw_video["status"] != "success":
        return raw_video

    frames = registry.get("frame-sampling").run({**raw_video["data"], **camera_config}, context)
    if frames["status"] != "success":
        return frames
    health = registry.get("camera-health-check").run({"camera_id": camera_config["camera_id"], **frames["data"]}, context)
    if health["status"] != "success":
        return health
    if health["data"]["health_status"] == "unavailable":
        return health

    detections = registry.get("object-detection").run({**frames["data"], "labels": camera_config.get("labels", ["car"])}, context)
    if detections["status"] != "success":
        return detections
    tracks = registry.get("object-tracking").run({"camera_id": camera_config["camera_id"], **detections["data"]}, context)
    if tracks["status"] != "success":
        return tracks
    roi_matches = registry.get("roi-mapping").run(
        {"camera_id": camera_config["camera_id"], "tracks": tracks["data"]["tracks"]},
        context,
    )
    if roi_matches["status"] != "success":
        return roi_matches
    event_result = registry.get("event-rule-engine").run(
        {
            "camera_id": camera_config["camera_id"],
            "frames": frames["data"]["frames"],
            "detections": detections["data"]["detections"],
            "tracks": tracks["data"]["tracks"],
            "roi_matches": roi_matches["data"]["matches"],
            "camera_health": health["data"],
            "templates": context.config.get("enabled_event_templates"),
        },
        context,
    )
    merged = registry.get("duplicate-event-merge").run({"events": event_result["data"]["events"]}, context)

    outputs = []
    for event in merged["data"]["events"]:
        snapshot = registry.get("evidence-snapshot").run({"event": event, "frames": frames["data"]["frames"]}, context)
        clip = registry.get("video-segment-extraction").run(
            {
                "event_id": event["event_id"],
                "camera_id": camera_config["camera_id"],
                "raw_segment_uri": raw_video["data"]["raw_segment_uri"],
                "event_time": event.get("end_time") or event.get("start_time") or _event_frame_time(event, frames["data"]["frames"], raw_video["data"]["started_at"]),
                "event_elapsed_seconds": _event_elapsed_seconds(event, frames["data"]["frames"]),
            },
            context,
        )
        review = registry.get("human-review-routing").run({"event": event, "camera_health": health["data"]}, context)
        outputs.append({"event": event, "snapshot": snapshot["data"], "clip": clip["data"], "review": review["data"]})

    return {
        "status": "success",
        "camera_id": camera_config["camera_id"],
        "frames": frames["data"]["frames"],
        "health": health["data"],
        "detections": detections["data"]["detections"],
        "tracks": tracks["data"]["tracks"],
        "roi_matches": roi_matches["data"]["matches"],
        "events": outputs,
    }


def _event_elapsed_seconds(event: dict, frames: list[dict]) -> float:
    selected = _selected_event_frame(event, frames)
    return float(selected.get("source_elapsed_seconds", 0))


def _event_frame_time(event: dict, frames: list[dict], fallback: str) -> str:
    selected = _selected_event_frame(event, frames)
    return selected.get("timestamp") or fallback


def _selected_event_frame(event: dict, frames: list[dict]) -> dict:
    evidence_ids = set(event.get("evidence_frame_ids", []))
    selected = None
    for frame in frames:
        if frame.get("frame_id") in evidence_ids:
            selected = frame
    if selected is None:
        selected = frames[0] if frames else {}
    return selected
