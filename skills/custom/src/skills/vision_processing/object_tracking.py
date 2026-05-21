from __future__ import annotations

from src.skills.base import BaseSkill, SkillContext
from src.skills.utils import bbox_center, distance, parse_time


class ObjectTrackingSkill(BaseSkill):
    name = "object-tracking"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        if "tracks" in input_data:
            return self.success({"tracks": input_data["tracks"]})

        camera_id = input_data.get("camera_id")
        detections = sorted(input_data.get("detections", []), key=lambda item: item.get("timestamp") or "")
        max_distance = context.config.get("tracking", {}).get("association_distance_pixels", 80)
        active: list[dict] = []

        for detection in detections:
            for obj in detection.get("objects", []):
                center = bbox_center(obj["bbox"])
                candidates = [
                    track
                    for track in active
                    if track["label"] == obj.get("label")
                    and distance(track["trajectory"][-1], center) <= max_distance
                ]
                if candidates:
                    track = min(candidates, key=lambda item: distance(item["trajectory"][-1], center))
                else:
                    track = {
                        "track_id": f"track_{len(active) + 1:04d}",
                        "camera_id": camera_id,
                        "label": obj.get("label"),
                        "start_time": detection.get("timestamp"),
                        "trajectory": [],
                        "confidences": [],
                        "evidence_frame_ids": [],
                    }
                    active.append(track)
                track["end_time"] = detection.get("timestamp")
                track["last_bbox"] = obj.get("bbox")
                track["trajectory"].append(center)
                track["confidences"].append(float(obj.get("confidence", 0)))
                if detection.get("frame_id"):
                    track["evidence_frame_ids"].append(detection["frame_id"])

        tracks = []
        for track in active:
            start = parse_time(track.get("start_time"))
            end = parse_time(track.get("end_time"), start)
            displacement = distance(track["trajectory"][0], track["trajectory"][-1]) if len(track["trajectory"]) > 1 else 0
            duration = max(0, int((end - start).total_seconds()))
            tracks.append(
                {
                    "track_id": track["track_id"],
                    "camera_id": track.get("camera_id"),
                    "label": track["label"],
                    "start_time": track.get("start_time"),
                    "end_time": track.get("end_time"),
                    "duration_seconds": duration,
                    "last_bbox": track.get("last_bbox"),
                    "trajectory": [[round(x, 2), round(y, 2)] for x, y in track["trajectory"]],
                    "movement_state": "stationary" if displacement <= context.config.get("tracking", {}).get("stationary_distance_pixels", 20) else "moving",
                    "confidence": round(sum(track["confidences"]) / max(len(track["confidences"]), 1), 4),
                    "evidence_frame_ids": track["evidence_frame_ids"],
                }
            )
        return self.success({"tracks": tracks})
