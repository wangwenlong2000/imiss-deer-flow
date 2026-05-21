from __future__ import annotations

from src.skills.base import BaseSkill, SkillContext


class SpatialOccupancyEventSkill(BaseSkill):
    name = "spatial-occupancy-event"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        config = input_data.get("method_config", input_data)
        labels = set(config.get("target_labels", []))
        roi_types = set(config.get("roi_types", []))
        excluded = set(config.get("excluded_roi_types", []))
        min_overlap = float(config.get("min_overlap_ratio", 0))
        min_confidence = float(config.get("min_confidence", 0))
        tracks = {track.get("track_id"): track for track in input_data.get("tracks", [])}
        matches = []

        for match in input_data.get("roi_matches", []):
            if not match.get("matched"):
                continue
            track = tracks.get(match.get("object_id"), {})
            label = track.get("label") or match.get("label")
            if labels and label not in labels:
                continue
            if roi_types and match.get("roi_type") not in roi_types:
                continue
            if match.get("roi_type") in excluded:
                continue
            if float(match.get("overlap_ratio", 0)) < min_overlap:
                continue
            confidence = min(float(track.get("confidence", match.get("confidence", 1.0))), float(match.get("confidence", 1.0)))
            if confidence < min_confidence:
                continue
            matches.append(
                {
                    "subject_id": match.get("object_id"),
                    "subject_type": "track" if match.get("object_id") in tracks else "object",
                    "label": label,
                    "roi_id": match.get("roi_id"),
                    "roi_type": match.get("roi_type"),
                    "overlap_ratio": match.get("overlap_ratio", 0),
                    "confidence": round(confidence, 4),
                    "evidence_frame_ids": track.get("evidence_frame_ids", []),
                }
            )

        confidence = sum(item["confidence"] for item in matches) / len(matches) if matches else 0
        return self.success({"method": "spatial_occupancy", "matched": bool(matches), "matches": matches}, confidence)

