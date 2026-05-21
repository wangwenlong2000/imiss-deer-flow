from __future__ import annotations

from src.skills.base import BaseSkill, SkillContext


class TemporalPersistenceEventSkill(BaseSkill):
    name = "temporal-persistence-event"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        config = input_data.get("method_config", input_data)
        labels = set(config.get("target_labels", []))
        states = set(config.get("movement_states", []))
        min_duration = int(config.get("min_duration_seconds", 0))
        matches = []

        for track in input_data.get("tracks", []):
            if labels and track.get("label") not in labels:
                continue
            if states and track.get("movement_state") not in states:
                continue
            if int(track.get("duration_seconds", 0)) < min_duration:
                continue
            matches.append(
                {
                    "subject_id": track.get("track_id"),
                    "duration_seconds": track.get("duration_seconds", 0),
                    "movement_state": track.get("movement_state"),
                    "confidence": track.get("confidence", 1.0),
                    "start_time": track.get("start_time"),
                    "end_time": track.get("end_time"),
                    "evidence_frame_ids": track.get("evidence_frame_ids", []),
                }
            )

        confidence = sum(float(item["confidence"]) for item in matches) / len(matches) if matches else 0
        return self.success({"method": "temporal_persistence", "matched": bool(matches), "matches": matches}, confidence)

