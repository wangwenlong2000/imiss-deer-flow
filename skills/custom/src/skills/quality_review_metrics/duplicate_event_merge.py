from __future__ import annotations

from src.skills.base import BaseSkill, SkillContext


class DuplicateEventMergeSkill(BaseSkill):
    name = "duplicate-event-merge"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        merged: dict[tuple, dict] = {}
        duplicates = []
        for event in input_data.get("events", []):
            key = (
                event.get("camera_id"),
                event.get("event_type"),
                event.get("roi_id"),
                tuple(sorted(event.get("related_tracks", []))),
            )
            if key not in merged:
                merged[key] = dict(event)
                merged[key]["merged_event_ids"] = []
                continue
            current = merged[key]
            current["confidence"] = max(float(current.get("confidence", 0)), float(event.get("confidence", 0)))
            current["start_time"] = min(filter(None, [current.get("start_time"), event.get("start_time")]), default=None)
            current["end_time"] = max(filter(None, [current.get("end_time"), event.get("end_time")]), default=None)
            current["merged_event_ids"].append(event.get("event_id"))
            duplicates.append({"main_event_id": current.get("event_id"), "merged_event_id": event.get("event_id")})
        return self.success({"events": list(merged.values()), "duplicates": duplicates})

