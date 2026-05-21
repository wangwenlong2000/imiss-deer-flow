from __future__ import annotations

from collections import Counter, defaultdict

from src.skills.base import BaseSkill, SkillContext
from src.skills.utils import bbox_center, distance, iter_detection_objects, subject_id


class ObjectCompositionEventSkill(BaseSkill):
    name = "object-composition-event"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        config = input_data.get("method_config", input_data)
        required = config.get("required", [])
        max_distance = float(config.get("max_distance_pixels", 150))
        group_by = config.get("group_by", "frame")
        objects = list(iter_detection_objects(input_data.get("detections", []))) + input_data.get("tracks", [])

        roi_by_object = {
            match.get("object_id"): match.get("roi_id")
            for match in input_data.get("roi_matches", [])
            if match.get("matched")
        }
        grouped: dict[str, list[dict]] = defaultdict(list)
        for obj in objects:
            key = roi_by_object.get(subject_id(obj), "all") if group_by == "roi" else obj.get("frame_id", "all")
            grouped[key].append(obj)

        groups = []
        for key, items in grouped.items():
            if not self._required_met(items, required):
                continue
            if config.get("spatial_relation") == "near" and not self._has_near_pair(items, max_distance):
                continue
            confidences = [float(item.get("confidence", 1.0)) for item in items]
            groups.append(
                {
                    "group_id": f"grp_{len(groups) + 1:03d}",
                    "object_ids": [subject_id(item) for item in items],
                    "labels": sorted({item.get("label") for item in items if item.get("label")}),
                    "roi_id": key if group_by == "roi" else None,
                    "confidence": round(sum(confidences) / max(len(confidences), 1), 4),
                }
            )

        confidence = sum(item["confidence"] for item in groups) / len(groups) if groups else 0
        return self.success({"method": "object_composition", "matched": bool(groups), "groups": groups}, confidence)

    def _required_met(self, items: list[dict], required: list[dict]) -> bool:
        counts = Counter(item.get("label") for item in items)
        for rule in required:
            total = sum(counts[label] for label in rule.get("labels", []))
            if total < int(rule.get("min_count", 1)):
                return False
        return True

    def _has_near_pair(self, items: list[dict], max_distance: float) -> bool:
        if len(items) < 2:
            return True
        centers = []
        for item in items:
            bbox = item.get("last_bbox") or item.get("bbox")
            if bbox:
                centers.append(bbox_center(bbox))
        return any(distance(a, b) <= max_distance for i, a in enumerate(centers) for b in centers[i + 1 :])

