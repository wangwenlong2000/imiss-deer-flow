from __future__ import annotations

from src.skills.base import BaseSkill, SkillContext
from src.skills.utils import bbox_bottom_center, bbox_center, bbox_iou_like_overlap, point_in_polygon, polygon_bounds, subject_id


class ROIMappingSkill(BaseSkill):
    name = "roi-mapping"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        camera_id = input_data.get("camera_id")
        camera = context.config.get("cameras", {}).get(camera_id, {})
        rois = input_data.get("rois") or camera.get("rois", [])
        roi_types = set(input_data.get("roi_types") or [roi.get("type") for roi in rois])
        objects = input_data.get("objects") or input_data.get("tracks") or []
        strategy = input_data.get("position_strategy", "bottom_center")
        matches = []

        for obj in objects:
            bbox = obj.get("last_bbox") or obj.get("bbox")
            if not bbox:
                continue
            for roi in rois:
                if roi.get("type") not in roi_types:
                    continue
                polygon = roi.get("polygon", [])
                point = bbox_center(bbox) if strategy == "center_point" else bbox_bottom_center(bbox)
                if strategy == "bbox_overlap":
                    overlap = bbox_iou_like_overlap(bbox, polygon_bounds(polygon))
                    matched = overlap > 0
                else:
                    matched = point_in_polygon(point, polygon)
                    overlap = 1.0 if matched else 0.0
                matches.append(
                    {
                        "object_id": subject_id(obj),
                        "roi_id": roi.get("id"),
                        "roi_type": roi.get("type"),
                        "overlap_ratio": round(overlap, 4),
                        "matched": matched,
                        "confidence": obj.get("confidence", 1.0),
                    }
                )
        return self.success({"matches": matches})

