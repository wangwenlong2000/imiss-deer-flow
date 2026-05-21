from __future__ import annotations

from collections import defaultdict

from src.skills.base import BaseSkill, SkillContext
from src.skills.utils import polygon_area


class DensityAggregationEventSkill(BaseSkill):
    name = "density-aggregation-event"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        config = input_data.get("method_config", input_data)
        labels = set(config.get("target_labels", []))
        roi_types = set(config.get("roi_types", []))
        min_count = int(config.get("min_count", 1))
        min_density = float(config.get("min_density", 0))
        min_duration = int(config.get("min_duration_seconds", 0))
        tracks = {track.get("track_id"): track for track in input_data.get("tracks", [])}
        by_roi: dict[str, list[dict]] = defaultdict(list)

        for match in input_data.get("roi_matches", []):
            if not match.get("matched"):
                continue
            if roi_types and match.get("roi_type") not in roi_types:
                continue
            track = tracks.get(match.get("object_id"))
            if not track:
                continue
            if labels and track.get("label") not in labels:
                continue
            if int(track.get("duration_seconds", 0)) < min_duration:
                continue
            by_roi[match.get("roi_id")].append(track)

        roi_area = self._roi_areas(input_data, context)
        clusters = []
        for roi_id, items in by_roi.items():
            count = len(items)
            area = max(roi_area.get(roi_id, 1.0), 1.0)
            density = count / area * 10000
            if count < min_count or density < min_density:
                continue
            confidences = [float(item.get("confidence", 1.0)) for item in items]
            clusters.append(
                {
                    "cluster_id": f"cluster_{len(clusters) + 1:03d}",
                    "count": count,
                    "density": round(density, 4),
                    "roi_id": roi_id,
                    "duration_seconds": min(int(item.get("duration_seconds", 0)) for item in items),
                    "confidence": round(sum(confidences) / max(len(confidences), 1), 4),
                }
            )

        confidence = sum(item["confidence"] for item in clusters) / len(clusters) if clusters else 0
        return self.success({"method": "density_aggregation", "matched": bool(clusters), "clusters": clusters}, confidence)

    def _roi_areas(self, input_data: dict, context: SkillContext) -> dict[str, float]:
        rois = input_data.get("rois")
        if rois is None:
            camera_id = input_data.get("camera_id")
            rois = context.config.get("cameras", {}).get(camera_id, {}).get("rois", [])
        return {roi.get("id"): polygon_area(roi.get("polygon", [])) for roi in rois}

