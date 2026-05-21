from __future__ import annotations

from src.skills.base import BaseSkill


class SkillRegistry:
    def __init__(self) -> None:
        self._skills: dict[str, BaseSkill] = {}

    def register(self, skill: BaseSkill) -> None:
        self._skills[skill.name] = skill

    def get(self, name: str) -> BaseSkill:
        if name not in self._skills:
            raise KeyError(f"Skill not registered: {name}")
        return self._skills[name]

    def list(self) -> list[str]:
        return sorted(self._skills.keys())


def build_default_registry() -> SkillRegistry:
    from src.skills.event_detection.density_aggregation_event import DensityAggregationEventSkill
    from src.skills.event_detection.event_rule_engine import EventRuleEngineSkill
    from src.skills.event_detection.event_template_mapping import EventTemplateMappingSkill
    from src.skills.event_detection.object_composition_event import ObjectCompositionEventSkill
    from src.skills.event_detection.spatial_occupancy_event import SpatialOccupancyEventSkill
    from src.skills.event_detection.temporal_persistence_event import TemporalPersistenceEventSkill
    from src.skills.evidence_processing.evidence_snapshot import EvidenceSnapshotSkill
    from src.skills.evidence_processing.privacy_masking import PrivacyMaskingSkill
    from src.skills.quality_review_metrics.duplicate_event_merge import DuplicateEventMergeSkill
    from src.skills.quality_review_metrics.human_review_routing import HumanReviewRoutingSkill
    from src.skills.video_data.camera_health_check import CameraHealthCheckSkill
    from src.skills.video_data.frame_sampling import FrameSamplingSkill
    from src.skills.video_data.video_segment_extraction import VideoSegmentExtractionSkill
    from src.skills.video_data.video_stream_ingestion import VideoStreamIngestionSkill
    from src.skills.vision_processing.object_detection import ObjectDetectionSkill
    from src.skills.vision_processing.object_tracking import ObjectTrackingSkill
    from src.skills.vision_processing.roi_mapping import ROIMappingSkill

    registry = SkillRegistry()
    for skill in [
        VideoStreamIngestionSkill(),
        FrameSamplingSkill(),
        CameraHealthCheckSkill(),
        VideoSegmentExtractionSkill(),
        ObjectDetectionSkill(),
        ObjectTrackingSkill(),
        ROIMappingSkill(),
        EventRuleEngineSkill(),
        SpatialOccupancyEventSkill(),
        TemporalPersistenceEventSkill(),
        ObjectCompositionEventSkill(),
        DensityAggregationEventSkill(),
        EventTemplateMappingSkill(),
        DuplicateEventMergeSkill(),
        EvidenceSnapshotSkill(),
        PrivacyMaskingSkill(),
        HumanReviewRoutingSkill(),
    ]:
        registry.register(skill)
    return registry

