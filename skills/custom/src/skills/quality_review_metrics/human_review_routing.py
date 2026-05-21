from __future__ import annotations

from src.skills.base import BaseSkill, SkillContext


class HumanReviewRoutingSkill(BaseSkill):
    name = "human-review-routing"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        event = input_data.get("event", input_data)
        templates = context.config.get("event_templates", {})
        template = templates.get(event.get("event_type"), {})
        threshold = float(input_data.get("review_threshold") or template.get("review_threshold", context.config.get("default_review_threshold", 0.85)))
        high_risk = set(context.config.get("high_risk_event_types", []))
        camera_health = input_data.get("camera_health", {})
        reasons = []

        if float(event.get("confidence", 0)) < threshold:
            reasons.append("confidence_below_threshold")
        if event.get("event_type") in high_risk:
            reasons.append("high_risk_event_type")
        if camera_health.get("health_status") == "degraded":
            reasons.append("camera_health_degraded")
        if event.get("affects_enforcement"):
            reasons.append("affects_enforcement")

        return self.success(
            {
                "event_id": event.get("event_id"),
                "review_required": bool(reasons),
                "reasons": reasons,
                "review_threshold": threshold,
                "queue": "manual_review" if reasons else "auto_pass",
            }
        )

