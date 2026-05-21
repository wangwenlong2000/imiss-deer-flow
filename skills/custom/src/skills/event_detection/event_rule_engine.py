from __future__ import annotations

from src.skills.base import BaseSkill, SkillContext


class EventRuleEngineSkill(BaseSkill):
    name = "event-rule-engine"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        if not context.registry:
            return self.failed("MISSING_REGISTRY", "context.registry is required")

        templates = context.config.get("event_templates", {})
        names = input_data.get("templates") or context.config.get("enabled_event_templates") or list(templates)
        events = []
        method_outputs = {}
        mapper = context.registry.get("event-template-mapping")

        for template_name in names:
            template = templates.get(template_name)
            if not template:
                continue
            results = []
            for method in template.get("methods", []):
                skill_name = method.get("skill")
                skill = context.registry.get(skill_name)
                result = skill.run(
                    {
                        "template_name": template_name,
                        "method_config": method,
                        "camera_id": input_data.get("camera_id"),
                        "frames": input_data.get("frames", []),
                        "detections": input_data.get("detections", []),
                        "tracks": input_data.get("tracks", []),
                        "roi_matches": input_data.get("roi_matches", []),
                        "segments": input_data.get("segments", []),
                        "rois": input_data.get("rois", []),
                    },
                    context,
                )
                if result["status"] != "success":
                    return result
                results.append(result["data"])
            method_outputs[template_name] = results
            mapped = mapper.run(
                {
                    "template_name": template_name,
                    "template": template,
                    "method_results": results,
                    "camera_id": input_data.get("camera_id"),
                },
                context,
            )
            if mapped["status"] == "success" and mapped["data"].get("matched"):
                event = {key: value for key, value in mapped["data"].items() if key != "matched"}
                if input_data.get("camera_health", {}).get("health_status") == "degraded":
                    event["confidence"] = round(event["confidence"] * 0.85, 4)
                    event["reason"] += "; camera health degraded"
                events.append(event)

        confidence = sum(event["confidence"] for event in events) / len(events) if events else 0
        return self.success({"events": events, "method_outputs": method_outputs}, confidence)

