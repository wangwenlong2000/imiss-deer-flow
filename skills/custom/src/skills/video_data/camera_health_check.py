from __future__ import annotations

from src.skills.base import BaseSkill, SkillContext


class CameraHealthCheckSkill(BaseSkill):
    name = "camera-health-check"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        camera_id = input_data.get("camera_id")
        frames = input_data.get("frames", [])
        forced = input_data.get("health_status") or context.config.get("camera_health", {}).get(camera_id)

        if not frames and forced != "ok":
            return self.success(
                {
                    "camera_id": camera_id,
                    "health_status": "unavailable",
                    "health_score": 0,
                    "issues": [{"type": "offline", "confidence": 1.0, "reason": "no frames available"}],
                },
                confidence=1.0,
            )

        if forced and forced != "ok":
            score = 55 if forced == "degraded" else 0
            return self.success(
                {
                    "camera_id": camera_id,
                    "health_status": forced,
                    "health_score": score,
                    "issues": [{"type": forced, "confidence": 0.9, "reason": "configured health override"}],
                },
                confidence=0.9,
            )

        return self.success({"camera_id": camera_id, "health_status": "ok", "health_score": 100, "issues": []})

