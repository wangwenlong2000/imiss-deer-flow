from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from uuid import uuid4

from src.skills.base import BaseSkill, SkillContext, utc_now_iso


class EvidenceSnapshotSkill(BaseSkill):
    name = "evidence-snapshot"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        event = input_data.get("event", input_data)
        event_id = event.get("event_id")
        if not event_id:
            return self.failed("MISSING_EVENT_ID", "event_id is required")

        frame = self._select_frame(event, input_data.get("frames", []))
        source_uri = frame.get("image_uri") or event.get("image_uri") or f"event://{event_id}"
        source_path = Path(source_uri.removeprefix("file://"))
        if source_path.exists():
            output_dir = Path(context.config.get("output_dir", "outputs")) / "evidence"
            output_dir.mkdir(parents=True, exist_ok=True)
            evidence_path = output_dir / f"{event_id}_snapshot.jpg"
            copyfile(source_path, evidence_path)
            evidence_uri = str(evidence_path)
        else:
            evidence_uri = context.storage.copy(source_uri, f"evidence/{event_id}_snapshot.jpg")

        privacy_masked = False
        if context.registry:
            masked = context.registry.get("privacy-masking").run({"uri": evidence_uri}, context)
            if masked["status"] == "success":
                evidence_uri = masked["data"]["uri"]
                privacy_masked = masked["data"]["privacy_masked"]

        digest = sha256(context.storage.read_bytes(evidence_uri)).hexdigest()
        return self.success(
            {
                "evidence_id": f"EVD_{uuid4().hex[:8]}",
                "event_id": event_id,
                "type": "snapshot",
                "uri": evidence_uri,
                "hash": f"sha256:{digest}",
                "created_at": utc_now_iso(),
                "privacy_masked": privacy_masked,
            }
        )

    def _select_frame(self, event: dict, frames: list[dict]) -> dict:
        evidence_ids = set(event.get("evidence_frame_ids", []))
        for frame in frames:
            if frame.get("frame_id") in evidence_ids:
                return frame
        return frames[0] if frames else {}
