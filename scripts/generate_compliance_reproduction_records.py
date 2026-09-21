#!/usr/bin/env python3
"""Generate one fresh audit + frontend/SSE reproduction bundle.

The bundle deliberately uses a synthetic type-1 output sample and a fixed
``public_release`` resolver.  It contains no real personal data and replaces
any reliance on historical audit IDs that are no longer available.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "backend/packages/harness"
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

from deerflow.compliance.actions import REFUSAL_TEXT  # noqa: E402
from deerflow.compliance.audit import Auditor  # noqa: E402
from deerflow.compliance.contract import DetectionUnit, TextItem  # noqa: E402
from deerflow.compliance.engine import ComplianceEngine  # noqa: E402
from deerflow.compliance.policy import load_policy_matrix  # noqa: E402
from deerflow.compliance.registry import build_registry  # noqa: E402

OUT = ROOT / "outputs/compliance-eval/reproduction-records"


class FixedPublicRelease:
    def resolve(self, _request):
        return "public_release"


def main() -> int:
    if OUT.exists():
        shutil.rmtree(OUT)
    audit_dir = OUT / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    auditor = Auditor(path=audit_dir, enabled=True, retain_days=90)
    registry = build_registry(config_path=ROOT / "config/compliance/detectors.yaml", strict=True)
    model = registry.get("model_tfidf_knn")
    if model is not None:
        model.enabled = False
    engine = ComplianceEngine(
        registry=registry,
        policy=load_policy_matrix(ROOT / "config/compliance/policy_matrix.yaml"),
        auditor=auditor,
        scene_resolver=FixedPublicRelease(),
    )
    message_id = "repro-output-type12-001"
    original = "测试输出：用户手机号 13800138000，位置 lat=31.230416, lon=121.473701。"
    unit = DetectionUnit(
        unit_id=message_id,
        gate="OutputGate",
        data_type="text",
        text_items=(TextItem(item_id="t-0001", text=original, source="output"),),
    )
    try:
        decision = engine.check(
            (unit,),
            gate="OutputGate",
            request_id="repro-output-request-001",
            thread_id="repro-thread-001",
            audit_clean=True,
            origin={"message_id": message_id, "streaming_mode": "compatible_retract"},
        )
        if not decision.audit_ref:
            raise RuntimeError("expected a fresh audit reference")
        audit_records = auditor.read_all()
        audit_record = next(item for item in audit_records if item["audit_ref"] == decision.audit_ref)
        replacement = REFUSAL_TEXT if "refuse" in decision.actions else original
        event = {
            "type": "compliance_retract",
            "message_id": message_id,
            "scene": decision.scene_key,
            "streaming_mode": "compatible_retract",
            "violation_types": sorted({hit.violation_type for hit in decision.hits}),
            "raw_detection_types": sorted({hit.violation_type for hit in (decision.raw_hits or decision.hits)}),
            "effective_violation_types": sorted({hit.violation_type for hit in decision.hits}),
            "downgraded_detection_types": sorted({hit.violation_type for hit in decision.downgraded_hits}),
            "action": list(decision.actions),
            "replacement": replacement,
            "notice": f"合规处置已执行，审计编号：{decision.audit_ref}。",
            "audit_ref": decision.audit_ref,
            "basis": list(decision.basis),
        }
        frontend_metadata = {
            "compliance": {
                "evaluated": True,
                "gate": decision.gate,
                "scene": decision.scene_key,
                "streaming_mode": "compatible_retract",
                "transient_exposure_possible": True,
                "actions": list(decision.actions),
                "violation_types": sorted({hit.violation_type for hit in decision.hits}),
                "raw_detection_types": sorted({hit.violation_type for hit in (decision.raw_hits or decision.hits)}),
                "effective_violation_types": sorted({hit.violation_type for hit in decision.hits}),
                "downgraded_detection_types": sorted({hit.violation_type for hit in decision.downgraded_hits}),
                "audit_ref": decision.audit_ref,
                "basis": list(decision.basis),
                "retracted": bool(decision.actions),
                "checks": [{
                    "gate": decision.gate,
                    "scene": decision.scene_key,
                    "status": "handled",
                    "violation_types": sorted({hit.violation_type for hit in decision.hits}),
                    "actions": list(decision.actions),
                    "basis": list(decision.basis),
                    "audit_ref": decision.audit_ref,
                }],
            }
        }
        bundle = {
            "schema_version": "compliance_reproduction_bundle_1",
            "fixture": {
                "message_id": message_id,
                "original_sha256": __import__("hashlib").sha256(original.encode()).hexdigest(),
                "original_not_written_to_audit": original not in json.dumps(audit_record, ensure_ascii=False),
            },
            "decision": decision.to_dict(),
            "audit_record": audit_record,
            "sse_event": event,
            "frontend_message_metadata": frontend_metadata,
            "assertions": {
                "same_audit_ref_across_decision_audit_sse_frontend": len({decision.audit_ref, audit_record["audit_ref"], event["audit_ref"], frontend_metadata["compliance"]["audit_ref"]}) == 1,
                "sse_json_serializable": json.loads(json.dumps(event, ensure_ascii=False))["type"] == "compliance_retract",
                "replacement_hides_original_when_action_mutates": original not in replacement if decision.actions else True,
                "original_not_in_audit_record": original not in json.dumps(audit_record, ensure_ascii=False),
            },
            "audit_file": str(next(audit_dir.glob("compliance-*.jsonl")).relative_to(ROOT)),
        }
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / "reproduction.json").write_text(json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(bundle["assertions"], ensure_ascii=False, indent=2))
        print(f"bundle written to: {OUT / 'reproduction.json'}")
        return 0
    finally:
        registry.close()


if __name__ == "__main__":
    raise SystemExit(main())
