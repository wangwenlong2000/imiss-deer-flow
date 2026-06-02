"""审计字段单测 —— spec 2026-05-27 §4."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from retrieval_protocol import audit


class TestAuditEnums(unittest.TestCase):
    def test_actions_set(self):
        expected = {
            "allow", "warn", "desensitize", "aggregate",
            "rewrite", "filter", "refuse", "manual_review",
        }
        self.assertEqual(audit.AUDIT_ACTIONS, frozenset(expected))

    def test_action_statuses(self):
        self.assertEqual(audit.AUDIT_ACTION_STATUSES, frozenset({"applied", "pending", "failed"}))

    def test_gates(self):
        self.assertEqual(audit.AUDIT_GATES, frozenset({"InputGate", "ContextGate", "OutputGate"}))

    def test_scenes(self):
        expected = {"self_use", "internal_org", "cross_org", "public_release", "research_anon"}
        self.assertEqual(audit.AUDIT_SCENES, frozenset(expected))

    def test_reason_codes_include_three_examples(self):
        # spec §4.2 例:struct_id_detected / geo_loc_with_object_time / re_identify_combo_risk
        self.assertIn("struct_id_detected", audit.AUDIT_REASON_CODES)
        self.assertIn("geo_loc_with_object_time", audit.AUDIT_REASON_CODES)
        self.assertIn("re_identify_combo_risk", audit.AUDIT_REASON_CODES)


class TestBuildEvidenceAction(unittest.TestCase):
    def test_minimal_valid(self):
        action = audit.build_evidence_action(
            evidence_id="ev_1",
            action="desensitize",
            action_status="applied",
            triggered_violation_types=[],
            risk_locations=[{"field_path": "meta.subject", "risk_type": "phone"}],
            reason_code="struct_id_detected",
            sensitivity_before="pii_masked",
            sensitivity_after="restricted",
        )
        self.assertEqual(action["evidence_id"], "ev_1")
        self.assertEqual(action["action"], "desensitize")
        self.assertEqual(action["risk_locations"], [{"field_path": "meta.subject", "risk_type": "phone"}])

    def test_unknown_action_rejected(self):
        with self.assertRaises(ValueError):
            audit.build_evidence_action(
                evidence_id="ev_1", action="frobnicate", action_status="applied",
                triggered_violation_types=[], risk_locations=[],
                reason_code="struct_id_detected",
                sensitivity_before="pii_masked", sensitivity_after="restricted",
            )

    def test_unknown_status_rejected(self):
        with self.assertRaises(ValueError):
            audit.build_evidence_action(
                evidence_id="ev_1", action="allow", action_status="bogus",
                triggered_violation_types=[], risk_locations=[],
                reason_code="struct_id_detected",
                sensitivity_before="open", sensitivity_after="open",
            )

    def test_unknown_reason_code_rejected(self):
        with self.assertRaises(ValueError):
            audit.build_evidence_action(
                evidence_id="ev_1", action="allow", action_status="applied",
                triggered_violation_types=[], risk_locations=[],
                reason_code="not_a_code",
                sensitivity_before="open", sensitivity_after="open",
            )


class TestValidateEvidenceAction(unittest.TestCase):
    def test_extra_key_in_risk_location_rejected(self):
        # spec §4.3 护栏:risk_locations[*] 只允许 field_path + risk_type 两个键
        action = {
            "evidence_id": "ev_1", "action": "allow", "action_status": "applied",
            "triggered_violation_types": [],
            "risk_locations": [
                {"field_path": "meta.subject", "risk_type": "phone", "raw": "13800138000"},
            ],
            "reason_code": "struct_id_detected",
            "sensitivity_before": "pii_masked", "sensitivity_after": "restricted",
        }
        errors = audit.validate_evidence_action(action)
        self.assertTrue(errors, "validate_evidence_action 应该报错 risk_locations 含第三键")
        self.assertTrue(any("risk_locations" in e for e in errors))

    def test_long_string_rejected(self):
        action = {
            "evidence_id": "ev_1", "action": "allow", "action_status": "applied",
            "triggered_violation_types": [],
            "risk_locations": [{"field_path": "x" * 200, "risk_type": "phone"}],
            "reason_code": "struct_id_detected",
            "sensitivity_before": "open", "sensitivity_after": "open",
        }
        errors = audit.validate_evidence_action(action)
        self.assertTrue(errors)
        self.assertTrue(any("length" in e or "长度" in e for e in errors))

    def test_empty_evidence_id_rejected(self):
        action = {
            "evidence_id": "", "action": "allow", "action_status": "applied",
            "triggered_violation_types": [],
            "risk_locations": [{"field_path": "meta.x", "risk_type": "phone"}],
            "reason_code": "struct_id_detected",
            "sensitivity_before": "open", "sensitivity_after": "open",
        }
        errors = audit.validate_evidence_action(action)
        self.assertTrue(errors)
        self.assertTrue(any("evidence_id" in e for e in errors))

    def test_unknown_sensitivity_level_rejected(self):
        action = {
            "evidence_id": "ev_1", "action": "allow", "action_status": "applied",
            "triggered_violation_types": [],
            "risk_locations": [{"field_path": "meta.x", "risk_type": "phone"}],
            "reason_code": "struct_id_detected",
            "sensitivity_before": "weird", "sensitivity_after": "open",
        }
        errors = audit.validate_evidence_action(action)
        self.assertTrue(errors)

    def test_valid_returns_empty(self):
        action = {
            "evidence_id": "ev_1", "action": "allow", "action_status": "applied",
            "triggered_violation_types": [],
            "risk_locations": [{"field_path": "meta.x", "risk_type": "phone"}],
            "reason_code": "struct_id_detected",
            "sensitivity_before": "open", "sensitivity_after": "open",
        }
        self.assertEqual(audit.validate_evidence_action(action), [])


if __name__ == "__main__":
    unittest.main()
