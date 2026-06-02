"""公共 API 引入测试 —— spec 2026-05-27 新增导出。"""
import unittest


class TestPublicApiNew(unittest.TestCase):
    def test_sensitivity_imports(self):
        from retrieval_protocol import classify_sensitivity, DEFAULT_SENSITIVITY, K_THRESHOLD_AGGREGATED_SAFE
        self.assertTrue(callable(classify_sensitivity))
        self.assertIn("telecom", DEFAULT_SENSITIVITY)
        self.assertEqual(K_THRESHOLD_AGGREGATED_SAFE, 10)

    def test_audit_imports(self):
        from retrieval_protocol import (
            AUDIT_ACTIONS, AUDIT_ACTION_STATUSES, AUDIT_GATES, AUDIT_SCENES,
            AUDIT_REASON_CODES, build_evidence_action, validate_evidence_action,
        )
        self.assertIn("filter", AUDIT_ACTIONS)
        self.assertIn("InputGate", AUDIT_GATES)
        self.assertTrue(callable(build_evidence_action))
        self.assertTrue(callable(validate_evidence_action))


if __name__ == "__main__":
    unittest.main()
