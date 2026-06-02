"""budget 字段单元测试 —— schema v1.1 Envelope 顶层 budget。"""

from __future__ import annotations

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import envelope, validate  # noqa: E402


class TestBuildBudget(unittest.TestCase):
    def test_evidence_count_only(self):
        b = envelope.build_budget(max_evidence_count=8)
        self.assertEqual(b, {"max_evidence_count": 8})

    def test_token_estimate_only(self):
        b = envelope.build_budget(max_token_estimate=4000)
        self.assertEqual(b, {"max_token_estimate": 4000})

    def test_both(self):
        b = envelope.build_budget(max_evidence_count=10, max_token_estimate=8000)
        self.assertEqual(set(b), {"max_evidence_count", "max_token_estimate"})

    def test_requires_at_least_one_field(self):
        with self.assertRaises(ValueError):
            envelope.build_budget()

    def test_rejects_non_positive(self):
        with self.assertRaises(ValueError):
            envelope.build_budget(max_evidence_count=0)
        with self.assertRaises(ValueError):
            envelope.build_budget(max_token_estimate=-1)

    def test_rejects_bool(self):
        with self.assertRaises(ValueError):
            envelope.build_budget(max_evidence_count=True)


class TestEnvelopeWithBudget(unittest.TestCase):
    def _env(self, **kw):
        return envelope.build_input_envelope(
            skill_name="s", scenario="netflow", capability="evidence_search",
            query="q", data_type="netflow", **kw,
        )

    def test_budget_absent_by_default(self):
        env = self._env()
        self.assertNotIn("budget", env)

    def test_budget_attached_when_given(self):
        env = self._env(budget=envelope.build_budget(max_evidence_count=5))
        self.assertEqual(env["budget"], {"max_evidence_count": 5})

    def test_empty_budget_rejected(self):
        with self.assertRaises(ValueError):
            self._env(budget={})


class TestValidateBudget(unittest.TestCase):
    def test_valid_budget(self):
        self.assertEqual(
            validate.validate_budget({"max_evidence_count": 5, "max_token_estimate": 1000}),
            [],
        )

    def test_non_object_fails(self):
        issues = validate.validate_budget("foo")
        self.assertTrue(any("must be an object" in s for s in issues))

    def test_empty_fails(self):
        issues = validate.validate_budget({})
        self.assertTrue(any("requires at least one" in s for s in issues))

    def test_negative_value_fails(self):
        issues = validate.validate_budget({"max_evidence_count": -3})
        self.assertTrue(any("positive integer" in s for s in issues))

    def test_validate_envelope_includes_budget(self):
        env = envelope.build_input_envelope(
            skill_name="s", scenario="netflow", capability="evidence_search",
            query="q", data_type="netflow",
            budget=envelope.build_budget(max_evidence_count=10),
        )
        self.assertEqual(validate.validate_input_envelope(env), [])

    def test_validate_envelope_rejects_bad_budget(self):
        env = envelope.build_input_envelope(
            skill_name="s", scenario="netflow", capability="evidence_search",
            query="q", data_type="netflow",
        )
        env["budget"] = {"max_evidence_count": 0}
        issues = validate.validate_input_envelope(env)
        self.assertTrue(any("positive integer" in s for s in issues))


if __name__ == "__main__":
    unittest.main()
