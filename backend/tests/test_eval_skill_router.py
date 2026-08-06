"""Tests for scripts/eval_skill_router.py.

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_eval_skill_router.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "packages" / "harness"))

from eval_skill_router import (
    EvalCase,
    generate_json_report,
    generate_text_report,
    load_eval_cases,
    run_offline_eval,
)


class TestLoadEvalCases:
    def test_returns_non_empty_list(self):
        cases = load_eval_cases()
        assert len(cases) > 0

    def test_all_cases_have_required_fields(self):
        cases = load_eval_cases()
        for c in cases:
            assert isinstance(c.query, str) and len(c.query) > 0
            assert isinstance(c.expected_skill_ids, list)
            assert isinstance(c.expect_trigger, bool)
            assert isinstance(c.category, str)

    def test_has_trigger_false_cases(self):
        cases = load_eval_cases()
        trigger_false = [c for c in cases if not c.expect_trigger]
        assert len(trigger_false) > 0

    def test_has_known_cases(self):
        cases = load_eval_cases()
        known = [c for c in cases if c.category == "known"]
        assert len(known) > 0


class TestRunOfflineEval:
    def test_returns_valid_report(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            report = run_offline_eval(load_eval_cases(), Path(td))
            assert "total_cases" in report
            assert "passed" in report
            assert "failed" in report
            assert report["mode"] == "offline"
            assert isinstance(report["details"], list)

    def test_report_structure(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            report = run_offline_eval(load_eval_cases(), Path(td))
            assert report["total_cases"] == report["passed"] + report["failed"]

    def test_details_have_required_fields(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            report = run_offline_eval(load_eval_cases(), Path(td))
            for d in report["details"]:
                assert "query" in d
                assert "expected" in d
                assert "actual" in d
                assert "status" in d
                assert d["status"] in ("PASS", "FAIL")


class TestReportGenerators:
    def test_text_report(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            report = run_offline_eval(load_eval_cases(), Path(td))
            text = generate_text_report(report)
            assert "SkillRouter Evaluation Report" in text
            assert str(report["total_cases"]) in text

    def test_json_report(self):
        report = {
            "total_cases": 2,
            "passed": 1,
            "failed": 1,
            "mode": "offline",
            "details": [],
        }
        json_str = generate_json_report(report)
        parsed = json.loads(json_str)
        assert parsed["total_cases"] == 2
        assert parsed["mode"] == "offline"
