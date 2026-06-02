"""errors.py 单元测试 —— schema v1.1 标准化错误码。"""

from __future__ import annotations

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import errors  # noqa: E402


class TestBuildError(unittest.TestCase):
    def test_defaults_action_from_table(self):
        e = errors.build_error(code="E_TIMEOUT", message="ES 检索超时(8s)")
        self.assertEqual(e["code"], "E_TIMEOUT")
        self.assertEqual(e["action"], "retry_with_backoff")
        self.assertTrue(e["retryable"])

    def test_explicit_action_overrides_default(self):
        e = errors.build_error(
            code="E_TIMEOUT",
            message="单次超时,放弃",
            action="abort",
        )
        self.assertEqual(e["action"], "abort")
        self.assertFalse(e["retryable"])

    def test_detail_is_preserved(self):
        e = errors.build_error(
            code="E_INDEX_MISSING",
            message="索引未构建",
            detail={"index": "netflow_demo_v3"},
        )
        self.assertEqual(e["detail"], {"index": "netflow_demo_v3"})
        self.assertEqual(e["action"], "degrade_to_local")

    def test_rejects_unknown_code(self):
        with self.assertRaises(ValueError):
            errors.build_error(code="E_NONSENSE", message="x")

    def test_rejects_unknown_action(self):
        with self.assertRaises(ValueError):
            errors.build_error(code="E_TIMEOUT", message="x", action="explode")

    def test_rejects_empty_message(self):
        with self.assertRaises(ValueError):
            errors.build_error(code="E_TIMEOUT", message="   ")

    def test_rejects_non_dict_detail(self):
        with self.assertRaises(ValueError):
            errors.build_error(code="E_TIMEOUT", message="x", detail=["a"])


class TestDeriveStatus(unittest.TestCase):
    def test_empty_errors_is_success(self):
        self.assertEqual(errors.derive_status([]), "success")
        self.assertEqual(errors.derive_status(None), "success")

    def test_errors_without_evidence_is_error(self):
        err = [errors.build_error(code="E_TIMEOUT", message="x")]
        self.assertEqual(errors.derive_status(err), "error")

    def test_errors_with_some_evidence_is_partial(self):
        err = [errors.build_error(code="E_PARTIAL_RESULT", message="x")]
        self.assertEqual(errors.derive_status(err, partial_if_any_evidence=True), "partial")


class TestValidateErrorsBlock(unittest.TestCase):
    def test_success_with_empty_errors_passes(self):
        self.assertEqual(errors.validate_errors_block([], status="success"), [])
        self.assertEqual(errors.validate_errors_block(None, status="success"), [])

    def test_success_with_non_empty_errors_fails(self):
        err = [errors.build_error(code="E_TIMEOUT", message="x")]
        issues = errors.validate_errors_block(err, status="success")
        self.assertTrue(any("forbids non-empty" in s for s in issues))

    def test_partial_without_errors_fails(self):
        issues = errors.validate_errors_block([], status="partial")
        self.assertTrue(any("requires at least one" in s for s in issues))

    def test_error_status_with_valid_error_passes(self):
        err = [errors.build_error(code="E_UPSTREAM_FAIL", message="x")]
        self.assertEqual(errors.validate_errors_block(err, status="error"), [])

    def test_error_with_bad_code_fails(self):
        bad = [{"code": "NOPE", "message": "x", "action": "abort", "retryable": False}]
        issues = errors.validate_errors_block(bad, status="error")
        self.assertTrue(any("not a registered error code" in s for s in issues))

    def test_error_missing_retryable_fails(self):
        bad = [{"code": "E_TIMEOUT", "message": "x", "action": "abort"}]
        issues = errors.validate_errors_block(bad, status="error")
        self.assertTrue(any("retryable" in s for s in issues))

    def test_non_list_errors_fails(self):
        issues = errors.validate_errors_block("oops", status="success")
        self.assertTrue(any("must be a list" in s for s in issues))


if __name__ == "__main__":
    unittest.main()
