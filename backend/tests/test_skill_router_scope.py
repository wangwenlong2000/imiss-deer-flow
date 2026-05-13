"""Tests for SkillScopeResolver.

Covers:
- resolve_base_scope with None, [], and partial frontend lists
- resolve_final_scope with router on/off
- Intersection logic (registry ∩ frontend)
- Out-of-scope router results are filtered

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_skill_router_scope.py
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from deerflow.routing.scope_resolver import SkillScopeResolver


class MockSkill:
    def __init__(self, name: str):
        self.name = name


class TestResolveBaseScope(unittest.TestCase):

    @patch("deerflow.skills.load_skills")
    def test_null_frontend_returns_all_registry(self, mock_load):
        mock_load.return_value = [MockSkill("a"), MockSkill("b"), MockSkill("c")]
        result = SkillScopeResolver.resolve_base_scope(frontend_enabled_skill_ids=None)
        self.assertEqual(result, ["a", "b", "c"])

    @patch("deerflow.skills.load_skills")
    def test_empty_frontend_returns_empty(self, mock_load):
        mock_load.return_value = [MockSkill("a"), MockSkill("b")]
        result = SkillScopeResolver.resolve_base_scope(frontend_enabled_skill_ids=[])
        self.assertEqual(result, [])

    @patch("deerflow.skills.load_skills")
    def test_partial_frontend_returns_intersection(self, mock_load):
        mock_load.return_value = [MockSkill("a"), MockSkill("b"), MockSkill("c")]
        result = SkillScopeResolver.resolve_base_scope(frontend_enabled_skill_ids=["a", "x"])
        self.assertEqual(result, ["a"])

    @patch("deerflow.skills.load_skills")
    def test_result_is_sorted(self, mock_load):
        mock_load.return_value = [MockSkill("c"), MockSkill("a"), MockSkill("b")]
        result = SkillScopeResolver.resolve_base_scope(frontend_enabled_skill_ids=["c", "a", "b"])
        self.assertEqual(result, ["a", "b", "c"])


class TestResolveFinalScope(unittest.TestCase):
    """resolve_final_scope does not call load_skills, so no mocking needed."""

    def test_router_off_returns_base(self):
        result = SkillScopeResolver.resolve_final_scope(
            skill_router_enabled=False,
            base_scope_ids=["a", "b"],
            routed_skill_ids=["a"],
        )
        self.assertEqual(result, ["a", "b"])

    def test_router_on_empty_routed_returns_empty(self):
        result = SkillScopeResolver.resolve_final_scope(
            skill_router_enabled=True,
            base_scope_ids=["a", "b"],
            routed_skill_ids=None,
        )
        self.assertEqual(result, [])

    def test_router_on_filters_to_base(self):
        result = SkillScopeResolver.resolve_final_scope(
            skill_router_enabled=True,
            base_scope_ids=["a", "b"],
            routed_skill_ids=["a", "c", "d"],  # c, d not in base
        )
        self.assertEqual(result, ["a"])

    def test_router_on_empty_base_returns_empty(self):
        result = SkillScopeResolver.resolve_final_scope(
            skill_router_enabled=True,
            base_scope_ids=[],
            routed_skill_ids=["a"],
        )
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
