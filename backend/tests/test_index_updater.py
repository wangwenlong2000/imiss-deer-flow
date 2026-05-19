"""Tests for deerflow.routing.index_updater module.

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_index_updater.py
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "packages" / "harness"))

from deerflow.routing.index_updater import (
    IndexUpdateResult,
    compute_skill_hash,
    update_single_skill_index,
    validate_skill_dir,
)


class TestValidateSkillDirInUpdater:
    def test_valid_dir(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            (p / "SKILL.md").write_text("---\nname: x\ndescription: d\n---\nbody")
            ok, err = validate_skill_dir(p)
            assert ok is True

    def test_no_skill_md(self):
        with tempfile.TemporaryDirectory() as td:
            ok, err = validate_skill_dir(Path(td))
            assert ok is False
            assert "SKILL.md not found" in (err or "")


class TestComputeSkillHashInUpdater:
    def test_same_content_same_hash(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            (p / "SKILL.md").write_text("content")
            h1 = compute_skill_hash(p)
            h2 = compute_skill_hash(p)
            assert h1 == h2

    def test_added_file_changes_hash(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            (p / "SKILL.md").write_text("content")
            h1 = compute_skill_hash(p)
            (p / "router_card.json").write_text("{}")
            h2 = compute_skill_hash(p)
            assert h1 != h2


class TestUpdateSingleSkillIndex:
    def test_skill_dir_not_found(self):
        with tempfile.TemporaryDirectory() as td:
            result = update_single_skill_index(
                skill_id="nonexistent",
                skills_root=Path(td),
            )
            assert result.success is False
            assert result.router_indexed is False
            assert result.router_status == "error"
            assert "not found" in (result.router_error or "")

    def test_validation_failure(self):
        with tempfile.TemporaryDirectory() as td:
            skills_root = Path(td)
            (skills_root / "custom").mkdir()
            skill_dir = skills_root / "custom" / "bad-skill"
            skill_dir.mkdir()
            # No SKILL.md
            result = update_single_skill_index(
                skill_id="bad-skill",
                skill_dir=skill_dir,
                skills_root=skills_root,
            )
            assert result.router_status == "invalid_card"
            assert result.router_indexed is False


class TestIndexUpdateResultFields:
    def test_ready_result(self):
        r = IndexUpdateResult(
            skill_id="x",
            success=True,
            router_indexed=True,
            router_status="ready",
            skill_hash="abc",
        )
        assert r.router_indexed is True
        assert r.router_error is None
        assert r.already_up_to_date is False

    def test_error_result(self):
        r = IndexUpdateResult(
            skill_id="x",
            success=True,
            router_indexed=False,
            router_status="index_failed",
            router_error="ES unreachable",
        )
        assert r.router_indexed is False
        assert r.router_error == "ES unreachable"
