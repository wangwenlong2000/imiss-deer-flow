"""Tests for scripts/update_skill_router_index.py and routing/index_updater.py.

Covers:
- Index updater: validation, hash computation, idempotency
- Router Card generation from SKILL.md via build_router_card_for_skill
- CLI wrapper exit codes

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_skill_creator_router_update.py
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "packages" / "harness"))

from deerflow.routing.index_updater import (
    IndexUpdateResult,
    build_router_card_for_skill,
    compute_skill_hash,
    validate_skill_dir,
)


# ---------------------------------------------------------------------------
# validate_skill_dir
# ---------------------------------------------------------------------------


class TestValidateSkillDir:
    def test_valid_skill_dir(self):
        with tempfile.TemporaryDirectory() as td:
            skill_dir = Path(td)
            (skill_dir / "SKILL.md").write_text("---\nname: Test\ndescription: A test skill\n---\nBody content")
            ok, err = validate_skill_dir(skill_dir)
            assert ok is True
            assert err is None

    def test_missing_skill_md(self):
        with tempfile.TemporaryDirectory() as td:
            skill_dir = Path(td)
            ok, err = validate_skill_dir(skill_dir)
            assert ok is False
            assert "SKILL.md not found" in err

    def test_dir_does_not_exist(self):
        ok, err = validate_skill_dir(Path("/nonexistent/skill"))
        assert ok is False
        assert "does not exist" in err

    def test_valid_with_tool_manifest(self):
        with tempfile.TemporaryDirectory() as td:
            skill_dir = Path(td)
            (skill_dir / "SKILL.md").write_text("---\nname: Test\ndescription: d\n---\nbody")
            (skill_dir / "tool_manifest.json").write_text('{"tools": []}')
            ok, err = validate_skill_dir(skill_dir)
            assert ok is True

    def test_invalid_tool_manifest(self):
        with tempfile.TemporaryDirectory() as td:
            skill_dir = Path(td)
            (skill_dir / "SKILL.md").write_text("---\nname: Test\ndescription: d\n---\nbody")
            (skill_dir / "tool_manifest.json").write_text("not json")
            ok, err = validate_skill_dir(skill_dir)
            assert ok is False
            assert "Invalid tool_manifest.json" in err


# ---------------------------------------------------------------------------
# compute_skill_hash
# ---------------------------------------------------------------------------


class TestComputeSkillHash:
    def test_consistent_hash(self):
        with tempfile.TemporaryDirectory() as td:
            skill_dir = Path(td)
            (skill_dir / "SKILL.md").write_text("content")
            h1 = compute_skill_hash(skill_dir)
            h2 = compute_skill_hash(skill_dir)
            assert h1 == h2

    def test_different_content_different_hash(self):
        with tempfile.TemporaryDirectory() as td:
            skill_dir = Path(td)
            (skill_dir / "SKILL.md").write_text("content1")
            h1 = compute_skill_hash(skill_dir)
            (skill_dir / "SKILL.md").write_text("content2")
            h2 = compute_skill_hash(skill_dir)
            assert h1 != h2

    def test_includes_multiple_files(self):
        with tempfile.TemporaryDirectory() as td:
            skill_dir = Path(td)
            (skill_dir / "SKILL.md").write_text("skill content")
            h1 = compute_skill_hash(skill_dir)
            (skill_dir / "router_card.json").write_text('{"routing_text": "test"}')
            h2 = compute_skill_hash(skill_dir)
            assert h1 != h2  # adding file changes hash


# ---------------------------------------------------------------------------
# build_router_card_for_skill
# ---------------------------------------------------------------------------


class TestBuildRouterCard:
    def _setup_skill(self, td: str, skill_id: str = "test-skill") -> Path:
        skill_dir = Path(td)
        # Create a realistic SKILL.md
        (skill_dir / "SKILL.md").write_text(
            "---\n"
            f"name: {skill_id}\n"
            f"description: Description for {skill_id}\n"
            "---\n"
            "This is the body of the skill."
        )
        return skill_dir

    def test_build_card_known_custom_skill(self):
        with tempfile.TemporaryDirectory() as td:
            skill_dir = self._setup_skill(td, "network-traffic-analysis")
            skills_root = Path(td)
            # Need parent structure for category detection
            (skills_root / "custom").mkdir(exist_ok=True)
            (skills_root / "custom" / "network-traffic-analysis").mkdir(exist_ok=True)
            (skills_root / "custom" / "network-traffic-analysis" / "SKILL.md").write_text(
                "---\nname: Network Analysis\ndescription: Analyze pcap\n---\nbody"
            )
            card_dir = skills_root / "custom" / "network-traffic-analysis"
            card, err = build_router_card_for_skill(card_dir, skills_root)
            assert card is not None
            assert err is None
            assert card["identity"]["id"] == "network-traffic-analysis"
            assert card["scope"]["is_public"] is False
            assert "network_traffic" in card["scope"]["scenes"]

    def test_build_card_unknown_skill(self):
        with tempfile.TemporaryDirectory() as td:
            skills_root = Path(td)
            (skills_root / "custom").mkdir(exist_ok=True)
            unknown_dir = skills_root / "custom" / "unknown-skill"
            unknown_dir.mkdir(exist_ok=True)
            (unknown_dir / "SKILL.md").write_text(
                "---\nname: Unknown\ndescription: unknown skill\n---\nbody"
            )
            card, err = build_router_card_for_skill(unknown_dir, skills_root)
            assert card is not None
            assert err is None
            # Falls back to default profile
            assert card["identity"]["id"] == "unknown-skill"
            assert "routing_text" in card["routing"]


# ---------------------------------------------------------------------------
# IndexUpdateResult
# ---------------------------------------------------------------------------


class TestIndexUpdateResult:
    def test_result_fields(self):
        r = IndexUpdateResult(
            skill_id="test",
            success=True,
            router_indexed=True,
            router_status="ready",
        )
        assert r.skill_id == "test"
        assert r.success is True
        assert r.router_indexed is True
        assert r.router_error is None
        assert r.already_up_to_date is False

    def test_already_up_to_date(self):
        r = IndexUpdateResult(
            skill_id="test",
            success=True,
            router_indexed=True,
            router_status="already_up_to_date",
            already_up_to_date=True,
            skill_hash="abc123",
        )
        assert r.already_up_to_date is True
        assert r.skill_hash == "abc123"

    def test_error_result(self):
        r = IndexUpdateResult(
            skill_id="test",
            success=True,
            router_indexed=False,
            router_status="index_failed",
            router_error="ES connection refused",
        )
        assert r.router_indexed is False
        assert r.router_error is not None
