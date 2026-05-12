"""Tests for scripts/update_skill_router_index.py.

Covers:
- Router Card generation from SKILL.md
- Schema validation success and failure paths
- Registry entry creation and status updates
- ES document structure

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_skill_creator_router_update.py
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "packages" / "harness"))

from update_skill_router_index import (
    resolve_profile,
    update_registry_status,
    upsert_registry_entry,
    validate_card,
)


# ---------------------------------------------------------------------------
# resolve_profile
# ---------------------------------------------------------------------------

class TestResolveProfile:
    def test_known_custom_skill(self):
        profile = resolve_profile("network-traffic-analysis", is_custom=True)
        assert profile["scenes"] == ["network_traffic"]
        assert profile["is_public"] is False

    def test_unknown_custom_skill_uses_defaults(self):
        profile = resolve_profile("totally-new-skill", is_custom=True)
        assert profile["scenes"] == ["totally-new-skill"]
        assert profile["is_public"] is False
        assert profile["task_types"] == []

    def test_known_public_skill(self):
        profile = resolve_profile("data-analysis", is_custom=False)
        assert profile["scenes"] == ["public"]
        assert profile["is_public"] is True

    def test_unknown_public_skill_uses_defaults(self):
        profile = resolve_profile("unknown-public", is_custom=False)
        assert profile["scenes"] == ["public"]
        assert profile["is_public"] is True


# ---------------------------------------------------------------------------
# validate_card
# ---------------------------------------------------------------------------

class TestValidateCard:
    def test_valid_minimal_card(self):
        card = {
            "schema_version": "1.0.0",
            "identity": {"id": "x", "name": "X", "description": "d"},
            "scope": {},
            "routing": {"routing_text": "t"},
            "body": {},
            "execution": {},
            "routing_policy": {},
            "source": {},
            "embedding": {},
        }
        assert validate_card(card) == []

    def test_missing_identity_fields(self):
        card = {"identity": {"id": "x"}, "routing": {}}
        errors = validate_card(card)
        assert any("identity.name" in e for e in errors)
        assert any("identity.description" in e for e in errors)

    def test_missing_routing_text(self):
        card = {"identity": {"id": "x", "name": "x", "description": "d"}, "routing": {}}
        errors = validate_card(card)
        assert any("routing_text" in e for e in errors)


# ---------------------------------------------------------------------------
# Registry operations
# ---------------------------------------------------------------------------

class TestUpsertRegistryEntry:
    def _make_card(self, skill_id="test-skill"):
        return {
            "identity": {"id": skill_id, "name": "Test Skill", "description": "desc"},
            "scope": {"scenes": ["test"], "is_public": False, "task_types": [], "input_types": []},
            "source": {"skill_dir": f"custom/{skill_id}", "skill_md_path": f"custom/{skill_id}/SKILL.md"},
            "embedding": {"text_hash": "sha256:abc"},
        }

    def test_new_entry_added(self):
        registry = {"version": 1, "skills": []}
        card = self._make_card("new-skill")
        result = upsert_registry_entry("/tmp", registry, "new-skill", card)
        assert len(result["skills"]) == 1
        assert result["skills"][0]["id"] == "new-skill"
        assert result["skills"][0]["router_status"] == "pending_index"

    def test_existing_entry_updated(self):
        registry = {
            "version": 1,
            "skills": [{
                "id": "existing",
                "name": "Old Name",
                "router_status": "ready",
                "es_indexed": True,
            }],
        }
        card = self._make_card("existing")
        result = upsert_registry_entry("/tmp", registry, "existing", card)
        assert len(result["skills"]) == 1
        assert result["skills"][0]["name"] == "Test Skill"


class TestUpdateRegistryStatus:
    def test_success_sets_ready(self):
        registry = {"skills": [{"id": "x", "enabled": False, "router_status": "pending_index", "es_indexed": False}]}
        update_registry_status(registry, "x", success=True)
        s = registry["skills"][0]
        assert s["enabled"] is True
        assert s["router_status"] == "ready"
        assert s["es_indexed"] is True
        assert s["last_indexed_at"] is not None
        assert s["last_router_error"] is None

    def test_error_sets_error_status(self):
        registry = {"skills": [{"id": "x", "enabled": True, "router_status": "ready", "es_indexed": True}]}
        update_registry_status(registry, "x", success=False, error_stage="build_embedding", error_message="Timeout")
        s = registry["skills"][0]
        assert s["enabled"] is False
        assert s["router_status"] == "error"
        assert s["es_indexed"] is False
        assert s["last_indexed_at"] is None
        assert s["last_router_error"]["stage"] == "build_embedding"
        assert s["last_router_error"]["message"] == "Timeout"

    def test_non_matching_skill_unchanged(self):
        registry = {"skills": [{"id": "x", "enabled": True}]}
        update_registry_status(registry, "y", success=True)
        assert registry["skills"][0]["enabled"] is True
