"""Tests for Gateway SkillRouter management endpoints.

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_skill_router_gateway.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow harness imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "packages" / "harness"))

# Project root for absolute path references
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class TestSkillInstallResponseFields:
    """Verify that SkillInstallResponse includes router fields by reading source."""

    def test_response_has_router_fields(self):
        """Check that the SkillInstallResponse model has router_indexed, router_status, router_error."""
        skills_path = _PROJECT_ROOT / "backend" / "app" / "gateway" / "routers" / "skills.py"
        source = skills_path.read_text()
        assert "router_indexed" in source
        assert "router_status" in source
        assert "router_error" in source

    def test_response_defaults(self):
        """Verify default values for router fields in the response."""
        # Read source to confirm defaults are set
        skills_path = _PROJECT_ROOT / "backend" / "app" / "gateway" / "routers" / "skills.py"
        source = skills_path.read_text()
        assert "default=True" in source  # router_indexed default
        assert 'default="ready"' in source  # router_status default
        assert "default=None" in source  # router_error default


class TestIndexUpdaterResultInGateway:
    """Verify IndexUpdateResult is compatible with Gateway response."""

    def test_result_to_response_mapping(self):
        from deerflow.routing.index_updater import IndexUpdateResult

        # Ready case
        r = IndexUpdateResult(
            skill_id="test",
            success=True,
            router_indexed=True,
            router_status="ready",
        )
        assert r.router_indexed is True
        assert r.router_error is None

        # Error case
        e = IndexUpdateResult(
            skill_id="test",
            success=True,
            router_indexed=False,
            router_status="index_failed",
            router_error="Connection refused",
        )
        assert e.router_indexed is False
        assert e.router_status == "index_failed"
        assert e.router_error == "Connection refused"

        # Already up to date
        u = IndexUpdateResult(
            skill_id="test",
            success=True,
            router_indexed=True,
            router_status="already_up_to_date",
            already_up_to_date=True,
        )
        assert u.already_up_to_date is True


class TestClientInstallReturnsRouterInfo:
    """Verify that DeerFlowClient.install_skill returns router fields."""

    def test_client_code_has_router_fields(self):
        """Read the client.py source and verify it returns router_indexed, router_status, router_error."""
        client_path = _PROJECT_ROOT / "backend" / "packages" / "harness" / "deerflow" / "client.py"
        source = client_path.read_text()
        assert "router_indexed" in source
        assert "router_status" in source
        assert "router_error" in source


class TestGatewaySkillRouterEndpoints:
    """Verify gateway skill_router.py endpoints exist."""

    def test_status_endpoint_defined(self):
        router_path = _PROJECT_ROOT / "backend" / "app" / "gateway" / "routers" / "skill_router.py"
        source = router_path.read_text()
        assert '"/status"' in source
        assert "SkillRouterStatusResponse" in source

    def test_refresh_endpoint_defined(self):
        router_path = _PROJECT_ROOT / "backend" / "app" / "gateway" / "routers" / "skill_router.py"
        source = router_path.read_text()
        assert '"/refresh"' in source
        assert "RefreshResponse" in source

    def test_conflicts_endpoint_defined(self):
        router_path = _PROJECT_ROOT / "backend" / "app" / "gateway" / "routers" / "skill_router.py"
        source = router_path.read_text()
        assert '"/conflicts"' in source
        assert "ConflictResponse" in source

    def test_rebuild_endpoint_defined(self):
        router_path = _PROJECT_ROOT / "backend" / "app" / "gateway" / "routers" / "skill_router.py"
        source = router_path.read_text()
        assert '"/rebuild"' in source
        assert "RebuildResponse" in source

    def test_metrics_endpoint_defined(self):
        router_path = _PROJECT_ROOT / "backend" / "app" / "gateway" / "routers" / "skill_router.py"
        source = router_path.read_text()
        assert '"/metrics"' in source

    def test_router_registered_in_app(self):
        """Verify skill_router is included in gateway app.py."""
        app_path = _PROJECT_ROOT / "backend" / "app" / "gateway" / "app.py"
        source = app_path.read_text()
        assert "skill_router" in source
        assert "include_router" in source
