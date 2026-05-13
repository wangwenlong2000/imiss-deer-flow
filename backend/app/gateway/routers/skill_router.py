"""Gateway endpoints for SkillRouter management.

Provides status, refresh, conflict detection, rebuild, and metrics endpoints.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent

router = APIRouter(prefix="/api/skill-router", tags=["skill-router"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class SkillRouterStatusResponse(BaseModel):
    enabled: bool
    es_connected: bool
    indexed_skills: int
    last_updated: str | None
    embedding_service: str
    reranker_service: str


class RefreshResponse(BaseModel):
    success: bool
    message: str


class ConflictDetail(BaseModel):
    skill_a: str
    skill_b: str
    overlap_score: float
    status: str
    suggestion: str


class ConflictResponse(BaseModel):
    conflicts: list[dict]
    total_checked: int
    has_hard_conflicts: bool


class RebuildResult(BaseModel):
    skill_id: str
    status: str
    error: str | None = None


class RebuildResponse(BaseModel):
    success: bool
    message: str
    skills_indexed: int
    errors: list[RebuildResult] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _check_service_health(base_url: str) -> str:
    """Check if a service is reachable."""
    try:
        with httpx.Client(timeout=3) as client:
            resp = client.get(f"{base_url}/models")
            if resp.status_code < 400:
                return "healthy"
            return f"unreachable (status={resp.status_code})"
    except Exception as e:
        return f"unreachable ({e})"


def _get_skills_root() -> Path:
    return _PROJECT_ROOT / "skills"


# ---------------------------------------------------------------------------
# GET /api/skill-router/status
# ---------------------------------------------------------------------------


@router.get("/status", response_model=SkillRouterStatusResponse)
async def get_skill_router_status() -> SkillRouterStatusResponse:
    """Return SkillRouter status including ES connectivity and model service health."""
    # Check if SkillRouter is enabled
    try:
        from deerflow.config.skill_router_config import get_skill_router_config
        config = get_skill_router_config()
        enabled = getattr(config, "enabled", True)
    except Exception:
        enabled = False

    # Check ES
    es_connected = False
    indexed_skills = 0
    last_updated = None

    es_url = os.getenv("ES_URL", "http://172.17.0.1:3128")
    es_index = os.getenv("SKILL_ROUTER_ES_INDEX", "citybrain-skill-router-cards")
    es_user = os.getenv("ES_USERNAME", "")
    es_pass = os.getenv("ES_PASSWORD", "")

    try:
        auth = (es_user, es_pass) if es_user else None
        with httpx.Client(timeout=5) as client:
            r = client.head(f"{es_url}/{es_index}", auth=auth)
            es_connected = r.status_code < 400
            if es_connected:
                r2 = client.get(f"{es_url}/{es_index}/_count", auth=auth)
                if r2.status_code < 400:
                    indexed_skills = r2.json().get("count", 0)
    except Exception:
        es_connected = False

    # Check embedding service
    emb_url = os.getenv("SKILLROUTER_EMBEDDING_BASE_URL", "http://192.168.200.1:7800/v1")
    embedding_service = _check_service_health(emb_url)

    # Check reranker service
    rerank_url = os.getenv("SKILLROUTER_RERANKER_BASE_URL", "http://192.168.200.1:7801/v1")
    reranker_service = _check_service_health(rerank_url)

    return SkillRouterStatusResponse(
        enabled=enabled,
        es_connected=es_connected,
        indexed_skills=indexed_skills,
        last_updated=last_updated,
        embedding_service=embedding_service,
        reranker_service=reranker_service,
    )


# ---------------------------------------------------------------------------
# POST /api/skill-router/refresh
# ---------------------------------------------------------------------------


@router.post("/refresh", response_model=RefreshResponse)
async def refresh_skill_router() -> RefreshResponse:
    """Trigger agent reset to reload skills with updated routing context."""
    try:
        from deerflow.client import DeerFlowClient

        client = DeerFlowClient()
        client.reset_agent()
        return RefreshResponse(
            success=True,
            message="SkillRouter configuration refreshed",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")


# ---------------------------------------------------------------------------
# GET /api/skill-router/conflicts
# ---------------------------------------------------------------------------


@router.get("/conflicts", response_model=ConflictResponse)
async def get_skill_router_conflicts() -> ConflictResponse:
    """Run conflict detection across all skills."""
    conflict_script = _PROJECT_ROOT / "scripts" / "check_skill_router_conflicts.py"
    if not conflict_script.exists():
        raise HTTPException(status_code=500, detail="Conflict detection script not found")

    try:
        result = subprocess.run(
            [sys.executable, str(conflict_script), "--all", "--skills-root", str(_get_skills_root()), "--no-embedding", "--json"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        report = json.loads(result.stdout)
        conflicts = report.get("conflicts", [])
        has_hard = any(c.get("status") == "conflict" for c in conflicts)
        return ConflictResponse(
            conflicts=conflicts,
            total_checked=len(conflicts),
            has_hard_conflicts=has_hard,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="Conflict check timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Conflict check failed: {e}")


# ---------------------------------------------------------------------------
# POST /api/skill-router/rebuild
# ---------------------------------------------------------------------------


@router.post("/rebuild", response_model=RebuildResponse)
async def rebuild_skill_router_index() -> RebuildResponse:
    """Trigger full ES index rebuild for all skills."""
    sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))
    sys.path.insert(0, str(_PROJECT_ROOT / "backend" / "packages" / "harness"))

    try:
        from deerflow.routing.index_updater import update_single_skill_index

        skills_root = _get_skills_root()
        results: list[RebuildResult] = []
        skills_indexed = 0

        for category in ("custom", "public"):
            cat_dir = skills_root / category
            if not cat_dir.is_dir():
                continue
            for d in sorted(cat_dir.iterdir()):
                if d.is_dir() and (d / "SKILL.md").exists():
                    skill_id = d.name
                    try:
                        result = update_single_skill_index(skill_id=skill_id, skill_dir=d, skills_root=skills_root)
                        results.append(RebuildResult(
                            skill_id=skill_id,
                            status=result.router_status,
                            error=result.router_error,
                        ))
                        if result.router_status == "ready":
                            skills_indexed += 1
                    except Exception as e:
                        results.append(RebuildResult(
                            skill_id=skill_id,
                            status="error",
                            error=str(e),
                        ))

        return RebuildResponse(
            success=True,
            message=f"Rebuilt {skills_indexed}/{len(results)} skills",
            skills_indexed=skills_indexed,
            errors=[r for r in results if r.status != "ready"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Rebuild failed: {e}")


# ---------------------------------------------------------------------------
# GET /api/skill-router/metrics
# ---------------------------------------------------------------------------


@router.get("/metrics")
async def get_skill_router_metrics() -> dict:
    """Return routing metrics snapshot."""
    try:
        from deerflow.routing.metrics import get_metrics_snapshot
        return get_metrics_snapshot()
    except ImportError:
        return {"error": "metrics module not available"}
