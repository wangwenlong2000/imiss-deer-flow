"""Scene resolution — deliberately a no-op in phase 1.

The system has no source of truth for "how widely may this data be used"
(self_use / internal_org / cross_org / public_release / research_anon), so
phase 1 resolves every request to *unknown* and the policy matrix falls back to
its ``_unknown`` column. See plan §7.2.

The seam exists so that wiring in a real resolver later (auth module, request
parameter, agent config) is a config change, not an engine change.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from deerflow.compliance.contract import SCENES, UNKNOWN_SCENE_KEY, Scene
from deerflow.compliance.types import DetectionRequest

logger = logging.getLogger(__name__)


@runtime_checkable
class SceneResolver(Protocol):
    """Maps a request onto a usage scene, or ``None`` when it cannot tell."""

    def resolve(self, request: DetectionRequest) -> Scene | None: ...


class NullSceneResolver:
    """Phase 1 placeholder. Always undecided.

    Returning ``None`` is a deliberate, honest answer: guessing a scene would be
    worse than admitting we do not know, because the matrix's ``_unknown`` column
    is tuned to be safe under exactly that uncertainty.
    """

    def resolve(self, request: DetectionRequest) -> Scene | None:  # noqa: ARG002 - protocol shape
        return None


class ManualSceneResolver:
    """Resolve the temporary front-end-selected scene.

    This is a development/test resolver. It enables policy-path verification,
    but it is not an authentication or authorization decision.
    """

    def resolve(self, request: DetectionRequest) -> Scene | None:
        raw = request.origin.get("compliance_context")
        if not isinstance(raw, Mapping):
            return None
        if raw.get("enabled") is not True:
            return None
        scene = raw.get("scene")
        return scene if scene in SCENES else None  # type: ignore[return-value]


def resolve_scene_key(resolver: SceneResolver | None, request: DetectionRequest, fallback_key: str = UNKNOWN_SCENE_KEY) -> tuple[str, tuple[Scene, ...]]:
    """Return ``(matrix_column_key, scenes_tuple)`` for *request*.

    A resolver that raises is treated as undecided rather than fatal — scene
    resolution is an enrichment, and losing it must not take the gate down.
    """
    if resolver is None:
        return fallback_key, ()
    try:
        scene = resolver.resolve(request)
    except Exception:
        logger.exception("SceneResolver failed; falling back to %s", fallback_key)
        return fallback_key, ()
    if not scene:
        return fallback_key, ()
    return scene, (scene,)


__all__ = ["ManualSceneResolver", "NullSceneResolver", "SceneResolver", "resolve_scene_key"]
