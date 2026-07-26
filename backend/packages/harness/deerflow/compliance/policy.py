"""Disposition matrix: ``violation x gate x scene -> actions``.

Identification and disposition are decoupled (guide §9.1): detectors say *what*
was hit, this module says *what happens about it*. The matrix lives in YAML
(``config/compliance/policy_matrix.yaml``) rather than in code so that the guide
can be re-transcribed without a code change.

This module must never import a concrete detector — enforced by
``test_compliance_decoupling.py``.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from deerflow.compliance.contract import (
    ACTIONS,
    BASELINE_VIOLATION_TYPES,
    GATES,
    UNKNOWN_SCENE_KEY,
    VIOLATION_TYPES,
    Action,
    DetectionHit,
    Gate,
)
from deerflow.compliance.types import rank_action

logger = logging.getLogger(__name__)


class PolicyMatrixError(ValueError):
    """Raised when the matrix file is missing, malformed, or inconsistent."""


@dataclass(frozen=True)
class PolicyResolution:
    """What the matrix says for one violation type."""

    violation_type: str
    gate: Gate
    scene_key: str
    actions: tuple[Action, ...]
    basis: tuple[str, ...]
    #: True when the matrix cell is `null` ("—" in the guide): this violation is
    #: not supposed to be reachable at this gate/scene at all.
    not_applicable: bool = False


class PolicyMatrix:
    """Loaded, validated disposition matrix."""

    def __init__(self, data: Mapping[str, Any], *, source_path: Path | None = None) -> None:
        self._raw = dict(data)
        self._source_path = source_path
        self.version = str(data.get("version") or "unknown")
        self.fallback_key = str(data.get("fallback_key") or UNKNOWN_SCENE_KEY)
        self._matrix: dict[str, dict[str, dict[str, tuple[str, ...] | None]]] = {}
        self._basis: dict[str, tuple[str, ...]] = {}
        self._load(data)

    # ── loading & validation ────────────────────────────────────────────────

    def _load(self, data: Mapping[str, Any]) -> None:
        matrix = data.get("matrix")
        if not isinstance(matrix, dict):
            raise PolicyMatrixError("policy matrix file has no `matrix` mapping")

        unknown_types = sorted(set(matrix) - set(VIOLATION_TYPES))
        if unknown_types:
            raise PolicyMatrixError(f"unknown violation type(s) in matrix: {unknown_types}")

        missing_types = sorted(set(VIOLATION_TYPES) - set(matrix))
        if missing_types:
            raise PolicyMatrixError(f"matrix is missing violation type(s): {missing_types}")

        for violation_type, gates in matrix.items():
            if not isinstance(gates, dict):
                raise PolicyMatrixError(f"{violation_type}: expected a mapping of gate -> scenes")
            unknown_gates = sorted(set(gates) - set(GATES))
            if unknown_gates:
                raise PolicyMatrixError(f"{violation_type}: unknown gate(s) {unknown_gates}")
            missing_gates = sorted(set(GATES) - set(gates))
            if missing_gates:
                raise PolicyMatrixError(f"{violation_type}: missing gate(s) {missing_gates}")

            per_gate: dict[str, dict[str, tuple[str, ...] | None]] = {}
            for gate, scenes in gates.items():
                if not isinstance(scenes, dict):
                    raise PolicyMatrixError(f"{violation_type}.{gate}: expected a mapping of scene -> actions")
                if self.fallback_key not in scenes:
                    raise PolicyMatrixError(f"{violation_type}.{gate}: missing required fallback column `{self.fallback_key}`")
                per_scene: dict[str, tuple[str, ...] | None] = {}
                for scene, actions in scenes.items():
                    if actions is None:
                        per_scene[scene] = None
                        continue
                    if not isinstance(actions, list) or not actions:
                        raise PolicyMatrixError(f"{violation_type}.{gate}.{scene}: expected a non-empty list of actions or null")
                    invalid = sorted(set(actions) - set(ACTIONS))
                    if invalid:
                        raise PolicyMatrixError(f"{violation_type}.{gate}.{scene}: unknown action(s) {invalid}")
                    per_scene[scene] = tuple(actions)
                per_gate[gate] = per_scene
            self._matrix[violation_type] = per_gate

        basis = data.get("basis") or {}
        if not isinstance(basis, dict):
            raise PolicyMatrixError("`basis` must be a mapping of violation_type -> list of clauses")
        self._basis = {str(k): tuple(str(item) for item in (v or ())) for k, v in basis.items()}

        self._validate_baseline_types()

    def _validate_baseline_types(self) -> None:
        """The three baseline violations must block even when the scene is unknown.

        The guide's chapter 3 core rules are explicit: hardcoded credentials,
        illegal content and politically sensitive content are refused in *every*
        scene. If a re-transcription ever softens one of those cells, this fails
        at load time rather than silently opening a hole in production.
        """
        for violation_type in BASELINE_VIOLATION_TYPES:
            for gate, scenes in self._matrix[violation_type].items():
                actions = scenes.get(self.fallback_key)
                if not actions or "refuse" not in actions:
                    raise PolicyMatrixError(f"baseline violation `{violation_type}` at {gate}.{self.fallback_key} must include `refuse`, got {actions}")

    # ── lookup ──────────────────────────────────────────────────────────────

    def resolve(self, violation_type: str, gate: Gate, scene_key: str | None = None) -> PolicyResolution:
        """Resolve one ``violation x gate x scene`` cell.

        An unresolved scene falls back to the configured fallback column rather
        than to "allow" — an unknown scene is a reason for caution, not licence.
        """
        key = scene_key or self.fallback_key
        gates = self._matrix.get(violation_type)
        if gates is None:
            raise PolicyMatrixError(f"no matrix entry for violation type {violation_type!r}")
        scenes = gates.get(gate)
        if scenes is None:
            raise PolicyMatrixError(f"no matrix entry for {violation_type}.{gate}")

        if key in scenes:
            actions = scenes[key]
        else:
            logger.warning("policy matrix: unknown scene %r for %s.%s; using fallback %r", key, violation_type, gate, self.fallback_key)
            actions = scenes[self.fallback_key]
            key = self.fallback_key

        if actions is None:
            # "—" in the guide. Reaching this means a detector fired where the
            # matrix says it should not; surface it instead of silently allowing.
            return PolicyResolution(
                violation_type=violation_type,
                gate=gate,
                scene_key=key,
                actions=(),
                basis=self._basis.get(violation_type, ()),
                not_applicable=True,
            )

        return PolicyResolution(
            violation_type=violation_type,
            gate=gate,
            scene_key=key,
            actions=tuple(actions),
            basis=self._basis.get(violation_type, ()),
        )

    def decide(self, hits: "list[DetectionHit] | tuple[DetectionHit, ...]", gate: Gate, scene_key: str | None = None) -> tuple[tuple[Action, ...], dict[str, tuple[Action, ...]], tuple[str, ...], list[str]]:
        """Merge the matrix verdicts for every hit into one disposition.

        Returns ``(merged_actions, per_violation_actions, basis, warnings)``.
        Merged actions are deduplicated and sorted weakest-first, so callers can
        read ``actions[-1]`` for the strongest one.
        """
        per_violation: dict[str, tuple[Action, ...]] = {}
        merged: set[str] = set()
        basis: list[str] = []
        warnings: list[str] = []

        for hit in hits:
            if hit.violation_type in per_violation:
                continue
            resolution = self.resolve(hit.violation_type, gate, scene_key)
            if resolution.not_applicable:
                warnings.append(f"detector `{hit.detector_id}` reported `{hit.violation_type}` at {gate}/{resolution.scene_key}, where the matrix declares it not applicable; no action taken")
                continue
            per_violation[hit.violation_type] = resolution.actions
            merged.update(resolution.actions)
            for clause in resolution.basis:
                if clause not in basis:
                    basis.append(clause)

        # "allow" alongside anything stronger is noise; drop it.
        if len(merged) > 1:
            merged.discard("allow")

        ordered = tuple(sorted(merged, key=rank_action))  # type: ignore[arg-type]
        return ordered, per_violation, tuple(basis), warnings

    # ── introspection (tests, docs, eval) ───────────────────────────────────

    @property
    def violation_types(self) -> tuple[str, ...]:
        return tuple(self._matrix)

    def cell(self, violation_type: str, gate: Gate, scene_key: str) -> tuple[str, ...] | None:
        return self._matrix[violation_type][gate][scene_key]

    def scene_keys(self, violation_type: str, gate: Gate) -> tuple[str, ...]:
        return tuple(self._matrix[violation_type][gate])


# ── loading helpers / singleton ─────────────────────────────────────────────

_lock = threading.Lock()
_cached: tuple[str, PolicyMatrix] | None = None


def load_policy_matrix(path: str | Path) -> PolicyMatrix:
    """Load and validate a matrix file. Raises on any inconsistency."""
    file_path = Path(path)
    if not file_path.is_file():
        raise PolicyMatrixError(f"policy matrix file not found: {file_path}")
    try:
        data = yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise PolicyMatrixError(f"policy matrix file is not valid YAML: {file_path}: {exc}") from exc
    return PolicyMatrix(data, source_path=file_path)


def get_policy_matrix(path: str | Path) -> PolicyMatrix:
    """Return a process-wide cached matrix for *path*."""
    global _cached
    key = str(Path(path).resolve())
    with _lock:
        if _cached is not None and _cached[0] == key:
            return _cached[1]
        matrix = load_policy_matrix(path)
        _cached = (key, matrix)
        return matrix


def reset_policy_matrix_cache() -> None:
    """Drop the cached matrix (tests, config reload)."""
    global _cached
    with _lock:
        _cached = None


__all__ = [
    "PolicyMatrix",
    "PolicyMatrixError",
    "PolicyResolution",
    "get_policy_matrix",
    "load_policy_matrix",
    "reset_policy_matrix_cache",
]
