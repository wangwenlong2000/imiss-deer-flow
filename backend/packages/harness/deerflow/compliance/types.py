"""Engine-internal data contracts.

Distinct from :mod:`deerflow.compliance.contract`: those types cross the detector
boundary and are frozen forever; these belong to the engine and may evolve with
it. Detector authors must not import this module.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from deerflow.compliance.contract import (
    ACTIONS,
    MUTATING_ACTIONS,
    Action,
    DetectionHit,
    DetectionUnit,
    Gate,
    IntentInfo,
    Scene,
    UserContext,
)

#: Action severity order, least to most disruptive. Used to merge the actions of
#: several hits into one decision without letting a weak hit soften a strong one.
_ACTION_RANK = {action: index for index, action in enumerate(ACTIONS)}


def rank_action(action: str) -> int:
    """Return the disruptiveness rank of *action* (unknown actions sort last)."""
    return _ACTION_RANK.get(action, len(_ACTION_RANK))


def strongest_action(actions: list[str] | tuple[str, ...]) -> str | None:
    """Return the most disruptive action in *actions*."""
    return max(actions, key=rank_action) if actions else None


@dataclass(frozen=True)
class DetectionRequest:
    """One call into the engine — a batch of units checked at a single gate."""

    gate: Gate
    units: tuple[DetectionUnit, ...]
    request_id: str
    thread_id: str | None = None
    user: UserContext | None = None
    intent: IntentInfo | None = None
    budget_ms: int = 400
    max_units: int = 32
    #: Free-form provenance for the audit trail (tool name, message id, ...).
    origin: Mapping[str, Any] = field(default_factory=dict)
    model_name: str | None = None


@dataclass
class Diagnostics:
    """Everything that went sideways, kept explicit rather than swallowed."""

    warnings: list[str] = field(default_factory=list)
    #: detector_id -> wall time spent in that detector.
    detector_timings_ms: dict[str, float] = field(default_factory=dict)
    #: detector_id -> error string. A failing detector never blocks the others.
    detector_errors: dict[str, str] = field(default_factory=dict)
    units_total: int = 0
    units_checked: int = 0
    truncated: bool = False
    elapsed_ms: float = 0.0
    #: True when the engine itself failed and `fail_mode: closed` forced a block.
    failed_closed: bool = False

    def warn(self, message: str) -> None:
        self.warnings.append(message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "warnings": list(self.warnings),
            "detector_timings_ms": dict(self.detector_timings_ms),
            "detector_errors": dict(self.detector_errors),
            "units_total": self.units_total,
            "units_checked": self.units_checked,
            "truncated": self.truncated,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "failed_closed": self.failed_closed,
        }


@dataclass
class ComplianceDecision:
    """The engine's verdict: what was hit, and what should happen about it."""

    request_id: str
    gate: Gate
    scene_key: str
    scenes: tuple[Scene, ...] = ()
    hits: tuple[DetectionHit, ...] = ()
    #: Union of all per-violation actions, deduplicated, weakest first.
    actions: tuple[Action, ...] = ()
    #: violation_type -> actions from the matrix, kept for audit granularity.
    per_violation_actions: dict[str, tuple[Action, ...]] = field(default_factory=dict)
    #: Rewritten / desensitized payload when an action produced one.
    mutated_payload: str | None = None
    #: Pointer into the audit log so a decision can always be traced back.
    audit_ref: str | None = None
    #: Compliance clauses backing the decision (guide requirement 5).
    basis: tuple[str, ...] = ()
    diagnostics: Diagnostics = field(default_factory=Diagnostics)

    @property
    def violated(self) -> bool:
        """True when at least one detector hit."""
        return bool(self.hits)

    @property
    def strongest(self) -> str | None:
        """The most disruptive action across all violations."""
        return strongest_action(list(self.actions))

    def requires(self, action: str) -> bool:
        return action in self.actions

    @property
    def blocks(self) -> bool:
        """True when the content must not pass through unchanged."""
        return any(action in MUTATING_ACTIONS for action in self.actions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "gate": self.gate,
            "scene_key": self.scene_key,
            "scenes": list(self.scenes),
            "actions": list(self.actions),
            "per_violation_actions": {k: list(v) for k, v in self.per_violation_actions.items()},
            "basis": list(self.basis),
            "audit_ref": self.audit_ref,
            "hits": [
                {
                    "detector_id": hit.detector_id,
                    "violation_type": hit.violation_type,
                    "confidence": round(float(hit.confidence), 4),
                    "severity": hit.severity,
                    "reason_code": hit.reason_code,
                    "risk_locations": [
                        {"kind": loc.kind, "locator": loc.locator, "text": loc.text, "entity_type": loc.entity_type}
                        for loc in hit.risk_locations
                    ],
                    "basis": list(hit.basis),
                    "evidence": dict(hit.evidence),
                }
                for hit in self.hits
            ],
            "diagnostics": self.diagnostics.to_dict(),
        }


def allow_decision(request_id: str, gate: Gate, scene_key: str, diagnostics: Diagnostics | None = None) -> ComplianceDecision:
    """A clean pass — no hits, no actions."""
    return ComplianceDecision(
        request_id=request_id,
        gate=gate,
        scene_key=scene_key,
        diagnostics=diagnostics or Diagnostics(),
    )


__all__ = [
    "ComplianceDecision",
    "DetectionRequest",
    "Diagnostics",
    "allow_decision",
    "rank_action",
    "strongest_action",
]
