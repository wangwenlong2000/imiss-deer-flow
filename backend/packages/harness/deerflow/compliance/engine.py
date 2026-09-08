"""ComplianceEngine — one engine, one detector set, one matrix, three gates.

Orchestrates the six steps from plan §2:

1. normalize   (done by the caller's normalizer; the engine receives units)
2. route       candidate detectors for gate x data_type
3. detect      run them, isolated and individually timed
4. intent      combine hits with recognized intent / permissions
5. policy      violation x scene -> actions
6. audit       persist the determination and its basis

Hard rules this file keeps:

* **It never imports a concrete detector.** Only ``registry.py`` does reflective
  loading; the engine only sees the ``Detector`` protocol. Enforced by
  ``test_compliance_decoupling.py``.
* **One detector cannot take down another.** Each runs inside its own
  try/except with its own timeout; a failure becomes a diagnostic and the pass
  continues.
* **Budget overruns are reported, never silent.** Truncation writes an explicit
  warning naming how many units were skipped.
"""

from __future__ import annotations

import copy
import logging
import time
import uuid
from collections.abc import Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Any

from deerflow.compliance.audit import Auditor, NullAuditor
from deerflow.compliance.contract import DetectContext, DetectionHit, DetectionUnit, Gate
from deerflow.compliance.intent import IntentGuard
from deerflow.compliance.policy import PolicyMatrix, get_policy_matrix
from deerflow.compliance.registry import DetectorRegistry, build_registry
from deerflow.compliance.router import DetectorRouter
from deerflow.compliance.scene import ManualSceneResolver, NullSceneResolver, SceneResolver, resolve_scene_key
from deerflow.compliance.types import ComplianceDecision, DetectionRequest, Diagnostics

logger = logging.getLogger(__name__)


class ComplianceEngine:
    """Stateless-per-request orchestrator shared by all three gates."""

    def __init__(
        self,
        *,
        registry: DetectorRegistry,
        policy: PolicyMatrix,
        auditor: Auditor | None = None,
        scene_resolver: SceneResolver | None = None,
        intent_guard: IntentGuard | None = None,
        fallback_scene_key: str = "_unknown",
    ) -> None:
        self._registry = registry
        self._router = DetectorRouter(registry)
        self._policy = policy
        self._auditor = auditor or NullAuditor()
        self._scene_resolver = scene_resolver if scene_resolver is not None else NullSceneResolver()
        self._intent_guard = intent_guard or IntentGuard()
        self._fallback_scene_key = fallback_scene_key

    # ── public API ──────────────────────────────────────────────────────────

    @property
    def registry(self) -> DetectorRegistry:
        return self._registry

    @property
    def policy(self) -> PolicyMatrix:
        return self._policy

    @property
    def router(self) -> DetectorRouter:
        return self._router

    def check(
        self,
        units: Sequence[DetectionUnit],
        *,
        gate: Gate,
        request_id: str | None = None,
        thread_id: str | None = None,
        user: Any = None,
        intent: Any = None,
        model_name: str | None = None,
        budget_ms: int = 400,
        max_units: int = 32,
        audit_clean: bool = False,
        origin: dict[str, Any] | None = None,
    ) -> ComplianceDecision:
        """Convenience wrapper that builds a ``DetectionRequest`` and runs it."""
        request = DetectionRequest(
            gate=gate,
            units=tuple(units),
            request_id=request_id or f"cmp-{uuid.uuid4().hex[:12]}",
            thread_id=thread_id,
            user=user,
            intent=intent,
            model_name=model_name,
            budget_ms=budget_ms,
            max_units=max_units,
            audit_clean=audit_clean,
            origin=origin or {},
        )
        return self.run(request)

    def run(self, request: DetectionRequest) -> ComplianceDecision:
        """Execute the full pipeline for *request*."""
        started = time.perf_counter()
        diagnostics = Diagnostics(units_total=len(request.units))

        scene_key, scenes = resolve_scene_key(self._scene_resolver, request, self._fallback_scene_key)

        units = self._apply_budget(request, diagnostics)
        diagnostics.units_checked = len(units)

        ctx = DetectContext(
            gate=request.gate,
            scenes=scenes,
            user=request.user,
            intent=request.intent,
            model_name=request.model_name,
            budget_ms=request.budget_ms,
        )

        hits = self._detect_all(units, ctx, request, diagnostics, started)

        verdict = self._intent_guard.apply(hits, gate=request.gate, intent=request.intent, user=request.user)
        for warning in verdict.warnings:
            diagnostics.warn(warning)
        hits = verdict.hits

        actions, per_violation, basis, policy_warnings = self._policy.decide(hits, request.gate, scene_key)
        for warning in policy_warnings:
            diagnostics.warn(warning)

        # role_check has no auth module behind it in phase 1; say so out loud.
        if "role_check" in actions:
            verified, reason = self._intent_guard.role_check(request.user)
            if not verified and reason:
                diagnostics.warn(reason)

        diagnostics.elapsed_ms = (time.perf_counter() - started) * 1000.0

        decision = ComplianceDecision(
            request_id=request.request_id,
            gate=request.gate,
            scene_key=scene_key,
            scenes=scenes,
            hits=tuple(hits),
            actions=actions,
            per_violation_actions=dict(per_violation),
            basis=basis,
            diagnostics=diagnostics,
        )

        if hits or request.audit_clean:
            decision.audit_ref = self._auditor.record(request, decision)
        return decision

    # ── steps ───────────────────────────────────────────────────────────────

    def _apply_budget(self, request: DetectionRequest, diagnostics: Diagnostics) -> tuple[DetectionUnit, ...]:
        """Cap the unit count, reporting exactly what was skipped.

        Silent truncation is the failure mode this guards against: a gate that
        checked 32 of 400 evidence items and said nothing reads as a clean pass.
        """
        units = request.units
        if len(units) <= request.max_units:
            return units
        skipped = len(units) - request.max_units
        diagnostics.truncated = True
        diagnostics.warn(f"budget: only the first {request.max_units} of {len(units)} unit(s) were inspected; {skipped} unit(s) were NOT checked at {request.gate}")
        return units[: request.max_units]

    def _detect_all(
        self,
        units: tuple[DetectionUnit, ...],
        ctx: DetectContext,
        request: DetectionRequest,
        diagnostics: Diagnostics,
        started: float,
    ) -> list[DetectionHit]:
        hits: list[DetectionHit] = []
        budget_s = request.budget_ms / 1000.0
        exhausted = False

        for unit in units:
            if (time.perf_counter() - started) > budget_s:
                exhausted = True
                break
            frozen = self._freeze(unit)
            for registration in self._router.candidates(unit, request.gate):
                if (time.perf_counter() - started) > budget_s:
                    exhausted = True
                    break
                hits.extend(self._detect_one(registration, frozen, ctx, request.gate, diagnostics))
            if exhausted:
                break

        if exhausted:
            diagnostics.truncated = True
            diagnostics.warn(f"budget: detection stopped after {request.budget_ms}ms; not every unit/detector pair ran at {request.gate}")
        return hits

    def _detect_one(self, registration: Any, unit: DetectionUnit, ctx: DetectContext, gate: Gate, diagnostics: Diagnostics) -> list[DetectionHit]:
        """Run one detector under isolation. Never raises."""
        detector_id = registration.detector_id
        began = time.perf_counter()
        try:
            raw_hits = registration.adapter.detect(unit, ctx) or ()
        except Exception as exc:
            # Fault boundary (plan §4.1): a broken detector is a diagnostic, not
            # an outage. The gate still reports whatever the others found.
            logger.exception("compliance: detector %s failed at %s", detector_id, gate)
            diagnostics.detector_errors[detector_id] = f"{type(exc).__name__}: {exc}"
            diagnostics.warn(f"detector `{detector_id}` failed at {gate}: {type(exc).__name__}: {exc}")
            return []
        finally:
            elapsed = (time.perf_counter() - began) * 1000.0
            diagnostics.detector_timings_ms[detector_id] = round(diagnostics.detector_timings_ms.get(detector_id, 0.0) + elapsed, 3)

        kept: list[DetectionHit] = []
        for hit in raw_hits:
            # The guide pins some violation types to a single gate (re_identify is
            # output-only). Enforce it here too, so a detector bug cannot smuggle
            # a hit into the wrong gate.
            if not self._router.allows(registration, hit.violation_type, gate):
                diagnostics.warn(f"detector `{detector_id}` reported `{hit.violation_type}` at {gate}, which its manifest does not declare; hit dropped")
                continue
            kept.append(hit)
        return kept

    @staticmethod
    def _freeze(unit: DetectionUnit) -> DetectionUnit:
        """Hand detectors a read-only deep copy.

        Data boundary (plan §4.1): a detector must not be able to reach agent
        state, and must not be able to mutate a payload another detector will
        later see.
        """
        import dataclasses

        return dataclasses.replace(unit, raw=MappingProxyType(copy.deepcopy(dict(unit.raw))))


# ── construction from config ────────────────────────────────────────────────


def _repo_root() -> Path:
    """Locate the repository root so relative config paths resolve.

    ``compliance/engine.py`` lives at
    ``<root>/backend/packages/harness/deerflow/compliance/engine.py``.
    """
    return Path(__file__).resolve().parents[5]


def resolve_config_path(path: str | Path) -> Path:
    """Resolve a possibly-relative config path against the repo root."""
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    from_cwd = Path.cwd() / candidate
    if from_cwd.exists():
        return from_cwd
    return _repo_root() / candidate


def build_engine(config: Any = None) -> ComplianceEngine:
    """Build an engine from the compliance configuration.

    Zero registered detectors is a supported state — the engine loads, routes to
    nobody, and allows everything. That is exactly what P0 ships.
    """
    if config is None:
        from deerflow.config.compliance_config import get_compliance_config

        config = get_compliance_config()

    registry = build_registry(config_path=resolve_config_path(config.detectors_config_path))
    policy = get_policy_matrix(resolve_config_path(config.policy_matrix_path))

    auditor: Auditor
    if config.audit.enabled:
        auditor = Auditor(path=resolve_config_path(config.audit.path), enabled=True, retain_days=config.audit.retain_days)
    else:
        auditor = NullAuditor()

    # Scene resolution is an authorization boundary.  Keep the null resolver
    # as the production default until a trusted upstream/IAM contract is
    # explicitly enabled and configured.
    scene_resolver: SceneResolver = NullSceneResolver()
    resolver_mode = getattr(config, "scene_resolver_mode", None) or "null"
    if resolver_mode == "manual_ui":
        scene_resolver = ManualSceneResolver()
    elif resolver_mode == "trusted_upstream" and config.scene.resolver:
        # Same reflective-load path as detectors: configured by data, not code.
        from deerflow.compliance.adapters.inprocess import _resolve_entry

        target = _resolve_entry(config.scene.resolver)
        scene_resolver = target() if isinstance(target, type) else target

    return ComplianceEngine(
        registry=registry,
        policy=policy,
        auditor=auditor,
        scene_resolver=scene_resolver,
        fallback_scene_key=config.scene.fallback_key,
    )


_engine: ComplianceEngine | None = None


def get_engine(config: Any = None) -> ComplianceEngine:
    """Process-wide engine singleton (model load is not on the request path)."""
    global _engine
    if _engine is None:
        _engine = build_engine(config)
    return _engine


def reset_engine() -> None:
    """Drop the cached engine (tests, config reload)."""
    global _engine
    if _engine is not None:
        _engine.registry.close()
    _engine = None


__all__ = ["ComplianceEngine", "build_engine", "get_engine", "reset_engine", "resolve_config_path"]
