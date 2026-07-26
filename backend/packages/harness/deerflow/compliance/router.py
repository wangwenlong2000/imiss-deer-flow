"""Candidate detector routing.

Picks which registered detectors should look at a given unit, based on
``gate x data_type x cost``. Routing is data-driven from manifests; this module
must never import a concrete detector (enforced by the decoupling test).

Several detectors may claim the same violation type on purpose — a model-based
detector and a rule-based baseline running side by side is how the audit trail
gets a second opinion.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from deerflow.compliance.contract import DetectionUnit, Gate

if TYPE_CHECKING:  # pragma: no cover - typing only, no runtime import
    from deerflow.compliance.registry import DetectorRegistration, DetectorRegistry

logger = logging.getLogger(__name__)

#: Cost ordering used to run cheap detectors first, so that a tight budget
#: spends what it has on the detectors most likely to finish.
_COST_ORDER = {"light": 0, "medium": 1, "heavy": 2}


class DetectorRouter:
    """Resolve the candidate detector set for a unit at a gate."""

    def __init__(self, registry: "DetectorRegistry") -> None:
        self._registry = registry

    def candidates(self, unit: DetectionUnit, gate: Gate) -> tuple["DetectorRegistration", ...]:
        """Detectors that declare *gate* and accept the unit's data type."""
        selected = [
            registration
            for registration in self._registry.enabled()
            if gate in registration.gates_for_all() and registration.accepts_data_type(unit.data_type)
        ]
        selected.sort(key=lambda r: (_COST_ORDER.get(r.cost_hint, 1), r.detector_id))
        return tuple(selected)

    def violation_types_for(self, gate: Gate) -> tuple[str, ...]:
        """Which violation types are actually covered at *gate*.

        Coverage gaps are reportable facts, not something to paper over: the
        evaluation report prints this so an empty column never reads as a pass.
        """
        covered: set[str] = set()
        for registration in self._registry.enabled():
            for violation_type, gates in registration.gates.items():
                if gate in gates:
                    covered.add(violation_type)
        return tuple(sorted(covered))

    def allows(self, registration: "DetectorRegistration", violation_type: str, gate: Gate) -> bool:
        """Whether *registration* may report *violation_type* at *gate*.

        Applied to results as well as to routing: the guide pins some violation
        types to a single gate (``re_identify`` is output-only), and a detector
        that reports one elsewhere gets that hit dropped.
        """
        return registration.handles(violation_type, gate)


__all__ = ["DetectorRouter"]
