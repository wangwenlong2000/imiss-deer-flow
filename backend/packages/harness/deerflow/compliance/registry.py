"""Detector registry — the only module that loads concrete detectors.

Discovery is by convention: every ``detectors/<name>/manifest.yaml`` is found and
validated at startup. The manifest is the **single source of truth** for a
detector's capabilities; the central ``detectors.yaml`` only says which ones are
switched on and overrides parameters. Move or delete a detector directory and its
declaration goes with it — nothing else needs editing.

``engine.py`` / ``router.py`` / ``policy.py`` must never import a concrete
detector; only this module resolves entry points, and it does so from data.
``test_compliance_decoupling.py`` enforces that with an AST scan.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from deerflow.compliance.adapters import build_adapter
from deerflow.compliance.contract import (
    COST_HINTS,
    GATES,
    VIOLATION_TYPES,
    is_compatible_contract_version,
)

logger = logging.getLogger(__name__)

DETECTORS_DIR = Path(__file__).parent / "detectors"
MANIFEST_FILENAME = "manifest.yaml"


class DetectorRegistryError(ValueError):
    """Raised when a manifest is invalid or a detector cannot be registered."""


@dataclass
class DetectorRegistration:
    """A validated, ready-to-call detector."""

    detector_id: str
    adapter_name: str
    violation_types: tuple[str, ...]
    #: violation_type -> gates where this detector is allowed to report it.
    gates: dict[str, tuple[str, ...]]
    #: ``None`` means "any data type".
    data_types: tuple[str, ...] | None
    cost_hint: str
    params: dict[str, Any]
    manifest_path: Path
    adapter: Any = None
    enabled: bool = True
    #: Extra manifest keys kept for docs/eval (content_text_spec, description...).
    meta: dict[str, Any] = field(default_factory=dict)

    def handles(self, violation_type: str, gate: str) -> bool:
        return gate in self.gates.get(violation_type, ())

    def gates_for_all(self) -> set[str]:
        result: set[str] = set()
        for gates in self.gates.values():
            result.update(gates)
        return result

    def accepts_data_type(self, data_type: str | None) -> bool:
        if self.data_types is None:
            return True
        if data_type is None:
            # A unit with no declared data type is only handed to detectors that
            # accept anything; a type-specific detector would be guessing.
            return False
        return data_type in self.data_types


# ── manifest parsing / validation ───────────────────────────────────────────


def _validate_manifest(data: Mapping[str, Any], manifest_path: Path) -> dict[str, Any]:
    """Validate one manifest, returning a normalized dict.

    Every failure here is a startup failure. A compliance detector that is
    quietly misconfigured is worse than one that is missing, because the gate
    still looks green.
    """
    detector_id = data.get("detector_id")
    if not detector_id or not isinstance(detector_id, str):
        raise DetectorRegistryError(f"{manifest_path}: `detector_id` is required")

    declared_version = str(data.get("contract_version") or "")
    if not is_compatible_contract_version(declared_version):
        raise DetectorRegistryError(f"{manifest_path}: incompatible contract_version {declared_version!r}")

    violation_types = data.get("violation_types")
    if not isinstance(violation_types, list) or not violation_types:
        raise DetectorRegistryError(f"{manifest_path}: `violation_types` must be a non-empty list")
    unknown = sorted(set(violation_types) - set(VIOLATION_TYPES))
    if unknown:
        raise DetectorRegistryError(f"{manifest_path}: unknown violation type(s) {unknown}")

    gates_raw = data.get("gates")
    if not isinstance(gates_raw, dict) or not gates_raw:
        raise DetectorRegistryError(f"{manifest_path}: `gates` must be a mapping of violation_type -> [gates]")

    missing = sorted(set(violation_types) - set(gates_raw))
    if missing:
        raise DetectorRegistryError(f"{manifest_path}: `gates` is missing entries for {missing}")
    extra = sorted(set(gates_raw) - set(violation_types))
    if extra:
        raise DetectorRegistryError(f"{manifest_path}: `gates` declares undeclared violation type(s) {extra}")

    gates: dict[str, tuple[str, ...]] = {}
    for violation_type, gate_list in gates_raw.items():
        if not isinstance(gate_list, list) or not gate_list:
            raise DetectorRegistryError(f"{manifest_path}: gates.{violation_type} must be a non-empty list")
        bad_gates = sorted(set(gate_list) - set(GATES))
        if bad_gates:
            raise DetectorRegistryError(f"{manifest_path}: gates.{violation_type} has unknown gate(s) {bad_gates}")
        gates[violation_type] = tuple(gate_list)

    cost_hint = str(data.get("cost_hint") or "medium")
    if cost_hint not in COST_HINTS:
        raise DetectorRegistryError(f"{manifest_path}: unknown cost_hint {cost_hint!r}; expected one of {COST_HINTS}")

    # Guide requirement 3: the context gate is performance-sensitive and must not
    # call heavyweight models. Declaring both is a design error, so refuse to boot.
    declared_gates = {gate for gate_list in gates.values() for gate in gate_list}
    if cost_hint == "heavy" and "ContextGate" in declared_gates:
        raise DetectorRegistryError(f"{manifest_path}: detector {detector_id!r} declares cost_hint `heavy` on ContextGate. The context gate must stay light (guide §1.2.3); use a lighter method or move this detector to another gate.")

    data_types_raw = data.get("data_types", None)
    if data_types_raw is None:
        data_types: tuple[str, ...] | None = None
    elif isinstance(data_types_raw, list):
        data_types = tuple(str(item) for item in data_types_raw)
    else:
        raise DetectorRegistryError(f"{manifest_path}: `data_types` must be a list or null")

    adapter = str(data.get("adapter") or "inprocess")

    default_params = data.get("default_params") or {}
    if not isinstance(default_params, dict):
        raise DetectorRegistryError(f"{manifest_path}: `default_params` must be a mapping")

    reserved = {"detector_id", "contract_version", "violation_types", "gates", "data_types", "cost_hint", "adapter", "entry", "default_params", "command", "endpoint", "headers", "cwd", "env", "timeout_ms"}
    meta = {k: v for k, v in data.items() if k not in reserved}

    return {
        "detector_id": detector_id,
        "adapter": adapter,
        "violation_types": tuple(violation_types),
        "gates": gates,
        "data_types": data_types,
        "cost_hint": cost_hint,
        "default_params": dict(default_params),
        "meta": meta,
        "raw": dict(data),
    }


def discover_manifests(detectors_dir: Path | None = None) -> list[Path]:
    """Return every ``detectors/*/manifest.yaml``, sorted for determinism."""
    root = Path(detectors_dir) if detectors_dir is not None else DETECTORS_DIR
    if not root.is_dir():
        return []
    return sorted(p for p in root.glob(f"*/{MANIFEST_FILENAME}") if p.is_file())


def load_detectors_config(path: str | Path | None) -> dict[str, dict[str, Any]]:
    """Read ``config/compliance/detectors.yaml`` -> ``{detector_id: entry}``.

    A missing file is not an error: it means "no overrides", and every discovered
    detector uses its manifest defaults.
    """
    if not path:
        return {}
    file_path = Path(path)
    if not file_path.is_file():
        logger.warning("compliance: detectors config not found at %s; using manifest defaults", file_path)
        return {}
    data = yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}
    entries = data.get("detectors") or []
    if not isinstance(entries, list):
        raise DetectorRegistryError(f"{file_path}: `detectors` must be a list")
    result: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("id"):
            raise DetectorRegistryError(f"{file_path}: every detectors[] entry needs an `id`")
        result[str(entry["id"])] = entry
    return result


class DetectorRegistry:
    """Discovered detectors, keyed by id."""

    def __init__(self, registrations: list[DetectorRegistration]) -> None:
        self._registrations = {r.detector_id: r for r in registrations}

    def __len__(self) -> int:
        return len(self._registrations)

    def __contains__(self, detector_id: object) -> bool:
        return detector_id in self._registrations

    def get(self, detector_id: str) -> DetectorRegistration | None:
        return self._registrations.get(detector_id)

    def all(self) -> tuple[DetectorRegistration, ...]:
        return tuple(self._registrations.values())

    def enabled(self) -> tuple[DetectorRegistration, ...]:
        return tuple(r for r in self._registrations.values() if r.enabled)

    def close(self) -> None:
        for registration in self._registrations.values():
            adapter = registration.adapter
            close = getattr(adapter, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    logger.exception("compliance: detector %s failed to close", registration.detector_id)


def build_registry(*, detectors_dir: Path | None = None, config_path: str | Path | None = None, strict: bool = False) -> DetectorRegistry:
    """Discover, validate and instantiate every detector.

    With ``strict=False`` (the default) a single broken detector is logged and
    skipped so the rest of the system still boots — the fault boundary from plan
    §4.1 applies to registration too. ``strict=True`` is for tests and CI, where
    a broken manifest should fail loudly.

    Zero detectors is a completely valid state: the engine runs, finds nothing,
    and allows everything. That is what P0 ships with.
    """
    overrides = load_detectors_config(config_path)
    registrations: list[DetectorRegistration] = []

    for manifest_path in discover_manifests(detectors_dir):
        try:
            raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
            if not isinstance(raw, dict):
                raise DetectorRegistryError(f"{manifest_path}: manifest must be a mapping")
            parsed = _validate_manifest(raw, manifest_path)

            detector_id = parsed["detector_id"]
            override = overrides.get(detector_id, {})
            enabled = bool(override.get("enabled", True))

            params = dict(parsed["default_params"])
            params.update(override.get("params") or {})

            adapter = None
            if enabled:
                adapter = build_adapter(
                    adapter=parsed["adapter"],
                    detector_id=detector_id,
                    manifest=parsed["raw"],
                    params=params,
                )

            registrations.append(
                DetectorRegistration(
                    detector_id=detector_id,
                    adapter_name=parsed["adapter"],
                    violation_types=parsed["violation_types"],
                    gates=parsed["gates"],
                    data_types=parsed["data_types"],
                    cost_hint=parsed["cost_hint"],
                    params=params,
                    manifest_path=manifest_path,
                    adapter=adapter,
                    enabled=enabled,
                    meta=parsed["meta"],
                )
            )
        except Exception as exc:
            if strict:
                raise
            logger.error("compliance: skipping detector at %s: %s", manifest_path, exc)

    ids = [r.detector_id for r in registrations]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    if duplicates:
        raise DetectorRegistryError(f"duplicate detector_id(s) across manifests: {duplicates}")

    logger.info("compliance: registered %d detector(s) (%d enabled)", len(registrations), sum(1 for r in registrations if r.enabled))
    return DetectorRegistry(registrations)


# ── singleton ───────────────────────────────────────────────────────────────

_lock = threading.Lock()
_registry: DetectorRegistry | None = None


def get_registry(*, config_path: str | Path | None = None, detectors_dir: Path | None = None) -> DetectorRegistry:
    global _registry
    with _lock:
        if _registry is None:
            _registry = build_registry(detectors_dir=detectors_dir, config_path=config_path)
        return _registry


def reset_registry() -> None:
    """Drop the cached registry (tests, config reload)."""
    global _registry
    with _lock:
        if _registry is not None:
            _registry.close()
        _registry = None


__all__ = [
    "DETECTORS_DIR",
    "DetectorRegistration",
    "DetectorRegistry",
    "DetectorRegistryError",
    "build_registry",
    "discover_manifests",
    "get_registry",
    "load_detectors_config",
    "reset_registry",
]
