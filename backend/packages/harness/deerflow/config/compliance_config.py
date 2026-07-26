"""Compliance subsystem configuration (maps to the `compliance` section of config.yaml).

Same shape as the other ``*_config.py`` modules: Pydantic models plus a module
singleton loaded by ``AppConfig.from_file()``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

#: ``closed`` = a detection failure blocks (fail-safe); ``open`` = it passes through.
FailMode = Literal["closed", "open"]


class GateConfig(BaseModel):
    """Shared settings for one of the three gates."""

    enabled: bool = Field(default=True)
    fail_mode: FailMode = Field(
        default="closed",
        description=(
            "What happens when detection itself errors out. `closed` blocks and alerts; "
            "`open` lets content through. Default `closed`: a compliance gate that fails "
            "open is indistinguishable from a gate that is switched off."
        ),
    )
    budget_ms: int = Field(default=400, ge=1, description="Wall-clock budget for the whole detection pass.")
    max_units: int = Field(default=32, ge=1, description="Cap on units inspected per call; the excess is reported, never dropped silently.")

    model_config = ConfigDict(extra="ignore")


class IncrementalScanConfig(BaseModel):
    """Output gate streaming scan settings."""

    enabled: bool = Field(default=True)
    interval_chars: int = Field(
        default=200,
        ge=1,
        description=(
            "Characters accumulated between incremental scans. Directly trades leak "
            "window against CPU: smaller means the retraction fires sooner but costs "
            "more detector calls."
        ),
    )

    model_config = ConfigDict(extra="ignore")


class OutputGateConfig(GateConfig):
    """Output gate: streaming with retract-on-hit; never buffered."""

    mode: Literal["stream_retract"] = Field(
        default="stream_retract",
        description="Only `stream_retract` is supported: keep streaming, retract when a violation lands.",
    )
    incremental_scan: IncrementalScanConfig = Field(default_factory=IncrementalScanConfig)

    model_config = ConfigDict(extra="ignore")


class GatesConfig(BaseModel):
    input: GateConfig = Field(default_factory=GateConfig)
    context: GateConfig = Field(default_factory=GateConfig)
    output: OutputGateConfig = Field(default_factory=OutputGateConfig)

    model_config = ConfigDict(extra="ignore")


class SceneConfig(BaseModel):
    """Scene resolution. Phase 1 has no information source — see plan §7.2."""

    resolver: str | None = Field(
        default=None,
        description="Entry point (`module.path:ClassName`) of a SceneResolver. null = NullSceneResolver, everything falls back to `_unknown`.",
    )
    fallback_key: str = Field(default="_unknown", description="Matrix column used when the scene cannot be resolved.")

    model_config = ConfigDict(extra="ignore")


class AuditConfig(BaseModel):
    enabled: bool = Field(default=True)
    path: str = Field(default="backend/.deer-flow/compliance/audit")
    retain_days: int = Field(default=90, ge=0)

    model_config = ConfigDict(extra="ignore")


class ComplianceConfig(BaseModel):
    """Top-level compliance configuration."""

    enabled: bool = Field(default=False, description="Master switch. Off by default so an incomplete rollout cannot half-block traffic.")
    gates: GatesConfig = Field(default_factory=GatesConfig)
    detectors_config_path: str = Field(default="config/compliance/detectors.yaml")
    policy_matrix_path: str = Field(default="config/compliance/policy_matrix.yaml")
    scene: SceneConfig = Field(default_factory=SceneConfig)
    audit: AuditConfig = Field(default_factory=AuditConfig)

    model_config = ConfigDict(extra="ignore")


# ── singleton (mirrors skill_router_config.py) ──────────────────────────────

_compliance_config: ComplianceConfig = ComplianceConfig()


def get_compliance_config() -> ComplianceConfig:
    """Get the current compliance configuration."""
    return _compliance_config


def set_compliance_config(config: ComplianceConfig) -> None:
    """Set the compliance configuration."""
    global _compliance_config
    _compliance_config = config


def load_compliance_config_from_dict(config_dict: dict) -> None:
    """Load compliance configuration from a dictionary."""
    global _compliance_config
    _compliance_config = ComplianceConfig(**(config_dict or {}))


__all__ = [
    "AuditConfig",
    "ComplianceConfig",
    "GateConfig",
    "GatesConfig",
    "IncrementalScanConfig",
    "OutputGateConfig",
    "SceneConfig",
    "get_compliance_config",
    "load_compliance_config_from_dict",
    "set_compliance_config",
]
