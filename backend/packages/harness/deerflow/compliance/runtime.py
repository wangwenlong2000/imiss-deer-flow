"""Shared runtime helpers for the three gate middlewares.

Keeps engine construction, the enabled check and the fail-mode decision in one
place so all three gates behave identically when things go wrong.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from deerflow.compliance.contract import Gate, UserContext
from deerflow.compliance.types import ComplianceDecision, Diagnostics

logger = logging.getLogger(__name__)

#: Shown when `fail_mode: closed` blocks content because detection itself broke.
FAIL_CLOSED_NOTICE = "【合规检查失败】合规检测未能完成，按 fail_mode=closed 策略拦截本次内容。请联系管理员查看合规检测服务状态。"


def get_compliance_config() -> Any:
    from deerflow.config.compliance_config import get_compliance_config as _get

    return _get()


def gate_config(gate: Gate) -> Any:
    """Return the per-gate configuration block."""
    gates = get_compliance_config().gates
    return {"InputGate": gates.input, "ContextGate": gates.context, "OutputGate": gates.output}[gate]


def gate_enabled(gate: Gate) -> bool:
    """A gate runs only when both the master switch and its own switch are on."""
    config = get_compliance_config()
    return bool(config.enabled and gate_config(gate).enabled)


def get_engine() -> Any:
    """Return the process-wide engine singleton."""
    from deerflow.compliance.engine import get_engine as _get

    return _get()


def runtime_context(value: Any) -> Mapping[str, Any]:
    """Extract LangGraph runtime context from a hook request or runtime."""
    if isinstance(value, Mapping):
        runtime = value.get("runtime", value)
        context = runtime.get("context", runtime) if isinstance(runtime, Mapping) else runtime
    else:
        runtime = getattr(value, "runtime", value)
        context = getattr(runtime, "context", runtime)
    return context if isinstance(context, Mapping) else {}


def compliance_context(value: Any) -> dict[str, Any]:
    """Return the manually selected compliance context, if one was supplied."""
    raw = runtime_context(value).get("compliance_context")
    return dict(raw) if isinstance(raw, Mapping) else {}


def user_context(value: Any) -> UserContext | None:
    """Convert the request context's user profile into the detector contract."""
    raw_user = compliance_context(value).get("user_context")
    if not isinstance(raw_user, Mapping):
        return None
    roles = raw_user.get("roles")
    return UserContext(
        user_id=str(raw_user.get("user_id")) if raw_user.get("user_id") is not None else None,
        roles=tuple(str(role) for role in roles) if isinstance(roles, (list, tuple)) else (),
        org_id=str(raw_user.get("org_id")) if raw_user.get("org_id") is not None else None,
    )


def failure_decision(gate: Gate, request_id: str, error: Exception) -> ComplianceDecision:
    """Build the decision used when detection itself raised.

    With ``fail_mode: closed`` this carries a ``refuse`` action, because a
    compliance gate that fails open is indistinguishable from one that is
    switched off — and the whole point is that content does not flow unchecked.
    """
    diagnostics = Diagnostics(failed_closed=True)
    diagnostics.warn(f"{gate}: compliance detection failed: {type(error).__name__}: {error}")

    mode = getattr(gate_config(gate), "fail_mode", "closed")
    actions = ("refuse",) if mode == "closed" else ()
    if mode != "closed":
        diagnostics.warn(f"{gate}: fail_mode=open, content passed through UNCHECKED")

    return ComplianceDecision(
        request_id=request_id,
        gate=gate,
        scene_key=get_compliance_config().scene.fallback_key,
        actions=actions,
        diagnostics=diagnostics,
    )


def user_notice(decision: ComplianceDecision) -> str:
    """Short, honest, user-facing summary of a compliance determination."""
    types = sorted({hit.violation_type for hit in decision.hits})
    parts = ["【合规提示】"]
    if types:
        parts.append(f"检测到合规风险类型：{', '.join(types)}。")
    parts.append(f"处置动作：{', '.join(decision.actions) or '无'}。")
    if decision.basis:
        parts.append(f"依据：{'；'.join(decision.basis)}。")
    if decision.audit_ref:
        parts.append(f"审计编号：{decision.audit_ref}。")
    return " ".join(parts)


__all__ = [
    "FAIL_CLOSED_NOTICE",
    "failure_decision",
    "gate_config",
    "gate_enabled",
    "get_compliance_config",
    "get_engine",
    "compliance_context",
    "runtime_context",
    "user_context",
    "user_notice",
]
