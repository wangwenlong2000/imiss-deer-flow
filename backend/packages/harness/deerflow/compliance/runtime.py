"""Shared runtime helpers for the three gate middlewares.

Keeps engine construction, the enabled check and the fail-mode decision in one
place so all three gates behave identically when things go wrong.
"""

from __future__ import annotations

import logging
from typing import Any

from deerflow.compliance.contract import Gate
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
    "user_notice",
]
