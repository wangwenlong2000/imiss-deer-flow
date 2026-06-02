"""审计动作 / 闸门 / 场景 / 原因码枚举常量。

spec 2026-05-27 §4。本模块仅依赖 Python 标准库。

风险值脱敏护栏意图: 后续 Task 6 的 build_evidence_action / validate_evidence_action
将拒绝任何含原始敏感值的 risk_locations —— 审计日志不得成为二次泄露源。
dataclass / Any 已在此预导入供 Task 6 直接使用。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .schema import SENSITIVITY_LEVELS as _SENSITIVITY_LEVELS_AUDIT

# spec §4.2: action 枚举
AUDIT_ACTIONS: frozenset[str] = frozenset({
    "allow", "warn", "desensitize", "aggregate",
    "rewrite", "filter", "refuse", "manual_review",
})

# spec §4.2: action_status 枚举
AUDIT_ACTION_STATUSES: frozenset[str] = frozenset({"applied", "pending", "failed"})

# spec §4.1: gate 枚举
AUDIT_GATES: frozenset[str] = frozenset({"InputGate", "ContextGate", "OutputGate"})

# spec §4.1: scene 枚举
AUDIT_SCENES: frozenset[str] = frozenset({
    "self_use", "internal_org", "cross_org", "public_release", "research_anon",
})

# spec §4.2: reason_code 初始集(首版按合规反馈,后续可扩)
AUDIT_REASON_CODES: frozenset[str] = frozenset({
    "struct_id_detected",
    "geo_loc_with_object_time",
    "re_identify_combo_risk",
    "secret_token_detected",
    "small_sample_k_below_threshold",
    "unmasked_face_or_plate",
    "streaming_link_present",
    "object_level_risk_label",
    "sensitive_facility_marker",
    "public_source_confirmed",
})

# ---------------------------------------------------------------------------
# 构造器与校验器
# ---------------------------------------------------------------------------

_MAX_FIELD_LEN = 128
_RISK_LOCATION_KEYS = frozenset({"field_path", "risk_type"})


def build_evidence_action(
    *,
    evidence_id: str,
    action: str,
    action_status: str,
    triggered_violation_types: list[str],
    risk_locations: list[dict[str, str]],
    reason_code: str,
    sensitivity_before: str,
    sensitivity_after: str,
) -> dict[str, Any]:
    """构造一条 ``EvidenceAction`` 记录。

    会立即调用 :func:`validate_evidence_action`,违规即抛 ``ValueError``。
    ``risk_locations[*]`` 严禁含 ``field_path`` / ``risk_type`` 之外的任何键。
    """
    record: dict[str, Any] = {
        "evidence_id": evidence_id,
        "action": action,
        "action_status": action_status,
        "triggered_violation_types": list(triggered_violation_types),
        "risk_locations": [dict(loc) for loc in risk_locations],
        "reason_code": reason_code,
        "sensitivity_before": sensitivity_before,
        "sensitivity_after": sensitivity_after,
    }
    errors = validate_evidence_action(record)
    if errors:
        raise ValueError("invalid EvidenceAction: " + "; ".join(errors))
    return record


def validate_evidence_action(record: dict[str, Any]) -> list[str]:
    """逐字段校验 EvidenceAction。返回错误信息列表,空列表 = 合规。"""
    errors: list[str] = []

    # evidence_id
    ev_id = record.get("evidence_id")
    if not isinstance(ev_id, str) or not ev_id:
        errors.append("evidence_id must be a non-empty string")
    elif len(ev_id) > _MAX_FIELD_LEN:
        errors.append(f"evidence_id length exceeds {_MAX_FIELD_LEN}")

    # action
    if record.get("action") not in AUDIT_ACTIONS:
        errors.append(f"action must be one of {sorted(AUDIT_ACTIONS)}")

    # action_status
    if record.get("action_status") not in AUDIT_ACTION_STATUSES:
        errors.append(f"action_status must be one of {sorted(AUDIT_ACTION_STATUSES)}")

    # triggered_violation_types
    tvt = record.get("triggered_violation_types")
    if not isinstance(tvt, list) or not all(isinstance(x, str) for x in tvt):
        errors.append("triggered_violation_types must be list[str]")

    # risk_locations 护栏
    risk_locs = record.get("risk_locations")
    if not isinstance(risk_locs, list):
        errors.append("risk_locations must be a list")
    else:
        for i, loc in enumerate(risk_locs):
            if not isinstance(loc, dict):
                errors.append(f"risk_locations[{i}] must be a dict")
                continue
            keys = set(loc.keys())
            if keys != _RISK_LOCATION_KEYS:
                errors.append(
                    f"risk_locations[{i}] keys must be exactly {{field_path, risk_type}}, got {sorted(keys)}"
                )
                continue
            for k in ("field_path", "risk_type"):
                v = loc.get(k)
                if not isinstance(v, str) or not v:
                    errors.append(f"risk_locations[{i}].{k} must be a non-empty string")
                elif len(v) > _MAX_FIELD_LEN:
                    errors.append(f"risk_locations[{i}].{k} length exceeds {_MAX_FIELD_LEN}")

    # reason_code
    if record.get("reason_code") not in AUDIT_REASON_CODES:
        errors.append(f"reason_code must be one of {sorted(AUDIT_REASON_CODES)}")

    # sensitivity_before/after
    for field in ("sensitivity_before", "sensitivity_after"):
        if record.get(field) not in _SENSITIVITY_LEVELS_AUDIT:
            errors.append(f"{field} must be one of {sorted(_SENSITIVITY_LEVELS_AUDIT)}")

    return errors
