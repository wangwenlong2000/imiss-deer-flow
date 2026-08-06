"""Guide-aligned profiles for the temporary manual compliance UI.

The annotation guide defines usage scenes, not a role hierarchy.  These
profiles bind one test persona to exactly one guide scene so a client cannot
combine an unrelated identity with a more permissive scene.  This remains a
test facility; only the trusted upstream resolver may claim IAM verification.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ManualComplianceProfile:
    identity: str
    scene: str
    role: str
    org_id: str

    @property
    def user_id(self) -> str:
        return f"manual-{self.identity}"


MANUAL_COMPLIANCE_PROFILES: dict[str, ManualComplianceProfile] = {
    "ordinary_user": ManualComplianceProfile("ordinary_user", "self_use", "user", "personal"),
    "internal_operator": ManualComplianceProfile(
        "internal_operator", "internal_org", "internal_operator", "city-governance"
    ),
    "cross_org_operator": ManualComplianceProfile(
        "cross_org_operator", "cross_org", "cross_org_operator", "partner-org"
    ),
    "publisher": ManualComplianceProfile("publisher", "public_release", "publisher", "publicity"),
    "researcher": ManualComplianceProfile("researcher", "research_anon", "researcher", "research"),
}


def validate_manual_context(raw: Any) -> tuple[ManualComplianceProfile | None, str]:
    """Validate a complete front-end test profile against the server whitelist."""
    if not isinstance(raw, Mapping) or raw.get("source") != "manual_ui" or raw.get("enabled") is not True:
        return None, "manual_context_missing_or_disabled"

    identity = raw.get("identity")
    profile = MANUAL_COMPLIANCE_PROFILES.get(str(identity)) if identity is not None else None
    if profile is None:
        return None, "manual_identity_unknown"
    if raw.get("scene") != profile.scene:
        return None, "manual_identity_scene_mismatch"

    user = raw.get("user_context")
    if not isinstance(user, Mapping):
        return None, "manual_user_context_missing"
    roles = user.get("roles")
    if (
        user.get("user_id") != profile.user_id
        or user.get("org_id") != profile.org_id
        or not isinstance(roles, (list, tuple))
        or tuple(str(role) for role in roles) != (profile.role,)
    ):
        return None, "manual_user_context_mismatch"

    return profile, "manual_profile_validated"


__all__ = ["MANUAL_COMPLIANCE_PROFILES", "ManualComplianceProfile", "validate_manual_context"]
