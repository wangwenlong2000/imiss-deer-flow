"""Authenticated namespaces for persistent memory.

The gateway creates a thread-bound signature after authenticating the actor.
Runtime code never treats a caller supplied user ID or compliance profile as
proof of ownership. Signatures deliberately have no execution-time expiry:
queued and long-running tasks retain their namespace, and never fall back to
global memory when a credential is missing or invalid.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

MEMORY_SCOPE_CONTEXT_KEY = "memory_scope"


@dataclass(frozen=True)
class MemoryScope:
    org_id: str
    user_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.org_id, str) or not self.org_id.strip():
            raise ValueError("Memory organization is required")
        if not isinstance(self.user_id, str) or not self.user_id.strip():
            raise ValueError("Memory user is required")

    @property
    def key(self) -> str:
        # JSON preserves the boundary even when identifiers contain separators.
        return hashlib.sha256(_canonical([self.org_id, self.user_id])).hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def memory_isolation_required() -> bool:
    """Return whether an unscoped read/write would expose another user's data."""
    if os.getenv("MOBILE_IAM_PROVIDER", "").strip():
        return True
    configured = os.getenv("MOBILE_IAM_REQUIRED", "false").strip().lower()
    if configured not in {"", "0", "false", "no", "off"}:
        # Invalid settings also fail closed; the gateway reports configuration
        # errors to callers, while harness hooks simply omit untrusted memory.
        return True
    from deerflow.config.compliance_config import get_compliance_config

    return getattr(get_compliance_config(), "scene_resolver_mode", None) == "trusted_upstream"


def _signing_key() -> bytes:
    key = os.getenv("DEER_FLOW_ADMISSION_SIGNING_KEY", "").encode("utf-8")
    if len(key) < 32:
        raise PermissionError("Authenticated memory signing is unavailable")
    return key


def build_memory_scope_context(org_id: str, user_id: str, thread_id: str) -> dict[str, Any]:
    """Build a namespace assertion only at an authenticated server boundary."""
    scope = MemoryScope(org_id=org_id, user_id=user_id)
    if not isinstance(thread_id, str) or not thread_id.strip():
        raise ValueError("Memory thread is required")
    claim = {"version": 1, "org_id": scope.org_id, "user_id": scope.user_id, "thread_id": thread_id}
    signature = hmac.new(_signing_key(), b"deerflow-memory-scope-v1\0" + _canonical(claim), hashlib.sha256).hexdigest()
    return {**claim, "signature": signature}


def memory_scope_from_context(context: Any) -> MemoryScope | None:
    """Resolve a signed runtime namespace, or return legacy development mode.

    Malformed or thread-mismatched assertions are denied even in development;
    they must never select the legacy namespace as a fallback.
    """
    if not isinstance(context, Mapping):
        context = {}
    claim = context.get(MEMORY_SCOPE_CONTEXT_KEY)
    if claim is None:
        if memory_isolation_required():
            raise PermissionError("Authenticated memory scope is required")
        return None
    if not isinstance(claim, Mapping):
        raise PermissionError("Invalid memory scope")
    expected_keys = {"version", "org_id", "user_id", "thread_id", "signature"}
    if set(claim) != expected_keys or type(claim.get("version")) is not int or claim["version"] != 1:
        raise PermissionError("Invalid memory scope")
    thread_id = context.get("thread_id")
    if not isinstance(thread_id, str) or not thread_id or claim.get("thread_id") != thread_id:
        raise PermissionError("Memory scope does not match the thread")
    signature = claim.get("signature")
    if not isinstance(signature, str) or len(signature) != 64 or any(character not in "0123456789abcdef" for character in signature):
        raise PermissionError("Invalid memory signature")
    unsigned = {key: value for key, value in claim.items() if key != "signature"}
    expected = hmac.new(_signing_key(), b"deerflow-memory-scope-v1\0" + _canonical(unsigned), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise PermissionError("Invalid memory signature")
    try:
        return MemoryScope(org_id=claim["org_id"], user_id=claim["user_id"])
    except (TypeError, ValueError) as exc:
        raise PermissionError("Invalid memory identity") from exc
