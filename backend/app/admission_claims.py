"""Signed scheduling claims passed from the authenticated mobile gateway.

Caller supplied user IDs and compliance context are not admission identities.
The gateway signs only claims built from its verified IAM principal and
authorization decision; the graph service validates the signature before using
the user, identity class, or task type for concurrency limits.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from deerflow.compliance.manual_context import MANUAL_COMPLIANCE_PROFILES

ADMISSION_METADATA_KEY = "deerflow_admission"
TASK_TYPES = frozenset({"analysis", "upload", "export", "share", "public_release", "research"})
CLAIM_MAX_AGE_SECONDS = 300
_USER_KEY_RE = re.compile(r"^[0-9a-f]{64}$")


class AdmissionSigningConfigurationError(RuntimeError):
    """Raised when authenticated admission cannot be signed securely."""


def _signing_key() -> bytes:
    key = os.getenv("DEER_FLOW_ADMISSION_SIGNING_KEY", "").encode("utf-8")
    if len(key) < 32:
        raise AdmissionSigningConfigurationError(
            "DEER_FLOW_ADMISSION_SIGNING_KEY must contain at least 32 bytes"
        )
    return key


def ensure_admission_signing_available() -> None:
    """Fail early when authenticated requests cannot be signed."""
    _signing_key()


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def build_admission_metadata(
    *,
    user_id: str,
    org_id: str,
    identity_class: str,
    task_type: str,
    thread_id: str,
    now: int | None = None,
) -> dict[str, Any]:
    """Create signed, privacy-preserving run metadata from a verified IAM result."""
    if not user_id.strip() or not org_id.strip():
        raise ValueError("Verified IAM user and organization IDs are required for admission metadata")
    if identity_class not in MANUAL_COMPLIANCE_PROFILES:
        raise ValueError("identity_class must use a compliance-defined identity profile")
    if task_type not in TASK_TYPES:
        raise ValueError("task_type must use a compliance-authorized operation type")
    if not thread_id.strip():
        raise ValueError("thread_id is required for admission metadata")

    key = _signing_key()
    claims: dict[str, Any] = {
        "version": 1,
        "thread_id": thread_id,
        "user_key": hmac.new(
            key,
            b"deerflow-user-v1\0" + org_id.encode("utf-8") + b"\0" + user_id.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest(),
        "identity_class": identity_class,
        "task_type": task_type,
        "issued_at": int(time.time() if now is None else now),
        "nonce": uuid4().hex,
    }
    claims["signature"] = hmac.new(key, _canonical_json(claims), hashlib.sha256).hexdigest()
    return {ADMISSION_METADATA_KEY: claims}


def verify_admission_metadata(
    metadata: Any,
    *,
    expected_thread_id: str | None,
    now: int | None = None,
    allow_expired: bool = False,
) -> dict[str, str] | None:
    """Validate a signed claim and return its bounded scheduling dimensions."""
    if not isinstance(metadata, Mapping):
        return None
    claims = metadata.get(ADMISSION_METADATA_KEY)
    if not isinstance(claims, Mapping):
        return None

    key_value = os.getenv("DEER_FLOW_ADMISSION_SIGNING_KEY", "").encode("utf-8")
    if len(key_value) < 32:
        return None

    try:
        version = claims.get("version")
        thread_id = claims.get("thread_id")
        user_key = claims.get("user_key")
        identity_class = claims.get("identity_class")
        task_type = claims.get("task_type")
        issued_at = claims.get("issued_at")
        nonce = claims.get("nonce")
        signature = claims.get("signature")
        if (
            version != 1
            or not isinstance(thread_id, str)
            or not thread_id
            or not isinstance(expected_thread_id, str)
            or thread_id != expected_thread_id
            or not isinstance(user_key, str)
            or not _USER_KEY_RE.fullmatch(user_key)
            or identity_class not in MANUAL_COMPLIANCE_PROFILES
            or task_type not in TASK_TYPES
            or not isinstance(issued_at, int)
            or isinstance(issued_at, bool)
            or not isinstance(nonce, str)
            or not nonce
            or not isinstance(signature, str)
        ):
            return None

        signed_payload = {str(key): value for key, value in claims.items() if key != "signature"}
        expected_signature = hmac.new(key_value, _canonical_json(signed_payload), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            return None

        current_time = int(time.time() if now is None else now)
        if not allow_expired and (issued_at > current_time + 30 or current_time - issued_at > CLAIM_MAX_AGE_SECONDS):
            return None
    except (TypeError, ValueError):
        return None

    return {
        "user_key": user_key,
        "identity_class": str(identity_class),
        "task_type": str(task_type),
    }


__all__ = [
    "ADMISSION_METADATA_KEY",
    "AdmissionSigningConfigurationError",
    "TASK_TYPES",
    "build_admission_metadata",
    "ensure_admission_signing_available",
    "verify_admission_metadata",
]
