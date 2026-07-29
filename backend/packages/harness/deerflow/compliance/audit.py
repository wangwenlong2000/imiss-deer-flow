"""Decision audit trail (JSONL).

Guide requirement 5: every determination must be able to answer "on the basis of
which rule / law / policy". That means persisting the hits, the matrix cells they
resolved to, and the clauses behind them — not just the final action.

One JSONL file per UTC day, append-only. Writes are best-effort: an audit failure
is logged but never propagates, because losing the log must not take a request
down. The failure itself is recorded in the decision diagnostics so the gap is
visible rather than silent.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from deerflow.compliance.types import ComplianceDecision, DetectionRequest

logger = logging.getLogger(__name__)

#: Evidence values longer than this are truncated in the audit record. The audit
#: log is a compliance artifact, not a second copy of the violating content.
MAX_EVIDENCE_CHARS = 2000


class Auditor:
    """Append compliance decisions to a daily JSONL file."""

    def __init__(self, *, path: str | Path, enabled: bool = True, retain_days: int = 90) -> None:
        self._dir = Path(path)
        self._enabled = enabled
        self._retain_days = max(int(retain_days), 0)
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _file_for(self, moment: datetime) -> Path:
        return self._dir / f"compliance-{moment.strftime('%Y%m%d')}.jsonl"

    def record(self, request: DetectionRequest, decision: ComplianceDecision) -> str | None:
        """Persist *decision*; return an audit reference, or ``None`` if disabled.

        The reference is stamped onto the decision so a user-facing warning can
        always be traced back to the exact record.
        """
        if not self._enabled:
            return None

        audit_ref = f"audit-{uuid.uuid4().hex[:16]}"
        now = datetime.now(UTC)
        record: dict[str, Any] = {
            "audit_ref": audit_ref,
            "timestamp": now.isoformat(),
            "request_id": request.request_id,
            "thread_id": request.thread_id,
            "gate": decision.gate,
            "scene_key": decision.scene_key,
            "scenes": list(decision.scenes),
            "actions": list(decision.actions),
            "per_violation_actions": {k: list(v) for k, v in decision.per_violation_actions.items()},
            "basis": list(decision.basis),
            "origin": dict(request.origin),
            "scene_resolution": dict(request.origin.get("scene_resolution") or {}),
            "request_context": dict(request.origin.get("compliance_request") or {}),
            "user": {"user_id": request.user.user_id, "roles": list(request.user.roles), "org_id": request.user.org_id} if request.user else None,
            "intent": {
                "intent": request.intent.intent,
                "intent_type": request.intent.intent_type,
                "requested_operation": request.intent.requested_operation,
                "risk_level": request.intent.risk_level,
                "is_high_risk": request.intent.high_risk,
                "confidence": request.intent.confidence,
                "reason_codes": list(request.intent.reason_codes),
                "reason": request.intent.reason,
                "source": request.intent.source,
            } if request.intent else None,
            "hits": [self._hit_record(hit) for hit in decision.hits],
            "diagnostics": decision.diagnostics.to_dict(),
        }

        try:
            self._append(record, now)
        except Exception as exc:
            # Never let auditing break a request — but never hide that it broke.
            logger.exception("compliance: failed to write audit record %s", audit_ref)
            decision.diagnostics.warn(f"audit write failed: {exc}")
            return audit_ref

        return audit_ref

    def _hit_record(self, hit: Any) -> dict[str, Any]:
        return {
            "detector_id": hit.detector_id,
            "violation_type": hit.violation_type,
            "confidence": round(float(hit.confidence), 6),
            "severity": hit.severity,
            "reason_code": hit.reason_code,
            "risk_locations": [
                {
                    "kind": loc.kind,
                    "locator": loc.locator,
                    "entity_type": loc.entity_type,
                    "text": _redacted_location_text(loc.text),
                }
                for loc in hit.risk_locations
            ],
            "basis": list(hit.basis),
            "evidence": _truncate_mapping(hit.evidence),
        }
    def _append(self, record: Mapping[str, Any], moment: datetime) -> None:
        target = self._file_for(moment)
        with self._lock:
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def purge_expired(self, *, now: datetime | None = None) -> int:
        """Delete audit files older than the retention window. Returns the count."""
        if not self._enabled or not self._retain_days or not self._dir.is_dir():
            return 0
        cutoff = (now or datetime.now(UTC)) - timedelta(days=self._retain_days)
        removed = 0
        for candidate in self._dir.glob("compliance-*.jsonl"):
            stamp = candidate.stem.removeprefix("compliance-")
            try:
                file_date = datetime.strptime(stamp, "%Y%m%d").replace(tzinfo=UTC)
            except ValueError:
                continue
            if file_date < cutoff:
                try:
                    os.remove(candidate)
                    removed += 1
                except OSError:
                    logger.warning("compliance: could not remove expired audit file %s", candidate)
        return removed

    def read_all(self) -> list[dict[str, Any]]:
        """Read every audit record (tests and reporting; not a hot path)."""
        records: list[dict[str, Any]] = []
        if not self._dir.is_dir():
            return records
        for candidate in sorted(self._dir.glob("compliance-*.jsonl")):
            for line in candidate.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    records.append(json.loads(line))
        return records


def _redacted_location_text(value: Any) -> str | None:
    """Keep correlation value without making audit a second sensitive store."""
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return f"[redacted sha256:{digest}]"


def _truncate(value: str | None, limit: int = MAX_EVIDENCE_CHARS) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if len(text) <= limit else text[:limit] + f"...<truncated {len(text) - limit} chars>"


def _truncate_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in dict(mapping).items():
        result[key] = _truncate(value) if isinstance(value, str) else value
    return result


class NullAuditor(Auditor):
    """Auditor that records nothing. Used when auditing is switched off."""

    def __init__(self) -> None:
        super().__init__(path=Path("."), enabled=False, retain_days=0)

    def record(self, request: DetectionRequest, decision: ComplianceDecision) -> str | None:
        return None


__all__ = ["Auditor", "NullAuditor"]
