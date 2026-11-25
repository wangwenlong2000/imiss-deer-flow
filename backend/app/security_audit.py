"""Request access auditing without credentials, query strings, or payloads."""

import json
import logging
from uuid import uuid4

from starlette.requests import Request

logger = logging.getLogger("deerflow.security.access")


def audit_access(request: Request, principal: object, status: int, reason: str) -> None:
    logger.info(
        json.dumps(
            {
                "event": "access_decision",
                "request_id": request.scope.setdefault("deerflow.audit_id", str(uuid4())),
                "method": request.method,
                "path": request.url.path,
                "org_id": getattr(principal, "org_id", None),
                "user_id": getattr(principal, "user_id", None),
                "status": status,
                "decision": "deny" if status >= 400 else "allow",
                "reason": reason,
            },
            ensure_ascii=False,
        )
    )
