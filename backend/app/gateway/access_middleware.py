"""Authenticate API access and protect shared platform administration."""

import re

from fastapi import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.gateway.mobile_iam import authenticate_mobile_request
from app.gateway.thread_access import require_thread_access
from app.security_audit import audit_access

_THREAD_PATH = re.compile(r"^/api/threads/([^/]+)(?:/|$)")
_ADMIN_ONLY = ("/api/mcp", "/api/channels", "/api/agents", "/api/skill-router")


class GatewayAccessMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not scope.get("path", "").startswith("/api/"):
            await self.app(scope, receive, send)
            return
        request = Request(scope, receive)
        principal = None
        try:
            _, principal = await authenticate_mobile_request(request)
            if principal is not None:
                path = request.url.path
                admin_path = any(path == prefix or path.startswith(prefix + "/") for prefix in _ADMIN_ONLY)
                admin_path |= path.startswith("/api/skills") and request.method not in {"GET", "HEAD", "OPTIONS"}
                if admin_path and "platform:admin" not in principal.permissions:
                    raise HTTPException(403, "Platform administration permission is required")
                match = _THREAD_PATH.match(path)
                if match:
                    operation = "upload" if "/uploads" in path and request.method in {"POST", "DELETE"} else "analysis"
                    await require_thread_access(request, match.group(1), operation_type=operation)
        except HTTPException as exc:
            audit_access(request, principal, exc.status_code, "authentication_or_authorization_failed")
            await JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers)(scope, receive, send)
            return

        async def audited_send(message: Message) -> None:
            if principal is not None and message["type"] == "http.response.start":
                audit_access(request, principal, message["status"], "gateway_operation")
            await send(message)

        await self.app(scope, receive, audited_send)
