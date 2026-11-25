"""User and conversation checks for thread scoped Gateway routes."""

from __future__ import annotations

import os
from uuid import uuid4

from fastapi import HTTPException, Request

from app.gateway.mobile_iam import (
    MobileAuthorizationInput,
    authenticate_mobile_request,
    authorize_mobile_request,
)
from app.internal_auth import internal_graph_headers


def thread_error_status(exc: Exception) -> int | None:
    """Read HTTP status without turning transport failures into missing data."""
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None) or getattr(exc, "status_code", None)


async def require_thread_access(
    request: Request | None,
    thread_id: str,
    *,
    operation_type: str,
) -> None:
    """Authorize a thread operation using persisted thread metadata.

    In development mode without an IAM provider this is a no-op. In an IAM
    deployment the thread must have been created by the authenticated mobile
    gateway and carry its server-written owner and conversation metadata.
    """
    if request is None:
        # Internal callers still pass through IAM when it is configured. The
        # ordinary development configuration keeps its no-provider behavior.
        request = Request({"type": "http", "headers": []})
    provider, principal = await authenticate_mobile_request(request)
    if provider is None or principal is None:
        return
    cache_key = (principal.org_id, principal.user_id, thread_id, operation_type)
    authorizations = request.scope.setdefault("_deerflow_thread_authorizations", set())
    if cache_key in authorizations:
        return

    from langgraph_sdk import get_client

    langgraph_url = os.getenv(
        "MOBILE_ADAPTER_LANGGRAPH_URL",
        os.getenv("LANGGRAPH_URL", "http://gaozhuohui-deer-flow-langgraph:3329"),
    )
    try:
        thread = await get_client(url=langgraph_url, headers=internal_graph_headers()).threads.get(thread_id)
    except Exception as exc:
        if thread_error_status(exc) == 404:
            raise HTTPException(status_code=404, detail="Thread not found") from exc
        raise HTTPException(status_code=503, detail="Thread service is unavailable") from exc

    if not isinstance(thread, dict):
        raise HTTPException(status_code=503, detail="Thread service returned invalid data")

    metadata = thread.get("metadata")
    if not isinstance(metadata, dict):
        raise HTTPException(status_code=404, detail="Thread not found")
    if metadata.get("user_id") != principal.user_id or metadata.get("org_id") != principal.org_id:
        raise HTTPException(status_code=404, detail="Thread not found")

    conversation_id = str(metadata.get("conversation_id") or "").strip()
    if not conversation_id:
        raise HTTPException(status_code=404, detail="Thread not found")

    await authorize_mobile_request(
        provider,
        principal,
        MobileAuthorizationInput(
            request_id=str(uuid4()),
            turn_id=str(uuid4()),
            conversation_id=conversation_id,
            operation_type=operation_type,
            target_audience="self",
            resource_id=conversation_id,
        ),
    )
    authorizations.add(cache_key)


__all__ = ["require_thread_access"]
