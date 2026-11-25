"""Protect native graph APIs when the deployment uses IAM.

Public callers may search/read their own threads and cancel/delete their own
runs. New threads/runs and state writes go through the authenticated Gateway,
which builds authoritative compliance and memory context. Internal SDK calls
use a purpose-separated server credential; signed run admission still applies.
"""

import json
import os
import re
from uuid import uuid4

import httpx
from fastapi import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.gateway.mobile_iam import (
    MobileAuthorizationInput,
    authenticate_mobile_request,
    authorize_mobile_request,
)
from app.internal_auth import INTERNAL_GRAPH_HEADER, internal_graph_headers, verify_internal_graph_header
from app.security_audit import audit_access

_THREAD_PATH = re.compile(r"^/threads/([^/]+)(/.*)?$")
_READ_POST = re.compile(r"^/(?:history|state/checkpoint|runs/[^/]+/join)/?$")
_CANCEL_POST = re.compile(r"^/runs/[^/]+/cancel/?$")
_RUN_DELETE = re.compile(r"^/runs/[^/]+/?$")


async def authorize_graph_metadata(provider, principal, metadata: dict) -> None:
    if metadata.get("org_id") != principal.org_id or metadata.get("user_id") != principal.user_id:
        raise HTTPException(404, "Thread not found")
    conversation_id = metadata.get("conversation_id")
    if not isinstance(conversation_id, str) or not conversation_id.strip():
        raise HTTPException(404, "Thread not found")
    await authorize_mobile_request(
        provider,
        principal,
        MobileAuthorizationInput(
            request_id=str(uuid4()),
            turn_id=str(uuid4()),
            conversation_id=conversation_id,
            operation_type="analysis",
            target_audience="self",
            resource_id=conversation_id,
        ),
    )


async def graph_thread_metadata(thread_id: str) -> dict:
    """Fetch metadata through the native API with an internal credential."""
    from langgraph_sdk import get_client

    url = os.getenv("MOBILE_ADAPTER_LANGGRAPH_URL", os.getenv("LANGGRAPH_URL", "http://gaozhuohui-deer-flow-langgraph:3329"))
    try:
        thread = await get_client(url=url, headers=internal_graph_headers()).threads.get(thread_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(404, "Thread not found") from exc
        raise HTTPException(503, "Thread service is unavailable") from exc
    except Exception as exc:
        raise HTTPException(503, "Thread service is unavailable") from exc
    metadata = thread.get("metadata") if isinstance(thread, dict) else None
    if not isinstance(metadata, dict):
        raise HTTPException(404, "Thread not found")
    return metadata


class GraphAccessMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") in {"/ok", "/health"}:
            await self.app(scope, receive, send)
            return
        request = Request(scope, receive)
        principal = None
        replay = receive
        search = False
        search_select = None
        try:
            internal = request.headers.get(INTERNAL_GRAPH_HEADER)
            if internal is not None:
                if not verify_internal_graph_header(internal):
                    raise HTTPException(403, "Invalid internal graph credential")
                # LangGraph can persist configurable x-* headers in run config.
                # Consume the credential here so it never reaches that layer.
                scope["headers"] = [(k, v) for k, v in scope["headers"] if k.lower() != INTERNAL_GRAPH_HEADER.encode()]
                await self.app(scope, receive, send)
                return
            provider, principal = await authenticate_mobile_request(request)
            if principal is None:
                await self.app(scope, receive, send)
                return

            path = request.url.path.rstrip("/")
            if request.method == "POST" and path == "/threads/search":
                search = True
                # Native filtering takes place before pagination/counting. Users
                # cannot clear or replace server ownership filters.
                try:
                    payload = json.loads(await request.body())
                except (ValueError, UnicodeDecodeError) as exc:
                    raise HTTPException(422, "A JSON object is required") from exc
                if not isinstance(payload, dict) or not isinstance(payload.get("metadata", {}), dict):
                    raise HTTPException(422, "A JSON object with metadata filters is required")
                payload["metadata"] = {**payload.get("metadata", {}), "org_id": principal.org_id, "user_id": principal.user_id}
                if "select" in payload:
                    if not isinstance(payload["select"], list) or any(not isinstance(field, str) for field in payload["select"]):
                        raise HTTPException(422, "Search select must be a list of fields")
                    search_select = payload["select"] or None
                    if search_select is not None:
                        payload["select"] = list(dict.fromkeys([*search_select, "metadata"]))
                body = json.dumps(payload).encode()
                scope["headers"] = [(k, v) for k, v in scope["headers"] if k.lower() != b"content-length"]
                delivered = False

                async def replay_body() -> Message:
                    nonlocal delivered
                    if not delivered:
                        delivered = True
                        return {"type": "http.request", "body": body, "more_body": False}
                    return await receive()

                replay = replay_body
            else:
                match = _THREAD_PATH.fullmatch(path)
                if match is None or match.group(1) in {"search", "count"}:
                    raise HTTPException(403, "Use the authenticated Gateway for this operation")
                suffix = match.group(2) or ""
                allowed = (
                    request.method in {"GET", "HEAD"}
                    or (request.method == "POST" and (_READ_POST.fullmatch(suffix) or _CANCEL_POST.fullmatch(suffix)))
                    or (request.method == "DELETE" and (not suffix or _RUN_DELETE.fullmatch(suffix)))
                )
                if not allowed:
                    raise HTTPException(403, "Use the authenticated Gateway for thread creation or state changes")
                metadata = await graph_thread_metadata(match.group(1))
                await authorize_graph_metadata(provider, principal, metadata)
        except HTTPException as exc:
            audit_access(request, principal, exc.status_code, "graph_access_denied")
            await JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers)(scope, receive, send)
            return

        async def audited_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                audit_access(request, principal, message["status"], "graph_operation")
            await send(message)

        if not search:
            await self.app(scope, replay, audited_send)
            return

        # Search records can contain messages/state. Recheck each current IAM
        # decision before returning it, including dialogs whose access was
        # revoked after creation. Never expose a raw count of inaccessible data.
        response_start = None
        response_body = bytearray()

        async def search_send(message: Message) -> None:
            nonlocal response_start
            if message["type"] == "http.response.start":
                response_start = message
                return
            if message["type"] != "http.response.body":
                return
            response_body.extend(message.get("body", b""))
            if message.get("more_body", False):
                return
            if response_start is None:
                raise RuntimeError("Graph search did not start a response")
            if response_start["status"] >= 400:
                await audited_send(response_start)
                await audited_send({"type": "http.response.body", "body": bytes(response_body)})
                return
            try:
                records = json.loads(response_body)
                if not isinstance(records, list):
                    raise ValueError("Invalid search response")
                visible = []
                for record in records:
                    if not isinstance(record, dict) or not isinstance(record.get("metadata"), dict):
                        continue
                    try:
                        await authorize_graph_metadata(provider, principal, record["metadata"])
                    except HTTPException as exc:
                        if exc.status_code in {403, 404}:
                            continue
                        raise
                    visible.append(record if search_select is None else {
                        key: value for key, value in record.items() if key in search_select or key == "extracted"
                    })
            except HTTPException as exc:
                await JSONResponse({"detail": exc.detail}, status_code=exc.status_code)(scope, replay, audited_send)
                return
            except (ValueError, UnicodeDecodeError):
                await JSONResponse({"detail": "Thread service returned invalid data"}, status_code=503)(scope, replay, audited_send)
                return
            await JSONResponse(visible, status_code=response_start["status"])(scope, replay, audited_send)

        await self.app(scope, replay, search_send)
