"""Bound global and signed-user admission to the single-process LangGraph queue.

The worker's --n-jobs-per-worker controls execution. This middleware limits
global, authenticated-user, identity-class, and task-type pending + running
runs. It releases its creation lock at response headers, after the native API
has stored the run, never for the lifetime of a stream.
"""

import asyncio
import json
import logging
import os
import re
from collections import Counter
from collections.abc import Mapping

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.admission_claims import ADMISSION_METADATA_KEY, TASK_TYPES, verify_admission_metadata
from app.gateway.mobile_iam import mobile_iam_required
from app.graph_access import GraphAccessMiddleware
from deerflow.compliance.manual_context import MANUAL_COMPLIANCE_PROFILES

logger = logging.getLogger(__name__)
_RUN_CREATE_PATH = re.compile(r"^(?:/threads/([^/]+))?/runs(?:/(?:stream|wait|batch))?/?$")


async def unfinished_run_count() -> int:
    # Use the installed runtime's queue, including runs that continue after a
    # browser disconnects. Counting HTTP connections would release those early.
    from langgraph_runtime.database import connect
    from langgraph_runtime.ops import Runs

    async with connect() as conn:
        stats = await Runs.stats(conn)
    return stats["n_pending"] + stats["n_running"]


async def unfinished_run_records() -> list[dict[str, object]]:
    """Return native pending/running records for scoped admission checks.

    The current `langgraph dev` deployment uses the in-memory runtime. If the
    selected runtime cannot enumerate run metadata, authenticated scoped
    admission fails closed instead of silently dropping per-user limits.
    """
    from langgraph_runtime.database import connect

    async with connect() as conn:
        store = getattr(conn, "store", None)
        records = store.get("runs") if isinstance(store, Mapping) else None
        if not isinstance(records, list):
            raise RuntimeError("The configured LangGraph runtime cannot enumerate active runs")
        return [
            record
            for record in records
            if isinstance(record, dict) and record.get("status") in {"pending", "running"}
        ]


def _positive_limit(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        value = default if raw is None or not raw.strip() else int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _boolean_setting(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _limit_map(name: str, allowed_keys: set[str]) -> dict[str, int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a JSON object of positive integer limits") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{name} must be a JSON object of positive integer limits")
    limits: dict[str, int] = {}
    for key, value in payload.items():
        if key not in allowed_keys or isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} contains an unknown key or invalid limit")
        limits[key] = value
    return limits


class RunAdmissionMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        max_inflight: int | None = None,
        max_per_user: int | None = None,
        identity_class_limits: dict[str, int] | None = None,
        task_type_limits: dict[str, int] | None = None,
        require_signed_admission: bool | None = None,
    ):
        self.app = app
        self.max_inflight = int(os.environ.get("DEER_FLOW_MAX_INFLIGHT_RUNS", "10")) if max_inflight is None else max_inflight
        if self.max_inflight < 1:
            raise ValueError("DEER_FLOW_MAX_INFLIGHT_RUNS must be a positive integer")
        self.max_per_user = _positive_limit("DEER_FLOW_MAX_INFLIGHT_PER_USER", 2) if max_per_user is None else max_per_user
        if self.max_per_user < 1:
            raise ValueError("DEER_FLOW_MAX_INFLIGHT_PER_USER must be a positive integer")
        self.identity_class_limits = (
            _limit_map("DEER_FLOW_IDENTITY_CLASS_LIMITS", set(MANUAL_COMPLIANCE_PROFILES))
            if identity_class_limits is None
            else identity_class_limits
        )
        self.task_type_limits = (
            _limit_map("DEER_FLOW_TASK_TYPE_LIMITS", set(TASK_TYPES))
            if task_type_limits is None
            else task_type_limits
        )
        self.require_signed_admission = (
            _boolean_setting("DEER_FLOW_REQUIRE_SIGNED_ADMISSION", False)
            if require_signed_admission is None
            else require_signed_admission
        )
        # An IAM deployment must never admit an internal task without a verified
        # actor, even if the legacy browser compatibility setting is false.
        self.require_signed_admission |= bool(os.getenv("MOBILE_IAM_PROVIDER", "").strip()) or mobile_iam_required()
        self._creation_lock = asyncio.Lock()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path_match = _RUN_CREATE_PATH.fullmatch(scope.get("path", ""))
        if scope["type"] != "http" or scope["method"] != "POST" or path_match is None:
            await self.app(scope, receive, send)
            return

        # Read before taking the lock so a slow client cannot block other runs.
        # Replay the original bytes; native validation and request semantics stay
        # unchanged. Batch creation must reserve its full size, not one slot.
        body = await Request(scope, receive).body()
        count = 1
        if scope["path"].rstrip("/").endswith("/batch"):
            try:
                payload = json.loads(body)
                if isinstance(payload, list):
                    count = len(payload)
            except (ValueError, UnicodeDecodeError):
                pass  # Let the native API report malformed JSON.

        body_sent = False
        path_thread_id = path_match.group(1)
        invalid_admission_claim = False

        def verified_claim(metadata: object, thread_id: str | None) -> dict[str, str] | None:
            nonlocal invalid_admission_claim
            claims_present = isinstance(metadata, Mapping) and ADMISSION_METADATA_KEY in metadata
            claims = verify_admission_metadata(metadata, expected_thread_id=thread_id)
            if claims_present and claims is None:
                invalid_admission_claim = True
            return claims

        def request_claims() -> list[dict[str, str] | None]:
            try:
                payload = json.loads(body)
            except (ValueError, UnicodeDecodeError):
                return [None] * count

            if isinstance(payload, list):
                claims: list[dict[str, str] | None] = []
                for item in payload:
                    if not isinstance(item, Mapping):
                        claims.append(None)
                        continue
                    item_thread = item.get("thread_id")
                    metadata = item.get("metadata")
                    claims.append(verified_claim(metadata, str(item_thread) if item_thread else None))
                return claims

            if not isinstance(payload, Mapping):
                return [None] * count
            request_thread_id = path_thread_id or (
                str(payload.get("thread_id")) if payload.get("thread_id") else None
            )
            return [verified_claim(payload.get("metadata"), request_thread_id)] * count

        requested_claims = request_claims()
        requested_users = Counter(claim["user_key"] for claim in requested_claims if claim)
        requested_classes = Counter(claim["identity_class"] for claim in requested_claims if claim)
        requested_task_types = Counter(claim["task_type"] for claim in requested_claims if claim)

        if invalid_admission_claim:
            await JSONResponse(
                {"detail": "网关身份声明无效，请重新提交任务。"},
                status_code=403,
            )(scope, receive, send)
            return
        if self.require_signed_admission and any(claim is None for claim in requested_claims):
            await JSONResponse(
                {"detail": "该服务要求通过 IAM 验证的任务身份。"},
                status_code=403,
            )(scope, receive, send)
            return

        async def replay_body() -> Message:
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self._creation_lock.acquire()
        locked = True

        async def forward(message: Message) -> None:
            nonlocal locked
            if message["type"] == "http.response.start" and locked:
                locked = False
                self._creation_lock.release()
            await send(message)

        try:
            try:
                unfinished = await unfinished_run_count()
            except Exception:
                logger.exception("Unable to read LangGraph queue capacity")
                await JSONResponse(
                    {"detail": "任务队列暂时不可用，请稍后重试。"},
                    status_code=503,
                    headers={"Retry-After": "3"},
                )(scope, replay_body, forward)
                return
            if unfinished + count > self.max_inflight:
                await JSONResponse(
                    {"detail": "当前任务较多，等待队列已满，请稍后重试。"},
                    status_code=429,
                    headers={"Retry-After": "3"},
                )(scope, replay_body, forward)
                return
            if requested_users:
                try:
                    active_records = await unfinished_run_records()
                except Exception:
                    logger.exception("Unable to read scoped LangGraph queue capacity")
                    await JSONResponse(
                        {"detail": "用户任务队列暂时不可用，请稍后重试。"},
                        status_code=503,
                        headers={"Retry-After": "3"},
                    )(scope, replay_body, forward)
                    return

                active_users: Counter[str] = Counter()
                active_classes: Counter[str] = Counter()
                active_task_types: Counter[str] = Counter()
                for record in active_records:
                    claims = verify_admission_metadata(
                        record.get("metadata"),
                        expected_thread_id=str(record.get("thread_id") or "") or None,
                        allow_expired=True,
                    )
                    if claims is None:
                        continue
                    active_users[claims["user_key"]] += 1
                    active_classes[claims["identity_class"]] += 1
                    active_task_types[claims["task_type"]] += 1

                user_limit_hit = any(
                    active_users[user_key] + requested > self.max_per_user
                    for user_key, requested in requested_users.items()
                )
                identity_limit_hit = any(
                    active_classes[identity_class] + requested > self.identity_class_limits[identity_class]
                    for identity_class, requested in requested_classes.items()
                    if identity_class in self.identity_class_limits
                )
                task_limit_hit = any(
                    active_task_types[task_type] + requested > self.task_type_limits[task_type]
                    for task_type, requested in requested_task_types.items()
                    if task_type in self.task_type_limits
                )
                if user_limit_hit or identity_limit_hit or task_limit_hit:
                    await JSONResponse(
                        {"detail": "该用户或任务类别的并发名额已满，请稍后重试。"},
                        status_code=429,
                        headers={"Retry-After": "3"},
                    )(scope, replay_body, forward)
                    return
            await self.app(scope, replay_body, forward)
        finally:
            if locked:
                self._creation_lock.release()


app = Starlette(middleware=[Middleware(GraphAccessMiddleware), Middleware(RunAdmissionMiddleware)])
