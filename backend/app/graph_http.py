"""Bound admission to the single-process LangGraph queue, preserving native runs/SSE.

The worker's --n-jobs-per-worker controls execution. This middleware limits the
total pending + running runs. It releases its creation lock at response headers,
after the native API has stored the run, never for the lifetime of a stream.
"""

import asyncio
import json
import logging
import os
import re

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)
_RUN_CREATE_PATH = re.compile(r"^(?:/threads/[^/]+)?/runs(?:/(?:stream|wait|batch))?/?$")


async def unfinished_run_count() -> int:
    # Use the installed runtime's queue, including runs that continue after a
    # browser disconnects. Counting HTTP connections would release those early.
    from langgraph_runtime.database import connect
    from langgraph_runtime.ops import Runs

    async with connect() as conn:
        stats = await Runs.stats(conn)
    return stats["n_pending"] + stats["n_running"]


class RunAdmissionMiddleware:
    def __init__(self, app: ASGIApp, max_inflight: int | None = None):
        self.app = app
        self.max_inflight = int(os.environ.get("DEER_FLOW_MAX_INFLIGHT_RUNS", "10")) if max_inflight is None else max_inflight
        if self.max_inflight < 1:
            raise ValueError("DEER_FLOW_MAX_INFLIGHT_RUNS must be a positive integer")
        self._creation_lock = asyncio.Lock()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] != "POST" or not _RUN_CREATE_PATH.fullmatch(scope["path"]):
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
            await self.app(scope, replay_body, forward)
        finally:
            if locked:
                self._creation_lock.release()


app = Starlette(middleware=[Middleware(RunAdmissionMiddleware)])
