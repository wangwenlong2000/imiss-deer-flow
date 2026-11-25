from __future__ import annotations

import asyncio
from types import SimpleNamespace

import langgraph_sdk

from app.gateway.routers import mobile


class _QueueFullError(Exception):
    status_code = 429
    response = SimpleNamespace(status_code=429, headers={"retry-after": "3"})


def test_mobile_json_error_keeps_queue_full_status_and_retry_after():
    result = mobile._mobile_run_error_response(
        conversation_id="conversation-1",
        thread_id="thread-1",
        exc=_QueueFullError("request rejected"),
    )

    assert result["code"] == 429
    assert result["retry_after"] == "3"
    assert "并发名额已满" in result["message"]


def test_mobile_sse_error_keeps_queue_full_status_and_retry_after(monkeypatch):
    class _Runs:
        async def stream(self, *_args, **_kwargs):
            if False:
                yield None
            raise _QueueFullError("request rejected")

    class _Threads:
        async def get_state(self, _thread_id):
            return {}

    client = SimpleNamespace(runs=_Runs(), threads=_Threads())
    monkeypatch.setattr(langgraph_sdk, "get_client", lambda **_kwargs: client)

    async def collect():
        request = mobile.DeerFlowMobileRunRequest(
            query="请分析数据",
            conversation_id="conversation-1",
            stream=True,
            thread_id="thread-1",
        )
        prepared = mobile.MobileChatRequest(
            query=request.query,
            session_id=request.conversation_id,
            thread_id=request.thread_id,
        )
        return [event async for event in mobile._stream_deerflow_mobile_run(request, prepared)]

    events = asyncio.run(collect())
    error = next(event for event in events if event.startswith("event: error\n"))

    assert '"code": 429' in error
    assert '"retry_after": "3"' in error
    assert "并发名额已满" in error
