"""Mobile queue status must come from the exact admitted native run."""

import asyncio
import json
from types import SimpleNamespace

import langgraph_sdk
import pytest

from app.gateway.routers import mobile


@pytest.mark.parametrize("from_headers", [True, False])
def test_mobile_stream_reports_pending_then_running_for_exact_run(monkeypatch, from_headers):
    calls = []
    running_observed = None

    class Runs:
        async def stream(self, thread_id, assistant_id, **kwargs):
            assert kwargs["on_disconnect"] == "continue"
            if from_headers:
                kwargs["on_run_created"]({"run_id": "run-a"})
            else:
                yield SimpleNamespace(event="metadata", data={"run_id": "run-a"})
            await asyncio.wait_for(running_observed.wait(), timeout=2)
            await asyncio.sleep(0)
            yield SimpleNamespace(event="values", data={"messages": []})

        async def get(self, thread_id, run_id):
            calls.append((thread_id, run_id))
            if len(calls) > 1:
                running_observed.set()
            return {"status": "pending" if len(calls) == 1 else "running"}

    class Threads:
        async def get_state(self, thread_id):
            return {}

    monkeypatch.setattr(langgraph_sdk, "get_client", lambda **kwargs: SimpleNamespace(runs=Runs(), threads=Threads()))
    monkeypatch.setattr(mobile, "MOBILE_RUN_STATUS_POLL_SECONDS", 0.005)

    async def exercise():
        nonlocal running_observed
        running_observed = asyncio.Event()
        request = mobile.DeerFlowMobileRunRequest(query="synthetic", conversation_id="dialog-a", stream=True)
        prepared = mobile.MobileChatRequest(query=request.query, session_id=request.conversation_id, thread_id="thread-a")
        events = [event async for event in mobile._stream_deerflow_mobile_run(request, prepared)]
        statuses = [json.loads(event.split("data: ", 1)[1]) for event in events if event.startswith("event: status\n")]
        assert [item["status"] for item in statuses] == ["pending", "running"]
        assert all(item["run_id"] == "run-a" and item["thread_id"] == "thread-a" for item in statuses)
        accepted = next(event for event in events if event.startswith("event: accepted\n"))
        assert '"status": "submitted"' in accepted

    asyncio.run(exercise())
    assert calls and all(call == ("thread-a", "run-a") for call in calls)


@pytest.mark.parametrize("from_headers", [True, False])
def test_fast_completed_native_run_keeps_admitted_id_without_status_poll(monkeypatch, from_headers):
    """A completed SSE must still identify the exact native run to its caller."""
    class Runs:
        async def stream(self, thread_id, assistant_id, **kwargs):
            if from_headers:
                kwargs["on_run_created"]({"run_id": "run-fast"})
            else:
                yield SimpleNamespace(event="metadata", data={"run_id": "run-fast"})
            yield SimpleNamespace(event="values", data={"messages": [
                {"type": "human", "content": "synthetic"}, {"type": "ai", "content": "verified answer"},
            ]})

        async def get(self, thread_id, run_id):
            assert (thread_id, run_id) == ("thread-fast", "run-fast")
            return {"status": "success"}

    class Threads:
        async def get_state(self, thread_id):
            return {}

    monkeypatch.setattr(langgraph_sdk, "get_client", lambda **kwargs: SimpleNamespace(runs=Runs(), threads=Threads()))

    async def exercise():
        request = mobile.DeerFlowMobileRunRequest(query="synthetic", conversation_id="dialog-fast", stream=True)
        prepared = mobile.MobileChatRequest(query=request.query, session_id=request.conversation_id, thread_id="thread-fast")
        return [event async for event in mobile._stream_deerflow_mobile_run(request, prepared)]

    events = asyncio.run(exercise())
    assert not any(event.startswith("event: status\n") for event in events)
    done = json.loads(next(event for event in events if event.startswith("event: done\n")).split("data: ", 1)[1])
    assert done["status"] == "completed"
    assert done["thread_id"] == "thread-fast"
    assert done["run_id"] == "run-fast"
