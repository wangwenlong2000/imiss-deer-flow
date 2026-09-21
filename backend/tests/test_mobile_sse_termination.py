"""Mobile adapter SSE terminal-event regressions."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "backend"))

from app.gateway.routers import mobile  # noqa: E402


def test_clarification_stream_has_structured_terminal_done(monkeypatch) -> None:
    async def fake_producer(_request, queue) -> None:  # noqa: ANN001
        await queue.put({"kind": "start", "thread_id": "thread-clarification"})
        await queue.put({"kind": "chunk", "raw_event": {"kind": "clarification"}})
        await queue.put({"kind": "done"})

    monkeypatch.setattr(mobile, "_produce_langgraph_stream", fake_producer)
    monkeypatch.setattr(
        mobile,
        "_extract_clarification_from_event",
        lambda *args, **kwargs: {
            "type": "clarification",
            "content": "请选择处理方式",
            "conversation_id": "conv-1",
            "thread_id": "thread-clarification",
        },
    )

    async def collect() -> list[str]:
        request = mobile.DeerFlowMobileRunRequest(
            query="请处理这份数据",
            conversation_id="conv-1",
            stream=True,
        )
        return [item async for item in mobile._stream_deerflow_mobile_run(request)]

    events = asyncio.run(collect())
    assert any(item.startswith("event: clarification\n") for item in events)
    terminal = next(item for item in events if item.startswith("event: done\n"))
    assert '"status": "awaiting_clarification"' in terminal
    assert events[-1] == "data: [DONE]\n\n"
