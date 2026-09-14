"""Regression tests for forwarding compliance refusals to the mobile API."""

from __future__ import annotations

import asyncio
import json

import pytest

from app.gateway.routers import mobile

REFUSAL = "该回答包含受限信息，已根据合规策略拒绝提供。"


def _retract_event() -> dict:
    return {
        "event": "custom",
        "data": {
            "type": "compliance_retract",
            "message_id": "message-1",
            "scene": "public_release",
            "streaming_mode": "compatible_retract",
            "violation_types": ["video_meta_leak"],
            "action": ["refuse"],
            "replacement": REFUSAL,
            "notice": "内容未通过合规检测。",
            "audit_ref": "audit-1",
            "basis": ["test-policy"],
        },
    }


def test_compliance_retract_becomes_feedback_and_replaces_final_answer() -> None:
    raw_events = [
        _retract_event(),
        {
            "event": "values",
            "data": {
                "messages": [
                    {"type": "human", "content": "查询敏感信息"},
                    {"type": "ai", "content": REFUSAL},
                ]
            },
        },
    ]

    result = mobile.adapt_events_to_mobile(
        session_id="conversation-1",
        thread_id="thread-1",
        raw_events=raw_events,
        current_query="查询敏感信息",
    )

    feedback = [segment for segment in result["segments"] if segment["metadata"]["source_event"] == "compliance_retract"]
    assert len(feedback) == 1
    assert feedback[0]["agent_id"] == "feedback"
    assert feedback[0]["agent_name"] == "反馈智能体"
    assert feedback[0]["content"] == REFUSAL
    assert feedback[0]["metadata"]["violation_types"] == ["video_meta_leak"]
    assert feedback[0]["metadata"]["action"] == ["refuse"]
    assert feedback[0]["metadata"]["audit_ref"] == "audit-1"
    assert not any(segment["metadata"]["source_event"] == "final_answer" for segment in result["segments"])


def test_stream_forwards_compliance_refusal_once_as_feedback(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_producer(_chat_req, queue) -> None:
        await queue.put({"kind": "start", "thread_id": "thread-1"})
        await queue.put({"kind": "chunk", "raw_event": _retract_event()})
        await queue.put(
            {
                "kind": "chunk",
                "raw_event": {
                    "event": "values",
                    "data": {
                        "messages": [
                            {"type": "human", "content": "查询敏感信息"},
                            {"type": "ai", "content": REFUSAL},
                        ]
                    },
                },
            }
        )
        await queue.put({"kind": "done"})

    monkeypatch.setattr(mobile, "_produce_langgraph_stream", fake_producer)
    request = mobile.DeerFlowMobileRunRequest(query="查询敏感信息", conversation_id="conversation-1", stream=True)

    async def collect_frames() -> list[str]:
        return [frame async for frame in mobile._stream_deerflow_mobile_run(request)]

    frames = asyncio.run(collect_frames())
    segment_payloads = [json.loads(frame.split("data: ", 1)[1]) for frame in frames if frame.startswith("event: segment\n")]

    assert len(segment_payloads) == 1
    assert segment_payloads[0]["role"] == "反馈智能体"
    assert segment_payloads[0]["content"] == REFUSAL
    assert segment_payloads[0]["metadata"]["source_event"] == "compliance_retract"
    assert sum(REFUSAL in frame for frame in frames) == 2  # segment plus final done summary
