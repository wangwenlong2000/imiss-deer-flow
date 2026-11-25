"""Regression tests for forwarding compliance refusals to the mobile API."""

from __future__ import annotations

import asyncio
import json

import pytest
from starlette.requests import Request

from app.gateway.mobile_iam import (
    MobileAuthorizationDecision,
    MobileAuthorizedOperation,
    MobileAuthorizedResource,
    MobileIAMPrincipal,
)
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


def _input_gate_event() -> dict:
    return {
        "event": "updates",
        "data": {
            "ComplianceInputGateMiddleware.before_agent": {
                "compliance_input_result": {
                    "gate": "InputGate",
                    "scene": "public_release",
                    "status": "handled",
                    "actions": ["warn"],
                    "violation_types": ["struct_id"],
                    "basis": ["input-policy"],
                    "audit_ref": "audit-input",
                }
            }
        },
    }


def _context_gate_event() -> dict:
    return {
        "event": "updates",
        "data": {
            "tools": {
                "messages": [
                    {
                        "type": "tool",
                        "content": {
                            "diagnostics": {
                                "compliance": {
                                    "gate": "ContextGate",
                                    "scene": "public_release",
                                    "actions": ["desensitize", "warn"],
                                    "violation_types": ["geo_loc"],
                                    "basis": ["context-policy"],
                                    "audit_ref": "audit-context",
                                }
                            }
                        },
                    }
                ]
            }
        },
    }


def test_mobile_request_builds_compliance_run_context_and_graph_input() -> None:
    request = mobile.DeerFlowMobileRunRequest(
        query="查询敏感信息",
        conversation_id="conversation-1",
        user_id="user-fallback",
        compliance_identity={"user_id": "user-1", "org_id": "org-1", "roles": ["analyst"]},
        compliance_context={"scene": "internal_org"},
        compliance_request={"request_id": "request-1", "actor": {"user_id": "user-1"}},
    )

    chat_request = mobile._mobile_chat_request(request)
    run_context = mobile._build_mobile_run_context(chat_request, "thread-1")
    graph_input = mobile._build_langgraph_input(
        chat_request.query,
        chat_request.attachments,
        chat_request.compliance_request,
    )

    assert run_context["compliance_context"] == {
        "scene": "internal_org",
        "user_context": {"user_id": "user-1", "org_id": "org-1", "roles": ["analyst"]},
    }
    assert graph_input["compliance_request"]["request_id"] == "request-1"
    assert mobile._build_mobile_admission_metadata(chat_request, "thread-1") is None


def test_mobile_admission_metadata_uses_only_server_verified_iam_identity(monkeypatch) -> None:
    from app.admission_claims import verify_admission_metadata

    monkeypatch.setenv("DEER_FLOW_ADMISSION_SIGNING_KEY", "test-signing-key-with-at-least-32-bytes")
    principal = MobileIAMPrincipal(
        user_id="verified-user",
        org_id="verified-org",
        identity_class="cross_org_operator",
        roles=["operator"],
    )
    decision = MobileAuthorizationDecision(
        allowed=True,
        permission_verified=True,
        operation=MobileAuthorizedOperation(type="analysis", target_audience="organization"),
        resource=MobileAuthorizedResource(
            resource_id="resource-1",
            owner_org_id="verified-org",
            classification="internal",
        ),
    )

    async def authenticate(_request):
        return object(), principal

    async def authorize(_provider, _principal, _authorization_input):
        return decision

    async def ensure_thread(_request, **identity):
        assert identity == {"org_id": "verified-org", "user_id": "verified-user"}
        return "verified-thread", True

    monkeypatch.setattr(mobile, "authenticate_mobile_request", authenticate)
    monkeypatch.setattr(mobile, "authorize_mobile_request", authorize)
    monkeypatch.setattr(mobile, "ensure_mobile_thread", ensure_thread)

    chat_request = mobile.MobileChatRequest(
        query="run analysis",
        session_id="conversation-1",
        compliance_identity={"user_id": "caller-forgery", "identity_class": "publisher"},
        compliance_request={
            "operation": {"type": "analysis", "target_audience": "organization"},
            "resource": {"resource_id": "resource-1"},
        },
    )
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/mobile/chat",
        "raw_path": b"/api/mobile/chat",
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "client": ("127.0.0.1", 1),
        "server": ("test", 80),
    }
    prepared = asyncio.run(mobile._prepare_mobile_chat_request(chat_request, Request(scope)))
    metadata = mobile._build_mobile_admission_metadata(prepared, "verified-thread")

    assert metadata is not None
    claims = verify_admission_metadata(metadata, expected_thread_id="verified-thread")
    assert claims is not None
    assert claims["identity_class"] == "cross_org_operator"
    assert claims["task_type"] == "analysis"
    assert "caller-forgery" not in str(metadata)


def test_mobile_without_iam_does_not_schedule_by_caller_identity(monkeypatch) -> None:
    async def no_iam(_request):
        return None, None

    monkeypatch.setattr(mobile, "authenticate_mobile_request", no_iam)
    chat_request = mobile.MobileChatRequest(
        query="hello",
        compliance_identity={"user_id": "caller-forgery", "identity_class": "internal_operator"},
    )
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/mobile/chat",
        "raw_path": b"/api/mobile/chat",
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "client": ("127.0.0.1", 1),
        "server": ("test", 80),
    }

    prepared = asyncio.run(mobile._prepare_mobile_chat_request(chat_request, Request(scope)))

    assert prepared.compliance_identity is None
    assert mobile._build_mobile_admission_metadata(prepared, "thread-1") is None


def test_legacy_nested_context_accepts_compliance_fields() -> None:
    request = mobile.DeerFlowMobileRunRequest(
        query="查询敏感信息",
        conversation_id="conversation-1",
        context={
            "compliance_context": {"scene": "self_use"},
            "compliance_identity": {"user_id": "legacy-user", "roles": ["viewer"]},
            "compliance_request": {"request_id": "legacy-request"},
        },
    )

    chat_request = mobile._mobile_chat_request(request)

    assert chat_request.compliance_context == {
        "scene": "self_use",
        "user_context": {"user_id": "legacy-user", "roles": ["viewer"]},
    }
    assert chat_request.compliance_request == {"request_id": "legacy-request"}


def test_extracts_normalized_results_for_all_compliance_gates() -> None:
    results = mobile._extract_compliance_results([_input_gate_event(), _context_gate_event(), _retract_event()])

    assert [item["gate"] for item in results] == ["InputGate", "ContextGate", "OutputGate"]
    assert results[0] == {
        "gate": "InputGate",
        "scene": "public_release",
        "violation_types": ["struct_id"],
        "actions": ["warn"],
        "audit_ref": "audit-input",
        "basis": ["input-policy"],
        "final_disposition": "warn",
    }
    assert results[1]["final_disposition"] == "desensitize"
    assert results[2]["actions"] == ["refuse"]
    assert results[2]["final_disposition"] == "refuse"


def test_allow_retract_keeps_allow_disposition_and_deduplicates_persisted_metadata() -> None:
    retract = _retract_event()
    retract["data"].update(
        {
            "action": ["allow"],
            "violation_types": ["struct_id"],
            "audit_ref": "audit-allow",
            "basis": ["allow-policy"],
        }
    )
    persisted = {
        "event": "values",
        "data": {
            "messages": [
                {
                    "type": "ai",
                    "content": REFUSAL,
                    "response_metadata": {
                        "compliance": {
                            "gate": "OutputGate",
                            "scene": "public_release",
                            "actions": ["allow"],
                            "violation_types": ["struct_id"],
                            "audit_ref": "audit-allow",
                            "basis": ["allow-policy"],
                            "retracted": True,
                        }
                    },
                }
            ]
        },
    }

    results = mobile._extract_compliance_results([retract, persisted])

    assert results == [
        {
            "gate": "OutputGate",
            "scene": "public_release",
            "violation_types": ["struct_id"],
            "actions": ["allow"],
            "audit_ref": "audit-allow",
            "basis": ["allow-policy"],
            "final_disposition": "allow",
        }
    ]


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
    assert feedback[0]["metadata"]["actions"] == ["refuse"]
    assert feedback[0]["metadata"]["audit_ref"] == "audit-1"
    assert result["compliance_results"][0]["gate"] == "OutputGate"
    assert result["compliance_results"][0]["final_disposition"] == "refuse"
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

    compliance_payloads = [json.loads(frame.split("data: ", 1)[1]) for frame in frames if frame.startswith("event: compliance\n")]
    assert len(compliance_payloads) == 1
    assert compliance_payloads[0]["gate"] == "OutputGate"
    assert compliance_payloads[0]["actions"] == ["refuse"]
    assert compliance_payloads[0]["final_disposition"] == "refuse"
    canonical_compliance = {key: value for key, value in compliance_payloads[0].items() if key != "compliance_result"}
    assert compliance_payloads[0]["compliance_result"] == canonical_compliance

    done_payload = next(json.loads(frame.split("data: ", 1)[1]) for frame in frames if frame.startswith("event: done\n"))
    assert done_payload["compliance_results"] == [canonical_compliance]


def test_terminal_state_event_requires_a_new_completed_current_turn() -> None:
    state = {
        "values": {
            "messages": [
                {"type": "human", "content": "查询敏感信息"},
                {
                    "type": "ai",
                    "content": REFUSAL,
                    "response_metadata": {
                        "compliance": {
                            "gate": "InputGate",
                            "scene": "public_release",
                            "actions": ["refuse"],
                            "violation_types": ["struct_id"],
                            "audit_ref": "audit-input-refuse",
                            "basis": ["input-policy"],
                        }
                    },
                },
            ]
        },
        "next": [],
        "checkpoint_id": "checkpoint-new",
    }

    event = mobile._terminal_state_event(
        state,
        current_query="查询敏感信息",
        initial_checkpoint_id="checkpoint-old",
    )

    assert event == {"event": "values", "data": state["values"]}
    assert (
        mobile._terminal_state_event(
            state,
            current_query="另一个问题",
            initial_checkpoint_id="checkpoint-old",
        )
        is None
    )
    assert (
        mobile._terminal_state_event(
            state,
            current_query="查询敏感信息",
            initial_checkpoint_id="checkpoint-new",
        )
        is None
    )


def test_stream_recovers_done_from_terminal_state_when_sdk_stream_stalls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def stalled_producer(_chat_req, queue) -> None:
        await queue.put(
            {
                "kind": "start",
                "thread_id": "thread-stalled",
                "initial_checkpoint_id": "checkpoint-old",
            }
        )
        while True:
            # The SDK can keep yielding non-displayable transport chunks after
            # LangGraph has already committed the terminal checkpoint.
            await queue.put(
                {
                    "kind": "chunk",
                    "raw_event": {"event": "metadata", "data": {}},
                }
            )
            await asyncio.sleep(0.001)

    terminal_event = {
        "event": "values",
        "data": {
            "messages": [
                {"type": "human", "content": "查询敏感信息"},
                {
                    "type": "ai",
                    "content": REFUSAL,
                    "response_metadata": {
                        "compliance": {
                            "gate": "InputGate",
                            "scene": "public_release",
                            "actions": ["refuse"],
                            "violation_types": ["struct_id"],
                            "audit_ref": "audit-stalled",
                            "basis": ["input-policy"],
                            "retracted": True,
                        }
                    },
                },
            ]
        },
    }

    async def fake_terminal_event(*_args, **_kwargs):
        return terminal_event

    monkeypatch.setattr(mobile, "_produce_langgraph_stream", stalled_producer)
    monkeypatch.setattr(mobile, "_load_terminal_state_event", fake_terminal_event)
    monkeypatch.setattr(mobile, "MOBILE_STREAM_HEARTBEAT_SECONDS", 0.01)
    request = mobile.DeerFlowMobileRunRequest(
        query="查询敏感信息",
        conversation_id="conversation-stalled",
        stream=True,
    )

    async def collect_frames() -> list[str]:
        async def collect() -> list[str]:
            return [frame async for frame in mobile._stream_deerflow_mobile_run(request)]

        return await asyncio.wait_for(
            collect(),
            timeout=0.5,
        )

    frames = asyncio.run(collect_frames())

    assert any(frame.startswith("event: compliance\n") for frame in frames)
    done_payload = next(json.loads(frame.split("data: ", 1)[1]) for frame in frames if frame.startswith("event: done\n"))
    assert done_payload["thread_id"] == "thread-stalled"
    assert done_payload["compliance_results"][0]["audit_ref"] == "audit-stalled"
    assert frames[-1] == "data: [DONE]\n\n"
