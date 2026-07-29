"""Deterministic HTTP/SSE proof for strict OutputGate buffering."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from deerflow.agents.middlewares.compliance_input_gate_middleware import ComplianceInputGateMiddleware
from deerflow.agents.middlewares.compliance_output_gate_middleware import ComplianceOutputGateMiddleware
from deerflow.agents.middlewares.intent_recognition_middleware import IntentRecognitionMiddleware
from deerflow.agents.middlewares.raw_transcript_middleware import RawTranscriptMiddleware
from deerflow.compliance.audit import Auditor
from deerflow.compliance.engine import ComplianceEngine
from deerflow.compliance.policy import load_policy_matrix
from deerflow.compliance.registry import build_registry
from deerflow.compliance.scene import TrustedSceneResolver
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config

REPO_ROOT = Path(__file__).resolve().parents[2]
PHONE = "13800138000"
LATITUDE = "31.230416"
LONGITUDE = "121.473701"
OUTPUTS = {
    "struct_id": f"用户手机号为{PHONE}。",
    "geo_loc": f"用户位置为lat={LATITUDE}, lon={LONGITUDE}。",
    "combined": f"用户手机号为{PHONE}，精确位置为lat={LATITUDE}, lon={LONGITUDE}。",
    "negative": "手机号脱敏通常使用部分字符替换，精确位置可以降到区县级。",
}


def _context(scene: str) -> dict:
    operation = {
        "self_use": ("analysis", "self"),
        "internal_org": ("analysis", "organization"),
        "cross_org": ("share", "external_org"),
        "public_release": ("public_release", "public"),
        "research_anon": ("research", "research_group"),
        "_unknown": ("analysis", "self"),
    }
    operation_type, audience = operation[scene]
    actor = {
        "user_id": "user-001",
        "org_id": "org-001",
        "department_id": "dept-001",
        "roles": ["researcher"] if scene == "research_anon" else ["analyst"],
        "permissions": ["statistics.research"] if scene == "research_anon" else ["traffic.read"],
    }
    resource = {
        "resource_id": "resource-001",
        "owner_user_id": "user-001",
        "owner_org_id": "org-001",
        "owner_department_id": "dept-001",
        "target_org_id": "org-002" if scene == "cross_org" else None,
        "classification": "confidential",
        "is_anonymized": scene == "research_anon",
    }
    result = {
        "request_id": f"req-stream-{scene}",
        "actor": actor,
        "operation": {"type": operation_type, "target_audience": audience},
        "resource": resource,
        "scene_hint": scene,
    }
    if scene == "_unknown":
        result.pop("actor")
    return result


def _prompt(scene: str) -> str:
    return {
        "self_use": "请进行本人数据分析。",
        "internal_org": "请用于组织内部分析。",
        "cross_org": "请跨机构共享安全摘要。",
        "public_release": "请生成对外公开的安全摘要。",
        "research_anon": "请用于科研统计研究。",
        "_unknown": "请进行分析。",
    }[scene]


def _fake_model(text: str) -> GenericFakeChatModel:
    def messages():
        while True:
            yield AIMessage(content=text, id="deterministic-output")

    return GenericFakeChatModel(messages=messages())


@pytest.fixture()
def strict_runtime(tmp_path):
    original = get_compliance_config()
    set_compliance_config(ComplianceConfig(enabled=True))
    registry = build_registry(config_path=REPO_ROOT / "config/compliance/detectors.yaml", strict=True)
    model = registry.get("model_tfidf_knn")
    if model is not None:
        model.enabled = False
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    engine = ComplianceEngine(
        registry=registry,
        policy=load_policy_matrix(REPO_ROOT / "config/compliance/policy_matrix.yaml"),
        auditor=auditor,
        scene_resolver=TrustedSceneResolver(),
    )
    yield engine, auditor
    registry.close()
    set_compliance_config(original)


def _agent(engine: ComplianceEngine, output: str):
    return create_agent(
        model=_fake_model(output),
        tools=[],
        middleware=[
            ComplianceOutputGateMiddleware(engine=engine),
            RawTranscriptMiddleware(),
            IntentRecognitionMiddleware(model_name="unused"),
            ComplianceInputGateMiddleware(engine=engine),
        ],
    )


async def _stream(agent, scene: str) -> tuple[list[tuple[str, object]], dict]:
    events: list[tuple[str, object]] = []
    final: dict = {}
    graph_input = {
        "messages": [{"role": "user", "content": _prompt(scene)}],
        "compliance_request": _context(scene),
    }
    async for mode, data in agent.astream(
        graph_input,
        context={"thread_id": f"thread-{scene}"},
        stream_mode=["messages", "updates", "custom", "values"],
    ):
        events.append((mode, data))
        if mode == "values" and isinstance(data, dict):
            final = data
    return events, final


def _dump(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=lambda item: getattr(item, "content", str(item)))


@pytest.mark.parametrize("scene", ["public_release", "cross_org", "research_anon"])
@pytest.mark.parametrize("kind", ["struct_id", "geo_loc", "combined"])
def test_strict_scenes_never_emit_raw_sensitive_output(strict_runtime, scene, kind):
    engine, auditor = strict_runtime
    events, final = asyncio.run(_stream(_agent(engine, OUTPUTS[kind]), scene))
    rendered = _dump(events)
    final_rendered = _dump(final)

    assert PHONE not in rendered
    assert LATITUDE not in rendered
    assert LONGITUDE not in rendered
    assert PHONE not in final_rendered
    assert LATITUDE not in final_rendered
    assert LONGITUDE not in final_rendered

    assistant = next(message for message in reversed(final["messages"]) if isinstance(message, AIMessage))
    compliance = assistant.response_metadata["compliance"]
    expected = {"struct_id"} if kind == "struct_id" else {"geo_loc"} if kind == "geo_loc" else {"struct_id", "geo_loc"}
    assert set(compliance["violation_types"]) == expected
    assert compliance["scene"] == scene
    assert compliance["streaming_mode"] == "strict_buffered"
    assert compliance["transient_exposure_possible"] is False
    assert assistant in final["raw_messages"]
    audit = auditor.read_all()[-1]
    assert audit["origin"]["streaming_mode"] == "strict_buffered"
    assert PHONE not in _dump(audit)
    assert LATITUDE not in _dump(audit)
    assert LONGITUDE not in _dump(audit)
    assert "[redacted sha256:" in _dump(audit)


@pytest.mark.parametrize("scene", ["self_use", "internal_org", "_unknown"])
def test_compatible_scene_documents_retract_window_but_persists_safe_state(strict_runtime, scene):
    engine, auditor = strict_runtime
    events, final = asyncio.run(_stream(_agent(engine, OUTPUTS["combined"]), scene))
    messages_stream = _dump([data for mode, data in events if mode == "messages"])
    final_rendered = _dump(final)

    # Compatibility mode intentionally permits the original model chunk before
    # the authoritative replacement. This is the behavior strict mode removes.
    assert PHONE in messages_stream
    assistant = next(message for message in reversed(final["messages"]) if isinstance(message, AIMessage))
    metadata = assistant.response_metadata["compliance"]
    assert metadata["streaming_mode"] == "compatible_retract"
    assert metadata["transient_exposure_possible"] is True
    if scene == "internal_org":
        assert PHONE not in final_rendered
        assert LATITUDE not in final_rendered
    elif scene == "self_use":
        # self_use is an explicit matrix allow; the content is policy-safe for
        # its owner even though compatibility transport exposed model chunks.
        assert metadata["actions"] == ["allow"]
    else:
        assert metadata["actions"] == ["warn", "manual_review"]
        assert PHONE in final_rendered
    assert auditor.read_all()[-1]["origin"]["transient_exposure_possible"] is True


def test_conceptual_negative_is_not_flagged(strict_runtime):
    engine, auditor = strict_runtime
    events, final = asyncio.run(_stream(_agent(engine, OUTPUTS["negative"]), "public_release"))
    assert OUTPUTS["negative"] in _dump(final)
    assistant = next(message for message in reversed(final["messages"]) if isinstance(message, AIMessage))
    assert "compliance" not in assistant.response_metadata
    assert auditor.read_all() == []
    # The strict model may emit the completed clean message, but never partial
    # pre-gate chunks. This negative is safe and therefore remains unchanged.
    assert OUTPUTS["negative"] in _dump([data for mode, data in events if mode == "messages"])


def test_real_http_sse_endpoint_never_contains_combined_raw_values(strict_runtime):
    engine, _ = strict_runtime
    agent = _agent(engine, OUTPUTS["combined"])
    app = FastAPI()

    @app.post("/threads/{thread_id}/runs/stream")
    async def stream_run(thread_id: str, request: Request):
        body = await request.json()

        async def event_source() -> AsyncIterator[str]:
            async for mode, data in agent.astream(
                body["input"],
                context={"thread_id": thread_id},
                stream_mode=["messages", "updates", "custom", "values"],
            ):
                yield f"event: {mode}\ndata: {_dump(data)}\n\n"

        return StreamingResponse(event_source(), media_type="text/event-stream")

    with TestClient(app) as client:
        response = client.post(
            "/threads/http-strict/runs/stream",
            json={
                "assistant_id": "lead_agent",
                "input": {
                    "messages": [{"role": "user", "content": _prompt("public_release")}],
                    "compliance_request": _context("public_release"),
                },
            },
        )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert PHONE not in response.text
    assert LATITUDE not in response.text
    assert LONGITUDE not in response.text
    assert "strict_buffered" in response.text
    assert "struct_id" in response.text
    assert "geo_loc" in response.text
