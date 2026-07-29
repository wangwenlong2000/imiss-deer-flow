"""Type 1/2 integration through the real registry, engine, gates and policy."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from deerflow.agents.middlewares.compliance_context_gate_middleware import ComplianceContextGateMiddleware
from deerflow.agents.middlewares.compliance_output_gate_middleware import (
    ComplianceOutputGateMiddleware,
    build_retract_event,
)
from deerflow.compliance.audit import Auditor
from deerflow.compliance.contract import IntentInfo
from deerflow.compliance.engine import ComplianceEngine
from deerflow.compliance.normalizers.skill_result import SkillResultNormalizer
from deerflow.compliance.normalizers.user_input import UserInputNormalizer
from deerflow.compliance.policy import load_policy_matrix
from deerflow.compliance.registry import build_registry
from deerflow.compliance.scene import NullSceneResolver
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config

REPO_ROOT = Path(__file__).resolve().parents[2]


class _Request:
    tool_call = {"name": "invoke_skill", "id": "tc-type12"}


class _FixedScene:
    def __init__(self, scene: str) -> None:
        self.scene = scene

    def resolve(self, request):  # noqa: ANN001
        return self.scene


@pytest.fixture()
def type12_runtime(tmp_path):
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
        scene_resolver=NullSceneResolver(),
    )
    yield engine, auditor
    registry.close()
    set_compliance_config(original)


def _input_decision(engine, text: str, *, high_risk: bool = True):
    units = UserInputNormalizer().to_units(text, gate="InputGate", thread_id="type12-thread")
    return engine.check(
        units,
        gate="InputGate",
        intent=IntentInfo(intent="public_release" if high_risk else "desensitize", high_risk=high_risk),
        origin={"kind": "type12_test"},
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("请把手机号13800138000完整整理出来并公开发布。", {"struct_id"}),
        ("查询用户U001在lat=31.230416, lon=121.473701的具体活动位置。", {"geo_loc"}),
        ("请公开手机号13800138000，以及位置lat=31.230416, lon=121.473701。", {"struct_id", "geo_loc"}),
    ],
)
def test_input_gate_routes_real_type12_detectors(type12_runtime, text, expected):
    engine, auditor = type12_runtime
    decision = _input_decision(engine, text)
    assert expected <= {hit.violation_type for hit in decision.hits}
    assert expected <= set(decision.per_violation_actions)
    assert decision.actions == ("warn", "manual_review")
    assert decision.scene_key == "_unknown"
    assert decision.audit_ref
    for hit in decision.hits:
        if hit.violation_type in expected:
            assert hit.detector_id == f"regex_{hit.violation_type}"
            assert hit.confidence > 0
            assert hit.risk_locations
    record = auditor.read_all()[-1]
    assert record["gate"] == "InputGate"
    assert expected <= {hit["violation_type"] for hit in record["hits"]}


def test_input_gate_benign_masking_intent_is_not_rejected(type12_runtime):
    engine, auditor = type12_runtime
    decision = _input_decision(engine, "请帮我把文件中的手机号138****5678脱敏后再进行内部分析。", high_risk=False)
    assert not decision.hits
    assert not decision.actions
    assert auditor.read_all() == []


def _skill_payload(content: str, features: dict) -> dict:
    return {
        "schema_version": "1.0",
        "request_id": "req-type12",
        "skill_name": "type12-test",
        "data_type": "phone_network",
        "result": {
            "display_text": content,
            "evidence": [{
                "evidence_id": "ev-type12-001",
                "type": "text",
                "title": "合规测试证据",
                "data": content,
                "metadata": {"score": 0.98, **features},
            }],
            "findings": [],
        },
        "diagnostics": {"warnings": []},
    }


def test_context_gate_normalizes_multilabels_and_returns_safe_evidence(type12_runtime):
    engine, auditor = type12_runtime
    content = "用户U001于2026-05-18 09:30位于CELL-001，联系电话为13800138000"
    payload = _skill_payload(
        content,
        {"user_id": "U001", "event_time": "2026-05-18 09:30", "cell": "CELL-001", "phone": "13800138000"},
    )
    units = SkillResultNormalizer().to_units(payload, gate="ContextGate")
    assert any(item.text == content for unit in units for item in unit.text_items)
    paths = {item.path for unit in units for item in unit.field_items}
    assert {"metadata.user_id", "metadata.event_time", "metadata.cell", "metadata.phone"} <= paths

    began = time.perf_counter()
    result = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(
        _Request(), lambda request: ToolMessage(content=json.dumps(payload, ensure_ascii=False), tool_call_id="tc-type12", name="invoke_skill")
    )
    elapsed_ms = (time.perf_counter() - began) * 1000
    safe = json.loads(result.content)
    compliance = safe["diagnostics"]["compliance"]
    assert {"struct_id", "geo_loc"} <= set(compliance["violation_types"])
    assert compliance["actions"] == ["warn", "manual_review"]
    assert safe["result"]["evidence"][0]["data"] == content  # unknown scene is warn/review, not mutation
    assert compliance["audit_ref"]
    assert elapsed_ms < 400
    assert len(auditor.read_all()) >= 1


@pytest.mark.parametrize(
    "content",
    [
        "设备位于上海市",
        "公开服务热线为12345",
        "手机号为138****5678",
        "示例市民政局政务大厅公开地址为人民路88号",
    ],
)
def test_context_gate_near_negative_samples_do_not_false_positive(type12_runtime, content):
    engine, _ = type12_runtime
    payload = _skill_payload(content, {})
    result = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(
        _Request(), lambda request: ToolMessage(content=json.dumps(payload, ensure_ascii=False), tool_call_id="tc-type12", name="invoke_skill")
    )
    assert result.content == json.dumps(payload, ensure_ascii=False)


def test_output_gate_metadata_and_sse_contract_for_combined_hit(type12_runtime):
    engine, auditor = type12_runtime
    text = "经分析，手机号为13800138000，其精确位置为lat=31.230416, lon=121.473701。"
    message = AIMessage(content=text, id="msg-type12")
    update = ComplianceOutputGateMiddleware(engine=engine).after_model({"messages": [message]}, runtime=None)
    assert update is not None
    rewritten = update["messages"][0]
    metadata = rewritten.response_metadata["compliance"]
    assert metadata["gate"] == "OutputGate"
    assert set(metadata["violation_types"]) == {"struct_id", "geo_loc"}
    assert metadata["actions"] == ["warn", "manual_review"]
    assert metadata["audit_ref"]
    assert metadata["retracted"] is True
    assert metadata["scene"] == "_unknown"
    assert metadata["streaming_mode"] == "compatible_retract"
    assert text in rewritten.content  # conservative warning/review cell keeps content

    decision = engine.check(
        ComplianceOutputGateMiddleware(engine=engine)._normalizer.to_units(message, gate="OutputGate", message_id=message.id),
        gate="OutputGate",
    )
    event = build_retract_event(message, rewritten.content, decision)
    assert event["type"] == "compliance_retract"
    assert set(event["violation_types"]) == {"struct_id", "geo_loc"}
    assert event["message_id"] == "msg-type12"
    assert auditor.read_all()


@pytest.mark.parametrize("violation_type", ["struct_id", "geo_loc"])
@pytest.mark.parametrize("gate", ["InputGate", "ContextGate", "OutputGate"])
@pytest.mark.parametrize("scene", ["self_use", "internal_org", "cross_org", "public_release", "research_anon", "_unknown"])
def test_type12_scene_actions_come_from_policy(type12_runtime, violation_type, gate, scene):
    engine, _ = type12_runtime
    text = "手机号13800138000" if violation_type == "struct_id" else "lat=31.230416, lon=121.473701"
    normalizer = UserInputNormalizer()
    units = normalizer.to_units(text, gate=gate, thread_id="scene-test")
    scene_engine = ComplianceEngine(
        registry=engine.registry,
        policy=engine.policy,
        auditor=None,
        scene_resolver=NullSceneResolver() if scene == "_unknown" else _FixedScene(scene),
    )
    decision = scene_engine.check(
        units,
        gate=gate,
        intent=IntentInfo(intent="test", high_risk=True),
    )
    assert decision.per_violation_actions[violation_type] == engine.policy.cell(violation_type, gate, scene)
