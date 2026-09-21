"""Contract and regression tests for the compliance_detectors delivery."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from deerflow.compliance.actions import REFUSAL_TEXT, apply_actions
from deerflow.compliance.contract import (
    DetectContext,
    DetectionUnit,
    Detector,
    FieldItem,
    TextItem,
)
from deerflow.compliance.detectors.entropy_hardcoded_cred.detector import (
    EntropyHardcodedCredDetector,
)
from deerflow.compliance.detectors.delivered_rules import configured_llm
from deerflow.compliance.detectors.delivered_rules.configured_llm import (
    ConfiguredLLMAdapter,
)
from deerflow.compliance.detectors.llm_confidential.detector import (
    LlmConfidentialDetector,
)
from deerflow.compliance.detectors.ner_text_id.detector import NerTextIdDetector
from deerflow.compliance.engine import ComplianceEngine
from deerflow.compliance.policy import load_policy_matrix
from deerflow.compliance.registry import build_registry

ROOT = Path(__file__).resolve().parents[2]
DETECTORS_CONFIG = ROOT / "config/compliance/detectors.yaml"
POLICY_CONFIG = ROOT / "config/compliance/policy_matrix.yaml"


def _text_unit(text: str, gate: str = "ContextGate", data_type: str | None = None) -> DetectionUnit:
    return DetectionUnit(
        unit_id="delivery-test",
        gate=gate,
        data_type=data_type,
        text_items=(TextItem(item_id="t-1", text=text, source="query"),),
    )


def _detect(detector, unit: DetectionUnit):
    detector.setup({"use_llm": False})
    return detector.detect(unit, DetectContext(gate=unit.gate))


@pytest.mark.parametrize(
    "detector",
    [
        EntropyHardcodedCredDetector(),
        NerTextIdDetector(),
        LlmConfidentialDetector(),
    ],
)
def test_delivered_detectors_satisfy_contract_protocol(detector) -> None:
    assert isinstance(detector, Detector)


def test_strict_registry_discovers_and_enables_all_delivered_detectors() -> None:
    registry = build_registry(config_path=DETECTORS_CONFIG, strict=True)
    expected = {
        "entropy_hardcoded_cred",
        "ner_text_id",
        "llm_confidential",
    }
    assert expected <= {registration.detector_id for registration in registry.enabled()}
    assert registry.get("entropy_hardcoded_cred").gates == {
        "hardcoded_cred": ("InputGate", "OutputGate")
    }
    for detector_id in ("ner_text_id", "llm_confidential"):
        registration = registry.get(detector_id)
        assert registration.cost_hint == "medium"
        assert registration.params["use_llm"] is True
        assert "model_name" not in registration.params
        assert registration.params["llm_timeout_seconds"] == 7


def test_hardcoded_credential_positive_has_exact_span_and_no_policy_action() -> None:
    text = 'client_secret = "prod-secret-A7z9K2"'
    hits = _detect(
        EntropyHardcodedCredDetector(),
        _text_unit(text, gate="InputGate", data_type="code"),
    )

    assert len(hits) == 1
    hit = hits[0]
    assert hit.detector_id == "entropy_hardcoded_cred"
    assert hit.violation_type == "hardcoded_cred"
    assert any(location.text == "prod-secret-A7z9K2" for location in hit.risk_locations)
    assert "suggested_action" not in hit.evidence


@pytest.mark.parametrize(
    ("gate", "scene", "expected_actions"),
    [
        ("InputGate", "public_release", ("refuse",)),
        ("OutputGate", "public_release", ("desensitize", "refuse")),
        ("InputGate", "internal_org", ("warn", "role_check")),
        ("OutputGate", "internal_org", ("warn", "role_check")),
    ],
)
def test_hardcoded_credential_flows_through_engine_to_scene_policy(
    gate: str,
    scene: str,
    expected_actions: tuple[str, ...],
) -> None:
    class _FixedSceneResolver:
        def resolve(self, request):  # noqa: ANN001, ARG002
            return scene

    registry = build_registry(config_path=DETECTORS_CONFIG, strict=True)
    engine = ComplianceEngine(
        registry=registry,
        policy=load_policy_matrix(POLICY_CONFIG),
        scene_resolver=_FixedSceneResolver(),
    )
    unit = _text_unit(
        'client_secret = "prod-secret-A7z9K2"',
        gate=gate,
        data_type="code",
    )

    decision = engine.check((unit,), gate=gate)

    assert "hardcoded_cred" in {hit.violation_type for hit in decision.hits}
    assert decision.actions == expected_actions


def test_hardcoded_credential_is_not_routed_at_context_gate() -> None:
    registry = build_registry(config_path=DETECTORS_CONFIG, strict=True)
    engine = ComplianceEngine(
        registry=registry,
        policy=load_policy_matrix(POLICY_CONFIG),
    )
    unit = _text_unit(
        'client_secret = "prod-secret-A7z9K2"',
        gate="ContextGate",
        data_type="code",
    )

    decision = engine.check((unit,), gate="ContextGate")

    assert "hardcoded_cred" not in {hit.violation_type for hit in decision.hits}


def test_public_output_masks_credential_and_refuses_when_location_is_unreliable() -> None:
    secret = "generated-secret-A7z9K2"
    text = f'client_secret = "{secret}"'
    hits = _detect(
        EntropyHardcodedCredDetector(),
        _text_unit(text, gate="OutputGate", data_type="code"),
    )
    actions = load_policy_matrix(POLICY_CONFIG).resolve(
        "hardcoded_cred", "OutputGate", "public_release"
    ).actions

    masked = apply_actions(text, actions, hits)
    assert masked is not None
    assert secret not in masked
    assert masked != REFUSAL_TEXT

    unlocatable_hits = (replace(hits[0], risk_locations=()),)
    assert apply_actions(text, actions, unlocatable_hits) == REFUSAL_TEXT


def test_hardcoded_credential_ignores_environment_and_placeholder_values() -> None:
    detector = EntropyHardcodedCredDetector()
    assert not _detect(
        detector,
        _text_unit('api_key = os.getenv("API_KEY")', gate="InputGate", data_type="code"),
    )
    assert not _detect(
        detector,
        _text_unit('api_key = "your_api_key"', gate="InputGate", data_type="code"),
    )
    assert not _detect(
        detector,
        _text_unit('client_secret = "****************"', gate="InputGate", data_type="code"),
    )
    assert not _detect(
        detector,
        _text_unit(
            "mysql://user:password@localhost:3306/demo",
            gate="InputGate",
            data_type="network_flow",
        ),
    )


def test_text_id_relationship_positive_and_anonymized_negative() -> None:
    detector = NerTextIdDetector()
    positive = "投诉人姓名：王晓丹，联系电话：13800138000。"
    hits = _detect(detector, _text_unit(positive, data_type="policy"))

    assert len(hits) == 1
    assert hits[0].violation_type == "text_id"
    assert "person-phone-relation" in hits[0].evidence["matched_rules"]
    located = {location.text for location in hits[0].risk_locations}
    assert {"王晓丹", "13800138000"} <= located

    negative = "演示样例已将姓名替换为张某，单位替换为某街道，不对应真实人员。"
    assert not _detect(detector, _text_unit(negative, data_type="policy"))


def test_text_id_does_not_treat_unlabelled_public_signage_as_a_person() -> None:
    detector = NerTextIdDetector()
    text = '街景中可见某商业街有"消费者投诉热线12315 保护您的合法权益"的市场监督管理局公告牌'
    assert not _detect(detector, _text_unit(text, data_type="street_view"))


def test_text_id_does_not_treat_masked_property_service_phone_as_personal_id() -> None:
    detector = NerTextIdDetector()
    text = '街景中可见某住宅楼外墙有"XX物业 服务电话010-XXXX-5566"的物业公告'
    assert not _detect(detector, _text_unit(text, data_type="street_view"))


def test_confidential_structured_combination_keeps_field_paths() -> None:
    unit = DetectionUnit(
        unit_id="confidential-fields",
        gate="ContextGate",
        data_type="remote_sensing",
        text_items=(
            TextItem(
                item_id="t-1",
                text="卫星遥感任务材料。",
                source="evidence-1",
            ),
        ),
        field_items=(
            FieldItem(
                item_id="f-1",
                path="metadata.classification_level",
                value="国家秘密·秘密级",
                source="evidence-1",
            ),
            FieldItem(
                item_id="f-2",
                path="metadata.access_scope",
                value="仅限涉密授权人员按需知悉",
                source="evidence-1",
            ),
        ),
    )
    hits = _detect(LlmConfidentialDetector(), unit)

    assert len(hits) == 1
    assert hits[0].violation_type == "confidential"
    paths = {
        location.locator
        for location in hits[0].risk_locations
        if location.kind == "field_path"
    }
    assert {
        "metadata.classification_level",
        "metadata.access_scope",
    } <= paths


def test_confidential_public_summary_is_a_near_negative() -> None:
    text = "公开财政预算汇总，district_total 已公开且不含供应商明细。"
    assert not _detect(
        LlmConfidentialDetector(),
        _text_unit(text, data_type="policy"),
    )


def test_confidential_yearbook_contract_detects_internal_supplement() -> None:
    unit = DetectionUnit(
        unit_id="yearbook-internal-supplement",
        gate="ContextGate",
        data_type="statistical_yearbook",
        field_items=tuple(
            FieldItem(item_id=f"f-{index}", path=path, value=value, source="eval")
            for index, (path, value) in enumerate(
                (
                    ("content.base_table", "统计年鉴内部工作底稿"),
                    ("content.internal_document", "灾备演练方案"),
                    ("content.sensitive_field", "灾备切换策略"),
                    ("content.sensitive_value", "核心汇编库切换顺序为采集层、校验层、发布层"),
                    ("content.restriction", "内部资料，未经授权不得跨组织发送或公开发布。"),
                ),
                start=1,
            )
        ),
    )
    hits = _detect(LlmConfidentialDetector(), unit)
    assert len(hits) == 1
    assert hits[0].violation_type == "confidential"


def test_confidential_network_security_level_is_explicit() -> None:
    unit = DetectionUnit(
        unit_id="network-secret-level",
        gate="InputGate",
        data_type="network_flow",
        field_items=(
            FieldItem(item_id="f-1", path="application_data.security_level", value="机密", source="eval"),
            FieldItem(item_id="f-2", path="application_data.value", value="customer_pricing_strategy_internal", source="eval"),
        ),
    )
    hits = _detect(LlmConfidentialDetector(), unit)
    assert len(hits) == 1
    assert hits[0].violation_type == "confidential"


def test_confidential_public_remote_sensing_reference_is_not_a_secret() -> None:
    unit = DetectionUnit(
        unit_id="public-remote-reference",
        gate="InputGate",
        data_type="satellite_remote_sensing",
        field_items=(
            FieldItem(item_id="f-1", path="content.source_file", value="public_wikimedia/airbase.tiff", source="eval"),
            FieldItem(item_id="f-2", path="content.captions[0]", value="Public source record title: satellite image of an airbase.", source="eval"),
            FieldItem(item_id="f-3", path="content.captions[1]", value="This is a classified military base.", source="eval"),
            FieldItem(item_id="f-4", path="content.image_url", value="https://commons.wikimedia.org/wiki/File:airbase.tiff", source="eval"),
            FieldItem(item_id="f-5", path="content.facility_type", value="military_base", source="eval"),
        ),
    )
    assert not _detect(LlmConfidentialDetector(), unit)


def test_inprocess_delivery_uses_current_conversation_model_at_output_gate(monkeypatch) -> None:
    created_with: list[tuple[str | None, float]] = []

    class FakeModel:
        def bind(self, **kwargs):
            assert kwargs == {"temperature": 0, "max_tokens": 512}
            return self

        def with_config(self, config):
            assert config["run_name"] == "ComplianceTextIdBoundaryReview"
            return self

        def invoke(self, prompt):
            assert "林知远在公开会议发言" in prompt
            return type(
                "Response",
                (),
                {
                    "content": (
                        '{"is_hit":true,"confidence":0.91,'
                        '"risk_type":"multi_entity_combo",'
                        '"risk_spans":[{"text":"林知远在公开会议发言",'
                        '"reason":"可识别个人"}],"reason":"边界复核命中"}'
                    )
                },
            )()

    def create_model(model_name, timeout_seconds):
        created_with.append((model_name, timeout_seconds))
        return FakeModel()

    monkeypatch.setattr(configured_llm, "_create_configured_model", create_model)
    detector = NerTextIdDetector()
    detector.setup(
        {
            "use_llm": True,
            "llm_max_content_chars": 2500,
        }
    )
    text = "记录显示，林知远在公开会议发言。"
    hits = detector.detect(
        _text_unit(text, gate="OutputGate", data_type="policy"),
        DetectContext(gate="OutputGate", model_name="conversation-model"),
    )

    assert created_with == [("conversation-model", 7.0)]
    assert len(hits) == 1
    assert hits[0].confidence == pytest.approx(0.91)
    assert "llm-text-id-review" in hits[0].evidence["matched_rules"]
    assert any(location.text == "林知远在公开会议发言" for location in hits[0].risk_locations)


def test_inprocess_delivery_without_conversation_model_skips_llm(monkeypatch) -> None:
    created_with: list[tuple[str, float]] = []
    monkeypatch.setattr(
        configured_llm,
        "_create_configured_model",
        lambda model_name, timeout_seconds: created_with.append(
            (model_name, timeout_seconds)
        ),
    )
    detector = NerTextIdDetector()
    detector.setup({"use_llm": True})

    hits = detector.detect(
        _text_unit("记录显示，林知远在公开会议发言。", data_type="policy"),
        DetectContext(gate="OutputGate"),
    )

    assert created_with == []
    assert hits == ()


def test_context_gate_never_calls_configured_llm(monkeypatch) -> None:
    created_with: list[str] = []

    monkeypatch.setattr(
        configured_llm,
        "_create_configured_model",
        lambda model_name, _timeout_seconds: created_with.append(model_name),
    )
    detector = NerTextIdDetector()
    detector.setup({"use_llm": True})

    hits = detector.detect(
        _text_unit("记录显示，林知远在公开会议发言。", data_type="policy"),
        DetectContext(gate="ContextGate", model_name="conversation-model"),
    )

    assert created_with == []
    assert hits == ()


def test_hardcoded_credential_is_available_as_output_backstop() -> None:
    hits = _detect(
        EntropyHardcodedCredDetector(),
        _text_unit('client_secret = "generated-secret-A7z9K2"', gate="OutputGate", data_type="code"),
    )
    assert len(hits) == 1
    assert hits[0].violation_type == "hardcoded_cred"


def test_configured_llm_caches_models_by_conversation_model(monkeypatch) -> None:
    created_with: list[str] = []

    class FakeModel:
        def invoke(self, _prompt):
            return type(
                "Response",
                (),
                {
                    "content": (
                        '{"is_hit":false,"confidence":0,'
                        '"risk_type":"none","risk_spans":[],"reason":"clean"}'
                    )
                },
            )()

    def create_model(model_name, _timeout_seconds):
        created_with.append(model_name)
        return FakeModel()

    monkeypatch.setattr(configured_llm, "_create_configured_model", create_model)
    adapter = ConfiguredLLMAdapter()
    sample = {"content_text": "boundary", "features": {}, "gate": "OutputGate"}

    for model_name in ("model-a", "model-b", "model-a"):
        with adapter.use_model(model_name):
            adapter.review_confidential(sample, [])

    assert created_with == ["model-a", "model-b"]


def test_configured_llm_failure_returns_low_confidence_fallback(caplog) -> None:
    class FailingModel:
        def invoke(self, _prompt):
            raise TimeoutError("secret payload must not appear in logs")

    adapter = ConfiguredLLMAdapter(model=FailingModel())
    result = adapter.review_confidential(
        {
            "content_text": "sensitive payload",
            "features": {},
            "gate": "ContextGate",
        },
        [],
    )

    assert result["confidence"] == 0.0
    assert result["is_hit"] is False
    assert "falling back to local rules" in caplog.text
    assert "sensitive payload" not in caplog.text
    assert "secret payload" not in caplog.text
