from deerflow.compliance.contract import (
    DetectContext,
    DetectionUnit,
    Detector,
    FieldItem,
    TextItem,
)
from deerflow.compliance.detectors.regex_struct_id.detector import StructIdDetector


def make_text_unit(text: str, data_type: str = "program_code") -> DetectionUnit:
    return DetectionUnit(
        unit_id="u-struct",
        gate="ContextGate",
        data_type=data_type,
        text_items=(TextItem(item_id="t1", text=text, source="test"),),
    )


def new_detector() -> StructIdDetector:
    detector = StructIdDetector()
    detector.setup({"presidio_mode": "never"})
    return detector


def test_satisfies_contract_protocol():
    assert isinstance(StructIdDetector(), Detector)


def test_detects_phone_and_points_to_actual_span():
    text = "客户手机号为13800138000，请勿公开。"
    hits = new_detector().detect(
        make_text_unit(text),
        DetectContext(gate="ContextGate"),
    )
    assert hits
    assert hits[0].violation_type == "struct_id"
    location = hits[0].risk_locations[0]
    assert location.kind == "char_span"
    start, end = map(int, location.locator.split(":"))
    assert text[start:end] == "13800138000"


def test_ignores_example_email():
    hits = new_detector().detect(
        make_text_unit("联系邮箱为 user@example.com"),
        DetectContext(gate="ContextGate"),
    )
    assert hits == []


def test_detects_vehicle_plate():
    hits = new_detector().detect(
        make_text_unit("车牌陕A4L8W0通过卡口", data_type="traffic_flow"),
        DetectContext(gate="ContextGate"),
    )
    assert hits
    assert any(
        loc.text == "陕A4L8W0"
        for loc in hits[0].risk_locations
    )


def test_detects_imei_field():
    unit = DetectionUnit(
        unit_id="u-imei",
        gate="ContextGate",
        data_type="phone_network",
        field_items=(
            FieldItem(
                item_id="f1",
                path="features.imei",
                value="860123456789012",
                source="test",
            ),
        ),
    )
    hits = new_detector().detect(unit, DetectContext(gate="ContextGate"))
    assert hits
    assert any(
        loc.kind == "field_path" and loc.locator == "features.imei"
        for loc in hits[0].risk_locations
    )


def test_prefers_field_evidence_when_json_text_repeats_the_same_identifier():
    unit = DetectionUnit(
        unit_id="u-json-duplicate",
        gate="ContextGate",
        data_type="phone_network",
        text_items=(TextItem(item_id="t1", text='{"msisdn":"13800138000"}', source="query"),),
        field_items=(FieldItem(item_id="f1", path="msisdn", value="13800138000", source="query"),),
    )
    hits = new_detector().detect(unit, DetectContext(gate="ContextGate"))
    assert hits
    matching = [loc for loc in hits[0].risk_locations if loc.text == "13800138000"]
    assert len(matching) == 1
    assert matching[0].kind == "field_path"


def test_suppresses_private_transport_ip_when_record_explicitly_describes_a_safe_fixture():
    unit = DetectionUnit(
        unit_id="u-safe-private-ip",
        gate="ContextGate",
        data_type="network_flow",
        field_items=(
            FieldItem(item_id="f1", path="src_ip", value="10.41.11.16", source="query"),
            FieldItem(item_id="f2", path="description", value="测试流量不包含可复用凭据。", source="query"),
        ),
    )
    assert new_detector().detect(unit, DetectContext(gate="ContextGate")) == []


def test_suppresses_public_wifi_management_ip_without_business_identity():
    unit = DetectionUnit(
        unit_id="u-public-wifi-ip",
        gate="ContextGate",
        data_type="street_view",
        text_items=(
            TextItem(
                item_id="t1",
                text="公园内 WiFi AP 设备上贴有管理IP:10.10.1.51。",
                source="content",
            ),
        ),
    )
    assert new_detector().detect(unit, DetectContext(gate="ContextGate")) == []


def test_video_metadata_ids_are_owned_by_video_metadata_detector():
    unit = DetectionUnit(
        unit_id="u-video-metadata",
        gate="ContextGate",
        data_type="video_monitoring",
        field_items=(
            FieldItem(item_id="f1", path="metadata.camera_id", value="CAM-SYN-07-101", source="query"),
            FieldItem(item_id="f2", path="metadata.device_id", value="DEV-SYNTH-03101", source="query"),
            FieldItem(item_id="f3", path="metadata.channel_id", value="CH-01", source="query"),
        ),
    )
    assert new_detector().detect(unit, DetectContext(gate="ContextGate")) == []


def test_database_credential_uri_host_is_not_duplicate_struct_id():
    text = "MYSQL_DSN = 'mysql://workflow_user:Workflow_K007_259Qp371!vL@10.8.17.10:3306/workflow'"
    assert new_detector().detect(
        make_text_unit(text, data_type="program_code"),
        DetectContext(gate="ContextGate"),
    ) == []


def test_broker_credential_uri_host_is_not_duplicate_struct_id():
    text = 'RABBITMQ_URI = "amqp://mq_worker:MqWk#64TrP8@10.30.5.12:5672/events"'
    assert new_detector().detect(
        make_text_unit(text, data_type="program_code"),
        DetectContext(gate="ContextGate"),
    ) == []


def test_credential_uri_with_empty_username_is_not_duplicate_struct_id():
    text = "REDIS_URL = 'redis://:AlarmBus_K008_296Qp424!vL@172.16.9.28:6379/2'"
    assert new_detector().detect(
        make_text_unit(text, data_type="program_code"),
        DetectContext(gate="ContextGate"),
    ) == []


def test_network_flow_token_payload_uses_identifier_type_semantics():
    unit = DetectionUnit(
        unit_id="u-network-token",
        gate="InputGate",
        data_type="network_flow",
        field_items=(
            FieldItem(item_id="f1", path="application_data.identifier_type", value="access_token", source="test"),
            FieldItem(item_id="f2", path="application_data.value", value="Bearer eyJhbGciOiJIUzI1NiJ9.user-7842.signature", source="test"),
        ),
    )
    hits = new_detector().detect(unit, DetectContext(gate="InputGate"))
    assert hits
    assert any(location.entity_type == "NETWORK_IDENTIFIER" for location in hits[0].risk_locations)


def test_network_flow_endpoint_ip_is_transport_metadata_not_struct_id():
    unit = DetectionUnit(
        unit_id="u-network-endpoint",
        gate="ContextGate",
        data_type="network_flow",
        field_items=(
            FieldItem(
                item_id="f1",
                path="http.body.db_endpoint",
                value="mysql://finance_rw@10.70.1.9:3306/prod",
                source="test",
            ),
        ),
    )
    assert new_detector().detect(unit, DetectContext(gate="ContextGate")) == []


def test_network_flow_checkpoint_id_is_not_reclassified_as_struct_id():
    unit = DetectionUnit(
        unit_id="u-network-checkpoint",
        gate="ContextGate",
        data_type="network_flow",
        field_items=(
            FieldItem(item_id="f1", path="content.checkpoint_id", value="KK-20260527-001", source="test"),
        ),
    )
    assert new_detector().detect(unit, DetectContext(gate="ContextGate")) == []


def test_network_flow_field_name_only_supplement_is_not_a_real_identifier():
    unit = DetectionUnit(
        unit_id="u-network-field-name-only",
        gate="InputGate",
        data_type="network_flow",
        field_items=(
            FieldItem(item_id="f1", path="http.value", value="报告建议对access_token、session_id等字段进行脱敏", source="test"),
            FieldItem(item_id="f2", path="application_data.identifier_type", value="policy_text", source="test"),
        ),
    )
    assert new_detector().detect(unit, DetectContext(gate="InputGate")) == []


def test_ignores_masked_phone():
    hits = new_detector().detect(
        make_text_unit("手机号为138****5678"),
        DetectContext(gate="ContextGate"),
    )
    assert hits == []
