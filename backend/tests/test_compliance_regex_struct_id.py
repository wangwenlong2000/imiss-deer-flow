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


def test_ignores_masked_phone():
    hits = new_detector().detect(
        make_text_unit("手机号为138****5678"),
        DetectContext(gate="ContextGate"),
    )
    assert hits == []
