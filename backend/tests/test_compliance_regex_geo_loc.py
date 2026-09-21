from deerflow.compliance.contract import (
    DetectContext,
    DetectionUnit,
    Detector,
    FieldItem,
    TextItem,
)
from deerflow.compliance.detectors.regex_geo_loc.detector import GeoLocDetector


def make_text_unit(
    text: str,
    data_type: str = "spatiotemporal_trajectory",
) -> DetectionUnit:
    return DetectionUnit(
        unit_id="u-geo",
        gate="ContextGate",
        data_type=data_type,
        text_items=(TextItem(item_id="t1", text=text, source="test"),),
    )


def new_detector() -> GeoLocDetector:
    detector = GeoLocDetector()
    detector.setup({"presidio_mode": "never"})
    return detector


def test_satisfies_contract_protocol():
    assert isinstance(GeoLocDetector(), Detector)


def test_detects_precise_coordinate_and_points_to_span():
    text = "设备位置 lat=31.230416, lon=121.473701"
    hits = new_detector().detect(
        make_text_unit(text),
        DetectContext(gate="ContextGate"),
    )
    assert hits
    assert hits[0].violation_type == "geo_loc"
    assert any(
        loc.kind == "char_span" and loc.text == "lat=31.230416"
        for loc in hits[0].risk_locations
    )


def test_detects_labelled_chinese_coordinate_pair():
    text = "地理坐标：纬度 39.9219，经度 116.4436"
    hits = new_detector().detect(
        make_text_unit(text),
        DetectContext(gate="OutputGate"),
    )

    assert hits
    assert any(
        loc.kind == "char_span" and "纬度 39.9219" in (loc.text or "")
        for loc in hits[0].risk_locations
    )


def test_detects_labelled_coordinate_pair_in_markdown_table():
    text = "| 字段 | 原始值 | 处置方式 | 处置后值 |\n| **纬度** | 39.9219 | 无需处置 | 39.9219 |\n| **经度** | 116.4436 | 无需处置 | 116.4436 |"
    hits = new_detector().detect(
        make_text_unit(text),
        DetectContext(gate="OutputGate"),
    )

    assert hits
    assert any("39.9219" in (loc.text or "") for loc in hits[0].risk_locations)


def test_ignores_explicitly_fictitious_coordinate_example():
    text = "报告中的示意坐标为虚构中心点 30.0000,120.0000，并标注不代表真实点位。"
    assert new_detector().detect(
        make_text_unit(text),
        DetectContext(gate="OutputGate"),
    ) == []


def test_reconstructs_generic_coordinate_array_from_json_fields():
    unit = DetectionUnit(
        unit_id="u-coordinate-array",
        gate="ContextGate",
        data_type="spatiotemporal_trajectory",
        field_items=(
            FieldItem(item_id="f1", path="location.coordinates[0]", value=31.230416, source="query"),
            FieldItem(item_id="f2", path="location.coordinates[1]", value=121.473701, source="query"),
        ),
    )
    hits = new_detector().detect(unit, DetectContext(gate="ContextGate"))
    assert hits
    assert any(
        loc.kind == "field_path" and loc.locator == "location.coordinates"
        for loc in hits[0].risk_locations
    )


def test_reconstructs_geojson_longitude_latitude_array():
    unit = DetectionUnit(
        unit_id="u-geojson-array",
        gate="ContextGate",
        data_type="remote_sensing",
        field_items=(
            FieldItem(item_id="f1", path="geometry.coordinates[0]", value=121.473701, source="query"),
            FieldItem(item_id="f2", path="geometry.coordinates[1]", value=31.230416, source="query"),
        ),
    )
    hits = new_detector().detect(unit, DetectContext(gate="ContextGate"))
    assert hits


def test_ignores_explicitly_synthetic_remote_metadata_coordinate():
    unit = DetectionUnit(
        unit_id="u-synthetic-remote",
        gate="InputGate",
        data_type="satellite_remote_sensing",
        field_items=(
            FieldItem(item_id="f1", path="content.captions[0]", value="LAT=31.0000,LON=121.0000", source="eval"),
            FieldItem(item_id="f2", path="content.metadata.synthetic_coordinates.synthetic", value=True, source="eval"),
        ),
    )
    assert new_detector().detect(unit, DetectContext(gate="InputGate")) == []


def test_detects_lat_lng_slash_table_format():
    text = "| lat / lng | 39.9219 / 116.4436 |"
    hits = new_detector().detect(
        make_text_unit(text),
        DetectContext(gate="OutputGate"),
    )

    assert hits
    assert any("39.9219" in (loc.text or "") for loc in hits[0].risk_locations)


def test_detects_raw_coordinate_inside_a_redaction_disclaimer():
    text = "原始坐标已泛化说明，但报告仍包含 39.9219, 116.4436。"
    hits = new_detector().detect(
        make_text_unit(text),
        DetectContext(gate="OutputGate"),
    )

    assert hits


def test_ignores_city_level_location():
    hits = new_detector().detect(
        make_text_unit("设备位于上海市"),
        DetectContext(gate="ContextGate"),
    )
    assert hits == []


def test_detects_single_object_route():
    text = (
        "匿名对象 TRK-015 连续路线："
        "07:30 上海市长宁区虹桥交通枢纽 -> "
        "09:20 上海市浦东新区陆家嘴金融区 -> "
        "18:35 上海市黄浦区人民广场商圈"
    )
    hits = new_detector().detect(
        make_text_unit(text),
        DetectContext(gate="ContextGate"),
    )
    assert hits
    assert any(
        loc.entity_type == "single_object_precise_route"
        for loc in hits[0].risk_locations
    )


def test_ignores_public_government_service_address():
    text = (
        "请提供示例市民政局政务服务大厅公开办公地址和公众导航指引"
        "（如示例市人民路88号）"
    )
    hits = new_detector().detect(
        make_text_unit(text),
        DetectContext(gate="ContextGate"),
    )
    assert hits == []


def test_detects_telecom_object_time_cell_combination():
    unit = DetectionUnit(
        unit_id="u-telecom",
        gate="ContextGate",
        data_type="phone_network",
        field_items=(
            FieldItem(item_id="f1", path="features.user_id", value="U123", source="test"),
            FieldItem(
                item_id="f2",
                path="features.event_time",
                value="2026-05-18 09:30",
                source="test",
            ),
            FieldItem(item_id="f3", path="features.cell", value="CELL-001", source="test"),
        ),
    )
    hits = new_detector().detect(unit, DetectContext(gate="ContextGate"))
    assert hits
    assert any(
        loc.kind == "field_path" and loc.locator == "features.cell"
        for loc in hits[0].risk_locations
    )
