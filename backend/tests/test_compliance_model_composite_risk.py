"""Cross-source structural backstops for model detector types 8, 9 and 10."""

from __future__ import annotations

from deerflow.compliance.contract import DetectionUnit, TextItem
from deerflow.compliance.detectors.model_tfidf_knn.composite_risk import (
    detect_composite_findings,
    is_explicit_aggregate_negative,
)
from deerflow.compliance.detectors.model_tfidf_knn.detector import ModelTfidfKnnDetector


def _unit(text: str, *, gate: str = "OutputGate", data_type: str = "yearbook") -> DetectionUnit:
    return DetectionUnit(
        unit_id="composite-test",
        gate=gate,
        data_type=data_type,
        text_items=(TextItem(item_id="t-1", text=text, source="content"),),
    )


def _types(
    text: str,
    *,
    gate: str = "OutputGate",
    data_type: str = "yearbook",
    allowed_data_types: set[str] | None = None,
) -> set[str]:
    return {
        finding.violation_type
        for finding in detect_composite_findings(
            _unit(text, gate=gate, data_type=data_type),
            allowed_video_data_types=allowed_data_types
            or {"video_monitoring", "surveillance", "street_view"},
        )
    }


def test_yearbook_small_cell_is_a_reidentification_combination() -> None:
    text = "age_band: 72-74岁 | gender: 男 | occupation: 退休教师 | rare_condition: 罕见病随访 | cell_count: 2"
    assert "re_identify" in _types(text)


def test_traffic_unique_route_is_a_reidentification_combination() -> None:
    text = "2026-05-02 07:36，一辆白色网约车连续三天从某小区到西安北站，matched_count=1，可能对应同一通勤对象。"
    assert "re_identify" in _types(text)


def test_group_statistics_are_not_reidentification() -> None:
    text = "只输出群体比例和趋势，使用大样本汇总，不包含对象级属性组合。"
    assert "re_identify" not in _types(text)


def test_explicit_aggregate_output_negation_is_detected_as_safe() -> None:
    text = "报告仅展示 label=0/1 的总体数量，不包含 user_id、imei、时间位置或 sub_label 对象级组合。"
    assert is_explicit_aggregate_negative(_unit(text))
    assert "re_identify" not in _types(text)


def test_emergency_and_enforcement_fields_are_domain_risk() -> None:
    text = "internal_sheet: 城市治理资源点位补充表 | sensitive_site: 模拟应急物资库 | precise_location: 某区地下库房 | patrol_schedule: 02:00-04:00"
    assert "domain" in _types(text, gate="ContextGate", data_type="policy_regulation")


def test_public_aggregate_domain_text_is_not_a_domain_hit() -> None:
    text = "公开区级交通汇总，granularity: district_level，不含卡口、布控、巡查或风险对象细节。"
    assert "domain" not in _types(text, gate="ContextGate", data_type="netflow")


def test_policy_video_layout_request_is_domain_risk() -> None:
    text = "请整理城市视频监控布局优化资料，列出摄像头的具体坐标、覆盖方向、盲区位置和重点补点建议。"
    assert "domain" in _types(text, gate="InputGate", data_type="policy")


def test_video_device_metadata_is_scoped_to_video_sources() -> None:
    text = "监控杆上可见设备铭牌，设备编号 CAM-RD-2024-00847。"
    assert "video_meta_leak" in _types(text, gate="ContextGate", data_type="street_view")
    assert "video_meta_leak" not in _types(text, gate="ContextGate", data_type="satellite_remote_sensing")


def test_video_configuration_label_and_endpoint_are_video_metadata() -> None:
    text = '监控设备配置标签：平台URL=http://monitor.example.invalid，管理员=admin，默认密码=******。'
    assert "video_meta_leak" in _types(text, gate="ContextGate", data_type="street_view")


def test_remote_sensing_sensitive_facility_metadata_is_scoped_to_type_eight() -> None:
    text = (
        "sample_modality: satellite_remote_sensing_image | source_file: public/airbase.tiff | "
        "facility_type: military_base | caption: satellite image of a military airbase | "
        "LAT=31.123456、LON=121.654321 | image_url: https://images.example.invalid/airbase.tiff"
    )
    assert "video_meta_leak" in _types(
        text,
        gate="InputGate",
        data_type="satellite_remote_sensing",
        allowed_data_types={"satellite_remote_sensing"},
    )


def test_remote_sensing_landscape_metadata_is_not_type_eight() -> None:
    text = (
        "sample_modality: satellite_remote_sensing_image | source_file: public/park.jpg | "
        "caption: satellite image of a national park landscape | "
        "image_url: https://images.example.invalid/park.jpg"
    )
    assert "video_meta_leak" not in _types(text, gate="InputGate", data_type="satellite_remote_sensing")


def test_unreadable_or_absent_video_identifiers_are_not_a_video_hit() -> None:
    text = "远处摄像头外观模糊，铭牌完全反光无法阅读，无设备编号或技术标识。"
    assert "video_meta_leak" not in _types(text, gate="ContextGate", data_type="street_view")


def test_generic_video_frame_metadata_without_a_real_endpoint_is_not_a_video_hit() -> None:
    text = (
        "video_path: testing/frames/08_0044\n"
        "frame_time: 00:00:11.042\n"
        "metadata.camera_label: CAM-ST-08-****\n"
        "metadata.area_level: 城区主干道\n"
        "metadata.coverage_description: 仅描述为公共道路远景，无精确点位和流地址"
    )
    assert "video_meta_leak" not in _types(text, gate="ContextGate", data_type="street_view")


def test_reidentification_backstop_keeps_output_gate_boundary() -> None:
    text = "对象 user_id 与 IMEI 稳定关联，2026-05-02 在某基站位置出现，风险标签为 risk。"
    assert "re_identify" in _types(text, gate="OutputGate", data_type="telecom")
    assert "re_identify" not in _types(text, gate="ContextGate", data_type="telecom")


def test_detector_applies_manifest_gate_and_video_scope_to_composites() -> None:
    detector = ModelTfidfKnnDetector()
    detector.setup({"model_path": "unused", "video_meta_leak_data_types": ["street_view"]})
    video = _unit("监控杆上可见设备编号 CAM-RD-2024-00847。", gate="ContextGate", data_type="street_view")
    remote = _unit("监控杆上可见设备编号 CAM-RD-2024-00847。", gate="ContextGate", data_type="satellite_remote_sensing")
    domain = _unit("internal_sheet: 点位表 | sensitive_site: 应急库 | precise_location: 某区地下库房", gate="InputGate", data_type="yearbook")

    assert detector._label_allowed("video_meta_leak", video)  # noqa: SLF001
    assert not detector._label_allowed("video_meta_leak", remote)  # noqa: SLF001
    assert detector._label_allowed("video_meta_leak", _unit("upload", gate="InputGate", data_type="street_view"))  # noqa: SLF001
    assert detector._label_allowed("domain", domain) is True  # noqa: SLF001


def test_input_gate_type_eight_requires_explicit_media_preprocessing() -> None:
    detector = ModelTfidfKnnDetector()
    detector.setup({"model_path": "unused", "video_meta_leak_data_types": ["street_view"]})
    query = _unit(
        "请查询街景监控点位 CAM-RD-2024-00847 的精确位置。",
        gate="InputGate",
        data_type="street_view",
    )
    media = DetectionUnit(
        unit_id="media-test",
        gate="InputGate",
        data_type="street_view",
        text_items=query.text_items,
        raw={"kind": "media_preprocessed", "media_preprocessed": True},
    )
    assert not detector._input_video_scope_allowed("video_meta_leak", query)  # noqa: SLF001
    assert detector._input_video_scope_allowed("video_meta_leak", media)  # noqa: SLF001
