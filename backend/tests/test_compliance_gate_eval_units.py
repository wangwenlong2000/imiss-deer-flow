"""Regression tests for the annotated-sample to production-unit adapter."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scripts.run_compliance_gate_eval import sample_to_unit  # noqa: E402
from scripts.run_compliance_unified_ten_type_eval import (  # noqa: E402
    build_fp_fn_manifest,
    display_path,
    resolve_path,
    score_prediction_rows,
)


def test_structured_features_become_field_items_without_text_duplication() -> None:
    unit = sample_to_unit(
        {
            "data_id": "sample-1",
            "data_type": "telecom",
            "content_text": "must not be copied into the text fallback",
            "features": {
                "device": {"imei": "867530912345678", "network": {"cell": "C-01"}},
            },
        },
        "InputGate",
    )

    assert unit.data_type == "phone_network"
    assert unit.text_items == ()
    assert [(item.path, item.value) for item in unit.field_items] == [
        ("device.imei", "867530912345678"),
        ("device.network.cell", "C-01"),
    ]


def test_unstructured_sample_keeps_content_as_text_fallback() -> None:
    unit = sample_to_unit(
        {"sample_id": "sample-2", "data_type": "policy", "content_text": "公开政策通知"},
        "ContextGate",
    )

    assert unit.data_type == "policy_regulation"
    assert unit.field_items == ()
    assert [(item.text, item.source) for item in unit.text_items] == [("公开政策通知", "content")]


def test_unified_remote_sensing_annotation_is_marked_as_preprocessed_media() -> None:
    from scripts.run_compliance_unified_ten_type_eval import unit_for

    unit = unit_for(
        {
            "data_id": "remote-1",
            "data_type": "satellite_remote_sensing",
            "content": {"source_file": "image.tiff", "captions": ["LAT=20.1000,LON=100.1000"]},
        },
        "InputGate",
    )
    assert unit.raw["media_preprocessed"] is True
    assert unit.raw["kind"] == "media_preprocessed"


def test_all_legacy_data_type_aliases_are_canonicalized() -> None:
    aliases = {
        "telecom": "phone_network",
        "netflow": "network_flow",
        "policy": "policy_regulation",
        "yearbook": "statistical_yearbook",
        "surveillance": "video_monitoring",
    }

    for source, expected in aliases.items():
        unit = sample_to_unit({"data_type": source, "features": {"value": "x"}}, "OutputGate")
        assert unit.data_type == expected


def test_unified_eval_resolves_documented_repo_relative_paths() -> None:
    input_path = resolve_path(Path("合规检测/合规评测全部十类违规类型数据"))
    assert input_path.is_dir()
    assert display_path(input_path) == "合规检测/合规评测全部十类违规类型数据"


def test_unified_eval_emits_fp_fn_sample_ids() -> None:
    manifest = build_fp_fn_manifest(
        [
            {
                "sample_id": "fn-1",
                "source": "source.jsonl",
                "source_row": 2,
                "data_type": "statistical_yearbook",
                "evaluated_gate": "InputGate",
                "annotation_gate": "missing",
                "gold": "confidential",
                "predicted": "none",
            },
            {
                "sample_id": "fp-1",
                "source": "source.jsonl",
                "source_row": 3,
                "data_type": "network_flow",
                "evaluated_gate": "InputGate",
                "annotation_gate": "InputGate",
                "gold": "none",
                "predicted": "confidential",
                "predicted_types": ["confidential"],
            },
        ]
    )
    assert manifest["confidential"]["false_negative"][0]["sample_id"] == "fn-1"
    assert manifest["confidential"]["false_positive"][0]["sample_id"] == "fp-1"


def test_unified_eval_keeps_later_co_hits_in_type_metrics() -> None:
    report = score_prediction_rows(
        [
            {
                "gold": "confidential",
                "predicted": "struct_id",
                "predicted_types": ["struct_id", "confidential"],
            },
        ]
    )
    assert report["per_violation_type"]["confidential"]["tp"] == 1
    assert report["per_violation_type"]["confidential"]["fn"] == 0
