"""Normalizers: source payloads -> detection units.

The ``SkillResult`` normalizer matters most. Its dotted field paths are what the
model detector turns into ``features`` keys, so the flattening rule here is
directly load-bearing for detection quality (plan §5.1.5 / risk 4).
"""

from __future__ import annotations

import json

import pytest
from langchain_core.messages import AIMessage

from deerflow.compliance.normalizers.base import flatten_fields, make_text_items, stringify
from deerflow.compliance.normalizers.llm_output import LlmOutputNormalizer, extract_text
from deerflow.compliance.normalizers.skill_result import (
    SkillResultNormalizer,
    evidence_scores,
    try_parse_skill_result,
)
from deerflow.compliance.normalizers.user_input import UploadedFileNormalizer, UserInputNormalizer


def _skill_result(**overrides) -> dict:
    payload = {
        "schema_version": "1.0",
        "request_id": "req-1",
        "skill_name": "video-analysis",
        "status": "success",
        "result": {
            "display_text": "Found 2 cameras.",
            "summary": {"title": "t", "overview": "o"},
            "findings": [],
            "evidence": [
                {
                    "evidence_id": "e-001",
                    "type": "table",
                    "title": "Camera list",
                    "description": "Cameras near the intersection",
                    "data": {"rows": [{"camera_id": "HK-0431", "lat": 31.19}]},
                    "metadata": {"camera_id": "HK-0431", "install_address": "虹桥路 1234 号", "score": 0.9},
                }
            ],
            "artifacts": [],
        },
        "diagnostics": {"warnings": []},
        "errors": [],
    }
    payload.update(overrides)
    return payload


# ── SkillResult detection ───────────────────────────────────────────────────


def test_parses_a_skill_result_dict() -> None:
    assert try_parse_skill_result(_skill_result()) is not None


def test_parses_a_skill_result_json_string() -> None:
    assert try_parse_skill_result(json.dumps(_skill_result())) is not None


def test_unwraps_the_invoke_skill_envelope() -> None:
    """`invoke_skill` returns {"status": "wrapped", "skill_result": {...}}."""
    wrapped = {"status": "wrapped", "skill_result": _skill_result()}
    parsed = try_parse_skill_result(json.dumps(wrapped))
    assert parsed is not None
    assert parsed["skill_name"] == "video-analysis"


def test_parses_from_langchain_content_blocks() -> None:
    blocks = [{"type": "text", "text": json.dumps(_skill_result())}]
    assert try_parse_skill_result(blocks) is not None


@pytest.mark.parametrize(
    "content",
    ["just some text", "", "{not json", json.dumps({"foo": "bar"}), json.dumps({"result": "not a mapping"}), None, 42, []],
    ids=["plain", "empty", "broken", "wrong_shape", "result_not_mapping", "none", "int", "empty_list"],
)
def test_non_skill_result_content_is_left_alone(content: object) -> None:
    """Tool messages carry arbitrary text; only the structured envelope is touched."""
    assert try_parse_skill_result(content) is None


# ── SkillResult -> units (guide §1.2.3) ─────────────────────────────────────


def test_each_evidence_entry_becomes_one_unit() -> None:
    payload = _skill_result()
    payload["result"]["evidence"].append(
        {"evidence_id": "e-002", "type": "text", "title": "Second", "data": "another camera"}
    )
    units = SkillResultNormalizer().to_units(payload, gate="ContextGate")
    evidence_units = [u for u in units if u.raw.get("kind") == "skill_evidence"]
    assert len(evidence_units) == 2


def test_description_and_data_go_into_text_items() -> None:
    units = SkillResultNormalizer().to_units(_skill_result(), gate="ContextGate")
    texts = [t.text for u in units for t in u.text_items]
    assert any("Cameras near the intersection" in t for t in texts)


def test_metadata_is_flattened_into_dotted_field_paths() -> None:
    """These become the model's `features` keys — the rule is load-bearing."""
    units = SkillResultNormalizer().to_units(_skill_result(), gate="ContextGate")
    paths = {f.path for u in units for f in u.field_items}
    assert "metadata.camera_id" in paths
    assert "metadata.install_address" in paths


def test_nested_data_is_flattened_with_indices() -> None:
    units = SkillResultNormalizer().to_units(_skill_result(), gate="ContextGate")
    paths = {f.path for u in units for f in u.field_items}
    assert "data.rows[0].camera_id" in paths


def test_display_text_is_checked_too() -> None:
    """It is what reaches the model when a skill returns no structured evidence."""
    payload = _skill_result()
    payload["result"]["evidence"] = []
    units = SkillResultNormalizer().to_units(payload, gate="ContextGate")
    assert len(units) == 1
    assert units[0].raw["kind"] == "skill_display_text"
    assert "Found 2 cameras" in units[0].text_items[0].text


def test_units_carry_the_requested_gate() -> None:
    units = SkillResultNormalizer().to_units(_skill_result(), gate="ContextGate")
    assert all(unit.gate == "ContextGate" for unit in units)


def test_evidence_id_is_preserved_as_the_source() -> None:
    """Traceability: a hit must be attributable to a specific evidence item."""
    units = SkillResultNormalizer().to_units(_skill_result(), gate="ContextGate")
    evidence_unit = next(u for u in units if u.raw.get("kind") == "skill_evidence")
    assert evidence_unit.raw["evidence_id"] == "e-001"
    assert all(item.source == "e-001" for item in evidence_unit.text_items)


def test_empty_evidence_entries_are_skipped() -> None:
    payload = _skill_result()
    payload["result"]["evidence"] = [{"evidence_id": "e-empty", "type": "text", "title": ""}]
    units = SkillResultNormalizer().to_units(payload, gate="ContextGate")
    assert all(u.raw.get("evidence_id") != "e-empty" for u in units)


def test_malformed_payload_yields_no_units() -> None:
    assert SkillResultNormalizer().to_units({"result": "nope"}, gate="ContextGate") == ()
    assert SkillResultNormalizer().to_units({}, gate="ContextGate") == ()


def test_evidence_scores_are_extracted_for_budget_prioritization() -> None:
    scores = evidence_scores(_skill_result())
    assert scores == {"e-001": 0.9}


def test_evidence_without_a_score_is_omitted_from_the_ranking() -> None:
    payload = _skill_result()
    del payload["result"]["evidence"][0]["metadata"]["score"]
    assert evidence_scores(payload) == {}


# ── llm_output ──────────────────────────────────────────────────────────────


def test_plain_string_content_is_extracted() -> None:
    assert extract_text("hello") == "hello"


def test_content_blocks_are_concatenated() -> None:
    blocks = [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]
    assert extract_text(blocks) == "ab"


def test_non_text_blocks_are_skipped() -> None:
    """Only what the user actually sees needs retracting."""
    blocks = [{"type": "thinking", "thinking": "secret"}, {"type": "text", "text": "visible"}]
    assert extract_text(blocks) == "visible"


def test_ai_message_becomes_one_unit() -> None:
    units = LlmOutputNormalizer().to_units(AIMessage(content="an answer", id="m-1"), gate="OutputGate")
    assert len(units) == 1
    assert units[0].unit_id == "output:m-1"
    assert units[0].gate == "OutputGate"
    assert units[0].text_items[0].text == "an answer"


def test_blank_output_yields_no_units() -> None:
    assert LlmOutputNormalizer().to_units(AIMessage(content="   ", id="m"), gate="OutputGate") == ()


def test_raw_string_is_accepted_for_incremental_scanning() -> None:
    units = LlmOutputNormalizer().to_units("partial answer", gate="OutputGate", message_id="m-1")
    assert units[0].text_items[0].text == "partial answer"


# ── user_input ──────────────────────────────────────────────────────────────


def test_query_becomes_one_unit() -> None:
    units = UserInputNormalizer().to_units("show me the cameras", gate="InputGate", thread_id="t-1")
    assert len(units) == 1
    assert units[0].raw["kind"] == "user_query"
    assert units[0].text_items[0].source == "query"


def test_blank_query_yields_no_units() -> None:
    assert UserInputNormalizer().to_units("  ", gate="InputGate") == ()


def test_text_uploads_are_read(tmp_path) -> None:
    upload = tmp_path / "data.csv"
    upload.write_text("phone\n13800138000\n", encoding="utf-8")
    units = UploadedFileNormalizer().to_units([str(upload)], gate="InputGate", thread_id="t-1")
    assert len(units) == 1
    assert "13800138000" in units[0].text_items[0].text


def test_binary_uploads_are_skipped(tmp_path) -> None:
    """Binaries need a dedicated detector, not inline text scanning."""
    upload = tmp_path / "image.png"
    upload.write_bytes(b"\x89PNG\r\n\x1a\n")
    assert UploadedFileNormalizer().to_units([str(upload)], gate="InputGate") == ()


def test_missing_upload_is_skipped(tmp_path) -> None:
    assert UploadedFileNormalizer().to_units([str(tmp_path / "gone.txt")], gate="InputGate") == ()


def test_large_upload_is_truncated_and_flagged(tmp_path) -> None:
    """Truncation is reported so the caller knows the scan was partial."""
    from deerflow.compliance.normalizers.user_input import MAX_UPLOAD_BYTES

    upload = tmp_path / "big.txt"
    upload.write_text("x" * (MAX_UPLOAD_BYTES + 1000), encoding="utf-8")
    units = UploadedFileNormalizer().to_units([str(upload)], gate="InputGate")
    assert units[0].raw["truncated"] is True


# ── base helpers ────────────────────────────────────────────────────────────


def test_flatten_fields_skips_empty_values() -> None:
    items = flatten_fields({"a": "x", "b": None, "c": ""}, prefix="m", source="s")
    assert {item.path for item in items} == {"m.a"}


def test_flatten_fields_respects_a_depth_limit() -> None:
    """Guards against a pathological structure blowing the latency budget."""
    deep: dict = {"level": {}}
    node = deep["level"]
    for _ in range(20):
        node["level"] = {}
        node = node["level"]
    node["value"] = "bottom"
    items = flatten_fields(deep, prefix="root", source="s", max_depth=4)
    assert all(item.path.count(".") <= 5 for item in items)


def test_stringify_renders_structures_as_json() -> None:
    assert stringify({"b": 1, "a": 2}) == '{"a": 2, "b": 1}'


def test_stringify_truncates_oversized_values() -> None:
    assert len(stringify("x" * 50_000, limit=100)) == 100


def test_make_text_items_skips_blanks() -> None:
    assert make_text_items([("s", ""), ("s", "   "), ("s", "real")]) == make_text_items([("s", "real")])
