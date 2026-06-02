"""validate.py 单元测试 —— task.md §7 校验规则的十条正反例。"""

from __future__ import annotations

import copy
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import (  # noqa: E402
    build_evidence_unit,
    build_retrieval_evidence,
    build_skill_result,
    build_summary,
    validate_evidence,
    validate_evidence_list,
    validate_input_envelope,
    validate_jsonl_lines,
    validate_skill_result,
    validate_time_range,
)


def _payload(evidence_id: str = "ev1") -> dict:
    return build_evidence_unit(
        evidence_id=evidence_id,
        data_type="netflow",
        text="bidirectional TLS session evidence summary",
        source_id="dataset_x",
        geo_scope={},
        features={"src_ip": "10.0.0.1", "anomaly_flag": True},
    )


def _wrapper(evidence_id: str = "ev1") -> dict:
    return build_retrieval_evidence(
        evidence_ref=evidence_id, rank=1, score=0.91,
        method="bm25_vector_rrf", payload=_payload(evidence_id),
    )


def _skill_result() -> dict:
    return build_skill_result(
        skill_name="network-traffic-analysis",
        scenario="netflow",
        capability="evidence_search",
        summary=build_summary(title="t", overview="o"),
        evidence=[_wrapper("ev1"), _wrapper("ev2")],
    )


class TestHappyPath(unittest.TestCase):
    """协议构造器产出的对象必须直接通过校验（构造器 ↔ 校验器闭环）。"""

    def test_builder_wrapper_is_valid(self):
        self.assertEqual(validate_evidence(_wrapper()), [])

    def test_builder_skill_result_is_valid(self):
        self.assertEqual(validate_skill_result(_skill_result()), [])

    def test_citybench_section_4_2_unit_is_valid(self):
        # task.md §4.2 的 citybench evidence_unit 直接包成 wrapper 后应合规。
        unit = build_evidence_unit(
            evidence_id="traj_ev_20120601_0700_beijing_wx4g0",
            data_type="spatiotemporal_trajectory",
            text="2012年6月1日早高峰，Beijing(geohash:wx4g0)区域，签到944次。",
            source_id="citybench_checkins_beijing",
            source_path="data_lake/Beijing_filtered_checkins.csv",
            time_range={"mode": "absolute", "start": "2012-06-01T07:00:00",
                        "end": "2012-06-01T09:00:00", "timezone": "Asia/Shanghai"},
            geo_scope={"city": "Beijing", "geohash": "wx4g0"},
            granularity="hourly_district",
            sensitivity_level="aggregated_safe",
            access_policy="open",
            locator={},
            features={"checkin_count": 944, "anomaly_flag": True},
        )
        wrapper = build_retrieval_evidence(evidence_ref="e-001", score=0.92, payload=unit)
        self.assertEqual(validate_evidence(wrapper), [])


class TestRule1Wrapper(unittest.TestCase):
    def test_type_must_be_retrieval(self):
        w = _wrapper()
        w["type"] = "analysis"
        self.assertTrue(any("[rule 1]" in e and "type" in e for e in validate_evidence(w)))

    def test_missing_score_and_raw_scores_fails(self):
        w = _wrapper()
        del w["score"]
        self.assertTrue(any("[rule 1]" in e for e in validate_evidence(w)))

    def test_raw_scores_alone_satisfies_rule_1(self):
        # 没有 score、但 retrieval.raw_scores 非空 —— 合规（task.md §7 规则 1 的「或」）。
        w = build_retrieval_evidence(
            evidence_ref="ev1", method="vector_rerank",
            raw_scores={"distance": 0.3}, payload=_payload(),
        )
        self.assertNotIn("score", w)
        self.assertEqual(validate_evidence(w), [])

    def test_missing_payload_fails(self):
        w = _wrapper()
        del w["payload"]
        self.assertTrue(any("[rule 1]" in e and "payload" in e for e in validate_evidence(w)))


class TestRule2Payload(unittest.TestCase):
    def test_each_required_field_is_checked(self):
        for field in ("evidence_id", "data_type", "text"):
            w = _wrapper()
            del w["payload"][field]
            self.assertTrue(
                any("[rule 2]" in e and field in e for e in validate_evidence(w)),
                msg=f"missing payload.{field} should raise rule 2",
            )

    def test_meta_source_id_required(self):
        w = _wrapper()
        del w["payload"]["meta"]["source_id"]
        self.assertTrue(any("[rule 2]" in e and "source_id" in e for e in validate_evidence(w)))

    def test_meta_features_required_and_object(self):
        w = _wrapper()
        w["payload"]["meta"]["features"] = ["not", "an", "object"]
        self.assertTrue(any("[rule 2]" in e and "features" in e for e in validate_evidence(w)))


class TestRule3Uniqueness(unittest.TestCase):
    def test_duplicate_evidence_id_in_list(self):
        errors = validate_evidence_list([_wrapper("dup"), _wrapper("dup")])
        self.assertTrue(any("[rule 3]" in e for e in errors))

    def test_duplicate_evidence_id_in_skill_result(self):
        sr = build_skill_result(
            skill_name="s", scenario="netflow", capability="evidence_search",
            summary=build_summary(title="t", overview="o"),
            evidence=[_wrapper("same"), _wrapper("same")],
        )
        self.assertTrue(any("[rule 3]" in e for e in validate_skill_result(sr)))

    def test_unique_ids_pass(self):
        self.assertEqual(validate_evidence_list([_wrapper("a"), _wrapper("b")]), [])


class TestRule4DataType(unittest.TestCase):
    def test_unregistered_data_type_rejected(self):
        w = _wrapper()
        w["payload"]["data_type"] = "weather_radar"
        self.assertTrue(any("[rule 4]" in e for e in validate_evidence(w)))

    def test_all_ten_registered_types_pass(self):
        from retrieval_protocol import DATA_TYPES

        for data_type in DATA_TYPES:
            unit = build_evidence_unit(
                evidence_id="e", data_type=data_type, text="t", source_id="s", features={},
            )
            w = build_retrieval_evidence(evidence_ref="e", score=0.5, payload=unit)
            self.assertEqual(validate_evidence(w), [], msg=f"{data_type} should validate")


class TestRule5GeoScope(unittest.TestCase):
    def test_empty_object_is_allowed(self):
        w = _wrapper()
        w["payload"]["meta"]["geo_scope"] = {}
        self.assertEqual(validate_evidence(w), [])

    def test_null_geo_scope_rejected(self):
        w = _wrapper()
        w["payload"]["meta"]["geo_scope"] = None
        self.assertTrue(any("[rule 5]" in e for e in validate_evidence(w)))

    def test_string_geo_scope_rejected(self):
        w = _wrapper()
        w["payload"]["meta"]["geo_scope"] = "Beijing"
        self.assertTrue(any("[rule 5]" in e for e in validate_evidence(w)))

    def test_absent_geo_scope_is_allowed(self):
        w = _wrapper()
        w["payload"]["meta"].pop("geo_scope", None)
        self.assertEqual(validate_evidence(w), [])


class TestRule6TimeRange(unittest.TestCase):
    def test_absolute_ok(self):
        self.assertEqual(
            validate_time_range(
                {"mode": "absolute", "start": "2024-03-01T00:00:00+08:00",
                 "end": "2024-03-31T23:59:59+08:00", "timezone": "Asia/Shanghai"},
                path="tr",
            ),
            [],
        )

    def test_relative_ok(self):
        self.assertEqual(
            validate_time_range({"mode": "relative", "start_offset_s": 0, "end_offset_s": 3600}, path="tr"),
            [],
        )

    def test_missing_mode_rejected(self):
        self.assertTrue(validate_time_range({"start": "x"}, path="tr"))

    def test_absolute_missing_timezone_rejected(self):
        errors = validate_time_range({"mode": "absolute", "start": "a", "end": "b"}, path="tr")
        self.assertTrue(any("timezone" in e for e in errors))

    def test_relative_missing_offsets_rejected(self):
        errors = validate_time_range({"mode": "relative"}, path="tr")
        self.assertEqual(len(errors), 2)

    def test_relative_offset_must_be_integer(self):
        errors = validate_time_range(
            {"mode": "relative", "start_offset_s": "0", "end_offset_s": 3600}, path="tr"
        )
        self.assertTrue(any("integer" in e for e in errors))

    def test_time_range_in_payload_is_validated(self):
        w = _wrapper()
        w["payload"]["meta"]["time_range"] = {"mode": "relative", "start_offset_s": 0}
        self.assertTrue(any("[rule 6]" in e for e in validate_evidence(w)))


class TestRule7Privacy(unittest.TestCase):
    def test_both_present_and_valid_pass(self):
        w = _wrapper()
        w["payload"]["meta"]["sensitivity_level"] = "pii_masked"
        w["payload"]["meta"]["access_policy"] = "restricted"
        self.assertEqual(validate_evidence(w), [])

    def test_sensitivity_without_access_rejected(self):
        w = _wrapper()
        w["payload"]["meta"]["sensitivity_level"] = "restricted"
        self.assertTrue(any("[rule 7]" in e and "access_policy" in e for e in validate_evidence(w)))

    def test_access_without_sensitivity_rejected(self):
        w = _wrapper()
        w["payload"]["meta"]["access_policy"] = "open"
        self.assertTrue(any("[rule 7]" in e and "sensitivity_level" in e for e in validate_evidence(w)))

    def test_invalid_enum_rejected(self):
        w = _wrapper()
        w["payload"]["meta"]["sensitivity_level"] = "top_secret"
        w["payload"]["meta"]["access_policy"] = "open"
        self.assertTrue(any("[rule 7]" in e for e in validate_evidence(w)))

    def test_neither_present_is_allowed(self):
        self.assertEqual(validate_evidence(_wrapper()), [])


class TestRule8Artifacts(unittest.TestCase):
    def test_artifacts_key_in_payload_rejected(self):
        w = _wrapper()
        w["payload"]["artifacts"] = [{"uri": "x.png"}]
        self.assertTrue(any("[rule 8]" in e for e in validate_evidence(w)))

    def test_attachments_key_in_meta_rejected(self):
        w = _wrapper()
        w["payload"]["meta"]["attachments"] = ["x.png"]
        self.assertTrue(any("[rule 8]" in e for e in validate_evidence(w)))

    def test_malformed_artifact_in_skill_result_rejected(self):
        sr = _skill_result()
        sr["result"]["artifacts"] = [{"title": "热力图"}]  # 缺 artifact_id / type / uri
        errors = validate_skill_result(sr)
        self.assertTrue(any("[rule 8]" in e for e in errors))

    def test_well_formed_artifact_passes(self):
        sr = _skill_result()
        sr["result"]["artifacts"] = [
            {"artifact_id": "a-001", "type": "file", "title": "热力图",
             "uri": "/mnt/user-data/outputs/heatmap.png", "media_type": "image/png"}
        ]
        self.assertEqual(validate_skill_result(sr), [])


class TestRule9SkillResultStructure(unittest.TestCase):
    def test_natural_language_string_rejected(self):
        errors = validate_skill_result("命中 10 条证据，其中 4 条异常。")
        self.assertTrue(any("[rule 9]" in e for e in errors))

    def test_missing_header_fields_rejected(self):
        sr = _skill_result()
        del sr["capability"]
        self.assertTrue(any("[rule 9]" in e and "capability" in e for e in validate_skill_result(sr)))

    def test_result_must_be_object(self):
        sr = _skill_result()
        sr["result"] = None
        self.assertTrue(any("[rule 9]" in e for e in validate_skill_result(sr)))

    def test_evidence_must_be_list(self):
        sr = _skill_result()
        sr["result"]["evidence"] = {"not": "a list"}
        self.assertTrue(any("[rule 9]" in e for e in validate_skill_result(sr)))


class TestJsonlExchange(unittest.TestCase):
    def test_valid_jsonl_text(self):
        text = "\n".join(json.dumps(w, ensure_ascii=False) for w in [_wrapper("a"), _wrapper("b")])
        self.assertEqual(validate_jsonl_lines(text), [])

    def test_jsonl_detects_duplicate_ids_across_lines(self):
        text = "\n".join(json.dumps(w, ensure_ascii=False) for w in [_wrapper("x"), _wrapper("x")])
        self.assertTrue(any("[rule 3]" in e for e in validate_jsonl_lines(text)))

    def test_jsonl_accepts_list_of_dicts(self):
        self.assertEqual(validate_jsonl_lines([_wrapper("a"), _wrapper("b")]), [])

    def test_jsonl_reports_malformed_line(self):
        self.assertTrue(any("[rule 9]" in e for e in validate_jsonl_lines("{not json}")))


class TestInputEnvelope(unittest.TestCase):
    def _envelope(self) -> dict:
        return {
            "schema_version": "1.0",
            "request_id": "uuid-v4",
            "skill_name": "citybench-rag-search",
            "scenario": "spatiotemporal_trajectory",
            "capability": "evidence_search",
            "input": {
                "data_sources": [],
                "parameters": {"query": "陆家嘴交通异常", "data_type": "traffic_flow",
                               "top_k": 10, "mode": "auto"},
                "filters": {},
                "context": {},
            },
        }

    def test_valid_envelope_passes(self):
        self.assertEqual(validate_input_envelope(self._envelope()), [])

    def test_missing_query_rejected(self):
        env = copy.deepcopy(self._envelope())
        del env["input"]["parameters"]["query"]
        self.assertTrue(any("query" in e for e in validate_input_envelope(env)))

    def test_unregistered_data_type_rejected(self):
        env = copy.deepcopy(self._envelope())
        env["input"]["parameters"]["data_type"] = "weather"
        self.assertTrue(validate_input_envelope(env))

    def test_relative_time_range_in_filters_validated(self):
        env = copy.deepcopy(self._envelope())
        env["input"]["filters"]["time_range"] = {"mode": "relative", "start_offset_s": 0, "end_offset_s": 3600}
        self.assertEqual(validate_input_envelope(env), [])

    def test_bad_mode_rejected(self):
        env = copy.deepcopy(self._envelope())
        env["input"]["parameters"]["mode"] = "turbo"
        self.assertTrue(validate_input_envelope(env))


if __name__ == "__main__":
    unittest.main()
