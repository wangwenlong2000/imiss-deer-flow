"""evidence.py 单元测试 —— task.md §4 统一结果证据。"""

from __future__ import annotations

import pathlib
import sys
import unittest
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import evidence  # noqa: E402


class TestGeoScope(unittest.TestCase):
    def test_only_explicit_fields_kept(self):
        scope = evidence.build_geo_scope(city="Beijing", geohash="wx4g0")
        self.assertEqual(scope, {"city": "Beijing", "geohash": "wx4g0"})

    def test_empty_when_nothing_given(self):
        self.assertEqual(evidence.build_geo_scope(), {})


class TestLocator(unittest.TestCase):
    def test_standard_and_extra_fields(self):
        loc = evidence.build_locator(file_path="a.py", line_start=10, line_end=20, symbol="_load")
        self.assertEqual(loc, {"file_path": "a.py", "line_start": 10, "line_end": 20, "symbol": "_load"})

    def test_drops_none_extras(self):
        self.assertEqual(evidence.build_locator(page=3, paragraph_id=None), {"page": 3})


class TestEvidenceUnit(unittest.TestCase):
    def test_reproduces_task_md_section_4_2_example(self):
        # task.md §4.2 的完整 evidence_unit 示例 —— 逐字段比对。
        unit = evidence.build_evidence_unit(
            evidence_id="traj_ev_20120601_0700_beijing_wx4g0",
            data_type="spatiotemporal_trajectory",
            text="2012年6月1日早高峰(7:00-9:00)，Beijing(geohash:wx4g0)区域，签到944次。",
            source_id="citybench_checkins_beijing",
            source_path="data_lake/Beijing_filtered_checkins.csv",
            time_range={
                "mode": "absolute",
                "start": "2012-06-01T07:00:00",
                "end": "2012-06-01T09:00:00",
                "timezone": "Asia/Shanghai",
            },
            geo_scope=evidence.build_geo_scope(
                city="Beijing", geohash="wx4g0", landmark="国贸-CBD核心区",
                district="朝阳区", lat=39.9087, lon=116.45, tags=["CBD", "通勤目的地"],
            ),
            granularity="hourly_district",
            sensitivity_level="aggregated_safe",
            access_policy="open",
            locator={},
            features={
                "checkin_count": 944,
                "unique_users": 563,
                "top_categories": ["便利店", "早餐店", "写字楼"],
                "wow_change_pct": 146.78,
                "anomaly_flag": True,
            },
        )
        self.assertEqual(unit["schema_version"], "1.1")
        self.assertEqual(unit["evidence_id"], "traj_ev_20120601_0700_beijing_wx4g0")
        self.assertEqual(unit["data_type"], "spatiotemporal_trajectory")
        self.assertEqual(unit["meta"]["source_id"], "citybench_checkins_beijing")
        self.assertEqual(unit["meta"]["geo_scope"]["landmark"], "国贸-CBD核心区")
        self.assertTrue(unit["meta"]["features"]["anomaly_flag"])

    def test_top_level_keys_and_order(self):
        unit = evidence.build_evidence_unit(
            evidence_id="e1", data_type="code", text="t", source_id="s", features={},
        )
        self.assertEqual(list(unit), ["schema_version", "evidence_id", "data_type", "text", "meta"])

    def test_features_is_always_present_object_and_last(self):
        # task.md §7 规则 2：meta.features 必须存在且为对象。
        unit = evidence.build_evidence_unit(
            evidence_id="e1", data_type="code", text="t", source_id="s", features={},
        )
        self.assertEqual(unit["meta"]["features"], {})
        self.assertEqual(list(unit["meta"])[-1], "features")

    def test_meta_omits_unset_optional_fields(self):
        unit = evidence.build_evidence_unit(
            evidence_id="e1", data_type="code", text="t", source_id="s", features={"lang": "py"},
        )
        self.assertEqual(list(unit["meta"]), ["source_id", "features"])

    def test_rejects_unregistered_data_type(self):
        with self.assertRaises(ValueError):
            evidence.build_evidence_unit(
                evidence_id="e1", data_type="weather", text="t", source_id="s", features={},
            )

    def test_rejects_bad_privacy_enums(self):
        with self.assertRaises(ValueError):
            evidence.build_evidence_unit(
                evidence_id="e1", data_type="netflow", text="t", source_id="s",
                features={}, sensitivity_level="secret",
            )
        with self.assertRaises(ValueError):
            evidence.build_evidence_unit(
                evidence_id="e1", data_type="netflow", text="t", source_id="s",
                features={}, access_policy="public",
            )

    def test_features_is_copied_not_aliased(self):
        src = {"a": 1}
        unit = evidence.build_evidence_unit(
            evidence_id="e1", data_type="code", text="t", source_id="s", features=src,
        )
        src["a"] = 999
        self.assertEqual(unit["meta"]["features"]["a"], 1)


class TestRetrievalEvidence(unittest.TestCase):
    def _payload(self):
        return evidence.build_evidence_unit(
            evidence_id="e1", data_type="code", text="t", source_id="s", features={},
        )

    def test_wrapper_shape_matches_task_md_section_4_1(self):
        wrapper = evidence.build_retrieval_evidence(
            evidence_ref="e-001", rank=1, score=0.92,
            method="bm25_vector_rrf", rrf_k=60,
            raw_scores={"bm25_rank": 1, "knn_rank": 2},
            matched_fields=["text", "meta.features.top_categories"],
            payload=self._payload(),
        )
        self.assertEqual(wrapper["evidence_ref"], "e-001")
        self.assertEqual(wrapper["type"], "retrieval")
        self.assertEqual(wrapper["rank"], 1)
        self.assertEqual(wrapper["score"], 0.92)
        self.assertEqual(wrapper["retrieval"]["method"], "bm25_vector_rrf")
        self.assertEqual(wrapper["retrieval"]["rrf_k"], 60)
        self.assertEqual(wrapper["retrieval"]["raw_scores"], {"bm25_rank": 1, "knn_rank": 2})
        self.assertIn("payload", wrapper)

    def test_explicit_retrieval_block_takes_precedence(self):
        block = evidence.build_retrieval_block(method="custom")
        wrapper = evidence.build_retrieval_evidence(
            evidence_ref="e1", score=0.5, retrieval=block, method="ignored", payload=self._payload(),
        )
        self.assertEqual(wrapper["retrieval"], {"method": "custom"})

    def test_no_retrieval_block_when_no_retrieval_args(self):
        wrapper = evidence.build_retrieval_evidence(
            evidence_ref="e1", score=0.5, payload=self._payload(),
        )
        self.assertNotIn("retrieval", wrapper)


class TestSkillResult(unittest.TestCase):
    def test_summary_and_key_metric(self):
        summary = evidence.build_summary(
            title="陆家嘴早高峰交通异常检索",
            overview="命中 10 条证据，其中 4 条标记异常。",
            key_metrics=[evidence.build_key_metric(name="result_count", value=10)],
        )
        self.assertEqual(summary["key_metrics"][0], {"name": "result_count", "value": 10})

    def test_artifact_shape(self):
        art = evidence.build_artifact(
            artifact_id="a-001", type="file", title="热力图",
            uri="/mnt/user-data/outputs/heatmap.png", media_type="image/png",
        )
        self.assertEqual(set(art), {"artifact_id", "type", "title", "uri", "media_type"})

    def test_skill_result_shape_matches_task_md_section_4_1(self):
        payload = evidence.build_evidence_unit(
            evidence_id="e1", data_type="traffic_flow", text="t", source_id="s", features={},
        )
        wrapper = evidence.build_retrieval_evidence(evidence_ref="e1", score=0.9, payload=payload)
        result = evidence.build_skill_result(
            skill_name="citybench-rag-search",
            scenario="spatiotemporal_trajectory",
            capability="evidence_search",
            summary=evidence.build_summary(title="t", overview="o"),
            evidence=[wrapper],
        )
        self.assertEqual(result["schema_version"], "1.1")
        self.assertEqual(result["status"], "success")
        self.assertEqual(uuid.UUID(result["request_id"]).version, 4)
        self.assertEqual(list(result["result"]), ["summary", "findings", "evidence", "artifacts"])
        self.assertEqual(result["result"]["evidence"], [wrapper])
        self.assertEqual(result["diagnostics"], {})
        self.assertEqual(result["errors"], [])

    def test_skill_result_respects_given_request_id_and_status(self):
        result = evidence.build_skill_result(
            skill_name="s", scenario="netflow", capability="evidence_search",
            summary=evidence.build_summary(title="t", overview="o"),
            evidence=[], request_id="fixed", status="partial",
        )
        self.assertEqual(result["request_id"], "fixed")
        self.assertEqual(result["status"], "partial")


if __name__ == "__main__":
    unittest.main()
