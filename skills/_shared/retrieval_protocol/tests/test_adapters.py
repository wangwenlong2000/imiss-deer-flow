"""adapters.py 单元测试 —— task.md §6 与四个已落地实现的兼容映射。

每个适配器的核心断言：用 §6 描述的原生命中形状喂入后，输出必须是**通过 §7 校验**
的 evidence wrapper。"""

from __future__ import annotations

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import (  # noqa: E402
    adapt_citybench_hit,
    adapt_citybench_result,
    adapt_network_traffic_hit,
    adapt_network_traffic_result,
    adapt_policy_hit,
    adapt_policy_result,
    adapt_road_traffic_hit,
    adapt_road_traffic_result,
    build_evidence_unit,
    build_network_traffic_skill_result,
    validate_evidence,
    validate_skill_result,
)


class TestCitybenchAdapter(unittest.TestCase):
    """task.md §6.1：source 已是合规 evidence_unit，零改动直通。"""

    def _native_hit(self):
        unit = build_evidence_unit(
            evidence_id="traj_ev_20120601_0700_beijing_wx4g0",
            data_type="spatiotemporal_trajectory",
            text="2012年6月1日早高峰，Beijing(geohash:wx4g0)签到944次。",
            source_id="citybench_checkins_beijing",
            geo_scope={"city": "Beijing", "geohash": "wx4g0"},
            features={"checkin_count": 944, "anomaly_flag": True},
        )
        return {"id": "traj_ev_20120601_0700_beijing_wx4g0", "rrf_score": 0.92, "source": unit}

    def test_output_passes_validation(self):
        self.assertEqual(validate_evidence(adapt_citybench_hit(self._native_hit())), [])

    def test_id_score_source_mapping(self):
        native = self._native_hit()
        wrapper = adapt_citybench_hit(native, rank=1)
        self.assertEqual(wrapper["evidence_ref"], "traj_ev_20120601_0700_beijing_wx4g0")
        self.assertEqual(wrapper["score"], 0.92)
        self.assertEqual(wrapper["rank"], 1)
        # source 整体直通为 payload，零改动（同一对象）。
        self.assertIs(wrapper["payload"], native["source"])
        self.assertEqual(wrapper["payload"]["data_type"], "spatiotemporal_trajectory")

    def test_result_list_assigns_ranks(self):
        wrappers = adapt_citybench_result([self._native_hit(), self._native_hit()])
        self.assertEqual([w["rank"] for w in wrappers], [1, 2])


class TestNetworkTrafficAdapter(unittest.TestCase):
    """task.md §6.2：薄映射，相对时间。"""

    def _native_hit(self, *, scan_like=True):
        return {
            "score": 0.0317,
            "doc_id": "virut__flow_summary__abc123",
            "doc_type": "flow_summary",
            "title": "TCP flow 10.0.0.5:443 -> 8.8.8.8:53",
            "summary": "TCP session 10.0.0.5 to 8.8.8.8, short connection, scan-like.",
            "dataset_name": "Virut",
            "source_file": "processed/Virut/Virut.flow.csv",
            "keywords": ["tcp", "scan"],
            "metadata": {
                "protocol": "TCP",
                "src_ip": "10.0.0.5",
                "dst_ip": "8.8.8.8",
                "dst_port": 53,
                "time_bucket": "t+3600s",
                "risk_level": "high",
                "is_scan_like": scan_like,
                "state_bucket": "SYN_ONLY",
            },
        }

    def test_output_passes_validation(self):
        self.assertEqual(validate_evidence(adapt_network_traffic_hit(self._native_hit())), [])

    def test_core_field_mapping(self):
        wrapper = adapt_network_traffic_hit(self._native_hit(), rank=1)
        payload = wrapper["payload"]
        self.assertEqual(payload["evidence_id"], "virut__flow_summary__abc123")
        self.assertEqual(payload["data_type"], "netflow")
        self.assertEqual(payload["meta"]["source_id"], "Virut")
        self.assertEqual(payload["meta"]["source_path"], "processed/Virut/Virut.flow.csv")
        self.assertEqual(payload["text"], self._native_hit()["summary"])

    def test_time_bucket_becomes_relative_time_range(self):
        # task.md §6.2 / §3.3：t+3600s 还原为相对时间，禁止解释成真实日期。
        payload = adapt_network_traffic_hit(self._native_hit())["payload"]
        self.assertEqual(
            payload["meta"]["time_range"],
            {"mode": "relative", "start_offset_s": 3600, "end_offset_s": 7200},
        )

    def test_geo_scope_is_empty_object(self):
        # §6.2：网络流量无空间属性，但 geo_scope 必须是对象。
        payload = adapt_network_traffic_hit(self._native_hit())["payload"]
        self.assertEqual(payload["meta"]["geo_scope"], {})

    def test_anomaly_flag_derived_from_scan_like(self):
        on = adapt_network_traffic_hit(self._native_hit(scan_like=True))["payload"]
        self.assertTrue(on["meta"]["features"]["anomaly_flag"])

    def test_time_bucket_not_left_in_features(self):
        payload = adapt_network_traffic_hit(self._native_hit())["payload"]
        self.assertNotIn("time_bucket", payload["meta"]["features"])

    def test_hit_without_time_bucket_omits_time_range(self):
        hit = self._native_hit()
        del hit["metadata"]["time_bucket"]
        payload = adapt_network_traffic_hit(hit)["payload"]
        self.assertNotIn("time_range", payload["meta"])
        self.assertEqual(validate_evidence(adapt_network_traffic_hit(hit)), [])

    def test_result_list_assigns_ranks(self):
        wrappers = adapt_network_traffic_result([self._native_hit(), self._native_hit()])
        self.assertEqual([w["rank"] for w in wrappers], [1, 2])


class TestNetworkTrafficSkillResult(unittest.TestCase):
    """task.md §6.2：整份 rag_search 结果包成 SkillResult。"""

    def _result(self):
        return {
            "query": "Virut 数据集是否存在扫描行为",
            "index_name": "network-traffic-rag",
            "hit_count": 2,
            "retrieval_strategy": "hybrid-text-plus-vector",
            "embedding_model": "text-embedding-v3-large",
            "hits": [
                {
                    "score": 0.03, "doc_id": "doc_a", "doc_type": "anomaly_summary",
                    "summary": "scan-like burst from 10.0.0.5", "dataset_name": "Virut",
                    "source_file": "Virut.flow.csv",
                    "metadata": {"src_ip": "10.0.0.5", "is_scan_like": True, "time_bucket": "t+0s"},
                },
                {
                    "score": 0.02, "doc_id": "doc_b", "doc_type": "flow_summary",
                    "summary": "normal TLS session", "dataset_name": "Virut",
                    "source_file": "Virut.flow.csv",
                    "metadata": {"src_ip": "10.0.0.9", "is_scan_like": False, "risk_level": "low"},
                },
            ],
        }

    def test_skill_result_passes_validation(self):
        skill_result = build_network_traffic_skill_result(self._result())
        self.assertEqual(validate_skill_result(skill_result), [])

    def test_skill_result_envelope_fields(self):
        skill_result = build_network_traffic_skill_result(self._result())
        self.assertEqual(skill_result["skill_name"], "network-traffic-analysis")
        self.assertEqual(skill_result["scenario"], "netflow")
        self.assertEqual(skill_result["capability"], "evidence_search")
        self.assertEqual(len(skill_result["result"]["evidence"]), 2)

    def test_anomaly_count_in_summary(self):
        skill_result = build_network_traffic_skill_result(self._result())
        metrics = {m["name"]: m["value"] for m in skill_result["result"]["summary"]["key_metrics"]}
        self.assertEqual(metrics["result_count"], 2)
        self.assertEqual(metrics["anomaly_count"], 1)

    def test_empty_hits_still_valid(self):
        skill_result = build_network_traffic_skill_result({"query": "q", "hit_count": 0, "hits": []})
        self.assertEqual(validate_skill_result(skill_result), [])


class TestRoadTrafficAdapter(unittest.TestCase):
    """task.md §6.3：交通流量年报 RAG，data_type=gazetteer。"""

    def _native_hit(self, *, with_id=False):
        hit = {
            "section_path": "第三章 / 3.2 高峰时段交通流量",
            "pages": "45-47",
            "preview": "2024 年西安市早高峰主干道平均流量同比上升 12.3%。",
            "distance": 0.27,
            "rerank_score": 0.88,
        }
        if with_id:
            hit["doc_id"] = "xian2024_sec_3_2"
        return hit

    def test_output_passes_validation(self):
        self.assertEqual(validate_evidence(adapt_road_traffic_hit(self._native_hit())), [])

    def test_data_type_and_locator_mapping(self):
        payload = adapt_road_traffic_hit(self._native_hit())["payload"]
        self.assertEqual(payload["data_type"], "gazetteer")
        self.assertEqual(payload["text"], self._native_hit()["preview"])
        self.assertEqual(payload["meta"]["locator"]["section"], "第三章 / 3.2 高峰时段交通流量")
        self.assertEqual(payload["meta"]["locator"]["page"], "45-47")

    def test_distance_and_rerank_in_raw_scores(self):
        wrapper = adapt_road_traffic_hit(self._native_hit())
        self.assertEqual(wrapper["retrieval"]["raw_scores"], {"distance": 0.27, "rerank_score": 0.88})
        self.assertEqual(wrapper["score"], 0.88)

    def test_evidence_id_generated_when_no_doc_id(self):
        wrapper = adapt_road_traffic_hit(self._native_hit(with_id=False))
        self.assertTrue(wrapper["evidence_ref"].startswith("gazetteer_"))
        # 同一命中两次生成的 id 必须稳定一致。
        again = adapt_road_traffic_hit(self._native_hit(with_id=False))
        self.assertEqual(wrapper["evidence_ref"], again["evidence_ref"])

    def test_explicit_doc_id_is_respected(self):
        wrapper = adapt_road_traffic_hit(self._native_hit(with_id=True))
        self.assertEqual(wrapper["evidence_ref"], "xian2024_sec_3_2")

    def test_hit_with_only_distance_still_valid(self):
        hit = self._native_hit()
        del hit["rerank_score"]
        wrapper = adapt_road_traffic_hit(hit)
        self.assertNotIn("score", wrapper)
        self.assertEqual(validate_evidence(wrapper), [])

    def test_result_list_assigns_ranks(self):
        wrappers = adapt_road_traffic_result([self._native_hit(), self._native_hit(with_id=True)])
        self.assertEqual([w["rank"] for w in wrappers], [1, 2])


class TestPolicyAdapter(unittest.TestCase):
    """task.md §6.4：政策法规 RAG，data_type=policy。"""

    def _native_hit(self):
        return {
            "doc_id": "policy_2023_0481",
            "text": "自 2024 年 1 月 1 日起，本市核心区实行错峰限行管理措施。",
            "title": "关于优化中心城区交通管理的通告",
            "policy_number": "市政发〔2023〕481号",
            "effective_date": "2024-01-01",
            "issuer": "市交通运输管理局",
            "page": 2,
            "paragraph": "para-7",
            "bm25_score": 11.4,
            "vector_score": 0.81,
            "score": 0.0301,
        }

    def test_output_passes_validation(self):
        self.assertEqual(validate_evidence(adapt_policy_hit(self._native_hit())), [])

    def test_core_mapping(self):
        payload = adapt_policy_hit(self._native_hit())["payload"]
        self.assertEqual(payload["evidence_id"], "policy_2023_0481")
        self.assertEqual(payload["data_type"], "policy")
        self.assertEqual(payload["meta"]["locator"], {"page": 2, "paragraph_id": "para-7"})
        self.assertEqual(payload["meta"]["features"]["policy_number"], "市政发〔2023〕481号")
        self.assertEqual(payload["meta"]["features"]["issuer"], "市交通运输管理局")

    def test_bm25_and_vector_in_raw_scores(self):
        wrapper = adapt_policy_hit(self._native_hit())
        self.assertEqual(wrapper["retrieval"]["raw_scores"], {"bm25_score": 11.4, "vector_score": 0.81})

    def test_result_list_assigns_ranks(self):
        wrappers = adapt_policy_result([self._native_hit(), self._native_hit()])
        self.assertEqual([w["rank"] for w in wrappers], [1, 2])


if __name__ == "__main__":
    unittest.main()
