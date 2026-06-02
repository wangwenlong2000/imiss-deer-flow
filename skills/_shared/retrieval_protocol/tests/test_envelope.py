"""envelope.py 单元测试 —— task.md §3 统一检索输入。"""

from __future__ import annotations

import pathlib
import sys
import unittest
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import envelope  # noqa: E402


class TestRequestId(unittest.TestCase):
    def test_new_request_id_is_uuid_v4(self):
        rid = envelope.new_request_id()
        parsed = uuid.UUID(rid)
        self.assertEqual(parsed.version, 4)

    def test_new_request_id_is_unique(self):
        self.assertNotEqual(envelope.new_request_id(), envelope.new_request_id())


class TestTimeRange(unittest.TestCase):
    def test_absolute_time_range(self):
        tr = envelope.build_time_range_absolute(
            start="2024-03-01T00:00:00+08:00",
            end="2024-03-31T23:59:59+08:00",
            timezone="Asia/Shanghai",
        )
        self.assertEqual(tr["mode"], "absolute")
        self.assertEqual(tr["start"], "2024-03-01T00:00:00+08:00")
        self.assertEqual(tr["timezone"], "Asia/Shanghai")

    def test_relative_time_range_matches_task_md_example(self):
        # task.md §3.3 网络流量示例。
        tr = envelope.build_time_range_relative(start_offset_s=0, end_offset_s=3600)
        self.assertEqual(tr, {"mode": "relative", "start_offset_s": 0, "end_offset_s": 3600})

    def test_relative_time_range_coerces_to_int(self):
        tr = envelope.build_time_range_relative(start_offset_s=0.0, end_offset_s=3600.0)
        self.assertIsInstance(tr["start_offset_s"], int)
        self.assertIsInstance(tr["end_offset_s"], int)


class TestRetrievalParams(unittest.TestCase):
    def test_defaults(self):
        retr = envelope.build_retrieval_params()
        self.assertEqual(retr, {"strategy": "hybrid", "rerank": True, "score_threshold": 0.0})

    def test_rejects_unknown_strategy(self):
        with self.assertRaises(ValueError):
            envelope.build_retrieval_params(strategy="fuzzy")


class TestFilters(unittest.TestCase):
    def test_only_explicit_fields_are_kept(self):
        # 非时空数据不填 city/time_range/geohash —— 它们不应出现在结果里。
        filters = envelope.build_filters(where={"lang": "python"})
        self.assertEqual(filters, {"where": {"lang": "python"}})

    def test_spatiotemporal_filters(self):
        filters = envelope.build_filters(
            city="Shanghai",
            geohash="wtw3s",
            anomaly_only=True,
            where={"congestion_level": ["heavy", "severe"]},
        )
        self.assertEqual(filters["city"], "Shanghai")
        self.assertEqual(filters["geohash"], "wtw3s")
        self.assertIs(filters["anomaly_only"], True)
        self.assertNotIn("time_range", filters)


class TestInputEnvelope(unittest.TestCase):
    def test_matches_task_md_section_3_1_shape(self):
        env = envelope.build_input_envelope(
            skill_name="citybench-rag-search",
            scenario="spatiotemporal_trajectory",
            capability="evidence_search",
            query="上海早高峰陆家嘴交通流量异常",
            data_type="traffic_flow",
            top_k=10,
            mode="auto",
            retrieval=envelope.build_retrieval_params(),
            filters=envelope.build_filters(city="Shanghai", anomaly_only=True),
        )
        self.assertEqual(env["schema_version"], "1.1")
        self.assertEqual(env["skill_name"], "citybench-rag-search")
        self.assertEqual(env["scenario"], "spatiotemporal_trajectory")
        self.assertEqual(env["capability"], "evidence_search")
        # 检索专用内容只占用 input.parameters 与 input.filters。
        self.assertEqual(env["input"]["parameters"]["query"], "上海早高峰陆家嘴交通流量异常")
        self.assertEqual(env["input"]["parameters"]["data_type"], "traffic_flow")
        self.assertEqual(env["input"]["filters"]["city"], "Shanghai")
        self.assertEqual(env["input"]["data_sources"], [])
        self.assertEqual(env["input"]["context"], {})

    def test_request_id_auto_generated_when_absent(self):
        env = envelope.build_input_envelope(
            skill_name="s", scenario="netflow", capability="evidence_search",
            query="q", data_type="netflow",
        )
        self.assertEqual(uuid.UUID(env["request_id"]).version, 4)

    def test_request_id_respected_when_given(self):
        env = envelope.build_input_envelope(
            skill_name="s", scenario="netflow", capability="evidence_search",
            query="q", data_type="netflow", request_id="fixed-id",
        )
        self.assertEqual(env["request_id"], "fixed-id")

    def test_defaults_top_k_and_mode(self):
        env = envelope.build_input_envelope(
            skill_name="s", scenario="code", capability="evidence_search",
            query="q", data_type="code",
        )
        self.assertEqual(env["input"]["parameters"]["top_k"], 10)
        self.assertEqual(env["input"]["parameters"]["mode"], "auto")
        # 未传 retrieval 时不写该键。
        self.assertNotIn("retrieval", env["input"]["parameters"])

    def test_rejects_unregistered_data_type(self):
        with self.assertRaises(ValueError):
            envelope.build_input_envelope(
                skill_name="s", scenario="x", capability="evidence_search",
                query="q", data_type="weather",
            )

    def test_rejects_unknown_mode(self):
        with self.assertRaises(ValueError):
            envelope.build_input_envelope(
                skill_name="s", scenario="x", capability="evidence_search",
                query="q", data_type="code", mode="turbo",
            )

    def test_code_query_needs_no_spatial_filters(self):
        # task.md §3.4：代码片段无时空字段，不伪造 city/time。
        env = envelope.build_input_envelope(
            skill_name="code-search", scenario="code", capability="evidence_search",
            query="_load_landmarks 调用位置", data_type="code", top_k=20,
            filters=envelope.build_filters(where={"lang": "python", "ast_node_type": "function_call"}),
        )
        self.assertEqual(set(env["input"]["filters"]), {"where"})


if __name__ == "__main__":
    unittest.main()
