"""schema.py 单元测试 —— task.md §2 / §4.4。"""

from __future__ import annotations

import pathlib
import sys
import unittest

# 让 retrieval_protocol 包可被导入（tests 目录的上两级即 skills/_shared）。
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import schema  # noqa: E402


class TestDataTypes(unittest.TestCase):
    def test_covers_ten_city_data_types(self):
        # task.md §1：协议覆盖 10 类城市数据。
        self.assertEqual(len(schema.DATA_TYPES), 10)

    def test_registered_values_match_task_md_section_2(self):
        expected = {
            "spatiotemporal_trajectory", "traffic_flow", "gazetteer", "netflow", "telecom",
            "code", "policy", "streetview", "remote_sensing", "surveillance",
        }
        self.assertEqual(set(schema.DATA_TYPES), expected)

    def test_groups_partition_all_data_types(self):
        # 每个 data_type 恰好属于一个大类。
        flattened = [dt for members in schema.DATA_TYPE_GROUPS.values() for dt in members]
        self.assertEqual(len(flattened), len(set(flattened)))
        self.assertEqual(set(flattened), set(schema.DATA_TYPES))

    def test_video_group_is_a_tuple_not_a_string(self):
        # 防御单元素元组漏写逗号的经典坑。
        self.assertEqual(schema.DATA_TYPE_GROUPS["video"], ("surveillance",))

    def test_data_type_group_lookup(self):
        self.assertEqual(schema.data_type_group("netflow"), "structured")
        self.assertEqual(schema.data_type_group("policy"), "text")
        self.assertEqual(schema.data_type_group("surveillance"), "video")
        self.assertIsNone(schema.data_type_group("not_a_type"))

    def test_is_registered_data_type(self):
        self.assertTrue(schema.is_registered_data_type("traffic_flow"))
        self.assertFalse(schema.is_registered_data_type("traffic"))


class TestFeaturesRegistry(unittest.TestCase):
    def test_every_data_type_has_features_entry(self):
        self.assertEqual(set(schema.FEATURES_REGISTRY), set(schema.DATA_TYPES))

    def test_netflow_features_match_task_md_section_4_4(self):
        self.assertEqual(
            schema.FEATURES_REGISTRY["netflow"],
            ("src_ip", "dst_ip", "protocol", "bytes", "packets", "flow_duration", "ja3_hash", "anomaly_flag"),
        )

    def test_spatiotemporal_features_match_task_md_section_4_4(self):
        self.assertEqual(
            schema.FEATURES_REGISTRY["spatiotemporal_trajectory"],
            ("checkin_count", "unique_users", "top_categories", "wow_change_pct", "anomaly_flag"),
        )


class TestEnums(unittest.TestCase):
    def test_time_range_modes(self):
        self.assertEqual(schema.TIME_RANGE_MODES, frozenset({"absolute", "relative"}))

    def test_retrieval_strategies(self):
        self.assertEqual(schema.RETRIEVAL_STRATEGIES, frozenset({"bm25", "vector", "hybrid"}))

    def test_run_modes(self):
        self.assertEqual(schema.RUN_MODES, frozenset({"auto", "es", "local", "demo"}))

    def test_privacy_enums(self):
        self.assertEqual(
            schema.SENSITIVITY_LEVELS,
            frozenset({"open", "aggregated_safe", "pii_masked", "restricted"}),
        )
        self.assertEqual(schema.ACCESS_POLICIES, frozenset({"open", "restricted", "internal_only"}))
        # 敏感档位是 SENSITIVITY_LEVELS 的子集。
        self.assertTrue(schema.SENSITIVE_LEVELS.issubset(schema.SENSITIVITY_LEVELS))

    def test_schema_version(self):
        # v1.1 增补 Envelope.budget 与 errors[] 标准化错误码。
        self.assertEqual(schema.SCHEMA_VERSION, "1.1")

    def test_evidence_type_retrieval(self):
        self.assertEqual(schema.EVIDENCE_TYPE_RETRIEVAL, "retrieval")


if __name__ == "__main__":
    unittest.main()
