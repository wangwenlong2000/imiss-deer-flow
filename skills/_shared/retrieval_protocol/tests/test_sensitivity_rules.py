"""敏感度落档规则单测 —— spec 2026-05-27 §3."""
from __future__ import annotations

import unittest

from retrieval_protocol import sensitivity_rules as sr


class TestDefaultTable(unittest.TestCase):
    def test_data_types_present(self):
        expected = {
            "gazetteer":      ("aggregated_safe", "open"),
            "telecom":        ("pii_masked",      "internal_only"),
            "code":           ("pii_masked",      "internal_only"),
            "streetview":     ("pii_masked",      "internal_only"),
            "remote_sensing": ("aggregated_safe", "open"),
            "surveillance":   ("restricted",      "restricted"),
            "spatiotemporal_trajectory": ("pii_masked", "internal_only"),
        }
        self.assertEqual(sr.DEFAULT_SENSITIVITY, expected)

    def test_k_threshold(self):
        self.assertEqual(sr.K_THRESHOLD_AGGREGATED_SAFE, 10)


class TestStructIdDetector(unittest.TestCase):
    def test_plain_phone(self):
        unit = {"text": "客户手机号是 13800138000，注意保密"}
        self.assertTrue(sr._has_struct_id(unit))

    def test_plain_id_card(self):
        unit = {"text": "证件号 110101199001011234"}
        self.assertTrue(sr._has_struct_id(unit))

    def test_plain_imei(self):
        unit = {"text": "imei: 356938035643809"}
        self.assertTrue(sr._has_struct_id(unit))

    def test_plain_email(self):
        unit = {"text": "联系 zhang.san@example.com"}
        self.assertTrue(sr._has_struct_id(unit))

    def test_plain_ipv4(self):
        unit = {"text": "源 IP 192.168.10.42"}
        self.assertTrue(sr._has_struct_id(unit))

    def test_precise_geo(self):
        unit = {"text": "采集点 39.908823,116.397470"}
        self.assertTrue(sr._has_struct_id(unit))

    def test_hashed_id_not_flagged(self):
        unit = {"text": "用户 a3f5c9d2 出现 12 次"}
        self.assertFalse(sr._has_struct_id(unit))

    def test_plain_mac_colon(self):
        unit = {"text": "MAC 00:1A:2B:3C:4D:5E"}
        self.assertTrue(sr._has_struct_id(unit))

    def test_plain_mac_dash(self):
        unit = {"text": "MAC 00-1A-2B-3C-4D-5E"}
        self.assertTrue(sr._has_struct_id(unit))


class TestAggregatedKDetector(unittest.TestCase):
    def test_unique_users_ge_k(self):
        unit = {"features": {"unique_users": 25}}
        self.assertTrue(sr._has_aggregated_k_ge(unit, 10))

    def test_unique_contacts_lt_k(self):
        unit = {"features": {"unique_contacts": 8}}
        self.assertFalse(sr._has_aggregated_k_ge(unit, 10))

    def test_no_k_signal(self):
        unit = {"features": {"call_count": 100}}  # call_count 不是主体数
        self.assertFalse(sr._has_aggregated_k_ge(unit, 10))


class TestSecretTokenDetector(unittest.TestCase):
    def test_api_key(self):
        unit = {"text": 'API_KEY="sk_live_8eF2..."'}
        self.assertTrue(sr._has_secret_token(unit))

    def test_password(self):
        unit = {"text": "password = 'admin123'"}
        self.assertTrue(sr._has_secret_token(unit))

    def test_db_connection(self):
        unit = {"text": "postgresql://user:pwd@10.0.0.1:5432/db"}
        self.assertTrue(sr._has_secret_token(unit))

    def test_normal_code(self):
        unit = {"text": "def add(a, b): return a + b"}
        self.assertFalse(sr._has_secret_token(unit))


class TestObjectTimeGeoCombo(unittest.TestCase):
    def test_combo_present(self):
        unit = {"features": {"target_id": "veh_A", "timestamp": "2026-05-27T08:00:00", "cell_id": "cell_42"}}
        self.assertTrue(sr._has_object_time_geo_combo(unit))

    def test_missing_geo(self):
        unit = {"features": {"target_id": "veh_A", "timestamp": "2026-05-27T08:00:00"}}
        self.assertFalse(sr._has_object_time_geo_combo(unit))


class TestRiskLabel(unittest.TestCase):
    def test_purefraud_flag(self):
        unit = {"features": {"purefraud_flag": True}}
        self.assertTrue(sr._has_risk_label(unit))

    def test_no_label(self):
        unit = {"features": {"call_count": 10}}
        self.assertFalse(sr._has_risk_label(unit))


class TestStreamingLink(unittest.TestCase):
    def test_stream_url(self):
        unit = {"features": {"stream_url": "rtsp://10.0.0.1/stream1"}}
        self.assertTrue(sr._has_streaming_link(unit))

    def test_channel_id(self):
        unit = {"features": {"channel_id": "cam_42"}}
        self.assertTrue(sr._has_streaming_link(unit))

    def test_no_link(self):
        unit = {"features": {"objects": ["person"]}}
        self.assertFalse(sr._has_streaming_link(unit))


class TestUnmaskedFacePlate(unittest.TestCase):
    def test_unmasked_face(self):
        unit = {"features": {"objects": [{"label": "face", "masked": False}]}}
        self.assertTrue(sr._has_unmasked_face_plate(unit))

    def test_masked_face(self):
        unit = {"features": {"objects": [{"label": "face", "masked": True}]}}
        self.assertFalse(sr._has_unmasked_face_plate(unit))

    def test_license_plate_default_unmasked(self):
        unit = {"features": {"objects": [{"label": "license_plate"}]}}  # 未显式 masked → 视为未打码
        self.assertTrue(sr._has_unmasked_face_plate(unit))

    def test_face_as_string(self):
        unit = {"features": {"objects": ["face"]}}
        self.assertTrue(sr._has_unmasked_face_plate(unit))

    def test_license_plate_as_string(self):
        unit = {"features": {"objects": ["license_plate"]}}
        self.assertTrue(sr._has_unmasked_face_plate(unit))

    def test_unrelated_string_not_flagged(self):
        unit = {"features": {"objects": ["tree", "car"]}}
        self.assertFalse(sr._has_unmasked_face_plate(unit))


class TestMarkedPublic(unittest.TestCase):
    def test_explicit_public(self):
        unit = {"features": {"public": True}}
        self.assertTrue(sr._marked_public(unit))

    def test_source_kind_public(self):
        unit = {"features": {"source_kind": "public"}}
        self.assertTrue(sr._marked_public(unit))

    def test_not_public(self):
        unit = {"features": {"source_kind": "internal"}}
        self.assertFalse(sr._marked_public(unit))


class TestClassifyGazetteer(unittest.TestCase):
    def test_default(self):
        unit = {"text": "2024 年全市道路总里程 X 公里", "features": {"unique_targets": 50}}
        self.assertEqual(sr.classify_sensitivity(data_type="gazetteer", evidence_unit=unit),
                         ("aggregated_safe", "open"))

    def test_small_sample_upgrade(self):
        unit = {"text": "样本社区 5 个的统计", "features": {"unique_targets": 5}}
        self.assertEqual(sr.classify_sensitivity(data_type="gazetteer", evidence_unit=unit),
                         ("restricted", "restricted"))

    def test_public_downgrade(self):
        unit = {"text": "公开年鉴第三章", "features": {"source_kind": "public", "unique_targets": 50}}
        self.assertEqual(sr.classify_sensitivity(data_type="gazetteer", evidence_unit=unit),
                         ("open", "open"))


class TestClassifyTelecom(unittest.TestCase):
    def test_default_with_hashed_id(self):
        unit = {"text": "user a3f5 在 community_42", "features": {"community_id": "42"}}
        self.assertEqual(sr.classify_sensitivity(data_type="telecom", evidence_unit=unit),
                         ("pii_masked", "internal_only"))

    def test_plain_phone_upgrade(self):
        unit = {"text": "号码 13800138000 通话", "features": {}}
        self.assertEqual(sr.classify_sensitivity(data_type="telecom", evidence_unit=unit),
                         ("restricted", "restricted"))

    def test_aggregated_safe_downgrade(self):
        unit = {"text": "区间通话量统计", "features": {"call_count": 1200, "unique_contacts": 25}}
        self.assertEqual(sr.classify_sensitivity(data_type="telecom", evidence_unit=unit),
                         ("aggregated_safe", "open"))

    def test_aggregated_k_below_threshold_keeps_default(self):
        unit = {"text": "区间通话量统计", "features": {"call_count": 1200, "unique_contacts": 8}}
        self.assertEqual(sr.classify_sensitivity(data_type="telecom", evidence_unit=unit),
                         ("pii_masked", "internal_only"))

    def test_risk_label_upgrade(self):
        unit = {"text": "聚类社区 42", "features": {"community_id": "42", "purefraud_flag": True}}
        self.assertEqual(sr.classify_sensitivity(data_type="telecom", evidence_unit=unit),
                         ("restricted", "restricted"))

    def test_real_call_edge_field_names_upgrade_restricted(self):
        """src_user_id + event_time + station 组合应升 restricted (即使全部哈希化)。"""
        level, policy = sr.classify_sensitivity(
            data_type="telecom",
            evidence_unit={
                "text": "call edge",
                "features": {
                    "src_user_id": "u_hash_a3f5",
                    "event_time": "2024-03-01T10:00:00+08:00",
                    "station": "station_hash_42",
                },
            },
        )
        self.assertEqual((level, policy), ("restricted", "restricted"))

    def test_real_call_edge_with_cell_upgrade_restricted(self):
        """cell 替代 station 也触发升档。"""
        level, _ = sr.classify_sensitivity(
            data_type="telecom",
            evidence_unit={
                "text": "x",
                "features": {
                    "src_user_id": "u_hash",
                    "event_date": "2024-03-01",
                    "cell": "cell_hash_7",
                },
            },
        )
        self.assertEqual(level, "restricted")


class TestClassifyCode(unittest.TestCase):
    def test_default(self):
        unit = {"text": "def add(a, b): return a + b", "features": {"lang": "python"}}
        self.assertEqual(sr.classify_sensitivity(data_type="code", evidence_unit=unit),
                         ("pii_masked", "internal_only"))

    def test_secret_upgrade(self):
        unit = {"text": 'API_KEY = "sk_live_xxxxx"', "features": {}}
        self.assertEqual(sr.classify_sensitivity(data_type="code", evidence_unit=unit),
                         ("restricted", "restricted"))

    def test_public_code_downgrade(self):
        unit = {"text": "def add(a, b): return a + b", "features": {"source_kind": "public"}}
        self.assertEqual(sr.classify_sensitivity(data_type="code", evidence_unit=unit),
                         ("open", "open"))

    def test_public_but_has_token_keeps_restricted(self):
        unit = {"text": 'token = "abc"', "features": {"source_kind": "public"}}
        self.assertEqual(sr.classify_sensitivity(data_type="code", evidence_unit=unit),
                         ("restricted", "restricted"))


class TestClassifyStreetview(unittest.TestCase):
    def test_default_masked(self):
        unit = {"features": {"objects": [{"label": "face", "masked": True}]}}
        self.assertEqual(sr.classify_sensitivity(data_type="streetview", evidence_unit=unit),
                         ("pii_masked", "internal_only"))

    def test_unmasked_face_upgrade(self):
        unit = {"features": {"objects": [{"label": "face", "masked": False}]}}
        self.assertEqual(sr.classify_sensitivity(data_type="streetview", evidence_unit=unit),
                         ("restricted", "restricted"))

    def test_count_only_downgrade(self):
        unit = {"features": {"target_count": 30, "unique_targets": 30, "category": "tree"}}
        self.assertEqual(sr.classify_sensitivity(data_type="streetview", evidence_unit=unit),
                         ("aggregated_safe", "open"))

    def test_target_count_with_precise_geo_upgrade(self):
        unit = {"text": "采集点 39.908823,116.397470",
                "features": {"target_count": 30, "unique_targets": 30}}
        self.assertEqual(sr.classify_sensitivity(data_type="streetview", evidence_unit=unit),
                         ("restricted", "restricted"))


class TestClassifyRemoteSensing(unittest.TestCase):
    def test_default(self):
        unit = {"features": {"tile_id": "T50T", "change_score": 0.12}}
        self.assertEqual(sr.classify_sensitivity(data_type="remote_sensing", evidence_unit=unit),
                         ("aggregated_safe", "open"))

    def test_precise_geo_upgrade(self):
        unit = {"text": "点位 39.908823,116.397470 变化"}
        self.assertEqual(sr.classify_sensitivity(data_type="remote_sensing", evidence_unit=unit),
                         ("restricted", "restricted"))

    def test_public_downgrade(self):
        unit = {"features": {"tile_id": "T50T", "source_kind": "public"}}
        self.assertEqual(sr.classify_sensitivity(data_type="remote_sensing", evidence_unit=unit),
                         ("open", "open"))


class TestClassifySurveillance(unittest.TestCase):
    def test_default(self):
        unit = {"features": {"stream_url": "rtsp://10.0.0.1/cam"}}
        self.assertEqual(sr.classify_sensitivity(data_type="surveillance", evidence_unit=unit),
                         ("restricted", "restricted"))

    def test_masked_no_link_downgrade_pii(self):
        unit = {"features": {"objects": [{"label": "face", "masked": True}]}}
        self.assertEqual(sr.classify_sensitivity(data_type="surveillance", evidence_unit=unit),
                         ("pii_masked", "internal_only"))

    def test_aggregated_only_downgrade_safe(self):
        unit = {"features": {"target_count": 120, "unique_targets": 80}}
        self.assertEqual(sr.classify_sensitivity(data_type="surveillance", evidence_unit=unit),
                         ("aggregated_safe", "open"))


class TestClassifyErrors(unittest.TestCase):
    def test_unregistered_data_type_raises(self):
        with self.assertRaises(ValueError):
            sr.classify_sensitivity(data_type="not_a_type", evidence_unit={})

    def test_empty_unit_returns_default(self):
        self.assertEqual(sr.classify_sensitivity(data_type="telecom", evidence_unit={}),
                         ("pii_masked", "internal_only"))


class TestClassifySpatiotemporalTrajectory(unittest.TestCase):
    def _c(self, unit):
        return sr.classify_sensitivity(data_type="spatiotemporal_trajectory", evidence_unit=unit)

    def test_individual_staypoint_restricted(self):
        # 个体停留点(user_id + 时间 + 位置) → 可重识别 → restricted
        unit = {"text": "x", "features": {"user_id": "u10", "timestamp": "2025-05-20 09:40", "geohash": "wtw1z", "stay_minutes": 67}}
        self.assertEqual(self._c(unit), ("restricted", "restricted"))

    def test_aggregated_safe(self):
        # 区域聚合,主体数 >= 10,无个体 ID → aggregated_safe
        unit = {"text": "x", "features": {"geohash": "wtw3e", "visit_count": 24, "unique_users": 12}}
        self.assertEqual(self._c(unit), ("aggregated_safe", "open"))

    def test_aggregated_below_k_pii(self):
        # 聚合但主体数 < 10 → 保守 pii_masked
        unit = {"text": "x", "features": {"geohash": "wtw3s", "visit_count": 20, "unique_users": 9}}
        self.assertEqual(self._c(unit), ("pii_masked", "internal_only"))

    def test_precise_latlon_in_text_restricted(self):
        # 明文精确经纬度 → struct_id 命中 → restricted
        unit = {"text": "31.2304,121.4737", "features": {}}
        self.assertEqual(self._c(unit), ("restricted", "restricted"))

    def test_hashed_subject_triple_restricted(self):
        # 哈希化对象 + 时间 + 位置三元组 → restricted
        unit = {"text": "x", "features": {"user_id_hash": "ab12", "event_time": "t", "geohash": "wtw1z"}}
        self.assertEqual(self._c(unit), ("restricted", "restricted"))

    def test_empty_unit_pii_default(self):
        self.assertEqual(self._c({}), ("pii_masked", "internal_only"))


if __name__ == "__main__":
    unittest.main()
