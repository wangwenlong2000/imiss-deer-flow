"""6 类 v1.1 新 adapter 单元测试。

覆盖 traffic_flow / telecom / code / streetview / remote_sensing / surveillance。
"""

from __future__ import annotations

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import adapters, validate  # noqa: E402


def _assert_valid(testcase, wrapper):
    issues = validate.validate_evidence(wrapper)
    testcase.assertEqual(issues, [], msg=f"wrapper invalid: {issues}")


class TestTrafficFlow(unittest.TestCase):
    def test_minimal(self):
        w = adapters.adapt_traffic_flow_hit({
            "doc_id": "tf-001",
            "summary": "陆家嘴早高峰流量同比 +18%",
            "city": "Shanghai",
            "geohash": "wtw3s",
            "flow_count": 1280,
            "peak_hour": 8,
            "score": 0.91,
        }, rank=1)
        _assert_valid(self, w)
        payload = w["payload"]
        self.assertEqual(payload["data_type"], "traffic_flow")
        self.assertEqual(payload["meta"]["geo_scope"]["city"], "Shanghai")
        self.assertIn("flow_count", payload["meta"]["features"])

    def test_with_time_range(self):
        w = adapters.adapt_traffic_flow_hit({
            "doc_id": "tf-002",
            "summary": "晚高峰严重拥堵",
            "time_start": "2024-03-01T17:00:00+08:00",
            "time_end": "2024-03-01T19:00:00+08:00",
            "anomaly_flag": True,
            "score": 0.85,
        })
        self.assertEqual(w["payload"]["meta"]["time_range"]["mode"], "absolute")


class TestTelecom(unittest.TestCase):
    def test_default_with_hashed_id(self):
        wrapper = adapters.adapt_telecom_hit({
            "doc_id": "tc1", "summary": "user a3f5 in community_42",
            "community_id": "42",
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "pii_masked")
        self.assertEqual(payload["meta"]["access_policy"], "internal_only")

    def test_upgrade_on_plain_phone(self):
        wrapper = adapters.adapt_telecom_hit({
            "doc_id": "tc2", "summary": "号码 13800138000 通话",
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "restricted")
        self.assertEqual(payload["meta"]["access_policy"], "restricted")

    def test_downgrade_on_aggregated_safe(self):
        wrapper = adapters.adapt_telecom_hit({
            "doc_id": "tc3", "summary": "区间通话量统计",
            "call_count": 1200, "unique_contacts": 25,
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "aggregated_safe")
        self.assertEqual(payload["meta"]["access_policy"], "open")

    def test_geo_scope_is_object(self):
        w = adapters.adapt_telecom_hit({
            "doc_id": "tc-002", "summary": "x", "call_count": 1, "score": 0.5,
        })
        self.assertEqual(w["payload"]["meta"]["geo_scope"], {})

    def test_user_node_features_passthrough(self):
        """用户节点 evidence: 14 个字段全部透传到 features。"""
        hit = {
            "doc_id": "tc-un-1",
            "summary": "user node passthrough",
            "province": "江苏",
            "dataset_name": "ds1",
            "user_id": "u_hash_a3f5",
            "label": "normal",
            "sub_label": "purefraud",
            "age": 35,
            "open_card_time": "2024-01-01",
            "access_mode": "4G",
            "monthly_fee": 88.0,
            "monthly_flow_mb": 12000,
            "monthly_call_duration": 380,
            "caller_ratio_3m": 0.62,
            "caller_dispersion_3m": 0.41,
            "cross_province_ratio_3m": 0.08,
            "broadband_flag": True,
            "source_table": "user_nodes",
            "score": 0.9,
        }
        wrapper = adapters.adapt_telecom_hit(hit, rank=1)
        _assert_valid(self, wrapper)
        feats = wrapper["payload"]["meta"]["features"]
        for key in ("province", "dataset_name", "user_id", "label", "sub_label",
                    "age", "open_card_time", "access_mode", "monthly_fee",
                    "monthly_flow_mb", "monthly_call_duration", "caller_ratio_3m",
                    "caller_dispersion_3m", "cross_province_ratio_3m",
                    "broadband_flag", "source_table"):
            self.assertIn(key, feats, msg=f"missing {key}")
        # 评分类 / wrapper 顶层字段不应落进 features
        self.assertNotIn("score", feats)
        self.assertNotIn("doc_id", feats)

    def test_call_edge_features_passthrough(self):
        """通话边 evidence: 15 个字段全部透传 + time_range 取 event_time 时不被吞。"""
        hit = {
            "doc_id": "tc-ce-1",
            "summary": "call edge",
            "province": "江苏",
            "dataset_name": "ds1",
            "src_user_id": "u_hash_src",
            "dst_counterparty_id": "u_hash_dst",
            "event_time": "2024-03-01T10:00:00+08:00",
            "event_date": "2024-03-01",
            "event_hour": 10,
            "duration": 65,
            "call_type": "voice",
            "imei": "imei_hash_xyz",
            "city": "南京",
            "county": "玄武",
            "station": "station_hash_42",
            "cell": "cell_hash_7",
            "roaming_place": "place_hash_abc",
            "counterparty_belong": "中国移动",
            "source_table": "call_edges",
            "score": 0.8,
        }
        wrapper = adapters.adapt_telecom_hit(hit, rank=2)
        _assert_valid(self, wrapper)
        feats = wrapper["payload"]["meta"]["features"]
        for key in ("src_user_id", "dst_counterparty_id", "event_time",
                    "event_date", "event_hour", "duration", "call_type", "imei",
                    "city", "county", "station", "cell", "roaming_place",
                    "counterparty_belong", "source_table"):
            self.assertIn(key, feats, msg=f"missing {key}")

    def test_edge_relation_features_passthrough(self):
        """设备关系 evidence: src_id / dst_id / edge_type / edge_count 等透传。"""
        hit = {
            "doc_id": "tc-er-1",
            "summary": "phone-imei edge",
            "src_id": "ph_42", "dst_id": "imei_88",
            "src_type": "phone", "dst_type": "imei",
            "edge_type": "uses_imei",
            "dataset": "edges_phone_imei",
            "user_id": "u_hash_a", "imei": "imei_hash_b",
            "edge_count": 14,
            "score": 0.7,
        }
        wrapper = adapters.adapt_telecom_hit(hit)
        _assert_valid(self, wrapper)
        feats = wrapper["payload"]["meta"]["features"]
        for key in ("src_id", "dst_id", "src_type", "dst_type", "edge_type",
                    "dataset", "edge_count"):
            self.assertIn(key, feats, msg=f"missing {key}")

    def test_aggregated_stat_features_passthrough(self):
        """聚合统计 evidence: relation_strength / risk_user_count / source_refs 等透传。"""
        hit = {
            "doc_id": "tc-agg-1",
            "summary": "community stats",
            "community_id": "comm_42",
            "relation_strength": 0.82,
            "risk_user_count": 3,
            "label_count": {"normal": 120, "risk": 3},
            "cross_province_ratio": 0.14,
            "caller_dispersion": 0.6,
            "source_table": "community_stats",
            "source_refs": ["call_edges#W12", "user_nodes#prov-江苏"],
            "score": 0.6,
        }
        wrapper = adapters.adapt_telecom_hit(hit)
        _assert_valid(self, wrapper)
        feats = wrapper["payload"]["meta"]["features"]
        for key in ("community_id", "relation_strength", "risk_user_count",
                    "label_count", "cross_province_ratio", "caller_dispersion",
                    "source_table", "source_refs"):
            self.assertIn(key, feats, msg=f"missing {key}")

    def test_call_edge_e2e_upgrades_to_restricted(self):
        """通话边 evidence 经过 adapter 后, sensitivity_level 应自动升为 restricted。"""
        hit = {
            "doc_id": "tc-ce-up",
            "summary": "call edge restricted",
            "src_user_id": "u_hash_src",
            "event_time": "2024-03-01T10:00:00+08:00",
            "station": "station_hash_42",
            "score": 0.8,
        }
        wrapper = adapters.adapt_telecom_hit(hit)
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "restricted")
        self.assertEqual(payload["meta"]["access_policy"], "restricted")


class TestCode(unittest.TestCase):
    def test_locator_and_features(self):
        w = adapters.adapt_code_hit({
            "doc_id": "code-001",
            "snippet": "def _load_landmarks(): ...",
            "file_path": "skills/_shared/retrieval_protocol/adapters.py",
            "line_start": 120, "line_end": 138,
            "lang": "python",
            "ast_node_type": "function_def",
            "symbol": "_load_landmarks",
            "bm25_score": 12.3,
        }, rank=1)
        _assert_valid(self, w)
        payload = w["payload"]
        self.assertEqual(payload["data_type"], "code")
        self.assertEqual(payload["meta"]["locator"]["line_start"], 120)
        self.assertEqual(payload["meta"]["locator"]["symbol"], "_load_landmarks")
        self.assertEqual(payload["meta"]["features"]["lang"], "python")
        self.assertEqual(w["retrieval"]["raw_scores"]["bm25_score"], 12.3)


class TestStreetview(unittest.TestCase):
    def test_image_geo(self):
        w = adapters.adapt_streetview_hit({
            "doc_id": "sv-001",
            "caption": "陆家嘴中环夜间街景,有 1 处违停",
            "objects": ["car", "truck", "pedestrian"],
            "bbox": [121.50, 31.23, 121.51, 31.24],
            "taken_at": "2024-03-01T22:30:00+08:00",
            "city": "Shanghai",
            "lat": 31.235, "lon": 121.505,
            "image_uri": "s3://streetview/sv-001.jpg",
            "score": 0.88,
        }, rank=1)
        _assert_valid(self, w)
        payload = w["payload"]
        self.assertEqual(payload["data_type"], "streetview")
        self.assertEqual(payload["meta"]["geo_scope"]["lat"], 31.235)
        self.assertIn("objects", payload["meta"]["features"])
        self.assertEqual(payload["meta"]["features"]["image_uri"], "s3://streetview/sv-001.jpg")

    def test_new_format_metadata_address(self):
        """5/29 实际形态: hit.metadata.{latitude, longitude, address.{...}}。"""
        hit = {
            "_id": "EQ8RlaMMjkqWPCy0WoItIg_012_030",
            "source_path": "/mnt/nas/streetview_meta/.../EQ8RlaMMjkqWPCy0WoItIg_012_030.png",
            "metadata": {
                "latitude": 34.261,
                "longitude": 108.946,
                "address": {
                    "formatted_address": "陕西省西安市碑林区北大街1号",
                    "business": "钟楼",
                    "country": "中国",
                    "province": "陕西省",
                    "city": "西安市",
                    "district": "碑林区",
                    "street": "北大街",
                    "street_number": "1号",
                    "adcode": "610103",
                    "sematic_description": "钟楼东北侧15米",
                    "pois": [
                        {"name": "钟楼", "tag": "旅游景点", "distance": "15", "direction": "东"},
                    ],
                    "roads": [{"name": "北大街", "distance": "15"}],
                },
            },
            "score": 0.85,
        }
        wrapper = adapters.adapt_streetview_hit(hit, rank=1)
        _assert_valid(self, wrapper)
        payload = wrapper["payload"]
        geo = payload["meta"]["geo_scope"]
        self.assertEqual(geo.get("lat"), 34.261)
        self.assertEqual(geo.get("lon"), 108.946)
        self.assertEqual(geo.get("city"), "西安市")
        self.assertEqual(geo.get("district"), "碑林区")
        feats = payload["meta"]["features"]
        self.assertIn("address", feats)
        self.assertEqual(feats["address"]["formatted_address"], "陕西省西安市碑林区北大街1号")
        self.assertEqual(feats["address"]["business"], "钟楼")
        self.assertEqual(len(feats["address"]["pois"]), 1)

    def test_metadata_address_partial_falls_back_to_top_level(self):
        """metadata.address 存在但缺 city/district 时, 应回退到 hit.city/district。"""
        hit = {
            "_id": "partial-001",
            "metadata": {
                "latitude": 31.235,
                "longitude": 121.505,
                "address": {
                    "formatted_address": "上海市黄浦区南京东路某号",
                    # 故意缺 city / district
                },
            },
            "city": "Shanghai",
            "district": "Huangpu",
            "score": 0.7,
        }
        wrapper = adapters.adapt_streetview_hit(hit)
        _assert_valid(self, wrapper)
        geo = wrapper["payload"]["meta"]["geo_scope"]
        self.assertEqual(geo.get("city"), "Shanghai")
        self.assertEqual(geo.get("district"), "Huangpu")
        feats = wrapper["payload"]["meta"]["features"]
        self.assertIn("address", feats)
        self.assertEqual(feats["address"]["formatted_address"], "上海市黄浦区南京东路某号")


class TestRemoteSensing(unittest.TestCase):
    def test_change_detection(self):
        w = adapters.adapt_remote_sensing_hit({
            "doc_id": "rs-001",
            "caption": "瓦片 T08-12 检出新增建筑 2 处",
            "objects": ["building_new"],
            "bbox": [121.4, 31.2, 121.5, 31.3],
            "tile_id": "T08-12",
            "taken_at": "2024-03-15T10:00:00+08:00",
            "change_score": 0.72,
            "cloud_cover": 0.1,
            "score": 0.81,
        }, rank=1)
        _assert_valid(self, w)
        payload = w["payload"]
        self.assertEqual(payload["data_type"], "remote_sensing")
        self.assertEqual(payload["meta"]["features"]["tile_id"], "T08-12")
        self.assertAlmostEqual(payload["meta"]["features"]["change_score"], 0.72)

    def test_new_format_imagery_record(self):
        """5/29 实际形态: id / title / content / similarity / url / hash /
        resolution / exif。"""
        hit = {
            "id": "rs-new-001",
            "title": "瓦片 T08-12 影像",
            "content": "陕西省西安市碑林区上空 0.5m 分辨率",
            "similarity": 0.82,
            "rank": 3,
            "url": "s3://rs/rs-new-001.tif",
            "hash": "sha256:abc123",
            "resolution": "0.5m",
            "exif": {"capture_time": "2024-03-15T10:00:00+08:00"},
        }
        wrapper = adapters.adapt_remote_sensing_hit(hit, rank=1)
        _assert_valid(self, wrapper)
        payload = wrapper["payload"]
        self.assertEqual(payload["evidence_id"], "rs-new-001")
        self.assertEqual(payload["meta"]["source_path"], "s3://rs/rs-new-001.tif")
        self.assertTrue(payload["text"])
        self.assertIn("瓦片", payload["text"])
        feats = payload["meta"]["features"]
        for key in ("id", "title", "content", "similarity", "url", "hash",
                    "resolution", "exif"):
            self.assertIn(key, feats, msg=f"missing {key}")

    def test_new_format_with_precise_coord_in_text_upgrades(self):
        """content 中含明文精确经纬度应升 restricted (struct_id LATLON 正则)."""
        hit = {
            "id": "rs-002",
            "title": "瓦片",
            "content": "中心点 121.4738, 31.2304",  # LATLON 4 位小数命中正则
            "url": "s3://rs/rs-002.tif",
        }
        wrapper = adapters.adapt_remote_sensing_hit(hit)
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "restricted")


class TestSurveillance(unittest.TestCase):
    def test_default_restricted(self):
        wrapper = adapters.adapt_surveillance_hit({
            "doc_id": "sv1", "summary": "frame summary",
            "stream_url": "rtsp://10.0.0.1/cam",
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "restricted")
        self.assertEqual(payload["meta"]["access_policy"], "restricted")

    def test_downgrade_when_masked_no_link(self):
        wrapper = adapters.adapt_surveillance_hit({
            "doc_id": "sv2", "summary": "blurred crowd",
            "objects": [{"label": "face", "masked": True}],
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "pii_masked")
        self.assertEqual(payload["meta"]["access_policy"], "internal_only")

    def test_new_format_video_record(self):
        """5/29 实际形态: video_id / camera_id / raw_segment_uri / started_at /
        ended_at / labels / object_summary / location。"""
        hit = {
            "video_id": "vid_001",
            "camera_id": "cam_42",
            "filename": "20240301_10_42.mp4",
            "raw_segment_uri": "s3://surv/vid_001.mp4",
            "started_at": "2024-03-01T10:00:00+08:00",
            "ended_at": "2024-03-01T10:30:00+08:00",
            "labels": ["traffic", "intersection"],
            "object_summary": {"person": 12, "car": 34},
            "location": {"city": "Shanghai", "camera_lat": 31.235, "camera_lon": 121.505},
            "metadata": {"frame_rate": 25, "resolution": "1920x1080"},
            "score": 0.91,
        }
        wrapper = adapters.adapt_surveillance_hit(hit, rank=1)
        _assert_valid(self, wrapper)
        payload = wrapper["payload"]
        self.assertEqual(payload["evidence_id"], "vid_001")
        self.assertEqual(payload["meta"]["source_path"], "s3://surv/vid_001.mp4")
        tr = payload["meta"]["time_range"]
        self.assertEqual(tr["start"], "2024-03-01T10:00:00+08:00")
        self.assertEqual(tr["end"], "2024-03-01T10:30:00+08:00")
        geo = payload["meta"]["geo_scope"]
        self.assertEqual(geo["city"], "Shanghai")
        self.assertEqual(geo["lat"], 31.235)
        self.assertEqual(geo["lon"], 121.505)
        feats = payload["meta"]["features"]
        for key in ("video_id", "camera_id", "filename", "raw_segment_uri",
                    "labels", "object_summary", "location", "metadata"):
            self.assertIn(key, feats, msg=f"missing {key}")


class TestBatchAdapters(unittest.TestCase):
    """逐 adapter 的 *_result 接口应当对齐 rank 编号(从 1 开始)。"""

    def test_traffic_flow_rank_numbering(self):
        wrappers = adapters.adapt_traffic_flow_result([
            {"doc_id": f"tf-{i}", "summary": "x", "score": 0.5, "flow_count": 1}
            for i in range(3)
        ])
        self.assertEqual([w["rank"] for w in wrappers], [1, 2, 3])

    def test_all_six_have_batch_helpers(self):
        # 仅做存在性检查 —— 每个 *_result 函数都应可调用且返回 list。
        for fn_name in (
            "adapt_traffic_flow_result", "adapt_telecom_result", "adapt_code_result",
            "adapt_streetview_result", "adapt_remote_sensing_result", "adapt_surveillance_result",
        ):
            fn = getattr(adapters, fn_name)
            self.assertTrue(callable(fn), fn_name)
            self.assertEqual(fn([]), [])


class TestRoadTrafficGazetteerSensitivity(unittest.TestCase):
    def test_default_aggregated_safe(self):
        from retrieval_protocol import adapt_road_traffic_hit
        wrapper = adapt_road_traffic_hit({
            "section_path": "第二章 道路概况", "pages": "12-13",
            "preview": "2024 年全市道路总里程 X 公里", "unique_targets": 50,
            "score": 0.85,
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "aggregated_safe")
        self.assertEqual(payload["meta"]["access_policy"], "open")
        _assert_valid(self, wrapper)

    def test_public_downgrade_to_open(self):
        from retrieval_protocol import adapt_road_traffic_hit
        wrapper = adapt_road_traffic_hit({
            "section_path": "公开年鉴", "pages": "1",
            "preview": "公开年鉴第三章", "source_kind": "public",
            "unique_targets": 50,
            "score": 0.85,
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "open")
        self.assertEqual(payload["meta"]["access_policy"], "open")
        _assert_valid(self, wrapper)


class TestCodeSensitivity(unittest.TestCase):
    def test_default_pii_masked(self):
        from retrieval_protocol import adapt_code_hit
        wrapper = adapt_code_hit({
            "doc_id": "c1", "snippet": "def add(a, b): return a + b",
            "lang": "python", "file_path": "/srv/repo/x.py",
            "line_start": 1, "line_end": 1,
            "score": 0.80,
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "pii_masked")
        self.assertEqual(payload["meta"]["access_policy"], "internal_only")
        _assert_valid(self, wrapper)

    def test_secret_upgrade(self):
        from retrieval_protocol import adapt_code_hit
        wrapper = adapt_code_hit({
            "doc_id": "c2", "snippet": 'API_KEY = "sk_live_xxxxx"',
            "lang": "python", "file_path": "/srv/repo/x.py",
            "score": 0.80,
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "restricted")
        self.assertEqual(payload["meta"]["access_policy"], "restricted")
        _assert_valid(self, wrapper)


class TestStreetviewSensitivity(unittest.TestCase):
    def test_unmasked_face_upgrade(self):
        from retrieval_protocol import adapt_streetview_hit
        wrapper = adapt_streetview_hit({
            "doc_id": "sv1", "summary": "frame",
            "objects": [{"label": "face", "masked": False}],
            "score": 0.80,
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "restricted")
        self.assertEqual(payload["meta"]["access_policy"], "restricted")
        _assert_valid(self, wrapper)

    def test_masked_default_pii(self):
        from retrieval_protocol import adapt_streetview_hit
        wrapper = adapt_streetview_hit({
            "doc_id": "sv2", "summary": "frame",
            "objects": [{"label": "face", "masked": True}],
            "score": 0.80,
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "pii_masked")
        self.assertEqual(payload["meta"]["access_policy"], "internal_only")
        _assert_valid(self, wrapper)


class TestRemoteSensingSensitivity(unittest.TestCase):
    def test_default_aggregated_safe(self):
        from retrieval_protocol import adapt_remote_sensing_hit
        wrapper = adapt_remote_sensing_hit({
            "doc_id": "r1", "summary": "tile change",
            "tile_id": "T50T", "change_score": 0.12,
            "score": 0.80,
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "aggregated_safe")
        self.assertEqual(payload["meta"]["access_policy"], "open")
        _assert_valid(self, wrapper)

    def test_precise_geo_upgrade(self):
        from retrieval_protocol import adapt_remote_sensing_hit
        wrapper = adapt_remote_sensing_hit({
            "doc_id": "r2",
            "summary": "点位 39.908823,116.397470 变化",
            "score": 0.80,
        })
        payload = wrapper["payload"]
        self.assertEqual(payload["meta"]["sensitivity_level"], "restricted")
        self.assertEqual(payload["meta"]["access_policy"], "restricted")
        _assert_valid(self, wrapper)


if __name__ == "__main__":
    unittest.main()
