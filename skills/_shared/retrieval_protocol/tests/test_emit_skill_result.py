"""emit_skill_result.py（平台层 wrapper）单元 + 端到端测试。

覆盖：score 取值与 rank 兜底、geo_scope 抽取、evidence_id 选取与去重、
逐条敏感度落档、整体 SkillResult 经 validate_skill_result 校验通过。
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import emit_skill_result as em  # noqa: E402
from retrieval_protocol import validate_skill_result  # noqa: E402

DT = "spatiotemporal_trajectory"


class TestScoreOf(unittest.TestCase):
    def test_semantic_score_field(self):
        self.assertEqual(em._score_of({"visit_count": 20}), 20.0)
        self.assertEqual(em._score_of({"similarity": 0.83}), 0.83)
        self.assertEqual(em._score_of({"stay_minutes": 67}), 67.0)

    def test_no_score_field_returns_none(self):
        self.assertIsNone(em._score_of({"region": "Shanghai", "name": "x"}))

    def test_non_numeric_ignored(self):
        self.assertIsNone(em._score_of({"score": "high"}))


class TestGeoOf(unittest.TestCase):
    def test_full_extract(self):
        g = em._geo_of({"geohash": "wtw3s", "landmark": "陆家嘴金融区",
                        "lat": 31.2244, "lon": 121.4872, "region": "Shanghai"})
        self.assertEqual(g["geohash"], "wtw3s")
        self.assertEqual(g["landmark"], "陆家嘴金融区")
        self.assertEqual(g["lat"], 31.2244)
        self.assertEqual(g["district"], "Shanghai")

    def test_grid_id_as_geohash(self):
        self.assertEqual(em._geo_of({"grid_id": "wtw1ze"})["geohash"], "wtw1ze")

    def test_empty_when_no_geo(self):
        self.assertEqual(em._geo_of({"foo": 1}), {})


class TestIdOf(unittest.TestCase):
    def test_prefers_evidence_id_then_geohash(self):
        self.assertEqual(em._id_of({"evidence_id": "E1", "geohash": "g"}, 0), "E1")
        self.assertEqual(em._id_of({"geohash": "wtw3s"}, 0), "wtw3s")

    def test_index_fallback(self):
        self.assertEqual(em._id_of({"foo": "bar"}, 7), "ev7")


class TestToEvidence(unittest.TestCase):
    def test_individual_trajectory_restricted(self):
        ev = em._to_evidence(
            {"user_id": "u10", "timestamp": "2025-05-20 09:40", "geohash": "wtw1z", "stay_minutes": 67},
            "extract_stay_points", DT, 0)
        self.assertEqual(ev["payload"]["meta"]["sensitivity_level"], "restricted")
        self.assertEqual(ev["score"], 67.0)
        self.assertEqual(ev["rank"], 1)

    def test_aggregated_safe(self):
        ev = em._to_evidence(
            {"geohash": "wtw3e", "unique_users": 12, "visit_count": 24},
            "analyze_region_heat", DT, 0)
        self.assertEqual(ev["payload"]["meta"]["sensitivity_level"], "aggregated_safe")

    def test_aggregated_below_k_pii(self):
        ev = em._to_evidence(
            {"geohash": "wtw3s", "unique_users": 9, "visit_count": 20},
            "analyze_region_heat", DT, 0)
        self.assertEqual(ev["payload"]["meta"]["sensitivity_level"], "pii_masked")

    def test_rank_fallback_score(self):
        # 无任何 score 字段 → 按 rank 兜底 1/(idx+1)，保证可校验
        ev = em._to_evidence({"region": "Shanghai"}, "profile_urban_region", DT, 1)
        self.assertEqual(ev["score"], 0.5)

    def test_features_carried_into_unit(self):
        rec = {"geohash": "wtw3s", "visit_count": 5}
        ev = em._to_evidence(rec, "map_spatial_grid", DT, 0)
        self.assertEqual(ev["payload"]["meta"]["features"], rec)
        self.assertEqual(ev["payload"]["data_type"], DT)


class TestEndToEnd(unittest.TestCase):
    def _run(self, out_dir: str, skill: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(pathlib.Path(em.__file__).resolve()),
             "--output-dir", out_dir, "--skill-name", skill],
            capture_output=True, text=True)

    def _write_jsonl(self, path: pathlib.Path, rows: list[dict]) -> None:
        path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")

    def test_validate_ok_and_dedup_evidence_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = pathlib.Path(tmp)
            # 同一 geohash 多条 → evidence_id 会撞，wrapper 必须去重
            self._write_jsonl(d / "grid.jsonl", [
                {"geohash": "wtw1z", "user_id": "u1", "visit_count": 3},
                {"geohash": "wtw1z", "user_id": "u2", "visit_count": 5},
                {"geohash": "wtw1z", "user_id": "u3", "visit_count": 2},
            ])
            r = self._run(tmp, "map_spatial_grid")
            self.assertIn("VALIDATE_OK", r.stdout, msg=r.stdout + r.stderr)
            sr = json.loads((d / "skill_result.json").read_text(encoding="utf-8"))
            ids = [e["payload"]["evidence_id"] for e in sr["result"]["evidence"]]
            self.assertEqual(len(ids), 3)
            self.assertEqual(len(ids), len(set(ids)))  # 唯一
            self.assertEqual(validate_skill_result(sr), [])

    def test_score_fallback_makes_scoreless_records_valid(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = pathlib.Path(tmp)
            self._write_jsonl(d / "regions.jsonl", [
                {"region": "A"}, {"region": "B"},  # 无 score 字段
            ])
            r = self._run(tmp, "profile_urban_region")
            self.assertIn("VALIDATE_OK", r.stdout, msg=r.stdout + r.stderr)
            sr = json.loads((d / "skill_result.json").read_text(encoding="utf-8"))
            self.assertTrue(all(isinstance(e["score"], (int, float)) for e in sr["result"]["evidence"]))

    def test_summary_metrics_from_summary_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = pathlib.Path(tmp)
            self._write_jsonl(d / "heat.jsonl", [{"geohash": "wtw3e", "unique_users": 12}])
            (d / "summary.json").write_text(json.dumps({"total": 64, "regions": 4}), encoding="utf-8")
            self._run(tmp, "analyze_region_heat")
            sr = json.loads((d / "skill_result.json").read_text(encoding="utf-8"))
            names = {m["name"] for m in sr["result"]["summary"].get("key_metrics", [])}
            self.assertIn("total", names)
            self.assertIn("regions", names)


if __name__ == "__main__":
    unittest.main()
