"""统一检索中间件单元测试 —— schema v1.1。

覆盖:
- 缓存命中/失效/超时;
- 监控日志写入;
- 审计留痕字段;
- 错误状态不入缓存。
"""

from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import envelope as envelope_mod, evidence as evidence_mod  # noqa: E402
from retrieval_protocol import middleware  # noqa: E402


def _make_envelope(query: str = "陆家嘴异常", data_type: str = "netflow") -> dict:
    return envelope_mod.build_input_envelope(
        skill_name="network-traffic-analysis",
        scenario="netflow",
        capability="evidence_search",
        query=query,
        data_type=data_type,
        top_k=10,
        mode="es",
    )


def _make_result(status: str = "success", evidence_count: int = 2) -> dict:
    wrappers = []
    for i in range(evidence_count):
        wrappers.append(evidence_mod.build_retrieval_evidence(
            evidence_ref=f"e-{i}",
            score=0.9 - i * 0.1,
            method="bm25_vector_rrf",
            payload=evidence_mod.build_evidence_unit(
                evidence_id=f"e-{i}",
                data_type="netflow",
                text=f"hit-{i}",
                source_id="ds",
                geo_scope={},
                features={"anomaly_flag": i == 0},
                sensitivity_level="pii_masked" if i == 0 else None,
                access_policy="restricted" if i == 0 else None,
            ),
        ))
    return evidence_mod.build_skill_result(
        skill_name="network-traffic-analysis",
        scenario="netflow",
        capability="evidence_search",
        status=status,
        summary=evidence_mod.build_summary(title="t", overview="o"),
        evidence=wrappers,
    )


class TestCacheKey(unittest.TestCase):
    def test_request_id_does_not_affect_key(self):
        e1 = _make_envelope()
        e2 = _make_envelope()
        # request_id 不同,但语义相同 → key 应一致。
        self.assertNotEqual(e1["request_id"], e2["request_id"])
        self.assertEqual(
            middleware.compute_cache_key(e1),
            middleware.compute_cache_key(e2),
        )

    def test_query_change_changes_key(self):
        e1 = _make_envelope(query="A")
        e2 = _make_envelope(query="B")
        self.assertNotEqual(
            middleware.compute_cache_key(e1),
            middleware.compute_cache_key(e2),
        )

    def test_budget_change_changes_key(self):
        e1 = _make_envelope()
        e2 = _make_envelope()
        e2["budget"] = envelope_mod.build_budget(max_evidence_count=5)
        self.assertNotEqual(
            middleware.compute_cache_key(e1),
            middleware.compute_cache_key(e2),
        )


class TestLRUCache(unittest.TestCase):
    def test_set_get(self):
        cache = middleware.LRUCache(max_entries=4)
        cache.put("a", {"x": 1})
        self.assertEqual(cache.get("a"), {"x": 1})

    def test_lru_eviction(self):
        cache = middleware.LRUCache(max_entries=2)
        cache.put("a", {"v": 1})
        cache.put("b", {"v": 2})
        cache.put("c", {"v": 3})
        self.assertIsNone(cache.get("a"))
        self.assertIsNotNone(cache.get("b"))
        self.assertIsNotNone(cache.get("c"))

    def test_ttl_expiry(self):
        cache = middleware.LRUCache(max_entries=4, ttl_seconds=0.05)
        cache.put("a", {"x": 1})
        time.sleep(0.1)
        self.assertIsNone(cache.get("a"))


class TestMiddlewareEndToEnd(unittest.TestCase):
    def test_cache_hit_skips_retrieve(self):
        called = []

        def retrieve(env):
            called.append(env)
            return _make_result()

        mw = middleware.RetrievalMiddleware()
        env = _make_envelope()
        r1 = mw.call(env, retrieve_fn=retrieve)
        r2 = mw.call(env, retrieve_fn=retrieve)
        self.assertIs(r1, r2)
        self.assertEqual(len(called), 1)

    def test_error_result_not_cached(self):
        calls = []

        def retrieve(env):
            calls.append(1)
            return _make_result(status="error")

        mw = middleware.RetrievalMiddleware()
        env = _make_envelope()
        mw.call(env, retrieve_fn=retrieve)
        mw.call(env, retrieve_fn=retrieve)
        self.assertEqual(len(calls), 2)

    def test_log_sink_emits_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = pathlib.Path(tmp) / "retrieval.jsonl"
            mw = middleware.RetrievalMiddleware(
                log_sink=middleware.JsonlSink(log_path),
            )
            env = _make_envelope()
            mw.call(env, retrieve_fn=lambda e: _make_result())
            mw.call(env, retrieve_fn=lambda e: _make_result())  # cache hit
            lines = log_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 2)
            recs = [json.loads(line) for line in lines]
            self.assertFalse(recs[0]["cache_hit"])
            self.assertTrue(recs[1]["cache_hit"])
            self.assertEqual(recs[0]["hit_count"], 2)
            self.assertEqual(recs[0]["skill_name"], "network-traffic-analysis")

    def test_audit_sink_records_sensitivity(self):
        records = []
        mw = middleware.RetrievalMiddleware(audit_sink=lambda r: records.append(r))
        mw.call(_make_envelope(), retrieve_fn=lambda e: _make_result(),
                user_id="zhangsan")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["user_id"], "zhangsan")
        # 第一条 evidence 标 pii_masked,故 sensitive_hit_count >= 1。
        self.assertGreaterEqual(records[0]["sensitive_hit_count"], 1)
        self.assertIn("pii_masked", records[0]["sensitivity_distribution"])

    def test_audit_sink_failure_does_not_crash(self):
        def failing_sink(rec):
            raise RuntimeError("compliance store down")
        mw = middleware.RetrievalMiddleware(audit_sink=failing_sink)
        result = mw.call(_make_envelope(), retrieve_fn=lambda e: _make_result())
        self.assertEqual(result["status"], "success")


class TestSummarizeSensitivity(unittest.TestCase):
    def test_counts_each_level(self):
        result = _make_result(evidence_count=3)
        evidence = result["result"]["evidence"]
        counts = middleware.summarize_sensitivity(evidence)
        # 第一条 pii_masked,其余无 sensitivity_level 字段 → 视为 open。
        self.assertEqual(counts.get("pii_masked"), 1)
        self.assertEqual(counts.get("open"), 2)


class TestExtendedAuditFields(unittest.TestCase):
    def _envelope_with_data_type(self, data_type: str = "telecom") -> dict:
        # 构造一个最小合法 envelope,仅供 audit 字段抽取用
        return {
            "schema_version": "1.1",
            "request_id": "rid-1",
            "skill_name": "test-skill",
            "scenario": "test-scene",
            "capability": "evidence_search",
            "input": {
                "data_sources": [],
                "parameters": {"query": "q", "data_type": data_type, "top_k": 5, "mode": "auto"},
                "filters": {},
                "context": {},
            },
        }

    def test_call_accepts_new_kwargs_backward_compat(self):
        # 老调用 (无新 kwargs) 仍然能用
        records = []
        mw = middleware.RetrievalMiddleware(audit_sink=lambda r: records.append(r))
        env = self._envelope_with_data_type()
        result = {"status": "success", "result": {"evidence": []}}
        mw.call(env, retrieve_fn=lambda e: result)
        self.assertEqual(len(records), 1)

    def test_audit_record_has_gate_scene_data_type(self):
        records = []
        mw = middleware.RetrievalMiddleware(audit_sink=lambda r: records.append(r))
        env = self._envelope_with_data_type("telecom")
        result = {"status": "success", "result": {"evidence": []}}
        mw.call(
            env, retrieve_fn=lambda e: result,
            gate="InputGate", scene="self_use",
            policy_version="2026-05-27.1", detector_version="d-0.3.0",
        )
        rec = records[0]
        self.assertEqual(rec["gate"], "InputGate")
        self.assertEqual(rec["scene"], "self_use")
        self.assertEqual(rec["data_type"], "telecom")
        self.assertEqual(rec["policy_version"], "2026-05-27.1")
        self.assertEqual(rec["detector_version"], "d-0.3.0")

    def test_audit_record_carries_evidence_actions(self):
        from retrieval_protocol.audit import build_evidence_action
        action = build_evidence_action(
            evidence_id="ev_1", action="filter", action_status="applied",
            triggered_violation_types=["V_PII_PHONE"],
            risk_locations=[{"field_path": "meta.subject", "risk_type": "phone"}],
            reason_code="struct_id_detected",
            sensitivity_before="pii_masked", sensitivity_after="restricted",
        )
        records = []
        mw = middleware.RetrievalMiddleware(audit_sink=lambda r: records.append(r))
        env = self._envelope_with_data_type("telecom")
        result = {"status": "success", "result": {"evidence": []}}
        mw.call(
            env, retrieve_fn=lambda e: result,
            gate="OutputGate", evidence_actions=[action],
        )
        self.assertEqual(records[0]["evidence_actions"], [action])

    def test_audit_record_omits_none_fields(self):
        records = []
        mw = middleware.RetrievalMiddleware(audit_sink=lambda r: records.append(r))
        env = self._envelope_with_data_type()
        result = {"status": "success", "result": {"evidence": []}}
        mw.call(env, retrieve_fn=lambda e: result)  # 不传新 kwargs
        rec = records[0]
        # spec §4.1: 新字段缺省 None 时不写入
        self.assertNotIn("gate", rec)
        self.assertNotIn("scene", rec)
        self.assertNotIn("policy_version", rec)
        self.assertNotIn("detector_version", rec)
        # data_type 仍应从 envelope 抽取
        self.assertEqual(rec["data_type"], "telecom")
        # evidence_actions 缺省空列表（始终写入）
        self.assertEqual(rec["evidence_actions"], [])


class TestJsonlSinkAsAuditSink(unittest.TestCase):
    def test_jsonl_audit_sink_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "audit.jsonl"
            sink = middleware.JsonlSink(path)
            mw = middleware.RetrievalMiddleware(audit_sink=sink.emit)
            env = {
                "schema_version": "1.1", "request_id": "rid", "skill_name": "s",
                "scenario": "sc", "capability": "evidence_search",
                "input": {"data_sources": [], "parameters": {"query": "q", "data_type": "telecom", "top_k": 1, "mode": "auto"}, "filters": {}, "context": {}},
            }
            mw.call(env, retrieve_fn=lambda e: {"status": "success", "result": {"evidence": []}},
                    gate="InputGate", scene="self_use")
            lines = path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 1)
            row = json.loads(lines[0])
            self.assertEqual(row["gate"], "InputGate")
            self.assertEqual(row["data_type"], "telecom")


if __name__ == "__main__":
    unittest.main()
