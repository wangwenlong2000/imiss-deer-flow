"""aggregator.py 单元测试 —— 对齐/去重/排序/预算裁剪。"""

from __future__ import annotations

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import aggregator, planner  # noqa: E402


def _wrapper(evidence_id, score, text="x"):
    """构造一条最小 retrieval evidence wrapper。"""
    return {
        "evidence_ref": evidence_id,
        "type": "retrieval",
        "score": score,
        "payload": {"evidence_id": evidence_id, "data_type": "netflow", "text": text},
    }


def _skill_result(evidence):
    return {
        "schema_version": "1.1",
        "request_id": "r",
        "skill_name": "s",
        "scenario": "netflow",
        "capability": "evidence_search",
        "status": "success",
        "result": {"summary": {}, "findings": [], "evidence": evidence, "artifacts": []},
        "diagnostics": {},
        "errors": [],
    }


def _plan(query_ids, total_budget=None):
    tasks = [planner.RetrievalTask(query_id=q, envelope={}) for q in query_ids]
    return planner.Plan(parent_query_id="root", tasks=tasks, total_budget=total_budget)


class TestAggregateBasic(unittest.TestCase):
    def test_align_and_merge_two_buckets(self):
        plan = _plan(["q1", "q2"])
        results = [
            {"query_id": "q1", "skill_result": _skill_result([_wrapper("e1", 0.9)])},
            {"query_id": "q2", "skill_result": _skill_result([_wrapper("e2", 0.8)])},
        ]
        agg = aggregator.Aggregator()
        out = agg.aggregate(plan=plan, skill_results=results)
        ev = out["result"]["evidence"]
        self.assertEqual(len(ev), 2)
        self.assertEqual(out["status"], "success")

    def test_dedup_keeps_best_score(self):
        plan = _plan(["q1", "q2"])
        results = [
            {"query_id": "q1", "skill_result": _skill_result([_wrapper("dup", 0.5)])},
            {"query_id": "q2", "skill_result": _skill_result([_wrapper("dup", 0.95)])},
        ]
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=results)
        ev = out["result"]["evidence"]
        self.assertEqual(len(ev), 1)
        self.assertAlmostEqual(ev[0]["score"], 0.95)

    def test_global_sort_desc_by_score(self):
        plan = _plan(["q1"])
        results = [
            {"query_id": "q1", "skill_result": _skill_result([
                _wrapper("a", 0.3), _wrapper("b", 0.9), _wrapper("c", 0.6),
            ])},
        ]
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=results)
        scores = [w["score"] for w in out["result"]["evidence"]]
        self.assertEqual(scores, [0.9, 0.6, 0.3])

    def test_unaligned_query_id_still_included(self):
        plan = _plan(["q1"])
        results = [
            {"query_id": "q1", "skill_result": _skill_result([_wrapper("a", 0.5)])},
            {"query_id": "zzz", "skill_result": _skill_result([_wrapper("b", 0.7)])},
        ]
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=results)
        # 未对齐的桶仍并入(不静默丢弃)
        ids = {w["payload"]["evidence_id"] for w in out["result"]["evidence"]}
        self.assertEqual(ids, {"a", "b"})


class TestAggregateHooks(unittest.TestCase):
    def test_custom_dedup_key(self):
        plan = _plan(["q1"])
        # 两条 evidence_id 不同但 text 相同 → 自定义 dedup 按 text 去重
        results = [
            {"query_id": "q1", "skill_result": _skill_result([
                _wrapper("a", 0.5, text="same"), _wrapper("b", 0.9, text="same"),
            ])},
        ]
        hooks = aggregator.AggregatorHooks(
            dedup_key=lambda w: w["payload"]["text"],
        )
        out = aggregator.Aggregator(hooks).aggregate(plan=plan, skill_results=results)
        self.assertEqual(len(out["result"]["evidence"]), 1)
        self.assertAlmostEqual(out["result"]["evidence"][0]["score"], 0.9)

    def test_custom_sort_key(self):
        plan = _plan(["q1"])
        results = [
            {"query_id": "q1", "skill_result": _skill_result([
                _wrapper("a", 0.9), _wrapper("b", 0.3),
            ])},
        ]
        # 反向:按 score 升序
        hooks = aggregator.AggregatorHooks(sort_key=lambda w: w.get("score", 0.0))
        out = aggregator.Aggregator(hooks).aggregate(plan=plan, skill_results=results)
        scores = [w["score"] for w in out["result"]["evidence"]]
        self.assertEqual(scores, [0.3, 0.9])

    def test_empty_results(self):
        plan = _plan(["q1"])
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=[])
        self.assertEqual(out["result"]["evidence"], [])
        self.assertEqual(out["status"], "success")


class TestAggregateBudget(unittest.TestCase):
    def test_clip_by_max_evidence_count(self):
        plan = _plan(["q1"], total_budget={"max_evidence_count": 2})
        results = [
            {"query_id": "q1", "skill_result": _skill_result([
                _wrapper("a", 0.9), _wrapper("b", 0.8), _wrapper("c", 0.7),
            ])},
        ]
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=results)
        ev = out["result"]["evidence"]
        self.assertEqual(len(ev), 2)
        # 保留分高的前 2
        self.assertEqual([w["payload"]["evidence_id"] for w in ev], ["a", "b"])
        self.assertEqual(out["status"], "partial")
        codes = [e["code"] for e in out["errors"]]
        self.assertIn("E_OUT_OF_BUDGET", codes)

    def test_clip_by_max_token_estimate(self):
        plan = _plan(["q1"], total_budget={"max_token_estimate": 5})
        # token = len(text)//4; text 长 20 → 5 token 一条就到顶
        results = [
            {"query_id": "q1", "skill_result": _skill_result([
                _wrapper("a", 0.9, text="x" * 20),
                _wrapper("b", 0.8, text="x" * 20),
            ])},
        ]
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=results)
        ev = out["result"]["evidence"]
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0]["payload"]["evidence_id"], "a")
        self.assertEqual(out["status"], "partial")
        self.assertIn("E_OUT_OF_BUDGET", [e["code"] for e in out["errors"]])

    def test_no_clip_when_within_budget(self):
        plan = _plan(["q1"], total_budget={"max_evidence_count": 10})
        results = [
            {"query_id": "q1", "skill_result": _skill_result([
                _wrapper("a", 0.9), _wrapper("b", 0.8),
            ])},
        ]
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=results)
        self.assertEqual(len(out["result"]["evidence"]), 2)
        self.assertEqual(out["status"], "success")
        self.assertEqual(out["errors"], [])

    def test_no_budget_no_clip(self):
        plan = _plan(["q1"])  # total_budget=None
        results = [
            {"query_id": "q1", "skill_result": _skill_result([
                _wrapper(f"e{i}", 0.5) for i in range(50)
            ])},
        ]
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=results)
        self.assertEqual(len(out["result"]["evidence"]), 50)
        self.assertEqual(out["status"], "success")

    def test_combined_count_and_token_clip(self):
        """count 与 token 两个限额同时生效:先截 count 再截 token。"""
        plan = _plan(["q1"], total_budget={"max_evidence_count": 3, "max_token_estimate": 10})
        # 4 条,每条 text 长 20 → 5 token。count 先截到 3 条(a,b,c),
        # 再 token 累计:5(a) → 10(b, 10>10 False 收) → 15(c, 15>10 True 停) → 保留 a,b
        results = [
            {"query_id": "q1", "skill_result": _skill_result([
                _wrapper("a", 0.9, text="x" * 20),
                _wrapper("b", 0.8, text="x" * 20),
                _wrapper("c", 0.7, text="x" * 20),
                _wrapper("d", 0.6, text="x" * 20),
            ])},
        ]
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=results)
        ev = out["result"]["evidence"]
        self.assertEqual([w["payload"]["evidence_id"] for w in ev], ["a", "b"])
        self.assertEqual(out["status"], "partial")
        # clipped = 原 4 - 保留 2 = 2
        codes = [e for e in out["errors"] if e["code"] == "E_OUT_OF_BUDGET"]
        self.assertEqual(len(codes), 1)
        self.assertEqual(codes[0]["detail"]["kept"], 2)
        self.assertEqual(codes[0]["detail"]["clipped"], 2)

    def test_single_oversized_wrapper_yields_zero_kept(self):
        """单条 evidence 的 token 已超 max_token_estimate → 一条都不保留(有意为之)。"""
        plan = _plan(["q1"], total_budget={"max_token_estimate": 3})
        # text 长 20 → 5 token > 3 → running=5 在第一条就 >3, break, 0 保留
        results = [
            {"query_id": "q1", "skill_result": _skill_result([
                _wrapper("big", 0.9, text="x" * 20),
            ])},
        ]
        out = aggregator.Aggregator().aggregate(plan=plan, skill_results=results)
        self.assertEqual(out["result"]["evidence"], [])
        self.assertEqual(out["status"], "partial")
        codes = [e for e in out["errors"] if e["code"] == "E_OUT_OF_BUDGET"]
        self.assertEqual(codes[0]["detail"]["kept"], 0)
        self.assertEqual(codes[0]["detail"]["clipped"], 1)


if __name__ == "__main__":
    unittest.main()
