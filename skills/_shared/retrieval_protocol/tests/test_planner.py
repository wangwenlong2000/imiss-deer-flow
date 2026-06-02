"""planner.py 单元测试 —— split_budget / build_plan。"""

from __future__ import annotations

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from retrieval_protocol import planner  # noqa: E402


class TestSplitBudget(unittest.TestCase):
    def test_equal_split_both_fields(self):
        total = {"max_evidence_count": 30, "max_token_estimate": 9000}
        parts = planner.split_budget(total, 3)
        self.assertEqual(len(parts), 3)
        self.assertEqual(sum(p["max_evidence_count"] for p in parts), 30)
        self.assertEqual(sum(p["max_token_estimate"] for p in parts), 9000)
        # 等分:每份 10 / 3000
        self.assertEqual([p["max_evidence_count"] for p in parts], [10, 10, 10])

    def test_remainder_goes_to_first(self):
        total = {"max_evidence_count": 10}
        parts = planner.split_budget(total, 3)
        # 10 // 3 = 3, 余 1 给第一个 → [4, 3, 3]
        self.assertEqual([p["max_evidence_count"] for p in parts], [4, 3, 3])
        self.assertEqual(sum(p["max_evidence_count"] for p in parts), 10)

    def test_weighted_split(self):
        total = {"max_evidence_count": 100}
        parts = planner.split_budget(total, 2, weights=[3.0, 1.0])
        # 75 / 25
        self.assertEqual(sum(p["max_evidence_count"] for p in parts), 100)
        self.assertGreater(parts[0]["max_evidence_count"], parts[1]["max_evidence_count"])
        self.assertEqual(parts[0]["max_evidence_count"], 75)
        self.assertEqual(parts[1]["max_evidence_count"], 25)

    def test_single_field_only(self):
        total = {"max_token_estimate": 8000}
        parts = planner.split_budget(total, 4)
        self.assertEqual(sum(p["max_token_estimate"] for p in parts), 8000)
        self.assertTrue(all("max_evidence_count" not in p for p in parts))

    def test_fan_out_one_returns_total(self):
        total = {"max_evidence_count": 7, "max_token_estimate": 100}
        parts = planner.split_budget(total, 1)
        self.assertEqual(parts, [{"max_evidence_count": 7, "max_token_estimate": 100}])

    def test_invalid_fan_out_raises(self):
        with self.assertRaises(ValueError):
            planner.split_budget({"max_evidence_count": 5}, 0)

    def test_weights_length_mismatch_raises(self):
        with self.assertRaises(ValueError):
            planner.split_budget({"max_evidence_count": 5}, 3, weights=[1.0, 1.0])

    def test_negative_total_value_raises(self):
        with self.assertRaises(ValueError):
            planner.split_budget({"max_evidence_count": -5}, 2)

    def test_weighted_remainder_sum_preserved(self):
        # weights=[3,1] total=10 → int(7.5)=7, int(2.5)=2 → [7,2] sum 9, 余1补首 → [8,2]
        parts = planner.split_budget({"max_evidence_count": 10}, 2, weights=[3.0, 1.0])
        self.assertEqual([p["max_evidence_count"] for p in parts], [8, 2])
        self.assertEqual(sum(p["max_evidence_count"] for p in parts), 10)


class TestBuildPlan(unittest.TestCase):
    def _env(self, q):
        return {"input": {"parameters": {"query": q, "data_type": "netflow"}}}

    def test_plan_without_budget(self):
        plan = planner.build_plan(
            parent_query_id="root",
            subqueries=[
                {"query_id": "q1", "envelope": self._env("a")},
                {"query_id": "q2", "envelope": self._env("b")},
            ],
        )
        self.assertEqual(plan.parent_query_id, "root")
        self.assertEqual([t.query_id for t in plan.tasks], ["q1", "q2"])
        self.assertIsNone(plan.total_budget)
        # 无 total_budget 时不注入 envelope.budget
        self.assertNotIn("budget", plan.tasks[0].envelope)

    def test_plan_injects_split_budget(self):
        plan = planner.build_plan(
            parent_query_id="root",
            subqueries=[
                {"query_id": "q1", "envelope": self._env("a")},
                {"query_id": "q2", "envelope": self._env("b")},
            ],
            total_budget={"max_evidence_count": 10},
        )
        b1 = plan.tasks[0].envelope["budget"]
        b2 = plan.tasks[1].envelope["budget"]
        self.assertEqual(b1["max_evidence_count"] + b2["max_evidence_count"], 10)

    def test_plan_weighted_budget(self):
        plan = planner.build_plan(
            parent_query_id="root",
            subqueries=[
                {"query_id": "q1", "envelope": self._env("a"), "weight": 3.0},
                {"query_id": "q2", "envelope": self._env("b"), "weight": 1.0},
            ],
            total_budget={"max_evidence_count": 100},
        )
        self.assertEqual(plan.tasks[0].envelope["budget"]["max_evidence_count"], 75)
        self.assertEqual(plan.tasks[1].envelope["budget"]["max_evidence_count"], 25)

    def test_parallel_group_default_and_explicit(self):
        plan = planner.build_plan(
            parent_query_id="root",
            subqueries=[
                {"query_id": "q1", "envelope": self._env("a")},
                {"query_id": "q2", "envelope": self._env("b"), "parallel_group": 1},
            ],
        )
        self.assertEqual(plan.tasks[0].parallel_group, 0)
        self.assertEqual(plan.tasks[1].parallel_group, 1)

    def test_empty_subqueries_raises(self):
        with self.assertRaises(ValueError):
            planner.build_plan(parent_query_id="root", subqueries=[])

    def test_does_not_mutate_input_envelope(self):
        env = self._env("a")
        planner.build_plan(
            parent_query_id="root",
            subqueries=[{"query_id": "q1", "envelope": env}],
            total_budget={"max_evidence_count": 5},
        )
        # 原始入参 envelope 不应被注入 budget(build_plan 应深拷贝)
        self.assertNotIn("budget", env)

    def test_deep_copies_nested_envelope(self):
        """build_plan 须深拷贝:task.envelope 的嵌套对象与入参不是同一引用。

        仅断言顶层 budget 不泄漏不足以区分深 / 浅拷贝(浅拷贝也能过),
        本例直接验证嵌套 dict 的 identity 已断开。
        """
        env = self._env("a")
        plan = planner.build_plan(
            parent_query_id="root",
            subqueries=[{"query_id": "q1", "envelope": env}],
        )
        task_env = plan.tasks[0].envelope
        self.assertIsNot(task_env, env)
        self.assertIsNot(task_env["input"], env["input"])
        self.assertIsNot(task_env["input"]["parameters"], env["input"]["parameters"])
        # 改 task 侧嵌套值不应回写入参
        task_env["input"]["parameters"]["query"] = "MUTATED"
        self.assertEqual(env["input"]["parameters"]["query"], "a")


if __name__ == "__main__":
    unittest.main()
