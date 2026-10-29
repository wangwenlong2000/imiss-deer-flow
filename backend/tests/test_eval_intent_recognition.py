"""Evaluation must penalize extra scenes and count failures in its denominator."""

import importlib.util
from pathlib import Path

import pytest

_path = Path(__file__).resolve().parents[2] / "scripts" / "eval_intent_recognition.py"
_spec = importlib.util.spec_from_file_location("intent_evaluation", _path)
evaluation = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(evaluation)


def test_extra_scene_is_a_failure_even_when_expected_scene_is_present():
    case = {"expected_scenes": ["policy_regulation"], "expected_intent": "task"}
    result = {"scenes": ["policy_regulation", "network_traffic"], "intent": "task"}
    scored = evaluation.score(case, result, "new_task")
    assert scored["correct"] is False
    assert scored["scene_fp"] == 1


def test_summary_keeps_failed_cases_and_accepts_an_empty_reason():
    case = {"expected_scenes": [], "expected_intent": "chitchat"}
    result = {"scenes": [], "intent": "chitchat", "reason": None}
    rows = [
        {"category": "control", "result": result, "act": "chitchat", "seconds": 0.1, "calls": 0, "score": evaluation.score(case, result, "chitchat")},
        {"category": "single", "seconds": 240, "calls": 1, "error": "TimeoutError"},
    ]
    summary = evaluation.summarize(rows)
    assert summary["accuracy"] == 0.5
    assert summary["count"] == 2
    assert summary["errors"] == 1
    assert summary["p95_seconds"] == 240


def test_default_evaluation_targets_the_public_optimized_entry():
    from deerflow.routing import intent

    assert evaluation.load_classifier(None) is intent


def test_clarification_is_scored_separately_from_an_empty_scene():
    case = {"expected_scenes": [], "expected_intent": "task", "expected_clarification": True}
    result = {"scenes": [], "intent": "task"}
    assert evaluation.score(case, result, "new_task")["correct"] is False
    result["clarification_question"] = "请指定数据对象"
    assert evaluation.score(case, result, "new_task")["correct"] is True


def test_business_plan_metrics_accept_scene_named_categories():
    result = {"scenes": ["video_surveillance"], "intent": "task", "planning_steps": [{"text": "核验视频"}]}
    case = {"expected_scenes": ["video_surveillance"], "expected_intent": "task"}
    row = {"category": "video_surveillance", "act": "new_task", "result": result, "score": evaluation.score(case, result, "new_task"), "calls": 1, "seconds": 1, "planner_forwarded": True}
    summary = evaluation.summarize([row])
    assert summary["business_cases"] == summary["plans_generated"] == summary["plans_forwarded"] == 1


@pytest.mark.parametrize("file", ["intent_eval_cases.json", "intent_eval_challenge_cases.json"])
def test_frozen_case_ids_are_unique(file):
    import json

    cases = json.loads(_path.with_name(file).read_text())["cases"]
    assert len({c["id"] for c in cases}) == len(cases)
    assert all(c["expected_intent"] for c in cases)
