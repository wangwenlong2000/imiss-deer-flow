#!/usr/bin/env python3
"""Compare intent implementations using frozen labels and the configured Qwen.

Runs classifiers only: no skills, business tools, or platform requests.
Example (from the backend environment):
  python ../scripts/eval_intent_recognition.py --output /tmp/intent.jsonl
Use --classifier-file to evaluate a saved classifier without changing live code.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import logging
import math
import statistics
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend" / "packages" / "harness"))
sys.path.insert(0, str(ROOT / "backend"))


class MeasuredModel:
    def __init__(self, model: Any, calls: list[dict]):
        self.model = model
        self.calls = calls

    def with_config(self, config):
        return MeasuredModel(self.model.with_config(config), self.calls)

    def bind(self, **kwargs):
        return MeasuredModel(self.model.bind(**kwargs), self.calls)

    def _start(self):
        call = {"started": time.perf_counter()}
        self.calls.append(call)
        return call

    @staticmethod
    def _finish(call, response):
        call["seconds"] = time.perf_counter() - call.pop("started")
        call["usage"] = getattr(response, "usage_metadata", None)
        call["response"] = getattr(response, "content", None)
        return response

    def invoke(self, *args, **kwargs):
        call = self._start()
        return self._finish(call, self.model.invoke(*args, **kwargs))

    async def ainvoke(self, *args, **kwargs):
        call = self._start()
        return self._finish(call, await self.model.ainvoke(*args, **kwargs))


def load_classifier(path: str | None):
    from deerflow.routing import intent

    if not path:
        return intent
    spec = importlib.util.spec_from_file_location("intent_eval_snapshot", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def score(case: dict, result: dict, act: str) -> dict:
    scenes = set(
        result.get("scenes") or ([result["scene"]] if result.get("scene") else [])
    )
    expected = set(case["expected_scenes"])
    exact_scene = scenes == expected
    exact_intent = result["intent"] == case["expected_intent"]
    exact_act = act == case.get("expected_act", act)
    clarification_checks = []
    if "expected_clarification" in case:
        clarification_checks.append(bool(result.get("clarification_question")) == case["expected_clarification"])
    slots = []
    for scene, params in case.get("expected_params", {}).items():
        tasks = [t for t in result.get("scene_tasks", []) if t["scene"] == scene]
        actual = tasks[0].get("params", {}) if tasks else result.get("params", {})
        for key, alternatives in params.items():
            # Alternatives allow equivalent surface forms; not model-based grading.
            slots.append(
                any(
                    v.casefold() in str(actual.get(key, "")).casefold()
                    for v in alternatives
                )
            )
    return {
        "scene_exact": exact_scene,
        "intent_exact": exact_intent,
        "act_exact": exact_act,
        "correct": exact_scene and exact_intent and exact_act and all(clarification_checks),
        "scene_tp": len(scenes & expected),
        "scene_fp": len(scenes - expected),
        "scene_fn": len(expected - scenes),
        "slot_checks": slots,
        "clarification_checks": clarification_checks,
    }


def summarize(rows: list[dict]) -> dict:
    good = [r for r in rows if "score" in r]
    seconds = sorted(r["seconds"] for r in rows)
    slots = [s for r in good for s in r["score"]["slot_checks"]]
    clarifications = [s for r in good for s in r["score"].get("clarification_checks", [])]
    tp = sum(r["score"]["scene_tp"] for r in good)
    fp = sum(r["score"]["scene_fp"] for r in good)
    fn = sum(r["score"]["scene_fn"] for r in good)
    business = [
        r
        for r in good
        if r["result"].get("intent") in {"task", "explicit_skill_request"}
        and r["act"] == "new_task"
    ]
    return {
        "count": len(rows),
        "correct": sum(r["score"]["correct"] for r in good),
        "accuracy": sum(r["score"]["correct"] for r in good) / len(rows),
        "scene_exact": sum(r["score"]["scene_exact"] for r in good) / len(rows),
        "scene_micro_f1": 2 * tp / (2 * tp + fp + fn) if tp + fp + fn else 1.0,
        "slot_correct": sum(slots),
        "slot_total": len(slots),
        "clarification_correct": sum(clarifications),
        "clarification_total": len(clarifications),
        "mean_calls": statistics.mean(r["calls"] for r in rows),
        "mean_seconds": statistics.mean(seconds),
        "median_seconds": statistics.median(seconds),
        "p95_seconds": seconds[max(0, math.ceil(len(seconds) * 0.95) - 1)],
        "business_cases": len(business),
        "plans_generated": sum(
            bool(r["result"].get("planning_steps")) for r in business
        ),
        "plans_forwarded": sum(r.get("planner_forwarded", False) for r in business),
        "deterministic_fallbacks": sum(
            (r["result"].get("reason") or "").startswith("structured_fallback")
            for r in good
        ),
        "total_calls": sum(r["calls"] for r in rows),
        "errors": len(rows) - len(good),
    }


async def run(args):
    from deerflow.agents.middlewares.intent_recognition_middleware import (
        IntentRecognitionMiddleware,
    )
    from deerflow.models import create_chat_model
    from deerflow.routing.dialogue_act import classify_dialogue_act
    from deerflow.routing.intent import RoutingIntentResult, load_scene_templates

    classifier = load_classifier(args.classifier_file)
    templates = load_scene_templates()
    data = Path(args.cases).read_bytes()
    cases = json.loads(data)["cases"]
    if args.split != "all":
        cases = [c for c in cases if c["split"] == args.split]
    if args.ids:
        cases = [c for c in cases if c["id"] in args.ids.split(",")]
    if args.limit:
        cases = cases[: args.limit]
    if not cases:
        raise ValueError("No cases selected")
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite {output}")
    model = create_chat_model(name="qwen3.6-35b-a3b", thinking_enabled=False)
    assert model.model_name == "qwen3.6-35b-a3b"
    assert model.extra_body["enable_thinking"] is False
    # Same bounded transport for both runs; report timeouts as errors, not omissions.
    model.request_timeout = 60
    model.max_retries = 0
    semaphore = asyncio.Semaphore(args.concurrency)

    async def one(case):
        async with semaphore:
            calls = []
            llm = MeasuredModel(model, calls)
            start = time.perf_counter()
            row = {
                "id": case["id"],
                "split": case["split"],
                "category": case["category"],
            }
            try:
                async with asyncio.timeout(240):
                    dialogue = await asyncio.to_thread(
                        classify_dialogue_act,
                        case["query"],
                        llm=llm,
                        previous_intent_context=case.get("previous_intent"),
                        pending_action=case.get("pending_action"),
                    )
                    if dialogue.act in {
                        "clarification_answer",
                        "parameter_update",
                        "task_followup",
                    }:
                        result = (
                            IntentRecognitionMiddleware._resume_intent_from_dialogue(
                                case["query"], dialogue.resume_intent_context
                            )
                        )
                    elif dialogue.act == "chitchat":
                        result = RoutingIntentResult(
                            intent="chitchat",
                            original_query=case["query"],
                            normalized_query=case["query"],
                            routing_query=case["query"],
                        )
                    else:
                        result = await classifier.aclassify_routing_intent_with_llm(
                            case["query"],
                            llm=llm,
                            scene_templates=templates,
                            previous_intent=case.get("previous_intent"),
                            uploaded_files=case.get("uploaded_files"),
                            available_skill_ids=case.get("available_skill_ids"),
                        )
                    row.update(result=result.model_dump(), act=dialogue.act)
                    row["score"] = score(case, row["result"], dialogue.act)
                    if (
                        result.planning_steps
                        and not case.get("pending_action")
                        and dialogue.act != "task_followup"
                    ):
                        from app.gateway.routers.mobile import adapt_events_to_mobile
                        from deerflow.agents.middlewares.todo_middleware import (
                            _build_routing_guidance_message,
                        )

                        msg = _build_routing_guidance_message(
                            {
                                "intent_context": row["result"],
                                "routing_context": {"route_mode": "scene_filter"},
                            }
                        )
                        adapted = adapt_events_to_mobile(
                            "eval",
                            "eval",
                            [
                                {
                                    "event": "values",
                                    "data": {"messages": [msg.model_dump()]},
                                }
                            ],
                            case["query"],
                        )
                        planner = [
                            s for s in adapted["segments"] if s["agent_id"] == "planner"
                        ]
                        row["planner_forwarded"] = len(planner) == 1 and all(
                            p.text in planner[0]["content"]
                            for p in result.planning_steps
                        )
            except Exception as exc:
                row["error"] = type(exc).__name__
            row.update(
                seconds=time.perf_counter() - start,
                calls=len(calls),
                call_details=calls,
            )
            with output.open("a") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            print(
                json.dumps(
                    {
                        k: row.get(k)
                        for k in ["id", "seconds", "calls", "score", "error"]
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            return row

    rows = await asyncio.gather(*(one(c) for c in cases))
    summary = {
        "model": model.model_name,
        "cases_sha256": hashlib.sha256(data).hexdigest(),
        "concurrency": args.concurrency,
        "overall": summarize(rows),
    }
    for split in {r["split"] for r in rows}:
        summary[split] = summarize([r for r in rows if r["split"] == split])
    for category in {r["category"] for r in rows}:
        summary[category] = summarize([r for r in rows if r["category"] == category])
    output.with_suffix(".summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cases", default=str(Path(__file__).with_name("intent_eval_cases.json"))
    )
    parser.add_argument("--classifier-file")
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--split", choices=["all", "development", "validation"], default="all"
    )
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--ids")
    options = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(run(options))
