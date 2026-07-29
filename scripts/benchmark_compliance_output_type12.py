#!/usr/bin/env python3
"""Repeatable type 1/2 OutputGate latency benchmark (no remote model)."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from collections.abc import Iterator
from pathlib import Path

from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from deerflow.agents.middlewares.compliance_output_gate_middleware import ComplianceOutputGateMiddleware
from deerflow.compliance.audit import NullAuditor
from deerflow.compliance.engine import ComplianceEngine
from deerflow.compliance.policy import load_policy_matrix
from deerflow.compliance.registry import build_registry
from deerflow.compliance.scene import TrustedSceneResolver
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config

ROOT = Path(__file__).resolve().parents[1]
CASES = {
    "struct_id": "用户手机号为13800138000。",
    "geo_loc": "用户位置为lat=31.230416, lon=121.473701。",
    "combined": "用户手机号为13800138000，精确位置为lat=31.230416, lon=121.473701。",
}


def _summary(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    p95_index = max(0, min(len(ordered) - 1, int(len(ordered) * 0.95 + 0.999999) - 1))
    return {
        "p50_ms": round(statistics.median(ordered), 3),
        "p95_ms": round(ordered[p95_index], 3),
        "max_ms": round(max(ordered), 3),
    }


def _state(scene: str) -> dict:
    return {
        "scene_context": {
            "scene": scene,
            "confidence": 0.99,
            "source": "server_resolver",
            "reason_codes": ["benchmark"],
            "fallback": False,
            "permission_checked": True,
            "request_id": f"benchmark-{scene}",
        },
        "compliance_request": {
            "request_id": f"benchmark-{scene}",
            "actor": {
                "user_id": "benchmark-user",
                "org_id": "benchmark-org",
                "department_id": "benchmark-dept",
                "roles": ["analyst"],
                "permissions": ["benchmark.read"],
            },
            "operation": {
                "type": "public_release" if scene == "public_release" else "analysis",
                "target_audience": "public" if scene == "public_release" else "organization",
            },
            "resource": {
                "resource_id": "benchmark-resource",
                "owner_user_id": "benchmark-user",
                "owner_org_id": "benchmark-org",
                "owner_department_id": "benchmark-dept",
                "classification": "confidential",
                "is_anonymized": False,
            },
        },
    }


def _fake(text: str) -> GenericFakeChatModel:
    def messages() -> Iterator[AIMessage]:
        while True:
            yield AIMessage(content=text, id="benchmark-output")

    return GenericFakeChatModel(messages=messages())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=30)
    args = parser.parse_args()
    iterations = max(args.iterations, 5)

    original = get_compliance_config()
    set_compliance_config(ComplianceConfig(enabled=True))
    registry = build_registry(config_path=ROOT / "config/compliance/detectors.yaml", strict=True)
    model = registry.get("model_tfidf_knn")
    if model is not None:
        model.enabled = False
    engine = ComplianceEngine(
        registry=registry,
        policy=load_policy_matrix(ROOT / "config/compliance/policy_matrix.yaml"),
        auditor=NullAuditor(),
        scene_resolver=TrustedSceneResolver(),
    )

    try:
        detection: dict[str, dict[str, float]] = {}
        for name, text in CASES.items():
            samples = []
            gate = ComplianceOutputGateMiddleware(engine=engine)
            for index in range(iterations):
                began = time.perf_counter()
                gate._check(
                    text,
                    AIMessage(content=text, id=f"detect-{name}-{index}"),
                    f"benchmark-{name}-{index}",
                    state=_state("public_release"),
                )
                samples.append((time.perf_counter() - began) * 1000)
            detection[name] = _summary(samples)

        first_event: dict[str, dict[str, float]] = {}
        for scene in ("public_release", "internal_org"):
            samples = []
            for index in range(iterations):
                agent = create_agent(
                    model=_fake(CASES["combined"]),
                    tools=[],
                    middleware=[ComplianceOutputGateMiddleware(engine=engine)],
                )
                graph_input = {
                    "messages": [{"role": "user", "content": "benchmark"}],
                    **_state(scene),
                }
                began = time.perf_counter()
                stream = agent.stream(graph_input, stream_mode=["messages", "updates"])
                next(stream)
                samples.append((time.perf_counter() - began) * 1000)
            first_event[scene] = _summary(samples)

        print(
            json.dumps(
                {
                    "iterations": iterations,
                    "output_gate_detection": detection,
                    "first_sse_event": first_event,
                    "model": "GenericFakeChatModel (deterministic fixture)",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        registry.close()
        set_compliance_config(original)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
