"""Keep structural feature matching bounded on long unbroken attachments."""

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from deerflow.compliance.detectors.model_tfidf_knn.vendor.model_detectors.feature_extractor import STRUCTURAL_PATTERNS

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("text", ["alpha->beta", "alpha → beta", "甲→乙", " a b -> c", "x->", "->x", "plain text", "x->y->z"])
def test_route_arrow_keeps_existing_boolean_feature(text):
    original = re.compile(r"\S+\s*(?:->|→)\s*\S+")
    assert bool(STRUCTURAL_PATTERNS["has_route_arrow"].search(text)) == bool(original.search(text))


def test_route_arrow_matches_the_original_feature_for_all_annotated_content():
    original = re.compile(r"\S+\s*(?:->|→)\s*\S+")
    paths = sorted((ROOT / "合规检测/合规评测全部十类违规类型数据").glob("*.jsonl"))
    assert len(paths) == 10
    for path in paths:
        for line in path.read_text().splitlines():
            record = json.loads(line)
            text = json.dumps(record.get("features") or record.get("content"), ensure_ascii=False)
            assert bool(STRUCTURAL_PATTERNS["has_route_arrow"].search(text)) == bool(original.search(text)), record.get("data_id")


def test_all_structural_patterns_finish_on_a_sub_limit_repetitive_payload():
    program = """
import time
from deerflow.compliance.detectors.model_tfidf_knn.vendor.model_detectors.feature_extractor import STRUCTURAL_PATTERNS
text = '合成业务材料 ' + 'a' * 199_000
started = time.monotonic()
for pattern in STRUCTURAL_PATTERNS.values():
    pattern.search(text)
assert time.monotonic() - started < 1
"""
    result = subprocess.run([sys.executable, "-c", program], timeout=3, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_real_engine_returns_within_a_bound_for_a_sub_limit_payload():
    program = """
import time
from deerflow.compliance.engine import ComplianceEngine
from deerflow.compliance.normalizers.user_input import UserInputNormalizer
from deerflow.compliance.policy import load_policy_matrix
from deerflow.compliance.registry import build_registry
registry = build_registry(config_path='../config/compliance/detectors.yaml', strict=True)
engine = ComplianceEngine(registry=registry, policy=load_policy_matrix('../config/compliance/policy_matrix.yaml'))
unit = UserInputNormalizer().to_units('合成业务材料 ' + 'a' * 199_000)[0]
started = time.monotonic()
decision = engine.check((unit,), gate='InputGate', budget_ms=400)
assert time.monotonic() - started < 8
assert decision.diagnostics.truncated
assert 'refuse' in decision.actions
registry.close()
"""
    result = subprocess.run([sys.executable, "-c", program], cwd=ROOT / "backend", timeout=10, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
