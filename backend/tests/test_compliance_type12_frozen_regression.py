"""Pinned assertions for the immutable type 1/2 engine regression."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA = REPO_ROOT / "datasets/compliance/type12_frozen_test"
EXPECTED = {
    "struct_id_test_44.jsonl": (44, "4710a515b4be9f5053dc94e6be21803b6aaef8c13f59b7c803d72315cd6beb66"),
    "geo_loc_test_40.jsonl": (40, "7e5175ac69c3b5f9f0eeb0d741ae2ac1399f8f2618abf1123f7cb2b5159323c8"),
}


def _eval_module():
    path = REPO_ROOT / "scripts/eval_compliance_type12_engine.py"
    spec = importlib.util.spec_from_file_location("type12_frozen_eval", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_frozen_files_are_unmodified():
    for name, (rows, digest) in EXPECTED.items():
        payload = (DATA / name).read_bytes()
        assert len(payload.splitlines()) == rows
        assert hashlib.sha256(payload).hexdigest() == digest


def test_real_engine_matches_legacy_and_preserves_baseline_quality():
    report = _eval_module().evaluate()
    assert report["struct_id"]["samples"] == 44
    assert report["geo_loc"]["samples"] == 40
    for metrics in report.values():
        assert metrics["accuracy"] >= 0.85
        assert metrics["f1"] >= 0.85
        assert metrics["legacy_engine_100pct_consistent"] is True
        assert metrics["mismatches"] == []
