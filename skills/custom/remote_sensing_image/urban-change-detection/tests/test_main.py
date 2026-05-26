"""urban-change-detection 技能单元测试。"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SKILL_DIR = Path(__file__).resolve().parent.parent
SCRIPT = SKILL_DIR / "scripts" / "main.py"


def _run(*args):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True, text=True, cwd=str(SKILL_DIR),
    )


# ---------------- check-inputs ----------------
def test_check_inputs_missing_all():
    r = _run("--action", "check-inputs", "--inputs-json", "{}")
    assert r.returncode == 0
    payload = json.loads(r.stdout)
    assert payload["ok"] is False
    names = {m["name"] for m in payload["missing_inputs"]}
    assert {"image_t1", "image_t2"} <= names
    assert "请补充以下输入" in payload["prompt"]


def test_check_inputs_invalid_path(tmp_path):
    inputs = {"image_t1": "/nonexistent/a.tif", "image_t2": "/nonexistent/b.tif"}
    r = _run("--action", "check-inputs", "--inputs-json", json.dumps(inputs))
    payload = json.loads(r.stdout)
    assert payload["ok"] is False
    assert len(payload["invalid_inputs"]) == 2


def test_check_inputs_ok_when_files_exist(tmp_path: Path):
    a = tmp_path / "a.tif"
    b = tmp_path / "b.tif"
    a.write_bytes(b"fake"); b.write_bytes(b"fake")
    r = _run("--action", "check-inputs",
             "--inputs-json", json.dumps({"image_t1": str(a), "image_t2": str(b)}))
    payload = json.loads(r.stdout)
    assert payload["ok"] is True


# ---------------- run (降级模式) ----------------
def test_run_fallback_mode(tmp_path: Path):
    """缺少真实 GeoTIFF 时应走降级随机数据路径，不崩溃。"""
    a = tmp_path / "a.tif"; b = tmp_path / "b.tif"
    a.write_bytes(b"fake"); b.write_bytes(b"fake")
    out = tmp_path / "out"
    r = _run("--action", "run",
             "--inputs-json", json.dumps({"image_t1": str(a), "image_t2": str(b)}),
             "--output-dir", str(out))
    assert r.returncode == 0, r.stderr
    payload = json.loads(r.stdout)
    assert payload["ok"] is True
    assert payload["method"] in ("cva", "pca", "deep")
    assert (out / "urban-change-detection" / "change_patches.geojson").exists()
    assert (out / "urban-change-detection" / "problem_points.csv").exists()
    assert (out / "urban-change-detection" / "report.md").exists()
