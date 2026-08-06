"""Tests for impervious-surface-mapping skill"""
import json, subprocess, sys, tempfile
from pathlib import Path
import pytest

SCRIPT = Path(__file__).parent.parent / "scripts" / "main.py"

def run_skill(action, inputs=None, output_dir=None):
    cmd = [sys.executable, str(SCRIPT), "--action", action,
           "--inputs-json", json.dumps(inputs or {})]
    if output_dir:
        cmd += ["--output-dir", str(output_dir)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.returncode, json.loads(r.stdout) if r.stdout.strip() else {}, r.stderr

class TestCheckInputs:
    def test_missing_image(self):
        code, result, _ = run_skill("check-inputs", {})
        assert result["ok"] is False
        assert any(m["name"] == "image_optical" for m in result["missing_inputs"])

    def test_provided_image(self):
        code, result, _ = run_skill("check-inputs", {"image_optical": "/fake.tif"})
        assert result["ok"] is True

class TestRun:
    def test_run_degraded(self, tmp_path):
        code, result, stderr = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        assert code == 0, f"stderr: {stderr}"
        assert result["ok"] is True
        assert "impervious_ratio" in result
        assert 0.0 <= result["impervious_ratio"] <= 1.0

    def test_output_files_exist(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        assert Path(result["outputs"]["report"]).exists()
        assert Path(result["outputs"]["impervious_zones"]).exists()

    def test_geojson_valid(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        with open(result["outputs"]["impervious_zones"]) as f:
            gj = json.load(f)
        assert gj["type"] == "FeatureCollection"

    def test_report_contains_ratio(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        content = Path(result["outputs"]["report"]).read_text("utf-8")
        assert "不透水面" in content
