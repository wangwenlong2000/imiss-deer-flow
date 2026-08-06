"""Tests for road-damage-assessment skill"""
import json, subprocess, sys
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

    def test_provided(self):
        code, result, _ = run_skill("check-inputs", {"image_optical": "/fake.tif"})
        assert result["ok"] is True

class TestRun:
    def test_run_degraded(self, tmp_path):
        code, result, stderr = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        assert code == 0, f"stderr: {stderr}"
        assert result["ok"] is True
        assert "overall_health_score" in result
        assert 0 <= result["overall_health_score"] <= 100

    def test_damage_segments_geojson(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        with open(result["outputs"]["damage_segments"]) as f:
            gj = json.load(f)
        assert gj["type"] == "FeatureCollection"
        assert len(gj["features"]) > 0

    def test_segments_csv(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        csv_path = Path(result["outputs"]["segments_csv"])
        lines = csv_path.read_text("utf-8").splitlines()
        assert "severity" in lines[0]

    def test_report_health_score(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        content = Path(result["outputs"]["report"]).read_text("utf-8")
        assert "健康评分" in content
        assert "破损" in content
