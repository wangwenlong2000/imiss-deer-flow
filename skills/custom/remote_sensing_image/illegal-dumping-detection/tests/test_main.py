"""Tests for illegal-dumping-detection skill"""
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
        assert any(m["name"] == "image_optical" for m in result["missing_inputs"])

    def test_provided(self):
        code, result, _ = run_skill("check-inputs", {"image_optical": "/fake.tif"})
        assert result["ok"] is True

class TestRun:
    def test_run_degraded(self, tmp_path):
        code, result, stderr = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        assert code == 0, f"stderr: {stderr}"
        assert result["ok"] is True
        assert "n_suspect_sites" in result
        assert result["n_suspect_sites"] >= 0

    def test_sites_geojson(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        with open(result["outputs"]["suspect_sites"]) as f:
            gj = json.load(f)
        assert gj["type"] == "FeatureCollection"
        # 模拟数据应检测到疑似点位
        assert len(gj["features"]) > 0

    def test_sites_csv(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        csv_path = Path(result["outputs"]["sites_csv"])
        lines = csv_path.read_text("utf-8").splitlines()
        assert lines[0].startswith("id,lon,lat")
        assert len(lines) > 1  # 有数据行

    def test_report_content(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif"}, output_dir=tmp_path)
        content = Path(result["outputs"]["report"]).read_text("utf-8")
        assert "非法倾倒" in content
        assert "优先级" in content
