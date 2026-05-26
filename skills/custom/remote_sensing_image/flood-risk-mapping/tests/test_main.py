"""Tests for flood-risk-mapping skill"""
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
    def test_missing_both(self):
        code, result, _ = run_skill("check-inputs", {})
        assert result["ok"] is False
        assert len(result["missing_inputs"]) == 2

    def test_missing_dem(self):
        code, result, _ = run_skill("check-inputs", {"image_optical": "/fake.tif"})
        assert result["ok"] is False
        assert any(m["name"] == "dem" for m in result["missing_inputs"])

    def test_all_provided(self):
        code, result, _ = run_skill("check-inputs",
            {"image_optical": "/fake.tif", "dem": "/fake_dem.tif"})
        assert result["ok"] is True

class TestRun:
    def test_run_degraded(self, tmp_path):
        code, result, stderr = run_skill("run",
            {"image_optical": "/nonexistent.tif", "dem": "/nonexistent_dem.tif"},
            output_dir=tmp_path)
        assert code == 0, f"stderr: {stderr}"
        assert result["ok"] is True
        assert "stats" in result
        stats = result["stats"]
        assert "high_risk_pct" in stats
        assert stats["high_risk_pct"] + stats["medium_risk_pct"] + stats["low_risk_pct"] <= 100.1

    def test_risk_zones_geojson(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif", "dem": "/nonexistent_dem.tif"},
            output_dir=tmp_path)
        with open(result["outputs"]["risk_zones"]) as f:
            gj = json.load(f)
        assert gj["type"] == "FeatureCollection"
        # 应有风险分区
        assert len(gj["features"]) > 0

    def test_report_content(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_optical": "/nonexistent.tif", "dem": "/nonexistent_dem.tif"},
            output_dir=tmp_path)
        content = Path(result["outputs"]["report"]).read_text("utf-8")
        assert "洪涝" in content
        assert "风险" in content
