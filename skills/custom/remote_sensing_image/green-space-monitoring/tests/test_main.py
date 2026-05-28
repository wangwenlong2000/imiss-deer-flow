"""Tests for green-space-monitoring skill"""
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
    def test_missing_both_images(self):
        code, result, _ = run_skill("check-inputs", {})
        assert result["ok"] is False
        assert len(result["missing_inputs"]) == 2

    def test_missing_one_image(self):
        code, result, _ = run_skill("check-inputs", {"image_t1": "/fake.tif"})
        assert result["ok"] is False

    def test_all_provided(self):
        code, result, _ = run_skill("check-inputs",
            {"image_t1": "/fake1.tif", "image_t2": "/fake2.tif"})
        assert result["ok"] is True

class TestRun:
    def test_run_degraded(self, tmp_path):
        code, result, stderr = run_skill("run",
            {"image_t1": "/nonexistent1.tif", "image_t2": "/nonexistent2.tif"},
            output_dir=tmp_path)
        assert code == 0, f"stderr: {stderr}"
        assert result["ok"] is True
        assert "stats" in result
        stats = result["stats"]
        assert "green_area_t1_pct" in stats
        assert "net_change_pct" in stats

    def test_output_files(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_t1": "/nonexistent1.tif", "image_t2": "/nonexistent2.tif"},
            output_dir=tmp_path)
        assert Path(result["outputs"]["report"]).exists()
        assert Path(result["outputs"]["green_loss_zones"]).exists()

    def test_report_content(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_t1": "/nonexistent1.tif", "image_t2": "/nonexistent2.tif"},
            output_dir=tmp_path)
        content = Path(result["outputs"]["report"]).read_text("utf-8")
        assert "绿地" in content
        assert "覆盖率" in content

    def test_evi_index(self, tmp_path):
        code, result, _ = run_skill("run",
            {"image_t1": "/nonexistent1.tif", "image_t2": "/nonexistent2.tif", "index": "evi"},
            output_dir=tmp_path)
        assert result["ok"] is True
        assert result["index"] == "evi"
