"""disaster-risk-assessment 技能测试。"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

SKILL_DIR = Path(__file__).resolve().parent.parent
SCRIPT = SKILL_DIR / "scripts" / "main.py"


def _run(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *args],
                          capture_output=True, text=True, cwd=str(SKILL_DIR))


def test_check_inputs_missing():
    r = _run("--action", "check-inputs", "--inputs-json", "{}")
    p = json.loads(r.stdout)
    assert p["ok"] is False
    assert {"hazard_type", "dem", "lulc"} <= {m["name"] for m in p["missing_inputs"]}


def test_check_inputs_invalid_hazard(tmp_path: Path):
    dem = tmp_path / "dem.tif"; lulc = tmp_path / "lulc.tif"
    dem.write_bytes(b"x"); lulc.write_bytes(b"x")
    r = _run("--action", "check-inputs",
             "--inputs-json", json.dumps({"hazard_type": "tsunami", "dem": str(dem), "lulc": str(lulc)}))
    p = json.loads(r.stdout)
    assert p["ok"] is False
    assert any("hazard_type" in str(x) for x in p["invalid_inputs"])


@pytest.mark.parametrize("hazard", ["flood", "landslide", "wildfire"])
def test_run_each_hazard(tmp_path: Path, hazard):
    dem = tmp_path / "dem.tif"; lulc = tmp_path / "lulc.tif"
    dem.write_bytes(b"x"); lulc.write_bytes(b"x")
    out = tmp_path / "out"
    r = _run("--action", "run",
             "--inputs-json", json.dumps({"hazard_type": hazard, "dem": str(dem), "lulc": str(lulc)}),
             "--output-dir", str(out))
    assert r.returncode == 0, r.stderr
    payload = json.loads(r.stdout)
    assert payload["ok"] is True
    assert payload["hazard_type"] == hazard
    assert (out / "disaster-risk-assessment" / "report.md").exists()
