"""urban-heat-assessment 技能测试。"""
import json
import subprocess
import sys
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent.parent
SCRIPT = SKILL_DIR / "scripts" / "main.py"


def _run(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *args],
                          capture_output=True, text=True, cwd=str(SKILL_DIR))


def test_check_inputs_missing():
    r = _run("--action", "check-inputs", "--inputs-json", "{}")
    payload = json.loads(r.stdout)
    assert payload["ok"] is False
    names = {m["name"] for m in payload["missing_inputs"]}
    assert {"thermal_image", "boundary"} <= names


def test_check_inputs_ok(tmp_path: Path):
    t = tmp_path / "t.tif"; b = tmp_path / "b.geojson"
    t.write_bytes(b"fake"); b.write_text('{"type":"FeatureCollection","features":[]}')
    r = _run("--action", "check-inputs",
             "--inputs-json", json.dumps({"thermal_image": str(t), "boundary": str(b)}))
    assert json.loads(r.stdout)["ok"] is True


def test_run_fallback(tmp_path: Path):
    t = tmp_path / "t.tif"; b = tmp_path / "b.geojson"
    t.write_bytes(b"fake"); b.write_text('{"type":"FeatureCollection","features":[]}')
    out = tmp_path / "o"
    r = _run("--action", "run",
             "--inputs-json", json.dumps({"thermal_image": str(t), "boundary": str(b)}),
             "--output-dir", str(out))
    assert r.returncode == 0, r.stderr
    payload = json.loads(r.stdout)
    assert payload["ok"] is True
    assert "lst_stats" in payload
    assert (out / "urban-heat-assessment" / "report.md").exists()
