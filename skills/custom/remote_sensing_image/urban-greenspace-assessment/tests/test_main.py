"""urban-greenspace-assessment 技能测试。"""
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
    p = json.loads(r.stdout)
    assert p["ok"] is False
    assert {"rs_image", "boundary"} <= {m["name"] for m in p["missing_inputs"]}


def test_run_fallback(tmp_path: Path):
    img = tmp_path / "img.tif"; b = tmp_path / "b.geojson"
    img.write_bytes(b"fake"); b.write_text('{"type":"FeatureCollection","features":[]}')
    out = tmp_path / "o"
    r = _run("--action", "run",
             "--inputs-json", json.dumps({"rs_image": str(img), "boundary": str(b)}),
             "--output-dir", str(out))
    assert r.returncode == 0, r.stderr
    payload = json.loads(r.stdout)
    assert payload["ok"] is True
    assert "green_coverage_pct" in payload
    assert (out / "urban-greenspace-assessment" / "metrics.json").exists()
    assert (out / "urban-greenspace-assessment" / "report.md").exists()
