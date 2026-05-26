"""urban-expansion-analysis 技能测试。"""
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
    assert {"classified_images", "years"} <= {m["name"] for m in p["missing_inputs"]}


def test_check_inputs_length_mismatch(tmp_path: Path):
    a = tmp_path / "a.tif"; b = tmp_path / "b.tif"
    a.write_bytes(b"x"); b.write_bytes(b"x")
    r = _run("--action", "check-inputs",
             "--inputs-json", json.dumps({"classified_images": [str(a), str(b)], "years": [2020]}))
    p = json.loads(r.stdout)
    assert p["ok"] is False
    assert any("years" in str(v) for v in p["invalid_inputs"])


def test_run_fallback(tmp_path: Path):
    imgs = []
    for y in (2015, 2020, 2025):
        f = tmp_path / f"lulc_{y}.tif"
        f.write_bytes(b"x")
        imgs.append(str(f))
    out = tmp_path / "out"
    r = _run("--action", "run",
             "--inputs-json", json.dumps({"classified_images": imgs, "years": [2015, 2020, 2025], "urban_classes": [1]}),
             "--output-dir", str(out))
    assert r.returncode == 0, r.stderr
    payload = json.loads(r.stdout)
    assert payload["ok"] is True
    assert "expansion_intensity" in payload
    assert len(payload["urban_area_m2"]) == 3
    assert (out / "urban-expansion-analysis" / "transition_matrix.csv").exists()
    assert (out / "urban-expansion-analysis" / "report.md").exists()
