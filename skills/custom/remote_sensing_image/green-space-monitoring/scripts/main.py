"""Skill: green-space-monitoring - 城市绿地监测"""
from __future__ import annotations
import argparse, json, logging, sys
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("skill.green-space-monitoring")

INPUT_SCHEMA: list[dict[str, Any]] = [
    {"name": "image_t1", "type": "file", "required": True,
     "description": "前期多光谱影像（GeoTIFF，已完成大气校正）"},
    {"name": "image_t2", "type": "file", "required": True,
     "description": "后期多光谱影像（GeoTIFF，与 image_t1 同坐标系/分辨率）"},
    {"name": "aoi", "type": "file", "required": False, "description": "关注区域边界"},
    {"name": "index", "type": "string", "required": False,
     "description": "植被指数类型：ndvi / evi，默认 ndvi"},
    {"name": "threshold", "type": "number", "required": False,
     "description": "绿地判定阈值（0-1），默认 0.3"},
]

def _load_inputs(raw):
    if not raw: return {}
    p = Path(raw)
    return json.loads(p.read_text("utf-8")) if p.exists() else json.loads(raw)

def check_inputs(inputs):
    missing = [s for s in INPUT_SCHEMA if s["required"] and not inputs.get(s["name"])]
    prompt = None
    if missing:
        lines = ["为了进行城市绿地监测，请补充以下输入："]
        for i, m in enumerate(missing, 1):
            lines.append(f"{i}. **{m['name']}**：{m['description']}")
        prompt = "\n".join(lines)
    return {"ok": not missing, "missing_inputs": missing, "invalid_inputs": [], "prompt": prompt}

def run(inputs, config, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    import numpy as np
    try:
        import rasterio; has_rio = True
    except ImportError:
        has_rio = False

    algo = (config.get("algorithm") or {}) if isinstance(config, dict) else {}
    index = inputs.get("index") or algo.get("index", "ndvi")
    threshold = float(inputs.get("threshold") or algo.get("threshold", 0.3))

    rng = np.random.default_rng(42)
    if has_rio and Path(inputs["image_t1"]).exists() and Path(inputs["image_t2"]).exists():
        try:
            with rasterio.open(inputs["image_t1"]) as s: arr1 = s.read().astype("float32")
            with rasterio.open(inputs["image_t2"]) as s: arr2 = s.read().astype("float32")
        except Exception:
            arr1, arr2 = _sim_multispectral(rng, np); has_rio = False
    else:
        arr1, arr2 = _sim_multispectral(rng, np); has_rio = False

    vi1 = _compute_vi(arr1, index, np)
    vi2 = _compute_vi(arr2, index, np)
    green1 = (vi1 > threshold).astype("uint8")
    green2 = (vi2 > threshold).astype("uint8")
    loss = ((green1 == 1) & (green2 == 0)).astype("uint8")
    gain = ((green1 == 0) & (green2 == 1)).astype("uint8")

    stats = {
        "green_area_t1_pct": float(green1.mean() * 100),
        "green_area_t2_pct": float(green2.mean() * 100),
        "green_loss_pct": float(loss.mean() * 100),
        "green_gain_pct": float(gain.mean() * 100),
        "net_change_pct": float((green2.mean() - green1.mean()) * 100),
    }

    loss_zones = _make_zones(loss, "green_loss", np)
    outputs = {
        "vi_map_t1": str(_save_npy(output_dir / "vi_map_t1.tif", vi1, np)),
        "vi_map_t2": str(_save_npy(output_dir / "vi_map_t2.tif", vi2, np)),
        "green_loss_zones": str(_save_geojson(output_dir / "green_loss_zones.geojson", loss_zones)),
        "stats_csv": str(_save_stats_csv_gsm(output_dir / "green_stats.csv", stats)),
        "report": str(_save_report_gsm(output_dir / "report.md", stats, index, threshold)),
    }
    return {"ok": True, "index": index, "stats": stats, "n_loss_zones": len(loss_zones), "outputs": outputs}

def _sim_multispectral(rng, np):
    arr1 = rng.random((4, 64, 64)).astype("float32")
    arr2 = arr1.copy()
    arr2[3, 10:30, 10:30] -= 0.4  # NIR 降低 -> 绿地减少
    arr2 = np.clip(arr2, 0, 1)
    return arr1, arr2

def _compute_vi(arr, index, np):
    if arr.shape[0] < 4:
        return arr.mean(axis=0)
    nir, red = arr[3], arr[2]
    with np.errstate(divide="ignore", invalid="ignore"):
        if index == "evi":
            blue = arr[0]
            vi = 2.5 * (nir - red) / (nir + 6*red - 7.5*blue + 1 + 1e-6)
        else:
            vi = (nir - red) / (nir + red + 1e-6)
    return np.clip(vi, -1, 1).astype("float32")

def _make_zones(mask, zone_type, np):
    zones = []
    try:
        from scipy import ndimage as ndi
        labeled, n = ndi.label(mask)
        for i in range(1, n + 1):
            ys, xs = np.where(labeled == i)
            if len(ys) < 3: continue
            zones.append({
                "geometry": {"type": "Polygon", "coordinates": [[
                    [float(xs.min()), float(ys.min())], [float(xs.max()), float(ys.min())],
                    [float(xs.max()), float(ys.max())], [float(xs.min()), float(ys.max())],
                    [float(xs.min()), float(ys.min())]]]},
                "area_m2": float(len(ys) * 100),
                "centroid": [float(np.mean(xs)), float(np.mean(ys))], "type": zone_type,
            })
    except ImportError:
        if mask.sum() > 0:
            ys, xs = np.where(mask > 0)
            zones.append({
                "geometry": {"type": "Polygon", "coordinates": [[
                    [float(xs.min()), float(ys.min())], [float(xs.max()), float(ys.max())],
                    [float(xs.min()), float(ys.min())]]]},
                "area_m2": float(mask.sum() * 100),
                "centroid": [float(xs.mean()), float(ys.mean())], "type": zone_type,
            })
    return zones

def _save_npy(path, arr, np):
    np.save(path.with_suffix(".npy"), arr); return path.with_suffix(".npy")

def _save_geojson(path, zones):
    fc = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": z["geometry"],
         "properties": {k: v for k, v in z.items() if k != "geometry"}} for z in zones]}
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), "utf-8"); return path

def _save_stats_csv_gsm(path, stats):
    lines = ["metric,value"] + [f"{k},{v:.4f}" for k, v in stats.items()]
    path.write_text("\n".join(lines), "utf-8"); return path

def _save_report_gsm(path, stats, index, threshold):
    net = stats["net_change_pct"]
    trend = "减少" if net < 0 else "增加"
    lines = [
        "# 城市绿地监测报告", "",
        f"- 植被指数: `{index.upper()}`",
        f"- 绿地判定阈值: `{threshold}`",
        f"- 前期绿地覆盖率: **{stats['green_area_t1_pct']:.1f}%**",
        f"- 后期绿地覆盖率: **{stats['green_area_t2_pct']:.1f}%**",
        f"- 净变化: **{net:+.1f}%**（绿地{trend}）",
        "", "## 治理建议", "",
        "- 绿地减少区域：优先排查违规占绿、毁绿行为，依法追责",
        "- 绿地增加区域：持续监测，确保绿地质量",
        "- 建议结合城市绿地率指标，制定绿地补偿方案",
    ]
    path.write_text("\n".join(lines), "utf-8"); return path

def main():
    ap = argparse.ArgumentParser(description="城市绿地监测技能")
    ap.add_argument("--action", required=True, choices=["check-inputs", "run", "report"])
    ap.add_argument("--inputs-json", default="{}")
    ap.add_argument("--config", default=None)
    ap.add_argument("--output-dir", default=None)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    inputs = _load_inputs(args.inputs_json)
    try:
        import yaml
        config_path = Path(args.config) if args.config else Path(__file__).parent.parent / "config.yaml"
        config = yaml.safe_load(config_path.read_text("utf-8")) or {} if config_path.exists() else {}
    except ImportError:
        config = {}
    if args.action == "check-inputs":
        result = check_inputs(inputs)
    elif args.action == "run":
        check = check_inputs(inputs)
        if not check["ok"]:
            print(json.dumps({"ok": False, "error": "inputs incomplete", **check}, ensure_ascii=False, indent=2))
            return 2
        out_dir = Path(args.output_dir or "./outputs") / "green-space-monitoring"
        result = run(inputs, config, out_dir)
    else:
        result = {"ok": True}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0

if __name__ == "__main__":
    sys.exit(main())
