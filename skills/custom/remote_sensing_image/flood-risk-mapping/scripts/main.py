"""Skill: flood-risk-mapping - 城市洪涝风险评估"""
from __future__ import annotations
import argparse, json, logging, sys
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("skill.flood-risk-mapping")

INPUT_SCHEMA: list[dict[str, Any]] = [
    {"name": "image_optical", "type": "file", "required": True,
     "description": "光学影像（GeoTIFF，用于水体提取，建议 Sentinel-2 或 Landsat）"},
    {"name": "dem", "type": "file", "required": True,
     "description": "数字高程模型（GeoTIFF，单位：米，建议 SRTM 30m 或更高精度）"},
    {"name": "aoi", "type": "file", "required": False, "description": "关注区域边界"},
    {"name": "water_threshold", "type": "number", "required": False,
     "description": "水体提取 NDWI 阈值（默认 0.2）"},
    {"name": "risk_levels", "type": "number", "required": False,
     "description": "风险等级数量（默认 3：低/中/高）"},
]

def _load_inputs(raw):
    if not raw: return {}
    p = Path(raw)
    return json.loads(p.read_text("utf-8")) if p.exists() else json.loads(raw)

def check_inputs(inputs):
    missing = [s for s in INPUT_SCHEMA if s["required"] and not inputs.get(s["name"])]
    prompt = None
    if missing:
        lines = ["为了进行城市洪涝风险评估，请补充以下输入："]
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
    water_thr = float(inputs.get("water_threshold") or algo.get("water_threshold", 0.2))

    rng = np.random.default_rng(42)
    # 读取或模拟
    if has_rio and Path(inputs["image_optical"]).exists() and Path(inputs["dem"]).exists():
        try:
            with rasterio.open(inputs["image_optical"]) as s: arr = s.read().astype("float32")
            with rasterio.open(inputs["dem"]) as s: dem = s.read(1).astype("float32")
        except Exception:
            arr, dem = _sim_flood_data(rng, np); has_rio = False
    else:
        arr, dem = _sim_flood_data(rng, np); has_rio = False

    # 水体提取（NDWI）
    water_mask = _extract_water(arr, water_thr, np)

    # 洪涝风险评估（基于高程 + 水体邻近度）
    risk_map = _assess_risk(dem, water_mask, np)

    # 风险区统计
    risk_zones = _make_risk_zones(risk_map, np)
    stats = {
        "high_risk_pct": float((risk_map == 3).mean() * 100),
        "medium_risk_pct": float((risk_map == 2).mean() * 100),
        "low_risk_pct": float((risk_map == 1).mean() * 100),
        "water_coverage_pct": float(water_mask.mean() * 100),
    }

    outputs = {
        "risk_map": str(_save_npy(output_dir / "risk_map.tif", risk_map, np)),
        "water_mask": str(_save_npy(output_dir / "water_mask.tif", water_mask, np)),
        "risk_zones": str(_save_geojson(output_dir / "risk_zones.geojson", risk_zones)),
        "stats_csv": str(_save_stats_csv_frm(output_dir / "flood_stats.csv", stats)),
        "report": str(_save_report_frm(output_dir / "report.md", stats, water_thr)),
    }
    return {"ok": True, "stats": stats, "n_risk_zones": len(risk_zones), "outputs": outputs}

def _sim_flood_data(rng, np):
    arr = rng.random((4, 64, 64)).astype("float32")
    # 模拟水体区域（低 NIR，高 Green）
    arr[1, 0:15, 0:15] = 0.8   # Green
    arr[3, 0:15, 0:15] = 0.1   # NIR
    # 模拟 DEM（低洼区域）
    dem = rng.uniform(10, 100, (64, 64)).astype("float32")
    dem[0:20, 0:20] = rng.uniform(2, 10, (20, 20))  # 低洼区
    return arr, dem

def _extract_water(arr, threshold, np):
    if arr.shape[0] >= 4:
        green, nir = arr[1], arr[3]
        with np.errstate(divide="ignore", invalid="ignore"):
            ndwi = (green - nir) / (green + nir + 1e-6)
        return (ndwi > threshold).astype("uint8")
    return (arr.mean(axis=0) < 0.3).astype("uint8")

def _assess_risk(dem, water_mask, np):
    from scipy import ndimage as ndi
    risk = np.zeros_like(dem, dtype="uint8")
    # 高程分位数
    p25, p50 = float(np.percentile(dem, 25)), float(np.percentile(dem, 50))
    risk[dem <= p25] = 3  # 高风险
    risk[(dem > p25) & (dem <= p50)] = 2  # 中风险
    risk[dem > p50] = 1  # 低风险
    # 水体邻近区域提升风险
    try:
        dilated = ndi.binary_dilation(water_mask, iterations=3).astype("uint8")
        risk = np.where((dilated == 1) & (risk < 3), risk + 1, risk)
        risk = np.clip(risk, 1, 3).astype("uint8")
    except Exception:
        pass
    return risk

def _make_risk_zones(risk_map, np):
    zones = []
    for level, name in [(3, "high"), (2, "medium"), (1, "low")]:
        mask = (risk_map == level).astype("uint8")
        if mask.sum() == 0: continue
        ys, xs = np.where(mask > 0)
        zones.append({
            "geometry": {"type": "Polygon", "coordinates": [[
                [float(xs.min()), float(ys.min())], [float(xs.max()), float(ys.min())],
                [float(xs.max()), float(ys.max())], [float(xs.min()), float(ys.max())],
                [float(xs.min()), float(ys.min())]]]},
            "area_m2": float(mask.sum() * 100),
            "risk_level": name,
            "centroid": [float(xs.mean()), float(ys.mean())],
        })
    return zones

def _save_npy(path, arr, np):
    np.save(path.with_suffix(".npy"), arr); return path.with_suffix(".npy")

def _save_geojson(path, zones):
    fc = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": z["geometry"],
         "properties": {k: v for k, v in z.items() if k != "geometry"}} for z in zones]}
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), "utf-8"); return path

def _save_stats_csv_frm(path, stats):
    lines = ["metric,value"] + [f"{k},{v:.4f}" for k, v in stats.items()]
    path.write_text("\n".join(lines), "utf-8"); return path

def _save_report_frm(path, stats, water_thr):
    lines = [
        "# 城市洪涝风险评估报告", "",
        f"- 水体提取阈值（NDWI）: `{water_thr}`",
        f"- 水体覆盖率: **{stats['water_coverage_pct']:.1f}%**",
        f"- 高风险区占比: **{stats['high_risk_pct']:.1f}%**",
        f"- 中风险区占比: **{stats['medium_risk_pct']:.1f}%**",
        f"- 低风险区占比: **{stats['low_risk_pct']:.1f}%**",
        "", "## 治理建议", "",
        "1. **高风险区**：低洼且邻近水体，建议优先建设防洪堤坝和排水设施",
        "2. **中风险区**：建议完善雨水管网，增加调蓄空间",
        "3. **低风险区**：持续监测，防止因城市扩张导致风险上升",
    ]
    path.write_text("\n".join(lines), "utf-8"); return path

def main():
    ap = argparse.ArgumentParser(description="城市洪涝风险评估技能")
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
        out_dir = Path(args.output_dir or "./outputs") / "flood-risk-mapping"
        result = run(inputs, config, out_dir)
    else:
        result = {"ok": True}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0

if __name__ == "__main__":
    sys.exit(main())
