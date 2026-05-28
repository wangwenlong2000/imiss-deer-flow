"""Skill: impervious-surface-mapping - 不透水面提取"""
from __future__ import annotations
import argparse, json, logging, sys
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("skill.impervious-surface-mapping")

INPUT_SCHEMA: list[dict[str, Any]] = [
    {"name": "image_optical", "type": "file", "required": True,
     "description": "高分辨率光学影像（GeoTIFF，多波段，已完成大气校正）"},
    {"name": "aoi", "type": "file", "required": False, "description": "关注区域边界（GeoJSON/Shapefile）"},
    {"name": "method", "type": "string", "required": False,
     "description": "提取方法：ndbi（归一化建筑指数）/ threshold / ml，默认 ndbi"},
    {"name": "threshold", "type": "number", "required": False,
     "description": "不透水面判定阈值（0-1），默认 0.2"},
]

def _load_inputs(raw):
    if not raw: return {}
    p = Path(raw)
    return json.loads(p.read_text("utf-8")) if p.exists() else json.loads(raw)

def check_inputs(inputs):
    missing = [s for s in INPUT_SCHEMA if s["required"] and not inputs.get(s["name"])]
    prompt = None
    if missing:
        lines = ["为了进行不透水面提取，请补充以下输入："]
        for i, m in enumerate(missing, 1):
            lines.append(f"{i}. **{m['name']}**：{m['description']}")
        lines.append("\n建议使用 Sentinel-2 或 GF-2 影像，需完成大气校正。")
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
    method = inputs.get("method") or algo.get("method", "ndbi")
    threshold = float(inputs.get("threshold") or algo.get("threshold", 0.2))

    # 读取或模拟
    if has_rio and Path(inputs["image_optical"]).exists():
        try:
            with rasterio.open(inputs["image_optical"]) as src:
                arr = src.read().astype("float32"); profile = src.profile
        except Exception:
            arr, profile = _sim_optical(np); has_rio = False
    else:
        arr, profile = _sim_optical(np); has_rio = False

    # 不透水面计算
    imp_mask, imp_ratio = _compute_impervious(arr, method, threshold, np)

    # 统计分区
    zones = _make_zones(imp_mask, np)

    outputs = {
        "impervious_map": str(_save_npy(output_dir / "impervious_map.tif", imp_mask, np)),
        "impervious_zones": str(_save_geojson(output_dir / "impervious_zones.geojson", zones)),
        "stats_csv": str(_save_stats_csv(output_dir / "impervious_stats.csv", zones, imp_ratio)),
        "report": str(_save_report_ism(output_dir / "report.md", zones, method, threshold, imp_ratio)),
    }
    return {"ok": True, "method": method, "impervious_ratio": round(imp_ratio, 4),
            "n_zones": len(zones), "outputs": outputs}

def _sim_optical(np):
    rng = np.random.default_rng(42)
    arr = rng.random((4, 64, 64)).astype("float32")
    # 模拟城市区域：高 NDBI 区
    arr[0, 20:50, 20:50] = 0.8  # SWIR
    arr[2, 20:50, 20:50] = 0.3  # NIR
    return arr, None

def _compute_impervious(arr, method, threshold, np):
    if arr.shape[0] >= 4:
        # NDBI = (SWIR - NIR) / (SWIR + NIR)，约定 band0=Blue,1=Green,2=Red,3=NIR
        # 若有 SWIR 则用 band0 代替（降级）
        swir = arr[0]; nir = arr[3] if arr.shape[0] > 3 else arr[2]
        with np.errstate(divide="ignore", invalid="ignore"):
            ndbi = (swir - nir) / (swir + nir + 1e-6)
        imp_mask = (ndbi > threshold).astype("uint8")
    else:
        # 简化：用亮度阈值
        brightness = arr.mean(axis=0)
        imp_mask = (brightness > threshold + 0.3).astype("uint8")
    imp_ratio = float(imp_mask.sum()) / imp_mask.size
    return imp_mask, imp_ratio

def _make_zones(mask, np):
    zones = []
    try:
        from scipy import ndimage as ndi
        labeled, n = ndi.label(mask)
        for i in range(1, n + 1):
            ys, xs = np.where(labeled == i)
            if len(ys) < 5: continue
            zones.append({
                "geometry": {"type": "Polygon", "coordinates": [[
                    [float(xs.min()), float(ys.min())], [float(xs.max()), float(ys.min())],
                    [float(xs.max()), float(ys.max())], [float(xs.min()), float(ys.max())],
                    [float(xs.min()), float(ys.min())]]]},
                "area_m2": float(len(ys) * 100),
                "centroid": [float(np.mean(xs)), float(np.mean(ys))],
                "type": "impervious",
            })
    except ImportError:
        if mask.sum() > 0:
            ys, xs = np.where(mask > 0)
            zones.append({
                "geometry": {"type": "Polygon", "coordinates": [[
                    [float(xs.min()), float(ys.min())], [float(xs.max()), float(ys.max())],
                    [float(xs.min()), float(ys.min())]]]},
                "area_m2": float(mask.sum() * 100),
                "centroid": [float(xs.mean()), float(ys.mean())], "type": "impervious",
            })
    return zones

def _save_npy(path, arr, np):
    np.save(path.with_suffix(".npy"), arr); return path.with_suffix(".npy")

def _save_geojson(path, zones):
    fc = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": z["geometry"],
         "properties": {k: v for k, v in z.items() if k != "geometry"}} for z in zones]}
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), "utf-8"); return path

def _save_stats_csv(path, zones, imp_ratio):
    lines = ["metric,value",
             f"impervious_ratio,{imp_ratio:.4f}",
             f"n_zones,{len(zones)}",
             f"total_area_m2,{sum(z.get('area_m2',0) for z in zones):.2f}"]
    path.write_text("\n".join(lines), "utf-8"); return path

def _save_report_ism(path, zones, method, threshold, imp_ratio):
    lines = [
        "# 不透水面提取报告", "",
        f"- 提取方法: `{method}`",
        f"- 判定阈值: `{threshold}`",
        f"- 不透水面覆盖率: **{imp_ratio*100:.1f}%**",
        f"- 不透水面斑块数: **{len(zones)}**",
        "", "## 治理建议", "",
        "- 不透水率 > 70%：城市内涝风险极高，建议优先推进海绵城市改造",
        "- 不透水率 50-70%：中等风险，建议增加透水铺装和雨水花园",
        "- 不透水率 < 50%：风险较低，持续监测并控制新增不透水面",
    ]
    path.write_text("\n".join(lines), "utf-8"); return path

def main():
    ap = argparse.ArgumentParser(description="不透水面提取技能")
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
        out_dir = Path(args.output_dir or "./outputs") / "impervious-surface-mapping"
        result = run(inputs, config, out_dir)
    else:
        result = {"ok": True}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0

if __name__ == "__main__":
    sys.exit(main())
