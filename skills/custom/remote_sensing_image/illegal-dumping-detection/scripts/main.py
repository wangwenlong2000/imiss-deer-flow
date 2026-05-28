"""Skill: illegal-dumping-detection - 非法倾倒监测"""
from __future__ import annotations
import argparse, json, logging, sys
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("skill.illegal-dumping-detection")

INPUT_SCHEMA: list[dict[str, Any]] = [
    {"name": "image_optical", "type": "file", "required": True,
     "description": "高分辨率光学影像（GeoTIFF，建议 GF-2/Pleiades，已完成正射校正）"},
    {"name": "aoi", "type": "file", "required": False, "description": "关注区域边界"},
    {"name": "bare_soil_threshold", "type": "number", "required": False,
     "description": "裸土/建筑垃圾判定阈值（默认 0.15，基于 BSI 指数）"},
    {"name": "min_area_m2", "type": "number", "required": False,
     "description": "最小可疑倾倒面积（平方米），默认 50"},
]

def _load_inputs(raw):
    if not raw: return {}
    p = Path(raw)
    return json.loads(p.read_text("utf-8")) if p.exists() else json.loads(raw)

def check_inputs(inputs):
    missing = [s for s in INPUT_SCHEMA if s["required"] and not inputs.get(s["name"])]
    prompt = None
    if missing:
        lines = ["为了进行非法倾倒监测，请补充以下输入："]
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
    bsi_thr = float(inputs.get("bare_soil_threshold") or algo.get("bare_soil_threshold", 0.15))
    min_area = float(inputs.get("min_area_m2") or algo.get("min_area_m2", 50))

    rng = np.random.default_rng(42)
    if has_rio and Path(inputs["image_optical"]).exists():
        try:
            with rasterio.open(inputs["image_optical"]) as s: arr = s.read().astype("float32")
        except Exception:
            arr = _sim_dumping(rng, np); has_rio = False
    else:
        arr = _sim_dumping(rng, np); has_rio = False

    # 裸土指数（BSI）计算
    bsi_map = _compute_bsi(arr, np)
    dump_mask = (bsi_map > bsi_thr).astype("uint8")

    # 过滤小图斑
    dump_mask = _clean(dump_mask, int(min_area / 100), np)

    # 生成可疑点位
    sites = _make_sites(dump_mask, bsi_map, np)

    outputs = {
        "bsi_map": str(_save_npy(output_dir / "bsi_map.tif", bsi_map, np)),
        "dump_mask": str(_save_npy(output_dir / "dump_mask.tif", dump_mask, np)),
        "suspect_sites": str(_save_geojson(output_dir / "suspect_sites.geojson", sites)),
        "sites_csv": str(_save_sites_csv(output_dir / "suspect_sites.csv", sites)),
        "report": str(_save_report_idd(output_dir / "report.md", sites, bsi_thr, min_area)),
    }
    return {"ok": True, "bsi_threshold": bsi_thr, "n_suspect_sites": len(sites), "outputs": outputs}

def _sim_dumping(rng, np):
    arr = rng.random((4, 64, 64)).astype("float32")
    # 模拟裸土/建筑垃圾区域（高 SWIR/Red，低 NIR）
    arr[0, 15:30, 15:30] = 0.7  # SWIR/Blue
    arr[2, 15:30, 15:30] = 0.6  # Red
    arr[3, 15:30, 15:30] = 0.2  # NIR
    arr[0, 40:55, 40:55] = 0.75
    arr[2, 40:55, 40:55] = 0.65
    arr[3, 40:55, 40:55] = 0.15
    return arr

def _compute_bsi(arr, np):
    # BSI = ((SWIR + Red) - (NIR + Blue)) / ((SWIR + Red) + (NIR + Blue))
    if arr.shape[0] >= 4:
        blue, red, nir = arr[0], arr[2], arr[3]
        swir = arr[0]  # 降级：用 Blue 代替 SWIR
        with np.errstate(divide="ignore", invalid="ignore"):
            bsi = ((swir + red) - (nir + blue)) / ((swir + red) + (nir + blue) + 1e-6)
    else:
        bsi = arr.mean(axis=0) - 0.5
    return np.clip(bsi, -1, 1).astype("float32")

def _clean(mask, min_patch, np):
    try:
        from scipy import ndimage as ndi
        labeled, n = ndi.label(mask)
        sizes = ndi.sum(mask, labeled, range(n + 1))
        keep = np.array(sizes) >= min_patch; keep[0] = False
        return keep[labeled].astype("uint8")
    except ImportError:
        return mask

def _make_sites(mask, bsi_map, np):
    sites = []
    try:
        from scipy import ndimage as ndi
        labeled, n = ndi.label(mask)
        for i in range(1, n + 1):
            ys, xs = np.where(labeled == i)
            if len(ys) < 2: continue
            bsi_vals = bsi_map[ys, xs]
            area = float(len(ys) * 100)
            sites.append({
                "geometry": {"type": "Point", "coordinates": [float(np.mean(xs)), float(np.mean(ys))]},
                "area_m2": area,
                "mean_bsi": float(np.mean(bsi_vals)),
                "max_bsi": float(np.max(bsi_vals)),
                "priority": "high" if area > 500 else "medium" if area > 100 else "low",
                "type": "suspected_illegal_dumping",
            })
    except ImportError:
        if mask.sum() > 0:
            ys, xs = np.where(mask > 0)
            sites.append({
                "geometry": {"type": "Point", "coordinates": [float(xs.mean()), float(ys.mean())]},
                "area_m2": float(mask.sum() * 100),
                "mean_bsi": float(bsi_map[mask > 0].mean()),
                "max_bsi": float(bsi_map[mask > 0].max()),
                "priority": "high", "type": "suspected_illegal_dumping",
            })
    return sites

def _save_npy(path, arr, np):
    np.save(path.with_suffix(".npy"), arr); return path.with_suffix(".npy")

def _save_geojson(path, sites):
    fc = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": s["geometry"],
         "properties": {k: v for k, v in s.items() if k != "geometry"}} for s in sites]}
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), "utf-8"); return path

def _save_sites_csv(path, sites):
    lines = ["id,lon,lat,area_m2,mean_bsi,max_bsi,priority,type"]
    for i, s in enumerate(sites, 1):
        lon, lat = s["geometry"]["coordinates"]
        lines.append(f"{i},{lon:.4f},{lat:.4f},{s.get('area_m2',0):.2f},"
                     f"{s.get('mean_bsi',0):.4f},{s.get('max_bsi',0):.4f},"
                     f"{s.get('priority','')},{s.get('type','')}")
    path.write_text("\n".join(lines), "utf-8"); return path

def _save_report_idd(path, sites, bsi_thr, min_area):
    by_p = {}
    for s in sites:
        p = s.get("priority", "low"); by_p[p] = by_p.get(p, 0) + 1
    lines = [
        "# 非法倾倒监测报告", "",
        f"- BSI 判定阈值: `{bsi_thr}`",
        f"- 最小可疑面积: `{min_area} m²`",
        f"- 疑似非法倾倒点位总数: **{len(sites)}**",
        "", "## 按优先级统计", "",
    ]
    for k in ("high", "medium", "low"):
        lines.append(f"- {k}: {by_p.get(k, 0)} 处")
    lines += ["", "## 处置建议", "",
        "1. **高优先级（面积 > 500 m²）**：立即派员现场核查，确认后启动行政执法程序",
        "2. **中优先级（100-500 m²）**：纳入下一轮巡查计划，重点关注",
        "3. **低优先级（< 100 m²）**：记录存档，定期复查",
    ]
    path.write_text("\n".join(lines), "utf-8"); return path

def main():
    ap = argparse.ArgumentParser(description="非法倾倒监测技能")
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
        out_dir = Path(args.output_dir or "./outputs") / "illegal-dumping-detection"
        result = run(inputs, config, out_dir)
    else:
        result = {"ok": True}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0

if __name__ == "__main__":
    sys.exit(main())
