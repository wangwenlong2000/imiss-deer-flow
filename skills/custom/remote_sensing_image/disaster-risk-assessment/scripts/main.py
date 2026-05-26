"""Skill: disaster-risk-assessment

自然灾害风险遥感评估：flood / landslide / wildfire。
构建危险性 H × 暴露度 E × 脆弱性 V 的综合风险模型。
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("skill.disaster-risk-assessment")

SUPPORTED_HAZARDS = ("flood", "landslide", "wildfire")

INPUT_SCHEMA: list[dict[str, Any]] = [
    {"name": "hazard_type", "type": "string", "required": True,
     "description": f"灾种，可选：{', '.join(SUPPORTED_HAZARDS)}"},
    {"name": "dem", "type": "file", "required": True, "description": "数字高程模型 GeoTIFF"},
    {"name": "lulc", "type": "file", "required": True, "description": "土地利用分类图 GeoTIFF"},
    {"name": "history_events", "type": "file", "required": False,
     "description": "历史灾害点位 GeoJSON 或 CSV（lon,lat,event_type,year）"},
    {"name": "population", "type": "file", "required": False, "description": "人口栅格"},
    {"name": "buildings", "type": "file", "required": False, "description": "建筑轮廓或建筑密度栅格"},
    {"name": "weather", "type": "file", "required": False, "description": "气象数据 CSV/NetCDF"},
    {"name": "rivers", "type": "file", "required": False, "description": "河流水系 GeoJSON（洪涝推荐）"},
    {"name": "shelters", "type": "file", "required": False, "description": "已有避难场所点位 GeoJSON"},
]


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except ImportError:
        return {}


def _load_inputs(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    p = Path(raw)
    if p.exists() and p.is_file():
        return json.loads(p.read_text(encoding="utf-8"))
    return json.loads(raw)


def check_inputs(inputs: dict[str, Any]) -> dict[str, Any]:
    missing, invalid = [], []
    for s in INPUT_SCHEMA:
        v = inputs.get(s["name"])
        if s["required"] and v in (None, ""):
            missing.append(s); continue
        if v and s["type"] == "file" and not Path(v).exists():
            invalid.append({**s, "reason": f"文件不存在: {v}"})
    ht = inputs.get("hazard_type")
    if ht and ht not in SUPPORTED_HAZARDS:
        invalid.append({"name": "hazard_type", "reason": f"不支持的灾种: {ht}，应为 {SUPPORTED_HAZARDS}"})
    prompt = None
    if missing:
        lines = ["为评估自然灾害风险，请补充："]
        for i, m in enumerate(missing, 1):
            lines.append(f"{i}. **{m['name']}**：{m['description']}")
        prompt = "\n".join(lines)
    return {"ok": not missing and not invalid, "missing_inputs": missing,
            "invalid_inputs": invalid, "prompt": prompt}


def run(inputs: dict[str, Any], config: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    import numpy as np
    output_dir.mkdir(parents=True, exist_ok=True)
    hazard_type = inputs["hazard_type"]
    weights = (config.get("hazard_weights") or {}).get(hazard_type, {})
    vuln_map = {int(k): float(v) for k, v in (config.get("lulc_vulnerability") or {}).items()}
    imperv_map = {int(k): float(v) for k, v in (config.get("lulc_impervious") or {}).items()}
    n_levels = int(config.get("risk_levels", 5))

    dem, dem_profile = _read_tif(inputs["dem"], default_shape=(64, 64), seed=11)
    lulc, _ = _read_tif(inputs["lulc"], default_shape=dem.shape, seed=12, is_int=True)
    lulc = _crop_to(lulc, dem.shape)

    # ---- 危险性 ----
    hazard = _compute_hazard(hazard_type, dem, lulc, inputs, weights, imperv_map)

    # ---- 暴露度 ----
    pop, _ = _read_tif(inputs.get("population"), default_shape=dem.shape, seed=13, optional=True)
    bld, _ = _read_tif(inputs.get("buildings"), default_shape=dem.shape, seed=14, optional=True)
    exposure = _compute_exposure(dem.shape, pop, bld, lulc, imperv_map)

    # ---- 脆弱性 ----
    vulnerability = _compute_vulnerability(lulc, vuln_map)

    # ---- 综合风险 ----
    H = _norm01(hazard); E = _norm01(exposure); V = _norm01(vulnerability)
    risk = H * E * V
    risk = _norm01(risk)
    risk_level = _quantile_bin(risk, n_levels)

    # ---- 高风险斑块 ----
    high_patches = _extract_high_risk(risk_level, n_levels, dem_profile, int(config.get("high_risk_min_patch", 50)))

    # ---- 避难场所缺口 ----
    shelter_gap = _shelter_gap(risk_level, inputs.get("shelters"),
                                coverage_m=float(config.get("shelter_coverage_m", 1000)),
                                pixel_size_m=_pixel_size_m(dem_profile))

    # ---- 输出 ----
    outputs: dict[str, str] = {}
    outputs["hazard"] = str(_write_geotiff(output_dir / "hazard.tif", H, dem_profile, "float32"))
    outputs["exposure"] = str(_write_geotiff(output_dir / "exposure.tif", E, dem_profile, "float32"))
    outputs["vulnerability"] = str(_write_geotiff(output_dir / "vulnerability.tif", V, dem_profile, "float32"))
    outputs["risk_level"] = str(_write_geotiff(output_dir / "risk_level.tif", risk_level, dem_profile, "uint8"))
    outputs["high_risk_patches"] = str(_write_geojson(output_dir / "high_risk_patches.geojson", high_patches))
    outputs["shelter_gap"] = str(_write_geojson(output_dir / "shelter_gap.geojson", shelter_gap))
    outputs["report"] = str(_write_report(output_dir / "report.md", hazard_type, risk_level, high_patches, shelter_gap))

    return {
        "ok": True, "hazard_type": hazard_type,
        "risk_summary": _summarize_risk(risk_level, n_levels),
        "n_high_risk_patches": len(high_patches),
        "n_shelter_gap_regions": len(shelter_gap),
        "outputs": outputs,
    }


# ---- 子函数 ----
def _read_tif(path, default_shape=(64, 64), seed=0, optional=False, is_int=False):
    import numpy as np
    if path and Path(path).exists():
        try:
            import rasterio  # type: ignore
            if Path(path).stat().st_size > 200:
                with rasterio.open(path) as src:
                    arr = src.read(1).astype("int32" if is_int else "float32")
                    return arr, src.profile
        except Exception:
            pass
    if optional and not path:
        return None, None
    rng = np.random.default_rng(seed)
    if is_int:
        arr = rng.integers(1, 8, size=default_shape).astype("int32")
    else:
        # DEM 风格：中间高、四周低
        h, w = default_shape
        y, x = np.ogrid[:h, :w]
        arr = 100 + 50 * np.exp(-((y - h / 2) ** 2 + (x - w / 2) ** 2) / (2 * (h / 3) ** 2))
        arr = arr + rng.normal(0, 3, size=default_shape)
        arr = arr.astype("float32")
    return arr, None


def _crop_to(arr, shape):
    h = min(arr.shape[0], shape[0]); w = min(arr.shape[1], shape[1])
    return arr[:h, :w]


def _pixel_size_m(profile):
    if profile is None:
        return 30.0
    try:
        return abs(float(profile.get("transform").a))
    except Exception:
        return 30.0


def _slope(dem, pixel_size_m=30.0):
    import numpy as np
    dzdx = np.gradient(dem, axis=1) / pixel_size_m
    dzdy = np.gradient(dem, axis=0) / pixel_size_m
    slope = np.degrees(np.arctan(np.sqrt(dzdx ** 2 + dzdy ** 2)))
    return slope.astype("float32")


def _distance_to_water(shape, inputs):
    import numpy as np
    # 降级：假设最低 10% DEM 为水体
    dem, _ = _read_tif(inputs.get("dem"))
    dem = _crop_to(dem, shape)
    water = (dem < np.percentile(dem, 10)).astype("uint8")
    try:
        from scipy import ndimage as ndi
        return ndi.distance_transform_edt(1 - water)
    except ImportError:
        return np.full(shape, 10.0)


def _norm01(arr):
    import numpy as np
    mn, mx = float(np.nanmin(arr)), float(np.nanmax(arr))
    if mx - mn < 1e-9:
        return np.zeros_like(arr, dtype="float32")
    return ((arr - mn) / (mx - mn)).astype("float32")


def _compute_hazard(hazard_type, dem, lulc, inputs, weights, imperv_map):
    import numpy as np
    shape = dem.shape
    slope = _slope(dem, _pixel_size_m(None))

    if hazard_type == "flood":
        dist_water = _distance_to_water(shape, inputs)
        elev_pct = _norm01(-dem)  # 越低越危险
        impervious = np.vectorize(lambda v: imperv_map.get(int(v), 0.3))(lulc).astype("float32")
        w = weights or {"slope": 0.2, "distance_to_water": 0.4, "impervious": 0.2, "elevation_percentile": 0.2}
        H = (w.get("slope", 0.2) * _norm01(-slope)
             + w.get("distance_to_water", 0.4) * _norm01(-dist_water)
             + w.get("impervious", 0.2) * impervious
             + w.get("elevation_percentile", 0.2) * elev_pct)
    elif hazard_type == "landslide":
        aspect = np.gradient(dem, axis=0).astype("float32")
        wetness = _norm01(-dem)
        lulc_risk = np.vectorize(lambda v: 0.8 if int(v) in (3, 4, 7) else 0.3)(lulc).astype("float32")
        w = weights or {"slope": 0.5, "aspect": 0.1, "wetness": 0.2, "lulc": 0.2}
        H = (w.get("slope", 0.5) * _norm01(slope)
             + w.get("aspect", 0.1) * _norm01(np.abs(aspect))
             + w.get("wetness", 0.2) * wetness
             + w.get("lulc", 0.2) * lulc_risk)
    else:  # wildfire
        fuel = np.vectorize(lambda v: 0.9 if int(v) in (3, 4) else 0.2)(lulc).astype("float32")
        history = _history_density(shape, inputs.get("history_events"))
        w = weights or {"fuel": 0.5, "slope": 0.2, "history": 0.3}
        H = (w.get("fuel", 0.5) * fuel
             + w.get("slope", 0.2) * _norm01(slope)
             + w.get("history", 0.3) * _norm01(history))

    return H.astype("float32")


def _history_density(shape, path):
    import numpy as np
    density = np.zeros(shape, dtype="float32")
    if not path or not Path(path).exists():
        return density
    h, w = shape
    try:
        if str(path).endswith(".geojson") or str(path).endswith(".json"):
            data = json.loads(Path(path).read_text(encoding="utf-8"))
            for feat in data.get("features", []):
                coords = feat.get("geometry", {}).get("coordinates")
                if coords and len(coords) == 2:
                    # 像元坐标近似（降级）
                    x = int(coords[0] % w); y = int(coords[1] % h)
                    density[y, x] += 1
        else:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        x = int(float(row.get("lon", 0)) % w)
                        y = int(float(row.get("lat", 0)) % h)
                        density[y, x] += 1
                    except Exception:
                        continue
    except Exception:
        pass
    try:
        from scipy.ndimage import gaussian_filter
        density = gaussian_filter(density, sigma=3)
    except ImportError:
        pass
    return density


def _compute_exposure(shape, pop, bld, lulc, imperv_map):
    import numpy as np
    if pop is None:
        pop = np.vectorize(lambda v: imperv_map.get(int(v), 0.1) * 100)(lulc).astype("float32")
    pop = _crop_to(pop, shape)
    if bld is None:
        bld = np.vectorize(lambda v: imperv_map.get(int(v), 0.1))(lulc).astype("float32")
    bld = _crop_to(bld, shape)
    return 0.6 * _norm01(pop) + 0.4 * _norm01(bld)


def _compute_vulnerability(lulc, vuln_map):
    import numpy as np
    return np.vectorize(lambda v: vuln_map.get(int(v), 0.3))(lulc).astype("float32")


def _quantile_bin(arr, n_levels):
    import numpy as np
    flat = arr.flatten()
    quantiles = np.quantile(flat, [(i + 1) / n_levels for i in range(n_levels - 1)])
    out = np.ones_like(arr, dtype="uint8")
    for i, q in enumerate(quantiles):
        out[arr > q] = i + 2
    return out


def _extract_high_risk(risk_level, n_levels, profile, min_patch):
    patches = []
    try:
        import numpy as np
        from scipy import ndimage as ndi
    except ImportError:
        return patches
    binary = (risk_level >= (n_levels - 1)).astype("uint8")
    labeled, n = ndi.label(binary)
    for i in range(1, n + 1):
        ys, xs = np.where(labeled == i)
        if len(ys) < min_patch:
            continue
        patches.append({
            "geometry": {"type": "Polygon", "coordinates": [[
                [float(xs.min()), float(ys.min())], [float(xs.max()), float(ys.min())],
                [float(xs.max()), float(ys.max())], [float(xs.min()), float(ys.max())],
                [float(xs.min()), float(ys.min())],
            ]]},
            "pixels": int(len(ys)),
            "level": int(risk_level[int(ys.mean()), int(xs.mean())]),
            "suggestion": "纳入重点防控；核实人口/建筑避险设施",
        })
    return patches


def _shelter_gap(risk_level, shelter_path, coverage_m, pixel_size_m):
    import numpy as np
    gaps = []
    high_mask = (risk_level >= risk_level.max()).astype("uint8")
    try:
        from scipy import ndimage as ndi
    except ImportError:
        return gaps
    # 避难场所栅格
    shelter_raster = np.zeros_like(high_mask)
    if shelter_path and Path(shelter_path).exists():
        try:
            data = json.loads(Path(shelter_path).read_text(encoding="utf-8"))
            h, w = high_mask.shape
            for feat in data.get("features", []):
                coords = feat.get("geometry", {}).get("coordinates") or []
                if len(coords) == 2:
                    x = int(coords[0] % w); y = int(coords[1] % h)
                    shelter_raster[y, x] = 1
        except Exception:
            pass
    # 距避难场所的距离
    if shelter_raster.sum() == 0:
        gap_mask = high_mask
    else:
        dist = ndi.distance_transform_edt(1 - shelter_raster) * pixel_size_m
        gap_mask = (high_mask & (dist > coverage_m)).astype("uint8")

    labeled, n = ndi.label(gap_mask)
    for i in range(1, n + 1):
        ys, xs = np.where(labeled == i)
        if len(ys) < 20:
            continue
        gaps.append({
            "geometry": {"type": "Polygon", "coordinates": [[
                [float(xs.min()), float(ys.min())], [float(xs.max()), float(ys.min())],
                [float(xs.max()), float(ys.max())], [float(xs.min()), float(ys.max())],
                [float(xs.min()), float(ys.min())],
            ]]},
            "pixels": int(len(ys)),
            "suggestion": f"高风险且距避难场所 > {coverage_m}m，建议新增避难点或调整疏散路线",
        })
    return gaps


def _summarize_risk(risk_level, n_levels):
    import numpy as np
    out = {}
    total = int(risk_level.size)
    for lv in range(1, n_levels + 1):
        out[f"level_{lv}_pct"] = float((risk_level == lv).sum() / total * 100)
    return out


def _write_geotiff(path: Path, arr, profile, dtype):
    try:
        import rasterio  # type: ignore
        if profile is not None:
            prof = profile.copy()
            prof.update(count=1, dtype=dtype, compress="LZW")
            with rasterio.open(path, "w", **prof) as dst:
                dst.write(arr.astype(dtype), 1)
            return path
    except ImportError:
        pass
    import numpy as np
    np.save(path.with_suffix(".npy"), arr.astype(dtype))
    return path.with_suffix(".npy")


def _write_geojson(path: Path, items):
    fc = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": it["geometry"],
         "properties": {k: v for k, v in it.items() if k != "geometry"}}
        for it in items
    ]}
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_report(path, hazard_type, risk_level, high_patches, shelter_gap):
    import numpy as np
    mx = int(risk_level.max())
    high_pct = float((risk_level >= mx - 1).sum() / risk_level.size * 100)
    content = [
        f"# 自然灾害风险评估报告（{hazard_type}）",
        "",
        f"- 最高风险等级: {mx}",
        f"- 高风险像元占比（Level {mx - 1}/{mx}）: {high_pct:.2f}%",
        f"- 高风险斑块数: **{len(high_patches)}**",
        f"- 避难场所覆盖缺口区域: **{len(shelter_gap)}**",
        "",
        "## 减灾建议",
    ]
    if hazard_type == "flood":
        content += [
            "1. 在高风险低洼地段改造排水管网和海绵设施",
            "2. 对 Level 4/5 居民区制定分级转移清单",
            "3. 沿河高风险段增设堤防/生态缓冲带",
        ]
    elif hazard_type == "landslide":
        content += [
            "1. 对高坡度+高脆弱性斑块进行工程治理（挡墙、锚固、植被恢复）",
            "2. 汛期前后加密监测并发布预警",
            "3. 限制高风险坡地新建居住用地",
        ]
    else:
        content += [
            "1. 对植被密集高坡度斑块建设防火隔离带与巡逻",
            "2. 在历史火点聚集区增设监测摄像头",
            "3. 干旱季节发布高火险预警，限制野外用火",
        ]
    content += [
        "",
        "## 避难场所",
        "- 对应急覆盖缺口区域优先选址：学校、体育馆、大型商业综合体；",
        "- 同步更新疏散路线图并定期演练。",
    ]
    path.write_text("\n".join(content), encoding="utf-8")
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="自然灾害风险遥感评估技能")
    ap.add_argument("--action", required=True, choices=["check-inputs", "run", "report"])
    ap.add_argument("--inputs-json", default="{}")
    ap.add_argument("--config", default=None)
    ap.add_argument("--output-dir", default=None)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)

    inputs = _load_inputs(args.inputs_json)
    cfg_path = Path(args.config) if args.config else Path(__file__).parent.parent / "config.yaml"
    config = _load_yaml(cfg_path) if cfg_path.exists() else {}

    if args.action == "check-inputs":
        result = check_inputs(inputs)
    elif args.action == "run":
        c = check_inputs(inputs)
        if not c["ok"]:
            print(json.dumps({"ok": False, "error": "inputs incomplete", **c}, ensure_ascii=False, indent=2))
            return 2
        out_dir = Path(args.output_dir or (config.get("io") or {}).get(
            "outputs_dir", "./outputs")) / "disaster-risk-assessment"
        result = run(inputs, config, out_dir)
    else:
        result = {"ok": True, "note": "report action 由调用方读取 run 的 markdown"}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
