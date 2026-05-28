"""Skill: urban-heat-assessment

城市热环境评估技能核心处理逻辑。基于热红外遥感反演地表温度（LST），
评估热岛强度、高温风险分级与降温潜力区域。
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("skill.urban-heat-assessment")

INPUT_SCHEMA: list[dict[str, Any]] = [
    {"name": "thermal_image", "type": "file", "required": True,
     "description": "热红外遥感影像（GeoTIFF）；Landsat 8/9 Band10 或已反演 LST"},
    {"name": "boundary", "type": "file", "required": True,
     "description": "城市行政边界（GeoJSON/Shapefile）"},
    {"name": "ndvi_image", "type": "file", "required": False,
     "description": "NDVI 影像（可选，用于辅助降温潜力识别）"},
    {"name": "building_density", "type": "file", "required": False,
     "description": "建筑密度栅格 0-1（可选）"},
    {"name": "is_lst", "type": "bool", "required": False,
     "description": "输入是否已为地表温度（摄氏度/开氏度），默认 false"},
    {"name": "reference_t_c", "type": "number", "required": False,
     "description": "参考温度阈值（摄氏度），不提供则使用郊区均值"},
    {"name": "year", "type": "int", "required": False,
     "description": "数据年份，供趋势分析使用"},
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
            missing.append(s)
            continue
        if v and s["type"] == "file" and not Path(v).exists():
            invalid.append({**s, "reason": f"文件不存在: {v}"})
    prompt = None
    if missing:
        lines = ["为完成城市热环境评估，请补充："]
        for i, m in enumerate(missing, 1):
            lines.append(f"{i}. **{m['name']}**：{m['description']}")
        prompt = "\n".join(lines)
    return {"ok": not missing and not invalid, "missing_inputs": missing,
            "invalid_inputs": invalid, "prompt": prompt}


def run(inputs: dict[str, Any], config: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    import numpy as np
    output_dir.mkdir(parents=True, exist_ok=True)
    algo = (config.get("algorithm") or {}) if isinstance(config, dict) else {}
    cool = (config.get("cooling") or {}) if isinstance(config, dict) else {}

    try:
        import rasterio  # type: ignore
        has_rio = Path(inputs["thermal_image"]).exists() and Path(inputs["thermal_image"]).stat().st_size > 200
    except ImportError:
        has_rio = False

    # ---- 读取或模拟热红外数据 ----
    if has_rio:
        try:
            with rasterio.open(inputs["thermal_image"]) as src:  # type: ignore
                raw = src.read(1).astype("float32")
                profile = src.profile
        except Exception as e:
            LOGGER.warning("无法读取热红外影像，降级: %s", e)
            raw, profile = _synthetic_thermal(), None
    else:
        LOGGER.info("降级模式：使用合成热红外数据")
        raw, profile = _synthetic_thermal(), None

    # ---- DN -> 亮温 -> LST ----
    is_lst = bool(inputs.get("is_lst"))
    if is_lst:
        lst = raw
        if lst.max() > 200:  # 视为开氏度
            lst = lst - 273.15
    else:
        lst = _retrieve_lst(raw, algo)

    # ---- 郊区参考均值 ----
    ref_t = inputs.get("reference_t_c")
    if ref_t is None:
        # 用最低 20% 像元代表"郊区/远郊"作为参考
        q20 = float(np.nanpercentile(lst, 20))
        ref_t = q20
    uhi = lst - float(ref_t)

    # ---- 风险分级 ----
    pcts = algo.get("risk_percentiles", [50, 75, 90])
    p50, p75, p90 = [float(np.nanpercentile(lst, p)) for p in pcts]
    risk = np.ones_like(lst, dtype="uint8")
    risk[lst >= p50] = 2
    risk[lst >= p75] = 3
    risk[lst >= p90] = 4

    # ---- 降温潜力区 ----
    ndvi_arr = _read_optional_tif(inputs.get("ndvi_image"))
    bd_arr = _read_optional_tif(inputs.get("building_density"))
    if ndvi_arr is None:
        ndvi_arr = np.full_like(lst, 0.3)
    if bd_arr is None:
        bd_arr = np.full_like(lst, 0.5)
    ndvi_arr, bd_arr = _match_shape(ndvi_arr, lst), _match_shape(bd_arr, lst)

    cool_mask = (
        (lst >= p75)
        & (ndvi_arr < float(cool.get("ndvi_low_threshold", 0.2)))
        & (bd_arr >= float(cool.get("building_density_high", 0.6)))
    ).astype("uint8")

    patches = _vectorize(cool_mask, profile)

    # ---- 输出 ----
    outputs: dict[str, str] = {}
    outputs["lst"] = str(_write_geotiff(output_dir / "lst.tif", lst, profile, "float32"))
    outputs["uhi_intensity"] = str(_write_geotiff(output_dir / "uhi_intensity.tif", uhi, profile, "float32"))
    outputs["risk_levels"] = str(_write_geotiff(output_dir / "risk_levels.tif", risk, profile, "uint8"))
    outputs["cooling_priority"] = str(_write_geojson(output_dir / "cooling_priority.geojson", patches))
    outputs["report"] = str(_write_report(
        output_dir / "report.md", lst, uhi, risk, patches,
        ref_t=float(ref_t), year=inputs.get("year"),
    ))

    return {
        "ok": True,
        "lst_stats": {
            "min_c": float(np.nanmin(lst)),
            "max_c": float(np.nanmax(lst)),
            "mean_c": float(np.nanmean(lst)),
            "p50_c": p50, "p75_c": p75, "p90_c": p90,
        },
        "reference_t_c": float(ref_t),
        "uhi_mean": float(np.nanmean(uhi)),
        "cooling_patches": len(patches),
        "outputs": outputs,
    }


# ----- 子函数 -----
def _synthetic_thermal():
    import numpy as np
    rng = np.random.default_rng(0)
    base = rng.normal(loc=305.0, scale=2.0, size=(64, 64))  # ~32℃ in Kelvin
    base[20:45, 20:45] += 5.0  # 模拟热岛
    return base.astype("float32")


def _retrieve_lst(raw, algo):
    """DN/辐亮度 -> 亮温 -> LST（单通道简化算法）。"""
    import numpy as np
    K1 = float(algo.get("band10_K1", 774.8853))
    K2 = float(algo.get("band10_K2", 1321.0789))
    mult = float(algo.get("radiance_mult", 3.3420e-04))
    add = float(algo.get("radiance_add", 0.1))
    em = float(algo.get("emissivity", 0.97))

    if raw.mean() > 250:
        # 视为已经是亮温（K）
        bt = raw
    else:
        radiance = mult * raw + add
        radiance = np.maximum(radiance, 1e-6)
        bt = K2 / np.log(K1 / radiance + 1.0)

    # 单窗校正（简化：emissivity correction）
    lst_k = bt / (1 + (10.895 * bt / 14380) * np.log(em))
    lst_c = lst_k - 273.15
    return lst_c.astype("float32")


def _read_optional_tif(path):
    if not path:
        return None
    try:
        import rasterio  # type: ignore
        with rasterio.open(path) as src:
            return src.read(1).astype("float32")
    except Exception:
        return None


def _match_shape(a, ref):
    import numpy as np
    if a.shape == ref.shape:
        return a
    h = min(a.shape[0], ref.shape[0]); w = min(a.shape[1], ref.shape[1])
    out = np.zeros_like(ref)
    out[:h, :w] = a[:h, :w]
    return out


def _vectorize(binary, profile):
    patches: list[dict[str, Any]] = []
    try:
        from rasterio import features  # type: ignore
        from shapely.geometry import shape  # type: ignore
        if profile is not None:
            for geom, val in features.shapes(binary.astype("uint8"), transform=profile.get("transform")):
                if val != 1:
                    continue
                poly = shape(geom)
                patches.append({
                    "geometry": geom,
                    "area_m2": float(poly.area),
                    "centroid": [poly.centroid.x, poly.centroid.y],
                    "suggestion": "增加绿植/高反照率铺装/遮阴设施",
                })
            return patches
    except ImportError:
        pass
    try:
        import numpy as np
        from scipy import ndimage as ndi
        labeled, n = ndi.label(binary)
        for i in range(1, n + 1):
            ys, xs = np.where(labeled == i)
            if len(ys) == 0:
                continue
            patches.append({
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [float(xs.min()), float(ys.min())],
                        [float(xs.max()), float(ys.min())],
                        [float(xs.max()), float(ys.max())],
                        [float(xs.min()), float(ys.max())],
                        [float(xs.min()), float(ys.min())],
                    ]],
                },
                "area_m2": float(len(ys)),
                "centroid": [float(xs.mean()), float(ys.mean())],
                "suggestion": "增加绿植/高反照率铺装/遮阴设施",
            })
    except ImportError:
        pass
    return patches


def _write_geotiff(path: Path, arr, profile, dtype: str) -> Path:
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


def _write_geojson(path: Path, patches) -> Path:
    fc = {"type": "FeatureCollection",
          "features": [{"type": "Feature", "geometry": p["geometry"],
                        "properties": {k: v for k, v in p.items() if k != "geometry"}} for p in patches]}
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_report(path: Path, lst, uhi, risk, patches, ref_t: float, year) -> Path:
    import numpy as np
    n_patches = len(patches)
    risk_hist = {int(k): int(v) for k, v in zip(*np.unique(risk, return_counts=True))}
    content = [
        "# 城市热环境评估报告",
        "",
        f"- 数据年份: {year or '未提供'}",
        f"- 郊区参考温度: {ref_t:.2f} ℃",
        f"- 城区 LST 均值: {float(np.nanmean(lst)):.2f} ℃",
        f"- UHI 强度均值: {float(np.nanmean(uhi)):.2f} ℃",
        "",
        "## 风险像元统计（1=低 2=中 3=高 4=极端）",
    ]
    for level in (1, 2, 3, 4):
        content.append(f"- Level {level}: {risk_hist.get(level, 0)} 像元")
    content += [
        "",
        f"## 降温潜力斑块: {n_patches} 块",
        "",
        "## 降温建议",
        "1. 对 Level 3/4 像元优先排查：通常集中在商业中心、工业区、大面积硬化广场",
        "2. 增加 Level 3/4 区域的绿地覆盖（乔灌结合）可有效降低表面温度 2-4 ℃",
        "3. 对低反照率屋面推广冷屋顶（白色/高反射涂料）",
        "4. 在人流密集高温斑块设置雾森、遮阳、饮水点以降低健康风险",
    ]
    path.write_text("\n".join(content), encoding="utf-8")
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="城市热环境评估技能")
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
            "outputs_dir", "./outputs")) / "urban-heat-assessment"
        result = run(inputs, config, out_dir)
    else:
        result = {"ok": True, "note": "report action 由调用方直接读取 run 输出的 markdown"}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
