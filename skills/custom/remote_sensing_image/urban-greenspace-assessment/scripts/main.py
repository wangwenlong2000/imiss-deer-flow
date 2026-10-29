"""Skill: urban-greenspace-assessment

城市绿地生态系统评估：提取绿地 → 计算景观指数 → 生态廊道 → 服务可达性。
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("skill.urban-greenspace-assessment")

INPUT_SCHEMA: list[dict[str, Any]] = [
    {"name": "rs_image", "type": "file", "required": False,
     "description": "高分辨率遥感影像（GeoTIFF，至少含红、近红波段）"},
    {"name": "boundary", "type": "file", "required": True,
     "description": "行政区划/街道边界（GeoJSON/Shapefile）"},
    {"name": "population", "type": "file", "required": False,
     "description": "人口栅格或网格化人口数据（用于评估服务公平性）"},
    {"name": "green_mask", "type": "file", "required": False,
     "description": "已有的绿地掩膜 GeoTIFF（若提供则跳过 NDVI 提取）"},
    {"name": "service_distance_m", "type": "number", "required": False,
     "description": "服务半径（米），默认 500m"},
    {"name": "ndvi_threshold", "type": "number", "required": False,
     "description": "绿地提取 NDVI 阈值，默认 0.25"},
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
        return json.loads(p.read_text(encoding="utf-8-sig"))
    return json.loads(raw)


def _inspect_raster(path: str) -> dict[str, Any]:
    try:
        import rasterio  # type: ignore
    except ImportError as exc:
        return {
            "ok": False,
            "reason": "当前环境缺少 rasterio，无法读取 GeoTIFF；请安装依赖后重试，或提供已验证的 green_mask 并在具备 rasterio 的环境运行",
            "exception": str(exc),
        }
    try:
        with rasterio.open(path) as src:
            return {
                "ok": True,
                "bands": int(src.count),
                "width": int(src.width),
                "height": int(src.height),
                "crs": str(src.crs) if src.crs else None,
                "dtype": src.dtypes[0] if src.dtypes else None,
            }
    except Exception as exc:
        return {
            "ok": False,
            "reason": f"无法作为 GeoTIFF 读取: {exc}",
            "exception": exc.__class__.__name__,
        }


def check_inputs(inputs: dict[str, Any]) -> dict[str, Any]:
    missing, invalid = [], []
    has_rs_image = bool(inputs.get("rs_image"))
    has_green_mask = bool(inputs.get("green_mask"))
    if not has_rs_image and not has_green_mask:
        missing.append({
            "name": "rs_image",
            "type": "file",
            "required": True,
            "description": "高分辨率遥感影像（GeoTIFF，至少含红、近红波段）；若已有绿地掩膜，可改传 green_mask"
        })
    for s in INPUT_SCHEMA:
        v = inputs.get(s["name"])
        if s["required"] and v in (None, ""):
            missing.append(s); continue
        if v and s["type"] == "file" and not Path(v).exists():
            invalid.append({**s, "reason": f"文件不存在: {v}"})
    if has_rs_image and not has_green_mask and Path(str(inputs["rs_image"])).exists():
        raster_info = _inspect_raster(str(inputs["rs_image"]))
        if not raster_info["ok"]:
            invalid.append({
                "name": "rs_image",
                "type": "file",
                "required": True,
                "reason": raster_info["reason"],
            })
        elif int(raster_info.get("bands", 0)) < 4:
            invalid.append({
                "name": "rs_image",
                "type": "file",
                "required": True,
                "reason": (
                    f"当前脚本只支持含红光与近红外波段的多光谱影像，检测到 {raster_info.get('bands')} 个波段。"
                    "RGB-only 影像不能计算真实 NDVI；请补充含 NIR 的多光谱 GeoTIFF，或提供已有绿地二值掩膜 green_mask。"
                ),
            })
    if has_green_mask and Path(str(inputs["green_mask"])).exists():
        mask_info = _inspect_raster(str(inputs["green_mask"]))
        if not mask_info["ok"]:
            invalid.append({
                "name": "green_mask",
                "type": "file",
                "required": True,
                "reason": f"已有绿地掩膜无法作为 GeoTIFF 读取：{mask_info['reason']}。请提供可读的单波段/多波段二值 GeoTIFF 掩膜。",
            })
    prompt = None
    if missing or invalid:
        lines = ["为评估城市绿地生态，请补充："]
        for i, m in enumerate(missing, 1):
            lines.append(f"{i}. **{m['name']}**：{m['description']}")
        offset = len(missing)
        for i, item in enumerate(invalid, 1):
            lines.append(f"{offset + i}. **{item['name']}**：{item.get('reason', '输入无效')}")
        prompt = "\n".join(lines)
    return {"ok": not missing and not invalid, "missing_inputs": missing,
            "invalid_inputs": invalid, "prompt": prompt}


def run(inputs: dict[str, Any], config: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    import numpy as np
    output_dir.mkdir(parents=True, exist_ok=True)
    ext_cfg = config.get("extraction") or {}
    ls_cfg = config.get("landscape") or {}
    eq_cfg = config.get("equity") or {}

    ndvi_th = float(inputs.get("ndvi_threshold") or ext_cfg.get("ndvi_threshold", 0.25))
    min_patch = int(ext_cfg.get("min_patch_size", 100))
    service_m = float(inputs.get("service_distance_m") or ls_cfg.get("service_distance_m", 500))

    # ---- 提取绿地 ----
    profile, green, valid = _extract_green(inputs, ndvi_th, min_patch)

    # ---- 景观指数 ----
    metrics = _compute_landscape_metrics(green, ls_cfg, profile, valid)

    # ---- 斑块矢量 ----
    patches = _vectorize(green, profile)

    # ---- 生态廊道（简化：连接大斑块的骨架线）----
    corridors = _build_corridors(patches, ls_cfg)

    # ---- 服务可达性 & 人均绿地 ----
    pop_arr = _read_optional_tif(inputs.get("population"), profile, valid)
    total_pop = float(pop_arr.sum()) if pop_arr is not None else 0.0
    total_green_m2 = metrics["green_area_m2"]
    per_capita = total_green_m2 / total_pop if total_pop > 0 else None
    weak = _detect_weak_areas(green, pop_arr, service_m, eq_cfg, profile, valid)

    # ---- 输出 ----
    outputs: dict[str, str] = {}
    outputs["green_mask"] = str(_write_geotiff(output_dir / "green_mask.tif", np.where(valid, green, 255), {**profile, "nodata": 255}, "uint8"))
    outputs["green_patches"] = str(_write_geojson(output_dir / "green_patches.geojson", patches, profile))
    outputs["corridors"] = str(_write_geojson(output_dir / "corridors.geojson", corridors, profile))
    metrics_out = {
        **metrics,
        "population": total_pop,
        "per_capita_green_m2": per_capita,
        "target_per_capita_m2": float(eq_cfg.get("target_per_capita_m2", 15)),
        "service_distance_m": service_m,
        "weak_area_status": "evaluated" if pop_arr is not None else "not_evaluated_no_population",
    }
    (output_dir / "metrics.json").write_text(
        json.dumps(metrics_out, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["metrics"] = str(output_dir / "metrics.json")
    outputs["weak_areas"] = str(_write_geojson(output_dir / "weak_areas.geojson", weak, profile))
    outputs["report"] = str(_write_report(output_dir / "report.md", metrics_out, patches, weak))

    return {"ok": True, **metrics_out, "outputs": outputs}


# ---- 子函数 ----
def _boundary_shapes(path, raster_crs):
    from rasterio.warp import transform_geom
    import fiona

    # GeoJSON without a CRS follows WGS84; Fiona also reads Shapefile/GPKG CRS.
    with fiona.open(path) as source:
        boundary_crs = source.crs_wkt or source.crs
        if not boundary_crs:
            raise ValueError("边界缺少 CRS，无法与影像对齐")
        shapes = [transform_geom(boundary_crs, raster_crs, feature["geometry"])
                  for feature in source if feature["geometry"] is not None]
    if not shapes or any(g["type"] not in {"Polygon", "MultiPolygon"} for g in shapes):
        raise ValueError("boundary 必须包含非空 Polygon/MultiPolygon")
    return shapes


def _extract_green(inputs, ndvi_th, min_patch):
    import numpy as np
    import rasterio
    from rasterio.features import geometry_mask

    use_mask = bool(inputs.get("green_mask"))
    path = inputs["green_mask"] if use_mask else inputs["rs_image"]
    with rasterio.open(path) as src:
        if not src.crs:
            raise ValueError("影像缺少 CRS，无法计算平方米面积或应用边界")
        if not use_mask and src.count < 4:
            raise ValueError("RGB-only 影像无法计算真实 NDVI，请提供含 NIR 的多光谱影像或 green_mask")
        bands = src.read([1] if use_mask else [3, 4], masked=True).astype("float64")
        valid = ~np.ma.getmaskarray(bands).any(axis=0)
        valid &= np.isfinite(bands.filled(np.nan)).all(axis=0)
        valid &= geometry_mask(_boundary_shapes(inputs["boundary"], src.crs),
                               out_shape=(src.height, src.width), transform=src.transform,
                               invert=True, all_touched=False)
        if not valid.any():
            raise ValueError("边界与影像有效像元无交集，无法统计")
        if use_mask:
            green = (bands[0].filled(0) > 0) & valid
        else:
            red, nir = bands.filled(0)
            ndvi = (nir - red) / (nir + red + 1e-6)
            green = _clean(((ndvi > ndvi_th) & valid).astype("uint8"), min_patch)
        return src.profile, green.astype("uint8"), valid


def _clean(binary, min_patch):
    from scipy import ndimage as ndi
    labeled, n = ndi.label(binary)
    sizes = ndi.sum(binary, labeled, range(n + 1))
    keep = sizes >= min_patch
    keep[0] = False
    return keep[labeled].astype("uint8")


def _pixel_areas(profile):
    """Square metres in the source CRS; geographic grids use ellipsoidal areas."""
    import numpy as np
    from pyproj import CRS
    crs = CRS.from_user_input(profile["crs"])
    transform = profile["transform"]
    if crs.is_projected:
        factor_x, factor_y = [axis.unit_conversion_factor for axis in crs.axis_info[:2]]
        return np.full((profile["height"], profile["width"]),
                       abs(transform.a * transform.e - transform.b * transform.d) * factor_x * factor_y)
    if not crs.is_geographic:
        raise ValueError("无法从影像 CRS 计算面积")
    geod = crs.get_geod()
    areas = np.empty((profile["height"], profile["width"]), dtype="float64")
    for row in range(profile["height"]):
        # On a north-up geographic grid, cell area is identical along each row.
        columns = range(profile["width"]) if transform.b or transform.d else range(1)
        for col in columns:
            corners = [transform * xy for xy in [(col, row), (col + 1, row),
                       (col + 1, row + 1), (col, row + 1)]]
            area, _ = geod.polygon_area_perimeter(*zip(*corners))
            areas[row, col] = abs(area)
        if not transform.b and not transform.d:
            areas[row, :] = areas[row, 0]
    return areas


def _compute_landscape_metrics(green, ls_cfg, profile, valid):
    import numpy as np
    from scipy import ndimage as ndi
    weights = _pixel_areas(profile)
    aoi_area = float(weights[valid].sum())
    green_area = float(weights[green > 0].sum())
    labeled, n_patches = ndi.label(green)
    sizes = ndi.sum(weights, labeled, range(1, n_patches + 1))
    large_area = float(ls_cfg.get("large_patch_area_m2", 10000))
    return {
        "green_coverage_pct": green_area / aoi_area * 100 if aoi_area else 0,
        "green_area_m2": green_area,
        "aoi_area_m2": aoi_area,
        "valid_pixel_count": int(np.count_nonzero(valid)),
        "area_method": "source_crs_grid_area" if profile["crs"].is_projected else "ellipsoidal_pixel_area",
        "boundary_rule": "pixel_center; excludes_nodata",
        "n_patches": int(n_patches),
        "mean_patch_size_m2": green_area / n_patches if n_patches else 0,
        "largest_patch_index": float(max(sizes) / green_area) if green_area else 0,
        "large_patch_area_m2": large_area,
        "n_large_patches": int(sum(size >= large_area for size in sizes)),
    }


def _vectorize(green, profile):
    from rasterio import features
    from shapely.geometry import shape
    from pyproj import CRS
    crs = CRS.from_user_input(profile["crs"])
    patches = []
    for geom, val in features.shapes(green.astype("uint8"), mask=green > 0,
                                     transform=profile["transform"]):
        if val != 1:
            continue
        poly = shape(geom)
        if crs.is_projected:
            factor_x, factor_y = [axis.unit_conversion_factor for axis in crs.axis_info[:2]]
            area = poly.area * factor_x * factor_y
        else:
            area = abs(crs.get_geod().geometry_area_perimeter(poly)[0])
        patches.append({"geometry": geom, "area_m2": float(area),
                        "compactness": float(4 * 3.14159 * poly.area / (poly.length ** 2 + 1e-6)),
                        "centroid": [poly.centroid.x, poly.centroid.y]})
    return patches


def _build_corridors(patches, ls_cfg):
    """简化廊道：在最近的大斑块对之间绘制 LineString。"""
    if len(patches) < 2:
        return []
    large = sorted(patches, key=lambda p: -p.get("area_m2", 0))[:10]
    corridors = []
    for i in range(len(large) - 1):
        a = large[i]["centroid"]; b = large[i + 1]["centroid"]
        corridors.append({
            "geometry": {"type": "LineString", "coordinates": [list(a), list(b)]},
            "start_patch_area_m2": large[i]["area_m2"],
            "end_patch_area_m2": large[i + 1]["area_m2"],
            "suggestion": "沿线增补行道树/口袋公园以增强连通",
        })
    return corridors


def _detect_weak_areas(green, pop_arr, service_m, eq_cfg, profile, valid):
    """A raster-distance approximation, not walking-network accessibility."""
    import numpy as np
    from scipy import ndimage as ndi
    if pop_arr is None:
        return []
    pixel_m = float(np.sqrt(_pixel_areas(profile)[valid].mean()))
    distance_m = (ndi.distance_transform_edt(green == 0, sampling=pixel_m)
                  if green.any() else np.full(green.shape, np.inf))
    short_service = valid & (distance_m > service_m) & (pop_arr > np.percentile(pop_arr[valid], 70))
    weak = _vectorize(_clean(short_service.astype("uint8"), 10), profile)
    for item in weak:
        item.update(reason=f"高人口 + 栅格近似距离 {service_m:g}m 内无绿地",
                    suggestion="建议优先选址口袋公园或社区花园")
    return weak


def _read_optional_tif(path, profile, valid):
    if not path:
        return None
    import numpy as np
    import rasterio
    with rasterio.open(path) as src:
        if (src.crs != profile["crs"] or src.transform != profile["transform"] or
                (src.height, src.width) != valid.shape):
            raise ValueError("人口栅格必须先与影像 CRS、transform 和网格大小对齐；不能按数组左上角裁切")
        return np.where(valid, src.read(1, masked=True).filled(0), 0).astype("float64")


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


def _write_geojson(path: Path, items, profile) -> Path:
    from rasterio.warp import transform_geom
    fc = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": transform_geom(profile["crs"], "EPSG:4326", it["geometry"]),
         "properties": {k: v for k, v in it.items() if k not in {"geometry", "centroid"}}}
        for it in items
    ]}
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_report(path: Path, metrics, patches, weak) -> Path:
    target = metrics.get("target_per_capita_m2", 15)
    pc = metrics.get("per_capita_green_m2")
    pc_line = f"{pc:.2f} m²/人" if isinstance(pc, (int, float)) else "未提供人口数据"
    content = [
        "# 城市绿地生态评估报告",
        "",
        f"- 绿地覆盖率: **{metrics.get('green_coverage_pct', 0):.2f}%**",
        f"- 绿地总面积: {metrics.get('green_area_m2', 0):.0f} m²",
        f"- 斑块总数: {metrics.get('n_patches', 0)}",
        f"- 平均斑块面积: {metrics.get('mean_patch_size_m2', 0):.0f} m²",
        f"- 最大斑块占比(LPI): {metrics.get('largest_patch_index', 0):.3f}",
        f"- 大斑块数量 (面积 ≥ {metrics['large_patch_area_m2']:g} m²): {metrics.get('n_large_patches', 0)}",
        f"- 边界内有效栅格面积: {metrics['aoi_area_m2']:.0f} m²（像元中心落入边界，排除 NoData）",
        f"- 人均绿地: {pc_line}（目标 {target} m²/人）",
        "",
        "## 薄弱区域",
        ("未提供人口栅格，本次未评估服务薄弱区。" if metrics.get("weak_area_status") == "not_evaluated_no_population" else
         f"识别出 **{len(weak)}** 处栅格距离近似的服务不足区域；不代表步行网络可达性。"),
        "",
        "## 优化建议",
        "1. 对 LPI 偏低的区域进行生态廊道贯通，提升连通性",
        "2. 人均绿地不足的街区优先布设 1000m² 以上的社区公园",
        "3. 保护平均斑块面积前 10% 的大型绿地，严禁蚕食",
    ]
    path.write_text("\n".join(content), encoding="utf-8")
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="城市绿地生态系统评估技能")
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
            "outputs_dir", "./outputs")) / "urban-greenspace-assessment"
        try:
            result = run(inputs, config, out_dir)
        except Exception as exc:
            print(json.dumps({
                "ok": False,
                "error": "script_execution_failed",
                "message": str(exc),
                "next_step": "请补充脚本支持的输入：含红光与近红外波段的多光谱 GeoTIFF，或已有绿地二值掩膜 green_mask；不要临时编写自定义脚本替代本 skill 脚本。"
            }, ensure_ascii=False, indent=2))
            return 3
    else:
        result = {"ok": True, "note": "report action 由调用方直接读取 run 的 markdown"}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
