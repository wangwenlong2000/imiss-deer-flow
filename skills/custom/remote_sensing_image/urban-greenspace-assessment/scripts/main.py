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
    profile, green = _extract_green(inputs, ndvi_th, min_patch)

    # ---- 景观指数 ----
    metrics = _compute_landscape_metrics(green, ls_cfg)

    # ---- 斑块矢量 ----
    patches = _vectorize(green, profile)

    # ---- 生态廊道（简化：连接大斑块的骨架线）----
    corridors = _build_corridors(patches, ls_cfg)

    # ---- 服务可达性 & 人均绿地 ----
    pop_arr = _read_optional_tif(inputs.get("population"))
    total_pop = float(pop_arr.sum()) if pop_arr is not None else 0.0
    total_green_m2 = metrics["green_area_m2"]
    per_capita = total_green_m2 / total_pop if total_pop > 0 else None
    weak = _detect_weak_areas(green, pop_arr, service_m, eq_cfg)

    # ---- 输出 ----
    outputs: dict[str, str] = {}
    outputs["green_mask"] = str(_write_geotiff(output_dir / "green_mask.tif", green, profile, "uint8"))
    outputs["green_patches"] = str(_write_geojson(output_dir / "green_patches.geojson", patches))
    outputs["corridors"] = str(_write_geojson(output_dir / "corridors.geojson", corridors))
    metrics_out = {
        **metrics,
        "population": total_pop,
        "per_capita_green_m2": per_capita,
        "target_per_capita_m2": float(eq_cfg.get("target_per_capita_m2", 15)),
        "service_distance_m": service_m,
    }
    (output_dir / "metrics.json").write_text(
        json.dumps(metrics_out, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["metrics"] = str(output_dir / "metrics.json")
    outputs["weak_areas"] = str(_write_geojson(output_dir / "weak_areas.geojson", weak))
    outputs["report"] = str(_write_report(output_dir / "report.md", metrics_out, patches, weak))

    return {"ok": True, **metrics_out, "outputs": outputs}


# ---- 子函数 ----
def _extract_green(inputs, ndvi_th, min_patch):
    import rasterio  # type: ignore
    if inputs.get("green_mask") and Path(inputs["green_mask"]).exists():
        with rasterio.open(inputs["green_mask"]) as src:
            return src.profile, (src.read(1) > 0).astype("uint8")
    if Path(inputs["rs_image"]).exists():
        with rasterio.open(inputs["rs_image"]) as src:
            if src.count < 4:
                raise ValueError(
                    f"当前脚本需要至少 4 个波段以读取红光和近红外波段，检测到 {src.count} 个波段。"
                    "RGB-only 影像无法计算真实 NDVI，请提供含 NIR 的多光谱影像或 green_mask。"
                )
            arr = src.read().astype("float32")
            profile = src.profile
            red = arr[2]; nir = arr[3]
            ndvi = (nir - red) / (nir + red + 1e-6)
            green = (ndvi > ndvi_th).astype("uint8")
            green = _clean(green, min_patch)
            return profile, green
    raise FileNotFoundError("未找到可用的 rs_image 或 green_mask")


def _clean(binary, min_patch):
    try:
        from scipy import ndimage as ndi
        labeled, n = ndi.label(binary)
        sizes = ndi.sum(binary, labeled, range(n + 1))
        keep = sizes >= min_patch
        keep[0] = False
        return keep[labeled].astype("uint8")
    except ImportError:
        return binary


def _compute_landscape_metrics(green, ls_cfg):
    import numpy as np
    total = int(green.size)
    green_pixels = int(green.sum())
    pland = green_pixels / total if total else 0
    # 以 10m 像元估算面积（合理的 Sentinel-2 默认）
    pixel_m2 = 100.0
    green_area_m2 = green_pixels * pixel_m2

    try:
        from scipy import ndimage as ndi
        labeled, n_patches = ndi.label(green)
        sizes = ndi.sum(green, labeled, range(1, n_patches + 1))
    except ImportError:
        n_patches = 1 if green_pixels > 0 else 0
        sizes = [green_pixels]

    mps_m2 = float((sum(sizes) / n_patches) * pixel_m2) if n_patches else 0.0
    lpi = float(max(sizes) * pixel_m2 / green_area_m2) if green_area_m2 > 0 else 0.0
    large_area = float(ls_cfg.get("large_patch_area_m2", 10000))
    n_large = int(sum(1 for s in sizes if s * pixel_m2 >= large_area))

    return {
        "green_coverage_pct": pland * 100.0,
        "green_area_m2": green_area_m2,
        "n_patches": int(n_patches),
        "mean_patch_size_m2": mps_m2,
        "largest_patch_index": lpi,
        "n_large_patches": n_large,
    }


def _vectorize(green, profile):
    patches: list[dict[str, Any]] = []
    try:
        from rasterio import features  # type: ignore
        from shapely.geometry import shape  # type: ignore
        if profile is not None:
            for geom, val in features.shapes(green.astype("uint8"), transform=profile.get("transform")):
                if val != 1:
                    continue
                poly = shape(geom)
                patches.append({
                    "geometry": geom,
                    "area_m2": float(poly.area),
                    "compactness": float(4 * 3.14159 * poly.area / (poly.length ** 2 + 1e-6)),
                    "centroid": [poly.centroid.x, poly.centroid.y],
                })
            return patches
    except ImportError:
        pass
    try:
        import numpy as np
        from scipy import ndimage as ndi
        labeled, n = ndi.label(green)
        for i in range(1, n + 1):
            ys, xs = np.where(labeled == i)
            if len(ys) == 0:
                continue
            patches.append({
                "geometry": {"type": "Polygon", "coordinates": [[
                    [float(xs.min()), float(ys.min())], [float(xs.max()), float(ys.min())],
                    [float(xs.max()), float(ys.max())], [float(xs.min()), float(ys.max())],
                    [float(xs.min()), float(ys.min())],
                ]]},
                "area_m2": float(len(ys) * 100),
                "compactness": 0.5,
                "centroid": [float(xs.mean()), float(ys.mean())],
            })
    except ImportError:
        pass
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


def _detect_weak_areas(green, pop_arr, service_m, eq_cfg):
    """粗略检测绿地服务薄弱区：人口栅格高值但邻域无绿地。"""
    import numpy as np
    weak = []
    try:
        from scipy import ndimage as ndi
        if pop_arr is None:
            pop_arr = np.ones_like(green, dtype="float32")
        pop_arr = pop_arr[:green.shape[0], :green.shape[1]] if pop_arr.shape != green.shape else pop_arr
        kernel = max(3, int(service_m / 50))  # 像元近似
        green_dil = ndi.maximum_filter(green, size=kernel)
        short_service = (green_dil == 0) & (pop_arr > np.percentile(pop_arr, 70))
        labeled, n = ndi.label(short_service)
        for i in range(1, n + 1):
            ys, xs = np.where(labeled == i)
            if len(ys) < 10:
                continue
            weak.append({
                "geometry": {"type": "Polygon", "coordinates": [[
                    [float(xs.min()), float(ys.min())], [float(xs.max()), float(ys.min())],
                    [float(xs.max()), float(ys.max())], [float(xs.min()), float(ys.max())],
                    [float(xs.min()), float(ys.min())],
                ]]},
                "reason": "高人口 + 500m 内无绿地",
                "suggestion": "建议优先选址口袋公园或社区花园",
            })
    except ImportError:
        pass
    return weak


def _read_optional_tif(path):
    if not path:
        return None
    try:
        import rasterio  # type: ignore
        with rasterio.open(path) as src:
            return src.read(1).astype("float32")
    except Exception:
        return None


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


def _write_geojson(path: Path, items) -> Path:
    fc = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": it["geometry"],
         "properties": {k: v for k, v in it.items() if k != "geometry"}}
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
        f"- 大斑块数量 (>1 ha): {metrics.get('n_large_patches', 0)}",
        f"- 人均绿地: {pc_line}（目标 {target} m²/人）",
        "",
        "## 薄弱区域",
        f"识别出 **{len(weak)}** 处绿地服务不足区域，建议按人流密度和步行可达性排序后新增口袋公园。",
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
