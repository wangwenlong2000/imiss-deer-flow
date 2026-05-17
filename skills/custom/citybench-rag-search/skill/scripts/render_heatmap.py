#!/usr/bin/env python3
"""
CityBench 检索结果热力图渲染
=============================

功能：把 search.py 输出的 search_results.jsonl 渲染成"经纬度散点 + 圆圈大小/颜色编码"的 PNG 地图。
本脚本是 RAG 检索（search.py）的下游：
    search_results.jsonl → render_heatmap.py → heatmap.png

气泡大小：按 checkin_count 平方根缩放
气泡颜色：异常区域 → 红/橙；正常 → 蓝/绿（按 wow_change_pct 渐变）
注记：业务地名 + 签到次数

依赖：优先使用 matplotlib；如果 matplotlib/NumPy 不可用，自动降级到 Pillow 兜底输出 PNG。

CLI:
    python3 render_heatmap.py \\
        --input /path/to/search_results.jsonl \\
        --output /tmp/heatmap.png \\
        --title "上海2012年6月早高峰签到热点"
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import logging
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# 同目录的 landmarks 模块
sys.path.insert(0, str(Path(__file__).parent.resolve()))
try:
    import landmarks  # type: ignore
except ImportError:
    landmarks = None


# ───────────────────── 城市底图边界（用于 axis 范围） ─────────────────────

CITY_BBOX = {
    "Shanghai":  {"lat_min": 30.95, "lat_max": 31.55, "lon_min": 121.05, "lon_max": 121.95,
                  "center": (31.23, 121.47), "label": "上海"},
    "Beijing":   {"lat_min": 39.70, "lat_max": 40.20, "lon_min": 116.10, "lon_max": 116.70,
                  "center": (39.91, 116.40), "label": "北京"},
    "Guangzhou": {"lat_min": 22.95, "lat_max": 23.40, "lon_min": 113.10, "lon_max": 113.55,
                  "center": (23.13, 113.32), "label": "广州"},
    "Shenzhen":  {"lat_min": 22.45, "lat_max": 22.75, "lon_min": 113.85, "lon_max": 114.30,
                  "center": (22.55, 114.06), "label": "深圳"},
}

# 上海主要参考线（黄浦江示意 —— 仅画一条贝塞尔近似曲线作为视觉参考）
SHANGHAI_HUANGPU_RIVER = [
    (31.40, 121.50), (31.34, 121.49), (31.27, 121.50), (31.24, 121.51),
    (31.22, 121.49), (31.20, 121.47), (31.17, 121.46), (31.13, 121.43),
    (31.10, 121.39), (31.07, 121.34),
]


def _setup_chinese_font():
    """注册中文字体，支持 Noto Sans CJK / 文泉驿等。"""
    import matplotlib
    import matplotlib.font_manager as fm
    candidates = ["Noto Sans CJK SC", "Noto Sans CJK JP", "WenQuanYi Zen Hei", "WenQuanYi Micro Hei",
                  "SimHei", "Microsoft YaHei", "PingFang SC", "Heiti SC"]
    available = {f.name for f in fm.fontManager.ttflist}
    for c in candidates:
        if c in available:
            matplotlib.rcParams["font.sans-serif"] = [c] + matplotlib.rcParams["font.sans-serif"]
            matplotlib.rcParams["axes.unicode_minus"] = False
            return c
    return None


def render(
    input_path: Path,
    output_path: Path,
    title: str,
    show_river: bool = True,
) -> dict:
    try:
        info = _render_matplotlib(
            input_path=input_path,
            output_path=output_path,
            title=title,
            show_river=show_river,
        )
        info["renderer"] = "matplotlib"
        return info
    except Exception as exc:
        logger.info("使用 Pillow 兜底渲染地图热力图（matplotlib 当前不可用）。")
        return _render_pillow(
            input_path=input_path,
            output_path=output_path,
            title=title,
            show_river=show_river,
            fallback_reason=str(exc),
        )


def _render_matplotlib(
    input_path: Path,
    output_path: Path,
    title: str,
    show_river: bool = True,
) -> dict:
    # Some demo machines have a matplotlib build compiled against a different
    # NumPy ABI. Suppress import-time stderr noise so fallback remains clean.
    with contextlib.redirect_stderr(io.StringIO()):
        import matplotlib
        matplotlib.use("Agg")  # 无显示环境
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
        from matplotlib.lines import Line2D

    font_name = _setup_chinese_font()
    if not font_name:
        logger.warning("未找到中文字体，标签可能显示为方框")

    # 读 search_results.jsonl
    points = []
    cities_in_data: set[str] = set()
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            src = r.get("source", r)
            meta = src.get("meta", {})
            geo = meta.get("geo_scope", {})
            feat = meta.get("features", {})
            city = geo.get("city", "")
            cities_in_data.add(city)

            # 优先用 evidence 自带的 lat/lon，其次查表
            lat = geo.get("lat")
            lon = geo.get("lon")
            landmark = geo.get("landmark")
            if (lat is None or lon is None) and landmarks is not None:
                info = landmarks.lookup(city, geo.get("geohash", ""))
                if info:
                    lat = info["lat"]
                    lon = info["lon"]
                    landmark = info["landmark"]
            if lat is None or lon is None:
                continue

            points.append({
                "lat": lat,
                "lon": lon,
                "city": city,
                "geohash": geo.get("geohash", ""),
                "landmark": landmark or geo.get("geohash", ""),
                "count": feat.get("checkin_count", 0) or 0,
                "users": feat.get("unique_users", 0) or 0,
                "wow": feat.get("wow_change_pct"),
                "anomaly": bool(feat.get("anomaly_flag", False)),
                "rrf": r.get("rrf_score", 0),
            })

    if not points:
        raise ValueError(f"输入文件 {input_path} 没有可绘制的点（缺 lat/lon）")

    # 同 (city, geohash) 聚合多条记录（多日累加）
    agg: dict[tuple, dict] = {}
    for p in points:
        k = (p["city"], p["geohash"])
        if k not in agg:
            agg[k] = dict(p, hit_days=1, total_count=p["count"], any_anomaly=p["anomaly"])
        else:
            a = agg[k]
            a["hit_days"] += 1
            a["total_count"] += p["count"]
            a["any_anomaly"] = a["any_anomaly"] or p["anomaly"]
    plot_points = list(agg.values())

    # 选定底图范围
    primary_city = max(cities_in_data, key=lambda c: sum(1 for p in points if p["city"] == c)) if cities_in_data else "Shanghai"
    bbox = CITY_BBOX.get(primary_city, CITY_BBOX["Shanghai"])

    # 画图
    fig, ax = plt.subplots(figsize=(11, 9), dpi=110)
    ax.set_xlim(bbox["lon_min"], bbox["lon_max"])
    ax.set_ylim(bbox["lat_min"], bbox["lat_max"])
    ax.set_aspect("equal", adjustable="box")
    ax.set_facecolor("#f4f1e8")  # 米黄底图

    # 网格
    ax.grid(True, linestyle="--", alpha=0.35, color="#888")
    ax.set_xlabel("经度 (°E)", fontsize=11)
    ax.set_ylabel("纬度 (°N)", fontsize=11)

    # 城市边界框
    ax.add_patch(Rectangle(
        (bbox["lon_min"], bbox["lat_min"]),
        bbox["lon_max"] - bbox["lon_min"],
        bbox["lat_max"] - bbox["lat_min"],
        fill=False, edgecolor="#666", linewidth=2,
    ))
    ax.text(bbox["lon_min"] + 0.01, bbox["lat_max"] - 0.02,
            f"{bbox['label']}市域示意", fontsize=12, color="#444",
            verticalalignment="top", fontweight="bold")

    # 黄浦江参考线（仅 Shanghai）
    if primary_city == "Shanghai" and show_river:
        rx = [p[1] for p in SHANGHAI_HUANGPU_RIVER]
        ry = [p[0] for p in SHANGHAI_HUANGPU_RIVER]
        ax.plot(rx, ry, color="#5a9bd4", linewidth=3.5, alpha=0.55, zorder=1)
        ax.text(rx[5] + 0.005, ry[5], "黄浦江", fontsize=9, color="#3a6e9a",
                fontweight="bold", zorder=2)

    # 主城区参考圆（外环示意）
    cx, cy = bbox["center"]
    ax.add_patch(plt.Circle((cy, cx), 0.18, fill=False, edgecolor="#aaa",
                            linestyle=":", linewidth=1.2, zorder=1))

    # 数据点
    max_count = max(p["total_count"] for p in plot_points) or 1

    for p in plot_points:
        # 大小：按 sqrt 缩放避免极值压缩
        size = 200 + (p["total_count"] / max_count) ** 0.5 * 2200

        # 颜色：异常 → 红，否则按签到强度蓝→紫
        if p["any_anomaly"]:
            color = "#d62728"  # 红
            edge = "#7a1010"
            alpha = 0.85
        else:
            intensity = p["total_count"] / max_count
            if intensity > 0.66:
                color = "#9467bd"  # 紫
            elif intensity > 0.33:
                color = "#1f77b4"  # 蓝
            else:
                color = "#2ca02c"  # 绿
            edge = "#333"
            alpha = 0.75

        ax.scatter(p["lon"], p["lat"], s=size, c=color, alpha=alpha,
                   edgecolors=edge, linewidth=1.5, zorder=4)

        # 注记
        label = f"{p['landmark']}\n{p['total_count']}次"
        if p["any_anomaly"]:
            label += " ⚠"
        ax.annotate(
            label,
            xy=(p["lon"], p["lat"]),
            xytext=(8, 8), textcoords="offset points",
            fontsize=9, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#666", alpha=0.85),
            zorder=5,
        )

    # 图例
    legend_elements = [
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#d62728",
               markeredgecolor="#7a1010", markersize=14,
               label="异常热点（环比 >100%）"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#9467bd",
               markeredgecolor="#333", markersize=12, label="高强度热点"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#1f77b4",
               markeredgecolor="#333", markersize=10, label="中强度热点"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#2ca02c",
               markeredgecolor="#333", markersize=8, label="低强度热点"),
    ]
    ax.legend(handles=legend_elements, loc="lower left", fontsize=9,
              framealpha=0.9, title="签到强度等级", title_fontsize=10)

    # 大小说明
    ax.text(
        bbox["lon_max"] - 0.01, bbox["lat_min"] + 0.02,
        f"圆圈大小 ∝ √(签到总量)\n最大值: {max_count} 次",
        fontsize=9, color="#444", ha="right",
        bbox=dict(boxstyle="round,pad=0.4", fc="white", ec="#888", alpha=0.85),
    )

    plt.title(title, fontsize=14, fontweight="bold", pad=15)
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=130, bbox_inches="tight", facecolor="white")
    plt.close()

    return {
        "output": str(output_path),
        "city": primary_city,
        "n_points": len(plot_points),
        "max_count": max_count,
        "anomaly_count": sum(1 for p in plot_points if p["any_anomaly"]),
        "font": font_name,
    }


def _load_pillow_font(size: int):
    from PIL import ImageFont

    candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/arphic/uming.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for font_path in candidates:
        path = Path(font_path)
        if path.exists():
            try:
                return ImageFont.truetype(str(path), size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def _pillow_text_box(draw, text: str, font, spacing: int = 4) -> tuple[int, int]:
    try:
        left, top, right, bottom = draw.multiline_textbbox((0, 0), text, font=font, spacing=spacing)
        return right - left, bottom - top
    except Exception:
        lines = text.splitlines() or [text]
        widths = []
        heights = []
        for line in lines:
            box = draw.textbbox((0, 0), line, font=font)
            widths.append(box[2] - box[0])
            heights.append(box[3] - box[1])
        return max(widths or [0]), sum(heights) + spacing * max(0, len(lines) - 1)


def _render_pillow(
    input_path: Path,
    output_path: Path,
    title: str,
    show_river: bool = True,
    fallback_reason: str = "",
) -> dict:
    """Render a PNG heatmap without matplotlib.

    This fallback keeps the demo reliable in environments where matplotlib or
    NumPy ABI versions are mismatched. It only needs Pillow, which is available
    in the DeerFlow runtime used for artifact/image handling.
    """
    from PIL import Image, ImageDraw

    points = []
    cities_in_data: set[str] = set()
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            src = r.get("source", r)
            meta = src.get("meta", {})
            geo = meta.get("geo_scope", {})
            feat = meta.get("features", {})
            city = geo.get("city", "")
            cities_in_data.add(city)

            lat = geo.get("lat")
            lon = geo.get("lon")
            landmark = geo.get("landmark")
            if (lat is None or lon is None) and landmarks is not None:
                info = landmarks.lookup(city, geo.get("geohash", ""))
                if info:
                    lat = info["lat"]
                    lon = info["lon"]
                    landmark = info["landmark"]
            if lat is None or lon is None:
                continue

            points.append({
                "lat": float(lat),
                "lon": float(lon),
                "city": city,
                "geohash": geo.get("geohash", ""),
                "landmark": landmark or geo.get("geohash", ""),
                "count": int(feat.get("checkin_count", 0) or 0),
                "users": int(feat.get("unique_users", 0) or 0),
                "wow": feat.get("wow_change_pct"),
                "anomaly": bool(feat.get("anomaly_flag", False)),
                "rrf": r.get("rrf_score", 0),
            })

    if not points:
        raise ValueError(f"输入文件 {input_path} 没有可绘制的点（缺 lat/lon）")

    agg: dict[tuple, dict] = {}
    for p in points:
        k = (p["city"], p["geohash"])
        if k not in agg:
            agg[k] = dict(p, hit_days=1, total_count=p["count"], any_anomaly=p["anomaly"])
        else:
            a = agg[k]
            a["hit_days"] += 1
            a["total_count"] += p["count"]
            a["any_anomaly"] = a["any_anomaly"] or p["anomaly"]
    plot_points = list(agg.values())

    primary_city = max(cities_in_data, key=lambda c: sum(1 for p in points if p["city"] == c)) if cities_in_data else "Shanghai"
    bbox = CITY_BBOX.get(primary_city, CITY_BBOX["Shanghai"])

    width, height = 1320, 980
    margin_left, margin_right, margin_top, margin_bottom = 105, 80, 120, 105
    img = Image.new("RGBA", (width, height), "#fffaf0")
    draw = ImageDraw.Draw(img, "RGBA")

    title_font = _load_pillow_font(34)
    label_font = _load_pillow_font(22)
    small_font = _load_pillow_font(18)
    tiny_font = _load_pillow_font(15)

    map_left = margin_left
    map_top = margin_top
    map_right = width - margin_right
    map_bottom = height - margin_bottom
    map_w = map_right - map_left
    map_h = map_bottom - map_top

    def project(lat: float, lon: float) -> tuple[float, float]:
        x = map_left + (lon - bbox["lon_min"]) / (bbox["lon_max"] - bbox["lon_min"]) * map_w
        y = map_bottom - (lat - bbox["lat_min"]) / (bbox["lat_max"] - bbox["lat_min"]) * map_h
        return x, y

    # Background and map frame.
    draw.rectangle((map_left, map_top, map_right, map_bottom), fill="#f3efe2", outline="#696969", width=3)
    for i in range(1, 6):
        x = map_left + map_w * i / 6
        y = map_top + map_h * i / 6
        draw.line((x, map_top, x, map_bottom), fill="#9b9b9b88", width=1)
        draw.line((map_left, y, map_right, y), fill="#9b9b9b88", width=1)

    title_w, _ = _pillow_text_box(draw, title, title_font)
    draw.text(((width - title_w) / 2, 32), title, fill="#252525", font=title_font)
    draw.text((map_left + 16, map_top + 14), f"{bbox['label']}市域示意", fill="#3d3d3d", font=label_font)
    draw.text((map_left + 18, map_bottom + 28), "经度 (°E)", fill="#4a4a4a", font=small_font)
    draw.text((18, map_top + map_h / 2), "纬度 (°N)", fill="#4a4a4a", font=small_font)

    # Huangpu River and outer-ring style reference line for Shanghai.
    if primary_city == "Shanghai" and show_river:
        river_points = [project(lat, lon) for lat, lon in SHANGHAI_HUANGPU_RIVER]
        if len(river_points) >= 2:
            draw.line(river_points, fill="#4b94cfaa", width=8, joint="curve")
            rx, ry = river_points[min(5, len(river_points) - 1)]
            draw.text((rx + 10, ry - 18), "黄浦江", fill="#316d9d", font=small_font)

    cx_lat, cx_lon = bbox["center"]
    cx, cy = project(cx_lat, cx_lon)
    ring_r_lon = 0.18 / (bbox["lon_max"] - bbox["lon_min"]) * map_w
    ring_r_lat = 0.18 / (bbox["lat_max"] - bbox["lat_min"]) * map_h
    draw.ellipse((cx - ring_r_lon, cy - ring_r_lat, cx + ring_r_lon, cy + ring_r_lat),
                 outline="#80808099", width=2)

    max_count = max(p["total_count"] for p in plot_points) or 1
    sorted_points = sorted(plot_points, key=lambda p: p["total_count"])
    rendered_points = []
    for p in sorted_points:
        x, y = project(p["lat"], p["lon"])
        radius = 24 + math.sqrt(p["total_count"] / max_count) * 66
        intensity = p["total_count"] / max_count
        if p["any_anomaly"]:
            fill = "#d62728cc"
            outline = "#7a1010ff"
        elif intensity > 0.66:
            fill = "#9467bdc0"
            outline = "#333333ff"
        elif intensity > 0.33:
            fill = "#1f77b4bf"
            outline = "#333333ff"
        else:
            fill = "#2ca02cbf"
            outline = "#333333ff"

        draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=fill, outline=outline, width=3)
        draw.ellipse((x - radius * 0.55, y - radius * 0.55, x + radius * 0.55, y + radius * 0.55),
                     outline="#ffffffaa", width=2)
        rendered_points.append((p, x, y, radius))

    label_offsets = {
        "wtw1z": (-95, -78),
        "wtw3s": (32, -18),
        "wtw3e": (30, 72),
        "wtw37": (-210, 56),
    }
    generic_offsets = [(24, -56), (30, 64), (-190, -52), (-205, 48), (48, 8)]
    for idx, (p, x, y, radius) in enumerate(sorted(rendered_points, key=lambda item: item[0]["total_count"], reverse=True)):
        label = f"{p['landmark']}\n{p['total_count']}次"
        if p["any_anomaly"]:
            label += " 异常"
        label_w, label_h = _pillow_text_box(draw, label, small_font)
        dx, dy = label_offsets.get(p["geohash"], generic_offsets[idx % len(generic_offsets)])
        label_x = min(max(x + dx, map_left + 6), map_right - label_w - 18)
        label_y = min(max(y + dy, map_top + 6), map_bottom - label_h - 18)
        draw.line((x, y, label_x, label_y + label_h / 2), fill="#444444aa", width=2)
        draw.rounded_rectangle(
            (label_x - 8, label_y - 6, label_x + label_w + 8, label_y + label_h + 6),
            radius=8,
            fill="#fffffff0",
            outline="#5d5d5dcc",
            width=1,
        )
        draw.multiline_text((label_x, label_y), label, fill="#202020", font=small_font, spacing=4)

    # Legend and metadata footnote.
    legend_x, legend_y = map_left + 18, map_bottom - 150
    draw.rounded_rectangle((legend_x - 12, legend_y - 16, legend_x + 330, legend_y + 118),
                           radius=10, fill="#fffffff0", outline="#8b8b8bcc")
    legend_items = [
        ("#d62728cc", "异常热点（环比 >100%）"),
        ("#9467bdc0", "高强度热点"),
        ("#1f77b4bf", "中强度热点"),
        ("#2ca02cbf", "低强度热点"),
    ]
    for idx, (color, label) in enumerate(legend_items):
        yy = legend_y + idx * 30
        draw.ellipse((legend_x, yy, legend_x + 20, yy + 20), fill=color, outline="#333333")
        draw.text((legend_x + 32, yy - 2), label, fill="#333333", font=tiny_font)

    note = f"圆圈大小 ∝ √(签到总量)；最大值: {max_count} 次"
    if fallback_reason:
        note += "；渲染器: Pillow fallback"
    note_w, note_h = _pillow_text_box(draw, note, tiny_font)
    draw.rounded_rectangle((map_right - note_w - 30, map_bottom - note_h - 30, map_right - 10, map_bottom - 8),
                           radius=8, fill="#fffffff0", outline="#8b8b8bcc")
    draw.text((map_right - note_w - 20, map_bottom - note_h - 20), note, fill="#444444", font=tiny_font)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    img.convert("RGB").save(output_path, "PNG")

    return {
        "output": str(output_path),
        "city": primary_city,
        "n_points": len(plot_points),
        "max_count": max_count,
        "anomaly_count": sum(1 for p in plot_points if p["any_anomaly"]),
        "font": "Pillow",
        "renderer": "pillow",
        "fallback_reason": fallback_reason,
    }


def main() -> int:
    p = argparse.ArgumentParser(description="CityBench 检索结果地图热力图")
    p.add_argument("--input", required=True, help="search_results.jsonl 路径")
    p.add_argument("--output", required=True, help="输出 PNG 路径")
    p.add_argument("--title", default="城市签到热点分布", help="图标题")
    p.add_argument("--no-river", action="store_true", help="不画黄浦江参考线")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    info = render(
        input_path=Path(args.input),
        output_path=Path(args.output),
        title=args.title,
        show_river=not args.no_river,
    )
    print(json.dumps(info, ensure_ascii=False, indent=2))
    print(f"\n[OK] Heatmap rendered: {info['output']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
