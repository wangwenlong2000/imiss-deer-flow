---
name: building-footprint-extraction
description: >
  Extracts building footprints from high-resolution optical remote sensing imagery using morphological analysis. Generates building polygon GeoJSON with area statistics and category classification. Invoke when the user mentions "建筑轮廓", "建筑提取", "建筑面积", "building footprint", "建筑普查", "违建识别", "建筑覆盖" or similar triggers.
license: MIT
---

# 建筑轮廓提取技能（building-footprint-extraction）

## 1. 技能概述

本技能基于高分辨率光学遥感影像，利用亮度阈值与形态学分析提取建筑轮廓，生成建筑面积统计与 GeoJSON 矢量，支持建筑普查、违建识别与城市更新规划。

## 2. 输入要求

| 变量名 | 类型 | 是否必需 | 说明 |
|--------|------|---------|------|
| `image_optical` | file (GeoTIFF) | 是 | 高分辨率光学影像（建议分辨率 ≤ 2m，已完成正射校正） |
| `aoi` | file (GeoJSON / Shapefile) | 否 | 关注区域边界 |
| `method` | string | 否 | `threshold`（阈值法）/ `morphology`（形态学），默认 `morphology` |
| `min_building_area_m2` | float | 否 | 最小建筑面积（平方米），默认 20 |

## 3. 处理流程

1. **输入校验**：`--action check-inputs`
2. **建筑提取**：基于亮度特征提取建筑候选区域
3. **形态学优化**：开运算去噪 + 闭运算填充空洞
4. **轮廓提取**：连通域分析，生成建筑多边形
5. **面积过滤**：滤除小于最小面积阈值的图斑
6. **成果输出**：building_mask.tif、building_footprints.geojson、building_stats.csv、report.md

## 4. 调用示例

```bash
python /home/ubuntu/skills/building-footprint-extraction/scripts/main.py \
  --action run \
  --inputs-json '{"image_optical":"/path/to/hr_image.tif","method":"morphology"}' \
  --output-dir /tmp/outputs/bfe
```

## 5. 输出文件

| 文件 | 含义 |
|------|------|
| `building_mask.tif` | 建筑二值掩膜 |
| `building_footprints.geojson` | 建筑轮廓矢量，属性含 area_m2、category |
| `building_stats.csv` | 建筑统计（ID、坐标、面积、类别） |
| `report.md` | 建筑提取统计与应用建议 |

## 6. 治理场景

- **违法建设识别**：与规划许可数据叠加，识别未批先建建筑
- **建筑普查核验**：辅助不动产登记与建筑普查数据核实
- **城市更新规划**：量化待改造区域建筑密度与分布
- **应急救援支持**：灾后快速评估建筑分布，辅助救援调度
