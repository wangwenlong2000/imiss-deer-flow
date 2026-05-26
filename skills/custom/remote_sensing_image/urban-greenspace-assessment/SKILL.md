---
name: urban-greenspace-assessment
description: >
  Assesses urban green-space coverage, connectivity, service accessibility and
  ecological function completeness. Invoke when the user mentions "green space",
  "park city", "ecological corridor", "per-capita green", "NDVI", "绿地", "公园城市",
  "生态廊道", "人均绿地", "15分钟公园" or similar. The skill extracts green patches
  from high-resolution imagery, computes landscape metrics, optionally fuses
  population data for equity analysis, and outputs coverage statistics, corridor
  maps, per-capita indicators and weak-area optimization suggestions.
version: 0.1.0
category: urban-governance
domain: remote-sensing
language:
  metadata: en
  report: zh-CN
entrypoint: scripts/main.py
license: MIT
tags: [greenspace, NDVI, landscape-metrics, park-city, equity]
---

# 城市绿地生态系统评估技能（urban-greenspace-assessment）

## 1. 技能概述

系统评估城市绿地覆盖率、空间连通性、服务可达性与生态功能完整性。
支持从多光谱遥感自动提取绿地图层，也可直接接收已有的绿地矢量/栅格。

## 2. 输入要求

| 变量名 | 类型 | 必需 | 说明 |
|--------|------|:--:|------|
| `rs_image` | file (GeoTIFF) | 是 | 高分辨率遥感影像，至少含红、近红波段（用于 NDVI） |
| `boundary` | file (GeoJSON/Shapefile) | 是 | 行政区划/街道边界 |
| `population` | file (GeoTIFF/CSV) | 否 | 人口栅格或网格化人口数据（用于服务公平性） |
| `green_mask` | file (GeoTIFF) | 否 | 已有的绿地掩膜，若提供则跳过 NDVI 提取 |
| `service_distance_m` | number | 否 | 服务半径（米），默认 500m（即 15 分钟步行圈） |
| `ndvi_threshold` | number | 否 | 绿地提取 NDVI 阈值，默认 0.25 |

## 3. 用户输入引导

> 为评估您关注区域的城市绿地生态，请提供：
> 1. **遥感影像**（`rs_image`）：GeoTIFF，至少含红、近红波段；推荐 Sentinel-2、GF-2、航摄正射影像。
> 2. **行政区划**（`boundary`）：GeoJSON/Shapefile；可以是区县、街道或自定义分析单元。
> 3. （可选）**人口数据**（`population`）：若希望评估人均绿地、15 分钟公园可达性，请提供 WorldPop 风格的人口栅格或逐网格 CSV。

## 4. 处理流程

1. `check-inputs` 输入校验
2. 计算 NDVI → 阈值 + 形态学清理得到绿地栅格；若提供 `green_mask` 直接使用
3. 计算景观指数：覆盖率 PLAND、平均斑块面积 MPS、连通性 CONNECT、最大斑块占比 LPI
4. 生态廊道识别：连通大斑块的"踏脚石"路径
5. 服务可达性：对每个绿地斑块生成 buffer，与人口栅格相交计算覆盖人口
6. 输出：统计表、生态廊道图、人均绿地、薄弱区域

## 5. 参数说明（`config.yaml`）

```yaml
extraction:
  ndvi_threshold: 0.25
  min_patch_size: 100
  morph_kernel: 3
landscape:
  service_distance_m: 500
  connectivity_radius_m: 300
  large_patch_area_m2: 10000
equity:
  target_per_capita_m2: 15
```

## 6. 调用示例

```bash
python /mnt/skills/custom/remote-sensing/urban-greenspace-assessment/scripts/main.py \
  --action run \
  --inputs-json '{"rs_image":"/mnt/user-data/uploads/s2.tif","boundary":"/mnt/user-data/uploads/district.geojson","population":"/mnt/user-data/uploads/pop.tif"}' \
  --output-dir /mnt/user-data/outputs/green_20260417
```

## 7. 输出文件

| 文件 | 含义 |
|------|------|
| `green_mask.tif` | 绿地二值栅格 |
| `green_patches.geojson` | 绿地斑块矢量（面积、紧致度） |
| `corridors.geojson` | 生态廊道/踏脚石连通路径 |
| `metrics.json` | 覆盖率、人均绿地、景观指数 |
| `weak_areas.geojson` | 人均绿地不足 / 15 分钟公园覆盖不足区域 |
| `report.md` | 综合评估报告 |

## 8. 治理场景

- 公园体系规划、"公园城市"考核
- 15 分钟公园圈核查与选址
- 生态修复项目成效评估
- 绿地服务公平性（城市边缘/老旧街区）诊断

## 9. 注意事项

- NDVI 阈值与物候相关，夏季建议 0.3，冬季建议 0.15
- 人均绿地目标值默认 15 m²/人，可按城市规划标准调整
- 若仅关注结构性指标，可不提供人口数据
