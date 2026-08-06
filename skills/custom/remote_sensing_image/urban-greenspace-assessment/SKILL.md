---
name: "V1+urban-greenspace-assessment"
description: >
  面向城市绿地生态系统评估的遥感 Skill。仅用于现有脚本 scripts/main.py 能完整处理的任务：基于含红光与近红外波段的单期多光谱 GeoTIFF，或已有可读绿地二值掩膜 green_mask，评估城市绿地覆盖率、绿地斑块格局、景观连通性、生态廊道、15 分钟公园服务覆盖和人均绿地公平性。当用户提出绿地覆盖率、公园城市、生态廊道、人均绿地、15 分钟公园圈、NDVI 绿地提取、绿地服务薄弱区识别、绿地优化建议等任务且输入满足脚本要求时使用。不要用于 RGB-only 影像绿地提取、两期或多时相绿地变化检测、城市热岛反演、水质反演、违建/渣土识别、纯影像下载，或任何需要 AI 临时编写新脚本才能完成的任务。
---

# V1+城市绿地生态系统评估 Skill

本 skill 是 `urban-greenspace-assessment` 的 V1 规范化版本，遵循“先判断任务边界，再选择处理链路，再运行脚本，最后输出可回溯结果”的遥感 skill 开发规范。执行时只能使用本 skill 已有脚本，不得为弥补脚本能力缺口临时编写自定义分析脚本。

## 一、角色定位

本 skill 负责对城市或片区内的绿地生态系统进行单期现状评估，核心能力包括：

- 从含红光与近红外波段的多光谱遥感影像，或已有可读绿地二值掩膜提取绿地范围。
- 计算绿地覆盖率、斑块数量、平均斑块面积、最大斑块占比等景观指标。
- 识别大斑块之间的潜在生态廊道或踏脚石连通路径。
- 在提供人口数据时评估人均绿地和 15 分钟公园服务覆盖不足区域。
- 生成 `green_mask.tif`、`green_patches.geojson`、`corridors.geojson`、`weak_areas.geojson`、`metrics.json` 和 `report.md`。

本 skill 不是通用遥感问答、影像下载、绿地变化检测、RGB-only 绿地识别、城市热岛分析或临时代码开发 skill。

## 二、适用任务

当用户提出以下需求时，优先使用本 skill：

- 评估某城区、街道、园区或规划范围的绿地覆盖率。
- 基于 NDVI 或已有绿地掩膜提取城市绿地斑块。
- 计算绿地斑块格局、景观连通性、最大斑块占比、平均斑块面积。
- 分析公园城市、生态廊道、人均绿地、15 分钟公园圈。
- 融合人口栅格或网格化人口数据，识别绿地服务薄弱区。
- 输出绿地优化建议、口袋公园补点建议、生态廊道贯通建议。

## 三、不适用任务

以下场景不要使用本 skill：

- 两期或多时相绿地增减变化检测，应转给绿地变化监测或通用变化检测 skill。
- 城市热岛强度、地表温度、热风险分析，应转给城市热岛评估 skill。
- 水体、水质、蓝绿空间综合水环境反演，应转给水体或水质遥感 skill。
- 违建、新增建设、渣土堆场、土地用途变更识别，应转给城市变化检测或目标识别 skill。
- 只需要检索、下载、筛选遥感影像，应转给遥感数据检索 skill。
- 用户只提供 RGB 影像且没有 NIR 波段，也没有已有 `green_mask`；现有脚本不能计算真实 NDVI，应提示补充含近红外波段的多光谱 GeoTIFF 或绿地二值掩膜。
- 用户目标虽然是绿地评估，但需要使用脚本未实现的 ExG、深度学习分割、道路网络 15 分钟可达性、阻力面生态廊道等方法；除非已有其他专门脚本，否则不要使用本 skill 伪装完成。
- 用户只问 NDVI 概念、绿地规划概念，不需要处理数据时，可使用通用解释流程。

## 四、输入解析

优先提取并检查以下输入：

| 输入键 | 必需 | 类型 | 用途 |
|---|---:|---|---|
| `rs_image` | 条件必需 | GeoTIFF | 多光谱遥感影像，必须至少 4 个波段并包含红光和近红外波段，用于 NDVI 提取；RGB-only 不支持 |
| `green_mask` | 条件必需 | GeoTIFF | 已有可读绿地二值掩膜；提供后可跳过 NDVI 提取 |
| `boundary` | 必需 | GeoJSON/Shapefile/GPKG | 行政区、街道、社区或自定义 AOI 边界 |
| `population` | 可选 | GeoTIFF/CSV | 人口栅格或网格数据，用于人均绿地和服务公平性 |
| `ndvi_threshold` | 可选 | number | NDVI 绿地阈值，默认读取 `config.yaml` |
| `service_distance_m` | 可选 | number | 服务半径，默认 500m |

最小输入集：

- 若没有已有绿地掩膜：`rs_image` + `boundary`。
- 若已有绿地掩膜：`green_mask` + `boundary`，`rs_image` 可选。
- 若需要人均绿地或 15 分钟公园服务公平性：再提供 `population`。

如果用户只提供 RGB 三波段影像，应停止执行并提示补充输入；不得改用 ExG、可见光阈值或新写脚本替代 NDVI 工作流。

## 五、默认工作流

1. 判断用户任务是否属于城市绿地生态系统单期评估。
2. 解析输入，优先检查 `green_mask`；没有 `green_mask` 时检查 `rs_image` 是否可用于 NDVI，且至少包含 4 个波段。
3. 如输入不足或输入超出脚本能力，先运行 `check-inputs` 或直接列出缺失项/无效项，不编造结果。
4. 根据任务目标读取最小必要 references：
   - 普通绿地覆盖和景观指标：读 `references/greenspace-assessment-guide.md`。
   - 数据质量、阈值、CRS、分辨率或云影问题：再读 `references/data-quality.md`。
5. 按明确命令调用 `scripts/main.py`；不得创建新的 `custom_analysis.py` 或其他临时脚本替代。
6. 检查输出文件是否存在，并读取 `metrics.json` 和 `report.md` 的关键结果。
7. 对用户输出直接结论、数据与方法、结果路径、质量限制和下一步建议。

## 六、资源导航

### `scripts/main.py`

核心脚本，支持三个 action：

- `check-inputs`：检查必需输入是否存在。
- `run`：执行绿地提取、景观指标、生态廊道和服务薄弱区识别。
- `report`：保留动作；实际报告由 `run` 生成的 `report.md` 提供。

调用前应在本 skill 根目录执行命令。

脚本能力边界：

- 支持：含红光与近红外波段的多光谱 GeoTIFF，或已有可读绿地二值 GeoTIFF 掩膜。
- 不支持：RGB-only 影像直接绿地提取、ExG 可见光绿地指数、深度学习绿地分割、网络步行可达性、严格阻力面生态廊道。
- 不支持时：返回缺失/无效输入说明，要求用户补充脚本支持的输入，不能由 AI 自行编写新脚本。

### `config.yaml`

默认参数文件，包含 NDVI 阈值、最小斑块像元数、大斑块面积阈值、人均绿地目标值、服务距离和输出目录。

### `references/`

- `references/greenspace-assessment-guide.md`：任务方法、路由、参数解释、输出解释。
- `references/data-quality.md`：输入影像质量、波段、CRS、分辨率、阈值和不确定性检查。

### `templates/`

- `templates/greenspace-assessment-report.md`：最终综合评估报告模板。

## 七、任务路由

### 路由 A：用户提供遥感影像，要求绿地覆盖率或景观指标

1. 检查 `rs_image` 与 `boundary`。
2. 如果 `rs_image` 是 RGB-only 或不可读 GeoTIFF，停止并要求用户补充含 NIR 的多光谱影像或 `green_mask`。
3. 读取 `references/greenspace-assessment-guide.md`。
4. 视季节和地物差异选择 `ndvi_threshold`，默认 0.30；冬季或北方落叶季可降至 0.15-0.25。
5. 运行 `scripts/main.py --action run`。
6. 输出绿地覆盖率、绿地面积、斑块数、平均斑块面积、LPI、大斑块数量和结果路径。

### 路由 B：用户提供已有绿地掩膜，要求格局或连通性分析

1. 检查 `green_mask` 与 `boundary`。
2. 不重新计算 NDVI，直接使用掩膜。
3. 读取 `references/greenspace-assessment-guide.md` 中“景观指标解释”部分。
4. 运行脚本并重点解释 `green_patches.geojson`、`corridors.geojson` 和 `metrics.json`。

### 路由 C：用户提供人口数据，要求人均绿地或 15 分钟公园圈

1. 检查 `population` 是否存在且可读。
2. 读取 `references/data-quality.md` 中“人口数据与空间对齐”部分。
3. 运行脚本并重点解释 `per_capita_green_m2`、`weak_areas.geojson`。
4. 如果人口数据无法读取或 CRS/分辨率无法确认，必须说明公平性结果置信度有限。

### 路由 D：用户要求绿地优化建议或规划报告

1. 先完成路由 A/B/C 中对应分析。
2. 使用 `templates/greenspace-assessment-report.md` 整理结果。
3. 建议必须基于输出指标，不得空泛生成规划口号。

## 八、精确调用命令

### 输入检查

```bash
cd /path/to/V1+urban-greenspace-assessment && python scripts/main.py \
  --action check-inputs \
  --inputs-json "{\"rs_image\":\"/path/to/image.tif\",\"boundary\":\"/path/to/boundary.geojson\"}"
```

### 完整运行

```bash
cd /path/to/V1+urban-greenspace-assessment && python scripts/main.py \
  --action run \
  --inputs-json "{\"rs_image\":\"/path/to/image.tif\",\"boundary\":\"/path/to/boundary.geojson\",\"population\":\"/path/to/pop.tif\",\"ndvi_threshold\":0.30,\"service_distance_m\":500}" \
  --config config.yaml \
  --output-dir archive/2026-06-22-greenspace-assessment
```

### 使用已有绿地掩膜

```bash
cd /path/to/V1+urban-greenspace-assessment && python scripts/main.py \
  --action run \
  --inputs-json "{\"green_mask\":\"/path/to/green_mask.tif\",\"boundary\":\"/path/to/boundary.geojson\",\"service_distance_m\":500}" \
  --config config.yaml \
  --output-dir archive/2026-06-22-greenspace-assessment
```

## 九、输出要求

对用户输出时，默认包含：

1. 直接结论：绿地覆盖率、绿地面积、斑块数量、人均绿地或薄弱区数量。
2. 数据与方法：输入影像/掩膜、AOI、NDVI 阈值、服务半径、人口数据是否使用。
3. 结果文件：列出 `green_mask`、`green_patches`、`corridors`、`weak_areas`、`metrics`、`report` 路径。
4. 质量控制：说明是否存在云影、季节、分辨率、CRS、人口数据对齐等限制。
5. 优化建议：只根据指标和薄弱区结果提出。

不要直接倾倒脚本 JSON；应把结果转成可读结论。

## 十、质量控制

必须检查：

- `rs_image` 或 `green_mask` 文件是否真实存在。
- 影像是否至少具备红光与近红外波段；脚本默认多波段影像第 3/4 波段为红光/近红外，其他传感器需谨慎说明。
- `boundary` 是否存在；当前脚本主要检查存在性，精确裁剪需后续增强。
- 输出目录中 `metrics.json` 和 `report.md` 是否生成。
- 若使用人口数据，必须说明人口栅格与绿地栅格的空间对齐存在近似处理。
- 当前脚本默认 10m 像元面积估算面积；如果输入不是 10m 分辨率，面积指标需在输出中标注“需按真实像元大小复核”。

## 十一、失败处理

- 缺少 `rs_image`/`green_mask` 或 `boundary`：先返回缺失项，不继续声称完成评估。
- 文件路径不存在：报告具体路径。
- RGB-only 影像、不可读 GeoTIFF、缺少 NIR 波段：明确说明现有脚本无法解决，要求用户补充含 NIR 的多光谱影像或已有绿地二值掩膜。
- rasterio/scipy 等依赖不可用：报告依赖缺失，不得降级生成合成评估成果。
- 输出文件缺失：说明脚本失败并报告 stderr 关键错误，不编造指标。
- 影像云量高、季节不适宜或阈值不稳：明确提示需要人工复核阈值或提供更优时相影像。

## 十二、禁止行为

- 不得在没有运行脚本或没有读取真实输出时给出真实面积、覆盖率或人均绿地结论。
- 不得编写 `custom_analysis.py`、临时 Python、ExG 可见光分析脚本或其他新脚本来补足本 skill 脚本不能解决的输入。
- 不得把 RGB-only 影像分析包装成 NDVI 绿地生态评估。
- 不得把降级、演示或合成结果当作真实遥感评估成果。
- 不得忽略输入影像分辨率差异对面积指标的影响。
- 不得把 500m 缓冲等同于真实步行网络可达性；应说明它是遥感/空间近似。
- 不得把本 skill 用于绿地增减变化检测或热岛分析。
