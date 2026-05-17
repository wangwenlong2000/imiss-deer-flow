---
name: citybench-rag-search
description: >
  [PRIORITY-HIGH] CityBench 城市签到/轨迹证据混合检索（含隐私脱敏 + 业务地名 + 地图热力图）。
  当用户提到任何关于
  "签到/check-in/CityBench/早高峰/晚高峰/夜间活动/通勤/城市轨迹/时空轨迹/
  签到热点/签到异常/隐私脱敏/差分隐私/k-匿名" 或者询问北京、上海、广州、深圳等
  中国城市的签到活动模式时，立即使用本 skill — 不要询问澄清问题，不要询问数据集，
  不要询问时间窗口。本 skill 内置自己的索引数据 (data/sample_evidence.jsonl) 和
  geohash → 业务地名映射 (data/geohash_landmarks.json)，不需要用户上传文件。
  English keywords: CityBench, check-in data, urban mobility, trajectory evidence,
  spatiotemporal search, hotspot analysis, anomaly detection, privacy desensitization,
  differential privacy, k-anonymity, geohash to landmark, heatmap visualization.
metadata:
  short-description: CityBench 签到证据检索（脱敏 → BM25+RRF 混合检索 → 真实地图热力图）
  priority: high
  author: citybench-team
---

# CityBench RAG Search — 城市签到证据检索（含脱敏 + 地图）

## ⚠️ Agent 行为规则（必读，按顺序执行）

1. **触发即执行，不要问澄清问题**。用户提到 "签到/CityBench/早高峰/check-in/北京签到/上海夜间/异常热点" 等任何关键词时，**直接运行下面的命令**，不要问 "你的数据在哪里？"、"时间窗口是什么？" — 这些参数脚本会自己处理。
2. **报告里必须用业务地名，不要用 geohash**。`search.py` 输出的 `landmark` 字段（如 "陆家嘴金融区"）才是甲方能看懂的；`geohash`（如 "wtw3s"）只能放进括号里做技术索引。
3. **报告里必须有真实地图，不要用 ASCII 框图**。检索完了立刻用 `render_heatmap.py` 生成 PNG。
4. **如果用户给的是原始 GPS CSV，先跑脱敏**。先 `desensitize_trajectory.py` 把原始坐标转成聚合 evidence，再用 `--local-file` 喂给 `search.py`。报告里要明示已经脱敏。
5. **永远会有结果**。ES 不可达自动降级到 demo 模式，零外部依赖。

## 三步流程（标准用法）

```
[原始轨迹 CSV]                                     ← 用户上传
       │
       ▼  ① 隐私脱敏（如果输入是原始 GPS）
desensitize_trajectory.py
       │  • user_id 哈希  • Laplace 噪声  • Geohash 网格化  • k-匿名过滤
       ▼
[evidence.jsonl + privacy_report.json]
       │
       ▼  ② RAG 混合检索
search.py  (BM25 + RRF + 地名注入)
       │
       ▼
[search_results.jsonl + summary.json]            ← 已带 landmark 字段
       │
       ▼  ③ 地图热力图渲染
render_heatmap.py
       │
       ▼
[heatmap.png]                                    ← 业务地名 + 真实经纬度
```

## 立即可执行的命令（COPY EXACTLY）

### A. 标准检索 + 出图（最常用）

```bash
cd /mnt/skills/custom/citybench-rag-search/skill && python3 scripts/search.py \
  --query "上海早高峰签到热点" \
  --city Shanghai \
  --time-start "2012-06-01T00:00:00" \
  --time-end "2012-06-30T23:59:59" \
  --top-k 24 \
  --output-dir /mnt/user-data/outputs/search-result && \
python3 scripts/render_heatmap.py \
  --input /mnt/user-data/outputs/search-result/search_results.jsonl \
  --output /mnt/user-data/outputs/search-result/heatmap.png \
  --title "上海2012年6月早高峰签到热点分布"
```

### B. 异常专查 + 出图（Q2 类问题）

```bash
cd /mnt/skills/custom/citybench-rag-search/skill && python3 scripts/search.py \
  --query "签到异常波动" \
  --city Shanghai \
  --anomaly-only \
  --top-k 10 \
  --output-dir /mnt/user-data/outputs/anomaly-result && \
python3 scripts/render_heatmap.py \
  --input /mnt/user-data/outputs/anomaly-result/search_results.jsonl \
  --output /mnt/user-data/outputs/anomaly-result/heatmap.png \
  --title "上海2012年6月异常热点分布"
```

### C. 隐私脱敏（用户上传了原始 GPS CSV）

```bash
cd /mnt/skills/custom/citybench-rag-search/skill && python3 scripts/desensitize_trajectory.py \
  --input /mnt/user-data/uploads/raw_trajectory.csv \
  --output-dir /mnt/user-data/outputs/desensitized \
  --epsilon 1.0 \
  --geohash-precision 5 \
  --min-users 5 \
  --city-label Shanghai
# 接着用脱敏后的 evidence 走检索
python3 scripts/search.py \
  --query "..." \
  --local-file /mnt/user-data/outputs/desensitized/evidence.jsonl \
  --mode local \
  --top-k 10 \
  --output-dir /mnt/user-data/outputs/search-result
```

### D. 仅出图（已有 search_results.jsonl）

```bash
cd /mnt/skills/custom/citybench-rag-search/skill && python3 scripts/render_heatmap.py \
  --input /path/to/search_results.jsonl \
  --output /mnt/user-data/outputs/heatmap.png \
  --title "图标题"
```

## 三个脚本的参数表

### `search.py` — RAG 混合检索

| 参数 | 必需 | 例 | 描述 |
|---|---|---|---|
| `--query` | YES | `"北京早高峰"` | 自然语言查询 |
| `--output-dir` | YES | `/mnt/user-data/outputs/result` | 输出目录 |
| `--city` | no | `Beijing` | 过滤城市 |
| `--time-start` | no | `"2012-06-01T00:00:00"` | ISO 8601 起点 |
| `--time-end` | no | `"2012-06-30T23:59:59"` | ISO 8601 终点 |
| `--geohash` | no | `wtw3s` | geohash 前缀 |
| `--anomaly-only` | no | (flag) | 只返回异常 |
| `--top-k` | no | `24` | 返回数量 |
| `--mode` | no | `auto` / `demo` / `local` / `es` | 强制模式（默认 auto）|
| `--local-file` | no | `/path/evidence.jsonl` | local 模式的输入文件 |

**不存在**的参数：`--hour-start`、`--hour-end`、`--time-slot`、`--data-source`、`--dataset`。**不要**编造这些参数。

### `desensitize_trajectory.py` — 隐私脱敏

| 参数 | 必需 | 默认 | 描述 |
|---|---|---|---|
| `--input` | YES | — | 原始轨迹 CSV |
| `--output-dir` | YES | — | 输出目录 |
| `--epsilon` | no | 1.0 | 差分隐私 ε（越小越严）|
| `--geohash-precision` | no | 5 | geohash 位数（5≈5km）|
| `--min-users` | no | 5 | k-匿名阈值 |
| `--salt` | no | citybench_v1 | 哈希 salt |
| `--user-col` | no | user_id | 用户列名 |
| `--lat-col` | no | latitude | 纬度列名 |
| `--lon-col` | no | longitude | 经度列名 |
| `--time-col` | no | timestamp | 时间列名 |
| `--cat-col` | no | category | POI 类别列名（可选）|
| `--city-label` | no | Shanghai | 输出 evidence 的 city 字段 |

### `render_heatmap.py` — 地图热力图

| 参数 | 必需 | 描述 |
|---|---|---|
| `--input` | YES | search_results.jsonl 路径 |
| `--output` | YES | 输出 PNG 路径 |
| `--title` | no | 图标题 |
| `--no-river` | no | 不画黄浦江参考线（默认画）|

## 输出文件

### `search.py` 写入 `--output-dir`：
- `search_results.jsonl`：每行一条 JSON，**`source.meta.geo_scope` 已注入 `landmark / district / lat / lon / tags`**
- `summary.json`：query 回显、命中数、模式、top-5 预览（含 landmark 和 lat/lon）

### `desensitize_trajectory.py` 写入 `--output-dir`：
- `evidence.jsonl`：聚合后的 evidence（兼容 `search.py --local-file`）
- `privacy_report.json`：脱敏审计报告，含四道关卡的执行参数和效果统计

### `render_heatmap.py` 写入 `--output`：
- `*.png`：城市底图 + 业务地名标注 + 异常红圆 + 黄浦江/外环参考线

## 用户问题 → 参数映射

| 用户说 | 加什么参数 |
|---|---|
| "北京" / "Beijing" | `--city Beijing` |
| "上海" / "Shanghai" | `--city Shanghai` |
| "广州" / "Guangzhou" | `--city Guangzhou` |
| "深圳" / "Shenzhen" | `--city Shenzhen` |
| "2012 年 6 月" | `--time-start "2012-06-01T00:00:00" --time-end "2012-06-30T23:59:59"` |
| "早高峰" / "晚高峰" / "夜间" | 加在 `--query` 里，**不要**加 hour 参数 |
| "异常 / 波动 / 激增" | `--anomaly-only` |
| "陆家嘴 / 虹桥 / 徐家汇 / 漕河泾" | 不加 `--city`，让查表器找；或查 `geohash_landmarks.json` 反查 geohash |
| "上传了 csv 原始轨迹" | 先跑 `desensitize_trajectory.py`，再 `search.py --local-file` |
| "要地图" / "要图" / "要可视化" | 检索后必跑 `render_heatmap.py` |

## 业务地名映射（地图层）

`data/geohash_landmarks.json` 内置 4 城共 14 个 geohash → 业务地名。当前覆盖：

| 城市 | Geohash | 业务地名 |
|---|---|---|
| Shanghai | wtw3s | 陆家嘴金融区 |
| Shanghai | wtw1z | 虹桥商务区 |
| Shanghai | wtw3e | 徐家汇-人民广场 |
| Shanghai | wtw37 | 漕河泾-徐汇南 |
| Beijing | wx4g0 | 国贸-CBD核心区 |
| Beijing | wx4eu | 西单-金融街 |
| Beijing | wx4er | 天安门-王府井 |
| ... | ... | ... |

**报告写法规范：第一次出现用 "业务地名(geohash)"，之后只用业务地名。**
比如：`陆家嘴金融区(wtw3s)` → 后续直接写"陆家嘴"。

## 隐私脱敏四道关卡（脱敏脚本内置）

| 步骤 | 算法 | 默认参数 | 防护目标 |
|---|---|---|---|
| 1. user_id 哈希 | SHA256 + salt → 12-hex | salt = "citybench_v1" | 防止 user_id 反查个人 |
| 2. Laplace 坐标加噪 | 每轴独立 Laplace(0, Δf/ε) | ε=1.0, Δf=0.001° (≈100m) | 单条 GPS 防定位攻击 |
| 3. 时空桶化 | 2 小时时段 + 5 位 geohash | precision=5 (≈5km) | 防止精确轨迹链路重建 |
| 4. K-匿名过滤 | 单元 unique_users < k 丢弃 | k=5 | 防止小群体反匿名 |

agent 在生成报告时，**应当在数据来源章节明示这四步脱敏已执行**，并引用 `privacy_report.json` 里的实际效果数字（哈希用户数、平均偏移、丢弃单元数）。

## 三档检索模式说明（agent 不需要选择，自动）

| 模式 | 数据源 | 依赖 | 使用场景 |
|---|---|---|---|
| **demo** | 内置 84 条样本 | Python stdlib | 默认兜底，零配置可演示 |
| **local** | 用户给的 evidence.jsonl（脱敏脚本输出可直接用）| stdlib | 跑过 ETL/脱敏但没起 ES 时 |
| **es** | Elasticsearch + DashScope embedding | elasticsearch、openai 包 + ES 服务 + API key | 生产环境完整 RAG |

启动顺序：尝试 ES → 失败则尝试 local（如果给了 `--local-file`）→ 兜底 demo。

## Demo 数据范围

内置 evidence (`data/sample_evidence.jsonl`) 覆盖：
- **城市**：Beijing(30 条)、Shanghai(24 条)、Guangzhou(15 条)、Shenzhen(15 条)
- **时间**：2012 年 6 月，多日多时段
- **时段**：早高峰/晚高峰/夜间/午间/上午/下午/凌晨
- **异常**：约 19 条标记为 anomaly_flag=true

## 与其他 skill 的衔接

`search_results.jsonl` 可直接喂给以下下游 skill：
- `$trajectory-anomaly-detection`：异常归因 + 时序分析
- `$profile_urban_region`：城市画像生成
- `$forecast_region_flow`：流量预测

## 故障排查

- `python3: command not found` → 在命令前加 `/usr/bin/`（路径要根据 sandbox 调整）
- `result_count == 0` → 过滤太严，去掉 `--city` 重试
- `render_heatmap.py` 报"找不到中文字体" → 安装 `fonts-noto-cjk` (`apt install fonts-noto-cjk`)
- 想强制 demo 测试 → 加 `--mode demo`
- `desensitize` 输出 0 条 evidence → 用户基数不够，调小 `--min-users`
