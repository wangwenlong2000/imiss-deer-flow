# spatiotemporal_trajectory 合规标注样本 · 提交说明

- **负责人**：陈曦（zcx12873@gmail.com）
- **数据类型**：spatiotemporal_trajectory（10 类之一，结构化分组）
- **违规类型分工**（指南 §5.1）：geo_loc + re_identify
- **标注文件**：`spatiotemporal_trajectory-100.jsonl`
- **规则版本**：20260525
- **提交时间**：2026-05-29

---

## 1. 样本数量统计

### 1.1 总体分布

| 维度 | 数量 |
|---|---|
| 总样本 | **100** |
| 正样本 (is_positive=true) | **50** |
| 负样本 (is_positive=false) | **50** |
| 其中边界样本 (sample_kind=boundary) | **6** |

### 1.2 按违规类型 × 极性

| 违规类型 | 正样本 | 负样本 | 小计 |
|---|---|---|---|
| `geo_loc`（精确地理位置泄露） | 25 | 25 | 50 |
| `re_identify`（模型输出再识别风险） | 25 | 25 | 50 |
| **合计** | **50** | **50** | **100** |

### 1.3 按 trigger_gate（仅正样本）

| Gate | geo_loc | re_identify | 小计 |
|---|---|---|---|
| InputGate | 9 | 0 | 9 |
| ContextGate | 9 | 0 | 9 |
| OutputGate | 7 | 25 | 32 |

> 负样本 trigger_gate=`none`（合规内容不触发任何闸门）。
> **`re_identify`（第 9 类，模型输出再识别）主要由输出闸判断，25 条正样本主触发闸统一为 `OutputGate`**（2026-05-31 修订，对齐违规类型判定规则）。`geo_loc` 正样本按输入/上下文/输出三闸分布（9 / 9 / 7），覆盖入参、上下文与模型输出三类泄露路径。

### 1.4 边界样本（共 6 条）

| sample_id | 类型 | 边界点 |
|---|---|---|
| 013 | geo_loc 正 InputGate | 坐标 4 位小数（百米级），处精确/模糊判定边界 |
| 021 | geo_loc 正 ContextGate | 街道级地址绑定 user_id，cross_org 起需收紧 |
| 028 | geo_loc 正 OutputGate | 街道级位置 + 通勤时间，模糊但绑定 user_id |
| 052 | geo_loc 负 | 街道级 4 小时实时人次 4823（k 充足但窗口窄） |
| 076 | re_identify 正 OutputGate | 4 维属性候选集 12 万（远超 k=10） |
| 100 | re_identify 负 | 4 维属性候选集 8412 人（属性单点不构成再识别） |

边界样本均给出 **明确正/负标签**，并在 `judgement_reason` 中说明判定取舍依据，符合指南"边界样本不得只标存疑"的要求。

### 1.5 applicable_scenes 覆盖

5 个场景（self_use / internal_org / cross_org / public_release / research_anon）均有样本覆盖。负样本 100% 覆盖 5 场景（均放行）；正样本根据违规严重程度配置 1-5 个场景，最常见组合为 `internal_org + cross_org + public_release`。

### 1.6 字段完整性

每条样本均包含 17 个必填字段：`data_id / sample_id / target_violation_type / final_violation_type / violation_name / data_type / trigger_gate / applicable_scenes / is_positive / sample_kind / content / risk_locations / expected_action / judgement_basis_type / judgement_reason / data_source / annotation_meta`。

`risk_locations` 全部使用 `{field, start, end, text, entity_type}` 五元组，**offset 通过 `content.find(text)` 自动计算**，与 content 实际字节位置一致（脚本校验通过）。

正样本 005 / 010 / 014 等关键样本附带 `scene_judgements` 字段，给出不同场景下的差异化判定。

---

## 2. 判定口径说明

### 2.1 geo_loc（精确地理位置泄露）三档

- **个体 + 精确坐标**（≥5 位小数，约 1 米）+ 时间 → 直判 positive，restricted
- **个体 + 详细门牌**（精确到楼栋/单元/门牌号）→ 直判 positive
- **个体 + 街道级**位置 → 视场景判定，cross_org 起需脱敏（边界场景）
- **城市/区县级** + k≥10 聚合统计 → 负样本

### 2.2 re_identify（再识别风险）三档

- **属性 + 时空** 组合在小范围（k<10）唯一定位个人 → positive
- **明示再识别意图**（"匹配 1 个人" / "找符合 X 特征的人"）→ positive
- **单维属性 + 大样本聚合** → negative（k≥10 阈值）

### 2.3 expected_action 取词

- 严格使用指南 §6 的 10 个动作词：`allow / role_check / desensitize / aggregate / rewrite / refuse / warn / report / block_storage / manual_review`
- 5 个 scene 均填，按"对外越严越好"梯度：`self_use → internal_org → cross_org → public_release → research_anon`

### 2.4 法律依据 judgement_basis_type

- 几乎所有正样本援引 **《个人信息保护法》第 28 条**（行踪轨迹、医疗健康等敏感个人信息特别保护）
- 涉及模型输出再识别的，额外援引 **《生成式人工智能服务管理暂行办法》第 4 条、第 7 条**
- 涉及未成年人的，援引 **《未成年人保护法》**
- 负样本主要援引 **项目规则**（区县级 / k≥10 阈值）

---

## 3. 算法调研说明（按指南第 8 条要求）

### 3.1 现成可复用工具

| 工具 / 词典 | 用途 | 接入方式 |
|---|---|---|
| **Python `re`** | 经纬度模式（`\d{2,3}\.\d{5,7}`，°/° 格式）、ISO 8601 时间戳、geohash 校验 | 直接编译为正则，命中即落 `precise_geo_coord` 实体 |
| **`pyproj` + 行政区 shapefile** | 坐标 → 区县/街道反查 | 用国家基础地理信息中心或高德 adcode 数据，本地 R-tree 索引 |
| **高德 / 百度 LBS API** | 坐标 → 详细地址（带门牌） | 谨慎使用：调用即等于"我们持有该坐标"，应在前置规则就拦截而非反查 |
| **`presidio-analyzer`**（微软开源） | PII 实体识别（身份证、手机号、姓名等） | pip 安装，自定义 `pattern_recognizer` 扩展中文经纬度 / track_id |
| **HanLP / LAC** | 中文地址 NER（省/市/区/街道/门牌） | 服务化或本地，输出粒度判定是否精确到门牌单元 |
| **k-匿名 / l-多样性算法**（`ARX-toolkit` Java / `pycanon`） | 再识别风险量化 | 对候选集做 k≥10 校验，作为 re_identify 兜底判据 |
| **`retrieval_protocol.sensitivity_rules`**（本项目自研） | 6 类 adapter 默认敏感度 + 升降级规则 | 已实现，覆盖 spatiotemporal_trajectory 之外的 6 类 evidence；本数据类型可后续接入 |

### 3.2 部分覆盖、需扩展

| 项 | 现状 | 需补充 |
|---|---|---|
| 中文 POI 楼层级解析 | HanLP NER 能识别"X 楼 X 层"，但不区分公开 POI（如"东方明珠"）与个人 POI（如"某住户家"） | 需扩展规则：POI 名称 + 楼层 + 时间 → 升 restricted |
| 轨迹去标识 (`user_id` 哈希) 后再识别 | 哈希后字段仍是稳定标识，无现成开源工具自动判定"哈希值 + 时空组合"再识别风险 | 需自研判据：见 `补充回复.md` 中"hash + station/cell 组合按 restricted" |
| LLM 输出层 re_identify 监测 | 现有工具均针对 *输入侧* PII，对模型 *推断输出* 的属性组合识别尚无成熟方案 | 计划用规则 + 嵌入相似度（输出文本 vs 候选 K 匿名集）做近似判断 |
| 街道/区县模糊度边界 | 无现成规则库 | 已在本提交样本 013/021/028/052/076/100 中沉淀边界判据，可作为后续规则版本基础 |

### 3.3 不能复用 / 必须自研

| 场景 | 原因 |
|---|---|
| "属性组合 + 时空 → 唯一定位" 的再识别判定 | 涉及 candidate set 估算，业务相关，无通用方案 |
| 巡查路线 / 涉警涉密时空轨迹 | 此类违规类型与具体业务深度耦合，开源词典覆盖率近乎为零 |
| 多 scene 差异化 expected_action 决策树 | 项目规则特有，需用本批 100 条作训练 / 评估数据 |

### 3.4 接入路线建议

1. **第一阶段（短期，1-2 周）**：基于本批 100 条样本 + 上述正则/词典做规则引擎 baseline，目标在 InputGate 召回率 ≥ 90%
2. **第二阶段（中期，3-4 周）**：接入 `retrieval_protocol.sensitivity_rules`，让 ContextGate 检索证据自动落敏感度档；OutputGate 用规则 + LLM Judge 双判
3. **第三阶段（长期）**：用 100 条样本评估 baseline 各项指标（按 violation_type / trigger_gate / scene 分项 F1），定向补强弱项

---

## 4. 样本生成方式与质控

- **生成方式**：100% 人工构造（`data_source.type=manual_synthetic`）
- **脱敏标记**：正样本均 `desensitized=true`（坐标用上海典型 demo 点，user_id 为哈希示例如 `u_8f3a`，地址采用公开知名地标）；负样本聚合统计 `desensitized=false`（本身无个体）
- **格式校验**：JSONL 全部通过 `json.loads` 解析；所有 `risk_locations` 的 `(start, end, text)` 经脚本验证与 `content[start:end]` 一致
- **review_status**：均为 `pending`，等待林雨霏汇总 / 合规组抽检

---

## 5. 已知问题与后续

1. `risk_locations.field` 我按指南示例填的是 `field`（不是 `field_path`）；本项目 `retrieval_protocol/audit.py` 强制 `field_path`。两者口径需在下一版规则文档统一。当前批次按指南口径走。
2. `expected_action` 用了指南 10 词表；本项目 `retrieval_protocol.AUDIT_ACTIONS` 是 8 词。差异详情见 `补充回复.md` 处理记录。
3. 部分负样本（041 气象、043 地形）严格说是"非时空轨迹"，但属于易被误报为 geo_loc 的相邻形态，保留用于测试系统不要误报公共地理数据。
4. 6 条边界样本均处于 k 阈值或精度阈值临界，可用于后续阈值调参。

---

## 修订记录

**2026-05-31（按评审反馈修复）**
- **问题4**：`re_identify`（第 9 类）正样本主触发闸统一改为 `OutputGate`（改 16 条，现 25 条全 `OutputGate`），对齐"该类主要由输出闸判断"的规则。
- **问题2**：`expected_action`（及 `scene_judgements`）裁剪到恰好 = `applicable_scenes`（去掉 28 条多出的非适用场景键），现"缺键 0 / 多余 0"。
- **问题3**：核查确认全文件无 `k_anonymize` 动作；`research_anon` 场景使用 `aggregate` / `rewrite` 等模板内枚举。
- 复核：`content[start:end] == risk_locations[i].text` 全部匹配（0 不符）。

---

**对接联系**：陈曦 zcx12873@gmail.com
