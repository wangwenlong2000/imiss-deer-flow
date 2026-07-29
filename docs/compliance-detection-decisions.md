# 合规违规检测 —— 实施决策日志

记录实施计划（`docs/compliance-detection-implementation-plan.md`）里没有写清楚、
由实施者自行判断的细节，以及遇到的阻塞项。

格式：日期 / 问题 / 选项 / 决定 / 理由。

---

## 2026-07-26 · D001 处置动作词表用哪一套

**问题**：计划 §4.5 的矩阵示例只用到 `allow / warn / desensitize / refuse / rewrite /
aggregate / manual_review` 七个动作，但指南 docx 第三章表格里还有"角色校验""模糊化""打码"，
契约里的 `Action` 字面量该收哪些？

**选项**
1. 按计划示例，只收七个
2. 按指南 §6.4 的统一模板收十个
3. 自造一套

**决定**：选 2 —— `allow / warn / report / role_check / manual_review / aggregate /
desensitize / rewrite / block_storage / refuse`。

**理由**：从交付包全量标注数据里抽取 `expected_action` 的值域，实测正好是这十个，
出现次数 `allow 3054 / desensitize 1646 / warn 1158 / refuse 962 / manual_review 868 /
role_check 810 / aggregate 762 / report 604 / rewrite 418 / block_storage 288`，
与指南 §6.4 的模板完全一致。计划 §9.2 说要拿 `expected_action` 跟矩阵做交叉校验，
两边词表必须同源，否则校验无从做起。

docx 里的"模糊化"和"打码"不在这个代号表里 —— 标注员把"模糊化"统一归并成 `desensitize`
（对照 geo_loc 的样本可确认）。矩阵录入沿用这个归并，并在 YAML 注释里写明映射关系。

`ACTIONS` 的顺序是**有语义的**（弱→强），多个命中合并时取最强动作，
保证弱命中不会冲淡强命中。

---

## 2026-07-26 · D002 docx 表格里的 "—" 怎么编码

**问题**：指南第三章矩阵有大量 "—" 格（如 `re_identify` 在 InputGate/ContextGate 全行，
`domain` 在 self_use 列）。是当作 `allow` 还是别的？

**选项**
1. 当 `allow`（放行）
2. 当 `[warn]`（保守）
3. 编码成 `null`，表示"不适用"，命中时记 warning 但不执行动作

**决定**：选 3。

**理由**："—" 的含义是"该违规类型在该闸门/场景下不适用"，不是"检测到了但放行"。
指南对 `domain` 的注解写得很明白："第 10 类在 self_use 下为 N/A —— 普通用户无业务权限
接触此类信息"。把它当 `allow` 会掩盖一个真实信号：如果检测器真的在这里命中了，
说明要么路由错了，要么标注有缺口，两种都该被看见。

所以 `null` 格命中时 `PolicyMatrix.decide()` 产出一条 warning、不产出动作。
正常情况下不可达（检测器的 manifest 已经限制了闸门），一旦可达就是 bug 信号。

---

## 2026-07-26 · D003 `_unknown` 兜底列的填法

**问题**：计划 §7.2 定了原则（底线三类照拦，其余七类只 warn + manual_review），
但没逐格给出值。

**决定**：
- `hardcoded_cred` / `illegal_content` / `political`：三个闸门全部 `[refuse, warn]`
- 其余七类：三个闸门全部 `[warn, manual_review]`

并在 `PolicyMatrix._validate_baseline_types()` 里做**加载期强制校验**：
底线三类的 `_unknown` 格必须含 `refuse`，否则直接抛异常拒绝启动。

**理由**：矩阵是手工从 docx 抄的，抄错一格就是一个静默的安全漏洞。
把"底线类必须拦"这条规则变成加载期断言，比写在注释里可靠得多。
`test_compliance_policy_matrix.py` 另有一份运行期断言做双保险。

---

## 2026-07-26 · D004 不复用 `deerflow.reflection.resolve_variable`

**问题**：`registry.py` 需要按 `module:Class` 字符串反射装载检测器，
项目里已有 `deerflow.reflection.resolve_variable` 做同样的事。

**决定**：在 `adapters/inprocess.py` 里自己实现一个 10 行的 `_resolve_entry`，不复用。

**理由**：`deerflow/reflection/resolvers.py` 用了 PEP 695 泛型语法
（`def resolve_variable[T](...)`），只能在 Python 3.12+ 解析。合规子系统的设计目标之一
是"检测器作者只 import `contract.py`，在精简环境里就能开发"，
让 `adapters` 依赖一个 3.12-only 模块与这个目标冲突。

代价是十行重复代码，收益是合规包可以独立导入。已在 `_resolve_entry` 的 docstring 里
写明理由，避免后人误以为是没看见现成工具。

---

## 2026-07-26 · D005 `datetime.UTC`（中途改过一次，最终与项目一致）

**问题**：项目其他地方（如 `invoke_skill_tool.py`）用 `from datetime import UTC`，
这是 Python 3.11+ 的别名。实施初期本机只有 Python 3.10 可用。

**过程**：
1. 最初写成 `timezone.utc`，理由是让模块在 3.10 也能导入（当时只有 3.10 环境）
2. 后来按 D008 装好了真正的 Python 3.12，3.10 兼容性不再需要
3. 跑 `ruff --fix`（`target-version = "py312"`）时 UP017 自动改回 `datetime.UTC`

**最终决定**：用 `from datetime import UTC`，与项目其他模块一致。

**理由**：两者运行期完全等价（`datetime.UTC is timezone.utc`）。项目声明
`requires-python = ">=3.12"`，ruff 也配了 `target-version = "py312"`，
没有理由为一个已经不存在的约束保留旧拼法。

**同一次 ruff --fix 也误伤了 vendor 代码，见 D014。**

---

## 2026-07-26 · D006 检测器的 `min_similarity` 与模型的 `min_similarity` 是两回事

**问题**：`TfidfKNNModel.predict(min_similarity=...)` 和计划 §5.2 描述的
"低相似度降级"用了同一个名字，但语义不同。容易混用。

**实测数据**（0624 测试集，56 条）：

| 配置 | accuracy | macro_f1 | mismatches |
|---|---|---|---|
| 基线（不传 min_similarity） | 0.9821 | 0.9859 | 1/56 |
| 传给模型 `min_similarity=0.25` | **0.9107** | **0.9125** | **5/56** |

**决定**：`ModelTfidfKnnDetector` **不**把 `min_similarity` 传给 `model.predict()`。
它只用这个阈值决定 `severity`：低于阈值时 `severity="low"`（矩阵路由到人工复核），
高于阈值时按置信度正常分级。命中**始终上报**。

**理由**：传给模型会让 `predict_proba` 直接把标签强制成 `none`，
把 4 个真阳性变成假阴性 —— 召回掉了 4 个点。这与计划 §5.2 的原意相反：
计划说的是"低相似度 → 只走 manual_review，不进拒答/改写"，即**降级处置**而非**丢弃**。

已在 `run_compliance_model_eval.py` 的 docstring 和 `--min-similarity` 帮助文本里
写明两者区别，避免后人调参时踩坑。

---

## 2026-07-26 · D007 `.gitignore` 的 `datasets/` 规则需要改写

**问题**：计划 §8 要求 `datasets/compliance/normalized/0624_supported_split/` 入库
（回归测试要用），但 `.gitignore` 里有一条 `datasets/`，
而 git 无法 re-include 一个已被排除的目录下的任何内容。

**决定**：把 `datasets/` 改成 `datasets/*`，并在其后追加 compliance 的逐层 re-include 链。

**理由**：`datasets/`（带斜杠）会让 git 直接跳过整个目录、不再递归，
任何 `!datasets/xxx` 都不生效。改成 `datasets/*` 只排除直接子项，
保留了 re-include 的可能。

对其他人的影响：**行为完全不变**。改动前后 `datasets/` 下所有非 compliance 内容依旧被忽略
（`datasets/*` 匹配到 `datasets/network-traffic` 这个目录本身就停止递归，
与原来的效果一致）。已用 `git status --porcelain datasets/` 验证：只多出
`?? datasets/compliance/`，没有任何既有文件状态变化。

---

## 2026-07-26 · D008 测试环境：`uv` 缺失，自建 Python 3.12 环境

**问题**：`backend/CLAUDE.md` 规定用 `cd backend && make test`（即
`PYTHONPATH=. uv run pytest tests/ -v`），但本机：
- 没有 `uv`（`command not found`）
- `backend/.venv/` 是空目录且属主为 root
- 系统 Python 是 3.10，项目要求 `>=3.12`
- `~/.local/lib` 和 `~/.cache/uv` 无写权限

**决定**：在 scratchpad 里自建环境，分两步：
1. `python3 -m venv` 建 3.10 环境，`pip install uv`
2. `UV_CACHE_DIR=<scratchpad> UV_PYTHON_INSTALL_DIR=<scratchpad> uv python install 3.12`
   拿到真正的 CPython 3.12.13，再建 3.12 venv 并装齐依赖

最终用 `PYTHONPATH=.:packages/harness <3.12venv>/bin/python -m pytest tests/` 跑全量。

**理由**：无人值守要求"自己验证"。跳过验证或只跑一部分都不可接受。
自建 3.12 环境让本次全部测试（含 middleware 相关）都能真实运行，
且与项目声明的 `requires-python = ">=3.12"` 一致。

**注意**：这个环境在 scratchpad 里，**没有**进入仓库，不影响任何人。
CI 和其他开发者仍应使用 `make test`。本次报告的测试数字来自这个等价环境。

---

## 2026-07-26 · D009 `content_text` 的 features 构造顺序

**问题**：`ModelTfidfKnnDetector._features()` 要把 `DetectionUnit` 的
`field_items` 和 `text_items` 合成一个 `features` dict，
但计划没说两者的先后顺序。顺序会直接影响 `content_text` 的拼接结果。

**决定**：先 `field_items`（按其在 unit 中的顺序），后 `text_items`；
`text_items` 的 key 取 `item.source`，冲突时追加 `.{item_id}` 去重。

**理由**：对照 0624 训练样本，结构化字段（`camera_id`、`lat`、`lon` 等）
排在描述性文本（`output_text`、`content`）之前是更常见的形态。
更重要的是保证**同一个 unit 每次产出的顺序稳定**，否则同样的输入会得到不同的
`content_text`、进而得到不同的预测，这比顺序"对不对"严重得多。

**剩余风险**（计划 §11 风险 4 未消除）：线上 SkillResult 的 `features` 键名
是否与训练样本同构，仍未用真实数据核对过。见下方"已阻塞"。

---

## 2026-07-26 · D010 P2/P3 中间件如何在无 LangGraph 运行时的情况下测试

**问题**：ContextGate 走 `wrap_tool_call`、OutputGate 走 `after_model` +
`get_stream_writer()`，这些都依赖 LangGraph 运行时上下文。

**决定**：中间件测试直接构造 `ToolCallRequest` / `AIMessage` 并调用中间件方法，
`get_stream_writer()` 用 monkeypatch 替换成收集器。不起真实 graph。

**理由**：与项目既有做法一致 —— `test_tool_error_handling_middleware.py`、
`test_view_image_middleware.py` 都是这么写的（文件名里的 `core_logic` 就是这个意思）。
起真实 graph 需要模型服务，不适合放进单测。

---

## 2026-07-26 · D011 `min_similarity` 用 0.20 而不是 0.25

**问题**：计划 §4.3 的 `detectors.yaml` 示例写了 `params: { min_similarity: 0.25 }`，
但 §5.2 正文说的是 `min_similarity=0.20 直接治这个病`。两个值不一致，该用哪个？

**实测**（0624 测试集 56 条，模型命中 31 条，统计最近邻相似度分布）：

```
真阳性 (n=30)：min=0.1393  p10=0.2007  median=0.6376  max=0.9754
假阳性 (n=1) ：0.1316   ← 就是那条否定语境样本

阈值 0.20 → 降级 3/30 真阳性，抓住 1/1 假阳性
阈值 0.25 → 降级 5/30 真阳性，抓住 1/1 假阳性   ← 多误伤 2 条，零额外收益
阈值 0.30 → 降级 7/30 真阳性，抓住 1/1 假阳性
```

**决定**：用 manifest 里的默认值 **0.20**，`detectors.yaml` 不做覆盖（`params: {}`）。

**理由**：唯一的假阳性落在 0.1316，0.20 已经完全覆盖它。再往上抬只是单方面把
真阳性降级成 `low`（进人工队列而非自动处置），没有任何额外的误报收益。
§4.3 里的 0.25 是在演示"参数覆盖的语法长什么样"，不是调优结论；§5.2 的 0.20 才是。

已把这段实测数据和复现方法写进 `config/compliance/detectors.yaml` 的注释里，
避免后人凭感觉调这个旋钮。

---

## 2026-07-26 · D012 端到端准确率 0.9286 低于模型离线 0.9821，是预期的

**问题**：`run_compliance_gate_eval.py` 跑出主指标 accuracy=0.9286，
而 `run_compliance_model_eval.py` 是 0.9821。同一个模型、同一份测试集，为什么差 5 个点？

**原因**：端到端链路多了一步**闸门过滤**。0624 测试集里有 4 条 InputGate 样本，
其中 3 条 gold=`domain`。模型检测器的 manifest 没有声明 InputGate，
引擎按 manifest 路由，这 3 条根本不会被送去检测 → 全部预测为 `none` → 3 个假阴性。

`domain` 的召回因此从 1.0 掉到 0.7273（8/11），主指标随之下降。

**决定**：**不改**。这是计划 §6.1 的明确范围决定："我的三个模型检测器不挂 InputGate"。

**理由**：这不是 bug，是覆盖缺口，而且报告已经把它显式打出来了：

```
InputGate    covered: (none)
             NOT covered: ...(全部 10 类)
(uncovered types have no detector registered yet — those columns below
 measure nothing, they do not mean 'no violations found')
```

指南第三章的矩阵确实给 `domain` 在 InputGate 配了处置动作，
所以这是一个**真实存在的能力缺口**，只是不属于本次范围。
硬把 InputGate 加进 manifest 会让模型在一个它没被训练过的闸门上做判断
（训练集里 InputGate 样本只有 22 条），是拿指标换真实效果，不做。

**建议后续**：要么补 InputGate 训练数据后重训并扩 manifest，
要么由第 1-7 类负责人的规则类检测器覆盖 InputGate。

---

## 2026-07-26 · D013 矩阵与标注 expected_action 的一致率只有 52%（重要发现）

**问题**：计划 §9.2 说标注样本自带 `expected_action`，可以拿来交叉校验矩阵录入，
"不一致即为录入错误或标注争议，白捡的一致性测试"。实际跑出来一致率远低于预期。

**实测**（`all_supported_deduplicated.jsonl` 282 条，473 个可比对格）：

```
domain           140/178   78.6%
re_identify      107/265   40.4%
video_meta_leak    0/30     0.0%
OVERALL          247/473   52.2%
去重后实际有分歧的矩阵格：15 个
```

用交付包全量标注数据（1610 个可比对格，覆盖 10 类）复算：

```
hardcoded_cred   100/100  100.0%   ← 底线类
political         50/50   100.0%   ← 底线类
illegal_content   72/74    97.3%   ← 底线类
confidential      73/85    85.9%
domain           155/195   79.5%
struct_id        236/320   73.8%
text_id           45/65    69.2%
re_identify      107/265   40.4%
geo_loc          146/416   35.1%
video_meta_leak   10/40    25.0%
OVERALL          994/1610  61.7%
```

**已排除是我抄错**：逐格回查 docx 原文核对过分歧最大的几类，转录无误。例如
- `geo_loc.ContextGate.self_use`：docx"放行"，标注 `['role_check','warn']`
- `domain.ContextGate.cross_org`：docx"拒答"，标注 `['desensitize','manual_review']`
- `re_identify.OutputGate.self_use`：docx"告警"，标注 `['role_check','warn']`

**决定**：矩阵**保持按 docx 7.23 第三章逐格照抄**，不按标注数据改。
分歧作为**发现**报告出来，交由数据负责人裁决。

**理由**：计划 §4.5 明确指定"录入以《合规评测标注指南7.23.docx》第三章为准"。
标注数据是另一批人按自己的理解填的，两者不一致本身就是这个校验要发现的东西。
我无权替业务方裁决哪边对 —— 每一格都要人工判断是"改指南 / 改标注 / 记为有意例外"。

**重要的正面结论**：**底线三类（hardcoded_cred / illegal_content / political）
一致率 100% / 100% / 97.3%** —— 最安全攸关的那部分，指南和标注是一致的，
矩阵转录也是对的。`test_compliance_audit_eval.py` 里有一条测试持续断言这三类
一致率 ≥ 0.95，防止后续回归。

**建议解法**：把 `outputs/compliance-eval/{ts}/report.json` 里
`matrix_cross_validation.discrepancies`（15 个去重后的格）发给标注负责人逐格确认。

---

## 2026-07-26 · D014 `ruff --fix` 会静默改写 vendor 代码（已加防护）

**问题**：给自己的代码跑 `ruff check --fix` 时，路径里包含了
`detectors/model_tfidf_knn/vendor/`，ruff 按 `target-version = "py312"` 把 4 个
交付包文件改写了（UP035/UP037 现代化）：

```
feature_extractor.py  holdout_split.py  io_utils.py  tfidf_knn.py
```

计划 §5.2 明确要求"vendor 代码零改动平移，日后交付包升级直接覆盖目录"。
被 lint 改过的模型实现，正是那种"没人会注意到、直到准确率悄悄下降"的改动。

**决定**：三层防护。

1. **`backend/ruff.toml` 加 `exclude`**：
   `packages/harness/deerflow/compliance/detectors/*/vendor/`
   —— 治本，`make lint` / `make format` 不再碰 vendor
2. **`make compliance-assets` 生成 `vendor/SHA256SUMS`**（入库）
   —— zip 是 gitignore 的，CI 没法跟 zip 比对，这个文件让哈希可离线校验
3. **`test_compliance_model_detector.py::test_vendor_code_is_unmodified`**
   —— 逐文件比对哈希，改了就红

**已恢复**：重跑 `make compliance-assets`，11/11 文件与交付包字节一致。

**理由**：只加注释说"别改这个目录"是不够的 —— 事实证明工具会自己改。
把不变式变成配置 + 测试，才拦得住。

---

## 已阻塞项汇总

### B001 · 线上 `features` 键名与训练样本是否同构（计划 §11 风险 4）

**状态**：⚠️ **已大幅收窄，但未完全消除**（2026-07-26 补测）。

**补测做了什么**：把 282 条标注样本的 `raw_content` 按真实 skill 的形状重新包装成
`SkillResult`（塞进 `evidence[].data`），走完整链路
`SkillResultNormalizer → ModelTfidfKnnDetector`，逐条比对预测。

**实测结果**：

```
domain          -> domain          47/47
re_identify     -> re_identify     63/63
video_meta_leak -> video_meta_leak 30/30
                   合计 140/140 = 100.0%

命中相似度中位数  domain 0.9456 | re_identify 0.9837 | video_meta_leak 0.9531
```

**关键发现：键名确实不一致，但不影响检测。**
训练样本的 features 键是 `source_split`、`city_governance_context.lat_lng`；
线上经 normalizer 展开后变成 `data.source_split`、`data.city_governance_context.lat_lng`
—— **每个键都多了 `data.` 前缀，是模型没见过的**。
但字符 n-gram 特征是建立在**值**上的，压过了路径特征，所以检测率没有损失。

**为什么仍不算完全消除**：这次用的是模型训练时见过的**内容**，只是换了个外壳。
它证明的是"normalizer→检测器这段管道不损失可检测性"，
**没有**证明一个全新的、模型没见过的 skill 输出也能被检出（那是泛化问题，
受限于训练集只有 226 条，见计划风险 9）。

**已加的回归测试**（`test_compliance_integration.py`）：
- `test_skill_result_field_paths_do_not_break_detection` —— 140 条全量断言
- `test_detection_confidence_stays_high_on_the_live_shape` —— 断言中位相似度 >0.80，
  防止 normalizer 改动把相似度压向 0.20 阈值导致全体降级为 `low` 而静默失去处置

**剩余建议**：上线前仍应拿一次真实 skill 输出跑
`python3 scripts/run_compliance_gate_eval.py --dump-features`，
人工对照键名。若大面积不匹配再考虑加键名映射表。

### B002 · `make test` 无法在本机原样执行

**状态**：已绕过，但 `make test` 这条命令本身在本机跑不通。

**原因**：见 D008 —— 无 `uv`、`.venv` 为空且属主 root、系统 Python 版本不符。

**已做的缓解**：自建等价的 Python 3.12 环境跑全量测试，
命令等价于 `PYTHONPATH=. pytest tests/`（`make test` 只是多了 `uv run` 和 `-v`）。

**建议解法**：`curl -LsSf https://astral.sh/uv/install.sh | sh` 装 uv，
然后 `cd backend && make install`。或者修复 `backend/.venv` 的属主
（`sudo chown -R $USER backend/.venv`）。

### B003 · IM 渠道撤回适配未做端到端验证（计划 §11 风险 7）

**状态**：代码路径已分析并做了适配，但没有真实 IM 环境可验证。

**原因**：需要真实的 Feishu / Slack / Telegram 应用凭证和可达的回调地址。

**已做的缓解**：见 P3 小节的说明 —— Slack/Telegram 走 `runs.wait()`
拿最终结果，`after_model` 改写后的 AIMessage 本身就是最终结果，天然不受影响；
Feishu 走 `runs.stream()` 并原地 patch 卡片，最后一次 patch 用的也是改写后的内容。
两条路径都不依赖 `compliance_retract` 自定义事件。

**建议解法**：配好任一 IM 渠道后，构造一条会触发 `re_identify` 的问题，
确认最终卡片/消息里不含违规原文。

---

## 2026-07-26 · D015 补：中间件从未与真引擎接过（测试缺口，已补）

**问题**：被质疑"是否真的集成了检测器"。复查发现一个真实缺口 ——
`test_compliance_gates.py` 等文件里三道闸中间件注入的全是 `_StubEngine`，
而 `middleware → get_engine() → build_engine() → registry → 真检测器 → 真模型`
这条**生产装配路径一次都没被执行过**。集成 bug 恰恰住在那里。

**补测结果**（新增 `backend/tests/test_compliance_integration.py`，全程无桩）：

| 验证项 | 结果 |
|---|---|
| `build_engine()` 从真配置装配 | ✅ 装出 `model_tfidf_knn / inprocess / enabled` |
| 引擎单例复用 | ✅ 不会每请求重载 6.7MB 模型 |
| 真 ContextGate + 真模型 + 真阳性样本 | ✅ 命中 `domain`，相似度 0.9365，actions `[warn, manual_review]` |
| 真 ContextGate + 真阴性样本 | ✅ 不误报，消息原样返回 |
| 真 OutputGate + 真 `re_identify` 样本 | ✅ 改写消息 + 发出 `compliance_retract` 事件 |
| 审计落盘 | ✅ 记录含 `violation_type` 与 `basis` |

**顺带发现的环境问题（非代码 bug）**：在**宿主上**以普通用户跑测试时，
默认审计目录 `backend/.deer-flow/compliance/` 写入失败 ——
该目录属主是 root（容器以 root 跑，写出来的文件就是 root 的）。
`Auditor` 按设计优雅降级了：记 warning、不中断请求、不吞掉错误。

> **2026-07-27 订正**：这条只对**宿主侧**成立。运行中的服务里，容器**以 root 运行**
> 且 `/app/backend/.deer-flow` 可写，**线上审计正常落盘** ——
> 已实测 `compliance-20260726.jsonl` 内含完整判定记录（含 basis 与 top_similarity）。
> 原文"审计实际上没落盘"的表述过宽，容易被读成线上也没留痕，特此更正。
> 单测把审计目录指向 `tmp_path`，不依赖这个环境条件。

**建议解法**：`sudo chown -R $USER backend/.deer-flow`，
或把 `compliance.audit.path` 指向进程有写权限的目录。
集成测试已把审计目录指向 `tmp_path`，不依赖这个环境条件。

**教训**：单测隔离得越干净，越容易把"每块都对"误当成"接起来是对的"。
桩测试和真集成测试是两种东西，两个都要有。

---

## 2026-07-27 · R0/D016 容器缺失合规资产挂载 —— 一个配置疏漏会导致全站故障

**问题**：准备在运行中的服务上打开合规检测前，实测容器内 `/app` 只有
`backend / config.yaml / extensions_config.json / logs / skills`，
**`models/`、`config/`、`datasets/` 都没挂载**。容器内实测：

```
BUILD FAILED -> PolicyMatrixError policy matrix file not found: /app/config/compliance/policy_matrix.yaml
```

**为什么这是全站故障而不是"合规不生效"**：
`get_engine()` 是在**请求路径上惰性构建**的。两道闸都会 catch 异常 →
`failure_decision()` → `fail_mode: closed` → `refuse`。所以直接
`enabled: true` 的后果是**每条回答和每条工具结果都被替换成【合规检查失败】**。
一个 volume 少写两行，表现为整个产品停止回答，而且日志会把锅甩给"合规检测"。

**决定**：三件事一起做。

1. `docker/docker-compose-dev.yaml` 给 `langgraph` 和 `gateway` 补
   `../models:/app/models` 和 `../config:/app/config`
   （gateway 也需要 —— `uploads.py` 调 `scan_upload_paths`）
2. 新增 `deerflow/compliance/preflight.py`：**启动期 canary 自检**
3. 新增 `compliance.strict_startup` 配置项（默认 `false`）

**为什么必须是 canary 而不是只 `build_engine()`**：模型权重是**懒加载**的，
`_ensure_model()` 在第一次 `detect()` 才读文件。而 `config/compliance/*.yaml` 入库、
`models/compliance/` 是 gitignore 的 —— 新克隆的环境会出现
"引擎构建成功、第一次真实请求才 FileNotFoundError"。只构建不检测的自检抓不到它。
canary 真跑一条无害样本过每道启用的闸，把懒加载提前到启动期。

**语义边界**（这是本条的核心）：
`fail_mode: closed` 的本意是"**这条内容**判不了，宁可拦住"，
**不是**"系统没装好，所以拦住所有人"。当前把两者混为一谈了。
所以自检失败时默认**不挂载闸门** + CRITICAL 日志（应用照常工作，问题在日志里嚷嚷），
`strict_startup: true` 才改为拒绝启动。运行期的检测失败仍然 fail closed，语义不变。

**测试写出来的一个真 bug**：`build_compliance_flow_middlewares()` 外层的
`except Exception` 会把 `strict_startup` 抛出的异常吞掉，导致该选项**完全无效**。
已引入专用异常 `ComplianceStartupError` 并在外层 `except` 之前重新抛出。
这正是"写测试断言行为而不是断言实现"的价值。

**顺带纠正一处环境认知**：运行中的 compose 项目名是 **`docker`**（取自目录名），
不是 Makefile 暗示的 `qwen36test-deer-flow-dev`；网络是 `docker_deer-flow-dev`。
用错项目名会触发新建网络，而这台机器上已有十几个别人的 deer-flow 网络，
子网分配会冲突报错（幸好是在创建网络阶段失败，没有破坏运行中的容器）。
正确命令：`cd docker && docker compose -p docker -f docker-compose-dev.yaml up -d --no-build <svc>`。
操作前先加 `--dry-run` 确认只动预期的容器。

**遗留**：生产 compose `docker/docker-compose.yaml` 有**同样的缺失**，
且它不 bind-mount `backend/`（代码烤进镜像），gitignore 的模型权重需要独立的卷或构建步骤。
本阶段不解决，记在此处。

---

## 2026-07-27 · R1/D017 撤回改在 `wrap_model_call` 做，`after_model` 降为兵底

**问题（上阶段的真缺陷，已实测确认）**：LangChain 的 `after_model` **逆序执行**，
OutputGate 挂在列表最前 → 它**最后**执行 → 其他 `after_model` 中间件都先看到未净化的违规原文。

实测（把 `wrap_model_call` 停掉即还原旧设计）：

```
downstream after_model saw secret : True
raw_messages contains secret      : True   ← 刷新页面违规原文重现
messages contains secret          : False
```

**两条真实泄漏路径**（原以为有三条，核查后订正）：
1. `RawTranscriptMiddleware` → `raw_messages`，而 `merge_raw_messages` 按 id 去重
   **保留先到的**，永久锁死；前端 `displayMessagesOfThread` 优先读 `raw_messages`
2. `TitleMiddleware` 把首条 assistant 回答截 500 字塞进 prompt 发给**外部模型**，
   再把返回的 title 持久化 —— 这条是往外发，比留在本地更严重

**订正**：`MemoryMiddleware` 和 `RunHistoryMiddleware` **不是**泄漏路径。
两者用的是 `after_agent`（单一出口节点，在所有 `after_model` 提交之后才跑一次），
拿到的已经是净化后的 `messages`。上阶段计划里把它们列为泄漏是错的。

**决定**：不采用"把 OutputGate 挪到列表末尾"的顺序修法，改为
**在 `wrap_model_call` 里净化**，`after_model` 保留为摘要门控的兵底。

**理由**：
- `_chain_model_call_handlers` 也是**第一个即最外层**，而 langchain 的
  `_build_commands` 做的是 `{"messages": response.result}` ——
  在 `wrap_model_call` 里净化意味着**违规原文从未进入 graph state**。
  reducer、transcript、title、checkpoint、SSE 帧，谁都看不到。
  保证从"依赖顺序"变成"结构性"。
- 顺序修法只能挡住**排在它后面**的中间件；任何人以后在
  `_build_middlewares` 末尾 `append` 一个新中间件就又把洞打开了。
  上阶段的测试恰恰断言了一个位置不变式，而那个不变式本身是错的。
- 两条规则合并成一条：`wrap_tool_call` 和 `wrap_model_call` 都是第一个即最外层，
  "合规闸挂最前"从此只有一个、且符合直觉的理由。"after_model 逆序"这个坑
  彻底离开设计。
- 不用拆 builder、不用改 `agent.py`、不用改 `subagents/executor.py`，子代理免费获得。
- 既有 412 项测试**一条都没破**。

**为什么还要保留 `after_model` 兵底**：`LoopDetectionMiddleware` 在硬停时会
`content + _HARD_STOP_MSG`，把一条带 tool_calls 的消息（OutputGate 按设计跳过）
变成用户可见的回答。这是模型节点**之后**的内容变更，`wrap_model_call` 管不到。
兵底按 `sha256(text)` 摘要门控：正常路径摘要命中 → 零额外检测；
内容被改过 → 摘要不匹配 → 全量重扫。

**新增测试** `test_compliance_output_gate_isolation.py`（9 项）断言的是
**可观测性不变式**而非列表下标：
- 起真 graph，跑真 middleware 链，断言 spy 中间件在 `after_model`/`after_agent`
  都看不到违规原文，且 `messages` 和 `raw_messages` 都不含
- **按 spy 位置参数化**（gate_first / spy_first / spy_between）——
  顺序修法只能过其中一部分，结构性修法三种都过。这才是"任何位置都打不开洞"
- 断言干净回答**只扫一次**（摘要门控生效），防止检测成本和审计记录悄悄翻倍
- 断言模型节点后被改写的内容会被兵底抓住

**已验证测试确实能抓 bug**：把 `wrap_model_call` 停掉重跑，断言如期失败（见上方实测输出）。

---

## 2026-07-27 · R5/D018 端到端验证结果（真实模型、运行中的服务）

在 `http://localhost:3538` 用真实 DashScope `qwen3.6-35b-a3b` 实测。

| 验证项 | 结果 |
|---|---|
| 启动自检 | `compliance preflight OK — detectors: model_tfidf_knn` |
| 无风险对话 | 回答正常，`compliance` metadata = None，**无误报** |
| 真实违规命中 | `re_identify`，severity=high，最近邻相似度 **0.6112** |
| 一期处置（`_unknown` 列） | `[warn, manual_review]` → 保留回答 + 追加【合规提示】 |
| 强处置（临时 `public_release`） | `[rewrite, refuse]` → 正文完全替换为【合规拦截】，原文残留 = False |
| **持久层（R1 关键验证）** | `messages` 与 `raw_messages` 同 id 内容一致，**都不含违规原文** |
| 审计落盘 | `compliance-20260726.jsonl`，含 audit_ref/gate/scene/type/severity/actions/basis |
| 前端类型检查 | `tsc --noEmit` EXIT=0，`eslint` EXIT=0 |
| 前端数据层 | `node --test compliance.test.ts` **12/12**，用例数据取自真实链路 |

**一个对前端设计有决定性影响的发现**：一期 `SceneResolver` 恒返回 `None`，
矩阵走 `_unknown` 兜底列，而模型检测器覆盖的三类在该列**都是** `[warn, manual_review]` ——
`apply_actions` 返回 `None`，走 `_notice_only()`。
所以**主导场景是"保留回答 + 追加提示"，不是"替换正文"**。

据此前端做了两个决定：
1. 横幅色调按 `mutated`（是否真的改了正文）而非"是否命中"。
   一律标红会让用户学会无视这个横幅，连真正的拒答一起无视。
2. 渲染时剥掉正文里追加的【合规提示】段落，否则同样的信息用户要读两遍。
   剥离要求前导空行，所以**独立**的拒答文案不会被剥成空白。

**临时改 `scene.fallback_key: public_release` 验证强处置后已还原**，
`git diff config.yaml` 与 HEAD 一致。

**未验证项（如实记录）**：React 组件的**视觉渲染**没有自动化验证 ——
本仓库无前端测试框架，本次也没有可用的浏览器自动化。
数据层（解析/合并/剥离/跨 thread 隔离）有 12 项测试覆盖，
但"横幅长什么样、展开收起是否正常"需要人工打开页面确认。

---

## 2026-07-27 · R4/D019 增量扫描：有可行方案，本阶段**主动不做**

`ComplianceOutputGateMiddleware.scan_increment()` 仍然没有生产调用者。

**调研已完成，结论明确（不要重走弯路）**：
- ❌ 六个 `AgentMiddleware` 生命周期钩子**都看不到 token**，
  `wrap_model_call` 拿到的是完整 `AIMessage`
- ❌ `AgentMiddleware.transformers` / `StreamTransformer` 机制真实存在，
  但只在 **beta v3 协议**（`Pregel._apregel_stream_v3`）下运行；
  本项目走 LangGraph Server 的 `runs.stream` → `astream`，**不触发**，接了还是死代码
- ✅ **唯一可行**：`BaseCallbackHandler.on_llm_new_token`，挂在
  `models/factory.py` 第 73-88 行（现有 tracer 的挂载点）。
  LangGraph 的 `StreamMessagesHandler` 会让模型内部流式化，per-token 回调确实触发；
  回调运行在 graph 上下文内，`get_stream_writer()` 可用

**决定**：本阶段**不实现**，保留为已记录的待办。

**理由**（这是主动取舍，不是被阻塞）：
1. 它只让撤回**更早**，不是"前端能展示合规处置"的必要条件 —— 本阶段目标已达成
2. 挂载点在 `create_chat_model()`，影响**所有**模型调用（含记忆抽取、标题生成、
   意图识别等所有内部 LLM 调用），而合规输出闸只关心面向用户的最终回答。
   要正确区分需要额外的上下文判别逻辑，风险与收益不成比例
3. 一期主导处置是 `[warn, manual_review]`（保留回答），
   "更早撤回"在这个处置下几乎没有可感收益 —— 真正需要它的是 `refuse`/`rewrite`，
   而那要等 `SceneResolver` 接上真实信息源之后才会成为常态

**建议实施时机**：`SceneResolver` 接入、强处置成为常态之后再做，
届时收益明确，也能顺带解决"只扫最终回答、不扫内部 LLM 调用"的判别问题。

---

## 2026-07-30 · D021 严格 scene 在模型流之前缓冲

前述 R5 关于 graph state、`raw_messages`、title 和 checkpoint 的结论仍然成立，
但“`wrap_model_call` 能保证原文不进入 SSE 帧”并不完整。LangGraph 的
`messages` stream handler 可以在外层 wrapper 返回前观察模型 token；因此 state
安全不自动等于传输安全。

新增确定性测试先证明了兼容链路的时序：

```text
raw model chunk -> user-visible messages SSE -> OutputGate complete scan
-> compliance_retract -> safe authoritative state
```

严格 scene 现在使用模型副本上的 `nostream` tag 和
`disable_streaming=True`。完整模型结果先返回到最外层 OutputGate，执行真实
Engine/Policy，再把安全结果交给 graph。严格集合为 `cross_org`、
`public_release`、`research_anon`。

`self_use`、`internal_org` 与 `_unknown` 保留兼容模式；`_unknown` 仍明确使用
保守的 warn/manual_review cell，不会降成 self_use。Audit 明确记录
`streaming_mode` 和 `transient_exposure_possible`，不再把撤回窗口描述成零风险。
`scan_increment()` 仍无生产调用者，但严格模式不再依赖它来防止首次泄漏。

确定性主链路和 HTTP/SSE 测试使用 `GenericFakeChatModel`，不改变生产模型行为，
稳定覆盖 struct_id、geo_loc、组合风险、负样本和六类 scene。严格 SSE、最终
messages/raw_messages 与审计中均断言不存在测试用完整标识符或精确坐标。

---

## 2026-07-27 · D020 测试环境版本与 `uv.lock` 不一致（被追问后自查发现）

**问题**：被追问"是不是真跑了测试"后自查，发现一个我此前**没有注意到**的问题 ——
scratchpad 里自建的 Python 3.12 环境是用 `pip install` 装的，
版本与 `uv.lock` 锁定的**不一致**：

| 包 | scratchpad（我跑测试用的） | uv.lock / 容器（真实部署） |
|---|---|---|
| langchain | 1.3.14 | **1.2.3** |
| langgraph | 1.2.9 | **1.0.10** |
| langchain-core | 1.5.1 | **1.2.17** |
| pydantic | 2.13.4 | **2.12.5** |

**为什么这很要紧**：R1 的修复直接依赖 langchain 内部实现 ——
`wrap_model_call` 的组合顺序、`_build_commands` 做 `{"messages": response.result}`、
`ModelResponse` 的 dataclass 结构。这些我是读 **1.3.14** 的源码确认的，
而线上跑的是 **1.2.3**。版本行为若有差异，修复可能在真实部署里根本不生效。

**补做的验证**：直接在**运行中的容器**里（即 uv.lock 锁定的真实环境）跑测试。

```
容器内 (langchain 1.2.3)：385 passed, 4 failed, 23 skipped
  4 failed  —— 全部是文件缺失，非逻辑失败：
              /app/scripts/ 与 /app/config.example.yaml 未挂载
              （运行时不需要，只有测试需要）
  23 skipped —— datasets/ 未挂载（有意不挂，运行时不需要）

R1 修复的核心测试单独跑：
  test_compliance_output_gate_isolation.py  9 passed  ← 全部通过
  含按 spy 位置参数化的三种排布，全部证明"任何中间件位置都打不开洞"
```

**结论**：R1 修复在**真实部署的 langchain 1.2.3 上确实生效**，不是只在新版本上成立。
另外 R5 的端到端验证本来就是在容器里跑的（真实 DashScope 对话 + raw_messages 核查），
那本身就是 1.2.3 上的经验证据。

**教训与改进**：
1. 自建测试环境时应当**按 `uv.lock` 装版本**，而不是 `pip install <pkg>` 拿最新的。
   本次侥幸没出问题，但这是运气，不是方法。
2. 凡是依赖第三方库**内部实现**的改动（本次的 `wrap_model_call` 组合顺序就是），
   必须在**部署实际使用的版本**上验证，不能只读文档或读另一个版本的源码。
3. 汇报测试结果时应当同时说明**跑在什么环境、什么版本**。
   上一次汇报只给了数字（"1149 passed"），没有重复这个前提，是表述上的疏漏。

**推荐的验证命令**（在真实环境里跑，写进 e2e 手册）：

```bash
docker exec qwen36test-deer-flow-langgraph sh -c \
  'cd /app/backend && PYTHONPATH=.:packages/harness \
   /app/backend/.venv/bin/python -m pytest tests/test_compliance_*.py -q'
```
