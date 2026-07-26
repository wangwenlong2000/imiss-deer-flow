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

**状态**：未验证，非本次实施能解决。

**原因**：需要跑一次真实的 skill 调用链路，拿到真实 `SkillResult`，
人工核对其 `evidence[].metadata` 展开出来的字段路径是否与 0624 训练样本的
`features` 键名同构。这需要：
- 运行中的 LangGraph Server + Gateway（本机无 `uv`，无法 `make dev`）
- 配好的模型服务（`config.yaml` 指向 `192.168.200.1` 的内网地址）
- 至少一个会返回结构化 evidence 的真实 skill

**已做的缓解**：
1. `SkillResultNormalizer` 的展开规则严格按指南 §1.2.3 实现，
   并用构造的 SkillResult 做了单测（`test_compliance_normalizers.py`）
2. `scripts/run_compliance_gate_eval.py` 支持 `--dump-features`，
   上线前可以直接把线上 SkillResult 喂进去看展开结果
3. 键名不匹配不会静默失效 —— 模型会给出低相似度，检测器降级为 `low` 严重度、
   走人工复核，`evidence.top_similarity` 会明确记录相似度值

**建议解法**：上线前跑
`python3 scripts/run_compliance_gate_eval.py --dump-features --input <真实SkillResult.jsonl>`，
把输出的 feature 键名与 `datasets/compliance/normalized/0624_supported_split/train.jsonl`
里的 `features` 键名做人工对照。若大面积不匹配，需要在
`SkillResultNormalizer` 里加一层键名映射表。

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
