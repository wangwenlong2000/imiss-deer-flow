# 无人值守实施提示词（复制以下全部内容到新窗口）

---

你将在**无人值守**模式下，独立完成 imiss-deer-flow 系统的合规违规检测功能实施。没有人会回答你的提问，也没有人会在中途给你确认。请自己做决定、自己验证、自己提交。

## 一、工作目录与唯一事实来源

- 工作目录：`/home/huangxiao/City_brain/imiss-deer-flow-qwen36-35b-a3b-test`
- **实施计划（唯一事实来源）**：`docs/compliance-detection-implementation-plan.md`
- 开工前**完整读一遍这份计划**，它已经定稿，包含目录结构、接口定义、配置 schema、测试清单、分阶段计划（P0–P6）和风险清单。按它执行，不要另起炉灶。
- 背景材料（只读，需要时查阅）：`合规检测/` 目录下的两份 docx 和模型交付包 zip；`backend/CLAUDE.md` 是项目开发规范。

## 二、Git 纪律（最重要，先读这一节）

### 2.1 当前仓库状态 —— 有陷阱

当前分支 `qwen36-35b-a3b-test` 的工作区里**已经有 26 个未提交的改动，全部属于另一项 video_surveillance 工作，与本次任务无关**（`skills/custom/video_surveillance/`、`scripts/*video*`、`outputs/skill-tests/`、`skills/registry.json`、`docs/video-surveillance-*` 等）。

**绝对禁止：**
- ❌ `git add -A` / `git add .` / `git commit -a` —— 会把别人的在建工作一起提交
- ❌ `git stash` / `git checkout --` / `git reset --hard` / `git clean` —— 会破坏别人未保存的工作
- ❌ 修改任何 `video_surveillance`、`outputs/skill-tests/`、`skills/registry.json` 相关文件
- ❌ `git push`（本次任务全程不推远端）
- ❌ 切到 `main` 或合并到 `main`
- ❌ `git rebase` / 改写已有提交历史

**必须：**
- ✅ 从当前 HEAD 新建分支：`git checkout -b feat/compliance-detection`
- ✅ **每次提交只用显式路径 staging**，例如：
  ```bash
  git add backend/packages/harness/deerflow/compliance/
  git add backend/tests/test_compliance_*.py
  git add config/compliance/
  git commit -m "..."
  ```
- ✅ 每次 `git commit` 之前先跑 `git status --porcelain` 和 `git diff --cached --stat`，**逐条确认暂存区里没有 video_surveillance 相关文件**。发现有，`git restore --staged <路径>` 撤出来。

### 2.2 提交粒度与信息

- **每完成一个阶段（P0/P1/…/P6）提交一次**，阶段内如有独立可用的子成果也可多提交几次。不要憋一个巨型提交。
- 提交前必须让相关测试通过（见第四节）。测试没过不许提交。
- 提交信息用中文，格式：
  ```
  feat(compliance): P1 接入 TF-IDF+kNN 模型检测器

  - 平移交付包 vendor 代码到 detectors/model_tfidf_knn/vendor/
  - 实现 row_builder，content_text 规则 282/282 精确匹配
  - 离线评测复现 accuracy=0.9821

  Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
  ```

### 2.3 不要提交的东西

- 模型权重（`models/compliance/*.json`，6.7–11.7 MB）→ 加进 `.gitignore`
- `datasets/compliance/` 下除 `0624_supported_split/` 之外的数据
- `*.pyc` / `__pycache__/`
- 根目录的 `violation_detection_model_normalized_723.zip`（`.gitignore:136` 的 `*.zip` 已覆盖，确认即可）
- `合规检测/` 目录下的 docx 原件

## 三、五条不可协商的设计约束

这些是计划里反复确认过的决定，**不要自作主张改**：

1. **彻底解耦**：`contract.py` 是检测器作者唯一 import 的模块；`engine.py`/`router.py`/`policy.py` **永不 import 任何具体检测器**，只有 `registry.py` 做反射装载。必须写 AST 扫描单测强制这一点（`test_compliance_decoupling.py`）。
2. **不使用 T1–T6 原子检测器分类**。契约里不许出现 `tech` 字段，文档里不许再提这套分类。
3. **OutputGate 用流式 + 命中即撤回，不做缓冲**。双层机制都要实现：`custom` 流事件负责立刻撤回屏幕内容，`after_model` 改写 AIMessage 负责持久层不留违规原文。
4. **场景（scene）一期留空**：`DetectContext.scenes` 恒为空 tuple，矩阵走 `_unknown` 兜底列。兜底原则是"宁可漏处置，不可误伤"——底线三类（`hardcoded_cred`/`illegal_content`/`political`）照拦，其余七类只 `[warn, manual_review]`。`SceneResolver` 接口要留好。
5. **`content_text` 拼接规则已定稿，直接实现，不要重新反推**：
   ```python
   def build_content_text(features: dict) -> str:
       if len(features) == 1 and "content" in features:
           return str(features["content"])
       return "\n".join(f"{k}: {v}" for k, v in features.items())
   ```
   必须写回归脚本 `scripts/verify_content_text_rule.py`，对 `0624_supported_split` 全量 282 条断言 **100% 字符串精确匹配 + 100% 模型预测一致率**。这个脚本要能被 CI 调用。
   **注意**：`all_normalized_800.jsonl` 用的是另一套规范，两者不可混用；模型 manifest 里要记 `content_text_spec: "0624"` 并做启动校验。

## 四、TDD 是强制的

`backend/CLAUDE.md` 明确写着 "Every new feature or bug fix MUST be accompanied by unit tests. No exceptions."

- 测试写在 `backend/tests/`，命名 `test_compliance_*.py`
- 单个测试：`cd backend && PYTHONPATH=. uv run pytest tests/test_compliance_xxx.py -v`
- 全量：`cd backend && make test`
- **每个阶段提交前必须跑一次全量 `make test`，确认没有把既有测试跑挂**。如果既有测试本来就是红的（与本任务无关），记录下来，不要去修别人的测试。
- 计划 §9.1 列了 9 个测试文件的覆盖点，按它写。

## 五、无人值守行为准则

1. **不要提问、不要等待确认**。遇到计划里没写清楚的细节，自己按"与既有代码风格一致 + 最小惊讶"原则决定，然后把决定记进 `docs/compliance-detection-decisions.md`（新建，格式：日期 / 问题 / 选项 / 决定 / 理由）。
2. **遇到真正做不下去的点**（缺依赖、缺外部服务、需要人工核对真实数据），**不要卡住**：跳过它，把其余能做的全部做完，在决策日志里写明"已阻塞 + 原因 + 建议的解法"，最后在总结里单独列出。
3. **不要扩大范围**。计划之外的重构、顺手优化、给别的模块修 bug —— 一律不做。
4. **不要伪造完成度**。测试没跑就说跑了、评测没复现就说复现了，是最严重的错误。跑不出来就如实写"未验证"。
5. 单次连续工作如果发现自己在原地打转（同一个错误改了 3 次还没过），停下来，把现状写进决策日志，跳到下一个能推进的任务。

## 六、执行顺序与完成标准

按计划 §10 的 P0 → P6 顺序做。每个阶段的完成标准：

| 阶段 | 完成标准 |
|---|---|
| **P0 骨架** | `contract.py` 可导出 JSON Schema；registry 能扫 manifest 自动发现；0 个检测器时引擎不报错；解耦 AST 测试通过 |
| **P1 模型检测器** | `verify_content_text_rule.py` 输出 282/282 + 预测一致率 100%；离线评测复现 `accuracy=0.9821, macro_f1=0.9859` |
| **P2 ContextGate** | 真实 SkillResult 能被规范化并检测；预算截断会写 warning；`fail_mode: closed` 生效 |
| **P3 OutputGate** | `after_model` 顺序断言测试通过；撤回事件结构正确；checkpoint 里不含违规原文 |
| **P4 InputGate** | 骨架跑通，挂载点就位（检测器留空是预期的） |
| **P5 留痕评测** | 端到端评测脚本能跑出四组指标，输出落 `outputs/compliance-eval/{timestamp}/` |
| **P6 文档** | 更新 `README.md` 和 `backend/CLAUDE.md`（项目强制要求）+ 写《检测器接入指南》 |

P1 的两个数字是硬指标，**复现不出来就不要往下走**，先在决策日志里记录实际数字和差异原因。

## 七、资产准备（P0 之前先做）

```bash
# 解压交付包到临时目录，按计划 §8 分发资产
# zip 位置：合规检测/violation_detection_model_normalized_723.zip
# 解压后先跑 sha256sum -c SHA256SUMS 校验完整性
```
- vendor 代码 → `backend/packages/harness/deerflow/compliance/detectors/model_tfidf_knn/vendor/`（零改动平移，入库）
- 模型权重 → `models/compliance/`（gitignore）
- `0624_supported_split/` → `datasets/compliance/normalized/0624_supported_split/`（入库，回归测试要用）
- 写一个 `make compliance-assets` 目标，把上述解压+校验+分发自动化

## 八、结束时的交付

全部做完（或全部能做的做完）后，输出一份总结，必须包含：

1. 每个阶段的实际完成状态（完成 / 部分完成 / 阻塞，阻塞的写原因）
2. `git log --oneline` 的提交列表
3. `make test` 的最终结果（通过数 / 失败数，失败的列出来）
4. P1 两个硬指标的**实际输出数字**（不是"符合预期"，要贴真实数字）
5. 决策日志里所有"已阻塞"条目的汇总
6. `git status --porcelain` 确认工作区里除了 video_surveillance 的原有改动，没有遗留本次任务的未提交文件

**最后再确认一次：全程不 push，不合并 main，不碰 video_surveillance 相关文件。**
