# 模型类检测器

本目录是第 8/9/10 类违规检测的**模型类实现**，已与规则类实现完全剥离。它不读取规则类的 `detector_rules.yaml`，不依赖手写业务词表，而是使用 8:2 训练/测试切分，通过训练集自动学习 TF-IDF + kNN 分类模型。

当前推荐将模型类作为泛化主流程，将规则类作为 baseline 和审计对照。

## 覆盖标签

模型输出四类标签：

```text
none
video_meta_leak
re_identify
domain
```

训练时的标签映射：

- `final_violation_type` 属于 `video_meta_leak`、`re_identify`、`domain` 时保留。
- 其他违规类型统一映射为 `none`。

## 文件结构

```text
model_detectors/
  README.md
  __init__.py
  feature_extractor.py          # 特征抽取，不使用静态业务词库
  tfidf_knn.py                  # 纯标准库 TF-IDF + kNN
  split_dataset.py              # 8:2 切分
  train_classifier.py           # 训练入口
  predict_classifier.py         # 预测入口
  evaluate_classifier.py        # 测试集评测入口
  metrics.py                    # 指标计算
  io_utils.py                   # JSONL 读写
  models/
    ml_detector.json            # 已训练模型
```

## 输入格式

输入为 normalized JSONL，每行一条样本：

```json
{
  "data_id": "anno-example-001",
  "sample_id": "example-001",
  "data_type": "traffic_flow",
  "gate": "OutputGate",
  "content_text": "模型输出文本或上下文证据文本",
  "features": {
    "content.text": "原始内容中的文本字段",
    "features.route": "A->B"
  }
}
```

模型使用的输入信息：

- `content_text`。
- `features` 的字段路径和值。
- `data_type`、`gate`。
- 通用结构特征，例如 URL、IP、时间、经纬度、长数字/哈希、小计数、箭头路线格式、字段数、文本长度、数字比例。

模型不会读取规则类静态词表，也不会读取人工标签参与预测。

## 使用方法

### 1. 生成 8:2 训练/测试集

```bash
python3 violation_detection/model_detectors/split_dataset.py \
  --input normalized/all_normalized_800.jsonl \
  --output-dir normalized/splits \
  --test-ratio 0.2 \
  --seed 20260618
```

输出：

```text
normalized/splits/train.jsonl
normalized/splits/test.jsonl
normalized/splits/split_summary.json
```

### 2. 训练模型

```bash
python3 violation_detection/model_detectors/train_classifier.py \
  --train normalized/splits/train.jsonl \
  --model violation_detection/model_detectors/models/ml_detector.json \
  --min-df 1 \
  --max-features 50000 \
  --k 1
```

### 3. 在测试集上评测

```bash
python3 violation_detection/model_detectors/evaluate_classifier.py \
  --input normalized/splits/test.jsonl \
  --model violation_detection/model_detectors/models/ml_detector.json \
  --min-accuracy 0.9
```

输出：

```text
reports/ml_eval_summary.json
reports/ml_eval_mismatches.jsonl
```

### 4. 对任意 JSONL 预测

```bash
python3 violation_detection/model_detectors/predict_classifier.py \
  --input normalized/all_normalized_800.jsonl \
  --model violation_detection/model_detectors/models/ml_detector.json \
  --output reports/ml_predictions.jsonl
```

## 输出格式

`predict_classifier.py` 输出 JSONL，每行结构如下：

```json
{
  "data_id": "anno-example-001",
  "sample_id": "example-001",
  "data_type": "traffic_flow",
  "gate": "OutputGate",
  "predicted_violation_type": "re_identify",
  "confidence": 1.0,
  "reason_code": "ml_tfidf_knn_classifier",
  "action_suggestion": ["manual_review"],
  "probabilities": {
    "none": 0.0,
    "video_meta_leak": 0.0,
    "re_identify": 1.0,
    "domain": 0.0
  },
  "nearest_neighbors": [],
  "evidence_features": []
}
```

字段说明：

- `predicted_violation_type`：预测标签。
- `confidence`：kNN 投票置信度。
- `probabilities`：四类标签概率。
- `nearest_neighbors`：最相似训练样本，便于审计。
- `evidence_features`：与最近邻重叠贡献最高的特征。

## 实现原理

模型类实现不使用业务词表，而是从训练集自动学习特征权重。

特征来源：

- 字符 n-gram：适合中文、混合字段、URL、编号。
- token n-gram：适合英文路径、字段名、接口和结构化片段。
- 字段路径 n-gram：例如 `content.metadata.camera_id`、`features.route`。
- 元信息：`data_type`、`gate`。
- 通用结构特征：URL、IP、时间、经纬度、长数字/哈希、小计数赋值、箭头路线、文本长度、数字比例等。

分类方法：

1. 训练集抽取稀疏特征。
2. 计算 document frequency 和 IDF。
3. 训练样本转成 L2 归一化 TF-IDF 向量。
4. 预测样本转成同一向量空间。
5. 计算与训练样本的 cosine similarity。
6. 使用最近邻标签作为预测结果。

当前参数：

```text
min_df = 1
max_features = 50000
k = 1
```

## 0624 修改版：清空权重后的最新 8:2 测试

### 数据范围与统计口径

本轮只使用 `违规测试-0624修改版/数据汇总.jsonl` 中属于以下目标任务的样例：

```text
domain
re_identify
video_meta_leak
```

每个目标任务同时保留其 `final_violation_type=none` 的负例。其他目标违规类型不参与本轮训练或测试。

0624 汇总文件包含 JSONL/YAML 格式副本、按违规类型拆分副本等物理重复记录。直接随机切分会让同一内容同时出现在训练集和测试集，因此先执行以下处理：

1. 对 `raw_content` 做 key 排序后的 canonical JSON 序列化。
2. 计算 SHA-256，相同内容只保留一条。
3. 检查同内容重复组的 `target_violation_type / final_violation_type / is_positive`，确认没有标签冲突。
4. 按 `target_violation_type × final_violation_type` 六个分层单元分别执行 8:2 划分。
5. 使用固定随机种子 `20260624`。
6. 划分后检查 `data_id`、`original_data_id`、`sample_id`、内容 SHA-256，四项训练/测试重叠均必须为 0。

去重结果：

| 项目 | 数量 |
| --- | ---: |
| 三类物理记录 | 441 |
| 去重后唯一样例 | 282 |
| 移除的重复记录 | 159 |
| 含重复的内容组 | 109 |
| 重复组标签冲突 | 0 |

### 8:2 数据分布

| 目标类型 | 全部正例 | 全部负例 | 全部 | 训练正例 | 训练负例 | 训练合计 | 测试正例 | 测试负例 | 测试合计 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `domain` | 56 | 52 | 108 | 45 | 42 | 87 | 11 | 10 | 21 |
| `re_identify` | 63 | 61 | 124 | 50 | 49 | 99 | 13 | 12 | 25 |
| `video_meta_leak` | 30 | 20 | 50 | 24 | 16 | 40 | 6 | 4 | 10 |
| **合计** | **149** | **133** | **282** | **119** | **107** | **226** | **30** | **26** | **56** |

实际比例为训练集 `226/282 = 80.14%`、测试集 `56/282 = 19.86%`。与精确 80% 的微小差异来自六个分层单元分别取整。

划分数据及审计信息：

```text
normalized/0624_supported_split/all_supported_deduplicated.jsonl
normalized/0624_supported_split/train.jsonl
normalized/0624_supported_split/test.jsonl
normalized/0624_supported_split/dedup_removed.jsonl
normalized/0624_supported_split/split_summary.json
```

### 从零训练

本轮使用独立的新模型文件，训练入口会新建 `TfidfKNNModel` 并调用 `fit`，不会加载或继承 `ml_detector.json` 等旧模型权重：

```bash
python3 violation_detection/model_detectors/train_classifier.py \
  --train normalized/0624_supported_split/train.jsonl \
  --model violation_detection/model_detectors/models/ml_detector_0624_fresh.json \
  --min-df 1 \
  --max-features 50000 \
  --k 1
```

本模型使用字符 n-gram、token n-gram、字段路径、`data_type`、`gate` 和通用结构特征构建 TF-IDF 稀疏向量，通过 cosine similarity 选择最近邻标签。训练阶段的 100% 命中是 `k=1` 对训练样例查找自身的预期结果，不作为泛化指标。

### 测试与预测命令

```bash
python3 violation_detection/model_detectors/evaluate_classifier.py \
  --input normalized/0624_supported_split/test.jsonl \
  --model violation_detection/model_detectors/models/ml_detector_0624_fresh.json \
  --output-summary reports/0624_supported_eval_summary.json \
  --output-mismatches reports/0624_supported_eval_mismatches.jsonl \
  --min-accuracy 0.98

python3 violation_detection/model_detectors/predict_classifier.py \
  --input normalized/0624_supported_split/test.jsonl \
  --model violation_detection/model_detectors/models/ml_detector_0624_fresh.json \
  --output reports/0624_supported_test_predictions.jsonl
```

### 本轮测试结果

| label | support | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: |
| `none` | 26 | 1.0000 | 0.9615 | 0.9804 |
| `domain` | 11 | 1.0000 | 1.0000 | 1.0000 |
| `re_identify` | 13 | 0.9286 | 1.0000 | 0.9630 |
| `video_meta_leak` | 6 | 1.0000 | 1.0000 | 1.0000 |

总体结果：

```text
samples = 56
correct = 55
accuracy = 0.9821
macro_f1 = 0.9859
mismatches = 1
```

混淆矩阵中只有一处错误：一条 `none` 被预测为 `re_identify`。该样例是包含 `user_id`、`imei`、位置等词的否定表达，最近邻相似度为 `0.131601`，说明 `k=1` 对否定语境和低相似度最近邻仍较敏感。

完整表格报告与明细：

```text
reports/0624_supported_eval_report.md
reports/0624_supported_eval_summary.json
reports/0624_supported_eval_mismatches.jsonl
reports/0624_supported_test_predictions.jsonl
```

## 历史基线：旧 818 条数据切分

当前 8:2 切分：

- 总样本：818 条。
- 训练集：654 条。
- 测试集：164 条。

测试集指标：

| label | support | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: |
| `none` | 132 | 1.0000 | 0.9848 | 0.9924 |
| `video_meta_leak` | 7 | 1.0000 | 1.0000 | 1.0000 |
| `re_identify` | 13 | 1.0000 | 1.0000 | 1.0000 |
| `domain` | 12 | 0.8571 | 1.0000 | 0.9231 |

总体：

```text
accuracy = 0.9878
macro_f1 = 0.9789
mismatches = 2 / 164
```

误差样本见：

```text
reports/ml_eval_mismatches.jsonl
```

## 注意事项

- 0624 最新指标来自去重后的固定分层 8:2 切分，不等价于跨域泛化保证。
- 当前只排除了精确内容重复，没有自动合并语义近似或模板近似样例。
- 模型仍依赖训练集覆盖；新增行业、新字段、新表达后建议重新切分和训练。
- `k=1` 对近似负样本敏感，0624 本轮唯一误差来自否定语境与正样本表达相似。
- 如需进一步降低误报，可补充 hard negative 后重新训练，或尝试 `k=3/5` 和相似度阈值。
