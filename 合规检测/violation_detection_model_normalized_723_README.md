# 违规检测模型方法与数据包

本包将以下三部分内容统一交付：

1. `violation_detection/model_detectors/`：违规检测的模型方法实现及已训练模型。
2. `normalized/`：当前工作区中的完整标准化 JSONL 数据、数据切分和统计文件。
3. `data/7.23 数据（清洗版).zip`：7 月 23 日清洗版原始数据压缩包。

## 包内目录

```text
README.md
SHA256SUMS
violation_detection/
  __init__.py
  model_detectors/
    *.py                         # 特征抽取、TF-IDF+kNN、训练、预测、评测与切分
    models/                      # 已训练模型版本
    tests/                       # 留出集切分单元测试
normalized/
  0624_supported_split/          # 0624 三类任务去重后的 8:2 数据
  splits/                        # 全量标准化数据的 8:2 数据
  by_violation/                  # 按违规类型汇总
  by_data_type/                  # 按数据类型汇总
  all_sources_by_violation_type/ # 各来源按违规类型汇总
  external_test/                 # 外部测试及去污染数据
  new_data_street_view/          # 新街景数据及其切分、统计
  all_normalized_800.jsonl       # 全量标准化数据
data/
  7.23 数据（清洗版).zip
```

## 方法说明

模型采用纯 Python 标准库实现的 TF-IDF + kNN 流程：

- 从训练数据自动学习字符 n-gram、token n-gram、字段路径、数据类型、Gate 及通用结构特征；
- 使用 TF-IDF 和 cosine similarity 检索训练集近邻；
- 输出 `none`、`domain`、`re_identify`、`video_meta_leak` 四类标签；
- 不读取规则检测器的静态业务词库，也不依赖第三方机器学习库。

`model_detectors/models/` 中同时保留多个已训练版本，文件名对应训练数据或实验用途；如需严格复现实验，请使用与目标数据切分对应的模型文件。

## 快速开始

在解压后的包根目录运行。要求 Python 3.9 或更高版本；运行模型方法只需要 Python 标准库。

### 评测 0624 三类任务模型

```bash
python3 violation_detection/model_detectors/evaluate_classifier.py \
  --input normalized/0624_supported_split/test.jsonl \
  --model violation_detection/model_detectors/models/ml_detector_0624_fresh.json \
  --output-summary reports/0624_supported_eval_summary.json \
  --output-mismatches reports/0624_supported_eval_mismatches.jsonl \
  --min-accuracy 0.98
```

该切分的配套训练集、测试集、去重记录和审计统计位于 `normalized/0624_supported_split/`。

### 使用模型预测

```bash
python3 violation_detection/model_detectors/predict_classifier.py \
  --input normalized/0624_supported_split/test.jsonl \
  --model violation_detection/model_detectors/models/ml_detector_0624_fresh.json \
  --output reports/0624_supported_test_predictions.jsonl
```

### 从零训练

```bash
python3 violation_detection/model_detectors/train_classifier.py \
  --train normalized/0624_supported_split/train.jsonl \
  --model /tmp/ml_detector_fresh.json \
  --min-df 1 \
  --max-features 50000 \
  --k 1
```

训练入口会新建模型，不会加载已有模型权重。训练输出中的训练集 100% 命中不代表泛化能力，应以独立测试集结果为准。

## 输入与输出

输入为 UTF-8 编码的 JSONL，每行一条标准化样本，主要字段包括：

```text
data_id / sample_id
data_type
gate 或 trigger_gate
content_text
features
final_violation_type
```

预测输出为 JSONL，包含 `predicted_violation_type`、`confidence`、`probabilities`、`nearest_neighbors` 和 `evidence_features` 等字段。完整字段说明见 `violation_detection/model_detectors/README.md`。

## 数据注意事项

- `normalized/` 是标准化后的数据集合，不同子目录可能包含同一来源的不同汇总、切分或审计副本；使用时应根据目录含义选择输入，避免把副本重复合并。
- `normalized/0624_supported_split/` 是针对 `domain`、`re_identify`、`video_meta_leak` 三类任务的去重 8:2 切分，负例统一以 `none` 参与四分类评测。
- 7.23 清洗版以原始 ZIP 形式保留，解压后包含 10 个 JSONL 数据文件；其内容与 `normalized/` 的标准化数据不是同一层级的副本。
- 数据中可能包含敏感或业务样例，仅限授权的开发、评测和审计环境使用。

## 完整性校验

包根目录的 `SHA256SUMS` 保存了除自身外所有文件的 SHA-256。解压后可运行：

```bash
sha256sum -c SHA256SUMS
```

