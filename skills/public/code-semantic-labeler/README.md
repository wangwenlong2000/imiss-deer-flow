# 代码语义标注器

一个使用责任链模式实现代码语义标注的 DeerFlow 技能，将代码中的节点映射到城市业务语义。

## 功能

- **责任链模式**：按顺序使用多个标注器，规则库优先，LLM 兜底
- **规则基于标注**：使用精确或模糊前缀匹配进行语义标注
- **LLM 兜底**：当规则未命中时，使用 LLM 进行语义标注
- **代码总结**：当 ontology_rules 中没有标注时，调用大模型对代码进行总结
- **AST 分析**：使用 code-to-ast-new 进行真实的 AST 分析
- **本体规则管理**：提供接口管理 ontology_rules，支持从文件加载和保存
- **工作流引擎**：串联代码切分、AST 分析和语义标注的完整流程
- **高可扩展性**：可以轻松添加新的标注器或修改规则

## 架构设计

- **抽象基类**：`BaseLabeler` 定义统一的标注接口
- **具体实现**：
  - `RuleBasedLabeler`：基于规则的语义标注器
  - `LLMFallbackLabeler`：LLM 兜底语义标注器
- **责任链**：`SemanticLabelingPipeline` 管理多个标注器并按顺序执行
- **代码总结**：`CodeSummarizer` 使用 LLM 对代码进行总结
- **工作流引擎**：`DeerFlowOrchestrator` 串联完整的处理流程
- **本体管理**：`OntologyManager` 管理 ontology_rules，支持从文件加载和保存

## 使用方法

### 编程接口

```python
from semantic_labeler import RuleBasedLabeler, LLMFallbackLabeler, SemanticLabelingPipeline, DeerFlowOrchestrator, CodeSummarizer, OntologyManager

# 初始化本体管理器
ontology_manager = OntologyManager("ontology_rules.json")

# 获取本体规则
ontology_rules = ontology_manager.get_rules()

# 如果规则为空，使用默认规则
if not ontology_rules:
    ontology_rules = {
        "cv2": "Camera",
        "model": "Detection",
        "tracker": "Tracking",
        "db": "Database",
        "numpy": "DataProcessing",
        "pandas": "DataProcessing",
    }
    # 保存默认规则
    ontology_manager.update_rules(ontology_rules)
    ontology_manager.save_rules()

# 创建标注器
rule_labeler = RuleBasedLabeler(ontology_rules)
llm_labeler = LLMFallbackLabeler()

# 创建语义标注管道
pipeline = SemanticLabelingPipeline([rule_labeler, llm_labeler])

# 创建代码总结器
code_summarizer = CodeSummarizer()

# 创建工作流引擎
orchestrator = DeerFlowOrchestrator(pipeline, code_summarizer)

# 处理原始代码
code = """
import cv2

# 读取视频
cap = cv2.VideoCapture(0)

# 目标检测
results = model.predict(frame)

# 目标跟踪
tracks = tracker.update(results)

# 保存结果
db.save(tracks)
"""

result = orchestrator.process_raw_code(code)
print(result)
```

### 命令行使用

```bash
# 分析代码字符串
python scripts/label.py \
  --code "import cv2\ncap = cv2.VideoCapture(0)\nresults = model.predict(frame)\ntracks = tracker.update(results)\ndb.save(tracks)"

# 分析代码文件
python scripts/label.py \
  --file input.py \
  --output output.json

# 添加本体规则
python scripts/label.py \
  --add-rule "cv2" "Camera"

# 指定本体规则文件
python scripts/label.py \
  --code "import cv2\ncap = cv2.VideoCapture(0)" \
  --rules-file custom_rules.json
```

## 参数

| 参数 | 描述 |
|------|------|
| `--code` | 要分析的 Python 代码字符串 |
| `--file` | 包含要分析的代码的文件路径 |
| `--output` | 保存输出结果的文件路径 |
| `--rules-file` | 本体规则文件路径 |
| `--add-rule` | 添加本体规则，格式为 `PATTERN LABEL` |
| `--update-rules` | 更新本体规则 |

## 本体规则示例

| 模式 | 标签 |
|------|------|
| `cv2` | `Camera` |
| `model` | `Detection` |
| `tracker` | `Tracking` |
| `db` | `Database` |
| `numpy` | `DataProcessing` |
| `pandas` | `DataProcessing` |
| `tensorflow` | `MachineLearning` |
| `pytorch` | `MachineLearning` |

## 输出格式

输出是一个包含语义标注结果的 JSON 字典：

```json
{
  "status": "success",
  "pipeline_steps": ["split", "ast_extract", "semantic_label", "code_summary"],
  "labeled_nodes": [
    {"name": "cv2.VideoCapture", "type": "Camera"},
    {"name": "model.predict", "type": "Detection"},
    {"name": "tracker.update", "type": "Tracking"},
    {"name": "db.save", "type": "Database"}
  ],
  "code_summary": "这是一段包含视频流读取和目标检测的 Python 代码。"
}
```

## 依赖

- Python 3.7+
- 标准库 `abc`, `ast`, `json`, `os`, `sys`
- code-to-ast-new 技能

## 注意事项

- 此技能使用模拟的 LLM 调用，实际实现中应该调用真实的 LLM API
- 规则库的质量直接影响标注结果的准确性
- 对于复杂的代码，可能需要更复杂的 AST 分析逻辑
- 代码总结功能在规则未命中时触发，提供代码的整体语义理解
- 本体规则文件默认保存在技能目录下的 ontology_rules.json 文件中
- 工作流引擎中的代码切分目前是模拟实现，实际使用时需要集成真实的技能

## 示例

### 示例 1：视频处理代码

**输入：**
```python
import cv2

# 读取视频
cap = cv2.VideoCapture(0)

# 处理帧
while True:
    ret, frame = cap.read()
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    cv2.imshow('Frame', gray)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()
cv2.destroyAllWindows()
```

**输出：**
```json
{
  "status": "success",
  "pipeline_steps": ["split", "ast_extract", "semantic_label", "code_summary"],
  "labeled_nodes": [
    {"name": "cv2.VideoCapture", "type": "Camera"},
    {"name": "cap.read", "type": "Camera"},
    {"name": "cv2.cvtColor", "type": "ImageProcessing"},
    {"name": "cv2.imshow", "type": "ImageProcessing"},
    {"name": "cv2.waitKey", "type": "ImageProcessing"},
    {"name": "cap.release", "type": "Camera"},
    {"name": "cv2.destroyAllWindows", "type": "ImageProcessing"}
  ],
  "code_summary": "这是一段包含视频流读取和目标检测的 Python 代码。"
}
```

### 示例 2：目标检测代码

**输入：**
```python
import cv2

# 加载模型
model = cv2.dnn.readNetFromCaffe('deploy.prototxt', 'model.caffemodel')

# 读取图像
image = cv2.imread('image.jpg')

# 目标检测
blob = cv2.dnn.blobFromImage(image, 0.007843, (300, 300), 127.5)
model.setInput(blob)
detections = model.forward()

# 处理检测结果
for i in range(detections.shape[2]):
    confidence = detections[0, 0, i, 2]
    if confidence > 0.2:
        # 绘制边界框
        box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
        (startX, startY, endX, endY) = box.astype("int")
        cv2.rectangle(image, (startX, startY), (endX, endY), (0, 255, 0), 2)

# 显示结果
cv2.imshow('Output', image)
cv2.waitKey(0)
```

**输出：**
```json
{
  "status": "success",
  "pipeline_steps": ["split", "ast_extract", "semantic_label", "code_summary"],
  "labeled_nodes": [
    {"name": "cv2.dnn.readNetFromCaffe", "type": "Detection"},
    {"name": "cv2.imread", "type": "ImageProcessing"},
    {"name": "cv2.dnn.blobFromImage", "type": "ImageProcessing"},
    {"name": "model.setInput", "type": "Detection"},
    {"name": "model.forward", "type": "Detection"},
    {"name": "np.array", "type": "DataProcessing"},
    {"name": "box.astype", "type": "DataProcessing"},
    {"name": "cv2.rectangle", "type": "ImageProcessing"},
    {"name": "cv2.imshow", "type": "ImageProcessing"},
    {"name": "cv2.waitKey", "type": "ImageProcessing"}
  ],
  "code_summary": "这是一段包含视频流读取和目标检测的 Python 代码。"
}
```

## 集成到 DeerFlow

1. 将此技能添加到 DeerFlow 的技能列表中
2. 在需要语义标注的地方调用此技能
3. 使用标注结果为 Agent 提供代码的语义上下文

## 扩展指南

### 添加新的标注器

1. 继承 `BaseLabeler` 类
2. 实现 `label` 方法
3. 将新标注器添加到 `SemanticLabelingPipeline` 中

### 修改本体规则

1. 编辑 ontology_rules.json 文件
2. 或使用命令行参数 `--add-rule` 添加规则
3. 或使用 `OntologyManager` 类的方法管理规则

### 集成真实的 LLM

1. 修改 `LLMFallbackLabeler._call_llm_api` 方法，调用真实的 LLM API
2. 修改 `CodeSummarizer._call_llm_api` 方法，调用真实的 LLM API

### 集成真实的代码切分

修改 `DeerFlowOrchestrator._mock_code_split` 方法，集成真实的代码切分技能

### 扩展 AST 分析

修改 `DeerFlowOrchestrator._extract_nodes_from_ast` 方法，添加更多的节点提取逻辑

## 本体规则管理

`OntologyManager` 类提供了以下方法来管理本体规则：

- `load_rules()`: 从文件加载规则
- `save_rules()`: 保存规则到文件
- `add_rule(pattern, label)`: 添加规则
- `remove_rule(pattern)`: 删除规则
- `get_rules()`: 获取所有规则
- `update_rules(new_rules)`: 更新规则

## 贡献

欢迎贡献！请随时提交 Pull Request。
