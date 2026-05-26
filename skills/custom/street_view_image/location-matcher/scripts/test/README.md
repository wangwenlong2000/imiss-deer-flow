# Location Matcher Scripts - 测试文档

## 目录

1. [快速开始](QUICKSTART.md) - 测试运行指南
2. [测试总结](TEST_SUMMARY.md) - 详细测试结果和分析
3. [本文件](README.md) - 总体说明

## 概述

本目录包含 `location-matcher` 项目中所有脚本的单元测试。测试验证脚本在 `conda run -n deerflow-street python` 环境下的执行能力。

## 测试文件

### `test_scripts_execution.py`

主测试文件，包含 30 个测试用例，覆盖以下功能：

| 测试类 | 测试数量 | 通过 | 跳过 | 说明 |
|--------|---------|------|------|------|
| `TestScriptExecution` | 6 | 6 | 0 | 脚本执行和 --help |
| `TestESGetMapping` | 1 | 1 | 0 | ES 索引 mapping 获取 |
| `TestESListIndices` | 2 | 2 | 0 | ES 索引列表查询 |
| `TestESQueryDSL` | 3 | 2 | 1 | ES DSL 查询执行 |
| `TestESRetrieveTopK` | 5 | 4 | 1 | Top-K 检索功能 |
| `TestGeocoding` | 2 | 2 | 0 | 地理编码功能 |
| `TestStreetServer` | 5 | 5 | 0 | 模型服务 API |
| `TestScriptImports` | 6 | 6 | 0 | 模块导入验证 |
| **总计** | **30** | **28** | **2** | |

## 服务配置

### 外部服务

- **Elasticsearch**: `http://localhost:3128`
  - 索引: `street`
  - 状态: ✅ 正常运行
  - Vector 索引: ✅ `index=true` (已修复)

- **模型服务**: `http://localhost:3130`
  - 状态: ✅ 正常运行
  - 可用模型:
    - `Qwen3-VL-Embedding-2B` (2048 维)
    - `ImAge4VPR` (6144 维)
    - `Qwen3-VL-Reranker-2B`

### Conda 环境

- **环境名**: `deerflow-street`
- **Python 版本**: 3.12
- **状态**: ✅ 已安装所有依赖

## 测试数据

### 测试图片
```
/mnt/nas/streetview_meta/queries/247query/247query/00001.jpg
```

描述：日本东京涩谷十字路口的街景照片

### 测试描述文本
```
日本东京涩谷十字路口的街景，画面中可以看到标志性的玻璃幕墙建筑——Q-FRONT大楼，
其底层设有星巴克和TSUTAYA书店...
```

完整描述见 `test_scripts_execution.py` 中的 `TEST_DESCRIPTION` 常量。

## 运行测试

### 基本命令

```bash
# 进入脚本目录
cd /home/anker/imiss-deer-flow/skills/custom/location-matcher/scripts

# 运行所有测试
python -m unittest discover -s test -p "test_*.py" -v

# 运行特定测试类
python -m unittest test.test_scripts_execution.TestScriptExecution -v
```

### 详细输出

添加 `-v` 参数显示每个测试的详细信息：

```bash
python -m unittest discover -s test -p "test_*.py" -v 2>&1 | less
```

### 保存结果

```bash
python -m unittest discover -s test -p "test_*.py" -v > test_results.log 2>&1
```

## 测试覆盖详情

### ✅ 通过测试 (28 个)

#### 脚本执行 (6/6)
- `test_conda_environment_exists` - 验证 conda 环境存在
- `test_es_get_mapping_help` - es_get_mapping.py --help
- `test_es_list_indices_help` - es_list_indices.py --help
- `test_es_query_dsl_help` - es_query_dsl.py --help
- `test_es_retrieve_topk_help` - es_retrieve_topk.py --help
- `test_geocoding_help` - geocoding.py --help

#### Elasticsearch 集成 (7/8)
- `test_es_get_mapping_street_index` - 获取并验证 street 索引 mapping
- `test_es_list_indices` - 列出所有 ES 索引
- `test_es_list_indices_contains_expected_fields` - 验证 street 索引字段
- `test_es_query_dsl_simple` - 简单 match_all 查询
- `test_es_query_dsl_with_source` - 带 _source 过滤的查询

#### Top-K 检索 (4/5)
- `test_retrieve_by_description` - 文本语义检索
- `test_retrieve_by_image` - 图片向量检索 ✅
- `test_retrieve_with_geo_filter` - 带地理过滤的检索
- `test_retrieve_with_thresholds` - 带分数阈值的检索

#### 地理编码 (2/2)
- `test_geocoding_fixed_location` - 固定坐标返回
- `test_geocoding_various_addresses` - 多种地址输入

#### 模型服务 (5/5)
- `test_health_check` - 服务健康检查
- `test_list_models` - 模型列表
- `test_embed_text` - 文本嵌入
- `test_embed_image` - 图片嵌入
- `test_embed_text_with_instruction` - 多种 instruction keys

#### 模块导入 (6/6)
- `test_import_es_get_mapping_via_conda` - es_get_mapping.py
- `test_import_es_list_indices_via_conda` - es_list_indices.py
- `test_import_es_query_dsl_via_conda` - es_query_dsl.py
- `test_import_es_retrieve_topk_via_conda` - es_retrieve_topk.py
- `test_import_geocoding` - geocoding.py
- `test_import_street_server` - street_server.py

### ⏭️ 跳过测试 (2 个)

#### 1. test_es_query_dsl_stdin
- **原因**: stdin 在 conda run 下不工作
- **影响**: 低
- **替代**: 使用 --dsl 参数

#### 2. test_retrieve_multimodal
- **原因**: RRF 需要商业 ES 许可证
- **错误**: `AuthorizationException(403)`
- **影响**: 中
- **替代**: 分别执行文本和图片检索

## Elasticsearch Mapping

### 当前配置

```json
{
  "street": {
    "mappings": {
      "properties": {
        "id": { "type": "keyword" },
        "source_path": { "type": "keyword" },
        "metadata": {
          "properties": {
            "latitude": { "type": "double" },
            "longitude": { "type": "double" },
            "utm_easting": { "type": "double" },
            "utm_northing": { "type": "double" }
          }
        },
        "vector-ImAge4VPR": {
          "type": "dense_vector",
          "dims": 4096,
          "index": true,
          "similarity": "cosine",
          "index_options": {
            "type": "bbq_hnsw",
            "m": 16,
            "ef_construction": 100,
            "rescore_vector": { "oversample": 3.0 }
          }
        },
        "vector-Qwen3-VL-Embedding-2B_urban_governance": {
          "type": "dense_vector",
          "dims": 2048,
          "index": true,
          "similarity": "cosine",
          "index_options": {
            "type": "bbq_hnsw",
            "m": 16,
            "ef_construction": 100,
            "rescore_vector": { "oversample": 3.0 }
          }
        }
      }
    }
  }
}
```

### 字段说明

| 字段 | 类型 | 维度 | 索引 | 用途 |
|------|------|------|------|------|
| `id` | keyword | - | ✅ | 唯一标识符 |
| `source_path` | keyword | - | ✅ | 图片路径 |
| `metadata.latitude` | double | - | ✅ | 纬度 |
| `metadata.longitude` | double | - | ✅ | 经度 |
| `metadata.utm_easting` | double | - | ✅ | UTM 东坐标 |
| `metadata.utm_northing` | double | - | ✅ | UTM 北坐标 |
| `vector-ImAge4VPR` | dense_vector | 4096 | ✅ | 图片向量 |
| `vector-Qwen3-VL-Embedding-2B_urban_governance` | dense_vector | 2048 | ✅ | 文本向量 |

## 已知问题和解决方案

### 问题 1: ES Vector 索引 (已解决 ✅)

**描述**: vector 字段 `index=false` 导致 knn 搜索不可用

**影响**:
- ❌ 图片向量检索
- ❌ RRF 融合检索

**解决**: 更新 ES mapping 设置 `index=true`

**验证**:
```bash
python -m unittest test.test_scripts_execution.TestESRetrieveTopK.test_retrieve_by_image -v
```

### 问题 2: RRF 许可证限制 (待解决)

**描述**: RRF (Reciprocal Rank Fusion) 需要商业 Elasticsearch 许可证

**ES 版本信息**:
- 版本: 9.3.3
- 构建: Docker
- 许可证类型: **basic** (免费版本)

**错误信息**:
```
AuthorizationException(403, 'security_exception', 
'current license is non-compliant for [Reciprocal Rank Fusion (RRF)]')
```

**许可证对比**:

| 许可证类型 | RRF 支持 | 说明 |
|-----------|---------|------|
| **basic** (当前) | ❌ 不支持 | 免费版本，仅包含基本功能 |
| **trial** | ✅ 支持 | 30 天试用期 |
| **gold** | ✅ 支持 | 商业许可证 |
| **platinum** | ✅ 支持 | 商业许可证 |
| **enterprise** | ✅ 支持 | 商业许可证 |

**验证当前许可证**:
```bash
curl -u citybrain-street:123456 http://localhost:3128/_license
# 输出: "type": "basic"  ← 这就是 RRF 不可用的原因
```

**影响**: 
- ❌ RRF 多模态融合检索不可用
- ✅ 单模态向量检索 (文本/图片) 正常工作

**替代方案**:
1. **分别检索**: 分别执行文本和图片检索，然后在应用层合并结果
2. **自定义融合**: 实现加权分数或其他融合算法
3. **升级许可证**: 联系 Elastic 购买 Gold/Platinum/Enterprise 许可证

**示例 - 分别检索**:
```bash
# 文本检索
python es_retrieve_topk.py --index street --description "..." --k 5

# 图片检索
python es_retrieve_topk.py --index street --image-path "..." --k 5

# 在应用层合并结果 (自定义 RRF 或其他融合策略)
```

### 问题 3: stdin 不支持 (技术限制)

**描述**: `conda run` 不转发 stdin 到子进程

**影响**: 无法通过 stdin 传递 DSL 查询

**替代方案**:
```bash
# 使用 --dsl 参数
conda run -n deerflow-street python es_query_dsl.py \
  --index street \
  --dsl '{"query": {"match_all": {}}}' \
  --es-url http://localhost:3128
```

## 测试脚本

### 创建测试数据

如果需要创建新的测试数据：

```python
# 示例：创建测试 JSONL 文件
import json

data = [
    {"id": "test_001", "image_path": "/path/to/image1.jpg"},
    {"id": "test_002", "image_path": "/path/to/image2.jpg"},
]

with open("/tmp/test.jsonl", "w") as f:
    for item in data:
        f.write(json.dumps(item) + "\n")
```

### 验证服务状态

```bash
# 检查 Elasticsearch
curl http://localhost:3128/_cluster/health?pretty

# 检查模型服务
curl http://localhost:3130/health

# 检查索引
curl http://localhost:3128/_cat/indices?v
```

## 贡献指南

### 添加新测试

1. 在 `test_scripts_execution.py` 中添加测试方法
2. 使用 `@unittest.skip` 装饰器标记已知失败的测试
3. 在文档中更新测试结果

### 测试命名规范

- `test_<功能>_<具体场景>` 
- 示例: `test_retrieve_by_description`, `test_health_check`

### 文档更新

每次修改测试后，同步更新：
- `TEST_SUMMARY.md` - 详细测试结果
- `QUICKSTART.md` - 快速开始指南
- `README.md` - 总体说明

## 参考资料

- [Elasticsearch kNN 搜索文档](https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html)
- [RRF 融合算法](https://www.elastic.co/guide/en/elasticsearch/reference/current/rrf.html)
- [Conda 运行命令](https://docs.conda.io/projects/conda/en/latest/commands/run.html)

## 维护者

- 测试创建: 2026-05-01
- 最后更新: 2026-05-01
- 状态: ✅ 维护中



cd /home/anker/imiss-deer-flow/skills/custom/location-matcher/scripts && \
conda run -n deerflow-street python skills/custom/location-matcher/scripts/es_retrieve_topk.py \
  --target-field id,metadata.latitude,metadata.longitude,source_path \
  --index street \
  --k 5 \
  --center-latitude 35.658 \
  --center-longitude 139.7016 \
  --max-distance 2000 \
  --image-path /mnt/nas/streetview_meta/queries/247query/247query/00001.jpg \
  --es-url http://172.17.0.1:3128 \
  --es-username citybrain-street \
  --es-password 123456 \
  2>&1 | tail -40




conda run -n deerflow-street python skills/custom/location-matcher/scripts/es_retrieve_topk.py \
  --target-field id,metadata.latitude,metadata.longitude,source_path \
  --index street \
  --k 50 \
  --description "交通事故" \
  --es-url http://172.17.0.1:3128 \
  --es-username citybrain-street \
  --es-password 123456 \
  2>&1 | tail -400


conda run -n deerflow-street python skills/custom/location-matcher/scripts/es_retrieve_topk.py \
  --target-field id,metadata.latitude,metadata.longitude,source_path \
  --index street \
  --k 50 \
  --description "占道摆摊" \
  --es-url http://172.17.0.1:3128 \
  --es-username citybrain-street \
  --es-password 123456 \
  2>&1 | tail -400




conda run -n deerflow-street python skills/custom/location-matcher/scripts/es_retrieve_topk.py --help

conda run -n deerflow-street python /mnt/skills/custom/location-matcher/scripts/es_retrieve_topk.py --target-field id,metadata.latitude,metadata.longitude,source_path --index street --k 5 --center-latitude 35.658 --center-longitude 139.7016 --max-distance 2000 --image-path /mnt/user-data/uploads/00001.jpg



conda run -n deerflow-street python /mnt/skills/custom/location-matcher/scripts/es_retrieve_topk.py --target-field id,metadata.latitude,metadata.longitude,source_path --index street --k 5 --description "交通事故"