# 测试总结

## 测试结果

```
Ran 30 tests in 46.486s
OK (skipped=2)

✅ 28 个测试通过
⏭️ 2 个测试跳过 (已知问题，有详细文档)
```

## 测试覆盖范围

### ✅ 已验证的功能 (28 个测试通过)

#### 1. 脚本执行 (6 个测试)
- ✅ Conda 环境 `deerflow-street` 存在
- ✅ `es_get_mapping.py --help` 执行成功
- ✅ `es_list_indices.py --help` 执行成功
- ✅ `es_query_dsl.py --help` 执行成功
- ✅ `es_retrieve_topk.py --help` 执行成功
- ✅ `geocoding.py --help` 执行成功

#### 2. Elasticsearch 集成 (7 个测试)
- ✅ **Get Mapping**: 成功获取 `street` 索引的 mapping
  - 验证字段类型: `id` (keyword), `source_path` (keyword), `metadata.*` (double)
  - 验证 vector 字段: `vector-ImAge4VPR`, `vector-Qwen3-VL-Embedding-2B_urban_governance`
  - ✅ **已修复**: vector 字段的 `index=true`，支持 knn 搜索
  - 验证 vector 维度: ImAge4VPR (4096), Qwen3-VL-Embedding-2B (2048)
  - 验证相似度: cosine
  
- ✅ **List Indices**: 成功列出所有索引
  - 验证 `street` 索引存在
  - 验证索引包含 `docs.count`, `store.size` 等字段
  
- ✅ **Query DSL**: 成功执行 DSL 查询
  - 简单 match_all 查询
  - 带 `_source` 过滤的查询
  - 验证返回结果包含 `id`, `source_path` 字段

#### 3. Top-K 检索 (4 个测试)
- ✅ **文本检索**: 使用描述文本进行语义检索
  - 测试 instruction key: `urban_governance`
  - 验证返回 `top_k`, `conclusion`, `query_info`
  
- ✅ **图片检索**: 使用图片进行向量检索 **(已恢复)**
  - 测试图片: `/mnt/nas/streetview_meta/queries/247query/247query/00001.jpg`
  - 验证返回 `top_k`, `conclusion`
  - 验证 `image_provided=true`, `description_provided=false`
  
- ✅ **地理过滤**: 带地理距离过滤的检索
  - 测试坐标: 35.6595, 139.7004 (涩谷)
  - 半径: 1000 米
  - 验证 geo_filter 正确应用
  
- ✅ **分数阈值**: 带相似度分数阈值的检索
  - 测试阈值: `min-description-score=0.5`
  - 验证阈值设置生效

#### 4. 地理编码 (2 个测试)
- ✅ 固定坐标返回正确 (东京涩谷: 35.654008, 139.705398)
- ✅ 多种地址输入测试通过 (中文、英文、空字符串)

#### 5. 模型服务 (5 个测试)
- ✅ **健康检查**: 服务状态正常，模型已加载
- ✅ **模型列表**: 包含 `Qwen3-VL-Embedding-2B` 和 `ImAge4VPR`
- ✅ **文本嵌入**: 
  - 输出维度: 2048
  - 支持多个文本批量嵌入
  - 支持 instruction key
  
- ✅ **图片嵌入**:
  - 输出维度: 6144 (ImAge4VPR)
  - 支持 base64 编码传输
  
- ✅ **多种 instruction keys**:
  - `urban_governance`
  - `traffic_order`
  - `safety_hazard`

#### 6. 模块导入 (5 个测试)
- ✅ 所有脚本模块可正常导入
- ✅ `street_server.py` 所有函数可调用
- ✅ `geocoding.py` 常量正确

### ⏭️ 跳过的测试 (2 个)

#### 1. `test_es_query_dsl_stdin`
- **原因**: stdin 在 `conda run` 下不工作
- **影响**: 低，`--dsl` 参数方式工作正常
- **替代方案**: 使用 `--dsl` 参数传递 JSON 查询

#### 2. `test_retrieve_multimodal`
- **原因**: RRF (Reciprocal Rank Fusion) 需要商业 Elasticsearch 许可证
- **ES 版本**: 9.3.3 (Docker)
- **许可证类型**: **basic** (基础版)
- **错误信息**: `AuthorizationException(403, 'security_exception', 'current license is non-compliant for [Reciprocal Rank Fusion (RRF)]')`
- **影响**: 中，多模态融合检索不可用
- **解决方案**: 
  - 升级到 Elasticsearch Gold/Platinum/Enterprise 许可证
  - 或者使用其他融合策略 (如加权分数融合)

**详细说明**:

当前 Elasticsearch 使用的是 **basic** 许可证（免费版本），不支持 RRF 功能。

| 许可证类型 | RRF 支持 | 说明 |
|-----------|---------|------|
| **basic** (当前) | ❌ 不支持 | 免费版本，仅包含基本功能 |
| **trial** | ✅ 支持 | 30 天试用 |
| **gold** | ✅ 支持 | 商业许可证 |
| **platinum** | ✅ 支持 | 商业许可证 |
| **enterprise** | ✅ 支持 | 商业许可证 |

**验证方法**:
```bash
# 查看许可证信息
curl -u citybrain-street:123456 http://localhost:3128/_license

# 当前输出:
# "type": "basic"  ← 这就是 RRF 不可用的原因
```

**替代方案**:
1. 分别执行文本和图片检索，然后在应用层合并结果
2. 实现自定义加权融合算法
3. 升级许可证（需要联系 Elastic 购买）

## 测试数据

### 测试图片
```
/mnt/nas/streetview_meta/queries/247query/247query/00001.jpg
```

### 测试描述
```
日本东京涩谷十字路口的街景，画面中可以看到标志性的玻璃幕墙建筑——Q-FRONT大楼，
其底层设有星巴克和TSUTAYA书店。画面左侧是密集的广告牌和商店招牌，包括
"サロンパス"、"三千里薬品"等日文标识，展现了涩谷作为潮流文化中心的视觉冲击力。
街道上行人正在等待红绿灯，呈现出典型的都市节奏。这个十字路口被誉为"全世界最繁忙的十字路口"，
在高峰时段，每次绿灯亮起时，可有上千人同时从四面八方穿越马路，形成壮观的"人潮交响曲"。
它不仅是东京的地标，也是全球流行文化的重要取景地，曾出现在《迷失东京》《速度与激情3》等多部影视作品中
```

### 服务配置
- **Elasticsearch**: `http://localhost:3128` ✅
- **模型服务**: `http://localhost:3130` ✅
- **索引名**: `street` ✅

### Elasticsearch Mapping 策略

**当前配置 (已更新)**:
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
          "index": true,  ✅ 已修复
          "similarity": "cosine",
          "index_options": {
            "type": "bbq_hnsw",
            "m": 16,
            "ef_construction": 100,
            "rescore_vector": {
              "oversample": 3.0
            }
          }
        },
        "vector-Qwen3-VL-Embedding-2B_urban_governance": {
          "type": "dense_vector",
          "dims": 2048,
          "index": true,  ✅ 已修复
          "similarity": "cosine",
          "index_options": {
            "type": "bbq_hnsw",
            "m": 16,
            "ef_construction": 100,
            "rescore_vector": {
              "oversample": 3.0
            }
          }
        }
      }
    }
  }
}
```

**✅ 已修复的问题**:
- vector 字段的 `index=true`，支持 knn 搜索
- 图片向量检索现在可以正常工作
- 使用 BBQ HNSW 索引，支持高效的近似最近邻搜索

## 运行测试

### 运行所有可用测试
```bash
cd /home/anker/imiss-deer-flow/skills/custom/location-matcher/scripts
python -m unittest discover -s test -p "test_*.py" -v
```

### 运行特定测试类
```bash
# 测试脚本执行
python -m unittest test.test_scripts_execution.TestScriptExecution -v

# 测试 Elasticsearch 集成
python -m unittest test.test_scripts_execution.TestESGetMapping -v
python -m unittest test.test_scripts_execution.TestESListIndices -v
python -m unittest test.test_scripts_execution.TestESQueryDSL -v

# 测试检索功能
python -m unittest test.test_scripts_execution.TestESRetrieveTopK -v

# 测试地理编码
python -m unittest test.test_scripts_execution.TestGeocoding -v

# 测试模型服务
python -m unittest test.test_scripts_execution.TestStreetServer -v

# 测试导入
python -m unittest test.test_scripts_execution.TestScriptImports -v
```

## 修复的问题

### 1. es_retrieve_topk.py 缩进错误
- **问题**: `haversine_meters` 函数定义后缺少函数体
- **修复**: 添加了完整的 Haversine 距离计算实现
- **影响**: 修复前脚本无法执行，修复后可正常运行

### 2. ES Vector 索引 (已修复 ✅)
- **问题**: vector 字段 `index=false`，导致 knn 搜索不可用
- **修复**: 更新 ES mapping，设置 `index=true`
- **影响**: 
  - ✅ 图片向量检索现在可用
  - ✅ 文本向量检索继续正常
  - ⏭️ RRF 融合检索仍不可用 (许可证限制)

## 测试历史

### 2026-05-01 更新
- ✅ 修复 ES vector 字段 `index=true`
- ✅ 恢复 `test_retrieve_by_image` 测试
- ⏭️ `test_retrieve_multimodal` 因许可证限制继续跳过
- ✅ 更新测试验证 vector 维度和相似度配置

## 下一步建议

### 启用 RRF 功能 (可选)
要启用多模态 RRF 融合测试，需要:

1. **升级 Elasticsearch 许可证** 到 Gold/Platinum/Enterprise
2. **运行测试**: `python -m unittest test.test_scripts_execution.TestESRetrieveTopK.test_retrieve_multimodal -v`
3. **验证 RRF 融合效果**: 比较纯文本、纯图片、多模态三种检索的差异

### 替代方案
如果不想升级许可证，可以:
1. 在应用层实现自定义融合策略
2. 分别执行文本和图片检索，然后合并结果
3. 使用加权分数或其他融合算法
