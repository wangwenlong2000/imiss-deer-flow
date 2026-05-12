# Location Matcher Scripts - 测试指南

## 快速运行

### 运行所有测试
```bash
cd /home/anker/imiss-deer-flow/skills/custom/location-matcher/scripts
python -m unittest discover -s test -p "test_*.py" -v
```

### 查看测试结果摘要
```bash
# 运行测试并保存结果
python -m unittest discover -s test -p "test_*.py" -v 2>&1 | tee test_results.log

# 查看统计
grep -E "^(Ran|OK|FAILED)" test_results.log
```

## 测试覆盖范围

### ✅ 已实现 (28/30 测试通过)

1. **脚本执行验证** (6/6)
   - Conda 环境检查
   - 所有脚本 --help 命令

2. **Elasticsearch 集成** (7/8)
   - ✅ 获取索引 mapping (index=true 已修复)
   - ✅ 列出所有索引
   - ✅ DSL 查询执行
   - ✅ 验证 vector 维度和相似度
   - ⏭️ stdin 方式跳过 (conda run 不支持)

3. **Top-K 检索** (4/5)
   - ✅ 文本语义检索
   - ✅ 图片向量检索 **(已恢复)**
   - ✅ 地理距离过滤
   - ✅ 分数阈值
   - ⏭️ 多模态 RRF 融合 (ES 许可证限制)

4. **地理编码** (2/2)
   - ✅ 固定坐标返回
   - ✅ 多地址测试

5. **模型服务** (5/5)
   - ✅ 健康检查
   - ✅ 模型列表
   - ✅ 文本嵌入 (2048 维)
   - ✅ 图片嵌入 (6144 维)
   - ✅ 多种 instruction keys

6. **模块导入** (5/5)
   - ✅ 所有脚本可导入

## 服务配置

- **Elasticsearch**: `http://localhost:3128` ✅
- **模型服务**: `http://localhost:3130` ✅
- **Conda 环境**: `deerflow-street` ✅
- **测试索引**: `street` ✅
- **测试图片**: `/mnt/nas/streetview_meta/queries/247query/247query/00001.jpg` ✅

## ES Vector 字段状态

### ✅ 已修复
- `vector-ImAge4VPR`: `index=true` (4096 维, cosine)
- `vector-Qwen3-VL-Embedding-2B_urban_governance`: `index=true` (2048 维, cosine)

### 索引类型
- BBQ HNSW (近似最近邻)
- m=16, ef_construction=100
- oversample=3.0

### 功能状态
- ✅ 单模态向量检索 (文本/图片)
- ✅ 带地理过滤的检索
- ⏭️ RRF 多模态融合 (需要商业许可证)

## 已知问题

### 1. stdin 不支持
**现象**: `test_es_query_dsl_stdin` 跳过

**原因**: `conda run` 不转发 stdin

**影响**: 低，`--dsl` 参数方式工作正常

### 2. RRF 许可证限制
**现象**: `test_retrieve_multimodal` 跳过

**错误**: `AuthorizationException(403, 'security_exception', 'current license is non-compliant for [RRF]')`

**原因**: 
- ES 版本: 9.3.3 (Docker)
- 许可证: **basic** (免费版本)
- RRF 需要 Gold/Platinum/Enterprise 许可证

**影响**: 中，多模态融合检索不可用

**验证**:
```bash
curl -u citybrain-street:123456 http://localhost:3128/_license
# 输出: "type": "basic"
```

**解决方案**:
- 升级 ES 许可证 (商业)
- 或实现自定义融合策略 (推荐)

## 测试文件结构

```
test/
├── __init__.py                    # Python 包初始化
├── test_scripts_execution.py      # 主测试文件 (30 个测试)
├── README.md                      # 测试说明
├── TEST_SUMMARY.md               # 详细测试总结
└── QUICKSTART.md                 # 快速开始指南
```

## 输出示例

```
test_es_get_mapping_street_index ... ok
test_es_list_indices ... ok
test_retrieve_by_description ... ok
test_retrieve_by_image ... ok  ← 已恢复
test_health_check ... ok
test_embed_text ... ok
test_embed_image ... ok
...

Ran 30 tests in 46.486s
OK (skipped=2)
```

## 运行单个测试

```bash
# 测试图片检索
python -m unittest test.test_scripts_execution.TestESRetrieveTopK.test_retrieve_by_image -v

# 测试文本检索
python -m unittest test.test_scripts_execution.TestESRetrieveTopK.test_retrieve_by_description -v

# 测试地理过滤
python -m unittest test.test_scripts_execution.TestESRetrieveTopK.test_retrieve_with_geo_filter -v

# 测试 ES mapping
python -m unittest test.test_scripts_execution.TestESGetMapping.test_es_get_mapping_street_index -v
```
