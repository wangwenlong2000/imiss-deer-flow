视频已成功登记入库并建立语义向量索引。以下是完整的处理报告：

---

## 📋 资产编号: 3B3D5A43

**标题:** 交通监控视频 - Trafic-30s  
**描述:** 城市道路监控录像，包含公交车和小轿车通行的交通场景

### 📦 文件信息
| 项目 | 值 |
|------|-----|
| 文件名 | Trafic-30s.mp4 |
| 大小 | 1,497.4 KB |
| MD5 | da38e2d29b12a5da09beda1eaed2c895 |
| 分辨率 | 1280×674 |
| 帧率 | 12 fps |
| 时长 | 30 秒 |
| 编码 | H.264 |

### 🔍 对象检测 (YOLOv8n)
- **公交车**: 5 辆
- **小轿车**: 26 辆
- **总检测**: 31 次（覆盖 3 帧）

### 🧠 语义向量化配置
- **关键帧提取**: 10 帧（每 3 秒一帧）
- **Embedding 模型**: CLIP ViT-B/32
- **特征维度**: 512 维
- **索引类型**: IVF3 + FlatL2
- **距离度量**: L2（欧几里得距离）
- **验证检索**: 自搜索召回正确（第 1 名距离=0.0）

### 🏷️ 语义标签
`traffic`, `road`, `vehicles`, `bus`, `car`, `surveillance`, `urban`

---

### 💾 输出文件清单

| 文件 | 路径 |
|------|------|
| 入库登记记录 | `/mnt/user-data/outputs/video_registration.json` |
| 索引配置清单 | `/mnt/user-data/outputs/index_configuration.json` |
| FAISS 向量索引 | `/mnt/user-data/workspace/video-index/faiss_index.index` |
| 视觉特征数据 | `/mnt/user-data/workspace/video-index/features.npy` |
| 元数据 | `/mnt/user-data/workspace/video-index/metadata.json` |
| 关键帧图片 | `/mnt/user-data/workspace/video-index/keyframes/` |

---

所有流程已完成：**视频解析 → 关键帧抽取 → CLIP 视觉特征提取 → FAISS 向量索引构建 → 入库登记**。该视频现已支持基于语义的相似度检索，可通过输入自然语言描述（如"有公交车的道路场景"）来匹配相关帧。