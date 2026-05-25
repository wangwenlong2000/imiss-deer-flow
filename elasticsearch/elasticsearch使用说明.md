# ELASTICSEARCH 使用说明

目前96服务器上已部署了 Elasticsearch 9.3.3，容器名为 `elasticsearch-street`，并已创建了用户名为 `citybrain-street` 的账号，密码为 `123456`，权限为 `superuser`。可以通过 `http://localhost:3128` 访问 Elasticsearch 服务，使用 Basic Auth 进行安全认证。

## 基础信息

| 项目 | 值 |
|---|---|
| 镜像 | `docker.elastic.co/elasticsearch/elasticsearch:9.3.3` |
| 容器名 | `elasticsearch-street` |
| HTTP 端口 | `3128` |
| Transport 端口 | `3129` |
| 访问地址 | `http://localhost:3128` |
| 安全认证 | 已开启（Basic Auth） |
| 自动重启 | `always` |

## 账号信息

| 用户名 | 密码 | 说明 |
|---|---|---|
| `citybrain-street` | `123456` | 已创建，权限为 `superuser` |


## 数据导入
导入数据用jsonl格式，单行一个json对象，示例：(具体字段根据实际需求调整)

```json
{"id": "1", "title": "文档标题1", "content": "文档内容1","vector-embedding_model_name": [0.1, 0.2, 0.3],"source_path": "/path/to/source/file1","metadata": {"author": "张三", "publish_date": "2024-01-01"}}
{"id": "2", "title": "文档标题2", "content": "文档内容2","vector-embedding_model_name": [0.4, 0.5, 0.6],"source_path": "/path/to/source/file2","metadata": {"author": "李四", "publish_date": "2024-02-01"}}
```

然后让AI帮忙写一个Python脚本，导入数据,下面是提示词

```text
我在这台服务器上部署了elasticsearch 数据库，基础信息如下，你帮我写一个Python脚本， 在elasticsearch数据库中 创建一个新的索引，命名为<你的数据集名>，在新的索引中将<xxx.jsonl>导入，向量字段需要建立索引进行向量搜索，文本字段建立text类型索引

## 基础信息

| 项目 | 值 |
|---|---|
| 镜像 | `docker.elastic.co/elasticsearch/elasticsearch:9.3.3` |
| 容器名 | `elasticsearch-street` |
| HTTP 端口 | `3128` |
| Transport 端口 | `3129` |
| 访问地址 | `http://localhost:3128` |
| 安全认证 | 已开启（Basic Auth） |
| 自动重启 | `always` |

## 账号信息

| 用户名 | 密码 | 说明 |
|---|---|---|
| `citybrain-street` | `123456` | 已创建，权限为 `superuser` |
```



## agent 访问数据库

### deerfolw 配置文件：
将这两个文件中的内容复制到自己的项目的配置文件中，确保正确设置环境变量和配置项：
[text](config.yaml) 
[text](.env)


### 暂时使用skill+python脚本访问数据库

location-matcher 路径下是我目前在用的访问 Elasticsearch 的 一个skill,其中有几个python脚本，包含了获取 mapping、列出索引、执行查询 DSL、检索 top-k 等功能，可以参考这些脚本来编写你自己的访问代码和skill：
[text](location-matcher/SKILL.md)
[text](location-matcher/scripts/es_get_mapping.py) 
[text](location-matcher/scripts/es_list_indices.py) 
[text](location-matcher/scripts/es_query_dsl.py) 
[text](location-matcher/scripts/es_retrieve_topk.py)

#### python环境管理
pyproject.toml 文件中已经包含了访问 Elasticsearch 所需的依赖库，如 `elasticsearch` 和 `requests`

## 人工常用访问示例

```bash
# 集群健康状态
curl -u citybrain-street:123456 http://localhost:3128/_cluster/health?pretty

# 查看所有索引（文档数、存储大小等）
curl -u citybrain-street:123456 http://localhost:3128/_cat/indices?v

# 查看索引 street 的 mapping
curl -u citybrain-street:123456 http://localhost:3128/street/_mapping?pretty

# 查看索引 street 的文档总数
curl -u citybrain-street:123456 http://localhost:3128/street/_count?pretty

# 查看索引 street 的前 3 条文档（不返回向量字段）
curl -u citybrain-street:123456 \
  "http://localhost:3128/street/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{"size":3,"_source":{"excludes":["vector-ImAge4VPR","vector-Qwen3-VL-Embedding-2B_urban_governance"]}}'

# 按 id 精确查询单条文档（不返回向量字段）
curl -u citybrain-street:123456 \
  "http://localhost:3128/street/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"id":"<填入文档id>"}},"_source":{"excludes":["vector-ImAge4VPR","vector-Qwen3-VL-Embedding-2B_urban_governance"]}}'

# 刷新索引（让刚写入的数据立即可搜索）
curl -u citybrain-street:123456 -X POST http://localhost:3128/street/_refresh

# 删除索引 street（不可恢复，谨慎操作）
curl -u citybrain-street:123456 -X DELETE http://localhost:3128/street
```

## 容器管理命令

### 启动容器

```bash
docker run -d \
  --name elasticsearch-street \
  --restart always \
  -p 3128:9200 \
  -p 3129:9300 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=true" \
  -e "ELASTIC_PASSWORD=123456" \
  -v esdata-street:/usr/share/elasticsearch/data \
  docker.elastic.co/elasticsearch/elasticsearch:9.3.3
```

### 停止容器

```bash
docker stop elasticsearch-street
```

### 重启容器

```bash
docker restart elasticsearch-street
```

### 删除容器

```bash
docker rm -f elasticsearch-street
```

### 查看容器状态

```bash
docker ps -f name=elasticsearch-street
```

### 查看容器日志

```bash
docker logs elasticsearch-street
```