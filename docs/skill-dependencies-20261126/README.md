# Skill 依赖版本盘点证据

核查日期：2026-11-26（北京时间）。范围为本仓库的 119 个 Skill（业务 102 个、公共 17 个），共 338 条依赖记录。

验收阅读入口：

- [逐 Skill 盘点说明](../Skill依赖版本盘点_20261126.md)：统计方法、关键版本、差异与逐项清单。
- [Excel 清单](../skills/Skill依赖版本清单_20261126.xlsx)：可筛选的逐 Skill、逐依赖、差异和原始版本工作表。
- [项目环境说明](../project-materials/项目环境说明_外部验收_20261126.md)：部署、外部服务、存储及验收范围。

## 数据文件

| 文件 | 内容与边界 |
| --- | --- |
| `skills.json` | 每个 Skill 的依赖约束、必需/可选条件、环境、检出版本、脚本证据和备注；包含独立 StreetModel 服务的声明 |
| `dependencies.csv` | 展平的 338 条依赖记录，UTF-8 BOM；可选分支与环境范围需结合对应列阅读 |
| `runtime-sandbox.json` | 配置镜像的 Python、Node、工具与 OS 包元数据；Python 共 421 条记录，含同名包多层安装，规范化后 405 个包名 |
| `sandbox-installed-versions.txt` | 解释器选中的分发包版本列表；是盘点快照，不是已求解、验证过的可复现安装锁 |
| `runtime-backend.json` | 单独读取的专用图后端 Python 及相关框架版本；不能用作沙箱版本 |
| `runtime-mounted-skills.json` | 专用图容器挂载的 SKILL.md 摘要及微信工具本地 Node 包目录检查；119 份 SKILL.md 与本仓库一致，未比对所有脚本 |
| `runtime-skill-assets.json` | OpenGrep 二进制及规则目录检查；二进制存在但缺执行位 |
| `workbook-data.json` | Excel 工作表的结构化内容，包含差异表和原始版本 |

## 使用注意

已装包数据来自配置镜像 `gaozhuohui-deerflow-sandbox:policies-regulations-tools`，摘要为 `sha256:89847f269c6355162a02c0acf4bb54e5b5753c2c12608c514f7d88aba52da672`。采集使用临时只读、禁网容器，没有执行各 Skill 业务、安装依赖或修改现有服务。

包元数据存在不等于能成功导入，更不等于业务功能通过。同名包多层安装应读取 `selected_version`，不要直接采用最后一条记录。Node 包检查覆盖记录中的全局目录和微信 Skill 本地目录，并非对所有缓存目录的穷尽搜索。街景 CUDA 服务本轮仅取得声明，未取得其独立虚拟环境的实测包版本。
