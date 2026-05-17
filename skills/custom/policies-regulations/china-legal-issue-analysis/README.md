# china-legal-issue-analysis

本目录由上游仓库 `CSlawyer1985/china-lawyer-analyst` 拆分整理而来，保留“法律问题分析”主线。

保留来源：
- 原 `SKILL.md` 中的案件类型识别、六段式要件清单、10步法 + IRAC、法律校验、反思修正
- 原 `README.md` 中“使用方式一：法律问题分析”
- 原 `core/`、`domains/`、`shared/`、`tools/`、`interpretations/` 的相关模块

提示：
- 上游二进制数据库 `data/case_types.db` 未能随拆分包一并携带。
- 已保留 `data/case_types_list.json` 作为案件类型清单参考。
- 若需要真正运行 `case_identifier.py` / `checklist_generator.py`，请从原仓库 `data/` 目录补入 `case_types.db`。
