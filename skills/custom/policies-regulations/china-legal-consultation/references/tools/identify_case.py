#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
案件类型粗识别脚本

用途：
- 接收一段用户案情描述
- 调用 references/tools/case_identifier.py
- 输出标准 JSON 结果，供 skill 工作流使用

用法：
    python3 scripts/identify_case.py "我借给朋友10万元，他一直不还"
    python3 scripts/identify_case.py "公司法定代表人未经决议对外担保是否有效" --top-k 5

输出示例：
{
  "ok": true,
  "query": "我借给朋友10万元，他一直不还",
  "result": {
    "case_type": "民间借贷",
    "case_id": 7,
    "confidence": 0.9,
    "method": "keyword_matching",
    "matched_keywords": ["借贷", "借"],
    "alternatives": []
  }
}
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="对自然语言案情描述做案件类型粗识别，并输出 JSON 结果。"
    )
    parser.add_argument(
        "query",
        type=str,
        help="用户案情描述文本，建议用引号包裹。"
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=3,
        help="返回备选案件类型数量上限，默认 3。"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="",
        help="可选：显式指定 case_types.db 路径。默认使用 references/data/case_types.db。"
    )
    return parser


def ensure_paths() -> Dict[str, Path]:
    """
    推导 skill 根目录、tools 目录、data 目录，并将 tools 目录加入 sys.path。
    """
    script_path = Path(__file__).resolve()
    skill_root = script_path.parent.parent
    tools_dir = skill_root / "references" / "tools"
    data_dir = skill_root / "references" / "data"

    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))

    return {
        "skill_root": skill_root,
        "tools_dir": tools_dir,
        "data_dir": data_dir,
    }


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    paths = ensure_paths()
    tools_dir = paths["tools_dir"]
    data_dir = paths["data_dir"]

    try:
        from case_identifier import CaseIdentifier  # type: ignore
    except Exception as exc:
        error = {
            "ok": False,
            "error": "IMPORT_ERROR",
            "message": "无法导入 case_identifier.py，请检查 references/tools 目录结构。",
            "details": str(exc),
            "expected_tools_dir": str(tools_dir),
        }
        print(json.dumps(error, ensure_ascii=False, indent=2))
        return 1

    db_path = Path(args.db_path).resolve() if args.db_path else (data_dir / "case_types.db")

    if not db_path.exists():
        error = {
            "ok": False,
            "error": "DB_NOT_FOUND",
            "message": "未找到 case_types.db，无法执行数据库驱动的案件类型识别。",
            "details": "请检查 references/data/case_types.db 是否存在；若数据库不可用，可退回参考 case_types_list.json 做人工粗分流。",
            "expected_db_path": str(db_path),
            "fallback_json": str(data_dir / "case_types_list.json"),
        }
        print(json.dumps(error, ensure_ascii=False, indent=2))
        return 2

    try:
        identifier = CaseIdentifier(db_path=str(db_path))
        result: Dict[str, Any] = identifier.identify(args.query, top_k=args.top_k)

        output = {
            "ok": True,
            "query": args.query,
            "result": result,
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    except Exception as exc:
        error = {
            "ok": False,
            "error": "IDENTIFY_FAILED",
            "message": "案件类型识别执行失败。",
            "details": str(exc),
            "db_path": str(db_path),
        }
        print(json.dumps(error, ensure_ascii=False, indent=2))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())