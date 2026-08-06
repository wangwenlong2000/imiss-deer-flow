#!/usr/bin/env python3
"""
文档智能分割脚本 - OpenClaw Subagent 调用入口

Usage:
    python split_doc.py <input_file> [output_dir] [department] [category]

Example:
    python split_doc.py "/path/to/采购管理制度.md" "./output" "供应链中心" "供应链-采购管理"
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rag_system import DashScopeEmbeddingClient, PROCESSED_DIR, SmartDocumentSplitter


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else str(PROCESSED_DIR)
    department = sys.argv[3] if len(sys.argv) > 3 else "供应链中心"
    category = sys.argv[4] if len(sys.argv) > 4 else "供应链-采购管理"

    embedder = DashScopeEmbeddingClient()
    if not embedder.healthcheck():
        print("❌ 无法调用 DashScope 文本向量服务，请检查 DASHSCOPE_API_KEY / 模型名 / 网络")
        sys.exit(1)
    print("✅ DashScope embedding 连接成功")

    doc_info = {
        "title": Path(input_file).stem,
        "department": department,
        "category": category,
        "tags": ["制度"],
        "effective_date": "2025-01-01",
    }

    splitter = SmartDocumentSplitter(embedder)
    files = splitter.split_document(input_file, output_dir, doc_info)
    print(f"\n✅ 分割完成！共生成 {len(files)} 个文件")
    print(f"📁 输出目录: {output_dir}")


if __name__ == "__main__":
    main()
