#!/usr/bin/env python3
"""
审批流程快速检索脚本 - OpenClaw Subagent 调用入口
支持索引缓存，避免每次重新计算 embedding

Usage:
    python search_flows.py "查询内容"
    python search_flows.py "采购200万以上怎么审批" --force-update

Example:
    python search_flows.py "预付款白名单供应商申请"
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from index_manager import FlowIndexManager
from rag_system import DashScopeEmbeddingClient, ApprovalFlowSearcher, READONLY_FLOWS_DIR, log

FLOWS_DIR = Path(os.getenv("RAG_FLOWS_DIR", str(READONLY_FLOWS_DIR)))


def main() -> None:
    parser = argparse.ArgumentParser(description="审批流程检索")
    parser.add_argument("query", help="查询内容")
    parser.add_argument("--flows-dir", default=str(FLOWS_DIR), help="流程数据目录")
    parser.add_argument("--force-update", action="store_true", help="强制更新索引")
    parser.add_argument("--rebuild", action="store_true", help="重建索引")
    args = parser.parse_args()

    embedder = DashScopeEmbeddingClient()
    if not embedder.healthcheck():
        print("❌ 无法调用 DashScope 文本向量服务，请检查 DASHSCOPE_API_KEY / 模型名 / 网络")
        sys.exit(1)

    index_manager = FlowIndexManager(args.flows_dir, embedder)
    log("正在检查索引...")
    index_manager.build_or_update_index(force_rebuild=(args.rebuild or args.force_update))

    searcher = ApprovalFlowSearcher(embedder)
    searcher.flows = index_manager.flows
    searcher.flow_embeddings = index_manager.flow_embeddings
    searcher.level3_groups = index_manager.level3_groups

    print()
    print(searcher.answer(args.query))


if __name__ == "__main__":
    main()
