#!/usr/bin/env python3
"""
Environment check script for PolicyRAG Skill.

Usage:
    python check_env.py
"""

import os
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent

DEFAULT_DATA_ROOT = Path(os.getenv("RAG_DATA_DIR", "/mnt/datasets/policies-regulations/policy-rag"))
DEFAULT_WORKSPACE_ROOT = Path(os.getenv("RAG_WORKSPACE_DIR", "/mnt/user-data/workspace/policy-rag"))
DEFAULT_FLOWS_DIR = Path(os.getenv("RAG_FLOWS_DIR", str(DEFAULT_DATA_ROOT / "flows")))
DEFAULT_EMBED_CACHE_DIR = Path(
    os.getenv("RAG_EMBED_CACHE_DIR", str(DEFAULT_WORKSPACE_ROOT / "cache" / "embed_cache"))
)
DEFAULT_INDEX_CACHE_DIR = Path(
    os.getenv("RAG_INDEX_CACHE_DIR", str(DEFAULT_WORKSPACE_ROOT / "cache" / "index"))
)
DASHSCOPE_EMBEDDING_MODEL = os.getenv("DASHSCOPE_EMBEDDING_MODEL", "text-embedding-v3")


def check_dashscope() -> bool:
    print("\n🔍 Checking DashScope embedding service...")
    from rag_system import DashScopeEmbeddingClient, load_env_value

    api_key = load_env_value("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        print("  ❌ DASHSCOPE_API_KEY is missing")
        print("     Please set it in scripts/.env, skill-root .env, or export it in the environment")
        return False

    try:
        DEFAULT_EMBED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        DEFAULT_INDEX_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        client = DashScopeEmbeddingClient()
        ok = client.healthcheck()
        if ok:
            print(f"  ✅ DashScope is reachable with model: {DASHSCOPE_EMBEDDING_MODEL}")
            print(f"     Embed cache dir: {DEFAULT_EMBED_CACHE_DIR}")
            print(f"     Index cache dir: {DEFAULT_INDEX_CACHE_DIR}")
            return True
        print("  ❌ DashScope healthcheck failed")
        return False
    except Exception as exc:
        print(f"  ❌ DashScope check failed: {exc}")
        return False


def check_python_deps() -> bool:
    print("\n🔍 Checking Python dependencies...")
    required = ["numpy", "dashscope"]
    optional = ["pandas", "xlrd", "openpyxl"]
    ok = True
    for pkg in required:
        try:
            __import__(pkg)
            print(f"  ✅ {pkg}")
        except ImportError:
            print(f"  ❌ {pkg} is missing")
            ok = False
    for pkg in optional:
        try:
            __import__(pkg)
            print(f"  ✅ {pkg} (optional)")
        except ImportError:
            print(f"  ⚠️  {pkg} is missing (optional)")
    if not ok:
        print("\n  Install dependencies with:")
        print("  python3 -m pip install numpy dashscope pandas xlrd openpyxl")
    return ok


def check_skill_files() -> bool:
    print("\n🔍 Checking skill files...")
    required_files = [
        "SKILL.md",
        "scripts/rag_system.py",
        "scripts/search_flows.py",
        "scripts/split_doc.py",
        "scripts/convert_excel.py",
        "scripts/index_manager.py",
    ]
    ok = True
    for rel in required_files:
        p = SKILL_ROOT / rel
        if p.exists():
            print(f"  ✅ {rel}")
        else:
            print(f"  ❌ {rel} not found")
            ok = False
    return ok


def check_data_files() -> bool:
    print("\n🔍 Checking flow data...")
    flows_dir = DEFAULT_FLOWS_DIR
    if not flows_dir.exists():
        print(f"  ⚠️  Flow directory not found: {flows_dir}")
        print("     Set RAG_FLOWS_DIR or create /mnt/datasets/policies-regulations/policy-rag/flows")
        return False
    json_files = list(flows_dir.glob("*_flows.json")) + list(flows_dir.glob("*_v2.json"))
    if not json_files:
        print(f"  ⚠️  No flow JSON files found in: {flows_dir}")
        return False
    print(f"  ✅ Found {len(json_files)} flow files in {flows_dir}")
    for f in json_files[:8]:
        print(f"     - {f.name}")
    if len(json_files) > 8:
        print("     - ...")
    return True


def test_search() -> bool:
    print("\n🔍 Testing search pipeline...")
    try:
        sys.path.insert(0, str(SCRIPT_DIR))
        from rag_system import DashScopeEmbeddingClient, ApprovalFlowSearcher

        embedder = DashScopeEmbeddingClient()
        searcher = ApprovalFlowSearcher(embedder)
        searcher.load_flows(str(DEFAULT_FLOWS_DIR))
        if not searcher.flows:
            print("  ⚠️  No flows loaded, skip search test")
            return False
        results = searcher.search("采购申请", top_k=1)
        if results:
            sim, flow = results[0]
            print(f"  ✅ Search test passed (similarity: {sim:.2%})")
            print(f"     Top match: {flow.get('流程名称', 'N/A')}")
            return True
        print("  ⚠️  Search returned no result")
        return False
    except Exception as exc:
        print(f"  ❌ Search test failed: {exc}")
        return False


def main() -> int:
    print("=" * 60)
    print("PolicyRAG Skill - Environment Check")
    print("=" * 60)
    print(f"Skill root: {SKILL_ROOT}")
    print(f"Readonly data root: {DEFAULT_DATA_ROOT}")
    print(f"Default flows dir: {DEFAULT_FLOWS_DIR}")
    print(f"Workspace root: {DEFAULT_WORKSPACE_ROOT}")
    print(f"Embed cache dir: {DEFAULT_EMBED_CACHE_DIR}")
    print(f"Index cache dir: {DEFAULT_INDEX_CACHE_DIR}")

    checks = {
        "DashScope service": check_dashscope(),
        "Python dependencies": check_python_deps(),
        "Skill files": check_skill_files(),
        "Flow data": check_data_files(),
    }
    if all(checks.values()):
        checks["Search test"] = test_search()

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for name, ok in checks.items():
        print(f"  {'✅ PASS' if ok else '❌ FAIL'}: {name}")
    if all(checks.values()):
        print("\nAll checks passed.")
        return 0
    print("\nSome checks failed. Review the messages above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
