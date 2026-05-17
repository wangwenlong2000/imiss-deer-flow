#!/usr/bin/env python3
"""
CityBench RAG skill 三档可用性自检。
快速验证 demo / local / es 三个模式哪些可用，输出 JSON 报告。

用法:
    python3 scripts/check_status.py
    python3 scripts/check_status.py --local-file path/to/evidence.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
SKILL_ROOT = SCRIPT_DIR.parent
DEMO_EVIDENCE = SKILL_ROOT / "data" / "sample_evidence.jsonl"


def check_demo() -> dict:
    if not DEMO_EVIDENCE.exists():
        return {"available": False, "error": f"missing {DEMO_EVIDENCE}"}
    n = 0
    cities = {}
    anomalies = 0
    with open(DEMO_EVIDENCE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                n += 1
                c = rec.get("meta", {}).get("geo_scope", {}).get("city", "?")
                cities[c] = cities.get(c, 0) + 1
                if rec.get("meta", {}).get("features", {}).get("anomaly_flag"):
                    anomalies += 1
            except json.JSONDecodeError:
                continue
    return {
        "available": True,
        "evidence_path": str(DEMO_EVIDENCE),
        "evidence_count": n,
        "cities": cities,
        "anomaly_count": anomalies,
    }


def check_local(path: str | None) -> dict:
    if not path:
        return {"available": False, "reason": "no --local-file given"}
    p = Path(path)
    if not p.exists():
        return {"available": False, "reason": f"file not found: {path}"}
    n = sum(1 for line in open(p, "r", encoding="utf-8") if line.strip())
    return {"available": True, "evidence_path": str(p), "evidence_count": n}


def check_es() -> dict:
    es_host = os.getenv("ES_HOST", "http://localhost:9200")
    es_index = os.getenv("ES_INDEX_NAME", "citybench_evidence")
    api_key = os.getenv("DASHSCOPE_API_KEY", "")

    rep: dict = {
        "available": False,
        "es_host": es_host,
        "index_name": es_index,
        "dashscope_key_present": bool(api_key),
    }

    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        rep["reason"] = "elasticsearch package not installed"
        return rep
    if not api_key:
        rep["reason"] = "DASHSCOPE_API_KEY env var not set"
        return rep

    try:
        es = Elasticsearch(es_host, request_timeout=5, max_retries=1)
        if not es.ping():
            rep["reason"] = f"cannot ping {es_host}"
            return rep
        rep["es_reachable"] = True
        if not es.indices.exists(index=es_index):
            rep["reason"] = f"index {es_index} does not exist"
            return rep
        count = es.count(index=es_index)["count"]
        stats = es.indices.stats(index=es_index)
        size = stats["_all"]["primaries"]["store"]["size_in_bytes"]
        rep.update({
            "available": count > 0,
            "document_count": count,
            "store_size_mb": round(size / (1024 * 1024), 2),
        })
        if count == 0:
            rep["reason"] = "index exists but has 0 documents"
    except Exception as e:
        rep["reason"] = f"es error: {e}"
    return rep


def main() -> int:
    parser = argparse.ArgumentParser(description="Check CityBench RAG skill mode availability")
    parser.add_argument("--local-file", help="Path to a local evidence.jsonl to test local mode")
    args = parser.parse_args()

    report = {
        "demo_mode": check_demo(),
        "local_mode": check_local(args.local_file),
        "es_mode": check_es(),
    }
    # 自动决定脚本默认会走哪一档
    if report["es_mode"].get("available"):
        report["effective_mode"] = "es"
    elif report["local_mode"].get("available"):
        report["effective_mode"] = "local"
    elif report["demo_mode"].get("available"):
        report["effective_mode"] = "demo"
    else:
        report["effective_mode"] = "NONE"

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["effective_mode"] != "NONE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
