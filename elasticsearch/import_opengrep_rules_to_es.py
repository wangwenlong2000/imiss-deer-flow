#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import yaml
from elasticsearch import Elasticsearch, helpers


def build_mapping():
    return {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "rule_id": {"type": "keyword"},
                "source_path": {"type": "keyword"},
                "ruleset": {"type": "keyword"},
                "languages": {"type": "keyword"},
                "severity": {"type": "keyword"},
                "message": {"type": "text"},
                "rule_text": {"type": "text"},
                "metadata": {"type": "object", "enabled": False},
                "raw_rule": {"type": "object", "enabled": False},
            }
        }
    }


def stable_id(source_path, rule_id):
    return hashlib.sha1(f"{source_path}:{rule_id}".encode("utf-8")).hexdigest()


def load_docs(rules_dir: Path):
    docs = []
    yaml_files = sorted(list(rules_dir.rglob("*.yaml")) + list(rules_dir.rglob("*.yml")))

    for path in yaml_files:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        rules = data.get("rules") if isinstance(data, dict) else None
        if not isinstance(rules, list):
            continue

        source_path = str(path.relative_to(rules_dir)).replace("\\", "/")
        top = source_path.split("/", 1)[0]
        ruleset = top if top else "unknown"

        for i, rule in enumerate(rules):
            if not isinstance(rule, dict):
                continue

            rule_id = str(rule.get("id") or f"{path.stem}-{i}")
            metadata = rule.get("metadata") if isinstance(rule.get("metadata"), dict) else {}
            languages = rule.get("languages") or []
            if not isinstance(languages, list):
                languages = [str(languages)]

            rule_text = yaml.safe_dump(rule, allow_unicode=True, sort_keys=False)

            docs.append({
                "id": stable_id(source_path, rule_id),
                "rule_id": rule_id,
                "source_path": source_path,
                "ruleset": ruleset,
                "languages": [str(x) for x in languages],
                "severity": str(rule.get("severity") or "").upper(),
                "message": str(rule.get("message") or ""),
                "rule_text": rule_text,
                "metadata": metadata,
                "raw_rule": rule,
            })

    return docs, len(yaml_files)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--es-url", required=True)
    parser.add_argument("--es-username", default="citybrain-street")
    parser.add_argument("--es-password", default="123456")
    parser.add_argument("--index", default="opengrep_rules")
    parser.add_argument("--rules-dir", default="skills/public/opengrep-compliance/rules")
    parser.add_argument("--recreate-index", action="store_true")
    args = parser.parse_args()

    rules_dir = Path(args.rules_dir).resolve()
    es = Elasticsearch(args.es_url, basic_auth=(args.es_username, args.es_password), request_timeout=120)

    if args.recreate_index and es.indices.exists(index=args.index):
        es.indices.delete(index=args.index)

    if not es.indices.exists(index=args.index):
        es.indices.create(index=args.index, body=build_mapping())

    docs, yaml_count = load_docs(rules_dir)

    actions = [
        {
            "_op_type": "index",
            "_index": args.index,
            "_id": doc["id"],
            "_source": doc,
        }
        for doc in docs
    ]

    if actions:
        helpers.bulk(es, actions, chunk_size=500)

    es.indices.refresh(index=args.index)

    print(json.dumps({
        "index": args.index,
        "rules_dir": str(rules_dir),
        "yaml_files_scanned": yaml_count,
        "rules_imported": len(actions),
        "documents_now_in_index": es.count(index=args.index)["count"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()