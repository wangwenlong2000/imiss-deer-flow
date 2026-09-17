"""Collect every type-7 source annotation and run a reproducible rule-only regression."""

from __future__ import annotations

import hashlib
import argparse
import json
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend/packages/harness"))
from deerflow.compliance.detectors.delivered_rules.vendor.confidential import ConfidentialDetector  # noqa: E402
from deerflow.compliance.detectors.delivered_rules.configured_llm import (  # noqa: E402
    CONFIDENTIAL_PROMPT_VERSION, CONFIDENTIAL_TEMPERATURE,
)

SOURCE = ROOT / "合规检测/合规评测全部十类违规类型数据"
OUT = ROOT / "合规检测/类型7-confidential整改"
FILES = ("compliance_annotations_250.jsonl", "network_flow_compliance_annotations_150.jsonl", "remote_sensing_all_120.jsonl")
RULE = ROOT / "backend/packages/harness/deerflow/compliance/detectors/delivered_rules/vendor/confidential.py"

# Fictional examples; the original annotations remain untouched.
ADDED = [
    ("statistical_yearbook", True, "ordinary_internal", "manual_review", "年鉴编制底稿：内部口径的下年度税收预测为 82.4 亿元，尚未公开。"),
    ("statistical_yearbook", True, "business_secret", "manual_review", "采购评审附件为商业秘密：客户名单、分层报价策略及谈判底价仍在竞标期。"),
    ("statistical_yearbook", True, "state_or_work_secret", "manual_review", "密级：国家秘密·秘密级；统计调查专项底稿仅限涉密人员按需知悉。"),
    ("statistical_yearbook", True, "state_or_work_secret", "manual_review", "工作秘密调查底稿包含未公开预测和内部口径，未经批准不得外发。"),
    ("policy_regulation", True, "ordinary_internal", "manual_review", "政策征求意见内部资料附未公开预测及部门内部口径，限起草组知悉。"),
    ("policy_regulation", True, "business_secret", "manual_review", "政策采购附件记录某企业核心商业秘密：客户名单及报价策略，禁止外发。"),
    ("policy_regulation", True, "state_or_work_secret", "manual_review", "专项政策材料标注机密级，保密期限三年，仅限授权人员查阅。"),
    ("network_flow", True, "ordinary_internal", "manual_review", "HTTPS 正文传输内部资料：未公开预测表及内部口径的季度收入测算。"),
    ("network_flow", True, "business_secret", "manual_review", "外发流量载荷含商业秘密：客户名单、报价策略及底线折扣。"),
    ("network_flow", True, "state_or_work_secret", "manual_review", "SMTP 附件标注国家秘密·秘密级，仅限涉密授权人员按需知悉。"),
    ("statistical_yearbook", False, "public", "allow", "公开年报只列已公开发布的全市营业收入汇总数据。"),
    ("statistical_yearbook", False, "public", "allow", "统计年鉴公开汇总表按区县展示总量，不含供应商报价或客户名单。"),
    ("policy_regulation", False, "public", "allow", "公开政策材料说明办事流程，正文不包含未公开预测或内部口径。"),
    ("policy_regulation", False, "public", "allow", "公开征求意见说明使用已公开月报汇总数据，标题含内部讨论字样。"),
    ("policy_regulation", False, "public", "allow", "公开保密教育培训材料解释国家秘密的密级，不载具体涉密文件。"),
    ("network_flow", False, "public", "allow", "HTTP 下载公开年报，响应是已公开汇总数据。"),
    ("network_flow", False, "public", "allow", "DNS 查询公开政策网站，路径含 internal 字符串但无内部资料。"),
    ("network_flow", False, "public", "allow", "HTTPS 响应是公开客户服务指南模板，不含真实客户名单和报价策略。"),
    ("policy_regulation", False, "ordinary_internal", "manual_review", "普通内部会议通知，仅列会议时间与地点；是否限制外发需按甲方制度人工复核。"),
    ("statistical_yearbook", False, "ordinary_internal", "manual_review", "未定稿的年鉴排版页只有已公开汇总数，是否限制流转需人工复核。"),
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def flatten(value: object, prefix: str = "") -> dict[str, str]:
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            result.update(flatten(item, f"{prefix}.{key}" if prefix else key))
        return result
    if isinstance(value, list):
        result = {}
        for index, item in enumerate(value):
            result.update(flatten(item, f"{prefix}[{index}]"))
        return result
    return {prefix: str(value)} if value is not None and prefix else {}


def score(rows: list[dict]) -> dict:
    count = Counter((bool(r["expected"]), bool(r["predicted"])) for r in rows)
    tp, fp, fn, tn = count[(True, True)], count[(False, True)], count[(True, False)], count[(False, False)]
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {"total": len(rows), "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "precision": round(precision, 6), "recall": round(recall, 6),
            "f1": round(2 * precision * recall / (precision + recall), 6) if precision + recall else 0.0,
            "fp_ids": [r["sample_id"] for r in rows if not r["expected"] and r["predicted"]],
            "fn_ids": [r["sample_id"] for r in rows if r["expected"] and not r["predicted"]]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evaluate", choices=("dev", "test"), help="Only run the named frozen partition")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    corpus = []
    sources = {}
    for name in FILES:
        path = SOURCE / name
        original = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        selected = [r for r in original if r.get("target_violation_type") == "confidential" or r.get("final_violation_type") == "confidential"]
        sources[name] = {"source_rows": len(original), "selected_rows": len(selected), "sha256": digest(path)}
        corpus.extend({**r, "regression_origin": name} for r in selected)
    for index, (kind, expected, category, disposition, content) in enumerate(ADDED, 1):
        sid = f"confidential-repair-{index:03d}"
        corpus.append({"data_id": sid, "sample_id": sid, "data_type": kind,
                       "target_violation_type": "confidential", "final_violation_type": "confidential" if expected else None,
                       "trigger_gate": "ContextGate", "is_positive": expected,
                       "sample_kind": "positive" if expected else "negative", "content": content,
                       "confidential_category": category, "policy_disposition": disposition,
                       "annotation_meta": {"rule_version": "confidential-repair-20260917", "review_status": "self_checked"},
                       "data_source": {"type": "manual_synthetic", "contains_real_sensitive_data": False},
                       "regression_origin": "added_synthetic"})
    ids = [r["sample_id"] for r in corpus]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate sample_id")
    # Stratify by origin and label, then use a stable hash for membership.
    groups = defaultdict(list)
    for row in corpus:
        groups[(row["regression_origin"], bool(row["is_positive"]))].append(row)
    desired = len(corpus) // 5
    quotas = {key: len(rows) // 5 for key, rows in groups.items()}
    remainder = desired - sum(quotas.values())
    for key in sorted(groups, key=lambda k: (-(len(groups[k]) / 5 - quotas[k]), k))[:remainder]:
        quotas[key] += 1
    dev_ids = set()
    for key, rows in groups.items():
        ordered = sorted(rows, key=lambda r: hashlib.sha256(("confidential-type7-v1:" + r["sample_id"]).encode()).hexdigest())
        dev_ids.update(r["sample_id"] for r in ordered[:quotas[key]])
    for row in corpus:
        row["evaluation_split"] = "dev" if row["sample_id"] in dev_ids else "test"
    dataset = OUT / "confidential_type7_regression.jsonl"
    dataset.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in corpus), encoding="utf-8")
    if not args.evaluate:
        print(json.dumps({"total": len(corpus), "dev": len(dev_ids), "test": len(corpus)-len(dev_ids)}))
        return
    detector = ConfidentialDetector(use_llm=False)
    predictions = []
    for row in (r for r in corpus if r["evaluation_split"] == args.evaluate):
        features = flatten(row["content"])
        content_text = row["content"] if isinstance(row["content"], str) else "\n".join(features.values())
        sample = {"data_id": row["data_id"], "sample_id": row["sample_id"], "data_type": row["data_type"],
                  "trigger_gate": row["trigger_gate"], "sample_kind": row["sample_kind"],
                  "content_text": content_text, "features": features}
        result = detector.detect(sample)
        predictions.append({"sample_id": row["sample_id"], "origin": row["regression_origin"],
                            "data_type": row["data_type"], "expected": bool(row["is_positive"]),
                            "predicted": result["is_hit"], "category": row.get("confidential_category"),
                            "matched_rules": result["matched_rules"], "suggested_action": result["suggested_action"]})
    pred_file = OUT / ("dev_predictions.jsonl" if args.evaluate == "dev" else "latest_predictions.jsonl")
    pred_file.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in predictions), encoding="utf-8")
    grouped = defaultdict(list)
    for row in predictions:
        grouped[row["origin"]].append(row)
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        commit = None
    metrics = {"dataset": str(dataset.relative_to(ROOT)), "dataset_sha256": digest(dataset),
               "split": args.evaluate, "split_method": "SHA-256 sample_id seed confidential-type7-v1, stratified by source and label, 20/80",
               "sources": sources, "detector_rule_sha256": digest(RULE), "git_head": commit,
               "evaluation_mode": "rule_only; llm_disabled; 80% was explored before split request and is not blind acceptance" if args.evaluate == "test" else "rule_only; llm_disabled; 20% rule development",
               "llm_review": {"enabled": False, "model": None, "model_version": None,
                              "prompt_version": CONFIDENTIAL_PROMPT_VERSION,
                              "temperature": CONFIDENTIAL_TEMPERATURE},
               "overall": score(predictions), "by_origin": {k: score(v) for k, v in grouped.items()}}
    metrics_file = OUT / ("dev_metrics.json" if args.evaluate == "dev" else "latest_metrics.json")
    metrics_file.write_text(json.dumps(metrics, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"overall": metrics["overall"], "by_origin": metrics["by_origin"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
