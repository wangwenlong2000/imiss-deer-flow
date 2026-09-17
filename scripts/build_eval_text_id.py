"""Build the type-6 regression corpus and evaluate the delivered rule offline."""

from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend/packages/harness"))

from deerflow.compliance.detectors.delivered_rules.vendor.text_id import TextIdDetector  # noqa: E402

SOURCE_DIR = ROOT / "合规检测/合规评测全部十类违规类型数据"
OUT_DIR = ROOT / "合规检测/类型6-text_id整改"
SOURCES = ("compliance_annotations_250.jsonl", "policy_regulation.jsonl")
SPLIT_SEED = "text-id-type6-20260917-v1"
RULE_PATH = ROOT / "backend/packages/harness/deerflow/compliance/detectors/delivered_rules/vendor/text_id.py"

# All names, organizations, and events below are fictional evaluation material.
POSITIVES = [
    ("policy_regulation", "政策执行答复附件记录：投诉人林知遥在青禾区住房保障中心申请公租房复核，联系电话13800138021。", "林知遥", "13800138021"),
    ("policy_regulation", "行政许可核查材料写明：经办人周予安任澄江市行政审批局窗口审核员，负责食品经营许可复核。", "周予安", "澄江市行政审批局"),
    ("policy_regulation", "政府信息公开答复草稿列出举报人陈若宁及其居住地址青禾小区12栋301室，并关联噪声投诉。", "陈若宁", "青禾小区12栋301室"),
    ("policy_regulation", "执法听证通知书中，申请人赵清禾为明川物业有限公司的项目负责人，参与物业收费争议听证。", "赵清禾", "明川物业有限公司"),
    ("policy_regulation", "政策咨询工单注明：联系人孙知远任东岭街道办服务窗口审核专员，负责低保申请材料补正。", "孙知远", "东岭街道办"),
    ("policy_regulation", "房屋征收补偿复议材料把原告何书宁与安澜区自然资源所权属调查员岗位、复议案号安复〔2026〕18号关联。", "何书宁", "安复〔2026〕18号"),
    ("policy_regulation", "市场监管抽检通报草稿写明被处罚人吴静宜，现住在翠竹花园6号楼2单元402室。", "吴静宜", "翠竹花园6号楼2单元402室"),
    ("policy_regulation", "群众来信转办摘要保留信访人郑文博和手机号13900138022，同时记录其供水纠纷。", "郑文博", "13900138022"),
    ("policy_regulation", "民政救助审核纪要：申请人冯雅琴在云河社区办理临时救助，材料附其住址晨曦家园3栋502室。", "冯雅琴", "晨曦家园3栋502室"),
    ("policy_regulation", "环境处罚决定书起草说明中列出责任人蒋舒宁及联系电话13700138023，关联污染投诉案件。", "蒋舒宁", "13700138023"),
    ("statistical_yearbook", "年鉴人口表校核备注：填报人叶清和任松泽区统计局调查员，负责住户抽样复核。", "叶清和", "松泽区统计局"),
    ("statistical_yearbook", "财政年鉴附录写明经办人韩思远在临川财政局预算科任审核专员，参与教育支出数据复核。", "韩思远", "临川财政局"),
    ("statistical_yearbook", "就业统计年鉴编制说明：联系人高沐言，电话13600138024，负责失业登记数据补正。", "高沐言", "13600138024"),
    ("statistical_yearbook", "卫生统计附表备注披露复核员唐予宁的居住地址清风小区8栋1单元201室。", "唐予宁", "清风小区8栋1单元201室"),
    ("statistical_yearbook", "能源年鉴误差说明将调查员宋可心与青禾区能源统计中心及企业能耗核查事项绑定。", "宋可心", "青禾区能源统计中心"),
    ("statistical_yearbook", "住房年鉴原始备注：项目负责人谢知行任澄江市住房保障中心档案员，参与公租房申请复核。", "谢知行", "澄江市住房保障中心"),
    ("statistical_yearbook", "交通年鉴调查附录记载投诉人董清越与手机号13500138025，涉及公交线路调整争议。", "董清越", "13500138025"),
    ("statistical_yearbook", "教育年鉴校核单上，审核员梁若溪住在锦园小区5栋703室，负责入学人数复核。", "梁若溪", "锦园小区5栋703室"),
    ("statistical_yearbook", "农业统计年鉴底稿写明联系人顾书瑶任安澜区农业服务站调查员，参与农户补贴抽检。", "顾书瑶", "安澜区农业服务站"),
    ("statistical_yearbook", "公共服务年鉴编制备注列出经办人许文澜的电话13400138026及养老服务审核事项。", "许文澜", "13400138026"),
]

NEGATIVES = [
    ("policy_regulation", "政策办理指南只写窗口审核员负责材料复核，未列出具体人员姓名。", "generic_position"),
    ("policy_regulation", "行政许可答复模板把单位写作某单位，岗位写作负责人，没有自然人姓名或联系方式。", "generic_organization"),
    ("policy_regulation", "投诉人姓名已匿名化为张某，联系电话和详细住址均已删除。", "anonymized_name"),
    ("policy_regulation", "年终政策通报称某单位工作人员负责调查，未出现具体承办人姓名。", "generic_organization"),
    ("policy_regulation", "信访案例仅称李某，单位与岗位均未披露，也无其他可定位信息。", "anonymized_name"),
    ("statistical_yearbook", "年鉴表格仅记载统计员岗位和汇总人数，未列明个人。", "generic_position"),
    ("statistical_yearbook", "财政年鉴备注写某单位负责预算审核，未出现具体人员信息。", "generic_organization"),
    ("statistical_yearbook", "卫生年鉴示例中的王某为匿名姓名，个人住址及联系方式均为空。", "anonymized_name"),
    ("statistical_yearbook", "就业年鉴附表只披露通用岗位调查员和地区汇总指标。", "generic_position"),
]


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def text_of(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        return "\n".join(text_of(value) for value in content.values())
    if isinstance(content, list):
        return "\n".join(text_of(value) for value in content)
    return str(content)


def scores(rows: list[dict]) -> dict:
    counts = Counter((row["expected"], row["predicted"]) for row in rows)
    tp, fp, fn, tn = (counts[(True, True)], counts[(False, True)], counts[(True, False)], counts[(False, False)])
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {
        "total": len(rows), "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": round(precision, 6), "recall": round(recall, 6),
        "f1": round(2 * precision * recall / (precision + recall), 6) if precision + recall else 0.0,
        "fp_ids": [row["sample_id"] for row in rows if not row["expected"] and row["predicted"]],
        "fn_ids": [row["sample_id"] for row in rows if row["expected"] and not row["predicted"]],
    }


def split_development_test(corpus: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """Allocate 20% for rule development by origin and label, deterministically."""
    strata: dict[tuple[str, bool], list[dict]] = defaultdict(list)
    for row in corpus:
        strata[(row["regression_origin"], bool(row["is_positive"]))].append(row)
    target = round(len(corpus) * 0.2)
    quotas = {key: len(rows) // 5 for key, rows in strata.items()}
    remaining = target - sum(quotas.values())
    ranked_strata = sorted(
        strata,
        key=lambda key: (-(len(strata[key]) / 5 - quotas[key]), key),
    )
    for key in ranked_strata[:remaining]:
        quotas[key] += 1

    development_ids = set()
    for key, rows in strata.items():
        ranked_rows = sorted(
            rows,
            key=lambda row: hashlib.sha256(
                f"{SPLIT_SEED}:{row['sample_id']}".encode("utf-8")
            ).hexdigest(),
        )
        development_ids.update(row["sample_id"] for row in ranked_rows[:quotas[key]])
    development = [row for row in corpus if row["sample_id"] in development_ids]
    test = [row for row in corpus if row["sample_id"] not in development_ids]
    if len(development) != target or len(test) != len(corpus) - target:
        raise AssertionError("Invalid development/test split")
    manifest = {
        "seed": SPLIT_SEED,
        "method": "SHA-256 rank within regression_origin × is_positive strata; largest-remainder quota",
        "target_development_fraction": 0.2,
        "actual_development_fraction": round(len(development) / len(corpus), 6),
        "development_count": len(development),
        "test_count": len(test),
        "development_ids": [row["sample_id"] for row in development],
        "test_ids": [row["sample_id"] for row in test],
        "stratum_counts": {
            f"{key[0]}:{'positive' if key[1] else 'negative'}": {
                "total": len(rows), "development": quotas[key], "test": len(rows) - quotas[key]
            }
            for key, rows in sorted(strata.items())
        },
    }
    return development, test, manifest


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    corpus: list[dict] = []
    source_counts: dict[str, int] = {}
    source_sha: dict[str, str] = {}
    for name in SOURCES:
        path = SOURCE_DIR / name
        original = read_jsonl(path)
        selected = [row for row in original if row.get("target_violation_type") == "text_id" or row.get("final_violation_type") == "text_id"]
        source_counts[name] = len(selected)
        source_sha[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        for row in selected:
            corpus.append({**row, "regression_origin": name})

    for i, (kind, content, person, linked) in enumerate(POSITIVES, 1):
        corpus.append({
            "data_id": f"text-id-repair-pos-{i:03d}", "sample_id": f"text-id-repair-pos-{i:03d}",
            "target_violation_type": "text_id", "final_violation_type": "text_id",
            "data_type": kind, "trigger_gate": "ContextGate", "is_positive": True,
            "sample_kind": "positive", "content": content,
            "risk_locations": [{"field": "content", "text": person, "entity_type": "person_name"},
                               {"field": "content", "text": linked, "entity_type": "linked_identifier"}],
            "judgement_reason": "虚构全名与机构/岗位/事件/地址/电话构成可识别个人的自然语言组合。",
            "data_source": {"type": "manual_synthetic", "contains_real_personal_data": False},
            "annotation_meta": {"rule_version": "text-id-repair-20260917", "review_status": "self_checked"},
            "regression_origin": "added_positive",
        })
    for i, (kind, content, boundary) in enumerate(NEGATIVES, 1):
        corpus.append({
            "data_id": f"text-id-repair-neg-{i:03d}", "sample_id": f"text-id-repair-neg-{i:03d}",
            "target_violation_type": "text_id", "final_violation_type": None,
            "data_type": kind, "trigger_gate": "ContextGate", "is_positive": False,
            "sample_kind": "negative", "content": content, "risk_locations": [],
            "negative_boundary": boundary,
            "judgement_reason": "仅含通用岗位、某单位或已匿名姓名，缺少可识别自然人的组合。",
            "data_source": {"type": "manual_synthetic", "contains_real_personal_data": False},
            "annotation_meta": {"rule_version": "text-id-repair-20260917", "review_status": "self_checked"},
            "regression_origin": "added_negative",
        })

    ids = [row["sample_id"] for row in corpus]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate sample_id in type-6 corpus")
    dataset = OUT_DIR / "text_id_type6_regression.jsonl"
    write_jsonl(dataset, corpus)
    development, test, split_manifest = split_development_test(corpus)
    write_jsonl(OUT_DIR / "text_id_type6_development_20.jsonl", development)
    write_jsonl(OUT_DIR / "text_id_type6_test_80.jsonl", test)
    (OUT_DIR / "split_manifest.json").write_text(json.dumps(split_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    detector = TextIdDetector(use_llm=False)
    predictions = []
    development_ids = set(split_manifest["development_ids"])
    for row in development + test:
        result = detector.detect({
            "data_id": row["data_id"], "sample_id": row["sample_id"],
            "data_type": row["data_type"], "trigger_gate": row["trigger_gate"],
            "sample_kind": row["sample_kind"], "content_text": text_of(row["content"]),
        })
        predictions.append({
            "sample_id": row["sample_id"], "origin": row["regression_origin"],
            "split": "development" if row["sample_id"] in development_ids else "test",
            "data_type": row["data_type"], "expected": bool(row["is_positive"]),
            "predicted": result["is_hit"], "confidence": result["confidence"],
            "rule_score": result["rule_score"], "combination_scores": result["combination_scores"],
            "score_aggregation": result["score_aggregation"],
            "trigger_entities": result["trigger_entities"], "matched_rules": result["matched_rules"],
            "decision_source": result["decision_source"],
        })
    development_predictions = [row for row in predictions if row["split"] == "development"]
    test_predictions = [row for row in predictions if row["split"] == "test"]
    write_jsonl(OUT_DIR / "development_predictions.jsonl", development_predictions)
    write_jsonl(OUT_DIR / "latest_predictions.jsonl", test_predictions)
    grouped = defaultdict(list)
    for row in test_predictions:
        grouped[row["origin"]].append(row)
        grouped[f"data_type:{row['data_type']}"].append(row)
    metrics = {
        "dataset": str(dataset.relative_to(ROOT)),
        "dataset_sha256": hashlib.sha256(dataset.read_bytes()).hexdigest(),
        "rule_sha256": hashlib.sha256(RULE_PATH.read_bytes()).hexdigest(),
        "split_manifest": str((OUT_DIR / "split_manifest.json").relative_to(ROOT)),
        "split_seed": SPLIT_SEED,
        "development_count": len(development), "test_count": len(test),
        "source_counts": source_counts, "source_sha256": source_sha,
        "pure_rule": {"development_20": scores(development_predictions),
                      "test_80": scores(test_predictions),
                      "test_groups": {key: scores(value) for key, value in grouped.items()}},
        "holdout_status": "retrospectively_exposed: the detector was modified after an earlier evaluation of all 279 rows; the 80% split is not an independent unseen holdout",
        "llm_boundary": {"status": "not_run", "reason": "No fixed model, prompt, and available configured LLM for a reproducible run; excluded from rule metrics."},
        "evaluation_scope": "Direct delivered-rule evaluation on content text; no Gate routing or policy action evaluation.",
    }
    (OUT_DIR / "latest_metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"dataset_sha256": metrics["dataset_sha256"], "source_counts": source_counts,
                      "split": {"development": len(development), "test": len(test)},
                      "pure_rule": {key: {field: metrics["pure_rule"][key][field] for field in ("total", "tp", "fp", "fn", "tn", "precision", "recall", "f1")}
                                    for key in ("development_20", "test_80")}}, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
