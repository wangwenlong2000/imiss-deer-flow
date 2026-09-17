"""Prepare a new type-6 corpus, freeze its split, then evaluate each partition.

Order: prepare -> development -> freeze -> test. The test command checks that
the rule file has not changed since freeze. Never use test outcomes for tuning.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend/packages/harness"))

from deerflow.compliance.detectors.delivered_rules.vendor.text_id import TextIdDetector  # noqa: E402

OUT = ROOT / "合规检测/类型6-text_id整改/新批次独立评测"
RULE = ROOT / "backend/packages/harness/deerflow/compliance/detectors/delivered_rules/vendor/text_id.py"
PRIOR = ROOT / "合规检测/类型6-text_id整改/text_id_type6_regression.jsonl"
SEED = "text-id-fresh-20260917-v1"

POLICY_NAMES = (
    "陆清漪", "乔景澄", "苏怀瑾", "秦若川", "陶书远",
    "魏南乔", "沈知夏", "顾明澈", "任星遥", "侯予川",
    "邓云舟", "傅思宁", "贺清晏", "蒋逸辰", "唐沐溪",
    "宋闻舟", "谢若棠", "韩知屿", "朱清漪", "孙景初",
    "周望舒", "吴舒然", "李安澄", "郑云舒", "王知言",
)
YEARBOOK_NAMES = (
    "曹语桐", "潘明月", "罗知礼", "许清黎", "方予墨",
    "陈念初", "高栖月", "林书雅", "何映舟", "张若淮",
    "梁芷宁", "叶闻溪", "刘星禾", "杨意远", "冯言蹊",
    "赵初晴", "马亦安", "黄之恒", "徐清予", "钱云岚",
    "钟栩然", "施予宁", "莫怀安", "常明远", "姜舒窈",
)
POLICY_ORGS = ("澜桥区民政服务站", "云栖市行政审批局", "青屿县市场监管所", "明川街道办", "松陵区住房保障中心")
YEARBOOK_ORGS = ("青屿县统计局", "云栖市财政局", "澜桥区卫生统计中心", "明川人口调查站", "松陵区就业服务站")
POLICY_EVENTS = ("低保复核", "餐饮许可复议", "食品抽检投诉", "房屋征收争议", "公租房资格审核")
YEARBOOK_EVENTS = ("住户抽样复核", "教育预算校核", "卫生服务调查", "人口迁移核查", "就业登记补正")
POLICY_POSITIONS = ("复核专员", "窗口审核员", "现场调查员", "信访联络员", "救助经办人")
YEARBOOK_POSITIONS = ("抽样调查员", "预算审核员", "卫生调查员", "人口复核员", "就业统计员")
AREAS = ("澜桥", "云栖", "青屿", "明川", "松陵")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def example(data_type: str, positive: bool, i: int) -> tuple[str, str]:
    family, variant = divmod(i, 5)
    area = AREAS[variant]
    phone = f"13{variant + 4}000{family + 2}8{variant}66"
    address = f"{area}家园{family + 3}栋{variant + 1}单元{family + 2}0{variant + 1}室"
    if data_type == "policy_regulation":
        name, org, event, position = (
            POLICY_NAMES[i], POLICY_ORGS[variant], POLICY_EVENTS[variant], POLICY_POSITIONS[family]
        )
        positives = (
            f"行政听证底稿附页：被处罚人{name}的居住地址为{address}，对应{event}通知书。",
            f"政策咨询回访记录将举报人{name}、联系电话{phone}与{org}办理的{event}事项写在同一段。",
            f"内部复议材料称{name}任{org}{position}，负责{event}案件的现场核查。",
            f"信息公开审查附件注明：{org}{position}{name}参与{event}调查，姓名和岗位连续出现。",
            f"救助工单摘要列出联系人{name}，登记住址{address}，并关联{event}复核。",
        )
        negatives = (
            f"{event}办事指南仅写{position}负责受理，咨询电话为政务热线，未列出自然人姓名。",
            f"{org}公开制度模板中的责任主体写作某单位，办理事项为{event}，不含个人联系方式。",
            f"{event}案例中的当事人已匿名为张某，详细住址、工作单位与号码均已删除。",
            f"拟发布的{event}流程图使用《姓名》占位字段与示例岗位{position}，不是实际人员记录。",
            f"{area}区{event}季度通报只按部门汇总案件数，签名栏为空，未披露具体经办人。",
        )
    else:
        name, org, event, position = (
            YEARBOOK_NAMES[i], YEARBOOK_ORGS[variant], YEARBOOK_EVENTS[variant], YEARBOOK_POSITIONS[family]
        )
        positives = (
            f"统计年鉴附注写明：调查员{name}任{org}{position}，完成{event}核查。",
            f"财政年鉴表后校验说明记录联系人{name}，住在{address}，参与{event}。",
            f"人口年鉴抽样复核底稿将{name}列为{org}{position}，负责{event}。",
            f"年鉴原始调查联系表保留填报人{name}与手机号{phone}，并注明{event}任务。",
            f"公共服务年鉴修订日志注明{org}{position}{name}对{event}异常记录作了复核。",
        )
        negatives = (
            f"{event}年鉴正式表仅列{area}地区总人数和统计员岗位，没有具体姓名。",
            f"{org}年度汇总说明的填报单位已写为某单位，个人字段全部为空。",
            f"{event}年鉴演示表把人员统一替换成李某，住址及联系方式未保留。",
            f"{area}地区财政统计表只有分组汇总值，附件中的调查员是通用岗位而非个人。",
            f"年鉴编制说明展示字段名《联系人》与《单位》，示例值均为空，不对应真实个人。",
        )
    content = (positives if positive else negatives)[family]
    relation = ("full_name_with_address", "full_name_with_phone", "name_org_position_event",
                "name_org_position_event", "full_name_with_address") if data_type == "policy_regulation" else (
                "name_org_position_event", "full_name_with_address", "name_org_position_event",
                "full_name_with_phone", "name_org_position_event")
    return content, relation[family] if positive else ("generic_role", "generic_org", "anonymized_name", "placeholder", "aggregate_only")[family]


def build_rows() -> list[dict]:
    rows = []
    for data_type in ("policy_regulation", "statistical_yearbook"):
        for positive in (True, False):
            for i in range(25):
                content, category = example(data_type, positive, i)
                sample_id = f"fresh-text-id-{'policy' if data_type == 'policy_regulation' else 'yearbook'}-{'pos' if positive else 'neg'}-{i + 1:03d}"
                rows.append({
                    "data_id": sample_id, "sample_id": sample_id,
                    "target_violation_type": "text_id", "final_violation_type": "text_id" if positive else None,
                    "violation_name": "自由文本标识符泄露", "data_type": data_type,
                    "trigger_gate": "ContextGate", "is_positive": positive,
                    "sample_kind": "positive" if positive else "negative", "content": content,
                    "risk_locations": [], "annotation_category": category,
                    "judgement_reason": ("自然语言将虚构全名与个人联系方式、住址或机构岗位事项绑定。"
                                         if positive else "仅含通用角色、单位占位、匿名姓名或汇总数据。"),
                    "data_source": {"type": "manual_synthetic", "contains_real_personal_data": False,
                                    "source_name": "独立合成政策材料/统计年鉴风格评测样本"},
                    "annotation_meta": {"rule_version": "fresh-holdout-20260917-v1", "review_status": "self_checked"},
                })
    return rows


def check_overlap(rows: list[dict]) -> None:
    if not PRIOR.exists():
        raise FileNotFoundError(PRIOR)
    old = read_jsonl(PRIOR)
    ids = {row["sample_id"] for row in old}
    texts = {json.dumps(row["content"], ensure_ascii=False, sort_keys=True) for row in old}
    if any(row["sample_id"] in ids or json.dumps(row["content"], ensure_ascii=False, sort_keys=True) in texts for row in rows):
        raise ValueError("Fresh corpus overlaps the earlier regression corpus")


def prepare() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    rows = build_rows()
    check_overlap(rows)
    if len(rows) != 100 or len({row["sample_id"] for row in rows}) != 100:
        raise AssertionError("Expected 100 distinct fresh rows")
    dataset = OUT / "fresh_text_id_100.jsonl"
    proposed = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    if dataset.exists() and dataset.read_text(encoding="utf-8") != proposed:
        raise RuntimeError("Frozen dataset differs; use a new version instead of overwriting")
    dataset.write_text(proposed, encoding="utf-8")

    development_ids = set()
    for data_type in ("policy_regulation", "statistical_yearbook"):
        for positive in (True, False):
            stratum = [row for row in rows if row["data_type"] == data_type and row["is_positive"] == positive]
            ordered = sorted(stratum, key=lambda row: hashlib.sha256(
                f"{SEED}:{row['sample_id']}".encode("utf-8")
            ).hexdigest())
            development_ids.update(row["sample_id"] for row in ordered[:5])
    development = [row for row in rows if row["sample_id"] in development_ids]
    test = [row for row in rows if row["sample_id"] not in development_ids]
    if len(development) != 20 or len(test) != 80:
        raise AssertionError("Expected an exact 20/80 split")
    write_jsonl(OUT / "development_20.jsonl", development)
    write_jsonl(OUT / "test_80.jsonl", test)
    manifest = {
        "seed": SEED, "method": "SHA-256 rank by sample_id in each data_type × is_positive stratum; 5/25 development per stratum",
        "dataset_sha256": sha(dataset), "development_sha256": sha(OUT / "development_20.jsonl"),
        "test_sha256": sha(OUT / "test_80.jsonl"),
        "development_ids": [row["sample_id"] for row in development],
        "test_ids": [row["sample_id"] for row in test],
        "prepared_before_tuning": True,
    }
    manifest_path = OUT / "split_manifest.json"
    manifest_text = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    if manifest_path.exists() and manifest_path.read_text(encoding="utf-8") != manifest_text:
        raise RuntimeError("Frozen split differs; use a new version instead of overwriting")
    manifest_path.write_text(manifest_text, encoding="utf-8")
    print(json.dumps({"prepared": 100, "development": 20, "test": 80,
                      "dataset_sha256": manifest["dataset_sha256"]}, ensure_ascii=True))


def validated_split() -> dict:
    manifest = json.loads((OUT / "split_manifest.json").read_text(encoding="utf-8"))
    for file, key in (("fresh_text_id_100.jsonl", "dataset_sha256"),
                      ("development_20.jsonl", "development_sha256"),
                      ("test_80.jsonl", "test_sha256")):
        if sha(OUT / file) != manifest[key]:
            raise RuntimeError(f"Frozen {file} hash mismatch")
    if set(manifest["development_ids"]) & set(manifest["test_ids"]):
        raise RuntimeError("Split overlap")
    return manifest


def freeze() -> None:
    manifest = validated_split()
    dev_result = OUT / "development_metrics.json"
    if not dev_result.exists():
        raise RuntimeError("Evaluate development partition before freezing the rule")
    frozen = {
        "rule_sha256": sha(RULE),
        "dataset_sha256": manifest["dataset_sha256"],
        "development_sha256": manifest["development_sha256"],
        "test_sha256": manifest["test_sha256"],
        "development_metrics_sha256": sha(dev_result),
        "test_not_used_for_tuning": True,
    }
    frozen_path = OUT / "frozen_rule.json"
    if frozen_path.exists() and json.loads(frozen_path.read_text(encoding="utf-8")) != frozen:
        raise RuntimeError("Existing freeze differs; cannot refreeze after a test")
    frozen_path.write_text(json.dumps(frozen, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(frozen, ensure_ascii=True))


def evaluate(split: str) -> None:
    manifest = validated_split()
    if split == "test":
        frozen = json.loads((OUT / "frozen_rule.json").read_text(encoding="utf-8"))
        if frozen["rule_sha256"] != sha(RULE) or frozen["dataset_sha256"] != manifest["dataset_sha256"]:
            raise RuntimeError("Rule or dataset changed after freeze")
    rows = read_jsonl(OUT / ("development_20.jsonl" if split == "development" else "test_80.jsonl"))
    expected_ids = manifest["development_ids" if split == "development" else "test_ids"]
    if [row["sample_id"] for row in rows] != expected_ids:
        raise RuntimeError("Split IDs changed")
    detector = TextIdDetector(use_llm=False)
    predictions = []
    for row in rows:
        result = detector.detect({
            "sample_id": row["sample_id"], "data_type": row["data_type"],
            "trigger_gate": row["trigger_gate"], "sample_kind": row["sample_kind"],
            "content_text": row["content"],
        })
        predictions.append({
            "sample_id": row["sample_id"], "data_type": row["data_type"],
            "expected": row["is_positive"], "predicted": result["is_hit"],
            "rule_score": result["rule_score"], "combination_scores": result["combination_scores"],
            "trigger_entities": result["trigger_entities"], "decision_source": result["decision_source"],
        })
    counts = Counter((row["expected"], row["predicted"]) for row in predictions)
    tp, fp, fn, tn = (counts[(True, True)], counts[(False, True)], counts[(True, False)], counts[(False, False)])
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    metrics = {
        "split": split, "dataset_sha256": manifest["dataset_sha256"], "rule_sha256": sha(RULE),
        "total": len(rows), "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": round(precision, 6), "recall": round(recall, 6),
        "f1": round(2 * precision * recall / (precision + recall), 6) if precision + recall else 0.0,
        "fp_ids": [row["sample_id"] for row in predictions if not row["expected"] and row["predicted"]],
        "fn_ids": [row["sample_id"] for row in predictions if row["expected"] and not row["predicted"]],
        "llm_boundary": {"status": "not_run"},
        "scope": "Direct delivered rule, LLM disabled; excludes Gate and policy routing",
    }
    write_jsonl(OUT / f"{split}_predictions.jsonl", predictions)
    (OUT / f"{split}_metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metrics, ensure_ascii=True, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare", "development", "freeze", "test"))
    action = parser.parse_args().action
    if action == "prepare":
        prepare()
    elif action == "freeze":
        freeze()
    else:
        evaluate(action)


if __name__ == "__main__":
    main()
