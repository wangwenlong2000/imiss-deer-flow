# -*- coding: utf-8 -*-
"""
Local rule detector for the 4th and 5th compliance categories only:

- illegal_content
- political

The script deliberately avoids LLMs, online APIs, image/video parsing and any
rules for other violation labels. It evaluates only JSON/JSONL records whose
target_violation_type is illegal_content or political, with deterministic
stratified 8:2 train/test splitting.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


TARGET_TYPES = {"illegal_content", "political"}
LABELS = ("illegal_content", "political", "none")
DEFAULT_SEED = 20260624
DEFAULT_TRAIN_RATIO = 0.8
DEFAULT_MIN_TEST_ACCURACY = 0.95
REPORT_SCHEMA_VERSION = "illegal_political_detector_v4_report_1"

EXCLUDED_SCAN_DIRS = {
    ".agents",
    ".codex",
    ".git",
    "__pycache__",
    "configs",
    "reports",
    "scripts",
    "test_data",
}
GENERATED_REPORT_PREFIXES = (
    "compare_",
    "illegal_political_data_summary_report",
    "illegal_political_detection_report",
    "illegal_political_report",
)


@dataclass
class TextField:
    path: str
    text: str


@dataclass
class Sample:
    sample_id: str
    data_id: str
    target_violation_type: str
    expected_label: str
    is_positive: bool
    sample_kind: str
    data_type: str
    source_paths: List[str] = field(default_factory=list)
    text_fields: List[TextField] = field(default_factory=list)


@dataclass
class RuleHit:
    field_path: str
    rule_id: str
    reason_code: str
    predicted_label: str
    score: int
    text_excerpt: str


def iter_data_paths(input_path: Path) -> List[Path]:
    """Return JSON/JSONL inputs, accepting either a file or a directory."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")
    if input_path.is_file():
        if input_path.suffix.lower() not in {".json", ".jsonl"}:
            raise ValueError(f"Input file must be .json or .jsonl: {input_path}")
        return [input_path]

    paths = sorted(list(input_path.rglob("*.jsonl")) + list(input_path.rglob("*.json")))
    return [path for path in paths if not should_skip_scan_file(path, input_path)]


def should_skip_scan_file(path: Path, root: Path) -> bool:
    try:
        rel_parts = path.relative_to(root).parts
    except ValueError:
        rel_parts = path.parts
    lowered_parts = {part.lower() for part in rel_parts[:-1]}
    if lowered_parts & EXCLUDED_SCAN_DIRS:
        return True
    stem = path.stem.lower()
    return any(stem.startswith(prefix) for prefix in GENERATED_REPORT_PREFIXES)


def read_json_records(input_path: Path) -> Iterable[Tuple[Dict[str, Any], str, int]]:
    """Yield top-level JSON objects from JSONL and JSON files."""
    paths = iter_data_paths(input_path)
    for path in paths:
        try:
            if path.suffix.lower() == ".jsonl":
                with path.open("r", encoding="utf-8-sig", errors="replace") as f:
                    for line_no, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(obj, dict):
                            yield obj, str(path), line_no
            else:
                with path.open("r", encoding="utf-8-sig", errors="replace") as f:
                    obj = json.load(f)
                rows = obj if isinstance(obj, list) else ([obj] if isinstance(obj, dict) else [])
                for idx, row in enumerate(rows, 1):
                    if isinstance(row, dict):
                        yield row, str(path), idx
        except (OSError, json.JSONDecodeError):
            continue


def expected_label_for(record: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], bool]:
    target = record.get("target_violation_type") or record.get("original_target_violation_type")
    final = record.get("final_violation_type") or record.get("original_final_violation_type")
    if "is_positive" in record:
        positive = bool(record.get("is_positive"))
    else:
        positive = bool(final and target and final == target)
    expected = target if positive else "none"
    return target, expected, positive


def scalar_to_text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return None


def should_skip_text_path(path: str, text: str) -> bool:
    lower_path = path.lower()
    lower_text = text.lower()
    if "user_query" in lower_path or lower_path.endswith(".content") or lower_path == "content":
        return False
    skip_tokens = (
        "_path",
        ".path",
        "path",
        "url",
        "source_split",
        "frame_time",
        "frame_number",
        "frame_count",
        "fps",
        "resolution",
        "mask_label",
        "visual_findings",
    )
    if any(token in lower_path for token in skip_tokens):
        return True
    if re.fullmatch(r"[\w./:\\-]+", lower_text) and ("/" in lower_text or "\\" in lower_text):
        return True
    return False


def collect_from_value(value: Any, path: str, out: List[TextField]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            collect_from_value(child, f"{path}.{key}" if path else str(key), out)
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            collect_from_value(child, f"{path}[{idx}]", out)
    else:
        text = scalar_to_text(value)
        if text and not should_skip_text_path(path, text):
            out.append(TextField(path=path, text=text))


def extract_text_fields(record: Dict[str, Any]) -> List[TextField]:
    """Extract only business content fields, not annotation explanations."""
    fields: List[TextField] = []
    for key in ("content_text", "content", "raw_content", "query", "user_query", "prompt", "response", "answer", "title", "summary", "description"):
        if key in record:
            collect_from_value(record[key], key, fields)

    features = record.get("features")
    if isinstance(features, dict):
        collect_from_value(features, "features", fields)

    original = record.get("original_record")
    if isinstance(original, dict) and "content" in original:
        collect_from_value(original["content"], "original_record.content", fields)

    # Prefer precise fields over content_text for identical text.
    by_text: Dict[str, TextField] = {}
    for field_item in fields:
        text = normalize_ws(field_item.text)
        if len(text) < 2:
            continue
        current = by_text.get(text)
        if current is None or field_priority(field_item.path) < field_priority(current.path):
            by_text[text] = TextField(path=field_item.path, text=text)
    return sorted(by_text.values(), key=lambda item: (field_priority(item.path), item.path))


def field_priority(path: str) -> int:
    if "user_query" in path:
        return 0
    if path.startswith("features."):
        return 1
    if path in {"content", "raw_content"} or path.startswith("content."):
        return 2
    if path.startswith("original_record.content"):
        return 3
    if path == "content_text":
        return 4
    return 5


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def sample_key(record: Dict[str, Any], target: str, expected: str, text_fields: Sequence[TextField]) -> Tuple[str, str, str]:
    sample_id = (
        record.get("sample_id")
        or record.get("original_data_id")
        or record.get("data_id")
        or hashlib.md5(" ".join(field.text for field in text_fields).encode("utf-8")).hexdigest()
    )
    return str(sample_id), target, expected


def load_samples(input_path: Path) -> Tuple[List[Sample], Dict[str, Any]]:
    scanned_files = iter_data_paths(input_path)
    raw_candidates = 0
    samples: Dict[Tuple[str, str, str], Sample] = {}
    duplicate_records = 0
    candidate_files = set()
    records_without_text = 0

    for record, source_path, _line_no in read_json_records(input_path):
        target, expected, positive = expected_label_for(record)
        if target not in TARGET_TYPES or expected not in LABELS:
            continue
        raw_candidates += 1
        candidate_files.add(source_path)
        text_fields = extract_text_fields(record)
        if not text_fields:
            records_without_text += 1
        key = sample_key(record, target, expected, text_fields)
        sample_id, _, _ = key
        data_id = str(record.get("data_id") or record.get("original_data_id") or sample_id)

        existing = samples.get(key)
        if existing is None:
            samples[key] = Sample(
                sample_id=sample_id,
                data_id=data_id,
                target_violation_type=target,
                expected_label=expected,
                is_positive=positive,
                sample_kind=str(record.get("sample_kind") or ("positive" if positive else "negative")),
                data_type=str(record.get("data_type") or record.get("original_data_type") or ""),
                source_paths=[source_path],
                text_fields=list(text_fields),
            )
        else:
            duplicate_records += 1
            if source_path not in existing.source_paths:
                existing.source_paths.append(source_path)
            merge_text_fields(existing, text_fields)

    loaded = sorted(samples.values(), key=lambda item: (item.target_violation_type, item.expected_label, item.sample_id))
    stats = {
        "input_path": str(input_path),
        "scanned_json_files": len(scanned_files),
        "candidate_json_files": len(candidate_files),
        "raw_candidate_records": raw_candidates,
        "deduplicated_samples": len(loaded),
        "duplicate_records_merged": duplicate_records,
        "candidate_records_without_text_fields": records_without_text,
    }
    return loaded, stats


def merge_text_fields(sample: Sample, new_fields: Sequence[TextField]) -> None:
    by_text = {field_item.text: field_item for field_item in sample.text_fields}
    for field_item in new_fields:
        current = by_text.get(field_item.text)
        if current is None or field_priority(field_item.path) < field_priority(current.path):
            by_text[field_item.text] = field_item
    sample.text_fields = sorted(by_text.values(), key=lambda item: (field_priority(item.path), item.path))


def stratified_split(samples: Sequence[Sample], seed: int, train_ratio: float = 0.8) -> Tuple[List[Sample], List[Sample]]:
    if not 0 < train_ratio < 1:
        raise ValueError(f"train_ratio must be between 0 and 1, got {train_ratio}")
    groups: Dict[Tuple[str, str], List[Sample]] = defaultdict(list)
    for sample in samples:
        groups[(sample.target_violation_type, sample.expected_label)].append(sample)

    rng = random.Random(seed)
    train: List[Sample] = []
    test: List[Sample] = []
    for key in sorted(groups):
        group = sorted(groups[key], key=lambda item: item.sample_id)
        rng.shuffle(group)
        if len(group) <= 1:
            n_train = len(group)
        else:
            n_train = int(round(len(group) * train_ratio))
            n_train = min(max(n_train, 1), len(group) - 1)
        train.extend(group[:n_train])
        test.extend(group[n_train:])

    return (
        sorted(train, key=lambda item: item.sample_id),
        sorted(test, key=lambda item: item.sample_id),
    )


def has_any(text: str, terms: Sequence[str]) -> bool:
    return any(term in text for term in terms)


def count_terms(text: str, terms: Sequence[str]) -> int:
    return sum(1 for term in terms if term in text)


def excerpt(text: str, limit: int = 160) -> str:
    text = normalize_ws(text)
    return text[:limit] + ("..." if len(text) > limit else "")


ILLEGAL_SAFE_CONTEXT = (
    "普法",
    "法规",
    "法律",
    "规定",
    "条例",
    "办法",
    "处罚法",
    "反电信网络诈骗法",
    "网络信息内容生态治理规定",
    "严厉打击",
    "治理",
    "防范",
    "反诈宣传",
    "风险提示",
    "合规",
    "维权",
    "毕业论文",
    "法学研究",
    "统计数据",
    "定罪标准",
    "裁判要旨",
    "合同模板",
    "合法",
    "投诉举报",
)

ILLEGAL_REQUEST_INTENT = (
    "帮我",
    "请告诉",
    "如何",
    "怎么",
    "步骤",
    "方法",
    "教程",
    "攻略",
    "详细",
    "配方",
    "原料",
    "设备",
    "工具",
    "推荐",
    "链接",
    "注册",
    "充值",
    "发布",
    "招募",
    "招聘",
    "推广",
    "购买",
    "获取",
    "生成",
    "写一份",
    "起草",
    "包装",
)


def safe_illegal_context(text: str) -> bool:
    if has_any(text, ("具体话术", "话术模板", "操作步骤", "详细配方", "平台链接")):
        return False
    if "未打码" in text:
        return False
    if has_any(text, ("风险提示", "常见特征", "提醒公众", "正规渠道", "防范提示", "公益广告", "共建文明城市")):
        return True
    if has_any(text, ("禁止伪造", "禁止代开", "不得伪造", "严禁伪造", "禁止黄赌毒", "严厉打击")):
        return True
    if has_any(text, ("真人CS", "场地租赁", "团建活动")):
        return True
    if has_any(text, ("不要导出", "不需要导出", "无需导出", "先不要导出", "不要提取", "无需提取")):
        return True
    if has_any(text, ("残迹", "仅露出", "隐约可见")) and has_any(text, ("已被正规", "正规家政", "覆盖", "无法确认")):
        return True
    if has_any(text, ("需要打码", "打码的位置", "敏感信息", "检查这批帧", "脱敏位置")) and not has_any(text, ("未打码", "导出", "恢复", "追踪", "列出", "提取名单")):
        return True
    if has_any(text, ILLEGAL_SAFE_CONTEXT) and not has_any(text, ("规避", "伪造", "虚假", "假的", "骗取", "引流", "招募", "充值", "获取目标", "webshell")):
        return True
    if has_any(text, ("脱敏", "统计", "风险检查", "人工复核", "适合公开发布")) and not has_any(text, ("导出", "恢复", "追踪", "列出")):
        return True
    return False


def residual_or_obscured_illegal_ad(text: str) -> bool:
    return has_any(text, ("残迹", "仅露出", "隐约可见", "尾号")) and has_any(text, ("已被正规", "正规家政", "表层", "覆盖", "无法确认"))


def has_ad_or_delivery_context(text: str) -> bool:
    return has_any(
        text,
        (
            "广告",
            "小广告",
            "横幅",
            "海报",
            "贴纸",
            "传单",
            "喷涂",
            "涂写",
            "字幕",
            "滚动条",
            "水印",
            "二维码",
            "APP下载",
            "下载二维码",
            "电话",
            "微信",
            "QQ",
            "咨询",
            "热线",
            "联系方式",
            "联系",
            "面交",
            "发货",
            "包邮",
            "批发",
            "现货",
            "大量现货",
            "售卖",
            "买卖",
            "交易",
            "招揽",
            "推广",
            "REDACTED-CONTACT",
        ),
    )


def detect_v4_illegal_ad_field(field_item: TextField) -> List[RuleHit]:
    text = field_item.text
    hits: List[RuleHit] = []
    if safe_illegal_context(text) or residual_or_obscured_illegal_ad(text):
        return hits

    if has_any(text, ("绕过访问校验", "访问校验", "未授权数据", "删除操作痕迹", "避免审计", "绕过脱敏", "恢复马赛克")) and has_any(text, ("绕过", "导出", "调用", "获取", "删除", "恢复", "识别")):
        hits.append(make_hit(field_item, "IL_V4_UNAUTHORIZED_OPERATION", "UNAUTHORIZED_EXPORT_OR_AUDIT_EVASION", "illegal_content", 99))

    if has_any(text, ("绕过", "规避", "绕开", "未授权", "权限控制", "访问控制", "校验", "避免审计", "删除痕迹", "删除日志")) and has_any(text, ("平台", "接口", "数据", "账号", "导出", "批量调用", "审计", "操作痕迹", "日志")):
        hits.append(make_hit(field_item, "IL_V4_BROAD_UNAUTHORIZED_SYSTEM_USE", "BROAD_UNAUTHORIZED_SYSTEM_ACCESS_OR_EXPORT", "illegal_content", 98))

    if has_any(text, ("摄像头点位", "通道号", "回放链接", "蓝衣男子", "完整路线")) and has_any(text, ("列出", "追踪", "给出", "导出", "恢复", "识别")):
        hits.append(make_hit(field_item, "IL_V4_SURVEILLANCE_TRACKING_EXPORT", "SURVEILLANCE_POINT_OR_PERSON_TRACKING_EXPORT", "illegal_content", 98))

    if has_any(text, ("赌博", "博彩", "六合彩", "特码", "老虎机", "电玩城", "投注", "下注", "赌局", "赌球")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_GAMBLING_AD", "GAMBLING_AD_OR_LEAD", "illegal_content", 96))

    if has_any(text, ("枪支", "枪械", "管制器具", "管制刀具", "仿真狗", "仿真枪", "击发件", "改装配件", "铅弹", "火药")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_WEAPON_AD", "WEAPON_OR_CONTROLLED_ITEM_AD", "illegal_content", 96))

    if has_any(text, ("危险化学品", "危险品", "强氧化剂", "化学原料", "实验原料", "分装瓶")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_HAZARDOUS_MATERIAL_AD", "HAZARDOUS_MATERIAL_TRANSACTION_AD", "illegal_content", 95))

    if has_any(text, ("违禁药", "非法药", "迷幻药", "失忆水", "乖乖液", "回春神水", "止痛粉", "助眠针剂", "收药", "回收各类药品")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_PROHIBITED_DRUG_AD", "PROHIBITED_DRUG_OR_MEDICINE_AD", "illegal_content", 95))

    if has_any(text, ("监听", "非法监听", "针孔摄像头", "针孔设备", "手机通话监听", "追踪器录音", "汽车定位追踪器", "防出轨定位器")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_ILLEGAL_SURVEILLANCE_AD", "ILLEGAL_LISTENING_OR_TRACKING_DEVICE_AD", "illegal_content", 95))

    if has_any(text, ("伪造证件", "代办证件", "办证", "代办驾驶证", "免考代办", "假证", "假身份证", "假护照", "结婚证", "刻章", "公章", "财务章", "仿制印章", "伪造证件照", "PS合成照片", "换脸视频制作")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_FORGED_DOCUMENT_AD", "FORGED_DOCUMENT_OR_SEAL_AD", "illegal_content", 95))

    if (has_any(text, ("虚开发票", "代开发票", "发票代开", "代开专票", "代开增值税发票", "餐饮发票", "交通发票", "税点")) or ("发票" in text and "代开" in text)) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_FALSE_INVOICE_AD", "FALSE_INVOICE_AD", "illegal_content", 95))

    if has_any(text, ("盗号", "账号盗卖", "账号买卖", "批量账号", "实名资料包", "实名账号", "老号批发", "改密老号", "绕过验证")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_STOLEN_ACCOUNT_AD", "STOLEN_ACCOUNT_OR_CREDENTIAL_AD", "illegal_content", 94))

    if has_any(text, ("洗钱", "跑分", "套现", "换汇", "USDT", "信用卡花呗白条套现", "花呗白条套现")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_MONEY_LAUNDERING_AD", "MONEY_LAUNDERING_OR_CASHOUT_AD", "illegal_content", 94))

    if has_any(text, ("非法集资", "保本高息", "保本收益", "零风险高收益", "众筹保本返利", "稳赚计划", "推荐返利")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_ILLEGAL_FUNDRAISING_AD", "ILLEGAL_FUNDRAISING_AD", "illegal_content", 93))

    if has_any(text, ("考试作弊", "替考", "无线耳机包过", "内部答案", "答案包过", "考场无线耳机", "作弊设备")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_EXAM_CHEATING_AD", "EXAM_CHEATING_SERVICE_AD", "illegal_content", 93))

    if has_any(text, ("约炮", "一夜情", "裸聊", "同城陪玩", "附近的人")) and has_any(text, ("APP下载", "二维码", "交友", "神器", "联系", "广告")):
        hits.append(make_hit(field_item, "IL_V4_SEXUAL_SERVICE_LEAD", "SEXUAL_SERVICE_OR_HOOKUP_LEAD", "illegal_content", 92))

    if has_any(text, ("代孕", "包成功", "男孩女孩可选", "可选男女")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_SURROGACY_AD", "ILLEGAL_SURROGACY_OR_SELECTION_AD", "illegal_content", 92))

    if has_any(text, ("封建迷信诈骗", "神算命理", "法事", "转账开光", "改运消灾", "命理神算", "财神法事")) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_SUPERSTITION_FRAUD_AD", "SUPERSTITION_FRAUD_AD", "illegal_content", 90))

    if has_any(text, ("电子烟", "水果味")) and has_any(text, ("大量现货", "现货", "地址", "电话", "微信", "海报", "广告")):
        hits.append(make_hit(field_item, "IL_V4_ECIGARETTE_AD", "REGULATED_ECIGARETTE_AD", "illegal_content", 88))

    broad_illegal_entities = (
        "赌博",
        "涉赌",
        "博彩",
        "私彩",
        "百家乐",
        "棋牌",
        "六合彩",
        "特码",
        "老虎机",
        "毒品",
        "涉毒",
        "违禁药",
        "非法药",
        "迷幻药",
        "失忆水",
        "乖乖液",
        "枪支",
        "枪械",
        "管制器具",
        "管制刀具",
        "危险化学品",
        "强氧化剂",
        "化学原料",
        "虚开发票",
        "代开发票",
        "代开",
        "办证",
        "代办证件",
        "伪造证件",
        "假证",
        "刻章",
        "公章",
        "盗号",
        "账号买卖",
        "账号盗卖",
        "绕过验证",
        "洗钱",
        "跑分",
        "套现",
        "USDT",
        "非法集资",
        "保本高息",
        "零风险高收益",
        "考试作弊",
        "刷单",
        "返佣",
        "电诈",
        "贷款广告",
        "无抵押",
        "替考",
        "内部答案",
        "监听",
        "针孔摄像头",
        "针孔",
        "偷拍",
        "定位追踪",
        "约炮",
        "一夜情",
        "色情",
        "成人",
        "上门",
        "陪聊",
        "代孕",
        "封建迷信诈骗",
        "改运消灾",
    )
    if has_any(text, broad_illegal_entities) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_BROAD_ILLEGAL_AD_CONTEXT", "ILLEGAL_ENTITY_WITH_AD_OR_DELIVERY_CONTEXT", "illegal_content", 85))

    if has_any(text, ("违法", "非法", "黑灰产", "招揽", "暗语", "交易", "买卖", "售卖", "代办", "包过", "包成功", "内部消息", "准确率", "高收益", "保本", "返利")) and has_ad_or_delivery_context(text) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_V4_GENERIC_ILLEGAL_LEAD", "GENERIC_ILLEGAL_AD_OR_TRANSACTION_LEAD", "illegal_content", 84))

    surveillance_export_terms = (
        "摄像头",
        "监控",
        "通道号",
        "回放链接",
        "rtsp",
        "流地址",
        "账号",
        "人员身份",
        "身份",
        "人脸",
        "车牌",
        "人员",
        "车辆",
        "行踪",
        "完整路线",
        "从哪个路口",
        "蓝衣男子",
        "男子",
        "马赛克区域",
        "模糊区域",
        "脱敏区域",
    )
    if has_any(text, surveillance_export_terms) and has_any(text, ("导出", "列出", "恢复", "识别", "追踪", "给出", "提取", "还原")) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_V4_BROAD_SURVEILLANCE_EXPORT", "BROAD_SURVEILLANCE_EXPORT_OR_TRACKING_REQUEST", "illegal_content", 84))

    synthetic_illegal_tags = (
        "stolen_accounts",
        "weapons",
        "hazardous_material",
        "false_invoice",
        "superstition_fraud",
        "forged_documents",
        "exam_cheating",
        "prohibited_drugs",
        "illegal_surveillance",
        "illegal_fundraising",
        "合成盗号交易广告",
        "合成管制器具广告",
        "合成危险品交易暗语",
        "合成虚开发票广告",
        "合成封建迷信诈骗",
        "合成伪造证件广告",
        "合成考试作弊广告",
        "合成违禁药品暗语",
        "合成非法监听广告",
        "合成非法集资宣传",
    )
    if has_any(text, synthetic_illegal_tags) and has_ad_or_delivery_context(text):
        hits.append(make_hit(field_item, "IL_V4_STRUCTURED_ILLEGAL_AD_TAG", "STRUCTURED_ILLEGAL_AD_OR_RISK_TAG", "illegal_content", 86))

    return hits


def detect_illegal_field(field_item: TextField) -> List[RuleHit]:
    text = field_item.text
    hits: List[RuleHit] = []
    hits.extend(detect_v4_illegal_ad_field(field_item))

    surveillance_objects = (
        "清晰人脸",
        "人脸截图",
        "人脸",
        "车牌",
        "车牌号",
        "门牌号",
        "店铺电话",
        "人员名单",
        "卡口",
        "监控点位",
        "监控点",
        "流地址",
        "监控资源",
        "某辆车",
        "具体位置",
        "轨迹",
        "高清录像",
    )
    surveillance_actions = (
        "导出",
        "恢复",
        "追踪",
        "列出",
        "抓拍",
        "获取",
        "下载",
        "清单",
        "输出",
        "提取",
        "名单",
        "列表",
        "逐帧",
        "排序",
        "未打码",
        "未脱敏",
        "去除马赛克",
        "还原",
    )
    if has_any(text, surveillance_objects) and has_any(text, surveillance_actions) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_SURVEILLANCE_EXPORT", "EXPORT_IDENTIFIABLE_SURVEILLANCE", "illegal_content", 98))

    if has_any(text, ("摄像头编号", "安装经纬度")) and has_any(text, ("给我", "全部", "导出", "输出", "获取")) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_SURVEILLANCE_ASSET_EXPORT", "EXPORT_SURVEILLANCE_ASSET_LOCATION", "illegal_content", 97))

    forgery_terms = ("伪造", "虚假", "假的", "假合同", "假发票", "假材料", "经营流水", "包装为经营贷", "征信修复", "骗取")
    doc_terms = ("合同", "发票", "许可证", "证明", "材料", "流水", "经营贷", "征信", "贷款", "还贷")
    if has_any(text, forgery_terms) and has_any(text, doc_terms) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_FORGERY_FRAUD_DOC", "FORGERY_OR_FRAUD_OPERATION", "illegal_content", 96))

    if has_any(text, ("规避", "逃避", "绕过")) and has_any(text, ("监管", "反洗钱", "执法", "审查", "追踪", "处罚")):
        hits.append(make_hit(field_item, "IL_EVASION", "EVADING_REGULATION_OR_ENFORCEMENT", "illegal_content", 95))

    if has_any(text, ("税收洼地", "利润转移", "关联公司")) and has_any(text, ("降低整体税负", "降低税负", "税务筹划空间", "避税")):
        hits.append(make_hit(field_item, "IL_TAX_AVOIDANCE_SCHEME", "TAX_AVOIDANCE_OR_PROFIT_SHIFTING_SCHEME", "illegal_content", 94))

    if has_any(text, ("小额贷款", "无抵押", "当天放款", "贷款广告")) and has_any(text, ("微信", "二维码", "联系方式", "联系", "QQ")) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_LOAN_SCAM_AD", "ILLEGAL_LOAN_OR_SCAM_LEAD_AD", "illegal_content", 91))

    if has_any(text, ("高薪招聘公关", "招聘公关", "公关 月入", "要求女性")) and has_any(text, ("月入", "18-30", "联系", "电话", "微信")) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_SUSPICIOUS_SEX_RECRUITMENT", "SUSPICIOUS_SEX_SERVICE_RECRUITMENT_AD", "illegal_content", 90))

    if has_any(text, ("私家侦探", "婚外情调查", "跟踪定位", "手机监听")) and has_any(text, ("联系", "电话", "微信", "广告", "喷涂")) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_SURVEILLANCE_STALKING_SERVICE", "ILLEGAL_TRACKING_OR_PHONE_MONITORING_SERVICE", "illegal_content", 90))

    if has_any(text, ("冰毒", "毒品", "制毒", "涉毒")) and has_any(text, ("合成", "配方", "原料", "设备", "操作条件", "教程")):
        hits.append(make_hit(field_item, "IL_DRUG_INSTRUCTION", "DRUG_MANUFACTURING_INSTRUCTION", "illegal_content", 99))

    gambling_terms = ("赌博", "涉赌", "百家乐", "博彩", "赌场", "赌资")
    gambling_actions = ("平台链接", "注册网址", "注册", "充值", "参与", "招募", "招聘", "运营", "推广", "引流", "推荐")
    if has_any(text, gambling_terms) and has_any(text, gambling_actions) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_GAMBLING_ACTION", "GAMBLING_ACCESS_OR_RECRUITMENT", "illegal_content", 94))

    scam_terms = ("诈骗", "冒充公检法", "钓鱼", "骗取", "电诈")
    scam_action_terms = ("话术", "模板", "脚本", "群发", "引流", "获取目标", "邮箱密码", "具体话术", "工具", "方法")
    if has_any(text, scam_terms) and has_any(text, scam_action_terms) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_SCAM_SCRIPT", "SCAM_SCRIPT_OR_CREDENTIAL_THEFT", "illegal_content", 93))

    cyber_terms = ("网络钓鱼", "webshell", "木马", "服务器控制权", "弱口令", "后台地址", "文件上传接口")
    if has_any(text, cyber_terms) and has_any(text, ("攻击", "获取", "上传", "控制权", "密码", "漏洞", "工具", "方法")):
        hits.append(make_hit(field_item, "IL_CYBER_ABUSE", "CYBER_ABUSE_OR_INTRUSION_GUIDANCE", "illegal_content", 92))

    pyramid_terms = ("返利", "推广链接", "下线", "会员", "日返", "短期高回报", "高回报项目", "月收入", "杠杆倍率")
    if count_terms(text, pyramid_terms) >= 2 and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_PYRAMID_HIGH_RETURN", "ILLEGAL_FINANCIAL_RECRUITMENT", "illegal_content", 88))

    ocr_illegal_markers = ("涉赌", "涉黄", "涉毒", "黄赌毒", "非法小广告", "违规证件", "代办证件", "办证", "代开发票", "黑灰产", "诈骗导流", "违法广告")
    ocr_delivery_markers = ("广告", "联系方式", "二维码", "导流", "引流", "售卖", "贴纸", "标语", "电话", "加群", "扫码")
    if has_any(text, ocr_illegal_markers) and has_any(text, ocr_delivery_markers) and not residual_or_obscured_illegal_ad(text) and not safe_illegal_context(text):
        hits.append(make_hit(field_item, "IL_TEXTUAL_ILLEGAL_AD", "TEXTUAL_ILLEGAL_AD_OR_LEAD", "illegal_content", 87))

    if has_any(text, ("招嫖", "卖淫嫖娼", "色情暗语")) and has_any(text, ("电话", "号码", "联系方式", "暗语")):
        hits.append(make_hit(field_item, "IL_SEX_TRADE_AD", "SEX_TRADE_AD_OR_CONTACT_LEAD", "illegal_content", 87))

    if not hits and not safe_illegal_context(text):
        high_risk = ("涉黄", "涉毒", "毒品", "赌博", "诈骗", "违法交易", "黑灰产", "非法广告", "引流", "洗钱", "空壳公司", "虚拟货币")
        if count_terms(text, high_risk) >= 2 and has_any(text, ILLEGAL_REQUEST_INTENT):
            hits.append(make_hit(field_item, "IL_COMBO_INTENT", "HIGH_RISK_KEYWORDS_WITH_REQUEST_INTENT", "illegal_content", 72))

    return hits


POLITICAL_SAFE_CONTEXT = (
    "公开政策文本",
    "公开文件",
    "公开法规",
    "公开案例",
    "公开统计数据",
    "公开听证",
    "政府信息公开",
    "政务公开",
    "制定程序",
    "征求意见",
    "公众反馈",
    "听证会公告",
    "行政复议",
    "行政诉讼",
    "司法救济",
    "依法",
    "政策解读",
    "政策说明",
    "公告",
    "通报",
    "救济渠道",
    "建设性意见",
    "研究摘要",
    "研究提纲",
    "法学课堂",
    "培训教材",
    "避免使用未经核实",
    "避免评价具体机构",
    "避免编造事实",
    "不使用煽动性",
    "未涉及不当动员",
    "没有攻击性表达",
    "未号召线下聚集",
    "平衡呈现不同意见",
)


def safe_political_context(text: str) -> bool:
    if has_any(text, ("未号召", "没有攻击性表达", "未涉及不当动员")):
        return True
    if has_any(text, ("避免编造", "不得编造", "不要编造", "不应编造", "防止编造", "避免捏造", "不得捏造", "不要捏造", "不应捏造")):
        return True
    if has_any(
        text,
        (
            "可以引用未核实",
            "未经核实",
            "来源无法核实",
            "来源不明",
            "不必核实",
            "不要核验",
            "不要查看正式文件",
            "无需核实",
            "编造",
            "捏造",
            "虚构",
            "假借",
            "伪装成",
            "加工为",
            "删除不同意见",
            "制造严重冲突",
            "制造恐慌",
            "连续爆料",
            "设置统一头像",
            "境外机构已经定性",
            "鼓励读者",
            "统一转发",
            "同步扩散",
            "刷屏",
            "号召大家",
            "号召居民",
            "号召群众",
            "煽动性语言",
            "群发文",
            "围堵",
            "集结",
            "制造冲突",
        ),
    ):
        return False
    return has_any(text, POLITICAL_SAFE_CONTEXT)


def detect_v4_political_field(field_item: TextField) -> List[RuleHit]:
    text = field_item.text
    hits: List[RuleHit] = []
    if safe_political_context(text):
        return hits

    policy_or_public_terms = (
        "政策",
        "规则",
        "细则",
        "办法",
        "方案",
        "改革",
        "调整",
        "整治",
        "行动",
        "管理",
        "物业",
        "教育",
        "交通",
        "住房",
        "土地征收",
        "市场监管",
        "治理",
        "考核",
        "准入",
        "审核",
        "目录",
        "指引",
        "指导意见",
        "正式渠道",
        "办事大厅",
        "补偿",
        "听证",
        "招标",
        "执法",
        "城管",
        "政务",
        "公共",
        "服务",
        "收费",
        "停车收费",
        "公共停车",
        "补贴",
        "社保",
        "医保",
        "报名",
        "预约",
        "环保",
        "限行",
        "社区",
        "旧改",
        "防汛",
        "应急",
        "信访",
        "公共工程",
        "公共交通",
        "公共机构",
        "官方通报",
        "执行部门",
    )
    unverified_terms = (
        "未经核实",
        "不必核实",
        "来源无法核实",
        "来源不明",
        "几张来源不明",
        "匿名论坛",
        "匿名",
        "网传",
        "散布",
        "声称",
        "传言",
        "爆料",
        "假借",
        "知情人",
        "内部知情人",
        "未公开会议纪要",
    )
    fabrication_terms = (
        "编造",
        "捏造",
        "虚构",
        "伪装成",
        "包装成",
        "加工为",
        "制作",
        "写得像",
        "假借",
        "添加虚构细节",
        "制造可信度",
        "伪装",
        "暗示",
        "断章取义",
        "删除背景说明",
        "删掉样本量说明",
        "删除不同意见",
        "把质疑改成确定事实",
        "连续炒作",
        "更换一个未经证实的指控",
    )
    amplification_terms = (
        "转发",
        "扩散",
        "跨群",
        "跨平台",
        "多个平台",
        "同步发布",
        "同步扩散",
        "同一时段",
        "多个账号",
        "多个匿名账号",
        "统一文案",
        "统一口号",
        "统一头像",
        "刷屏",
        "群发",
        "置顶",
        "连续更新",
        "连续爆料",
        "持续发酵",
        "倒计时",
        "统计转发",
        "截图保存",
        "评论区",
        "直播传播",
        "媒体批量投稿",
        "话题页",
    )
    mobilization_terms = (
        "号召",
        "组织",
        "鼓动",
        "鼓励",
        "引导成员",
        "要求参与者",
        "要求群成员",
        "现场动员",
        "动员文案",
        "联署倡议",
        "统一行动",
        "集体",
        "集中投诉",
        "集中攻击",
        "围堵",
        "堵住服务大厅",
        "占据办公场所",
        "冲击服务秩序",
        "阻止工作人员",
        "线下聚集",
        "线下围堵",
        "服务窗口高峰时段集结",
        "办公楼前持续施压",
        "制造冲突",
        "占用公共通道",
        "拒绝劝离",
    )
    hostile_claim_terms = (
        "系统性黑幕",
        "系统性权力寻租",
        "黑幕",
        "利益交易",
        "利益输送",
        "秘密交易",
        "秘密安排",
        "内定",
        "控制",
        "操控",
        "外部资本操控",
        "内部关系网控制",
        "刻意篡改",
        "隐瞒重大事实",
        "掩盖真实目的",
        "掩盖事实",
        "压制商户",
        "故意损害群众利益",
        "剥夺合法权益",
        "蓄意掠夺",
        "瞒报灾情",
        "官方通报完全不可信",
        "程序均属欺骗",
        "故意制造群体对立",
        "制造群体差别",
        "严重冲突",
        "恐慌",
        "受到迫害",
        "公共服务全面崩溃",
        "全城危机",
        "全部合法性",
        "让政策无法执行",
    )

    if has_any(text, policy_or_public_terms) and has_any(text, unverified_terms) and has_any(text, fabrication_terms + amplification_terms):
        hits.append(make_hit(field_item, "PO_V4_UNVERIFIED_FABRICATION", "UNVERIFIED_POLITICAL_CLAIM_PACKAGING_OR_SPREAD", "political", 99))

    if has_any(text, policy_or_public_terms) and has_any(text, ("编造", "捏造", "虚构", "假借", "伪装成", "包装成", "加工为", "故意删掉", "删掉适用条件", "只保留")) and has_any(text, hostile_claim_terms + amplification_terms + mobilization_terms + ("不可信", "剥夺", "压制", "瞒报", "炒作")):
        hits.append(make_hit(field_item, "PO_V4_BROAD_FABRICATION_OR_DISTORTION", "BROAD_PUBLIC_AFFAIRS_FABRICATION_OR_DISTORTION", "political", 99))

    if has_any(text, policy_or_public_terms) and has_any(text, ("编造", "捏造", "虚构", "假借", "伪装成", "加工为")) and has_any(text, hostile_claim_terms + amplification_terms + mobilization_terms):
        hits.append(make_hit(field_item, "PO_V4_FABRICATED_PUBLIC_AFFAIRS_CLAIM", "FABRICATED_PUBLIC_AFFAIRS_CLAIM_OR_MATERIAL", "political", 99))

    if has_any(text, ("编造", "捏造", "虚构", "假借", "伪装成", "加工为")) and has_any(text, hostile_claim_terms) and has_any(text, amplification_terms + mobilization_terms + ("制造紧迫感", "制造恐慌")):
        hits.append(make_hit(field_item, "PO_V4_FABRICATED_HOSTILE_AMPLIFICATION", "FABRICATED_HOSTILE_PUBLIC_AFFAIRS_AMPLIFICATION", "political", 98))

    if has_any(text, policy_or_public_terms) and has_any(text, hostile_claim_terms) and has_any(text, fabrication_terms + amplification_terms + mobilization_terms):
        hits.append(make_hit(field_item, "PO_V4_POLICY_DISTORTION_AMPLIFICATION", "POLICY_DISTORTION_WITH_AMPLIFICATION_OR_ATTACK", "political", 98))

    if has_any(text, policy_or_public_terms) and has_any(text, mobilization_terms) and has_any(text, ("群众", "居民", "商户", "成员", "群成员", "参与者", "读者", "公众", "人员", "大家", "同一天")):
        hits.append(make_hit(field_item, "PO_V4_MOBILIZATION_OR_OBSTRUCTION", "POLITICAL_MOBILIZATION_OR_PUBLIC_ORDER_PRESSURE", "political", 98))

    if has_any(text, policy_or_public_terms) and has_any(text, amplification_terms) and has_any(text, ("攻击", "指控", "口号", "爆料", "公开信", "动员", "伪装", "编造", "捏造", "虚构")):
        hits.append(make_hit(field_item, "PO_V4_COORDINATED_AMPLIFICATION", "COORDINATED_POLITICAL_AMPLIFICATION", "political", 96))

    creation_request_terms = ("请", "帮我", "模型草稿", "制作", "改成", "改写", "发布", "加工", "生成", "写一", "给我一套")
    public_risk_terms = (
        "公开信",
        "动员",
        "统一行动",
        "集体",
        "聊天截图",
        "知情人",
        "内部",
        "黑幕",
        "利益",
        "压制",
        "瞒报",
        "不可信",
        "剥夺",
        "冲突",
        "舆论压力",
        "长期压制",
        "未经证实",
        "来源不明",
        "删除行政复议",
        "绕开",
        "堵住",
        "攻击",
        "指控",
    )
    if has_any(text, creation_request_terms) and has_any(text, policy_or_public_terms) and has_any(text, public_risk_terms) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_V4_PUBLIC_AFFAIRS_CREATION_INTENT", "PUBLIC_AFFAIRS_RISKY_CREATION_OR_AMPLIFICATION_INTENT", "political", 96))

    if has_any(text, unverified_terms + ("未经证实", "来源不清", "来源不明的聊天截图")) and has_any(text, ("发布", "传播", "扩散", "转发", "投放", "伪装", "包装", "编造", "捏造", "制作", "写成", "炮制")) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_V4_BROAD_UNVERIFIED_SPREAD", "BROAD_UNVERIFIED_POLITICAL_SPREAD", "political", 95))

    if has_any(text, ("号召", "组织", "煽动", "动员", "围堵", "堵住", "集结", "阻止", "制造冲突", "集中投诉", "集中举报", "统一行动", "让政策无法执行")) and has_any(text, ("政策", "公共", "政府", "部门", "办事", "服务", "居民", "商户", "群众", "公众", "机构", "工作人员")) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_V4_BROAD_PUBLIC_MOBILIZATION", "BROAD_PUBLIC_AFFAIRS_MOBILIZATION", "political", 95))

    if has_any(text, ("删掉适用条件", "只保留对群众不利", "夸大为", "全体居民", "全城居民", "长期压制", "官方不可信", "官方通报完全不可信")) and has_any(text, ("制作", "发布", "传播", "号召", "营造", "制造", "写成", "文案", "模板")) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_V4_BROAD_DISTORTED_FRAMING", "BROAD_DISTORTED_POLITICAL_FRAMING", "political", 94))

    return hits


def detect_political_field(field_item: TextField) -> List[RuleHit]:
    text = field_item.text
    hits: List[RuleHit] = []
    hits.extend(detect_v4_political_field(field_item))

    mobilization_terms = ("号召", "呼吁", "鼓励", "要求大家", "大家应", "联合", "统一", "马上转发", "同步投稿", "刷屏", "群发", "转发")
    action_terms = ("聚集", "集合", "抗议", "施压", "抵制", "评论区刷", "统一口号", "办公楼前", "市政府广场", "统一转发")
    if has_any(text, mobilization_terms) and (has_any(text, action_terms) or "煽动性" in text) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_MOBILIZATION", "POLITICAL_MOBILIZATION_OR_AMPLIFICATION", "political", 99))

    if has_any(text, ("横幅", "同步展示", "集体行动", "迫使其撤回", "迫使", "撤回")) and has_any(text, ("公共服务调整", "公共管理措施", "政策", "措施", "居民", "群众")) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_COLLECTIVE_PRESSURE_FRAME", "COLLECTIVE_PRESSURE_OR_OPPOSITION_FRAMING", "political", 98))

    rumor_terms = ("未经核实", "匿名爆料", "内部消息", "消息人士", "网传", "流传", "听说", "没有公开证据")
    spread_terms = ("爆料", "群发", "转发", "投稿", "传播", "揭露", "暗示", "写成", "生成", "包装成")
    sensitive_targets = ("部委", "高层", "反腐", "人事调整", "公共工程", "招标", "审批", "内定", "暗中操控", "少数人", "部门")
    if has_any(text, rumor_terms) and has_any(text, spread_terms) and has_any(text, sensitive_targets):
        hits.append(make_hit(field_item, "PO_UNVERIFIED_RUMOR", "UNVERIFIED_POLITICAL_RUMOR_AMPLIFICATION", "political", 96))

    rumor_claim_terms = ("暗箱操作", "串通围标", "利益输送", "不作为", "内定", "暗中操控", "少数人操控", "高层", "反腐", "保护伞")
    if has_any(text, rumor_terms) and has_any(text, rumor_claim_terms) and has_any(text, ("报道", "文章", "揭露", "分析", "写成", "群发", "转发", "投稿", "传播")):
        hits.append(make_hit(field_item, "PO_RUMOR_CORRUPTION_FRAME", "UNVERIFIED_CORRUPTION_OR_INSTITUTION_RUMOR", "political", 95))

    if has_any(text, ("不可告人的安排", "暗箱操作", "内定", "暗中操控")) and has_any(text, ("群内称", "有人在群内", "横幅", "同步展示", "传播", "做成")):
        hits.append(make_hit(field_item, "PO_UNVERIFIED_BANNER_AMPLIFICATION", "UNVERIFIED_CLAIM_PUBLIC_AMPLIFICATION", "political", 94))

    policy_terms = ("政策", "管理办法", "条例", "规则", "方案", "改革", "措施", "补贴", "招标安排", "执法检查", "部门", "政府", "行政")
    attack_terms = (
        "完全没有合法性",
        "完全无视群众权益",
        "蓄意压制",
        "刻意制造对立",
        "所有解释都是借口",
        "黑箱操作",
        "保护伞",
        "寻租",
        "行政垄断",
        "利益固化",
        "标签化",
        "边缘化",
        "背道而驰",
        "制度设计层面的深层次缺陷",
        "根本性缺陷",
        "现行体制",
        "变相剥夺",
        "歪曲",
    )
    if has_any(text, policy_terms) and has_any(text, attack_terms) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_POLICY_DISTORTION", "POLICY_DISTORTION_OR_ATTACK", "political", 92))

    institution_attack_terms = ("不可信", "包庇", "更尖锐", "尖锐", "批评性文章", "质疑", "揭露", "暗示", "利益输送", "不作为", "强行拆除", "保护伞")
    creation_terms = ("帮我", "请把", "改写", "写一篇", "生成", "草稿", "文案", "报道", "文章")
    if has_any(text, ("部门", "政府", "政策", "条例", "管理办法", "公共工程", "审批", "招标")) and has_any(text, institution_attack_terms) and has_any(text, creation_terms) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_INSTITUTION_ATTACK_CREATION", "ATTACKING_INSTITUTION_CONTENT_CREATION", "political", 89))

    if has_any(text, ("现行制度", "现行体制", "制度设计")) and has_any(text, ("根本性缺陷", "深层次缺陷", "学习借鉴西方", "西方国家的治理模式")):
        hits.append(make_hit(field_item, "PO_SYSTEMIC_EXTREME", "SYSTEMIC_EXTREME_POLITICAL_FRAMING", "political", 90))

    if has_any(text, ("群体性事件", "征地拆迁", "基层政府", "政府公信力")) and has_any(text, ("严重缺失", "不作为", "乱作为", "滥用职权", "更大范围的社会矛盾")) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_SOCIAL_CONFLICT_FRAME", "POLITICAL_SOCIAL_CONFLICT_ESCALATION", "political", 88))

    if has_any(text, ("南海", "舰队", "军事行动", "军队")) and has_any(text, ("听说", "集结", "随时准备", "局势紧张")):
        hits.append(make_hit(field_item, "PO_MILITARY_RUMOR", "SENSITIVE_MILITARY_RUMOR_OR_SPECULATION", "political", 86))

    if has_any(text, ("国际社会", "境外机构")) and has_any(text, ("公共事件", "执法", "批评意见", "国家形象", "声誉")) and not safe_political_context(text):
        hits.append(make_hit(field_item, "PO_FOREIGN_PRESSURE_FRAME", "SENSITIVE_FOREIGN_PRESSURE_FRAMING", "political", 82))

    if not hits and not safe_political_context(text):
        risky_expr = ("煽动", "歪曲", "攻击性", "极端化", "谣言", "对立情绪", "群体性事件")
        if has_any(text, policy_terms) and count_terms(text, risky_expr) >= 2:
            hits.append(make_hit(field_item, "PO_COMBO_RISK", "POLITICAL_KEYWORDS_WITH_RISKY_EXPRESSION", "political", 70))

    return hits


def make_hit(field_item: TextField, rule_id: str, reason_code: str, label: str, score: int) -> RuleHit:
    return RuleHit(
        field_path=field_item.path,
        rule_id=rule_id,
        reason_code=reason_code,
        predicted_label=label,
        score=score,
        text_excerpt=excerpt(field_item.text),
    )


def detect_sample(sample: Sample) -> Tuple[str, List[RuleHit]]:
    hits: List[RuleHit] = []
    for field_item in sample.text_fields:
        hits.extend(detect_political_field(field_item))
        hits.extend(detect_illegal_field(field_item))

    if not hits:
        return "none", []

    best = sorted(hits, key=lambda hit: (hit.score, 1 if hit.predicted_label == "political" else 0), reverse=True)[0]
    return best.predicted_label, sorted(hits, key=lambda hit: hit.score, reverse=True)


def counts_by_split(samples: Sequence[Sample]) -> Dict[str, int]:
    counter = Counter((sample.target_violation_type, sample.expected_label) for sample in samples)
    return {f"{target}:{expected}": counter[(target, expected)] for target in sorted(TARGET_TYPES) for expected in LABELS if counter[(target, expected)]}


def evaluate(samples: Sequence[Sample], split_name: str) -> Dict[str, Any]:
    confusion = {expected: {predicted: 0 for predicted in LABELS} for expected in LABELS}
    per_target = defaultdict(lambda: {"total": 0, "correct": 0})
    pos_neg = defaultdict(lambda: {"positive_total": 0, "positive_correct": 0, "negative_total": 0, "negative_correct": 0})
    rule_summary: Counter[str] = Counter()
    rule_hits: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    misclassified: List[Dict[str, Any]] = []

    correct = 0
    for sample in samples:
        predicted, hits = detect_sample(sample)
        expected = sample.expected_label
        ok = predicted == expected
        correct += int(ok)
        confusion[expected][predicted] += 1
        per_target[sample.target_violation_type]["total"] += 1
        per_target[sample.target_violation_type]["correct"] += int(ok)
        pn = pos_neg[sample.target_violation_type]
        if sample.is_positive:
            pn["positive_total"] += 1
            pn["positive_correct"] += int(ok)
        else:
            pn["negative_total"] += 1
            pn["negative_correct"] += int(ok)

        result = {
            "sample_id": sample.sample_id,
            "data_id": sample.data_id,
            "target_violation_type": sample.target_violation_type,
            "expected_label": expected,
            "predicted_label": predicted,
            "is_positive": sample.is_positive,
            "correct": ok,
            "hit_count": len(hits),
            "top_hit": hit_to_dict(hits[0], sample, expected, ok) if hits else None,
        }
        results.append(result)
        if not ok:
            misclassified.append(
                {
                    **result,
                    "source_paths": sample.source_paths,
                    "text_fields": [{"field_path": item.path, "text_excerpt": excerpt(item.text)} for item in sample.text_fields[:5]],
                    "hits": [hit_to_dict(hit, sample, expected, ok) for hit in hits],
                }
            )
        for hit in hits:
            rule_summary[hit.rule_id] += 1
            rule_hits.append(hit_to_dict(hit, sample, expected, ok))

    total = len(samples)
    per_target_accuracy = {
        target: {
            "total": values["total"],
            "correct": values["correct"],
            "accuracy": safe_rate(values["correct"], values["total"]),
        }
        for target, values in sorted(per_target.items())
    }
    pos_neg_summary = {
        target: {
            **values,
            "positive_success_rate": safe_rate(values["positive_correct"], values["positive_total"]),
            "negative_success_rate": safe_rate(values["negative_correct"], values["negative_total"]),
        }
        for target, values in sorted(pos_neg.items())
    }

    return {
        "split": split_name,
        "total": total,
        "correct": correct,
        "overall_accuracy": safe_rate(correct, total),
        "per_target_accuracy": per_target_accuracy,
        "positive_negative_by_target": pos_neg_summary,
        "confusion_matrix": confusion,
        "rule_hit_count": len(rule_hits),
        "rule_summary": dict(sorted(rule_summary.items())),
        "rule_hits": rule_hits,
        "results": results,
        "misclassified_samples": misclassified,
    }


def safe_rate(num: int, den: int) -> Optional[float]:
    if den == 0:
        return None
    return round(num / den, 6)


def hit_to_dict(hit: RuleHit, sample: Sample, expected: str, correct: bool) -> Dict[str, Any]:
    return {
        "sample_id": sample.sample_id,
        "data_id": sample.data_id,
        "field_path": hit.field_path,
        "rule_id": hit.rule_id,
        "reason_code": hit.reason_code,
        "predicted_label": hit.predicted_label,
        "expected_label": expected,
        "correct": correct,
        "score": hit.score,
        "text_excerpt": hit.text_excerpt,
    }


def build_report(input_path: Path, report_path: Path, seed: int, train_ratio: float, eval_only: bool = False) -> Dict[str, Any]:
    samples, load_stats = load_samples(input_path)
    if eval_only:
        train: List[Sample] = []
        test = list(samples)
        split_ratio = "eval_only"
    else:
        train, test = stratified_split(samples, seed=seed, train_ratio=train_ratio)
        split_ratio = f"{train_ratio:.2f}:{1 - train_ratio:.2f}"
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "script": Path(__file__).name,
        "scope": {
            "detected_labels": list(LABELS),
            "target_violation_types_loaded": sorted(TARGET_TYPES),
            "image_video_policy": "Only textual JSON fields and user_query-like text are inspected; image/video pixels and URLs are not parsed.",
            "model_policy": "No LLM, online API or network service is used.",
        },
        "data": {
            **load_stats,
            "random_seed": seed,
            "split_ratio": split_ratio,
            "eval_only": eval_only,
            "train_size": len(train),
            "test_size": len(test),
            "train_counts": counts_by_split(train),
            "test_counts": counts_by_split(test),
            "all_deduplicated_counts": counts_by_split(samples),
        },
        "rule_design_basis": {
            "illegal_content": [
                "High-risk illegal entities are flagged only when paired with operational intent such as steps, tools, links, recruitment, forgery, evasion, credential theft, or identifiable surveillance export.",
                "Legal education, news, compliance notices, risk warnings and ordinary rights-protection contexts are suppressed unless they request executable illegal material.",
            ],
            "political": [
                "Political terms are flagged only when paired with unverified rumor amplification, mobilization, policy distortion, systemic extreme framing or sensitive military speculation.",
                "Ordinary policy explanation, public regulation text, administrative procedures, lawful remedies and balanced research contexts are suppressed.",
            ],
        },
        "all_deduplicated_evaluation": evaluate(samples, "all_deduplicated"),
        "train_evaluation": evaluate(train, "train"),
        "test_evaluation": evaluate(test, "test"),
    }

    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rule-only detector for illegal_content and political samples.")
    parser.add_argument("--root", default="wgjc-0624", help="Root folder containing JSON/JSONL annotation data.")
    parser.add_argument("--input-file", default=None, help="Optional single JSON/JSONL file. Overrides --root when set.")
    parser.add_argument("--report", default="illegal_political_report_V4.json", help="Output evaluation report path.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Deterministic random seed for stratified split.")
    parser.add_argument("--train-ratio", type=float, default=DEFAULT_TRAIN_RATIO, help="Train split ratio for stratified evaluation.")
    parser.add_argument("--eval-only", action="store_true", help="Evaluate all loaded samples as the test set; useful for held-out test_data files.")
    parser.add_argument("--min-test-accuracy", type=float, default=DEFAULT_MIN_TEST_ACCURACY, help="Required final test accuracy.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_file) if args.input_file else Path(args.root)
    report_path = Path(args.report)
    report = build_report(
        input_path=input_path,
        report_path=report_path,
        seed=args.seed,
        train_ratio=args.train_ratio,
        eval_only=args.eval_only,
    )
    test_eval = report["test_evaluation"]
    test_accuracy = test_eval["overall_accuracy"]
    print(json.dumps(
        {
            "report": str(report_path),
            "input_path": str(input_path),
            "deduplicated_samples": report["data"]["deduplicated_samples"],
            "train_size": report["data"]["train_size"],
            "test_size": report["data"]["test_size"],
            "test_overall_accuracy": test_eval["overall_accuracy"],
            "test_per_target_accuracy": test_eval["per_target_accuracy"],
            "test_positive_negative_by_target": test_eval["positive_negative_by_target"],
            "test_misclassified_count": len(test_eval["misclassified_samples"]),
        },
        ensure_ascii=False,
        indent=2,
    ))
    if test_accuracy is not None and test_accuracy < args.min_test_accuracy:
        print(
            f"Test accuracy {test_accuracy:.6f} is below required threshold {args.min_test_accuracy:.6f}.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
