#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


DEFAULT_VIDEO_LIBRARY_INDEX = "citybrain-video-library"
DEFAULT_EMBEDDING_STORAGE_MODE = "in_place"


CAPABILITIES: dict[str, dict[str, Any]] = {
    "video_asset_retrieval": {
        "label": "视频资产检索",
        "skills": ["video-search"],
        "deliverables": ["视频命中列表", "命中片段", "匹配字段", "下一步分析建议"],
        # 不要在这里放“视频”“录像”“摄像头”“找”这类泛化词。它们几乎出现在每一条
        # 视频类请求里，会让本能力变成兜底赢家，把抽帧、目标检测、事件分析等请求
        # 一并吸走。检索类请求必须带明确的检索动作或视频库语境。
        "keywords": ["搜索", "检索", "调取", "查找", "有没有", "数据库", "视频库", "camera", "cam_"],
    },
    "semantic_video_retrieval": {
        "label": "语义视频检索",
        "skills": ["video-search"],
        "deliverables": ["关键词命中列表", "语义相似视频列表", "融合排序", "语义命中原因"],
        "keywords": ["语义", "streetmodel", "相似", "以图搜视频", "图片搜索视频", "这张图", "画面里", "傍晚", "下雨", "行人过马路", "交通拥堵", "变道", "可疑行为"],
    },
    "single_video_event_understanding": {
        "label": "单视频事件理解",
        "skills": ["single-video-event-analysis"],
        "deliverables": ["视觉时间线", "事件候选", "证据帧", "置信度", "人工复核状态"],
        "keywords": ["分析", "检测", "打架", "事故", "摔倒", "入侵", "烟雾", "火灾", "人群聚集", "发生了什么"],
    },
    "video_metadata_normalization": {
        "label": "视频元数据规范化",
        "skills": ["video-stream-ingestion"],
        "deliverables": ["video_session_id", "raw_segment_uri", "时长/分辨率/帧率", "编码信息", "标准输入 JSON"],
        "keywords": [
            "时长", "分辨率", "帧率", "fps", "编码", "码率", "元数据", "metadata",
            "标准输入", "规范化", "标准化", "视频信息", "基本信息", "ffprobe",
        ],
    },
    "object_statistics": {
        "label": "对象统计与趋势分析",
        "skills": ["object-statistics"],
        "deliverables": ["统计表", "分布/比例", "趋势摘要", "报告结论"],
        "keywords": ["统计", "多少", "分布", "比例", "车流量", "人流量", "峰值", "平均停留", "每个时间段"],
    },
    "object_detection": {
        "label": "视频目标检测",
        "skills": ["video-object-analytics"],
        "deliverables": ["逐帧目标数量", "目标类别", "检测结果"],
        # “边界框/bbox/逐帧检测”是目标检测独有的表述。不要加“置信度”——
        # 人工复核请求（“根据告警置信度分类”）同样含该词，会被误判成目标检测。
        "keywords": [
            "目标检测", "检测目标", "检测画面", "几个人", "几辆车", "有哪些人", "有哪些车",
            "主要目标", "对象检测", "识别目标", "人车目标", "边界框", "bbox", "检测每一帧", "逐帧检测",
        ],
    },
    "object_tracking": {
        "label": "视频目标跟踪",
        "skills": ["video-object-analytics"],
        "deliverables": ["track_id", "目标轨迹", "持续时间", "移动/静止状态"],
        "keywords": ["跟踪", "轨迹", "跨帧", "怎么移动", "移动路线", "目标运动", "追踪人员", "追踪车辆"],
    },
    "evidence_preservation": {
        "label": "证据固化",
        "skills": ["evidence-package-generation"],
        "deliverables": ["证据快照", "证据片段", "证据包 manifest", "完整性哈希"],
        "keywords": ["证据", "截图", "快照", "剪辑", "截取", "前后", "哈希", "打包", "导出", "隐私", "遮蔽", "模糊"],
    },
    "video_library_governance": {
        "label": "视频入库与索引管理",
        "skills": ["video-stream-ingestion", "batch-video-ingestion", "video-embedding-index"],
        "deliverables": ["入库状态", "video_id", "索引/嵌入状态", "失败列表"],
        "keywords": ["入库", "导入", "上传", "elasticsearch", "向量", "嵌入", "索引", "存储状态", "删除"],
    },
    "camera_health_operations": {
        "label": "摄像头健康运维",
        "skills": ["camera-health-check"],
        "deliverables": ["健康状态", "问题类型", "严重程度", "运维建议"],
        "keywords": ["健康", "在线", "画面质量", "模糊", "遮挡", "黑屏", "冻结", "离线"],
    },
    "cross_video_investigation": {
        "label": "跨视频深度研判",
        "skills": ["video-search", "object-tracking", "object-statistics"],
        "deliverables": ["对比结论", "关联线索", "风险说明", "能力缺口提示"],
        "keywords": ["对比", "两个路口", "追踪", "轨迹", "热力图", "不同摄像头", "同一时间", "跨摄像头"],
    },
    "development_test": {
        "label": "测试与开发",
        "skills": ["video-search", "video-embedding-index"],
        "deliverables": ["测试报告", "效果对比", "性能指标"],
        "keywords": ["测试", "benchmark", "准不准", "效果", "对比关键词", "开发", "调试"],
    },
    # 以下能力此前完全缺失，导致对应 skill 没有任何路由入口，请求只能落到
    # video_asset_retrieval / evidence_preservation 这类兜底能力上。
    "frame_sampling": {
        "label": "视频抽帧",
        "skills": ["frame-sampling"],
        "deliverables": ["帧列表", "frame_id/时间戳/图像尺寸", "帧图片文件"],
        "keywords": ["抽帧", "抽取一帧", "采样帧", "每隔", "每秒一帧", "提取帧", "帧序列", "结构化记录"],
    },
    "media_transcoding": {
        "label": "视频格式转换与剪切",
        "skills": ["ffmpeg-utils"],
        "deliverables": ["转码后的视频", "关键帧图片", "指定时间区间的片段"],
        "keywords": ["格式转换", "转换成统一格式", "转码", "关键帧", "统一格式"],
    },
    "event_deduplication": {
        "label": "重复事件合并",
        "skills": ["duplicate-event-merge"],
        "deliverables": ["合并后的事件列表", "合并依据", "重复项分组"],
        "keywords": ["合并重复", "重复事件", "去重", "相似告警", "重复告警", "合并依据", "相似度合并"],
    },
    "evidence_snapshot": {
        "label": "证据截图",
        "skills": ["evidence-snapshot"],
        "deliverables": ["证据图片", "evidence_id", "完整性哈希", "时间戳与摄像头编号"],
        "keywords": ["证据图片", "证据截图", "关键画面", "导出证据", "截取画面", "取证截图"],
    },
    "privacy_protection": {
        "label": "隐私脱敏",
        "skills": ["privacy-masking"],
        "deliverables": ["脱敏后的图片或视频", "打码区域说明", "脱敏方式"],
        "keywords": ["打码", "脱敏", "马赛克", "模糊处理", "隐私处理", "可以公开", "对外发布"],
    },
    "review_triage": {
        "label": "人工复核分流",
        "skills": ["human-review-routing"],
        "deliverables": ["复核队列分类", "分流理由", "优先级"],
        "keywords": ["人工复核", "自动通过", "优先复核", "复核分类", "分流", "复核队列"],
    },
    "roi_zone_statistics": {
        "label": "区域进出与停留统计",
        "skills": ["roi-transit-statistics"],
        "deliverables": ["各区域进入/离开/停留计数", "区域定义", "目标明细"],
        "keywords": ["进入", "离开", "停留", "区域统计", "划定区域", "标出区域", "roi"],
    },
}


SCENARIOS: dict[str, dict[str, Any]] = {
    "traffic_police_video_review": {
        "label": "交警视频调阅",
        "roles": ["交警", "交警队"],
        "skills": ["video-search"],
        "deliverables": ["相关监控录像", "可用时间段", "调阅结果说明"],
    },
    "property_security_review": {
        "label": "物业安防复核",
        "roles": ["物业", "小区", "园区", "安保"],
        "skills": ["video-search", "single-video-event-analysis", "human-review-routing"],
        "deliverables": ["可疑进入时间线", "证据帧", "人工复核建议"],
    },
    "urban_management_flow_statistics": {
        "label": "城管人流统计",
        "roles": ["城管", "商圈", "人流"],
        "skills": ["object-statistics", "video-search"],
        "deliverables": ["峰值时段", "人流趋势", "统计报告"],
    },
    "emergency_fire_risk_search": {
        "label": "应急火灾风险搜索",
        "roles": ["应急", "火灾", "烟雾", "消防"],
        "skills": ["video-search", "single-video-event-analysis", "evidence-package-generation"],
        "deliverables": ["火灾风险候选", "证据帧", "人工复核列表"],
    },
    "legal_evidence_package": {
        "label": "法庭证据准备",
        "roles": ["法庭", "法院", "司法", "诉讼", "哈希", "完整性"],
        "skills": ["evidence-package-generation"],
        "deliverables": ["证据包", "完整性哈希", "manifest", "隐私处理版本"],
    },
}


CHAIN_EXPANSIONS: dict[str, list[str]] = {
    "semantic_video_retrieval": ["video-search"],
    "single_video_event_understanding": ["single-video-event-analysis"],
    "video_metadata_normalization": ["video-stream-ingestion"],
    "evidence_preservation": ["evidence-package-generation"],
    "camera_health_operations": ["frame-sampling", "camera-health-check"],
}

DESTRUCTIVE_PATTERNS = ["删除", "清空", "重建", "覆盖", "purge", "delete", "drop"]
HUMAN_REVIEW_PATTERNS = ["打架", "事故", "摔倒", "入侵", "烟雾", "火灾", "人群聚集", "法庭", "执法", "应急"]
TIME_PATTERNS = [
    "今天",
    "昨天",
    "前天",
    "上午",
    "下午",
    "晚上",
    "昨晚",
    "过去",
    "最近",
    "本周",
    "本月",
]
VIDEO_SOURCE_PATTERNS = ["视频库", "索引", "监控点", "摄像头", "录像", "视频文件", "目录"]
# 越靠前 = 同分时越优先。专用能力必须排在 evidence_preservation /
# video_asset_retrieval 这类宽口径能力之前，否则又会被兜底能力吃掉。
CAPABILITY_PRIORITY = [
    "development_test",
    "camera_health_operations",
    "cross_video_investigation",
    "video_metadata_normalization",
    "event_deduplication",
    "review_triage",
    "privacy_protection",
    "media_transcoding",
    "frame_sampling",
    "roi_zone_statistics",
    "evidence_snapshot",
    "single_video_event_understanding",
    "object_tracking",
    "object_detection",
    "object_statistics",
    "evidence_preservation",
    "video_library_governance",
    "semantic_video_retrieval",
    "video_asset_retrieval",
]
CHAIN_ORDER = [
    "video-stream-ingestion",
    "batch-video-ingestion",
    "video-embedding-index",
    "video-search",
    "frame-sampling",
    "camera-health-check",
    "object-statistics",
    "object-tracking",
    "video-object-analytics",
    "roi-mapping",
    "roi-transit-statistics",
    "single-video-event-analysis",
    "evidence-snapshot",
    "video-segment-extraction",
    "evidence-package-generation",
    "privacy-masking",
    "video-privacy-masking",
    "human-review-routing",
]


def normalize(text: str) -> str:
    return text.casefold().strip()


def score_keywords(request: str, keywords: list[str]) -> int:
    """按关键词的具体程度加权打分。

    等权计数会让泛化词吃掉专用词：“请分析这段监控视频，找出异常事件”里，
    “视频”“找”这类通用词能给 video_asset_retrieval 攒出 2 分，而
    single_video_event_understanding 只靠“分析”得 1 分，结果事件分析被
    判成视频检索。按关键词长度加权后，“目标检测”这类专用词的权重自然
    高于“找”，泛化词不再能压过专用词。
    """
    req = strip_ambiguous_keywords(request)
    return sum(len(normalize(keyword)) for keyword in keywords if normalize(keyword) in req)


# “模糊”“遮挡”既可能描述画面质量，也可能出现在“模糊车牌”“文字被遮挡”这类
# OCR/身份类越界请求中。后者不应被算作摄像头健康信号，否则能力边界问题会被
# 路由到 camera_health_operations。
AMBIGUOUS_QUALITY_PHRASES = [
    "模糊车牌",
    "模糊的车牌",
    "车牌模糊",
    "模糊文字",
    "文字模糊",
]


def strip_ambiguous_keywords(request: str) -> str:
    req = normalize(request)
    for phrase in AMBIGUOUS_QUALITY_PHRASES:
        req = req.replace(normalize(phrase), "")
    return req


# “我上传了一段监控”只是在描述附件来源，不是要求把视频入库。
# 只有“入库/导入/上传到视频库”这类动作才应进入 video_library_governance。
UPLOAD_DESCRIPTION_PHRASES = [
    "我上传了",
    "我已上传",
    "已上传",
    "刚上传",
    "上传了一段",
    "上传了一个",
    "上传的视频",
    "上传的这段",
    "上传的录像",
]


def select_capability(request: str) -> str:
    request_without_existing_ingestion = request.replace("已入库", "")
    for phrase in UPLOAD_DESCRIPTION_PHRASES:
        request_without_existing_ingestion = request_without_existing_ingestion.replace(phrase, "")
    if any(word in request_without_existing_ingestion for word in ["入库", "导入", "上传"]):
        return "video_library_governance"
    if (
        any(word in request for word in ["测试", "benchmark", "准不准"])
        or ("效果" in request and any(word in request for word in ["StreetModel", "streetmodel", "嵌入", "检索", "搜索"]))
        or ("关键词搜索" in request and "向量搜索" in request)
    ):
        return "development_test"
    # Explicit object analytics must win over generic video/search/event words.
    # For example, “前两秒有几辆车” contains both “视频/画面” and “多少”;
    # it must not be routed to semantic retrieval or a business event review.
    if any(word in request for word in CAPABILITIES["object_tracking"]["keywords"]):
        return "object_tracking"
    if any(word in request for word in CAPABILITIES["object_detection"]["keywords"]):
        return "object_detection"
    scores = {name: score_keywords(request, data["keywords"]) for name, data in CAPABILITIES.items()}
    if re.search(r"\.(mp4|mov|avi|mkv|webm)\b", request, flags=re.IGNORECASE) and scores["single_video_event_understanding"] > 0:
        scores["single_video_event_understanding"] += 2
    if scores["semantic_video_retrieval"] > 0 and any(word in request for word in ["找", "搜索", "检索", "有没有"]):
        scores["semantic_video_retrieval"] += 2
    if scores["camera_health_operations"] > 0:
        scores["camera_health_operations"] += 1
    if scores["cross_video_investigation"] > 0:
        scores["cross_video_investigation"] += 1
    winner, score = max(scores.items(), key=lambda item: (item[1], -CAPABILITY_PRIORITY.index(item[0])))
    if score <= 0:
        return "video_asset_retrieval"
    # 单个本地视频的计数请求要走 video-object-analytics，而不是依赖视频库索引的
    # object-statistics；否则在没有索引记录时只会拿到空统计或索引错误。
    if winner == "object_statistics" and is_single_local_video_request(request):
        return "object_detection"
    return winner


SINGLE_VIDEO_PATTERNS = ["这段视频", "这个视频", "本段视频", "本视频", "该视频", "上传的视频", "这段录像", "这个录像"]


def is_single_local_video_request(request: str) -> bool:
    req = normalize(request)
    if not any(normalize(pattern) in req for pattern in SINGLE_VIDEO_PATTERNS) and not has_video_path(request):
        return False
    return not has_camera_id(request) and not has_time_range(request)


def select_scenario(request: str, capability: str) -> str:
    scenario_scores = {name: score_keywords(request, data["roles"]) for name, data in SCENARIOS.items()}
    winner, score = max(scenario_scores.items(), key=lambda item: item[1])
    if score > 0:
        return winner
    if capability == "evidence_preservation":
        return "legal_evidence_package" if any(word in request for word in ["法庭", "法院", "完整性", "哈希"]) else "generic_evidence_workflow"
    if capability == "object_statistics":
        return "urban_management_flow_statistics" if any(word in request for word in ["城管", "商圈", "人流"]) else "generic_statistics_workflow"
    if capability == "camera_health_operations":
        return "camera_health_ops"
    return "generic_video_intelligence"


def unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def sort_chain(chain: list[str]) -> list[str]:
    deduped = unique(chain)
    order = {skill: index for index, skill in enumerate(CHAIN_ORDER)}
    return sorted(deduped, key=lambda skill: order.get(skill, len(CHAIN_ORDER)))


def has_video_path(request: str) -> bool:
    return bool(re.search(r"\.(mp4|mov|avi|mkv|webm)\b", request, flags=re.IGNORECASE))


def has_camera_id(request: str) -> bool:
    return bool(re.search(r"\bCAM[_A-Z0-9-]*\b", request, flags=re.IGNORECASE))


def extract_camera_id(request: str) -> str | None:
    match = re.search(r"\bCAM[_A-Z0-9-]*\b", request, flags=re.IGNORECASE)
    return match.group(0) if match else None


def has_video_source_hint(request: str) -> bool:
    return has_video_path(request) or has_camera_id(request) or any(pattern in request for pattern in VIDEO_SOURCE_PATTERNS)


def has_time_range(request: str) -> bool:
    if any(pattern in request for pattern in TIME_PATTERNS):
        return True
    return bool(re.search(r"\d{1,2}[:：]\d{2}|\d{1,2}\s*[-至到]\s*\d{1,2}\s*点|\d{4}[-/年]\d{1,2}[-/月]\d{1,2}", request))


def has_specific_place(request: str) -> bool:
    if any(word in request for word in ["这个商圈", "该商圈", "这个路口", "这个区域", "小区门口"]):
        return False
    return any(word in request for word in ["区", "路", "街", "口", "商圈", "小区", "园区", "广场", "市场"])


def missing_fields(request: str, capability: str, scenario: str) -> list[str]:
    fields: list[str] = []
    video_source_known = has_video_source_hint(request)

    if scenario == "property_security_review":
        if not has_specific_place(request):
            fields.append("小区名称或摄像头 ID")
        if not video_source_known:
            fields.append("视频来源")
    elif scenario == "emergency_fire_risk_search":
        if not video_source_known:
            fields.append("视频库索引或监控点范围")
        if not has_time_range(request):
            fields.append("搜索时间范围")
    elif scenario == "urban_management_flow_statistics":
        if not has_specific_place(request):
            fields.append("商圈名称或区域")
        if not has_time_range(request):
            fields.append("统计时间范围")
        if not video_source_known:
            fields.append("视频来源")
    elif capability == "cross_video_investigation":
        if not has_time_range(request):
            fields.append("时间范围")
        if not video_source_known:
            fields.append("摄像头范围或视频来源")
        fields.append("可疑人员定义")
    elif capability == "development_test":
        if not video_source_known:
            fields.append("测试视频库索引或视频文件路径")
    elif capability == "video_library_governance":
        if not has_video_path(request) and not re.search(r"(/[\\w._-]+)+/?", request):
            fields.append("视频文件路径或目录")
    elif capability == "camera_health_operations":
        if not video_source_known:
            fields.append("摄像头 ID 或视频文件路径")
        elif has_camera_id(request) and not has_video_path(request):
            camera_id = extract_camera_id(request)
            fields.append(f"摄像头 {camera_id} 对应的视频文件路径" if camera_id else "摄像头对应的视频来源")
    elif capability == "object_statistics":
        if not has_time_range(request):
            fields.append("统计时间范围")
        if not video_source_known and not has_specific_place(request):
            fields.append("视频来源或检索条件")
    elif capability in {"object_detection", "object_tracking"}:
        if not has_video_path(request):
            fields.append("视频文件路径")
    elif capability == "evidence_preservation":
        if not has_video_path(request):
            fields.append("视频文件路径")
        if not has_time_range(request):
            fields.append("事件时间或片段范围")
    elif capability == "video_metadata_normalization":
        if not video_source_known and not has_video_path(request):
            fields.append("视频文件路径")
    elif capability in {"frame_sampling", "media_transcoding", "evidence_snapshot", "privacy_protection", "roi_zone_statistics"}:
        if not video_source_known and not has_video_path(request):
            fields.append("视频文件路径")
        if capability == "roi_zone_statistics" and not has_specific_place(request):
            fields.append("区域范围定义")
    elif capability in {"event_deduplication", "review_triage"}:
        # 这两类的输入是上游产出的事件/告警 JSON，不是视频文件本身。
        if not re.search(r"\.json\b", request, flags=re.IGNORECASE):
            fields.append("事件或告警列表 JSON")

    return unique(fields)


def ingestion_input_question() -> str:
    return """请按以下格式提供视频入库信息；每次入库都会从视频抽取代表帧并自动生成图片向量，把 StreetModel 向量原位写入同一条源视频文档，不依赖视频共享挂载目录；向量失败时源文档仍会保留，不需要提供 vector 数组，只填写已知字段即可：
{
  "index": "citybrain-video-library",
  "target_index": "citybrain-video-library",
  "embedding_storage_mode": "in_place",
  "videos": [
    {
      "file_path": "/path/to/video.mp4",
      "video_id": "可选的唯一视频ID",
      "camera_id": "可选的摄像头ID",
      "started_at": "可选，ISO 8601 时间",
      "location": {"city": "可选", "district": "可选", "address": "可选"},
      "description": "可选的视频描述",
      "tags": ["可选标签"]
    }
  ]
}
也可以只回复视频文件路径或目录。"""


def follow_up_question(fields: list[str], request: str = "", capability: str = "") -> str | None:
    if not fields:
        return None
    if capability == "video_library_governance" and "视频文件路径或目录" in fields:
        return ingestion_input_question()
    if len(fields) == 1:
        return f"请问{fields[0]}是什么？".replace(" ID是什么", " ID 是什么")
    return f"请问{'、'.join(fields[:-1])}和{fields[-1]}是什么？".replace(" ID是什么", " ID 是什么")


EVENT_INTENT_WORDS = [
    "事故", "打架", "摔倒", "入侵", "烟雾", "火灾", "人群聚集", "拥堵",
    "异常", "事件", "发生了什么", "可疑",
]


def has_event_intent(request: str) -> bool:
    req = normalize(request)
    return any(normalize(word) in req for word in EVENT_INTENT_WORDS)


def recommended_chain(capability: str, scenario: str, request: str) -> list[str]:
    chain: list[str] = []
    if scenario in SCENARIOS:
        chain.extend(SCENARIOS[scenario]["skills"])
    elif capability in CAPABILITIES:
        chain.extend(CHAIN_EXPANSIONS.get(capability, CAPABILITIES[capability]["skills"]))

    if capability == "semantic_video_retrieval":
        chain = ["video-search"]
    if capability in {"object_detection", "object_tracking"}:
        chain = ["video-object-analytics"]
    if capability == "evidence_preservation" and has_video_path(request) and has_time_range(request):
        chain = ["evidence-package-generation"]
    # 复合请求：先看画面质量、再判断有没有事件。健康检查不能替代事件语义分析，
    # 事件判断必须走唯一入口 single-video-event-analysis。
    if capability == "camera_health_operations" and has_event_intent(request):
        chain.append("single-video-event-analysis")
    if capability == "privacy_protection" or any(word in request for word in ["隐私", "车牌", "人脸", "遮蔽", "模糊人脸", "模糊车牌"]):
        # 要"脱敏视频"就得输出视频：privacy-masking 只处理单张证据图片，
        # 拿它交差会让用户拿到一张图而不是能对外发布的视频。
        wants_video = any(word in normalize(request) for word in ["脱敏视频", "打码视频", "视频脱敏", "公开使用的视频", "整段视频", "视频打码"]) or (
            capability == "privacy_protection" and "视频" in request and "图片" not in request
        )
        if wants_video:
            chain = [skill for skill in chain if skill != "privacy-masking"]
            chain.append("video-privacy-masking")
        else:
            chain.append("privacy-masking")
    if any(word in request for word in ["人工", "复核"]):
        chain.append("human-review-routing")
    if any(word in request for word in ["片段", "截取", "剪辑"]) and not (capability == "evidence_preservation" and has_video_path(request) and has_time_range(request)):
        chain.append("video-segment-extraction")
    if any(word in request for word in ["先", "然后", "最后", "→", "->"]):
        if "入库" in request:
            chain.extend(["video-stream-ingestion", "batch-video-ingestion"])
        if any(word in request for word in ["向量", "嵌入", "语义"]):
            chain.append("video-embedding-index")
        if any(word in request for word in ["搜索", "检索", "相似"]):
            chain.append("video-search")
        if "统计" in request:
            chain.append("object-statistics")
        if any(word in request for word in ["事件", "检测"]):
            chain.append("single-video-event-analysis")
        if any(word in request for word in ["证据", "打包"]):
            chain.append("evidence-package-generation")
    return sort_chain(chain)


def build_steps(chain: list[str]) -> list[dict[str, str]]:
    labels = {
        "video-search": "Load video-search. Use image-vector retrieval for image input; for text input use keyword matching plus LLM-optimized text-to-video vector retrieval and fuse both result lists.",
        "single-video-event-analysis": "Load single-video-event-analysis and inspect frames for visible event evidence.",
        "evidence-package-generation": "Load evidence-package-generation and run it once. For a local video path plus HH:MM:SS/MM:SS offset, pass --raw-segment-uri and --event-elapsed-seconds directly; summarize package_id, manifest_uri, manifest_hash, and artifact_summary, then stop.",
        "privacy-masking": "Load privacy-masking when explicit sensitive regions or privacy config are available.",
        "object-statistics": "Load object-statistics and aggregate counts, distributions, or trends.",
        "video-stream-ingestion": "Load video-stream-ingestion to normalize local video files; do not pre-probe with ls or ffprobe.",
        "batch-video-ingestion": "Load batch-video-ingestion and run metadata-only ingestion, preserving user-provided camera, time, location, description, and tags; do not run object detection unless explicitly requested.",
        "video-embedding-index": "Load video-embedding-index, extract representative frames locally, submit them as inline images to StreetModel, aggregate the image vectors, and enrich the source video document in place. Do not require a shared video mount. Run once and report any StreetModel failure without probing service health.",
        "camera-health-check": "Load camera-health-check after frames are available.",
        "frame-sampling": "Load frame-sampling to create stable frame records from video.",
        "human-review-routing": "Load human-review-routing to decide manual review status.",
        "object-tracking": "Load object-tracking for track-level object analytics.",
        "video-object-analytics": "Call the structured video_object_analytics tool. Do not call bash or compose downstream CLI commands. Use operation=detect for object counts and operation=track for cross-frame trajectories.",
    }
    if not chain:
        return [{"step": "1", "skill": "video-search", "action": "Start with indexed video retrieval, then refine using the result."}]
    return [{"step": str(index), "skill": skill, "action": labels.get(skill, f"Load {skill} and follow its SKILL.md.")} for index, skill in enumerate(chain, start=1)]


MASKING_INTENT_WORDS = ["打码", "脱敏", "马赛克", "模糊处理", "遮蔽", "隐私处理", "可以公开", "对外发布"]


def has_masking_intent(request: str) -> bool:
    req = normalize(request)
    return any(normalize(word) in req for word in MASKING_INTENT_WORDS)


def capability_gap(request: str, capability: str) -> dict[str, Any] | None:
    lowered = normalize(request)
    gaps = []
    # 打码请求里的“人脸”“车牌”指的是要遮蔽的区域，不是要识别的身份或文字。
    # 遮蔽一个区域不需要认出这是谁、车牌号是多少，因此不能据此报能力缺口，
    # 否则会对着一个明明支持的请求回答“做不到”。
    masking = has_masking_intent(request) or capability == "privacy_protection"
    if any(word in lowered for word in ["实时", "rtsp", "gb28181", "直播", "实时流"]):
        gaps.append("Real-time stream connection is not covered by current local-video skills.")
    if not masking and any(word in lowered for word in ["同一人", "人脸", "身份", "认人", "是谁"]):
        gaps.append("Person identity recognition is not provided by current skills; detection boxes carry no identity.")
    if "热力图" in lowered:
        gaps.append("Heatmap visualization is only partially supported unless aggregated location bins already exist.")
    if not masking and any(word in lowered for word in ["ocr", "文字识别", "车牌"]):
        gaps.append("OCR or license-plate recognition is not provided by current skills.")
    if capability == "cross_video_investigation" and not gaps:
        gaps.append("Cross-video investigation is partially supported by retrieval, tracking, and statistics; reliable identity correlation may need additional business confirmation.")
    if not gaps:
        return None
    return {"supported": "partial", "gaps": gaps, "next_action": "Return the supported plan first and ask before implementing new capability."}


def build_plan(request: str, role: str | None = None) -> dict[str, Any]:
    combined_request = f"{role or ''} {request}".strip()
    capability = select_capability(combined_request)
    embedding_attempt_required = capability == "video_library_governance"
    scenario = select_scenario(combined_request, capability)
    chain = recommended_chain(capability, scenario, combined_request)
    capability_data = CAPABILITIES.get(capability, CAPABILITIES["video_asset_retrieval"])
    scenario_data = SCENARIOS.get(scenario, {})
    mode = "development_test" if capability == "development_test" else "business_operation"
    requires_confirmation = any(normalize(pattern) in normalize(combined_request) for pattern in DESTRUCTIVE_PATTERNS)
    requires_human_review = any(pattern in combined_request for pattern in HUMAN_REVIEW_PATTERNS)
    deliverables = unique(list(scenario_data.get("deliverables", [])) + list(capability_data["deliverables"]))
    if capability == "video_library_governance":
        deliverables = ["入库状态", "video_id", "统一视频索引", "嵌入状态", "失败列表"]
    missing = missing_fields(combined_request, capability, scenario)
    guardrails = [
        "Do not write ad hoc code for this business request.",
        "Load each downstream skill's SKILL.md before executing its script.",
        "Return capability_gap instead of implementing missing functionality.",
        "If follow_up_question is present, stop immediately and make the whole visible response exactly that question.",
        "If a downstream video skill fails, summarize the concrete failure and stop instead of debugging services.",
    ]
    if requires_confirmation:
        guardrails.append("Ask for explicit confirmation before destructive index or video-library operations.")
    if requires_human_review:
        guardrails.append("Include evidence references and human-review status for high-risk or enforcement-sensitive conclusions.")
    if capability == "evidence_preservation" and has_video_path(combined_request) and has_time_range(combined_request):
        guardrails.append("For local video evidence packages, call evidence-package-generation once with direct CLI fields and stop after summarizing the returned package. Do not list, read, hash, or present generated files after success.")
    if capability == "video_library_governance":
        guardrails.append("Use metadata-only source ingestion and preserve provided business metadata. Do not run object detection unless explicitly requested.")
        guardrails.append("Every ingestion must attempt an in-place StreetModel sampled-frame image enrichment of the same source-index document after insertion, without requiring a shared video mount.")
        guardrails.append("Use a partial Elasticsearch document update for in-place enrichment. Never replace the complete source document with a compact vector document.")
        guardrails.append("If embedding fails after source ingestion succeeds, keep the source document, do not roll it back, and report overall_status=partial_success with source_ingestion_status=success, vector_status=failed, and the concrete vector failure.")
        guardrails.append("After embedding succeeds or returns a concrete StreetModel failure, summarize and stop. Do not probe Elasticsearch or StreetModel health unless debugging was requested.")

    return {
        "skill": "city-video-intelligence",
        "version": "1.0.0",
        "status": "success",
        "request": request,
        "role": role,
        "mode": mode,
        "capability": capability,
        "capability_label": capability_data["label"],
        "business_scenario": scenario,
        "business_scenario_label": scenario_data.get("label", scenario),
        "recommended_skill_chain": chain,
        "execution_plan": build_steps(chain),
        "expected_deliverables": deliverables,
        "embedding_attempt_required": embedding_attempt_required,
        "embedding_failure_policy": "keep_source_and_report_partial_success" if embedding_attempt_required else None,
        "source_index": DEFAULT_VIDEO_LIBRARY_INDEX if embedding_attempt_required else None,
        "embedding_target_index": DEFAULT_VIDEO_LIBRARY_INDEX if embedding_attempt_required else None,
        "embedding_storage_mode": DEFAULT_EMBEDDING_STORAGE_MODE if embedding_attempt_required else None,
        "missing_fields": missing,
        "follow_up_question": follow_up_question(missing, combined_request, capability),
        "guardrails": guardrails,
        "requires_confirmation": requires_confirmation,
        "requires_human_review": requires_human_review,
        "capability_gap": capability_gap(combined_request, capability),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Plan city video intelligence business skill orchestration.")
    parser.add_argument("--request", required=True, help="User's natural-language video intelligence request.")
    parser.add_argument("--role", default=None, help="Optional business role, such as traffic police or property security.")
    parser.add_argument("--output", default=None, help="Path to write JSON result.")
    parser.add_argument("--format", choices=["json"], default="json")
    args = parser.parse_args()

    result = build_plan(args.request, role=args.role)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
