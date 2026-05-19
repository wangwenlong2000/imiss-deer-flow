"""Query segmenter for SkillRouterMiddleware.

Performs rule-based task segmentation on user queries, splitting multi-scenario
requests into coarse-grained task segments and determining whether routing
should be skipped (e.g. for chit-chat).
"""

import logging
import re
import uuid

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# should_route
# ---------------------------------------------------------------------------

_SKIP_KEYWORDS = [
    "你好",
    "在吗",
    "谢谢",
    "你是谁",
    "介绍一下自己",
]

_SKIP_PATTERNS = [
    r"^ok\s*$",
    r"^thanks?\s*$",
    r"^嗯\s*$",
    r"^好的\s*$",
]


def is_obvious_chitchat(query: str, uploaded_files: list[dict] | None = None) -> bool:
    """Return True when a turn is clearly conversational and should not route."""
    if uploaded_files:
        return False
    if not query or not query.strip():
        return True

    text = query.strip()
    if text in _SKIP_KEYWORDS:
        return True

    return any(re.match(pat, text, re.IGNORECASE) for pat in _SKIP_PATTERNS)


def should_route(query: str, uploaded_files: list[dict] | None = None) -> bool:
    """Return True if *query* needs professional Skill routing.

    Skip obvious chit-chat.  Do NOT skip when:
    - there are uploaded files,
    - the query references "this file/table/data",
    - the query contains task intents like analyse, generate, process, etc.
    """
    if not query or not query.strip():
        return False

    text = query.strip()
    lower = text.lower()

    if is_obvious_chitchat(text):
        return False

    # --- signals that force routing ---

    # Uploaded files present
    if uploaded_files:
        return True

    # References to "this file/table/data"
    file_refs = ["这个文件", "这个表", "这个数据", "这份文件", "这份数据", "这份表"]
    for ref in file_refs:
        if ref in text:
            return True

    # Task-intent keywords
    task_intents = [
        "分析", "判断", "生成", "统计", "检索", "处理", "识别",
        "解析", "预测", "评估", "对比", "提取", "计算",
        "画图", "制图", "汇总", "总结", "查询", "查找",
        "合规", "风险", "异常", "安全",
    ]
    for kw in task_intents:
        if kw in text:
            return True

    return False


# ---------------------------------------------------------------------------
# Task segmentation
# ---------------------------------------------------------------------------

# Keyword patterns that hint at a specific scene.  Order matters: more specific
# patterns should appear first.
_SCENE_PATTERNS = [
    ("network_traffic", re.compile(
        r"pcap|pcapng|\.cap|流量|网络|异常通信|可疑域名|安全事件|"
        r"协议|DNS|HTTP|TLS|TCP|会话|流量分析",
        re.IGNORECASE,
    )),
    ("policy_regulation", re.compile(
        r"法规|政策|法律|合规|台账|条例|通知|整治|依据|"
        r"条款|条文|法律条文|合规风险|合规判断",
        re.IGNORECASE,
    )),
    ("data_analysis", re.compile(
        r"excel|csv|统计|图表|表格|数据清洗|表格解析|"
        r"画图|绘制|可视化",
        re.IGNORECASE,
    )),
]


def segment_query(query: str) -> list[dict]:
    """Split *query* into coarse-grained task segments.

    First version uses rule-based matching.  Returns a list of dicts with
    keys: ``segment_id``, ``text``, ``scene``, ``input_refs``.
    """
    text = query.strip()
    if not text:
        return []

    matched_scenes: list[str] = []
    for scene, pat in _SCENE_PATTERNS:
        if pat.search(text):
            matched_scenes.append(scene)

    # If no scene matched, return a single "unknown" segment
    if not matched_scenes:
        return [{
            "segment_id": _new_id(),
            "text": text,
            "scene": None,
            "input_refs": [],
        }]

    # Build one segment per matched scene.  Also extract relevant sub-texts
    # when possible; for v1, use the full query as segment text.
    segments: list[dict] = []
    for scene in matched_scenes:
        segments.append({
            "segment_id": _new_id(),
            "text": text,
            "scene": scene,
            "input_refs": [],
        })

    return segments


def _new_id() -> str:
    return f"seg_{uuid.uuid4().hex[:6]}"
