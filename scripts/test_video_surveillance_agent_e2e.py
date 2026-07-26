#!/usr/bin/env python3
"""视频监控 skill 的真实 Agent 端到端测试（Layer C）。

自包含：自己创建 assistant / thread、上传视频、发起 run、解析 SSE，不依赖其他测试脚本。

与 `test_video_surveillance_skills.py` 的区别：

  test_video_surveillance_skills.py   直接调用 skill 脚本，验证脚本本身的契约
  本脚本                                自然语言 -> lead_agent -> skill，验证 Agent 真的走了目标链路

断言只基于 SSE 中的**真实证据**：

  - `tool_calls`：模型实际发起的工具调用（不是场景过滤器注入的候选 skill 列表）
  - `tool` 结果消息：skill 实际返回的 JSON，用来确认 skill 真的执行并返回了契约字段
  - 最终回答文本：用来确认没有编造结论

用法：

    python3 scripts/test_video_surveillance_agent_e2e.py                    # 全部用例
    python3 scripts/test_video_surveillance_agent_e2e.py --cases E01 E05    # 指定用例
    python3 scripts/test_video_surveillance_agent_e2e.py --list             # 只列用例
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import mimetypes
import re
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent


# --------------------------------------------------------------------------
# 用例定义
#
# 每个用例对应一条已修复或需要守住的链路，断言字段含义：
#   expect_skills          必须在真实工具调用中出现的 skill
#   expect_skills_any      必须至少命中其中一个 skill
#   forbid_skills          不允许出现的 skill
#   expect_tools           必须调用的工具名（如结构化工具 video_object_analytics）
#   expect_analytics_ops   video_object_analytics 的 operation
#   expect_result_fields   skill 返回 JSON 中必须出现的字段（证明 skill 真的执行过）
#   expect_result_text     工具结果中必须出现的字符串（如 error_code）
#   forbid_raw_video_bash  禁止用 bash 手写 ffmpeg/ffprobe/yolo 替代 skill
#   expect_answer_any      最终回答必须命中其中至少一个关键词
#   forbid_answer          最终回答不允许出现的字符串（防编造）
#   allow_no_tool_calls    是否允许零工具调用（纯咨询类问题）
# --------------------------------------------------------------------------
CASES: list[dict[str, Any]] = [
    {
        "id": "E01",
        "skills": ["video-stream-ingestion"],
        "covers": "4.3 + 4.6 视频元数据路由与字段补齐",
        "question": "请读取我上传的这个视频的时长、分辨率、帧率和编码信息，整理成后续分析可以直接使用的标准输入。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills": ["video-stream-ingestion"],
        "expect_result_fields": ["video_session_id", "fps", "codec", "video_duration_seconds"],
        "forbid_raw_video_bash": True,
        "expect_answer_any": ["900", "14分", "15分", "1280"],
    },
    {
        "id": "E02",
        "skills": ["city-video-intelligence", "single-video-event-analysis", "camera-health-check"],
        "covers": "4.7 上传描述 + 画面质量 + 事件复合链路",
        "question": "我上传了一段路口监控，请检查画面质量，判断有没有事故、拥堵或其他异常，并给出时间线和证据。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills": ["city-video-intelligence"],
        "expect_skills_any": ["single-video-event-analysis", "camera-health-check"],
        "forbid_skills": ["video-embedding-index", "batch-video-ingestion"],
        "forbid_raw_video_bash": True,
    },
    {
        "id": "E03",
        "skills": ["video-object-analytics", "object-detection"],
        "covers": "4.4 单视频计数走 video-object-analytics 而非 ES 统计",
        "question": "统计这段视频里出现了多少辆车、多少个行人。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_tools": ["video_object_analytics"],
        "forbid_skills": ["object-statistics"],
        "forbid_raw_video_bash": True,
    },
    {
        "id": "E04",
        "skills": ["city-video-intelligence"],
        "covers": "4.2 + 4.8 能力边界与 capability_gap",
        "question": "我想确认视频里具体人员的身份、读取模糊车牌、生成全市热力图并接入实时 RTSP，"
                    "但当前只有这段本地视频。请说明哪些能力无法由现有数据支持，不要编造结果。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_answer_any": ["无法", "不支持", "不具备", "缺少"],
        "forbid_answer": ["身份已确认", "车牌号为"],
        "allow_no_tool_calls": True,
    },
    {
        "id": "E05",
        "skills": ["video-stream-ingestion"],
        "covers": "4.1 失败契约：视频不存在时必须报错而不是编造事件",
        "question": "请分析 /mnt/user-data/uploads/does-not-exist-camera-42.mp4 这段监控里有没有打架或摔倒，并给出时间线。",
        "expect_answer_any": ["不存在", "未找到", "找不到", "无法访问", "SOURCE_NOT_FOUND", "没有找到"],
        "forbid_answer": ["检测到打架", "发生了打架", "检测到摔倒", "确认存在"],
        "allow_no_tool_calls": True,
    },
    {
        "id": "E06",
        "skills": ["evidence-package-generation"],
        "covers": "证据包缺少关键输入时只做一次合并追问，不编造 artifact",
        "question": "请把仓库事件的截图、前后 20 秒片段、哈希和打码副本整理成证据包。",
        "expect_clarification": True,
        "expect_answer_any": ["请问", "需要", "提供", "哪个", "路径", "在哪"],
        "forbid_answer": ["证据包已生成", "sha256:", "已打包完成"],
        "allow_no_tool_calls": True,
    },
    {
        "id": "E07",
        "skills": ["human-review-routing"],
        "covers": "人工复核规则咨询走 human-review-routing",
        "question": "事件置信度低、摄像头画面有遮挡或事件涉及执法时，哪些结果必须进入人工复核？",
        "expect_skills": ["human-review-routing"],
    },
    {
        "id": "E08",
        "skills": ["video-object-analytics", "object-tracking"],
        "covers": "目标检测与跟踪走结构化工具，且不输出事件结论",
        "question": "检测这段视频前 10 秒里的行人和车辆，并跟踪它们是移动还是停留。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_tools": ["video_object_analytics"],
        "forbid_raw_video_bash": True,
        "forbid_answer": ["发生了打架", "存在交通事故", "确认拥堵事件"],
    },
    {
        "id": "E09",
        "skills": ["frame-sampling"],
        "covers": "抽帧：只准备审查素材，不下事件结论",
        "question": "请从这段视频里每隔 5 秒抽一帧，只准备后续审查用的素材，不要判断有没有事件。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["frame-sampling", "city-video-intelligence", "analyze-video"],
        "forbid_answer": ["发生了打架", "存在交通事故", "确认拥堵"],
    },
    {
        "id": "E10",
        "skills": ["city-video-intelligence", "single-video-event-analysis"],
        "covers": "单视频事件语义分析唯一入口",
        "question": "请逐段审查这段监控画面，给出可见证据的时间线和事件候选；证据不足时明确标记需要人工复核。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["single-video-event-analysis", "city-video-intelligence"],
        "forbid_raw_video_bash": True,
    },
    {
        "id": "E11",
        "skills": ["camera-health-check"],
        "covers": "摄像头健康检查",
        "question": "这个摄像头的画面是不是有模糊、遮挡或者黑屏的问题？请给出健康状态和评分。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["camera-health-check", "city-video-intelligence"],
        "forbid_raw_video_bash": True,
    },
    {
        "id": "E12",
        "skills": ["evidence-snapshot", "ffmpeg-utils"],
        "covers": "证据截图与完整性哈希",
        "question": "请把这段视频第 5 秒的画面导出成一张证据截图，并给出这张图的哈希值。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["evidence-snapshot", "ffmpeg-utils", "city-video-intelligence"],
        "expect_answer_any": ["哈希", "sha256", "SHA-256", "md5"],
    },
    {
        "id": "E13",
        "skills": ["video-segment-extraction"],
        "covers": "事件片段截取",
        "question": "以第 15 秒为事件时间，截取前后各 5 秒的视频片段用于留证。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["video-segment-extraction", "ffmpeg-utils", "city-video-intelligence"],
    },
    {
        "id": "E14",
        "skills": ["privacy-masking"],
        "covers": "隐私打码",
        "question": "这段视频要对外提供，请先把画面里的人脸和车牌做打码处理，再给我处理后的图片。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["privacy-masking", "city-video-intelligence"],
    },
    {
        "id": "E15",
        "skills": ["evidence-package-generation"],
        "covers": "输入齐全时生成证据包",
        "question": "事件时间是第 12 秒，事件类型是交通拥堵。请基于我上传的这段视频生成完整证据包："
                    "包含证据截图、前后各 5 秒片段、完整性哈希和打码副本。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["evidence-package-generation", "evidence-snapshot", "city-video-intelligence"],
    },
    {
        "id": "E16",
        "skills": ["roi-mapping"],
        "covers": "ROI 区域判定",
        "question": "请判断画面中检测到的车辆有没有进入这个区域：多边形顶点是 (300,200)、(900,200)、(900,600)、(300,600)。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["roi-mapping", "video-object-analytics", "city-video-intelligence"],
    },
    {
        "id": "E17",
        "skills": ["duplicate-event-merge"],
        "covers": "事件去重合并",
        "question": "我这里有三条事件候选，请合并成唯一事件列表：\n"
                    "1) CAM_1 交通拥堵 ROI_A 置信度0.6 10:00:00-10:00:10\n"
                    "2) CAM_1 交通拥堵 ROI_A 置信度0.9 10:00:05-10:00:20\n"
                    "3) CAM_2 交通拥堵 ROI_A 置信度0.7\n"
                    "第 1 条和第 2 条是同一摄像头同一区域的重复事件。",
        "expect_skills_any": ["duplicate-event-merge", "city-video-intelligence"],
        "expect_answer_any": ["合并", "去重", "唯一"],
    },
    {
        "id": "E18",
        "skills": ["video-search"],
        "covers": "ES 不可用时如实报告，不伪造检索命中",
        "question": "找出摄像头 CAM_008 昨天 18 点到 20 点之间出现公交车的所有录像，并按时间排序。",
        "expect_skills_any": ["video-search", "city-video-intelligence"],
        "expect_answer_any": ["无法", "不可用", "未入库", "没有", "失败", "连接", "索引"],
        "forbid_answer": ["找到 3 段", "共检索到", "命中 5 条"],
    },
    {
        "id": "E19",
        "skills": ["object-statistics"],
        "covers": "视频库统计在 ES 不可用时如实报告",
        "question": "统计上周整个视频库里各时段的车流量趋势，给出分布和峰值。",
        # ES 不可用时，"如实报告不可用"和"先追问澄清"都是可接受的诚实回应；
        # 不可接受的是编造统计数字。路由是否进入视频编排作为观察项记录，不作硬断言。
        "expect_answer_any": ["无法", "不可用", "没有", "失败", "缺少", "索引",
                             "请问", "确认", "哪一周", "范围"],
        "forbid_answer": ["峰值出现在 18", "车流量为 1200", "统计结果如下：\n| 时段"],
    },
    {
        "id": "E20",
        "skills": ["video-embedding-index", "batch-video-ingestion"],
        "covers": "StreetModel 不可用时入库与向量索引如实报告",
        "question": "把我上传的这个视频登记入库，并建立可以语义搜索的向量索引。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["video-embedding-index", "batch-video-ingestion",
                              "video-stream-ingestion", "city-video-intelligence"],
        "expect_answer_any": ["无法", "不可用", "失败", "未启用", "连接", "服务"],
    },
    {
        "id": "E21",
        "skills": ["batch-video-ingestion"],
        "covers": "批量登记只记元数据，不做内容检测",
        "question": "把 /mnt/datasets/Vedio-demo 这个目录里的视频批量登记到视频库，只登记元数据，不要做内容检测。",
        "expect_skills_any": ["batch-video-ingestion", "video-stream-ingestion", "city-video-intelligence"],
        # 没有真正调用入库 skill 却宣称"登记已完成"，属于伪造执行结果
        "forbid_answer": ["检测到行人", "检测到车辆 46", "批量登记已完成", "登记完成"],
    },
    {
        "id": "E22",
        "skills": ["analyze-video"],
        "covers": "长录像按场景切章节并抽关键帧",
        "question": "请按画面场景变化，把这段录像切成若干章节，并为每个章节抽一张关键帧，只准备素材不下结论。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["analyze-video", "frame-sampling", "single-video-event-analysis",
                              "city-video-intelligence"],
        "forbid_answer": ["存在交通事故", "确认拥堵"],
    },
    {
        "id": "E23",
        "skills": ["ffmpeg-utils"],
        "covers": "原子 ffmpeg 操作走 skill 而不是手写命令",
        "question": "请导出这段视频 00:00:08 位置的一张关键帧图片。",
        "video_file": "Vedio-demo/Trafic-30s.mp4",
        "expect_skills_any": ["ffmpeg-utils", "evidence-snapshot", "frame-sampling", "city-video-intelligence"],
    },
]


# --------------------------------------------------------------------------
# HTTP / SSE 基础设施
# --------------------------------------------------------------------------
def post_json(url: str, payload: dict[str, Any], timeout: int) -> tuple[int, str]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def upload_file(url: str, file_path: Path, timeout: int) -> tuple[int, str]:
    boundary = f"----deerflow-e2e-{uuid.uuid4().hex}"
    content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    header = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="files"; filename="{file_path.name}"\r\n'
        f"Content-Type: {content_type}\r\n\r\n"
    ).encode("utf-8")
    body = header + file_path.read_bytes() + f"\r\n--{boundary}--\r\n".encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(len(body)),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def parse_sse(raw: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    name: str | None = None
    lines: list[str] = []

    def flush() -> None:
        nonlocal name, lines
        if not lines:
            return
        text = "\n".join(lines)
        try:
            payload: Any = json.loads(text)
        except json.JSONDecodeError:
            payload = text
        events.append({"event": name, "data": payload})
        name, lines = None, []

    for line in raw.splitlines():
        if line.startswith("event:"):
            name = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            lines.append(line.split(":", 1)[1].lstrip())
        elif not line.strip():
            flush()
    flush()
    return events


def content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(content_text(item) for item in content)
    if isinstance(content, dict):
        return str(content.get("text") or content.get("content") or "")
    return ""


def create_assistant(api: str, stamp: str, model: str, timeout: int) -> tuple[str, str]:
    status, body = post_json(
        f"{api.rstrip('/')}/assistants",
        {
            "graph_id": "lead_agent",
            "name": f"video-surveillance-e2e-{stamp}",
            "config": {"configurable": {"model_name": model, "thinking_enabled": False}},
            "metadata": {"purpose": "video-surveillance-agent-e2e", "history_preserved": True},
        },
        timeout,
    )
    if status >= 300:
        raise RuntimeError(f"assistant create HTTP {status}: {body[:400]}")
    return str(json.loads(body)["assistant_id"]), body


# --------------------------------------------------------------------------
# SSE 证据提取
# --------------------------------------------------------------------------
SKILL_RE = re.compile(
    r"\b(city-video-intelligence|single-video-event-analysis|video-stream-ingestion|frame-sampling|"
    r"object-detection|object-tracking|roi-mapping|evidence-snapshot|video-segment-extraction|"
    r"privacy-masking|human-review-routing|camera-health-check|duplicate-event-merge|video-search|"
    r"batch-video-ingestion|object-statistics|evidence-package-generation|video-embedding-index|"
    r"ffmpeg-utils|analyze-video)\b"
)
RAW_VIDEO_CMD_RE = re.compile(r"\b(ffmpeg|ffprobe|yolo)\b", re.IGNORECASE)


def extract_evidence(raw: str) -> dict[str, Any]:
    calls: list[dict[str, Any]] = []
    tool_results: list[dict[str, Any]] = []
    ai_texts: list[str] = []
    models: set[str] = set()
    errors: list[Any] = []
    seen_calls: set[str] = set()
    seen_results: set[str] = set()

    for event in parse_sse(raw):
        data = event.get("data")
        if event.get("event") == "error":
            errors.append(data)
        if not isinstance(data, dict):
            continue
        for message in data.get("messages", []):
            if not isinstance(message, dict):
                continue
            mtype = message.get("type") or message.get("role")
            metadata = message.get("response_metadata") or {}
            for key in ("model_name", "model"):
                if isinstance(metadata.get(key), str):
                    models.add(metadata[key])

            if mtype in {"ai", "assistant"}:
                text = content_text(message.get("content"))
                if text:
                    ai_texts.append(text)
                raw_calls = message.get("tool_calls") or message.get("additional_kwargs", {}).get("tool_calls") or []
                for call in raw_calls if isinstance(raw_calls, list) else []:
                    if not isinstance(call, dict):
                        continue
                    name = call.get("name") or call.get("function", {}).get("name")
                    args = call.get("args") or call.get("function", {}).get("arguments") or {}
                    if isinstance(args, str):
                        try:
                            args = json.loads(args)
                        except json.JSONDecodeError:
                            args = {"_raw": args}
                    key = json.dumps([name, args], ensure_ascii=False, sort_keys=True)[:1000]
                    if key not in seen_calls:
                        seen_calls.add(key)
                        calls.append({"name": name, "args": args})

            elif mtype == "tool":
                content = message.get("content")
                text = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
                key = f"{message.get('name')}::{text[:400]}"
                if key not in seen_results:
                    seen_results.add(key)
                    tool_results.append({
                        "name": message.get("name"),
                        "status": message.get("status"),
                        "content": text,
                    })

    call_blob = json.dumps(calls, ensure_ascii=False)
    skills_touched = set(SKILL_RE.findall(call_blob))
    # invoke_skill 的 skill_name 参数是最权威的证据
    for call in calls:
        if call["name"] == "invoke_skill" and isinstance(call["args"], dict):
            name = call["args"].get("skill_name")
            if name:
                skills_touched.add(str(name))
    # 结构化工具等价于 video-object-analytics 能力（文档 §5.3）
    if any(call["name"] == "video_object_analytics" for call in calls):
        skills_touched.add("video-object-analytics")

    raw_video_bash = []
    for call in calls:
        if call["name"] not in {"bash", "bash_tool"}:
            continue
        args_text = json.dumps(call["args"], ensure_ascii=False)
        # 通过 bash 执行 skill 自己的 run.py 是正常执行方式，不算绕过
        if RAW_VIDEO_CMD_RE.search(args_text) and "/mnt/skills/" not in args_text:
            raw_video_bash.append(call)

    # 追问可能是通过 ask_clarification 工具发出的，不在最终文本里，
    # 因此"回答面"要把追问内容一并算上，否则会把正确的追问误判为没回答。
    clarifications = [
        str(call["args"].get("question") or call["args"].get("context") or "")
        for call in calls
        if call["name"] in {"ask_clarification", "ask_user", "request_clarification"}
        and isinstance(call["args"], dict)
    ]
    final_answer = ai_texts[-1] if ai_texts else ""

    return {
        "tool_calls": calls,
        "tool_call_names": sorted({str(c["name"]) for c in calls}),
        "tool_results": tool_results,
        "clarifications": clarifications,
        "answer_surface": "\n".join([final_answer, *clarifications]),
        "skills_touched": sorted(skills_touched),
        "analytics_ops": sorted({
            str(c["args"].get("operation"))
            for c in calls
            if c["name"] == "video_object_analytics" and isinstance(c["args"], dict) and c["args"].get("operation")
        }),
        "raw_video_bash": raw_video_bash,
        "result_blob": "\n".join(item["content"] for item in tool_results),
        "final_answer": ai_texts[-1] if ai_texts else "",
        "models": sorted(models),
        "errors": errors,
        "recursion_limit_hit": "Recursion limit" in json.dumps(errors, ensure_ascii=False)
        or "GraphRecursionError" in json.dumps(errors, ensure_ascii=False),
    }


def check_case(case: dict[str, Any], ev: dict[str, Any]) -> tuple[str, list[str]]:
    failures: list[str] = []
    touched = set(ev["skills_touched"])

    if not ev["final_answer"] and not ev["clarifications"]:
        failures.append("既没有最终回答，也没有发出追问")
    if ev["recursion_limit_hit"]:
        failures.append("触发 LangGraph 递归上限")
    if ev["errors"] and not ev["recursion_limit_hit"]:
        failures.append(f"运行出现错误事件: {json.dumps(ev['errors'][0], ensure_ascii=False)[:160]}")

    for skill in case.get("expect_skills", []):
        if skill not in touched:
            failures.append(f"未调用期望的 skill `{skill}`（实际: {sorted(touched) or '无'}）")
    any_skills = case.get("expect_skills_any")
    if any_skills and not (touched & set(any_skills)):
        failures.append(f"期望至少调用其中之一 {any_skills}，实际: {sorted(touched) or '无'}")
    for skill in case.get("forbid_skills", []):
        if skill in touched:
            failures.append(f"不应调用 `{skill}`")
    for tool in case.get("expect_tools", []):
        if tool not in ev["tool_call_names"]:
            failures.append(f"未调用期望的工具 `{tool}`（实际: {ev['tool_call_names'] or '无'}）")
    for op in case.get("expect_analytics_ops", []):
        if op not in ev["analytics_ops"]:
            failures.append(f"video_object_analytics 缺少 operation=`{op}`")

    for field in case.get("expect_result_fields", []):
        if field not in ev["result_blob"]:
            failures.append(f"skill 返回中缺少字段 `{field}`（说明 skill 未真正执行或契约不符）")
    for text in case.get("expect_result_text", []):
        if text not in ev["result_blob"]:
            failures.append(f"工具结果中缺少 `{text}`")

    if case.get("forbid_raw_video_bash") and ev["raw_video_bash"]:
        commands = [json.dumps(c["args"], ensure_ascii=False)[:120] for c in ev["raw_video_bash"]]
        failures.append(f"绕过 skill 手写视频命令: {commands[:2]}")

    # 追问也是有效回答面：agent 用 ask_clarification 提问时内容不在最终文本里
    answer = ev["answer_surface"]
    expect_any = case.get("expect_answer_any")
    if expect_any and not any(word in answer for word in expect_any):
        failures.append(f"最终回答未命中任一关键词 {expect_any}")
    for word in case.get("forbid_answer", []):
        if word in answer:
            failures.append(f"最终回答出现了不应出现的结论「{word}」")

    if case.get("expect_clarification") and not ev["clarifications"]:
        failures.append("缺少输入时应发起追问，实际没有")

    if not case.get("allow_no_tool_calls") and not ev["tool_calls"]:
        failures.append("没有任何工具调用")

    return ("pass" if not failures else "fail"), failures


# --------------------------------------------------------------------------
# 用例执行
# --------------------------------------------------------------------------
def prepare_thread(args: Any, case: dict[str, Any], case_dir: Path,
                   dataset_root: Path) -> tuple[str, dict[str, Any] | None]:
    status, body = post_json(
        f"{args.api.rstrip('/')}/threads",
        {"metadata": {"purpose": "video-surveillance-agent-e2e", "case_id": case["id"],
                      "title": case["question"][:80], "history_preserved": True}},
        args.timeout,
    )
    (case_dir / "thread-create-response.json").write_text(body, encoding="utf-8")
    if status >= 300:
        raise RuntimeError(f"thread create HTTP {status}: {body[:300]}")
    thread_id = json.loads(body)["thread_id"]

    uploaded_file = None
    video_file = case.get("video_file")
    if video_file:
        host_video = (dataset_root / video_file).resolve()
        if not host_video.is_file():
            raise RuntimeError(f"视频不存在: {host_video}")
        up_status, up_body = upload_file(
            f"{args.gateway.rstrip('/')}/api/threads/{thread_id}/uploads", host_video, args.timeout)
        (case_dir / "upload-response.json").write_text(up_body, encoding="utf-8")
        if up_status >= 300:
            raise RuntimeError(f"upload HTTP {up_status}: {up_body[:300]}")
        info = (json.loads(up_body).get("files") or [None])[0]
        if not info:
            raise RuntimeError("上传响应里没有文件")
        uploaded_file = {
            "filename": info["filename"],
            "size": int(info["size"]),
            "path": info.get("virtual_path", f"/mnt/user-data/uploads/{info['filename']}"),
            "status": "uploaded",
        }
    return thread_id, uploaded_file


def stream_run(args: Any, assistant_id: str, case: dict[str, Any], thread_id: str,
               uploaded_file: dict[str, Any] | None, case_dir: Path) -> str:
    message: dict[str, Any] = {"role": "user", "content": case["question"]}
    if uploaded_file:
        message["additional_kwargs"] = {"files": [uploaded_file]}
    payload = {
        "assistant_id": assistant_id,
        "input": {"messages": [message]},
        # 新架构只传 context.thread_id，不要同时传 config.configurable
        "context": {"thread_id": thread_id},
        "config": {"recursion_limit": args.recursion_limit},
        "stream_mode": ["values"],
    }
    request = urllib.request.Request(
        f"{args.api.rstrip('/')}/threads/{thread_id}/runs/stream",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=args.timeout) as response:
        raw = response.read().decode("utf-8", errors="replace")
    (case_dir / "turn-1.sse").write_text(raw, encoding="utf-8")
    return raw


def write_report(summary: dict[str, Any], out_root: Path) -> None:
    lines = [
        "# 视频监控 Agent 端到端测试结果",
        "",
        f"模型：`{summary['model']}`　assistant：`{summary['assistant_id']}`　"
        f"recursion_limit：`{summary['recursion_limit']}`",
        "",
        "| 用例 | 覆盖的修复 | 结果 | 实际触达 skill | 工具 | 失败原因 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in summary["results"]:
        ev = item["evidence"]
        reason = "；".join(item["failures"])[:180].replace("|", "/") or (item["error"] or "")
        lines.append(
            f"| {item['id']} | {item['covers']} | {item['status']} | "
            f"{', '.join(ev.get('skills_touched') or []) or '无'} | "
            f"{', '.join(ev.get('tool_call_names') or []) or '无'} | {reason} |"
        )
    lines += ["", "## Skill 覆盖矩阵", "",
              "「用例覆盖」= 有用例针对该 skill；「实际触达」= Agent 在某个用例里真的调用了它。", "",
              "| skill | 用例覆盖 | 实际触达 | 相关用例 |", "| --- | --- | --- | --- |"]
    bundle = REPO_ROOT / "skills" / "custom" / "video_surveillance"
    all_skills = sorted(d.parent.name for d in bundle.glob("*/router_card.json"))
    touched_all: set[str] = set()
    for item in summary["results"]:
        touched_all |= set(item["evidence"].get("skills_touched") or [])
    for skill in all_skills:
        planned = [i["id"] for i in summary["results"] if skill in (i.get("skills") or [])]
        reached = [i["id"] for i in summary["results"]
                   if skill in (i["evidence"].get("skills_touched") or [])]
        lines.append(
            f"| `{skill}` | {'✓ ' + ', '.join(planned) if planned else '—'} | "
            f"{'✓ ' + ', '.join(reached) if reached else '✗'} | {', '.join(sorted(set(planned + reached))) or '—'} |")
    covered = sum(1 for s in all_skills if any(s in (i.get('skills') or []) for i in summary['results']))
    lines += ["", f"用例覆盖 {covered}/{len(all_skills)} 个 skill；Agent 实际触达 "
                  f"{len([s for s in all_skills if s in touched_all])}/{len(all_skills)} 个。", ""]
    lines += [
        "每个用例目录下保留 `turn-1.sse`（原始流）、`tool-calls.json`（真实调用与返回）、"
        "`agent_final.md`（最终回答）与 `validation.json`（判定明细）。",
    ]
    (out_root / "REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api", default="http://127.0.0.1:3538/api/langgraph")
    parser.add_argument("--gateway", default="http://127.0.0.1:3538")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--dataset-root", default=str(REPO_ROOT / "datasets"))
    parser.add_argument("--output-root",
                        default=str(REPO_ROOT / "outputs" / "skill-tests" / "video-surveillance-agent-e2e"))
    parser.add_argument("--model", default="qwen3.6-35b-a3b")
    parser.add_argument("--recursion-limit", type=int, default=100)
    parser.add_argument("--cases", nargs="*", default=None)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.list:
        for case in CASES:
            print(f"{case['id']}  {case['covers']}")
            print(f"      {case['question'][:70]}")
        return 0

    selected = [c for c in CASES if not args.cases or c["id"] in args.cases]
    if not selected:
        print(f"没有匹配的用例: {args.cases}")
        return 2

    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_root = Path(args.output_root) / stamp
    out_root.mkdir(parents=True, exist_ok=True)
    dataset_root = Path(args.dataset_root)
    print(f"结果目录: {out_root}\n模型: {args.model}\n")

    assistant_id, assistant_body = create_assistant(args.api, stamp, args.model, args.timeout)
    (out_root / "assistant-create-response.json").write_text(assistant_body, encoding="utf-8")
    print(f"assistant: {assistant_id}\n")

    results: list[dict[str, Any]] = []
    for index, case in enumerate(selected, start=1):
        print(f"[{index}/{len(selected)}] {case['id']} — {case['covers']}")
        case_dir = out_root / case["id"]
        case_dir.mkdir(parents=True, exist_ok=True)
        started = dt.datetime.now()
        thread_id: str | None = None
        uploaded_file: dict[str, Any] | None = None
        ev: dict[str, Any] = {}
        error: str | None = None

        try:
            thread_id, uploaded_file = prepare_thread(args, case, case_dir, dataset_root)
            raw = stream_run(args, assistant_id, case, thread_id, uploaded_file, case_dir)
            ev = extract_evidence(raw)
            status, failures = check_case(case, ev)
        except Exception as exc:  # 单个用例失败不影响整批
            status, failures, error = "error", [], repr(exc)

        elapsed = (dt.datetime.now() - started).total_seconds()
        record = {
            "id": case["id"],
            "covers": case["covers"],
            "skills": case.get("skills", []),
            "question": case["question"],
            "thread_id": thread_id,
            "uploaded_file": uploaded_file,
            "status": status,
            "failures": failures,
            "error": error,
            "elapsed_seconds": round(elapsed, 1),
            "evidence": {
                "tool_call_names": ev.get("tool_call_names", []),
                "skills_touched": ev.get("skills_touched", []),
                "analytics_ops": ev.get("analytics_ops", []),
                "raw_video_bash_count": len(ev.get("raw_video_bash", [])),
                "models": ev.get("models", []),
                "recursion_limit_hit": ev.get("recursion_limit_hit"),
                "final_answer_chars": len(ev.get("final_answer", "")),
                "clarifications": ev.get("clarifications", []),
            },
        }
        results.append(record)
        (case_dir / "validation.json").write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        if ev.get("final_answer"):
            (case_dir / "agent_final.md").write_text(ev["final_answer"], encoding="utf-8")
        if ev.get("tool_calls"):
            (case_dir / "tool-calls.json").write_text(
                json.dumps({"calls": ev["tool_calls"], "results": ev.get("tool_results", [])},
                           ensure_ascii=False, indent=2), encoding="utf-8")

        icon = {"pass": "PASS", "fail": "FAIL", "error": "ERR "}[status]
        print(f"   [{icon}] {elapsed:.0f}s  工具: {record['evidence']['tool_call_names'] or '无'}  "
              f"skill: {record['evidence']['skills_touched'] or '无'}")
        for failure in failures:
            print(f"          - {failure}")
        if error:
            print(f"          - {error}")

    summary = {
        "assistant_id": assistant_id,
        "model": args.model,
        "recursion_limit": args.recursion_limit,
        "counts": {s: sum(1 for r in results if r["status"] == s) for s in ("pass", "fail", "error")},
        "results": results,
    }
    (out_root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(summary, out_root)

    counts = summary["counts"]
    print(f"\n=== 汇总 === pass={counts['pass']} fail={counts['fail']} error={counts['error']}")
    print(f"报告: {out_root / 'REPORT.md'}")
    return 1 if counts["fail"] or counts["error"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
