#!/usr/bin/env python3
"""Video-surveillance skill test suite (StreetModel / Elasticsearch bypassed).

参照 docs/deerflow-agent-skill-test.md 的三层结构，本套件覆盖两层可离线执行的测试：

  layer=plan  业务编排与原子 skill 的纯 Python 契约测试（宿主机执行，仅依赖标准库）
  layer=exec  真实视频端到端执行（在 AioSandbox 镜像内执行，依赖 ffmpeg/OpenCV/Ultralytics）

StreetModel 向量服务与 Elasticsearch 未启用，因此本套件不执行 video-search /
video-embedding-index / object-statistics 的在线检索路径，只在编排计划层面验证
它们是否被正确推荐，并把执行结果标记为 skipped。

用法：

    python3 scripts/test_video_surveillance_skills.py --layer all
    python3 scripts/test_video_surveillance_skills.py --layer plan
    python3 scripts/test_video_surveillance_skills.py --layer exec

`--layer exec-inner` 是容器内部入口，不要在宿主机直接调用。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
BUNDLE_REL = "skills/custom/video_surveillance"


# --------------------------------------------------------------------------
# 测试框架
# --------------------------------------------------------------------------
class Suite:
    def __init__(self, name: str, out_dir: Path) -> None:
        self.name = name
        self.out_dir = out_dir
        self.results: list[dict[str, Any]] = []
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def case(self, case_id: str, title: str, fn: Callable[[], dict[str, Any]]) -> None:
        started = dt.datetime.now()
        try:
            detail = fn() or {}
            status = detail.pop("_status", "pass")
            message = detail.pop("_message", "")
        except AssertionError as exc:
            status, message, detail = "fail", str(exc), {}
        except Exception as exc:  # noqa: BLE001
            status, message, detail = "error", f"{type(exc).__name__}: {exc}", {}
        elapsed = (dt.datetime.now() - started).total_seconds()
        record = {
            "id": case_id,
            "title": title,
            "status": status,
            "message": message,
            "elapsed_seconds": round(elapsed, 2),
            "detail": detail,
        }
        self.results.append(record)
        icon = {"pass": "PASS", "fail": "FAIL", "error": "ERR ", "skip": "SKIP"}[status]
        print(f"[{icon}] {case_id} {title} ({elapsed:.1f}s)")
        if message:
            print(f"       -> {message}")

    def summary(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for item in self.results:
            counts[item["status"]] = counts.get(item["status"], 0) + 1
        return {"suite": self.name, "counts": counts, "results": self.results}


def run_cmd(cmd: list[str], cwd: Path | None = None, timeout: int = 600) -> dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return {
        "cmd": cmd,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def run_skill(
    skill: str,
    args: list[str],
    *,
    root: Path,
    expect_returncode: int | None = 0,
    timeout: int = 600,
) -> dict[str, Any]:
    """执行某个 skill 的 run.py 并解析其 JSON 结果契约。"""
    script = root / BUNDLE_REL / skill / "scripts" / "run.py"
    assert script.exists(), f"skill 脚本不存在: {script}"
    proc = run_cmd([sys.executable, str(script), *args], cwd=root, timeout=timeout)
    if expect_returncode is not None:
        assert proc["returncode"] == expect_returncode, (
            f"{skill} 退出码 {proc['returncode']} != {expect_returncode}; stderr={proc['stderr'][-500:]}"
        )
    stdout = proc["stdout"].strip()
    assert stdout, f"{skill} 没有输出 JSON; stderr={proc['stderr'][-500:]}"
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        # 部分脚本会先打印进度行，取最后一个 JSON 对象
        start = stdout.find("{")
        payload = json.loads(stdout[start:])
    return payload


def write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# Layer A：编排与契约（宿主机，仅标准库）
# --------------------------------------------------------------------------
def plan(request: str, role: str | None = None) -> dict[str, Any]:
    args = ["--request", request, "--format", "json"]
    if role:
        args += ["--role", role]
    return run_skill("city-video-intelligence", args, root=REPO_ROOT)


def layer_plan(out_dir: Path) -> dict[str, Any]:
    suite = Suite("plan", out_dir)
    bundle = REPO_ROOT / BUNDLE_REL
    config = str(bundle / "configs" / "deerflow_config.json")

    # ---- A1 单视频事件业务编排（对应文档 T01）
    def a1() -> dict[str, Any]:
        result = plan("请分析 /mnt/datasets/Vedio-demo/Trafic.mp4 是否存在打架、摔倒或交通事故，并给出可见证据时间线")
        write_json(out_dir / "A1-plan-event.json", result)
        assert result["status"] == "success", result
        assert result["capability"] == "single_video_event_understanding", result["capability"]
        assert result["recommended_skill_chain"] == ["single-video-event-analysis"], result["recommended_skill_chain"]
        assert result["requires_human_review"] is True, "高风险事件必须要求人工复核"
        return {k: result[k] for k in ("capability", "recommended_skill_chain", "requires_human_review")}

    suite.case("A1", "单视频事件请求 -> single-video-event-analysis 且要求人工复核", a1)

    # ---- A2 交警视频调阅（对应文档 T02）
    def a2() -> dict[str, Any]:
        result = plan("我是交警，需要调取大成路路口今天上午 8-10 点的监控。")
        write_json(out_dir / "A2-plan-traffic-police.json", result)
        assert result["status"] == "success", result
        assert result["business_scenario"] == "traffic_police_video_review", result["business_scenario"]
        assert result["capability"] == "video_asset_retrieval", result["capability"]
        assert "video-search" in result["recommended_skill_chain"], result["recommended_skill_chain"]
        assert "single-video-event-analysis" not in result["recommended_skill_chain"], (
            "调阅请求不应直接进入事件分析"
        )
        return {k: result[k] for k in ("business_scenario", "capability", "recommended_skill_chain")}

    suite.case("A2", "交警调阅请求 -> video-search，不误入事件分析", a2)

    # ---- A3 证据包缺少关键输入（对应文档 T03）
    def a3() -> dict[str, Any]:
        result = plan("请把仓库事件的截图、前后 20 秒片段、哈希和打码副本整理成证据包。")
        write_json(out_dir / "A3-plan-evidence-missing.json", result)
        assert result["status"] == "success", result
        assert result["capability"] in {"evidence_preservation", "legal_evidence_package"}, result["capability"]
        assert result.get("follow_up_question"), "缺少输入时必须返回追问"
        assert result["follow_up_question"].count("？") <= 1, "只能提出一个合并后的追问"
        return {
            "capability": result["capability"],
            "missing_fields": result.get("missing_fields"),
            "follow_up_question": result["follow_up_question"],
        }

    suite.case("A3", "证据包缺少视频路径/事件时间 -> 单一合并追问", a3)

    # ---- A4 语义检索请求：计划层可用，执行层因 StreetModel 未启用而跳过
    def a4() -> dict[str, Any]:
        result = plan("帮我找出画面里像是傍晚下雨天行人过马路的相似视频。")
        write_json(out_dir / "A4-plan-semantic-retrieval.json", result)
        assert result["status"] == "success", result
        assert result["capability"] == "semantic_video_retrieval", result["capability"]
        assert "video-search" in result["recommended_skill_chain"], result["recommended_skill_chain"]
        return {
            "_status": "pass",
            "_message": "计划正确；StreetModel/ES 未启用，检索执行不在本次范围",
            "capability": result["capability"],
            "recommended_skill_chain": result["recommended_skill_chain"],
            "embedding_attempt_required": result.get("embedding_attempt_required"),
        }

    suite.case("A4", "语义检索请求 -> video-search（仅计划，执行依赖 StreetModel）", a4)

    # ---- A5 能力边界（对应文档 C-040）
    def a5() -> dict[str, Any]:
        result = plan(
            "我想确认具体人员身份、读取模糊车牌、生成全市热力图并接入实时 RTSP，"
            "但当前数据只有本地视频和检测框。请说明哪些能力无法由现有数据支持。"
        )
        write_json(out_dir / "A5-plan-capability-gap.json", result)
        assert result["status"] == "success", result
        gap = result.get("capability_gap")
        assert isinstance(gap, dict) and gap.get("gaps"), f"越界请求必须返回 capability_gap: {gap}"
        blob = json.dumps(gap, ensure_ascii=False).lower()
        for label, needles in (
            ("实时流/RTSP", ("real-time", "stream")),
            ("人员身份识别", ("identity",)),
            ("全市热力图", ("heatmap",)),
            ("车牌/OCR", ("plate", "ocr")),
        ):
            assert any(n in blob for n in needles), f"gaps 未声明越界项「{label}」: {gap['gaps']}"
        assert result["capability"] != "camera_health_operations", (
            "「模糊车牌」不应被当成画面质量问题而路由到摄像头健康运维"
        )
        return {
            "capability": result["capability"],
            "business_scenario": result["business_scenario"],
            "gaps": gap["gaps"],
        }

    suite.case("A5", "越界能力请求 -> capability_gap 覆盖全部 4 项越界能力", a5)

    # ---- A5b 摄像头健康路由未因 A5 的修复而回归
    def a5b() -> dict[str, Any]:
        cases = {
            "摄像头 CAM_008 画面模糊、有遮挡，请检查一下健康状态": "camera_health_operations",
            "这个路口的监控是不是黑屏了，画面质量怎么样": "camera_health_operations",
        }
        actual = {}
        for request, expected in cases.items():
            result = plan(request)
            actual[request] = result["capability"]
            assert result["capability"] == expected, f"{request} -> {result['capability']}，期望 {expected}"
            assert "camera-health-check" in result["recommended_skill_chain"], result["recommended_skill_chain"]
        return actual

    suite.case("A5b", "真实画面质量问题仍路由到摄像头健康运维（回归）", a5b)

    # ---- A1b 「我上传了一段监控 + 画面质量 + 事件」复合请求（Layer C C-001）
    def a1b() -> dict[str, Any]:
        result = plan("我上传了一段路口监控，请检查画面质量，判断有没有事故、拥堵或其他异常，并给出时间线和证据。")
        write_json(out_dir / "A1b-plan-upload-compound.json", result)
        assert result["status"] == "success", result
        assert result["capability"] != "video_library_governance", (
            "「我上传了一段监控」是在描述附件来源，不是入库请求"
        )
        chain = result["recommended_skill_chain"]
        assert "video-embedding-index" not in chain, f"不应触发向量索引（依赖 StreetModel）: {chain}"
        assert "camera-health-check" in chain, f"用户明确问了画面质量: {chain}"
        assert "single-video-event-analysis" in chain, (
            f"事件判断必须走唯一入口 single-video-event-analysis，健康检查不能替代: {chain}"
        )
        assert result["requires_human_review"] is True, "事故/拥堵属于高风险事件"
        return {"capability": result["capability"], "recommended_skill_chain": chain}

    suite.case("A1b", "上传描述 + 画面质量 + 事件 -> 健康检查与事件分析同时入链", a1b)

    # ---- A1c 真正的入库请求仍走 video_library_governance
    def a1c() -> dict[str, Any]:
        result = plan("把这个视频目录批量导入视频库，只登记元数据，不做内容检测。")
        write_json(out_dir / "A1c-plan-ingestion.json", result)
        assert result["capability"] == "video_library_governance", result["capability"]
        assert "batch-video-ingestion" in result["recommended_skill_chain"], result["recommended_skill_chain"]
        return {"capability": result["capability"], "recommended_skill_chain": result["recommended_skill_chain"]}

    suite.case("A1c", "真实入库请求仍路由到 video_library_governance（回归）", a1c)

    # ---- A5c 视频元数据请求必须落到 video-stream-ingestion（文档 §6 建议 3）
    def a5c() -> dict[str, Any]:
        result = plan("请读取这个本地视频的时长、分辨率、帧率和时间信息，整理成后续分析可以使用的标准输入。")
        write_json(out_dir / "A5c-plan-metadata.json", result)
        assert result["status"] == "success", result
        assert result["capability"] == "video_metadata_normalization", result["capability"]
        assert result["recommended_skill_chain"] == ["video-stream-ingestion"], result["recommended_skill_chain"]
        assert "single-video-event-analysis" not in result["recommended_skill_chain"], (
            "纯元数据请求不应触发事件分析"
        )
        return {"capability": result["capability"], "recommended_skill_chain": result["recommended_skill_chain"]}

    suite.case("A5c", "视频元数据请求 -> video-stream-ingestion，不误入事件分析", a5c)

    # ---- A5d 单视频计数走 video-object-analytics，视频库统计仍走 object-statistics
    def a5d() -> dict[str, Any]:
        single = plan("统计这段视频里出现了多少辆车")
        library = plan("统计 CAM_008 昨天各时段的车流量趋势")
        write_json(out_dir / "A5d-plan-counting.json", {"single_video": single, "library": library})
        assert single["recommended_skill_chain"] == ["video-object-analytics"], single["recommended_skill_chain"]
        assert library["recommended_skill_chain"] == ["object-statistics"], library["recommended_skill_chain"]
        return {
            "单视频计数": single["capability"],
            "视频库统计": library["capability"],
        }

    suite.case("A5d", "单视频计数 -> video-object-analytics；视频库统计 -> object-statistics", a5d)

    # ---- A6 本地视频输入失败契约（对应文档 4.2）
    def a6() -> dict[str, Any]:
        result = run_skill(
            "video-stream-ingestion",
            [
                "--video", "/tmp/city-brain-video-does-not-exist.mp4",
                "--camera-id", "CAM_TEST",
                "--source-type", "local_file",
                "--capture-seconds", "1",
                "--config", config,
            ],
            root=REPO_ROOT,
            expect_returncode=1,
        )
        write_json(out_dir / "A6-ingestion-not-found.json", result)
        assert result["status"] == "failed", result
        assert result["error_code"] == "SOURCE_NOT_FOUND", result
        assert "duration" not in json.dumps(result), "不得为不存在的视频伪造元数据"
        return {"error_code": result["error_code"], "retryable": result.get("retryable")}

    suite.case("A6", "不存在的本地视频 -> SOURCE_NOT_FOUND 且非零退出码", a6)

    # ---- A7 事件去重：空输入仍返回标准成功 JSON
    def a7_empty() -> dict[str, Any]:
        result = run_skill("duplicate-event-merge", [], root=REPO_ROOT)
        assert result["status"] == "success", result
        assert result["data"]["events"] == [] and result["data"]["duplicates"] == [], result["data"]
        return {"events": 0}

    suite.case("A7", "事件去重空输入 -> 标准成功 JSON", a7_empty)

    # ---- A8 事件去重：真实重复合并
    def a8() -> dict[str, Any]:
        events = {
            "events": [
                {"event_id": "E1", "camera_id": "CAM_1", "event_type": "traffic_congestion",
                 "roi_id": "ROI_A", "related_tracks": ["T1"], "confidence": 0.6,
                 "start_time": "2026-05-20T10:00:00+08:00", "end_time": "2026-05-20T10:00:10+08:00"},
                {"event_id": "E2", "camera_id": "CAM_1", "event_type": "traffic_congestion",
                 "roi_id": "ROI_A", "related_tracks": ["T1"], "confidence": 0.9,
                 "start_time": "2026-05-20T10:00:05+08:00", "end_time": "2026-05-20T10:00:20+08:00"},
                {"event_id": "E3", "camera_id": "CAM_2", "event_type": "traffic_congestion",
                 "roi_id": "ROI_A", "related_tracks": ["T9"], "confidence": 0.7},
            ]
        }
        path = write_json(out_dir / "A8-events-input.json", events)
        result = run_skill("duplicate-event-merge", ["--events-json", str(path)], root=REPO_ROOT)
        write_json(out_dir / "A8-dedup.json", result)
        data = result["data"]
        assert len(data["events"]) == 2, data["events"]
        merged = next(e for e in data["events"] if e["event_id"] == "E1")
        assert merged["confidence"] == 0.9, merged
        assert merged["end_time"] == "2026-05-20T10:00:20+08:00", merged
        assert data["duplicates"] == [{"main_event_id": "E1", "merged_event_id": "E2"}], data["duplicates"]
        return {"merged_events": len(data["events"]), "duplicates": data["duplicates"]}

    suite.case("A8", "同摄像头同 ROI 重复事件 -> 合并并取高置信度与时间并集", a8)

    # ---- A9 ROI 几何：点在多边形内/外
    def a9() -> dict[str, Any]:
        rois = {"rois": [{"id": "ROI_GATE", "type": "restricted_area",
                          "polygon": [[0, 0], [100, 0], [100, 100], [0, 100]]}]}
        tracks = {"tracks": [
            {"track_id": "T_IN", "last_bbox": [40, 40, 60, 80]},    # bottom_center=(50,80) 内
            {"track_id": "T_OUT", "last_bbox": [200, 200, 260, 280]},  # (230,280) 外
        ]}
        roi_path = write_json(out_dir / "A9-rois.json", rois)
        track_path = write_json(out_dir / "A9-tracks.json", tracks)
        result = run_skill(
            "roi-mapping",
            ["--tracks-json", str(track_path), "--rois-json", str(roi_path),
             "--position-strategy", "bottom_center"],
            root=REPO_ROOT,
        )
        write_json(out_dir / "A9-roi-mapping.json", result)
        matches = {m["object_id"]: m["matched"] for m in result["data"]["matches"]}
        assert matches.get("T_IN") is True, matches
        assert matches.get("T_OUT") is False, matches
        return {"matches": matches}

    suite.case("A9", "ROI 几何 -> 多边形内命中、多边形外不命中", a9)

    # ---- A10 人工复核契约
    def a10() -> dict[str, Any]:
        low_conf = write_json(out_dir / "A10-event-low.json", {
            "event": {"event_id": "E_LOW", "event_type": "fight", "confidence": 0.2}
        })
        degraded = write_json(out_dir / "A10-health-degraded.json", {"health_status": "degraded"})
        result = run_skill(
            "human-review-routing",
            ["--event-json", str(low_conf), "--camera-health-json", str(degraded), "--config", config],
            root=REPO_ROOT,
        )
        write_json(out_dir / "A10-review-low.json", result)
        data = result["data"]
        assert data["queue"] == "manual_review", data
        assert data["review_required"] is True, data
        assert "confidence_below_threshold" in data["reasons"], data["reasons"]
        assert "camera_health_degraded" in data["reasons"], data["reasons"]

        high_conf = write_json(out_dir / "A10-event-high.json", {
            "event": {"event_id": "E_OK", "event_type": "traffic_congestion", "confidence": 0.99}
        })
        ok = run_skill(
            "human-review-routing",
            ["--event-json", str(high_conf), "--config", config],
            root=REPO_ROOT,
        )
        write_json(out_dir / "A10-review-high.json", ok)
        assert ok["data"]["queue"] == "auto_pass", ok["data"]
        return {"low_confidence": data["reasons"], "high_confidence_queue": ok["data"]["queue"]}

    suite.case("A10", "人工复核契约 -> 低置信/画面退化进 manual_review，高置信 auto_pass", a10)

    # ---- A11 Router Card 基础校验
    def a11() -> dict[str, Any]:
        cards = sorted((REPO_ROOT / "skills").rglob("router_card.json"))
        ids: list[str] = []
        problems: list[str] = []
        for path in cards:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                problems.append(f"{path}: JSON 解析失败 {exc}")
                continue
            identity = data.get("identity", {})
            if not identity.get("id") or not identity.get("name"):
                problems.append(f"{path}: identity 缺失")
            if not data.get("scope") or not data.get("routing"):
                problems.append(f"{path}: scope/routing 缺失")
            if identity.get("id"):
                ids.append(identity["id"])
        assert not problems, "; ".join(problems[:5])
        assert len(set(ids)) == len(ids), "存在重复的 Router Card ID"
        return {"router_cards": len(cards), "unique_ids": len(set(ids))}

    suite.case("A11", "Router Card 基础字段与 ID 唯一性", a11)

    # ---- A12 registry 与视频 bundle 一致性（文档 7.1 的回归项）
    def a12() -> dict[str, Any]:
        registry = json.loads((REPO_ROOT / "skills" / "registry.json").read_text(encoding="utf-8"))
        entries = registry.get("skills", registry)
        if isinstance(entries, dict):
            entries = list(entries.values())
        video_entries = [e for e in entries if "video_surveillance" in json.dumps(e, ensure_ascii=False)]
        registry_ids = {e.get("id") or e.get("skill_id") for e in video_entries}
        disk_ids = {p.parent.name for p in bundle.glob("*/router_card.json")}
        missing = sorted(disk_ids - registry_ids)
        stale = sorted(registry_ids - disk_ids)
        bad_paths = []
        for entry in video_entries:
            md = entry.get("skill_md_path")
            if not md:
                continue
            if "video_surveillance" not in md:
                bad_paths.append(md)
            elif not (REPO_ROOT / md).exists():
                bad_paths.append(f"{md} (文件不存在)")
        assert not missing, f"registry 缺少 bundle 中的 skill: {missing}"
        assert not stale, f"registry 保留了 bundle 中已不存在的 skill: {stale}"
        assert not bad_paths, f"skill_md_path 错误: {bad_paths[:5]}"
        # registry 里的 scope 字段必须与 Router Card 保持同步，否则场景过滤器
        # 注入的候选描述会与实际 Router Card 不一致。
        drift: list[str] = []
        for entry in video_entries:
            card_path = entry.get("router_card_path")
            if not card_path or not (REPO_ROOT / card_path).exists():
                continue
            scope = json.loads((REPO_ROOT / card_path).read_text(encoding="utf-8")).get("scope", {})
            for field in ("scenes", "task_types", "input_types", "output_types"):
                if field in scope and entry.get(field) is not None and entry[field] != scope[field]:
                    drift.append(f"{entry.get('id')}.{field}")
        assert not drift, f"registry 与 Router Card 的 scope 不同步: {drift}"
        return {"bundle_skills": len(disk_ids), "registry_entries": len(video_entries)}

    suite.case("A12", "registry.json 与视频 bundle 路径/scope 一致（文档 7.1 回归）", a12)

    # ---- A13 路由冲突检查（关闭 embedding）
    def a13() -> dict[str, Any]:
        script = REPO_ROOT / "scripts" / "check_skill_router_conflicts.py"
        if not script.exists():
            return {"_status": "skip", "_message": "check_skill_router_conflicts.py 不存在"}
        proc = run_cmd(
            [sys.executable, str(script), "--all", "--skills-root", "skills", "--no-embedding", "--json"],
            cwd=REPO_ROOT,
        )
        (out_dir / "A13-router-conflicts.json").write_text(proc["stdout"], encoding="utf-8")
        assert proc["returncode"] == 0, proc["stderr"][-500:]
        payload = json.loads(proc["stdout"][proc["stdout"].find("{"):])
        conflicts = payload.get("conflicts", [])
        assert not conflicts, f"存在路由冲突: {conflicts[:3]}"
        return {"conflicts": len(conflicts)}

    suite.case("A13", "无 embedding 的 Router Card 集合边界检查 -> conflicts=[]", a13)

    # ---- A13b bundle 完整性：SKILL.md 齐备、脚本可编译、SKILL.md 引用的脚本存在
    def a13b() -> dict[str, Any]:
        problems: list[str] = []
        scripts_checked = 0
        skill_dirs = sorted(p.parent for p in bundle.glob("*/router_card.json"))
        for skill_dir in skill_dirs:
            skill_md = skill_dir / "SKILL.md"
            if not skill_md.exists():
                problems.append(f"{skill_dir.name}: 缺少 SKILL.md")
                continue
            text = skill_md.read_text(encoding="utf-8")
            for ref in set(re.findall(r"scripts/[A-Za-z0-9_./-]+\.py", text)):
                if not (skill_dir / ref).exists():
                    problems.append(f"{skill_dir.name}: SKILL.md 引用了不存在的 {ref}")
            for script in sorted(skill_dir.glob("scripts/*.py")):
                scripts_checked += 1
                proc = run_cmd([sys.executable, "-m", "py_compile", str(script)], cwd=REPO_ROOT, timeout=120)
                if proc["returncode"] != 0:
                    problems.append(f"{skill_dir.name}/{script.name}: 语法错误 {proc['stderr'][-200:]}")
        assert not problems, "; ".join(problems[:5])
        return {"skills": len(skill_dirs), "scripts_compiled": scripts_checked}

    suite.case("A13b", "视频 bundle 完整性：SKILL.md、脚本引用与语法", a13b)

    # ---- A14 视频检索 / 向量索引：StreetModel 与 ES 未启用，显式跳过执行
    def a14() -> dict[str, Any]:
        return {
            "_status": "skip",
            "_message": "StreetModel(219.245.185.245:3130) 与 Elasticsearch(172.17.0.1:3128) 未启用，"
                        "video-search / video-embedding-index / object-statistics 的在线检索路径不在本次执行范围",
            "skipped_skills": ["video-search", "video-embedding-index", "object-statistics"],
        }

    suite.case("A14", "在线检索类 skill 执行（依赖 StreetModel/ES）", a14)

    # ---- A15 bundle 内所有 Markdown 引用的 skills/custom 路径必须存在
    # A13b 只校验 SKILL.md 里的 scripts/*.py。README.md 与 DEERFLOW_BUNDLE.md 曾长期保留
    # 重构前的 skills/custom/<skill-id>/ 旧路径（真实路径是 skills/custom/video_surveillance/<skill-id>/），
    # Agent 读到这些文档就会照着跑不存在的命令，白白消耗步数预算。
    def a15() -> dict[str, Any]:
        pattern = re.compile(r"(?:/mnt/skills|skills)/custom/[A-Za-z0-9_./-]+")
        broken: list[str] = []
        checked = 0
        md_files = sorted(bundle.glob("*.md")) + sorted(bundle.glob("*/SKILL.md"))
        for md in md_files:
            text = md.read_text(encoding="utf-8")
            for raw in sorted(set(pattern.findall(text))):
                ref = raw.rstrip(".,)：:；;")
                # 文档里的 /mnt/skills 是容器内路径，对应仓库里的 skills/
                rel = ref.replace("/mnt/skills/", "skills/", 1) if ref.startswith("/mnt/skills/") else ref
                checked += 1
                if not (REPO_ROOT / rel).exists():
                    broken.append(f"{md.relative_to(REPO_ROOT)}: {raw}")
        assert not broken, f"文档引用了不存在的路径: {broken[:6]}"
        return {"markdown_files": len(md_files), "paths_checked": checked}

    suite.case("A15", "bundle 内 Markdown 引用的 skills/custom 路径全部存在", a15)

    # ---- A16 README 技能表必须覆盖 bundle 内全部 skill
    # 曾出现技能表只列 19 条、而 bundle 有 21 个 skill 的情况，漏掉的恰好是
    # city-video-intelligence（唯一业务编排入口）与 video-object-analytics（结构化工具入口）。
    def a16() -> dict[str, Any]:
        readme = bundle / "README.md"
        assert readme.exists(), "缺少 README.md"
        listed = set(re.findall(r"^\|\s*`([a-z0-9-]+)`\s*\|", readme.read_text(encoding="utf-8"), re.MULTILINE))
        disk_ids = {p.parent.name for p in bundle.glob("*/router_card.json")}
        missing = sorted(disk_ids - listed)
        stale = sorted(listed - disk_ids)
        assert not missing, f"README 技能表缺少 bundle 中的 skill: {missing}"
        assert not stale, f"README 技能表列出了 bundle 中不存在的 skill: {stale}"
        return {"listed": len(listed), "bundle_skills": len(disk_ids)}

    suite.case("A16", "README 技能表与 bundle 内 skill 一一对应", a16)

    # ---- A17 14 条用户题库在业务编排层的路由
    # 此前 city-video-intelligence 的关键词是等权计数，"视频""找"这类泛化词能压过
    # "目标检测"这类专用词，14 题里只有 3~4 题路由正确；抽帧、去重、证据截图、
    # 格式转换等能力甚至没有任何 capability 入口，请求只能落到兜底能力上。
    def a17() -> dict[str, Any]:
        cases_path = REPO_ROOT / "scripts" / "video_routing_questions_cases.json"
        if not cases_path.exists():
            return {"_status": "skip", "_message": "video_routing_questions_cases.json 不存在"}
        cases = json.loads(cases_path.read_text(encoding="utf-8"))
        script = bundle / "city-video-intelligence" / "scripts" / "run.py"
        problems: list[str] = []
        for case in cases:
            out_file = out_dir / f"A17-{case['id']}.json"
            proc = run_cmd(
                [sys.executable, str(script), "--request", case["question"], "--output", str(out_file)],
                cwd=REPO_ROOT,
            )
            if proc["returncode"] != 0:
                problems.append(f"{case['id']}: 退出码 {proc['returncode']}")
                continue
            payload = json.loads(out_file.read_text(encoding="utf-8"))
            plan = payload.get("plan", payload)
            capability = plan.get("capability")
            chain = plan.get("recommended_skill_chain", [])
            gaps = " ".join((plan.get("capability_gap") or {}).get("gaps", []))
            if case.get("expected_capability") and capability != case["expected_capability"]:
                problems.append(f"{case['id']}: capability={capability}，期望 {case['expected_capability']}")
            if case.get("expect_any") and not (set(case["expect_any"]) & set(chain)):
                problems.append(f"{case['id']}: 链路 {chain} 未命中 {case['expect_any']}")
            forbidden = sorted(set(case.get("forbid", [])) & set(chain))
            if forbidden:
                problems.append(f"{case['id']}: 链路使用了禁止的 skill {forbidden}")
            for token in case.get("expect_gap", []):
                if token not in gaps:
                    problems.append(f"{case['id']}: 缺少能力缺口声明 {token!r}")
            for token in case.get("expect_no_gap", []):
                if token.lower() in gaps.lower():
                    problems.append(f"{case['id']}: 误报能力缺口 {token!r}")
        assert not problems, "; ".join(problems[:6])
        return {"cases": len(cases)}

    suite.case("A17", "14 条用户题库在 city-video-intelligence 的能力与链路路由", a17)

    # ---- A18 补缺口的两个新 skill 的执行契约
    def a18() -> dict[str, Any]:
        work = out_dir / "A18"
        work.mkdir(parents=True, exist_ok=True)
        tracks = {
            "tracks": [
                {
                    "track_id": "track_0001", "camera_id": "CAM_T", "label": "car",
                    "start_time": "2026-07-26T10:00:00+00:00", "end_time": "2026-07-26T10:00:10+00:00",
                    "duration_seconds": 10, "trajectory": [[10, 10], [60, 60], [120, 120], [300, 300], [600, 600]],
                },
                {
                    "track_id": "track_0002", "camera_id": "CAM_T", "label": "person",
                    "start_time": "2026-07-26T10:00:00+00:00", "end_time": "2026-07-26T10:00:12+00:00",
                    "duration_seconds": 12, "trajectory": [[120, 120], [125, 122], [128, 125], [130, 128]],
                },
            ]
        }
        rois = {
            "rois": [
                {"id": "ROI_GATE", "name": "门口", "type": "gate", "polygon": [[0, 0], [200, 0], [200, 200], [0, 200]]},
                {"id": "ROI_PARK", "name": "停车区", "type": "parking", "polygon": [[400, 400], [900, 400], [900, 900], [400, 900]]},
            ]
        }
        (work / "tracks.json").write_text(json.dumps(tracks, ensure_ascii=False), encoding="utf-8")
        (work / "rois.json").write_text(json.dumps(rois, ensure_ascii=False), encoding="utf-8")
        script = bundle / "roi-transit-statistics" / "scripts" / "run.py"
        proc = run_cmd(
            [
                sys.executable, str(script), "--tracks-json", str(work / "tracks.json"),
                "--rois-json", str(work / "rois.json"), "--camera-id", "CAM_T",
                "--dwell-seconds", "3", "--output", str(work / "roi_stats.json"),
            ],
            cwd=REPO_ROOT,
        )
        assert proc["returncode"] == 0, proc["stderr"][-400:]
        payload = json.loads((work / "roi_stats.json").read_text(encoding="utf-8"))
        assert payload["status"] == "success", payload
        by_id = {roi["roi_id"]: roi for roi in payload["data"]["rois"]}
        # track_0001 起点在门口内，中途离开 -> left=1；随后进入停车区 -> entered=1。
        assert by_id["ROI_GATE"]["left"] == 1, by_id["ROI_GATE"]
        assert by_id["ROI_PARK"]["entered"] == 1, by_id["ROI_PARK"]
        # track_0002 全程停在门口内，超过 3 秒阈值 -> dwelled 命中。
        assert by_id["ROI_GATE"]["dwelled"] >= 1, by_id["ROI_GATE"]
        assert payload["data"]["dwell_seconds_is_approximate"] is True
        # 只做计数，不得输出事件语义结论。
        blob = json.dumps(payload, ensure_ascii=False)
        leaked = [word for word in ("入侵", "徘徊", "拥堵", "违停", "intrusion", "loitering") if word in blob]
        assert not leaked, f"ROI 统计结果里出现了事件语义词: {leaked}"

        # 缺输入必须走标准失败契约，而不是抛 traceback。
        missing = run_cmd(
            [sys.executable, str(script), "--rois-json", str(work / "rois.json"), "--output", str(work / "missing.json")],
            cwd=REPO_ROOT,
        )
        assert missing["returncode"] == 1, missing
        assert json.loads((work / "missing.json").read_text(encoding="utf-8"))["error_code"] == "MISSING_TRACKS"

        # video-privacy-masking 只在宿主机做契约校验；真实转码在 Layer B 的沙箱内跑。
        vpm = bundle / "video-privacy-masking" / "scripts" / "run.py"
        no_video = run_cmd(
            [sys.executable, str(vpm), "--video-uri", str(work / "nope.mp4"), "--output", str(work / "vpm.json")],
            cwd=REPO_ROOT,
        )
        assert no_video["returncode"] == 1, no_video
        assert json.loads((work / "vpm.json").read_text(encoding="utf-8"))["error_code"] == "VIDEO_NOT_FOUND"
        return {"roi_rois": len(by_id), "checks": "counts+contract"}

    suite.case("A18", "roi-transit-statistics 计数正确性与两个新 skill 的失败契约", a18)

    return suite.summary()


# --------------------------------------------------------------------------
# Layer B：沙箱内真实视频执行
# --------------------------------------------------------------------------
def layer_exec_inner(out_dir: Path, video: str, skills_root: Path) -> dict[str, Any]:
    """在沙箱容器内执行，skills_root 通常是 /mnt/skills。"""
    suite = Suite("exec", out_dir)
    bundle = skills_root / "custom" / "video_surveillance"
    config = str(bundle / "configs" / "deerflow_config.json")
    root = skills_root.parent  # 只用于 run_skill 的 cwd
    work = out_dir / "work"
    work.mkdir(parents=True, exist_ok=True)
    state: dict[str, Any] = {}

    def skill(name: str, args: list[str], **kw: Any) -> dict[str, Any]:
        script = bundle / name / "scripts" / "run.py"
        assert script.exists(), f"skill 脚本不存在: {script}"
        expect = kw.pop("expect_returncode", 0)
        proc = run_cmd([sys.executable, str(script), *args], cwd=root, timeout=kw.pop("timeout", 900))
        if expect is not None:
            assert proc["returncode"] == expect, (
                f"{name} 退出码 {proc['returncode']} != {expect}; stderr={proc['stderr'][-800:]}"
            )
        stdout = proc["stdout"].strip()
        assert stdout, f"{name} 无 JSON 输出; stderr={proc['stderr'][-800:]}"
        start = stdout.find("{")
        return json.loads(stdout[start:])

    # ---- B0 依赖自检
    def b0() -> dict[str, Any]:
        deps: dict[str, Any] = {
            "ffmpeg": shutil.which("ffmpeg"),
            "ffprobe": shutil.which("ffprobe"),
        }
        for mod in ("cv2", "numpy", "ultralytics", "torch"):
            try:
                import importlib
                deps[mod] = getattr(importlib.import_module(mod), "__version__", "unknown")
            except Exception:  # noqa: BLE001
                deps[mod] = None
        deps["video_exists"] = Path(video).exists()
        deps["yolo_model"] = (skills_root / "custom" / "models" / "yolov8n.pt").exists()
        missing = [k for k, v in deps.items() if not v]
        assert not missing, f"沙箱缺少依赖: {missing}"
        return deps

    suite.case("B0", "沙箱依赖自检（ffmpeg / OpenCV / Ultralytics / 视频 / 权重）", b0)

    # ---- B1 视频接入
    def b1() -> dict[str, Any]:
        result = skill("video-stream-ingestion", [
            "--video", video,
            "--camera-id", "CAM_DEERFLOW_001",
            "--source-type", "local_file",
            "--capture-seconds", "10",
            "--config", config,
            "--output", str(work / "b1-ingestion.json"),
        ])
        assert result["status"] == "success", result
        data = result.get("data", result)
        state["ingestion"] = data
        # 用户问"时长、分辨率、帧率、编码"时必须能由本 skill 直接答出，
        # 否则 Agent 会退化成自己调 ffprobe（Layer C C-002 的历史问题）。
        required = ["video_duration_seconds", "width", "height", "resolution", "fps",
                    "codec", "frame_count", "file_size_bytes"]
        missing = [field for field in required if data.get(field) in (None, "")]
        assert not missing, f"缺少视频元数据字段: {missing}"
        assert data["video_duration_seconds"] > data["duration_seconds"], (
            "video_duration_seconds 应为完整时长，duration_seconds 为采集窗口"
        )
        assert 0 < float(data["fps"]) < 240, f"fps 不合理: {data['fps']}"
        return {k: data[k] for k in required + ["duration_seconds", "raw_segment_uri"]}

    suite.case("B1", "真实视频接入 -> 返回完整时长/分辨率/帧率/编码元数据", b1)

    # ---- B2 抽帧
    def b2() -> dict[str, Any]:
        frames_out = work / "frames"
        result = skill("frame-sampling", [
            "--video", video,
            "--camera-id", "CAM_DEERFLOW_001",
            "--capture-seconds", "10",
            "--interval-seconds", "2",
            "--output-dir", str(frames_out),
            "--config", config,
            "--output", str(work / "b2-frames.json"),
        ])
        assert result["status"] == "success", result
        frames = result["data"]["frames"]
        assert len(frames) >= 3, f"抽帧数量过少: {len(frames)}"
        existing = [f for f in frames if Path(f["image_uri"].removeprefix("file://")).exists()]
        assert len(existing) == len(frames), f"帧文件缺失 {len(frames) - len(existing)} 个"
        state["frames_json"] = str(work / "b2-frames.json")
        state["frames"] = frames
        return {"frame_count": len(frames), "first_frame": frames[0]["image_uri"]}

    suite.case("B2", "抽帧 -> 帧数与磁盘上的图片文件一致", b2)

    # ---- B3 目标检测
    def b3() -> dict[str, Any]:
        if "frames_json" not in state:
            return {"_status": "skip", "_message": "依赖 B2 抽帧结果"}
        result = skill("object-detection", [
            "--frames-json", state["frames_json"],
            "--labels", "person,car,bus,truck,motorcycle,bicycle",
            "--config", config,
            "--output", str(work / "b3-detections.json"),
        ], timeout=1200)
        assert result["status"] == "success", result
        frames = result["data"]["detections"]
        objects = [obj for frame in frames for obj in frame.get("objects", [])]
        assert objects, "真实交通视频未检出任何目标"
        allowed = {"person", "car", "bus", "truck", "motorcycle", "bicycle"}
        labels = {obj.get("label") for obj in objects}
        assert labels <= allowed, f"检出了未申请的类别: {labels - allowed}"
        for obj in objects[:30]:
            bbox = obj.get("bbox")
            assert bbox and len(bbox) == 4, f"检测框格式错误: {obj}"
            assert bbox[0] < bbox[2] and bbox[1] < bbox[3], f"bbox 坐标非法: {bbox}"
            assert 0.0 <= float(obj.get("confidence", 0)) <= 1.0, obj
        state["detections_json"] = str(work / "b3-detections.json")
        return {
            "frames_with_detection": len(frames),
            "objects": len(objects),
            "labels": sorted(labels),
            "model": result["data"].get("model"),
        }

    suite.case("B3", "YOLO 目标检测 -> 有检出、类别受限、bbox/置信度合法", b3)

    # ---- B4 目标跟踪
    def b4() -> dict[str, Any]:
        if "detections_json" not in state:
            return {"_status": "skip", "_message": "依赖 B3 检测结果"}
        result = skill("object-tracking", [
            "--detections-json", state["detections_json"],
            "--camera-id", "CAM_DEERFLOW_001",
            "--config", config,
            "--output", str(work / "b4-tracks.json"),
        ])
        assert result["status"] == "success", result
        tracks = result["data"]["tracks"]
        assert tracks, "未生成任何轨迹"
        blob = json.dumps(result, ensure_ascii=False)
        for word in ("打架", "fight", "拥堵", "congestion", "入侵", "intrusion", "摔倒", "fall_down"):
            assert word not in blob, f"跟踪结果不应输出事件语义: {word}"
        state["tracks_json"] = str(work / "b4-tracks.json")
        return {"tracks": len(tracks), "sample": tracks[0]}

    suite.case("B4", "目标跟踪 -> 生成轨迹且不输出事件结论", b4)

    # ---- B5 ROI 匹配（真实轨迹）
    def b5() -> dict[str, Any]:
        if "tracks_json" not in state:
            return {"_status": "skip", "_message": "依赖 B4 轨迹结果"}
        result = skill("roi-mapping", [
            "--tracks-json", state["tracks_json"],
            "--camera-id", "CAM_DEERFLOW_001",
            "--config", config,
            "--output", str(work / "b5-roi.json"),
        ])
        assert result["status"] == "success", result
        matches = result["data"]["matches"]
        assert matches, "配置中的全画面 ROI 未产生任何匹配"
        assert any(m["matched"] for m in matches), "全画面 ROI 应至少命中一个目标"
        return {"matches": len(matches), "matched": sum(1 for m in matches if m["matched"])}

    suite.case("B5", "ROI 匹配 -> 全画面 ROI 命中真实轨迹", b5)

    # ---- B6 摄像头健康检查
    def b6() -> dict[str, Any]:
        if "frames_json" not in state:
            return {"_status": "skip", "_message": "依赖 B2 抽帧结果"}
        result = skill("camera-health-check", [
            "--frames-json", state["frames_json"],
            "--camera-id", "CAM_DEERFLOW_001",
            "--config", config,
            "--output", str(work / "b6-health.json"),
        ])
        assert result["status"] == "success", result
        data = result["data"]
        assert data.get("health_status") in {"ok", "degraded", "offline"}, data
        return data

    suite.case("B6", "摄像头健康检查 -> 返回合法 health_status", b6)

    # ---- B7 单视频事件分析（唯一事件语义入口）
    def b7() -> dict[str, Any]:
        result = skill("single-video-event-analysis", [
            "--video", video,
            "--camera-id", "CAM_DEERFLOW_001",
            "--output-dir", str(work / "event-analysis"),
            "--coarse-fps", "0.5",
            "--dense-fps", "2",
            "--max-frames", "60",
            "--config", config,
            "--output", str(work / "b7-event-analysis.json"),
        ], timeout=1500)
        assert result["status"] == "success", result
        data = result["data"]
        manifest_uri = data.get("review_manifest_uri")
        assert manifest_uri, f"缺少 review_manifest_uri: {data}"
        manifest_path = Path(manifest_uri.removeprefix("file://"))
        assert manifest_path.exists(), f"review_manifest 文件不存在: {manifest_path}"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest.get("schema") == "single_video_event_review_manifest", manifest.get("schema")
        blob = json.dumps(manifest, ensure_ascii=False)
        assert "review_frames" in blob or "frames" in blob, "manifest 缺少审帧列表"
        state["event_manifest"] = str(manifest_path)
        return {
            "review_manifest_uri": str(manifest_path),
            "keys": sorted(data.keys()),
        }

    suite.case("B7", "单视频事件分析 -> 生成 review_manifest 审帧产物", b7)

    # ---- B8 证据截图
    def b8() -> dict[str, Any]:
        if "frames_json" not in state:
            return {"_status": "skip", "_message": "依赖 B2 抽帧结果"}
        frames = state["frames"]
        event = write_json(work / "b8-event.json", {
            "event": {
                "event_id": "EVT_TEST_0001",
                "event_type": "traffic_congestion",
                "camera_id": "CAM_DEERFLOW_001",
                "confidence": 0.62,
                "evidence_frame_ids": [frames[1]["frame_id"]],
            }
        })
        result = skill("evidence-snapshot", [
            "--event-json", str(event),
            "--frames-json", state["frames_json"],
            "--output-dir", str(work / "evidence"),
            "--config", config,
            "--output", str(work / "b8-snapshot.json"),
        ])
        assert result["status"] == "success", result
        data = result["data"]
        snapshot = Path(data["uri"].removeprefix("file://"))
        assert snapshot.exists() and snapshot.stat().st_size > 0, f"证据截图未生成: {snapshot}"
        assert data["hash"].startswith("sha256:"), data["hash"]
        state["event_json"] = str(event)
        state["snapshot_uri"] = str(snapshot)
        return {"uri": str(snapshot), "hash": data["hash"], "privacy_masked": data.get("privacy_masked")}

    suite.case("B8", "证据截图 -> 生成真实图片文件与 sha256 哈希", b8)

    # ---- B9 片段截取
    def b9() -> dict[str, Any]:
        result = skill("video-segment-extraction", [
            "--event-id", "EVT_TEST_0001",
            "--raw-segment-uri", video,
            "--event-time", "2026-05-20T10:00:05+08:00",
            "--event-elapsed-seconds", "5",
            "--pre-seconds", "2",
            "--post-seconds", "2",
            "--output-dir", str(work / "segments"),
            "--config", config,
            "--output", str(work / "b9-segment.json"),
        ], timeout=900)
        assert result["status"] == "success", result
        data = result["data"]
        segment = Path(str(data["clip_uri"]).removeprefix("file://"))
        assert segment.exists() and segment.stat().st_size > 0, f"片段未生成: {segment}"
        probe = run_cmd(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                         "-of", "default=nw=1:nk=1", str(segment)])
        duration = float(probe["stdout"].strip() or 0)
        assert 3.0 <= duration <= 5.5, f"片段时长 {duration}s 不在 4s±容差范围内"
        return {"uri": str(segment), "duration_seconds": round(duration, 2)}

    suite.case("B9", "片段截取 -> 生成前后各 2 秒、实际时长约 4 秒的 mp4", b9)

    # ---- B10 隐私打码
    def b10() -> dict[str, Any]:
        if "snapshot_uri" not in state:
            return {"_status": "skip", "_message": "依赖 B8 证据截图"}
        source = Path(state["snapshot_uri"])
        regions = write_json(work / "b10-regions.json", {
            "sensitive_regions": [{"type": "face", "bbox": [10, 10, 200, 200]}]
        })
        before = source.read_bytes()
        result = skill("privacy-masking", [
            "--image-uri", str(source),
            "--sensitive-regions-json", str(regions),
            "--method", "blur",
            "--config", config,
            "--output", str(work / "b10-masked.json"),
        ])
        assert result["status"] == "success", result
        data = result["data"]
        masked = Path(data.get("masked_uri") or data.get("uri", "")).with_suffix(
            Path(data.get("masked_uri") or data.get("uri", "")).suffix
        )
        assert masked.exists(), f"打码结果文件不存在: {masked}"
        assert masked.read_bytes() != before, "打码后的图片与原图完全一致"
        return {"masked_uri": str(masked), "method": data.get("method")}

    suite.case("B10", "隐私打码 -> 输出与原图不同的打码副本", b10)

    # ---- B11 事件 -> 人工复核闭环
    def b11() -> dict[str, Any]:
        if "event_json" not in state:
            return {"_status": "skip", "_message": "依赖 B8 事件输入"}
        health = work / "b6-health.json"
        args = ["--event-json", state["event_json"], "--config", config,
                "--output", str(work / "b11-review.json")]
        if health.exists():
            args += ["--camera-health-json", str(health)]
        result = skill("human-review-routing", args)
        assert result["status"] == "success", result
        data = result["data"]
        assert data["queue"] in {"manual_review", "auto_pass"}, data
        assert data["review_required"] is True, f"置信度 0.62 低于阈值，应进入人工复核: {data}"
        return data

    suite.case("B11", "证据事件 -> 低置信度进入人工复核队列", b11)

    # ---- B12 ffmpeg-utils 原子操作
    def b12() -> dict[str, Any]:
        out_path = work / "b12-keyframe.jpg"
        result = skill("ffmpeg-utils", [
            "--operation", "keyframe",
            "--input-path", video,
            "--timestamp", "00:00:03",
            "--output-path", str(out_path),
            "--output", str(work / "b12-ffmpeg.json"),
        ], timeout=600)
        assert result["status"] == "success", result
        assert out_path.exists() and out_path.stat().st_size > 0, "关键帧未导出"
        return {"uri": str(out_path), "size": out_path.stat().st_size}

    suite.case("B12", "ffmpeg-utils -> 按时间戳导出关键帧", b12)

    # ---- B13 负面契约：检测输入不存在
    def b13() -> dict[str, Any]:
        script = bundle / "object-detection" / "scripts" / "run.py"
        proc = run_cmd([sys.executable, str(script),
                        "--frames-json", str(work / "does-not-exist.json"),
                        "--labels", "person",
                        "--config", config], cwd=root, timeout=300)
        (work / "b13-detection-missing-input.txt").write_text(
            f"returncode={proc['returncode']}\n--- stdout ---\n{proc['stdout']}\n--- stderr ---\n{proc['stderr']}",
            encoding="utf-8",
        )
        stdout = proc["stdout"].strip()
        assert stdout.startswith("{") or "{" in stdout, (
            "object-detection 对缺失的 --frames-json 直接抛出 "
            f"{proc['stderr'].strip().splitlines()[-1] if proc['stderr'].strip() else 'Traceback'}，"
            "未返回 status=failed 的标准失败 JSON（违反 skill 失败契约）"
        )
        result = json.loads(stdout[stdout.find("{"):])
        assert result.get("status") == "failed", f"缺失输入应返回 failed: {result}"
        assert result.get("error_code"), f"缺少 error_code: {result}"
        assert not result.get("data", {}).get("detections"), "失败时不得返回检测结果"
        return {"error_code": result["error_code"], "message": result.get("message", "")[:120]}

    suite.case("B13", "目标检测输入缺失 -> 失败契约且不伪造检测结果", b13)

    # ---- B14 负面契约：片段截取缺少事件时间
    def b14() -> dict[str, Any]:
        result = skill("video-segment-extraction", [
            "--event-id", "EVT_TEST_0002",
            "--raw-segment-uri", video,
            "--output-dir", str(work / "segments"),
            "--config", config,
        ], expect_returncode=1)
        assert result["status"] == "failed", result
        assert result["error_code"] == "MISSING_EVENT", result
        seg = work / "segments" / "evidence" / "EVT_TEST_0002_clip.mp4"
        assert not seg.exists(), "缺少事件时间时不得生成片段文件"
        return {"error_code": result["error_code"], "message": result["message"]}

    suite.case("B14", "片段截取缺少事件时间 -> MISSING_EVENT 且不产出文件", b14)

    # ---- B15 失败契约横向扫描：所有接收 *-json 输入的 skill
    def b15() -> dict[str, Any]:
        missing = str(work / "no-such-input.json")
        targets = [
            ("object-detection", ["--frames-json", missing]),
            ("object-tracking", ["--detections-json", missing]),
            ("roi-mapping", ["--tracks-json", missing]),
            ("duplicate-event-merge", ["--events-json", missing]),
            ("evidence-snapshot", ["--event-json", missing]),
            ("human-review-routing", ["--event-json", missing]),
            ("camera-health-check", ["--frames-json", missing]),
            ("privacy-masking", ["--sensitive-regions-json", missing, "--image-uri", missing]),
            ("roi-transit-statistics", ["--tracks-json", missing]),
            ("video-privacy-masking", ["--sensitive-regions-json", missing, "--video-uri", missing]),
        ]
        compliant: list[str] = []
        violating: dict[str, str] = {}
        for name, extra in targets:
            script = bundle / name / "scripts" / "run.py"
            if not script.exists():
                continue
            proc = run_cmd([sys.executable, str(script), *extra], cwd=root, timeout=300)
            stdout = proc["stdout"].strip()
            if "{" in stdout:
                try:
                    payload = json.loads(stdout[stdout.find("{"):])
                except json.JSONDecodeError:
                    violating[name] = "stdout 不是合法 JSON"
                    continue
                if payload.get("status") == "failed" and payload.get("error_code"):
                    compliant.append(name)
                else:
                    violating[name] = f"status={payload.get('status')} error_code={payload.get('error_code')}"
            else:
                last = proc["stderr"].strip().splitlines()[-1] if proc["stderr"].strip() else "无输出"
                violating[name] = f"未返回失败 JSON：{last[:120]}"
        write_json(work / "b15-failure-contract-scan.json",
                   {"compliant": compliant, "violating": violating})
        assert not violating, (
            f"以下 skill 在输入文件缺失时未返回标准失败 JSON: {json.dumps(violating, ensure_ascii=False)}"
        )
        return {"compliant": compliant}

    suite.case("B15", "输入文件缺失时的失败契约横向扫描", b15)

    # ---- B16 负面契约：输入是非法 JSON
    def b16() -> dict[str, Any]:
        broken = work / "b16-broken.json"
        broken.write_text('{"frames": [', encoding="utf-8")
        script = bundle / "object-detection" / "scripts" / "run.py"
        proc = run_cmd([sys.executable, str(script), "--frames-json", str(broken),
                        "--labels", "person", "--config", config], cwd=root, timeout=300)
        stdout = proc["stdout"].strip()
        assert "{" in stdout, f"非法 JSON 输入未返回失败 JSON: {proc['stderr'][-200:]}"
        result = json.loads(stdout[stdout.find("{"):])
        assert result.get("status") == "failed", result
        assert result.get("error_code") == "INPUT_INVALID_JSON", result
        assert proc["returncode"] == 1, proc["returncode"]
        return {"error_code": result["error_code"]}

    suite.case("B16", "输入为非法 JSON -> INPUT_INVALID_JSON", b16)

    # ---- B17 视频级隐私打码：真实转码并逐像素校验遮蔽区域
    # 这是本轮新补的能力缺口。privacy-masking 只处理单张图片，
    # “输出一份可以公开使用的脱敏视频”此前在 bundle 里无人承接。
    def b17() -> dict[str, Any]:
        # 先截 6 秒短片，避免对 900 秒原片整段转码。
        clip = work / "b17-source.mp4"
        cut = run_cmd(
            ["ffmpeg", "-y", "-v", "error", "-i", video, "-t", "6", "-c", "copy", str(clip)],
            cwd=root,
            timeout=600,
        )
        assert cut["returncode"] == 0, cut["stderr"][-300:]
        regions = write_json(work / "b17-regions.json", {
            "sensitive_regions": [
                {"type": "face", "bbox": [100, 100, 300, 260]},
                {"type": "plate", "bbox": [500, 300, 760, 380], "start_seconds": 2, "end_seconds": 8},
            ]
        })
        masked = work / "b17-masked.mp4"
        result = skill("video-privacy-masking", [
            "--video-uri", str(clip),
            "--sensitive-regions-json", str(regions),
            "--method", "solid",
            "--output-video", str(masked),
            "--output", str(work / "b17-result.json"),
        ], timeout=900)
        assert result["status"] == "success", result
        data = result["data"]
        assert masked.exists(), f"脱敏视频未生成: {masked}"
        assert data["privacy_masked"] is True
        assert str(data.get("sha256", "")).startswith("sha256:"), data
        assert data.get("duration_seconds") and data["duration_seconds"] > 5, data

        # 逐像素校验：常驻区域全程被遮蔽，带时间窗口的区域只在窗口内被遮蔽，
        # 窗口外与未申报区域必须保持原样，否则就是遮错了地方。
        import cv2  # noqa: PLC0415 - 仅沙箱内可用
        import numpy as np  # noqa: PLC0415

        def frame_at(path: str, ms: int):
            cap = cv2.VideoCapture(path)
            cap.set(cv2.CAP_PROP_POS_MSEC, ms)
            ok, image = cap.read()
            cap.release()
            assert ok and image is not None, f"无法读取 {path} 在 {ms}ms 的帧"
            return image

        deltas: dict[str, dict[str, float]] = {}
        for ms in (500, 4000):
            before, after = frame_at(str(clip), ms), frame_at(str(masked), ms)

            def mean_delta(y1: int, y2: int, x1: int, x2: int) -> float:
                return float(np.mean(np.abs(before[y1:y2, x1:x2].astype(int) - after[y1:y2, x1:x2].astype(int))))

            deltas[str(ms)] = {
                "face": mean_delta(100, 260, 100, 300),
                "plate": mean_delta(300, 380, 500, 760),
                "untouched": mean_delta(0, 80, 0, 80),
            }
        assert deltas["500"]["face"] > 10, f"常驻遮蔽区域在 t=0.5s 未被遮蔽: {deltas['500']}"
        assert deltas["4000"]["face"] > 10, f"常驻遮蔽区域在 t=4s 未被遮蔽: {deltas['4000']}"
        assert deltas["4000"]["plate"] > 10, f"时间窗口内的区域未被遮蔽: {deltas['4000']}"
        assert deltas["500"]["plate"] < 5, f"时间窗口外的区域被误遮蔽: {deltas['500']}"
        assert deltas["4000"]["untouched"] < 5, f"未申报区域被误改: {deltas['4000']}"
        return {"masked_uri": str(masked), "deltas": deltas}

    suite.case("B17", "视频级隐私打码 -> 真实脱敏视频且遮蔽区域与时间窗口正确", b17)

    # ---- B18 analyze-video 抽帧与 metadata
    # 覆盖盘点时发现的漏网之鱼：analyze-video 有可执行脚本、没有任何外部服务依赖，
    # 却从来没有被 Layer A/B 执行过，等于 bundle 里一直有一个"没人验证过能不能跑"的 skill。
    def b18() -> dict[str, Any]:
        out_root = work / "b18-frames"
        script = bundle / "analyze-video" / "scripts" / "extract_frames.py"
        assert script.exists(), f"analyze-video 脚本不存在: {script}"
        proc = run_cmd(
            [sys.executable, str(script), video, "--output-dir", str(out_root)],
            cwd=root,
            timeout=900,
        )
        assert proc["returncode"] == 0, f"analyze-video 退出码 {proc['returncode']}; stderr={proc['stderr'][-600:]}"
        meta_files = list(out_root.rglob("metadata.json"))
        assert meta_files, f"未生成 metadata.json；产物目录: {[str(p) for p in out_root.rglob('*')][:10]}"
        meta = json.loads(meta_files[0].read_text(encoding="utf-8"))
        images = [p for p in out_root.rglob("*") if p.suffix.lower() in {".jpg", ".jpeg", ".png"}]
        assert images, "没有抽出任何帧图片"
        for image in images[:5]:
            assert image.stat().st_size > 0, f"帧图片为空文件: {image}"
        # 该 skill 明确声明不产出事件结论，输出里不应出现事件语义词。
        blob = json.dumps(meta, ensure_ascii=False)
        leaked = [w for w in ("打架", "事故", "入侵", "拥堵", "摔倒", "fight", "accident") if w in blob]
        assert not leaked, f"analyze-video 的 metadata 里出现了事件语义结论: {leaked}"
        return {"metadata": str(meta_files[0]), "frames": len(images), "metadata_keys": sorted(meta)[:8]}

    suite.case("B18", "analyze-video -> 抽帧产物与 metadata.json，且不输出事件结论", b18)

    return suite.summary()


def layer_exec(out_dir: Path, image: str, video_rel: str) -> dict[str, Any]:
    """在沙箱镜像内运行 Layer B。"""
    datasets_host = os.environ.get("DEER_FLOW_HOST_DATASETS_PATH") or str(REPO_ROOT / "datasets")
    inner_out = out_dir / "sandbox"
    inner_out.mkdir(parents=True, exist_ok=True)
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{REPO_ROOT / 'skills'}:/mnt/skills:ro",
        "-v", f"{datasets_host}:/mnt/datasets:ro",
        "-v", f"{inner_out}:/mnt/out",
        "-v", f"{REPO_ROOT / 'scripts'}:/mnt/testsuite:ro",
        # 以宿主机 UID 运行，避免产物变成 root 所有导致无法清理
        "--user", f"{os.getuid()}:{os.getgid()}",
        "-e", "HOME=/tmp",
        "-e", "YOLO_AUTOINSTALL=false",
        "-e", "YOLO_VERBOSE=false",
        "-e", "YOLO_CONFIG_DIR=/tmp/ultralytics",
        "-e", "MPLCONFIGDIR=/tmp/mpl",
        "--entrypoint", "python3",
        image,
        "/mnt/testsuite/test_video_surveillance_skills.py",
        "--layer", "exec-inner",
        "--out", "/mnt/out",
        "--video", f"/mnt/datasets/{video_rel}",
        "--skills-root", "/mnt/skills",
    ]
    print("$ " + " ".join(cmd))
    proc = subprocess.run(cmd, text=True, timeout=5400)
    summary_path = inner_out / "summary.json"
    if summary_path.exists():
        return json.loads(summary_path.read_text(encoding="utf-8"))
    return {
        "suite": "exec",
        "counts": {"error": 1},
        "results": [{
            "id": "B--",
            "title": "沙箱执行",
            "status": "error",
            "message": f"容器退出码 {proc.returncode}，未生成 summary.json",
            "detail": {},
        }],
    }


# --------------------------------------------------------------------------
# 报告
# --------------------------------------------------------------------------
def render_report(summaries: list[dict[str, Any]], env: dict[str, Any], out_dir: Path) -> Path:
    lines = [
        "# 视频监控 skill 测试结果",
        "",
        f"运行时间：{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 环境",
        "",
        "| 项目 | 值 |",
        "| --- | --- |",
    ]
    for key, value in env.items():
        lines.append(f"| {key} | {value} |")
    for summary in summaries:
        counts = summary["counts"]
        total = sum(counts.values())
        passed = counts.get("pass", 0)
        lines += [
            "",
            f"## Layer `{summary['suite']}`（{passed}/{total} 通过）",
            "",
            "| 用例 | 标题 | 结果 | 说明 |",
            "| --- | --- | --- | --- |",
        ]
        for item in summary["results"]:
            note = (item["message"] or "").replace("|", "/").replace("\n", " ")[:160]
            if not note and item["detail"]:
                note = json.dumps(item["detail"], ensure_ascii=False)[:160].replace("|", "/")
            lines.append(f"| {item['id']} | {item['title']} | {item['status']} | {note} |")
    path = out_dir / "REPORT.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def collect_env() -> dict[str, Any]:
    def probe(url: str) -> str:
        try:
            import urllib.request
            with urllib.request.urlopen(url, timeout=5) as resp:
                return f"HTTP {resp.status}"
        except Exception as exc:  # noqa: BLE001
            return f"不可用 ({type(exc).__name__})"

    return {
        "仓库": str(REPO_ROOT),
        "沙箱镜像": os.environ.get("DEER_FLOW_SANDBOX_IMAGE", "(未设置)"),
        "数据集挂载": os.environ.get("DEER_FLOW_HOST_DATASETS_PATH", str(REPO_ROOT / "datasets")),
        "LangGraph API": probe("http://127.0.0.1:3538/"),
        "Elasticsearch": probe("http://172.17.0.1:3128"),
        "StreetModel": probe("http://219.245.185.245:3130"),
        "skill_router": "disabled (config.yaml skill_router.enabled=false)",
    }


def load_dotenv() -> None:
    env_file = REPO_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--layer", choices=["plan", "exec", "exec-inner", "all"], default="all")
    parser.add_argument("--out", default=None, help="结果输出目录")
    parser.add_argument("--video", default="Vedio-demo/Trafic.mp4",
                        help="Layer B 使用的视频（相对 /mnt/datasets，或容器内绝对路径）")
    parser.add_argument("--skills-root", default=None, help="容器内 skills 根目录，仅 exec-inner 使用")
    parser.add_argument("--image", default=None, help="沙箱镜像，默认取 DEER_FLOW_SANDBOX_IMAGE")
    args = parser.parse_args()

    if args.layer == "exec-inner":
        out_dir = Path(args.out or "/mnt/out")
        summary = layer_exec_inner(out_dir, args.video, Path(args.skills_root or "/mnt/skills"))
        write_json(out_dir / "summary.json", summary)
        return 0 if not summary["counts"].get("fail") and not summary["counts"].get("error") else 1

    load_dotenv()
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = Path(args.out).resolve() if args.out else REPO_ROOT / "outputs" / "skill-tests" / "video-surveillance-suite" / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"结果目录: {out_dir}\n")

    summaries: list[dict[str, Any]] = []
    if args.layer in {"plan", "all"}:
        print("=== Layer A: 编排与契约 ===")
        summaries.append(layer_plan(out_dir / "plan"))
        print()
    if args.layer in {"exec", "all"}:
        print("=== Layer B: 沙箱真实视频执行 ===")
        image = args.image or os.environ.get("DEER_FLOW_SANDBOX_IMAGE")
        if not image:
            summaries.append({"suite": "exec", "counts": {"skip": 1}, "results": [{
                "id": "B--", "title": "沙箱执行", "status": "skip",
                "message": "未配置 DEER_FLOW_SANDBOX_IMAGE", "detail": {}}]})
        else:
            summaries.append(layer_exec(out_dir, image, args.video))
        print()

    env = collect_env()
    write_json(out_dir / "summary.json", {"env": env, "layers": summaries})
    report = render_report(summaries, env, out_dir)

    print("=== 汇总 ===")
    failed = 0
    for summary in summaries:
        counts = summary["counts"]
        total = sum(counts.values())
        print(f"  {summary['suite']}: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" (共 {total})")
        failed += counts.get("fail", 0) + counts.get("error", 0)
    print(f"报告: {report}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
