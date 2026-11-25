#!/usr/bin/env python3
"""Export safe, reproducible summaries of the real OneCity load campaigns."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

RUNS = (
    ("business-baseline", "现有服务基线", 3),
    ("business-soak", "现有服务持续负载", 3),
    ("fixed-jobs2", "独立服务两执行任务", 2),
    ("fixed-jobs3", "三执行任务超限文本阻塞故障", 3),
    ("fixed-jobs3-retest", "三执行任务修复后复测", 3),
    ("final-overload", "最终八请求容量边界", 3),
    ("final-soak", "最终六轮持续负载", 3),
)
TIMINGS = {
    "queue_seconds": "query_to_first_observed_running_seconds",
    "first_event_seconds": "query_to_first_sse_event_seconds",
    "first_answer_seconds": "query_to_first_intent_planning_or_answer_feedback_seconds",
    "model_finished_seconds": "query_to_sse_end_seconds",
    "total_seconds": "request_to_verification_end_seconds",
    "upload_seconds": "upload_seconds",
    "native_lifetime_seconds": "native_created_to_updated_seconds_including_queue",
    "terminal_settle_seconds": "native_terminal_confirmation_seconds",
    "headers_seconds": "query_to_response_headers_seconds",
}
TERMINAL = {"success", "error", "timeout", "interrupted", "cancelled", "completed", "failed"}


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def quantiles(values):
    ordered = sorted(float(value) for value in values if number(value))
    if not ordered:
        return {"count": 0}
    return {
        "count": len(ordered),
        "min": round(ordered[0], 3),
        "mean": round(statistics.mean(ordered), 3),
        "p50": round(ordered[math.ceil(len(ordered) * 0.50) - 1], 3),
        "p95": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 3),
        "max": round(ordered[-1], 3),
    }


def fraction(numerator, denominator):
    return {"numerator": numerator, "denominator": denominator, "rate": round(numerator / denominator, 6) if denominator else None}


def capacity_rejected(case):
    return case.get("query_http_status") == 429 or any(error.get("code") == 429 for error in case.get("errors", []))


def accepted(case):
    return case.get("query_http_status") == 200 and not capacity_rejected(case)


def safe_word(value, fallback="unknown"):
    text = str(value or "")
    return text if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", text) else fallback


def native_record(case):
    return case.get("final_native_verification") or case.get("native_run") or {}


def lifetime(case):
    native = native_record(case)
    try:
        created = datetime.fromisoformat(native["created_at"])
        updated = datetime.fromisoformat(native["updated_at"])
        elapsed = (updated - created).total_seconds()
        if native.get("status") in TERMINAL and elapsed >= 0:
            return round(elapsed, 3)
    except (KeyError, TypeError, ValueError):
        pass
    return case.get("native_lifetime_seconds")


def outcome(case):
    if case.get("passed") is True:
        return "passed"
    if capacity_rejected(case):
        return "capacity_rejected"
    if case.get("upload_rejected"):
        return "upload_rejected" if case.get("upload_status") in {200, 400, 403, 413, 415, 422} else "upload_transport_failed"
    if accepted(case):
        return "accepted_task_incomplete"
    return "request_failed_before_admission"


def completion_counts(cases):
    return {
        "native_error_timeout_or_interrupt": fraction(sum(native_record(case).get("status") in {"error", "timeout", "interrupted", "cancelled"} for case in cases), len(cases)),
        "native_terminal_unconfirmed": fraction(sum(native_record(case).get("status") not in TERMINAL for case in cases), len(cases)),
        "native_success_but_client_failed": fraction(sum(native_record(case).get("status") == "success" and not case.get("passed") for case in cases), len(cases)),
        "target_value_check_failed_or_missing": fraction(sum(case.get("answer_correct") is not True for case in cases), len(cases)),
    }


def safe_case(case):
    result = {
        "case": safe_word(case.get("case")),
        "account": safe_word(case.get("account")),
        "scene": safe_word(case.get("scene")),
        "input_bytes": case.get("input_bytes") if number(case.get("input_bytes")) else None,
        "rows": case.get("rows") if number(case.get("rows")) else None,
        "outcome": outcome(case),
        "passed": case.get("passed") is True,
        "accepted": accepted(case),
        "query_http_status": case.get("query_http_status"),
        "upload_http_status": case.get("upload_status"),
        "download_http_status": case.get("download_http_status"),
        "native_terminal_status": safe_word(native_record(case).get("status")),
        "stream_finished": case.get("done_finished"),
        "target_values_correct": case.get("answer_correct"),
        "delivery_complete": case.get("delivery_complete"),
        "correctness_check": "exact_downloaded_csv_groups_and_totals" if case.get("scene") in {"road_traffic", "statistics_table"} else "target_numbers_present_with_numeric_boundaries_and_nonempty_output",
        "errors": [{"code": error.get("code"), "status": safe_word(error.get("status"))} for error in case.get("errors", [])],
        "exception_type": safe_word(case.get("exception_type")) if case.get("exception_type") else None,
        "observed_status_transitions": [{"status": safe_word(item.get("status")), "seconds_from_query": item.get("seconds")} for item in case.get("statuses", [])],
        "timings_seconds": {new: case[old] for old, new in TIMINGS.items() if number(case.get(old))},
    }
    native_lifetime = lifetime(case)
    if number(native_lifetime):
        result["timings_seconds"][TIMINGS["native_lifetime_seconds"]] = native_lifetime
    return result


def timing_summary(cases):
    result = {}
    for old, new in TIMINGS.items():
        values = [lifetime(case) if old == "native_lifetime_seconds" else case.get(old) for case in cases]
        result[new] = quantiles(values)
    return result


def memory_mib(value):
    match = re.match(r"\s*([0-9.]+)\s*([A-Za-z]+)", str(value or "").split("/", 1)[0])
    if not match:
        return None
    unit = match.group(2).lower()
    factors = {"b": 1 / 1048576, "kb": 1000 / 1048576, "mb": 1000000 / 1048576, "gb": 1000000000 / 1048576, "tb": 1000000000000 / 1048576, "kib": 1 / 1024, "mib": 1, "gib": 1024, "tib": 1048576}
    return float(match.group(1)) * factors[unit] if unit in factors else None


def service_role(name):
    value = str(name or "").lower()
    if "sandbox" in value:
        return "sandbox"
    if "langgraph" in value:
        return "langgraph"
    if "gateway" in value:
        return "gateway"
    return "onecity"


def resource_summary(samples):
    services = defaultdict(lambda: {"samples": 0, "cpu_peak_percent": 0.0, "memory_peak_mib": 0.0})
    gpu = {}
    by_time = defaultdict(lambda: {"cpu": 0.0, "memory": 0.0})
    for sample in samples:
        if "gpu" in sample:
            index = str(sample["gpu"])
            item = gpu.setdefault(index, {"samples": 0, "utilization_peak_percent": 0.0, "memory_peak_mib": 0.0})
            item["samples"] += 1
            item["utilization_peak_percent"] = max(item["utilization_peak_percent"], float(sample.get("utilization_percent") or 0))
            item["memory_peak_mib"] = max(item["memory_peak_mib"], float(sample.get("memory_used_mib") or 0))
            continue
        if not sample.get("Name"):
            continue
        role = service_role(sample["Name"])
        item = services[role]
        item["samples"] += 1
        cpu_text = str(sample.get("CPUPerc") or "0").rstrip("%")
        try:
            cpu = float(cpu_text)
        except ValueError:
            cpu = 0.0
        memory = memory_mib(sample.get("MemUsage")) or 0.0
        item["cpu_peak_percent"] = round(max(item["cpu_peak_percent"], cpu), 3)
        item["memory_peak_mib"] = round(max(item["memory_peak_mib"], memory), 3)
        by_time[(sample.get("elapsed"), role)]["cpu"] += cpu
        by_time[(sample.get("elapsed"), role)]["memory"] += memory
    for role, item in services.items():
        groups = [value for (elapsed, group_role), value in by_time.items() if group_role == role]
        item["sampled_group_cpu_peak_percent"] = round(max((value["cpu"] for value in groups), default=0), 3)
        item["sampled_group_memory_peak_mib"] = round(max((value["memory"] for value in groups), default=0), 3)
    return {
        "services": dict(services),
        "host_gpu": gpu,
        "sampling_interval_seconds": 3,
        "cpu_unit": "Docker CPU percent; 100 percent approximates one CPU core",
        "memory_unit": "MiB; parsed from Docker decimal or binary memory units",
        "gpu_scope": "Shared local host GPUs only. The configured model endpoint's remote GPU is not observable here.",
        "sandbox_scope": "Matching validation sandboxes present at each sample, including idle containers from preceding waves.",
    }


def observed_intervals(cases):
    intervals = []
    pending_intervals = []
    for case in cases:
        origin = case.get("query_clock")
        end = case.get("finished_clock")
        states = case.get("statuses", [])
        if not number(origin) or not number(end):
            continue
        running = next((item.get("seconds") for item in states if item.get("status") == "running" and number(item.get("seconds"))), None)
        pending = next((item.get("seconds") for item in states if item.get("status") == "pending" and number(item.get("seconds"))), None)
        if number(running) and origin + running <= end:
            intervals.append((origin + running, end))
        if number(pending):
            pending_end = origin + running if number(running) else end
            if origin + pending < pending_end:
                pending_intervals.append((origin + pending, pending_end))
    events = [(point, delta) for start, end in intervals for point, delta in [(start, 1), (end, -1)]]
    count = maximum = 0
    pending_while_three = False
    for point, delta in sorted(events):
        count += delta
        maximum = max(maximum, count)
        if count >= 3 and any(start <= point < end for start, end in pending_intervals):
            pending_while_three = True
    return {
        "client_observed_active_interval_overlap": maximum,
        "observed_pending_while_multiple_started_intervals": pending_while_three,
        "running_intervals_observed": len(intervals),
        "poll_interval_seconds": 1,
        "method": "First observed running status plus query_clock until client verification finished_clock. Created_at is never treated as execution start.",
        "limitation": "Polling can miss short states. End includes SSE delivery, downloads and native terminal confirmation. This is client activity overlap, not a native execution or worker count.",
    }


def summarize_run(name, label, workers, source):
    cases = source.get("cases", [])
    admitted = [case for case in cases if accepted(case)]
    successful = sum(case.get("passed") is True for case in cases)
    capacity = sum(capacity_rejected(case) for case in cases)
    uploads = sum(outcome(case) == "upload_rejected" for case in cases)
    upload_transport = sum(outcome(case) == "upload_transport_failed" for case in cases)
    incomplete = sum(accepted(case) and case.get("passed") is not True for case in cases)
    return {
        "name": name,
        "label": label,
        "configured_workers": workers,
        "waves": source.get("waves"),
        "concurrent_requests": source.get("concurrency"),
        "account_count": source.get("account_count"),
        "elapsed_seconds": source.get("elapsed_seconds"),
        "requests": len(cases),
        "successful": successful,
        "overall_success": fraction(successful, len(cases)),
        "accepted_task_success": fraction(sum(case.get("passed") is True for case in admitted), len(admitted)),
        "capacity_rejections": fraction(capacity, len(cases)),
        "explicit_upload_rejections": fraction(uploads, len(cases)),
        "upload_transport_failures": fraction(upload_transport, len(cases)),
        "accepted_task_incomplete": fraction(incomplete, len(admitted)),
        "accepted_completion_detail": completion_counts(admitted),
        "timings_accepted_only": timing_summary(admitted),
        "timings_accepted_successful_only": timing_summary([case for case in admitted if case.get("passed") is True]),
        "observed_concurrency": observed_intervals(admitted),
        "resources": resource_summary(source.get("resources", [])),
        "cases": [safe_case(case) for case in cases],
    }


def markdown(report):
    lines = [
        "# OneCityAgent 真实业务并发与持续负载结果",
        "",
        "本次使用四个独立测试账号，通过已配置的 Qwen3.6-35B-A3B 实际业务模型执行问答、合成道路交通 CSV、城市统计 CSV 与政策资料阅读任务。"
        "文件从 190 字节到 177613 字节，CSV 从 16 行到 16000 行。旧基线另保留超过完整扫描上限的 363737 字节 / 32768 行拒绝样例。",
        "",
        "| 批次 | 请求/并发 | 成功 | 受理任务成功 | 容量拒绝 | 明确上传拒绝 | 上传传输失败 | 总时长秒 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run in report["runs"]:
        ratio = run["accepted_task_success"]
        lines.append(
            f"| {run['label']} | {run['requests']}/{run['concurrent_requests']} | {run['successful']}/{run['requests']} | "
            f"{ratio['numerator']}/{ratio['denominator']} | {run['capacity_rejections']['numerator']} | "
            f"{run['explicit_upload_rejections']['numerator']} | {run['upload_transport_failures']['numerator']} | {run['elapsed_seconds']} |"
        )
    total = report["all_campaigns"]
    final = report["final_campaigns"]
    lines.extend(
        [
            "",
            f"全部历史与整改批次共 {total['requests']} 次请求，实际业务通过 {total['successful']} 次。"
            f"最终版本的持续负载与容量边界共 {final['requests']} 次请求，{final['successful']} 次业务通过，{final['capacity_rejections']} 次容量拒绝，"
            f"受理任务 {final['accepted_successes']}/{final['accepted']} 成功。历史失败均按实际结果保留。",
            "最终持续负载包含六轮、每轮四个并发请求，共 24 项，持续 317.612 秒（约 5 分 18 秒）。该窗口覆盖本次持续负载验收，全天稳定性仍需更长观测。",
            "",
            "## 最终版本时延",
            "",
            "下表仅统计已受理任务，容量 429 和上传阶段拒绝不进入这些时延分位数。p50 和 p95 由逐任务记录按 nearest-rank 重算。",
            "",
            "| 批次 | 指标 | 样本数 | 平均秒 | p50 秒 | p95 秒 | 最大秒 |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for run in report["runs"]:
        if run["name"] not in {"final-overload", "final-soak", "fixed-jobs2", "fixed-jobs3-retest"}:
            continue
        for key, label in [(TIMINGS["queue_seconds"], "问答请求至首次观测 running"), (TIMINGS["first_answer_seconds"], "首意图/规划/回答反馈"), (TIMINGS["model_finished_seconds"], "问答请求至 SSE 结束"), (TIMINGS["total_seconds"], "请求至结果校验结束")]:
            values = run["timings_accepted_only"][key]
            if values["count"]:
                lines.append(f"| {run['label']} | {label} | {values['count']} | {values['mean']} | {values['p50']} | {values['p95']} | {values['max']} |")
    lines.extend(
        [
            "",
            "`queue_seconds` 从发起问答到约一秒轮询首次看到 running，包含 HTTP、IAM 和路由时间，是观测到的受理至执行时延。首反馈可能是意图或规划，SSE 结束时间包含工具、合规和传输。native created→updated 同样包含排队，不能视为纯执行或纯模型时长。",
            "",
            "## 故障与整改保留",
            "",
            "- 原基线的超限 CSV 在上传阶段拒绝，原先 OneCity 返回 code 0 却无附件。整改后拒绝原因与审计记录透传，失败文件撤回。",
            "- 早期持续负载中两项原生图最终 success，但客户端终态检查时仍 running，原记录保持失败。两执行任务批次也保留一项原生 success 而流式 finished=false 的失败。",
            "- 三执行任务首轮因另一路超限重复文本触发正则平方级回溯，Gateway 单核持续占用，导致 12 次请求全部失败。其中四个原生图最终 success，流式交付仍失败，不能计为成功。修复已知未完成内容跳过检测器与箭头结构正则后，三执行任务复测 12/12 通过。",
            "- 最终八请求负载中两个请求收到容量 429，其余六项均完成并校验。容量拒绝计入总请求分母，排除在已受理任务时延与成功率分母之外。",
            "",
            "## 资源与观测并发",
            "",
            "| 批次 | 服务 | CPU 峰值百分比 | 单容器内存峰值 MiB | 同采样组内存峰值 MiB |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for run in report["runs"]:
        if run["name"] not in {"fixed-jobs3", "final-overload", "final-soak"}:
            continue
        for role, values in run["resources"]["services"].items():
            lines.append(f"| {run['label']} | {role} | {values['cpu_peak_percent']} | {values['memory_peak_mib']} | {values['sampled_group_memory_peak_mib']} |")
    lines.extend(["", "Docker CPU 100% 约表示占用一个 CPU 核。内存按 Docker 报告单位转换为 MiB，沙箱组统计含前序批次遗留的空闲测试沙箱。资源约三秒采样，表中峰值为采样峰值。", "", "| 批次 | 宿主 GPU | 利用率采样峰值百分比 | 已用显存采样峰值 MiB |", "| --- | --- | --- | --- |"])
    for run in report["runs"]:
        for index, values in run["resources"]["host_gpu"].items():
            lines.append(f"| {run['label']} | {index} | {values['utilization_peak_percent']} | {values['memory_peak_mib']} |")
    lines.extend(
        [
            "",
            "宿主 GPU 采样反映共享本机显卡，无法观测配置中的远端模型 GPU，也不能据此归因本任务的 GPU 用量。未记录 GPU 样本的历史批次保持缺测。",
            "",
            "客户端活动区间从首次 running 观测延续到结果校验结束。轮询可能漏掉短状态，终点又包含传输、下载与终态确认。因此 `client_observed_active_interval_overlap` 只描述客户端活动重叠，不能证明 worker 数量或精确原生执行并发。本次状态记录观察到多个任务 running，同时另有任务 pending。",
            "",
            "## 两执行任务与三执行任务比较",
            "",
            report["worker_comparison"]["interpretation"],
            "",
            "问答和政策材料只检查目标数字以完整数字边界出现及非空交付，CSV 检查下载文件的分组、行数与汇总值完全一致。这些检查覆盖本次明确任务要求，不能作为一般语义问答准确率。",
            "",
            "逐任务公开记录仅保留测试标签、场景、文件规模、状态、时延、校验布尔值与错误码。原始答案、账号凭据、会话/线程/任务 ID 和服务端点均留在私有测试目录。",
            "",
        ]
    )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private-base", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("docs/validation-20261125"))
    args = parser.parse_args()
    reports = []
    hashes = {}
    for name, label, workers in RUNS:
        path = args.private_base / name / "summary.json"
        content = path.read_bytes()
        source = json.loads(content)
        reports.append(summarize_run(name, label, workers, source))
        hashes[name] = hashlib.sha256(content).hexdigest()
    final = [run for run in reports if run["name"] in {"final-overload", "final-soak"}]
    jobs2 = next(run for run in reports if run["name"] == "fixed-jobs2")
    jobs3 = next(run for run in reports if run["name"] == "fixed-jobs3-retest")
    old = jobs2["timings_accepted_only"][TIMINGS["total_seconds"]]
    new = jobs3["timings_accepted_only"][TIMINGS["total_seconds"]]
    report = {
        "schema_version": "onecity_real_business_validation_1",
        "generated_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017 -- Python 3.10 compatibility
        "source_summary_sha256": hashes,
        "scope": "Real configured business model and actual tools through OneCity multiuser APIs, synthetic test data",
        "business_model": "Qwen3.6-35B-A3B",
        "quantile_method": "nearest rank: sorted[ceil(n*p)-1]; admitted task records only",
        "timing_field_definitions": TIMINGS,
        "all_campaigns": {"requests": sum(run["requests"] for run in reports), "successful": sum(run["successful"] for run in reports)},
        "final_campaigns": {
            "requests": sum(run["requests"] for run in final),
            "successful": sum(run["successful"] for run in final),
            "accepted": sum(run["accepted_task_success"]["denominator"] for run in final),
            "accepted_successes": sum(run["accepted_task_success"]["numerator"] for run in final),
            "capacity_rejections": sum(run["capacity_rejections"]["numerator"] for run in final),
        },
        "worker_comparison": {
            "two_workers": jobs2["name"],
            "three_workers": jobs3["name"],
            "observed_total_mean_seconds": {"two_workers": old.get("mean"), "three_workers": new.get("mean")},
            "observed_mean_change_percent": round((new["mean"] / old["mean"] - 1) * 100, 3) if old.get("mean") else None,
            "interpretation": (
                f"本次两执行任务 12 请求通过 {jobs2['successful']} 项，三执行任务修复后 12 请求通过 {jobs3['successful']} 项。"
                f"已受理任务总时长均值从 {old.get('mean')} 秒变为 {new.get('mean')} 秒，批次墙钟时间从 {jobs2['elapsed_seconds']} 秒变为 {jobs3['elapsed_seconds']} 秒。"
                "样本为固定顺序的小规模合成任务，两批代码包含正则与异常处置差异、环境冷启动状态不同，也未随机化，观测改善不能单独归因于 worker 数量。"
                "三执行任务配置的验证范围限本机、本次模型服务和任务规模。"
            ),
        },
        "runs": reports,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "business-load.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    (args.output_dir / "business-load.md").write_text(markdown(report))
    print(json.dumps({"requests": report["all_campaigns"]["requests"], "final": report["final_campaigns"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
