#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from utils.path import outputs_root, to_repo_relative_display


DEFAULT_ACTIONS = [
    "inspect",
    "overview-report",
    "signature-review",
    "dns-tunnel-review",
    "data-exfiltration-review",
    "lateral-movement-review",
    "risk-fusion-review",
]


def sanitize_name(value: str) -> str:
    filtered = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in value.strip())
    return filtered.strip("-._") or "network-traffic"


def default_output_file(files: list[str]) -> Path:
    first = Path(files[0]).name if files else "network-traffic"
    stem = first
    for suffix in (".flow.csv", ".packet.csv", ".csv", ".jsonl", ".json", ".parquet", ".xlsx", ".xls"):
        if stem.lower().endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return outputs_root() / f"{sanitize_name(stem)}_Incident_Report.md"


def parse_json_payload(stdout: str) -> dict[str, Any]:
    text = stdout.strip()
    if not text:
        raise ValueError("empty stdout")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(text[start : end + 1])


def run_action(
    *,
    script_dir: Path,
    files: list[str],
    action: str,
    limit: int,
    view: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(script_dir / "analyze.py"),
        "--files",
        *files,
        "--action",
        action,
        "--format",
        "skill-result-json",
        "--limit",
        str(limit),
    ]
    if view != "auto":
        cmd.extend(["--view", view])

    completed = subprocess.run(
        cmd,
        cwd=str(script_dir),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_seconds,
        check=False,
    )
    payload: dict[str, Any]
    if completed.stdout.strip():
        try:
            payload = parse_json_payload(completed.stdout)
        except Exception as exc:
            payload = {
                "status": "failed",
                "capability": action,
                "errors": [
                    {
                        "code": "REPORT_ACTION_JSON_PARSE_FAILED",
                        "message": f"Could not parse JSON output for {action}: {exc}",
                    }
                ],
                "diagnostics": {"stdout": completed.stdout[-2000:], "stderr": completed.stderr[-2000:]},
            }
    else:
        payload = {
            "status": "failed",
            "capability": action,
            "errors": [
                {
                    "code": "REPORT_ACTION_NO_OUTPUT",
                    "message": f"{action} produced no stdout.",
                }
            ],
            "diagnostics": {"stderr": completed.stderr[-2000:]},
        }

    payload.setdefault("capability", action)
    payload.setdefault("status", "failed" if completed.returncode else "success")
    payload.setdefault("_report_command", " ".join(cmd))
    payload.setdefault("_returncode", completed.returncode)
    if completed.returncode != 0 and not payload.get("errors"):
        payload["errors"] = [
            {
                "code": "REPORT_ACTION_FAILED",
                "message": f"{action} exited with code {completed.returncode}.",
            }
        ]
    return payload


def evidence_by_id(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    evidence = payload.get("result", {}).get("evidence", [])
    return {
        str(item.get("evidence_id")): item
        for item in evidence
        if isinstance(item, dict) and item.get("evidence_id")
    }


def markdown_escape(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")


def render_metrics(metrics: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    if not metrics:
        return lines
    lines.extend(["| Metric | Value |", "|---|---|"])
    for metric in metrics:
        lines.append(f"| {markdown_escape(metric.get('name'))} | {markdown_escape(metric.get('value'))} |")
    return lines


def render_table_evidence(evidence: dict[str, Any], *, row_limit: int = 10) -> list[str]:
    columns = evidence.get("columns") or []
    rows = evidence.get("rows") or []
    if not columns or not rows:
        return []
    selected_rows = rows[:row_limit]
    lines = [
        f"**{markdown_escape(evidence.get('title') or evidence.get('evidence_id'))}**",
        "",
        "| " + " | ".join(markdown_escape(column) for column in columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in selected_rows:
        lines.append("| " + " | ".join(markdown_escape(value) for value in row) + " |")
    if len(rows) > row_limit:
        lines.append(f"\n_Only first {row_limit} of {len(rows)} rows shown._")
    return lines


def render_action_section(payload: dict[str, Any]) -> list[str]:
    action = str(payload.get("capability") or "unknown")
    status = str(payload.get("status") or "unknown")
    result = payload.get("result") or {}
    summary = result.get("summary") or {}
    findings = result.get("findings") or []
    evidence_map = evidence_by_id(payload)
    diagnostics = payload.get("diagnostics") or {}
    warnings = diagnostics.get("warnings") or []
    errors = payload.get("errors") or []

    title = summary.get("title") or action.replace("-", " ").title()
    lines = [f"## {title}", "", f"- **Action**: `{action}`", f"- **Status**: `{status}`"]
    if summary.get("severity"):
        lines.append(f"- **Severity**: `{summary.get('severity')}`")
    if summary.get("confidence") is not None:
        lines.append(f"- **Confidence**: `{summary.get('confidence')}`")
    if summary.get("overview"):
        lines.extend(["", str(summary["overview"])])

    metrics = summary.get("key_metrics") or []
    if metrics:
        lines.extend(["", "### Key Metrics", "", *render_metrics(metrics)])

    if findings:
        lines.extend(["", "### Findings"])
        for finding in findings[:20]:
            refs = ", ".join(f"`{ref}`" for ref in finding.get("evidence_refs", []))
            lines.extend(
                [
                    "",
                    f"- **{markdown_escape(finding.get('severity', 'info')).upper()}** "
                    f"{markdown_escape(finding.get('title') or finding.get('finding_id'))}",
                    f"  - Type: `{markdown_escape(finding.get('type'))}`",
                    f"  - Confidence: `{markdown_escape(finding.get('confidence'))}`",
                    f"  - Evidence: {refs or 'none'}",
                    f"  - Detail: {markdown_escape(finding.get('description'))}",
                ]
            )

    table_evidence = [
        item for item in evidence_map.values()
        if item.get("type") == "table" and item.get("evidence_id") != "e-raw-report"
    ]
    if table_evidence:
        lines.extend(["", "### Evidence Tables"])
        for item in table_evidence[:3]:
            lines.extend(["", *render_table_evidence(item)])

    if warnings:
        lines.extend(["", "### Diagnostics"])
        for warning in warnings[:12]:
            lines.append(f"- `{markdown_escape(warning.get('code'))}`: {markdown_escape(warning.get('message'))}")
    if errors:
        lines.extend(["", "### Errors"])
        for error in errors:
            lines.append(f"- `{markdown_escape(error.get('code'))}`: {markdown_escape(error.get('message'))}")
    return lines


def summarize_run(action_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    severity_rank = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0, "error": 4}
    highest = "info"
    finding_count = 0
    failed_actions: list[str] = []
    for payload in action_payloads:
        if payload.get("status") != "success":
            failed_actions.append(str(payload.get("capability")))
        summary = payload.get("result", {}).get("summary", {})
        severity = str(summary.get("severity") or "info").lower()
        if severity_rank.get(severity, 0) > severity_rank.get(highest, 0):
            highest = severity
        finding_count += len(payload.get("result", {}).get("findings") or [])
    return {
        "highest_severity": highest,
        "finding_count": finding_count,
        "failed_actions": failed_actions,
    }


def build_report(*, files: list[str], actions: list[str], action_payloads: list[dict[str, Any]]) -> str:
    now = datetime.now(timezone.utc).isoformat()
    run_summary = summarize_run(action_payloads)
    lines = [
        "# Network Traffic Incident Report",
        "",
        f"- **Generated At**: {now}",
        f"- **Input Files**: {len(files)}",
        f"- **Actions Run**: {len(actions)}",
        f"- **Highest Severity**: `{run_summary['highest_severity']}`",
        f"- **Structured Findings**: {run_summary['finding_count']}",
    ]
    for file_path in files:
        lines.append(f"- **Source**: `{to_repo_relative_display(file_path)}`")
    if run_summary["failed_actions"]:
        lines.append(f"- **Failed Actions**: {', '.join(f'`{item}`' for item in run_summary['failed_actions'])}")

    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- This report is generated inside the analysis runtime and written to the user-data output mount.",
            "- Structured findings and evidence are produced by `analyze.py --format skill-result-json`.",
            "- Treat diagnostics and missing-field warnings as part of the trust context.",
            "- Flow metadata can indicate exfiltration or C2 behavior, but payload content requires packet or endpoint forensics.",
            "",
        ]
    )
    for payload in action_payloads:
        lines.extend(render_action_section(payload))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a Markdown incident report from structured network traffic analyses.")
    parser.add_argument("--files", nargs="+", required=True, help="Input traffic files")
    parser.add_argument("--output-file", default=None, help="Output Markdown file. Defaults to /mnt/user-data/outputs/<dataset>_Incident_Report.md")
    parser.add_argument("--actions", nargs="+", default=DEFAULT_ACTIONS, help="Analysis actions to run before report generation")
    parser.add_argument("--limit", type=int, default=30, help="Per-action row/finding limit")
    parser.add_argument("--view", choices=["auto", "flow", "packet"], default="auto", help="Preferred analysis view passed to analyze.py")
    parser.add_argument("--timeout-seconds", type=int, default=180, help="Timeout per action")
    parser.add_argument("--format", choices=["json", "text"], default="text", help="CLI result format")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    files = [str(Path(item)) for item in args.files]
    output_file = Path(args.output_file).expanduser() if args.output_file else default_output_file(files)
    script_dir = Path(__file__).resolve().parent

    try:
        payloads = [
            run_action(
                script_dir=script_dir,
                files=files,
                action=action,
                limit=args.limit,
                view=args.view,
                timeout_seconds=args.timeout_seconds,
            )
            for action in args.actions
        ]
        report = build_report(files=files, actions=args.actions, action_payloads=payloads)
        atomic_write_text(output_file, report)
        result = {
            "status": "success",
            "output_file": str(output_file),
            "artifacts": [
                {
                    "type": "report",
                    "path": str(output_file),
                    "mime_type": "text/markdown",
                }
            ],
            "bytes_written": output_file.stat().st_size,
            "actions": [
                {
                    "action": payload.get("capability"),
                    "status": payload.get("status"),
                    "findings": len(payload.get("result", {}).get("findings") or []),
                    "errors": payload.get("errors", []),
                }
                for payload in payloads
            ],
        }
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"Incident report written to {output_file} ({result['bytes_written']} bytes)")
        return 0
    except subprocess.TimeoutExpired as exc:
        print(f"Error: action timed out after {exc.timeout} seconds", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
