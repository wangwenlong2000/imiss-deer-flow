#!/usr/bin/env python3
"""Offline OpenGrep runner for the opengrep-compliance DeerFlow skill."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def skill_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_binary() -> Path:
    system = platform.system().lower()
    vendor_root = skill_root() / "vendor" / "opengrep"
    if system == "windows":
        return vendor_root / "windows" / "opengrep.exe"
    if system == "darwin":
        return vendor_root / "darwin" / "opengrep"
    return vendor_root / "linux" / "opengrep"


def has_rule_files(rules_dir: Path) -> bool:
    return any(rules_dir.rglob("*.yaml")) or any(rules_dir.rglob("*.yml"))


def load_json_report(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def summarize(report: dict[str, Any], json_report: Path, sarif_report: Path, returncode: int) -> dict[str, Any]:
    results = report.get("results") or []
    severities = Counter()
    top_findings = []

    for item in results:
        extra = item.get("extra") or {}
        severity = str(extra.get("severity") or "UNKNOWN").upper()
        severities[severity] += 1

        if len(top_findings) < 25:
            start = item.get("start") or {}
            top_findings.append(
                {
                    "rule_id": item.get("check_id"),
                    "path": item.get("path"),
                    "line": start.get("line"),
                    "severity": severity,
                    "message": extra.get("message"),
                }
            )

    return {
        "returncode": returncode,
        "total_findings": len(results),
        "findings_by_severity": dict(sorted(severities.items())),
        "top_findings": top_findings,
        "json_report": str(json_report),
        "sarif_report": str(sarif_report),
    }


def render_markdown_report(summary: dict[str, Any], target: Path, rules_dir: Path) -> str:
    lines = [
        "# OpenGrep Rule Scan Report",
        "",
        "This is layer 1 of the hybrid compliance workflow: deterministic OpenGrep rule scanning.",
        "",
        "## Layer 1 Summary",
        "",
        f"- Scan target: `{target}`",
        f"- Rules directory: `{rules_dir}`",
        f"- OpenGrep return code: `{summary.get('returncode')}`",
        f"- Total findings: `{summary.get('total_findings', 0)}`",
        "",
        "## Findings By Severity",
        "",
    ]

    findings_by_severity = summary.get("findings_by_severity") or {}
    if findings_by_severity:
        for severity, count in findings_by_severity.items():
            lines.append(f"- `{severity}`: `{count}`")
    else:
        lines.append("- No findings.")

    lines.extend(["", "## Top Findings", ""])
    top_findings = summary.get("top_findings") or []
    if top_findings:
        lines.append("| Severity | Rule | Location | Message |")
        lines.append("| --- | --- | --- | --- |")
        for finding in top_findings:
            severity = str(finding.get("severity") or "UNKNOWN")
            rule_id = str(finding.get("rule_id") or "")
            path = str(finding.get("path") or "")
            line = finding.get("line")
            message = str(finding.get("message") or "").replace("|", "\\|").replace("\n", " ")
            location = f"{path}:{line}" if line else path
            lines.append(f"| `{severity}` | `{rule_id}` | `{location}` | {message} |")
    else:
        lines.append("No findings were reported by OpenGrep.")

    lines.extend(
        [
            "",
            "## Report Files",
            "",
            f"- JSON report: `{summary.get('json_report')}`",
            f"- SARIF report: `{summary.get('sarif_report')}`",
            f"- Summary report: `{summary.get('summary_report')}`",
            f"- Rule scan report: `{summary.get('rule_report')}`",
            f"- LLM analysis template: `{summary.get('llm_analysis_template')}`",
            "",
            "## Layer 2 LLM Analysis Tasks",
            "",
            "Use the findings above plus source-code context to perform the second layer of analysis:",
            "",
            "1. Connect related findings into possible attack chains.",
            "2. Recommend concrete remediation steps and code changes.",
            "3. Evaluate likely false positives and explain why.",
            "4. Prioritize findings by exploitability, blast radius, and fix cost.",
            "",
            "## Suggested Review Workflow",
            "",
            "1. Fix confirmed `ERROR` findings first.",
            "2. Review `WARNING` findings and document accepted exceptions.",
            "3. Re-run this skill after remediation to confirm the finding count is reduced.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_llm_analysis_template(summary: dict[str, Any], target: Path) -> str:
    return "\n".join(
        [
            "# Hybrid OpenGrep + LLM Analysis",
            "",
            "This document is the layer 2 analysis workspace for DeerFlow after OpenGrep finishes layer 1 rule scanning.",
            "",
            "## Inputs",
            "",
            f"- Scan target: `{target}`",
            f"- Rule scan report: `{summary.get('rule_report')}`",
            f"- JSON report: `{summary.get('json_report')}`",
            f"- Summary report: `{summary.get('summary_report')}`",
            f"- Total OpenGrep findings: `{summary.get('total_findings', 0)}`",
            "",
            "## Layer 1: Deterministic Rule Findings",
            "",
            "OpenGrep findings are fast, deterministic, and grounded in local YAML rules. Treat them as evidence, not as the final risk narrative.",
            "",
            "## Layer 2: LLM Deep Analysis",
            "",
            "Fill this section after reading the relevant source code and OpenGrep findings.",
            "",
            "### Attack Chain Analysis",
            "",
            "- Connect related findings across files, entry points, data flows, configuration, and execution paths.",
            "- Explain realistic exploit paths only when the code context supports them.",
            "",
            "### Concrete Fix Recommendations",
            "",
            "- Provide specific code-level remediation guidance.",
            "- Include replacement snippets when the fix is local and low risk.",
            "",
            "### False Positive And Priority Assessment",
            "",
            "- Mark findings as confirmed, likely, possible, or likely false positive.",
            "- Prioritize by exploitability, exposed surface, sensitive data impact, and fix cost.",
            "",
            "### Final Remediation Plan",
            "",
            "1. Address confirmed high-impact issues.",
            "2. Batch related medium-risk fixes.",
            "3. Document accepted exceptions and add suppressions only with justification.",
            "",
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline OpenGrep compliance scan.")
    parser.add_argument("--target", default=".", help="File or directory to scan.")
    parser.add_argument("--output-dir", default="reports/opengrep", help="Directory for reports.")
    parser.add_argument("--rules", default=None, help="Local rules directory. Defaults to this skill's rules directory.")
    parser.add_argument("--binary", default=None, help="OpenGrep binary path. Defaults to this skill's vendored binary.")
    parser.add_argument("--timeout", type=int, default=600, help="Scan timeout in seconds.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binary = Path(args.binary).resolve() if args.binary else default_binary()
    rules_dir = Path(args.rules).resolve() if args.rules else skill_root() / "rules"
    target = Path(args.target).resolve()
    output_dir = Path(args.output_dir).resolve()
    json_report = output_dir / "opengrep.json"
    sarif_report = output_dir / "opengrep.sarif"
    summary_report = output_dir / "summary.json"
    markdown_report = output_dir / "report.md"
    rule_report = output_dir / "rule-report.md"
    llm_analysis_template = output_dir / "llm-analysis-template.md"

    if not binary.exists():
        raise FileNotFoundError(f"OpenGrep binary not found: {binary}")
    if not rules_dir.exists():
        raise FileNotFoundError(f"OpenGrep rules directory not found: {rules_dir}")
    if not has_rule_files(rules_dir):
        raise FileNotFoundError(f"No YAML rule files found under: {rules_dir}")
    if not target.exists():
        raise FileNotFoundError(f"Scan target does not exist: {target}")

    output_dir.mkdir(parents=True, exist_ok=True)

    command = [
        str(binary),
        "scan",
        "--config",
        str(rules_dir),
        "--json-output",
        str(json_report),
        "--sarif-output",
        str(sarif_report),
        str(target),
    ]

    completed = subprocess.run(command, text=True, capture_output=True, timeout=args.timeout)
    report = load_json_report(json_report)
    summary = summarize(report, json_report, sarif_report, completed.returncode)
    summary["summary_report"] = str(summary_report)
    summary["markdown_report"] = str(markdown_report)
    summary["rule_report"] = str(rule_report)
    summary["llm_analysis_template"] = str(llm_analysis_template)
    summary["stdout_tail"] = completed.stdout[-4000:]
    summary["stderr_tail"] = completed.stderr[-4000:]

    rendered_rule_report = render_markdown_report(summary, target, rules_dir)
    rendered_llm_template = render_llm_analysis_template(summary, target)
    summary_report.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    rule_report.write_text(rendered_rule_report, encoding="utf-8")
    llm_analysis_template.write_text(rendered_llm_template, encoding="utf-8")
    markdown_report.write_text(rendered_rule_report + "\n" + rendered_llm_template, encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    return completed.returncode


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.TimeoutExpired as exc:
        print(json.dumps({"error": "OpenGrep scan timed out", "timeout": exc.timeout}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(124)
