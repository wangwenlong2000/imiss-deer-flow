#!/usr/bin/env python3
"""Network traffic analysis entry point.

This module serves as the CLI entry point. All business logic has been
refactored into purpose-specific modules under utils/, actions/, and
sibling packages (anomaly_models, feature_engineering, etc.).
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Safe duckdb import — does not crash if duckdb is missing
try:
    import duckdb as _duckdb_module  # type: ignore
    _DUCKDB_AVAILABLE = True
    _DUCKDB_EXCEPTIONS = (
        _duckdb_module.ParserException,
        _duckdb_module.BinderException,
        _duckdb_module.CatalogException,
        _duckdb_module.ConversionException,
    )
    _DUCKDB_EXCEPTION_MAP = {
        "ParserException": _duckdb_module.ParserException,
        "BinderException": _duckdb_module.BinderException,
        "CatalogException": _duckdb_module.CatalogException,
        "ConversionException": _duckdb_module.ConversionException,
        "IOException": _duckdb_module.IOException,
    }
except ImportError:
    _duckdb_module = None  # type: ignore
    _DUCKDB_AVAILABLE = False
    _DUCKDB_EXCEPTIONS = (Exception,)
    _DUCKDB_EXCEPTION_MAP = {}

# Import all action functions from their dedicated modules
from actions import (
    detect_anomaly_action,
    inspect_action,
    overview_report_action,
    packet_review_action,
    periodicity_review_action,
    protocol_drift_section,
    protocol_review_action,
    scan_review_action,
    session_review_action,
    short_connection_review_action,
    timeseries_action,
)
from actions.behavior_analysis_action import execute_behavior_analysis
from actions.device_identification_action import execute_device_identification
from actions.encrypted_flow_analysis_action import execute_encrypted_flow_analysis
from actions.graph_analysis_action import execute_graph_analysis
from actions.qos_analysis_action import execute_qos_analysis
from actions.root_cause_action import execute_root_cause_analysis
from actions.threat_intel_match_action import execute_threat_intel_match
from actions.forecast_traffic_action import execute_forecast_analysis
from analysis.anomaly_models import score_generic_candidates, score_scan_candidates, score_session_candidates, score_short_connection_candidates, score_timeseries_rcf
from capability_catalog import build_capability_catalog, render_capability_catalog
from constants import (
    CACHE_DIR,
    CANONICAL_COLUMNS,
    CAPABILITY_GUIDANCE,
    FLOW_PREFERRED_FIELDS,
    NUMERIC_COLUMNS,
    PACKET_PREFERRED_FIELDS,
    SUPPORTED_ANOMALY_RULES,
    SUPPORTED_PATTERNS,
)
from utils.db import connect_build_db, connect_cached_db
from analysis.feature_engineering import failure_rate_candidate_sql, handshake_failure_candidate_sql, icmp_probe_candidate_sql, rare_port_candidate_sql, rows_from_query, rst_heavy_candidate_sql, scan_candidate_sql, session_candidate_sql, short_connection_candidate_sql, small_packet_burst_candidate_sql, source_microflow_summary_sql, volume_spike_candidate_sql
from file_resolution import get_default_search_roots, is_explicit_path_reference, normalize_name, resolve_reference
from utils.formatter import export_rows, format_rows, render_rows_section, render_section
from core.schema_mapping import (
    add_ip_udf,
    analysis_time_bucket_expr,
    available_canonical_fields,
    booleanish_expr,
    build_flows_view,
    build_where_clause,
    detect_mapping,
    ensure_required,
    execute_render,
    get_columns,
    infer_analysis_view,
    load_mapping,
    load_sources,
    metric_sql,
    numeric_expr,
    quote_identifier,
    quote_literal,
    relative_interval_seconds,
    sanitize_table_name,
    sql_literal,
    timestamp_expr,
)
from analysis.review import risk_fusion_review_action, signature_review_action
from analysis.signature_matching import scan_signature_hits
from utils.io import ensure_cache_dir, ensure_duckdb, ensure_pytz, ensure_yaml, load_json, save_json
from utils.math import (
    _coerce_event_seconds,
    _dominant_periodicity,
    _lag_autocorrelation,
    _mean_std,
    _safe_float_local,
    _safe_ratio_local,
    _safe_text,
    _shannon_entropy,
    _text_entropy_local,
    _zscore,
)
from utils.path import compute_cache_key, discover_files, repo_root, resolve_file_reference, to_repo_relative_display
from utils.zeek import (
    _discover_zeek_logs,
    _load_zeek_json_rows,
    _private_ip_predicate,
    _signature_source_candidates,
    _zeek_semantic_candidates,
    _zeek_value,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

duckdb = None
yaml = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze tabular network traffic logs")
    parser.add_argument("--files", nargs="+", default=[], help="File paths or directories")
    parser.add_argument(
        "--action",
        required=False,
        choices=sorted(build_capability_catalog()["actions"]),
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="Run runtime preflight checks and exit.",
    )
    parser.add_argument("--field-mapping", default=None, help="Path to field mapping YAML")
    parser.add_argument(
        "--ingestion-mode",
        choices=["lenient", "strict"],
        default="lenient",
        help="CSV ingestion mode. lenient skips/pads malformed CSV rows; strict fails on malformed CSV input.",
    )
    parser.add_argument("--filters", default=None, help="JSON array or object of filters")
    parser.add_argument("--group-by", default=None, help="Comma-separated group-by fields")
    parser.add_argument("--metrics", default=None, help="Comma-separated metrics like count,sum:bytes")
    parser.add_argument("--start-time", default=None, help="Inclusive time filter")
    parser.add_argument("--end-time", default=None, help="Inclusive time filter")
    parser.add_argument("--baseline-start", default=None, help="Behavior-analysis baseline window start timestamp")
    parser.add_argument("--baseline-end", default=None, help="Behavior-analysis baseline window end timestamp")
    parser.add_argument("--current-start", default=None, help="Behavior-analysis current window start timestamp")
    parser.add_argument("--current-end", default=None, help="Behavior-analysis current window end timestamp")
    parser.add_argument("--output-file", default=None, help="Export destination")
    parser.add_argument(
        "--format",
        choices=["table", "text", "skill-result-json"],
        default="table",
        help="Output format. Use skill-result-json for the shared custom SkillResult envelope.",
    )
    parser.add_argument("--sql", default=None, help="Custom SQL for query or export")
    parser.add_argument("--dimension", default="src_ip", help="Dimension for topn or distribution")
    parser.add_argument("--metric", default="bytes", help="Metric for topn")
    parser.add_argument("--limit", type=int, default=50, help="Row limit")
    parser.add_argument("--interval", choices=["second", "minute", "hour", "day"], default="hour", help="Timeseries bucket size")
    parser.add_argument(
        "--rule",
        default="scan-source",
        help="Anomaly rule. Run --action list-capabilities to see the current supported rules.",
    )
    parser.add_argument(
        "--anomaly-engine",
        choices=["rule", "iforest", "lof", "rcf", "hybrid"],
        default="hybrid",
        help="Anomaly scoring engine for detect-anomaly. rule preserves legacy SQL-only behavior.",
    )
    parser.add_argument("--view", choices=["auto", "flow", "packet"], default="auto", help="Preferred analysis view")
    parser.add_argument("--source-ip", default=None, help="Source IP for graph attack path analysis")
    parser.add_argument("--target-ip", default=None, help="Target IP for graph attack path analysis")
    parser.add_argument("--horizon", type=int, default=24, help="Forecast horizon (number of future periods)")
    parser.add_argument("--drift-metric", default="bytes", help="Numeric canonical field to monitor for concept drift")
    parser.add_argument(
        "--drift-order-by",
        choices=["analysis_time", "input_order"],
        default="analysis_time",
        help="Ordering used before evaluating the drift stream",
    )
    return parser


def _first_meaningful_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip().strip("#").strip()
        if stripped:
            return stripped
    return "Completed."


def _skill_result_title(action: str) -> str:
    return action.replace("-", " ").title()


def _skill_result_error(
    *,
    code: str,
    message: str,
    severity: str = "error",
    recoverable: bool = True,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    error = {
        "code": code,
        "message": message,
        "severity": severity,
        "recoverable": recoverable,
    }
    if details:
        error["details"] = details
    return error


def wrap_skill_result(
    *,
    action: str,
    files: list[str],
    output: str,
    status: str = "success",
    errors: list[dict[str, Any]] | None = None,
    warnings: list[dict[str, Any]] | None = None,
    result_overrides: dict[str, Any] | None = None,
    diagnostics_overrides: dict[str, Any] | None = None,
) -> str:
    now = datetime.now(timezone.utc).isoformat()
    errors = errors or []
    warnings = warnings or []
    result_overrides = result_overrides or {}
    diagnostics_overrides = diagnostics_overrides or {}
    evidence = []
    if output:
        evidence.append(
            {
                "evidence_id": "e-001",
                "type": "text",
                "title": "Raw Action Output",
                "content": output,
            }
        )
    overview = _first_meaningful_line(output)
    if status != "success" and errors:
        overview = errors[0].get("message", "Failed.")

    result = {
        "summary": {
            "title": _skill_result_title(action),
            "overview": overview,
            "severity": "error" if status == "failed" else "info",
            "confidence": None,
            "key_metrics": [],
        },
        "findings": [],
        "evidence": evidence,
        "artifacts": [],
    }
    for key in ("summary", "findings", "evidence", "artifacts"):
        if key in result_overrides:
            result[key] = result_overrides[key]

    diagnostics = {
        "warnings": warnings,
        "data_quality": {
            "input_files": len(files),
        },
        "provenance": [
            {
                "source": to_repo_relative_display(file_path),
                "type": "input_file",
            }
            for file_path in files
        ],
        "runtime": {
            "finished_at": now,
        },
    }
    if diagnostics_overrides:
        diagnostics["warnings"] = diagnostics_overrides.get("warnings", diagnostics["warnings"])
        diagnostics["data_quality"].update(diagnostics_overrides.get("data_quality", {}))
        for key, value in diagnostics_overrides.items():
            if key not in {"warnings", "data_quality"}:
                diagnostics[key] = value

    payload = {
        "schema_version": "1.0",
        "request_id": str(uuid.uuid4()),
        "skill_name": "network-traffic-analysis",
        "scenario": "network_traffic",
        "capability": action,
        "status": status,
        "result": result,
        "diagnostics": diagnostics,
        "errors": errors,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def print_skill_result_error(
    *,
    args: argparse.Namespace,
    files: list[str],
    code: str,
    message: str,
    exit_code: int,
    recoverable: bool = True,
    details: dict[str, Any] | None = None,
) -> int:
    print(
        wrap_skill_result(
            action=args.action,
            files=files,
            output="",
            status="failed",
            errors=[
                _skill_result_error(
                    code=code,
                    message=message,
                    recoverable=recoverable,
                    details=details,
                )
            ],
        )
    )
    return exit_code


def run_self_check(output_format: str = "text") -> int:
    """Run runtime preflight checks. Returns 0 if all pass, 1 otherwise."""
    from utils.path import skill_root, repo_root, dataset_root, uploads_root, outputs_root
    from core.schema_mapping import default_field_mapping_candidates

    checks: list[dict[str, Any]] = []
    all_pass = True

    # 1. skill_root
    sr = skill_root()
    mounted_runtime = str(sr).startswith("/mnt/skills/")
    status = "ok" if sr.exists() else "missing"
    if status != "ok":
        all_pass = False
    checks.append({"name": "skill_root", "status": status, "path": str(sr)})

    # 2. dataset_root
    dr = dataset_root()
    status = "ok" if dr.exists() else "missing"
    checks.append({"name": "dataset_root", "status": status, "path": str(dr)})

    # 3. uploads_root
    ur = uploads_root()
    status = "ok" if ur.exists() else "missing"
    checks.append({"name": "uploads_root", "status": status, "path": str(ur)})

    # 4. outputs_root
    output_root = outputs_root()
    output_check: dict[str, Any] = {"name": "outputs_root", "path": str(output_root)}
    if output_root.exists() or output_root.parent.exists():
        try:
            output_root.mkdir(parents=True, exist_ok=True)
            probe_path = output_root / ".network_traffic_write_test"
            probe_path.write_text("ok", encoding="utf-8")
            if probe_path.read_text(encoding="utf-8") != "ok":
                raise OSError("write probe content mismatch")
            probe_path.unlink(missing_ok=True)
            output_check["status"] = "ok"
            output_check["writable"] = True
        except Exception as exc:
            output_check["status"] = "failed"
            output_check["writable"] = False
            output_check["error"] = str(exc)
            all_pass = False
    else:
        output_check["status"] = "missing"
        output_check["writable"] = False
        if mounted_runtime:
            all_pass = False
    checks.append(output_check)

    # 5. field_mapping
    mapping_found = False
    mapping_path = None
    searched: list[str] = []
    for candidate in default_field_mapping_candidates():
        searched.append(str(candidate))
        if candidate.exists():
            mapping_found = True
            mapping_path = candidate
            break
    status = "ok" if mapping_found else "failed"
    if status != "ok":
        all_pass = False
    checks.append({
        "name": "field_mapping",
        "status": status,
        "path": str(mapping_path) if mapping_path else "not found",
        "searched": searched,
    })

    # 6. duckdb
    if _DUCKDB_AVAILABLE:
        checks.append({"name": "duckdb", "status": "ok", "version": _duckdb_module.__version__})
    else:
        checks.append({"name": "duckdb", "status": "failed", "error": "not installed"})
        all_pass = False

    # 7. pyyaml
    try:
        import yaml as _yaml
        checks.append({"name": "pyyaml", "status": "ok", "version": getattr(_yaml, "__version__", "unknown")})
    except ImportError:
        checks.append({"name": "pyyaml", "status": "failed", "error": "not installed"})
        all_pass = False

    # 8. openpyxl
    try:
        import openpyxl as _openpyxl
        checks.append({"name": "openpyxl", "status": "ok", "version": getattr(_openpyxl, "__version__", "unknown")})
    except ImportError:
        checks.append({"name": "openpyxl", "status": "failed", "error": "not installed"})
        all_pass = False

    # 9. pytz
    try:
        import pytz as _pytz
        checks.append({"name": "pytz", "status": "ok", "version": getattr(_pytz, "__version__", "unknown")})
    except ImportError:
        checks.append({"name": "pytz", "status": "failed", "error": "not installed"})
        all_pass = False

    if output_format == "skill-result-json":
        errors = [
            {
                "code": f"SELF_CHECK_{check['name'].upper()}",
                "message": f"Self-check failed for {check['name']}",
                "details": check,
            }
            for check in checks
            if check["status"] not in {"ok", "missing"}
        ]
        print(json.dumps({
            "status": "success" if all_pass else "failed",
            "checks": checks,
            "errors": errors,
        }, indent=2, ensure_ascii=False))
        return 0 if all_pass else 1

    # Print results
    print("=== Network Traffic Analysis Self-Check ===")
    for check in checks:
        status = check["status"].upper()
        details = ", ".join(f"{k}={v}" for k, v in check.items() if k not in ("name", "status"))
        print(f"  [{status}] {check['name']}: {details}")
    print()
    if all_pass:
        print("All checks passed.")
        return 0
    else:
        failed = [c["name"] for c in checks if c["status"] != "ok"]
        print(f"FAILED: {', '.join(failed)}")
        return 1


def main() -> int:
    ensure_cache_dir()
    parser = build_parser()
    args = parser.parse_args()
    cleanup_db_copy: Path | None = None

    if args.self_check:
        return run_self_check(args.format)

    if not args.action:
        parser.error("--action is required (or use --self-check)")

    if args.action == "list-capabilities":
        output = render_capability_catalog()
        if args.format == "skill-result-json":
            print(wrap_skill_result(action=args.action, files=[], output=output))
        else:
            print(output)
        return 0

    try:
        ensure_pytz()
        ensure_duckdb()
        files = discover_files(args.files)
    except ValueError as exc:
        if args.format == "skill-result-json":
            return print_skill_result_error(
                args=args,
                files=[],
                code="INPUT_RESOLUTION_ERROR",
                message=f"Input resolution error: {exc}",
                exit_code=2,
            )
        parser.error(str(exc))
    except ImportError as exc:
        if args.format == "skill-result-json":
            return print_skill_result_error(
                args=args,
                files=[],
                code="MISSING_DEPENDENCY",
                message=f"Missing dependency: {exc}",
                exit_code=3,
                details={"hint": "Install required packages: pip install duckdb rrcf scikit-learn"},
            )
        logger.error(f"Missing dependency: {exc}\nHint: Install required packages: pip install duckdb rrcf scikit-learn")
        return 3
    if not files:
        if args.format == "skill-result-json":
            return print_skill_result_error(
                args=args,
                files=[],
                code="NO_SUPPORTED_INPUT_FILES",
                message="No supported files found from --files.",
                exit_code=2,
            )
        parser.error("No supported files found from --files")
    mapping = load_mapping(args.field_mapping)
    cache_key = compute_cache_key(files, mapping, ingestion_mode=args.ingestion_mode)
    db_path = CACHE_DIR / f"{cache_key}.duckdb"
    tables_path = CACHE_DIR / f"{cache_key}.tables.json"
    mappings_path = CACHE_DIR / f"{cache_key}.mappings.json"

    if db_path.exists() and tables_path.exists() and mappings_path.exists():
        con, cleanup_db_copy = connect_cached_db(db_path)
        table_info = load_json(tables_path) or {}
        mappings = load_json(mappings_path) or {}
        if args.format != "skill-result-json":
            logger.info(f"Cache hit: {db_path}")
    else:
        con, cleanup_db_copy, cache_ready = connect_build_db(db_path, tables_path, mappings_path)
        if cache_ready:
            table_info = load_json(tables_path) or {}
            mappings = load_json(mappings_path) or {}
            if args.format != "skill-result-json":
                logger.info(f"Cache became available during build wait: {db_path}")
        else:
            table_info = load_sources(con, files, ingestion_mode=args.ingestion_mode, quiet=args.format == "skill-result-json")
            if not table_info:
                message = "No tables were loaded. Check file paths and formats."
                if args.format == "skill-result-json":
                    return print_skill_result_error(
                        args=args,
                        files=files,
                        code="NO_TABLES_LOADED",
                        message=message,
                        exit_code=1,
                    )
                logger.error(message)
                return 1
            mappings = build_flows_view(con, table_info, mapping)
            save_json(tables_path, table_info)
            save_json(mappings_path, mappings)
            logger.info(f"Loaded {len(table_info)} source table(s)")

    add_ip_udf(con)
    mappings = build_flows_view(con, table_info, mapping)

    # Normalize simple dict filters (e.g. {"src_ip": "10.0.2.108"}) to list format
    # This allows users to use intuitive JSON objects instead of strict lists
    if args.filters:
        try:
            parsed = json.loads(args.filters)
            if isinstance(parsed, dict) and "field" not in parsed:
                args.filters = json.dumps([{"field": k, "value": v} for k, v in parsed.items()])
        except Exception:
            pass  # Let downstream code handle malformed JSON

    where_clause = build_where_clause(args.filters, args.start_time, args.end_time)
    analysis_view = infer_analysis_view(
        files,
        args.view,
        action=args.action,
        dimension=args.dimension,
        rule=args.rule,
    )

    try:
        if args.action == "inspect":
            output = inspect_action(con, table_info, mappings)
            if args.format == "skill-result-json":
                from actions.inspect_action import build_skill_result_parts as build_inspect_skill_result_parts
                structured_result = build_inspect_skill_result_parts(con, table_info, mappings, output)
        elif args.action == "summary":
            from actions.summary_action import execute_summary, format_results as format_summary, build_skill_result_parts as build_summary_skill_result_parts
            summary_results = execute_summary(con, mappings, where_clause, files, limit=args.limit)
            output = format_summary(summary_results)
            if args.format == "skill-result-json":
                structured_result = build_summary_skill_result_parts(con, summary_results, output)
        elif args.action == "overview-report":
            output = overview_report_action(con, mappings, where_clause, analysis_view)
        elif args.action == "scan-review":
            from actions.scan import execute_scan_review, format_scan_review, build_skill_result_parts as build_scan_skill_result_parts
            scan_data = execute_scan_review(con, mappings, where_clause, analysis_view, args.limit)
            output = format_scan_review(scan_data)
            if args.format == "skill-result-json":
                structured_result = build_scan_skill_result_parts(scan_data, output)
        elif args.action == "session-review":
            from actions.session import execute_session_review, format_session_review, build_skill_result_parts as build_session_skill_result_parts
            session_data = execute_session_review(con, mappings, where_clause, analysis_view, args.limit)
            output = format_session_review(session_data)
            if args.format == "skill-result-json":
                structured_result = build_session_skill_result_parts(session_data, output)
        elif args.action == "short-connection-review":
            from actions.short_connection import execute_short_connection_review, format_short_connection_review, build_skill_result_parts as build_short_connection_skill_result_parts
            short_data = execute_short_connection_review(con, mappings, where_clause, args.limit)
            output = format_short_connection_review(short_data)
            if args.format == "skill-result-json":
                structured_result = build_short_connection_skill_result_parts(short_data, output)
        elif args.action == "protocol-review":
            from actions.protocol_action import execute_protocol_review, format_protocol_review, build_skill_result_parts as build_protocol_skill_result_parts
            protocol_data = execute_protocol_review(con, mappings, where_clause, analysis_view, args.limit)
            output = format_protocol_review(protocol_data, con=con, where_clause=where_clause, limit=args.limit)
            if args.format == "skill-result-json":
                structured_result = build_protocol_skill_result_parts(protocol_data, output)
        elif args.action == "dns-tunnel-review":
            from actions.dns_tunnel import execute_dns_tunnel_review, format_dns_tunnel_review, build_skill_result_parts as build_dns_tunnel_skill_result_parts
            dns_data = execute_dns_tunnel_review(con, mappings, where_clause, files, args.limit)
            output = format_dns_tunnel_review(dns_data)
            if args.format == "skill-result-json":
                structured_result = build_dns_tunnel_skill_result_parts(dns_data, output)
        elif args.action == "data-exfiltration-review":
            from actions.data_exfiltration import execute_data_exfiltration_review, format_data_exfiltration_review, build_skill_result_parts as build_data_exfiltration_skill_result_parts
            exfil_data = execute_data_exfiltration_review(con, mappings, where_clause, args.limit)
            output = format_data_exfiltration_review(exfil_data)
            if args.format == "skill-result-json":
                structured_result = build_data_exfiltration_skill_result_parts(exfil_data, output)
        elif args.action == "lateral-movement-review":
            from actions.lateral_movement import execute_lateral_movement_review, format_lateral_movement_review, build_skill_result_parts as build_lateral_movement_skill_result_parts
            lateral_data = execute_lateral_movement_review(con, mappings, where_clause, args.limit)
            output = format_lateral_movement_review(lateral_data)
            if args.format == "skill-result-json":
                structured_result = build_lateral_movement_skill_result_parts(lateral_data, output)
        elif args.action == "zeek-review":
            from analysis.review import zeek_review_action
            from actions.zeek_review_adapter import execute_zeek_review, build_skill_result_parts as build_zeek_skill_result_parts
            output = zeek_review_action(files, args.limit)
            if args.format == "skill-result-json":
                zeek_data = execute_zeek_review(files, args.limit)
                structured_result = build_zeek_skill_result_parts(zeek_data, output, args.limit)
        elif args.action == "packet-review":
            from actions.packet import execute_packet_review, format_packet_review, build_skill_result_parts as build_packet_skill_result_parts
            packet_data = execute_packet_review(con, mappings, where_clause, args.limit)
            output = format_packet_review(packet_data)
            if args.format == "skill-result-json":
                structured_result = build_packet_skill_result_parts(packet_data, output)
        elif args.action == "signature-review":
            data = signature_review_action(con, mappings, where_clause, args.limit)
            from actions.signature_review_adapter import build_skill_result_parts as build_signature_skill_result_parts, render_text
            output = render_text(data, args.limit)
            if args.format == "skill-result-json":
                structured_result = build_signature_skill_result_parts(data)
        elif args.action == "risk-fusion-review":
            data = risk_fusion_review_action(con, mappings, where_clause, files, args.limit)
            from actions.risk_fusion_result_adapter import build_skill_result_parts as build_risk_fusion_skill_result_parts, render_text
            output = render_text(data, args.limit)
            if args.format == "skill-result-json":
                structured_result = build_risk_fusion_skill_result_parts(data)
        elif args.action == "periodicity-review":
            from actions.periodicity import execute_periodicity_review, format_periodicity_review, build_skill_result_parts as build_periodicity_skill_result_parts
            periodicity_data = execute_periodicity_review(con, mappings, where_clause, args.interval, args.limit)
            output = format_periodicity_review(periodicity_data)
            if args.format == "skill-result-json":
                structured_result = build_periodicity_skill_result_parts(periodicity_data, output)
        elif args.action == "query":
            if not args.sql:
                if args.format == "skill-result-json":
                    raise ValueError("--sql is required for query")
                parser.error("--sql is required for query")
            output = execute_render(con, args.sql, args.output_file)
        elif args.action == "topn":
            from actions.topn_action import execute_topn, format_results as format_topn, build_skill_result_parts as build_topn_skill_result_parts
            topn_results = execute_topn(con, mappings, where_clause, files, dimension=args.dimension, metric=args.metric, limit=args.limit)
            output = format_topn(topn_results)
            if args.format == "skill-result-json":
                structured_result = build_topn_skill_result_parts(topn_results, output)
        elif args.action == "timeseries":
            from actions.timeseries_action import execute_timeseries, format_results as format_timeseries, build_skill_result_parts as build_timeseries_skill_result_parts
            ts_results = execute_timeseries(con, where_clause, files, interval=args.interval, limit=args.limit, output_file=args.output_file)
            output = format_timeseries(ts_results)
            if args.format == "skill-result-json":
                structured_result = build_timeseries_skill_result_parts(ts_results, output)
        elif args.action == "distribution":
            from actions.distribution_action import execute_distribution, format_results as format_distribution, build_skill_result_parts as build_distribution_skill_result_parts
            dist_results = execute_distribution(con, mappings, where_clause, files, dimension=args.dimension, limit=args.limit)
            output = format_distribution(dist_results)
            if args.format == "skill-result-json":
                structured_result = build_distribution_skill_result_parts(dist_results, output)
        elif args.action == "filter":
            output = execute_render(
                con,
                f"SELECT * FROM flows {where_clause} ORDER BY analysis_time_ts NULLS LAST, analysis_time_relative_s NULLS LAST LIMIT {args.limit}",
                args.output_file,
            )
        elif args.action == "aggregate":
            from actions.aggregate_action import execute_aggregate, format_results as format_aggregate, build_skill_result_parts as build_aggregate_skill_result_parts
            agg_results = execute_aggregate(con, mappings, where_clause, files, group_by=args.group_by, metrics=args.metrics, limit=args.limit)
            output = format_aggregate(agg_results)
            if args.format == "skill-result-json":
                structured_result = build_aggregate_skill_result_parts(agg_results, output)
        elif args.action == "detect-anomaly":
            anomaly_result = detect_anomaly_action(
                con,
                mappings,
                where_clause,
                rule=args.rule,
                engine=args.anomaly_engine,
                limit=args.limit,
                output_file=args.output_file,
            )
            output = anomaly_result["text"]
            if args.format == "skill-result-json":
                structured_result = anomaly_result["skill_result"]
        elif args.action == "encrypted-flow-analysis":
            results = execute_encrypted_flow_analysis(con, mappings, where_clause, files, limit=args.limit, view=analysis_view)
            from actions.encrypted_flow_analysis_action import format_results as format_encrypted
            from actions.encrypted_flow_analysis_action import build_skill_result_parts as build_encrypted_skill_result_parts
            output = format_encrypted(results)
            if args.format == "skill-result-json":
                structured_result = build_encrypted_skill_result_parts(results, output)
        elif args.action == "device-identification":
            results = execute_device_identification(con, mappings, where_clause, files, limit=args.limit)
            from actions.device_identification_action import format_results as format_device
            from actions.device_identification_action import build_skill_result_parts as build_device_skill_result_parts
            output = format_device(results)
            if args.format == "skill-result-json":
                structured_result = build_device_skill_result_parts(results, output)
        elif args.action == "behavior-analysis":
            results = execute_behavior_analysis(
                con,
                mappings,
                where_clause,
                files,
                limit=args.limit,
                baseline_start=args.baseline_start,
                baseline_end=args.baseline_end,
                current_start=args.current_start,
                current_end=args.current_end,
            )
            from actions.behavior_analysis_action import format_results as format_behavior
            from actions.behavior_analysis_action import build_skill_result_parts as build_behavior_skill_result_parts
            output = format_behavior(results)
            if args.format == "skill-result-json":
                structured_result = build_behavior_skill_result_parts(results, output)
        elif args.action == "graph-analysis":
            results = execute_graph_analysis(con, mappings, where_clause, files, limit=args.limit, source_ip=args.source_ip, target_ip=args.target_ip)
            from actions.graph_analysis_action import format_results as format_graph
            from actions.graph_analysis_action import build_skill_result_parts as build_graph_skill_result_parts
            output = format_graph(results)
            if args.format == "skill-result-json":
                structured_result = build_graph_skill_result_parts(results, output)
        elif args.action == "qos-analysis":
            results = execute_qos_analysis(con, mappings, where_clause, files, limit=args.limit)
            from actions.qos_analysis_action import format_results as format_qos
            from actions.qos_analysis_action import build_skill_result_parts as build_qos_skill_result_parts
            output = format_qos(results)
            if args.format == "skill-result-json":
                structured_result = build_qos_skill_result_parts(results, output)
        elif args.action == "root-cause-analysis":
            # Run behavior-analysis first to get context for RCA
            behavior_results = execute_behavior_analysis(
                con,
                mappings,
                where_clause,
                files,
                limit=min(args.limit, 50),
            )
            results = execute_root_cause_analysis(
                con, mappings, where_clause, files,
                limit=args.limit,
                behavior_context=behavior_results,
            )
            from actions.root_cause_action import format_results as format_rca
            from actions.root_cause_action import build_skill_result_parts as build_rca_skill_result_parts
            output = format_rca(results)
            if args.format == "skill-result-json":
                structured_result = build_rca_skill_result_parts(results, output)
        elif args.action == "threat-intel-match":
            results = execute_threat_intel_match(con, mappings, where_clause, files, limit=args.limit)
            from actions.threat_intel_match_action import format_results as format_threat
            from actions.threat_intel_match_action import build_skill_result_parts as build_threat_skill_result_parts
            output = format_threat(results)
            if args.format == "skill-result-json":
                structured_result = build_threat_skill_result_parts(results, output)
        elif args.action == "forecast-traffic":
            results = execute_forecast_analysis(con, mappings, where_clause, files, horizon=args.horizon, interval=args.interval)
            from actions.forecast_traffic_action import format_results as format_forecast
            from actions.forecast_traffic_action import build_skill_result_parts as build_forecast_skill_result_parts
            output = format_forecast(results)
            if args.format == "skill-result-json":
                structured_result = build_forecast_skill_result_parts(results, output)
        elif args.action == "detect-concept-drift":
            from analysis.online_learning import OnlineLearner

            metric = args.drift_metric
            if metric not in NUMERIC_COLUMNS:
                raise ValueError(f"--drift-metric must be a numeric canonical field, got: {metric}")
            ensure_required(mappings, [metric])
            metric_filter = f"{quote_identifier(metric)} IS NOT NULL"
            scoped_where_clause = (
                f"{where_clause} AND {metric_filter}"
                if where_clause
                else f"WHERE {metric_filter}"
            )
            order_clause = "analysis_time_ts NULLS LAST, analysis_time_relative_s NULLS LAST"
            if args.drift_order_by == "input_order":
                order_clause = "pcap_name NULLS LAST, packet_number NULLS LAST, analysis_time_ts NULLS LAST, analysis_time_relative_s NULLS LAST"
            rows = con.execute(
                f"""
                SELECT CAST({quote_identifier(metric)} AS DOUBLE) AS metric_value,
                       TRY_CAST(timestamp AS VARCHAR) AS timestamp
                FROM flows
                {scoped_where_clause}
                ORDER BY {order_clause}
                LIMIT {max(args.limit, 1)}
                """
            ).fetchall()
            stream = [float(row[0]) for row in rows if row[0] is not None]
            timestamps = [str(row[1]) if row[1] is not None else "" for row in rows if row[0] is not None]

            if len(stream) < 2:
                output = (
                    "# Concept Drift Detection Results\n\n"
                    f"**Metric**: {metric}\n"
                    f"**Ordered By**: {args.drift_order_by}\n"
                    f"**Samples Used**: {len(stream)}\n\n"
                    "Insufficient numeric samples for drift detection."
                )
            else:
                learner = OnlineLearner()
                drift_result = learner.detect_drift_adwin(stream)

                # Compute additional rolling window statistics
                window_size = min(100, len(stream) // 4)
                rolling_stats = []
                if len(stream) >= window_size * 2:
                    for i in range(0, len(stream) - window_size + 1, max(1, len(stream) // 20)):
                        window = stream[i:i + window_size]
                        w_mean = sum(window) / len(window)
                        w_var = sum((x - w_mean) ** 2 for x in window) / max(len(window) - 1, 1)
                        rolling_stats.append({
                            "window_start": i,
                            "window_mean": round(w_mean, 2),
                            "window_std": round(math.sqrt(w_var), 2),
                            "timestamp": timestamps[i] if i < len(timestamps) else "",
                        })

                # Stability analysis: compare first half vs second half
                mid = len(stream) // 2
                first_half = stream[:mid]
                second_half = stream[mid:]
                mean1 = sum(first_half) / len(first_half)
                mean2 = sum(second_half) / len(second_half)
                var1 = sum((x - mean1) ** 2 for x in first_half) / max(len(first_half) - 1, 1)
                var2 = sum((x - mean2) ** 2 for x in second_half) / max(len(second_half) - 1, 1)
                mean_shift = abs(mean2 - mean1) / max(math.sqrt((var1 + var2) / 2), 1e-6)

                output = "# Concept Drift Detection Results\n\n"
                output += f"**Metric**: {metric}\n"
                output += f"**Ordered By**: {args.drift_order_by}\n"
                output += f"**Total Samples**: {len(stream)}\n\n"

                output += "## Drift Detection Result\n\n"
                output += f"- **Drift Detected**: {'Yes' if drift_result.drift_detected else 'No'}\n"
                output += f"- **Drift Type**: {drift_result.drift_type}\n"
                output += f"- **Severity**: {drift_result.drift_severity}\n"
                output += f"- **Confidence**: {drift_result.confidence:.2%}\n"
                output += f"- **Drift Point**: {drift_result.drift_point} (of {len(stream)})\n"
                output += f"- **Recommendation**: {drift_result.recommendation}\n\n"

                output += "## Stability Analysis\n\n"
                output += "| Segment | Mean | Std Dev | Samples |\n"
                output += "|---------|------|---------|--------|\n"
                output += f"| First Half | {mean1:.2f} | {math.sqrt(var1):.2f} | {len(first_half)} |\n"
                output += f"| Second Half | {mean2:.2f} | {math.sqrt(var2):.2f} | {len(second_half)} |\n\n"
                output += f"**Mean Shift (Cohen's d)**: {mean_shift:.2f}\n"
                if mean_shift > 0.8:
                    output += "_Large effect size: significant distributional shift_\n\n"
                elif mean_shift > 0.5:
                    output += "_Medium effect size: moderate distributional shift_\n\n"
                elif mean_shift > 0.2:
                    output += "_Small effect size: mild distributional shift_\n\n"
                else:
                    output += "_Negligible effect size: stable distribution_\n\n"

                if rolling_stats:
                    output += "## Rolling Window Statistics (window={})\n\n".format(window_size)
                    output += "| Window Start | Mean | Std Dev | Timestamp |\n"
                    output += "|-------------|------|---------|----------|\n"
                    for rs in rolling_stats[:10]:
                        output += f"| {rs['window_start']} | {rs['window_mean']} | {rs['window_std']} | {rs['timestamp']} |\n"
        else:
            if not args.output_file:
                if args.format == "skill-result-json":
                    raise ValueError("--output-file is required for export")
                parser.error("--output-file is required for export")
            sql = args.sql or f"SELECT * FROM flows {where_clause} ORDER BY analysis_time_ts NULLS LAST, analysis_time_relative_s NULLS LAST LIMIT {args.limit}"
            output = execute_render(con, sql, args.output_file)
        if args.format == "skill-result-json":
            result_overrides = {}
            diagnostics_overrides = {}
            if "structured_result" in locals():
                result_overrides = {
                    key: structured_result[key]
                    for key in ("summary", "findings", "evidence", "artifacts")
                    if key in structured_result
                }
                diagnostics_overrides = structured_result.get("diagnostics", {})
            ingestion_tables = []
            for table_name, meta in table_info.items():
                ingestion_tables.append(
                    {
                        "table": table_name,
                        "file": meta.get("file"),
                        "ingestion_mode": meta.get("ingestion_mode", "unknown"),
                        "rows_loaded": meta.get("rows_loaded"),
                        "physical_data_rows": meta.get("physical_data_rows"),
                        "approx_dropped_rows": meta.get("approx_dropped_rows"),
                        "approx_null_key_rows": meta.get("approx_null_key_rows"),
                    }
                )
            ingestion_warnings: list[dict[str, Any]] = []
            for meta in table_info.values():
                msg = meta.get("ingestion_warning")
                if msg:
                    ingestion_warnings.append({
                        "code": "lenient_ingestion",
                        "message": msg,
                        "severity": "warning",
                    })
            for table_name, meta in table_info.items():
                dropped = meta.get("approx_dropped_rows") or 0
                null_key = meta.get("approx_null_key_rows") or 0
                loaded = meta.get("rows_loaded", 0)
                degraded_rows = dropped + null_key
                degradation_pct = round(degraded_rows / max(loaded, 1) * 100, 1) if loaded else 0
                if degradation_pct >= 10:
                    ingestion_warnings.append({
                        "code": "ingestion_degradation",
                        "message": (
                            f"Table {table_name} has ~{degraded_rows} degraded rows "
                            f"({dropped} dropped, {null_key} null-key; {degradation_pct}% of {loaded} loaded). "
                            f"Results may be incomplete or biased due to discarded CSV lines or rows with NULL src_ip/dst_ip."
                        ),
                        "severity": "error",
                    })
            for meta in table_info.values():
                if meta.get("ingestion_mode") in (None, "unknown") and str(meta.get("file", "")).lower().endswith(".csv"):
                    ingestion_warnings.append(
                        {
                            "code": "legacy_cache",
                            "message": "CSV ingestion metadata is unavailable because this result used a legacy cache. Rebuild the cache to populate row-quality counters.",
                            "severity": "warning",
                        }
                    )
            if ingestion_tables:
                diagnostics_overrides.setdefault("data_quality", {})
                diagnostics_overrides["data_quality"]["ingestion"] = ingestion_tables
            if ingestion_warnings:
                existing_warnings = diagnostics_overrides.get("warnings") or []
                diagnostics_overrides["warnings"] = [*existing_warnings, *ingestion_warnings]
            print(
                wrap_skill_result(
                    action=args.action,
                    files=files,
                    output=output,
                    result_overrides=result_overrides,
                    diagnostics_overrides=diagnostics_overrides,
                )
            )
        else:
            print(output)
        return 0
    except ValueError as exc:
        # Configuration / user input errors — show clean message
        message = f"Configuration error: {exc}"
        if args.format == "skill-result-json":
            return print_skill_result_error(
                args=args,
                files=files,
                code="CONFIGURATION_ERROR",
                message=message,
                exit_code=2,
            )
        logger.error(message)
        return 2
    except tuple(_DUCKDB_EXCEPTIONS) as exc:
        # SQL errors should now be handled by execute_render, but catch here as fallback
        message = f"SQL error: {exc}"
        if args.format == "skill-result-json":
            return print_skill_result_error(
                args=args,
                files=files,
                code="SQL_ERROR",
                message=message,
                exit_code=1,
                details={"hint": "Run --action inspect to verify schema."},
            )
        logger.error(f"{message}\nHint: Run --action inspect to verify schema.")
        return 1
    except ImportError as exc:
        message = f"Missing dependency: {exc}"
        if args.format == "skill-result-json":
            return print_skill_result_error(
                args=args,
                files=files,
                code="MISSING_DEPENDENCY",
                message=message,
                exit_code=3,
                details={"hint": "Install required packages: pip install duckdb rrcf scikit-learn"},
            )
        logger.error(f"{message}\nHint: Install required packages: pip install duckdb rrcf scikit-learn")
        return 3
    except KeyboardInterrupt:
        if args.format == "skill-result-json":
            return print_skill_result_error(
                args=args,
                files=files,
                code="INTERRUPTED",
                message="Interrupted by user.",
                exit_code=130,
            )
        logger.error("Interrupted by user.")
        return 130
    except Exception as exc:
        # Catch-all for unexpected errors — include type for debugging
        message = f"Unexpected error ({type(exc).__name__}): {exc}"
        if args.format == "skill-result-json":
            return print_skill_result_error(
                args=args,
                files=files,
                code="UNEXPECTED_ERROR",
                message=message,
                exit_code=1,
                recoverable=False,
            )
        logger.error(message)
        logger.debug("Full traceback:", exc_info=True)
        return 1
    finally:
        if "con" in locals():
            con.close()
        if cleanup_db_copy is not None:
            with suppress(Exception):
                cleanup_db_copy.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
