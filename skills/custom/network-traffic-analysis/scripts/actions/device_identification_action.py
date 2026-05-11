"""
Device Identification Action

Action handler for device fingerprinting and identification.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import append_file_errors, fetch_rows, format_dict_rows, present_fields, scoped_where
from analysis.device_fingerprint import DeviceFingerprinter


def execute_device_identification(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    **kwargs,
) -> dict:
    limit = kwargs.get("limit", 50)

    results = {
        "action": "device-identification",
        "files_analyzed": [],
        "summary": {
            "total_flows_analyzed": 0,
            "devices_identified": 0,
            "device_types": {
                "mobile": 0,
                "desktop": 0,
                "server": 0,
                "iot": 0,
                "network_equipment": 0,
                "unknown": 0,
            },
        },
        "device_profiles": [],
    }

    fingerprinter = DeviceFingerprinter()
    available = present_fields(mappings)
    file_result = {
        "file": files[0] if files else "selected scope",
        "flow_count": 0,
        "devices": [],
    }

    try:
        if "src_ip" not in available:
            file_result["error"] = "Device identification requires src_ip in the canonical flow view."
            results["files_analyzed"].append(file_result)
            return results

        def optional_text_expr(field: str) -> str:
            return f"MAX(COALESCE({field}, ''))" if field in available else "''"

        sql = f"""
            SELECT
                src_ip,
                COUNT(*) AS flow_count,
                AVG(COALESCE(bytes, 0)) AS avg_bytes,
                AVG(COALESCE(packets, 0)) AS avg_packets,
                MIN(COALESCE(dst_port, 0)) AS sample_dst_port,
                STRING_AGG(DISTINCT COALESCE(protocol, 'UNKNOWN'), ', ') AS protocols,
                {optional_text_expr("service")} AS service,
                {optional_text_expr("tls_sni")} AS tls_sni,
                {optional_text_expr("http_host")} AS http_host,
                {optional_text_expr("http_user_agent")} AS http_user_agent,
                {optional_text_expr("dhcp_fingerprint")} AS dhcp_fingerprint,
                {optional_text_expr("dhcp_vendor")} AS dhcp_vendor,
                {optional_text_expr("dhcp_hostname")} AS dhcp_hostname,
                {optional_text_expr("ssh_hassh")} AS ssh_hassh,
                {optional_text_expr("p0f_os")} AS p0f_os,
                {optional_text_expr("tcp_syn_signature")} AS tcp_syn_signature,
                {optional_text_expr("mac_src")} AS mac_src,
                {optional_text_expr("mac_dst")} AS mac_dst,
                {optional_text_expr("tls_version")} AS tls_version,
                {optional_text_expr("tls_server_cipher")} AS cipher_suite,
                {optional_text_expr("ja3_hash")} AS ja3_hash,
                {optional_text_expr("ja3s_hash")} AS ja3s_hash
            FROM flows
            {scoped_where(where_clause, "src_ip IS NOT NULL")}
            GROUP BY 1
            ORDER BY flow_count DESC, avg_bytes DESC, src_ip ASC
            LIMIT {limit}
        """
        device_flows = fetch_rows(con, sql)
        file_result["flow_count"] = len(device_flows)
        results["summary"]["total_flows_analyzed"] += len(device_flows)

        rich_fields = {
            "http_user_agent",
            "dhcp_fingerprint",
            "dhcp_vendor",
            "dhcp_hostname",
            "ssh_hassh",
            "p0f_os",
            "tcp_syn_signature",
            "mac_src",
            "mac_dst",
            "tls_version",
            "tls_server_cipher",
            "tls_sni",
            "http_host",
            "ja3_hash",
            "ja3s_hash",
        }
        rich_fields_available = sorted(rich_fields & available)
        rich_fields_populated_any = False

        for device_flow in device_flows:
            device_input = {
                "dst_port": int(device_flow.get("sample_dst_port") or 0),
                "bytes": float(device_flow.get("avg_bytes") or 0),
                "packets": float(device_flow.get("avg_packets") or 0),
                "protocol": device_flow.get("protocols", ""),
                "service": device_flow.get("service", ""),
                "tls_sni": device_flow.get("tls_sni", ""),
                "http_host": device_flow.get("http_host", ""),
                "http_user_agent": device_flow.get("http_user_agent", ""),
                "dhcp_fingerprint": device_flow.get("dhcp_fingerprint", ""),
                "dhcp_vendor": device_flow.get("dhcp_vendor", ""),
                "dhcp_hostname": device_flow.get("dhcp_hostname", ""),
                "ssh_hassh": device_flow.get("ssh_hassh", ""),
                "p0f_os": device_flow.get("p0f_os", ""),
                "tcp_syn_signature": device_flow.get("tcp_syn_signature", ""),
                "mac_src": device_flow.get("mac_src", ""),
                "mac_dst": device_flow.get("mac_dst", ""),
                "tls_version": device_flow.get("tls_version", ""),
                "cipher_suite": device_flow.get("cipher_suite", ""),
                "ja3_hash": device_flow.get("ja3_hash", ""),
                "ja3s_hash": device_flow.get("ja3s_hash", ""),
            }
            device_id = fingerprinter.identify_device_type(device_input)
            confidence = round(device_id["confidence"], 4)
            populated_fields = sorted(
                field for field in rich_fields_available if str(device_flow.get(field) or "").strip()
            )
            if populated_fields:
                rich_fields_populated_any = True
            if not populated_fields:
                confidence = min(confidence, 0.4)
            profile = {
                "ip_address": device_flow.get("src_ip", ""),
                "device_type": device_id["device_type"],
                "device_os": device_id["device_os"],
                "confidence": confidence,
                "flow_count": int(device_flow.get("flow_count") or 0),
                "avg_bytes": round(float(device_flow.get("avg_bytes") or 0), 2),
                "protocols": device_flow.get("protocols", ""),
                "fingerprint_fields": ", ".join(populated_fields) if populated_fields else "none",
                "classification_method": device_id.get("classification_method", "unknown"),
                "matched_profile": device_id.get("matched_profile", ""),
                "evidence": ", ".join(device_id.get("indicators", [])) if device_id.get("indicators") else "none",
                "source": device_id.get("source", ""),
            }
            file_result["devices"].append(profile)
            results["device_profiles"].append(profile)
            results["summary"]["devices_identified"] += 1
            dtype = device_id["device_type"]
            if dtype in results["summary"]["device_types"]:
                results["summary"]["device_types"][dtype] += 1
            else:
                results["summary"]["device_types"]["unknown"] += 1

        if not rich_fields_available:
            file_result["note"] = "Device identification ran in reduced-confidence mode because HTTP/TLS/JA3/DHCP/HASSH/MAC/p0f fingerprint fields are not present in the canonical flow view."
        elif not rich_fields_populated_any:
            file_result["note"] = "Device identification ran in reduced-confidence mode because fingerprint columns exist but are empty in the selected flow scope."
        file_result["available_fingerprint_columns"] = rich_fields_available
        file_result["profile_source"] = {
            "local_profiles": fingerprinter.profile_metadata,
            "external_sources": fingerprinter.external_metadata,
        }
        results["files_analyzed"].append(file_result)
    except Exception as e:
        file_result["error"] = str(e)
        results["files_analyzed"].append(file_result)

    return results


def format_results(results: dict) -> str:
    output = []
    output.append("# Device Identification Results\n")

    summary = results["summary"]
    output.append("## Summary\n")
    output.append(f"- **Total Flows Analyzed**: {summary['total_flows_analyzed']}")
    output.append(f"- **Devices Identified**: {summary['devices_identified']}\n")
    output.append("### Device Type Distribution\n")
    for dtype, count in summary["device_types"].items():
        if count > 0:
            output.append(f"- **{dtype}**: {count}")

    for file_result in results["files_analyzed"]:
        if file_result.get("devices"):
            output.append(f"\n## File: {file_result['file']}\n")
            output.append(format_dict_rows(file_result["devices"]))
        if file_result.get("note"):
            output.append(f"\n{file_result['note']}\n")

    append_file_errors(output, results)
    return "\n".join(output)


def build_skill_result_parts(results: dict, raw_output: str) -> dict[str, Any]:
    summary = results.get("summary", {})
    device_profiles = results.get("device_profiles", [])
    errors = [
        {"file": item.get("file", "selected scope"), "error": item["error"]}
        for item in results.get("files_analyzed", [])
        if item.get("error")
    ]
    warnings = [
        item["note"]
        for item in results.get("files_analyzed", [])
        if item.get("note")
    ]

    findings: list[dict[str, Any]] = []
    for index, profile in enumerate(device_profiles, 1):
        confidence = float(profile.get("confidence") or 0)
        device_type = profile.get("device_type") or "unknown"
        if device_type == "unknown" and confidence < 0.5:
            continue
        severity = "info" if confidence < 0.75 else "low"
        findings.append(
            {
                "finding_id": f"f-device-{index:03d}",
                "type": "device_profile",
                "severity": severity,
                "confidence": round(confidence, 4),
                "title": f"{profile.get('ip_address', 'unknown')} identified as {device_type}",
                "description": (
                    "Device classification is based on local profiles, downloaded external fingerprint references when available, then lower-confidence flow heuristics. "
                    "Confidence is reduced when HTTP/TLS/device fingerprints are unavailable."
                ),
                "entities": [
                    {"type": "src_ip", "value": profile.get("ip_address", "")},
                    {"type": "device_type", "value": device_type},
                ],
                "evidence_refs": ["e-device-profiles"],
                "recommended_actions": [
                    "Correlate high-confidence device labels with asset inventory.",
                    "Treat reduced-confidence labels as triage hints, not inventory truth.",
                ],
            }
        )

    evidence: list[dict[str, Any]] = [
        {
            "evidence_id": "e-device-type-distribution",
            "type": "metric",
            "title": "Device Type Distribution",
            "content": summary.get("device_types", {}),
        }
    ]
    if device_profiles:
        evidence.append(
            {
                "evidence_id": "e-device-profiles",
                "type": "table",
                "title": "Device Profiles",
                "columns": list(device_profiles[0].keys()),
                "rows": device_profiles,
            }
        )
    evidence.append(
        {
            "evidence_id": "e-raw-report",
            "type": "text",
            "title": "Raw Device Identification Report",
            "content": raw_output,
        }
    )

    return {
        "summary": {
            "title": "Device Identification",
            "overview": (
                f"Identified {summary.get('devices_identified', 0)} candidate device profiles "
                f"from {summary.get('total_flows_analyzed', 0)} analyzed source entities."
            ),
            "severity": "info",
            "confidence": 0.65 if warnings else 0.75,
            "key_metrics": [
                {"name": "devices_identified", "value": summary.get("devices_identified", 0)},
                {"name": "total_flows_analyzed", "value": summary.get("total_flows_analyzed", 0)},
                {"name": "unknown_devices", "value": summary.get("device_types", {}).get("unknown", 0)},
            ],
        },
        "findings": findings,
        "evidence": evidence,
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "files_with_errors": len(errors),
                "profiles_returned": len(device_profiles),
            },
            "device_profile_sources": [
                item.get("profile_source", {})
                for item in results.get("files_analyzed", [])
                if item.get("profile_source")
            ],
            "available_fingerprint_columns": [
                item.get("available_fingerprint_columns", [])
                for item in results.get("files_analyzed", [])
                if item.get("available_fingerprint_columns") is not None
            ],
            "errors": errors,
        },
    }
