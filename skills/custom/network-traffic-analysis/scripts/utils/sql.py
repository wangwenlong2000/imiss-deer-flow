from __future__ import annotations

import ipaddress
import re
from typing import Any

import duckdb  # type: ignore

from constants import SUPPORTED_ANOMALY_RULES


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sanitize_table_name(name: str) -> str:
    name = re.sub(r"[^\w]", "_", name)
    if name and name[0].isdigit():
        name = f"t_{name}"
    if name.lower() == "flows":
        name = "flows_source"
    return name


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return quote_literal(str(value))


def add_ip_udf(con: duckdb.DuckDBPyConnection) -> None:
    def ip_in_cidr(ip_value: Any, cidr_value: Any) -> bool:
        if ip_value in (None, "") or cidr_value in (None, ""):
            return False
        try:
            return ipaddress.ip_address(str(ip_value)) in ipaddress.ip_network(str(cidr_value), strict=False)
        except ValueError:
            return False

    con.create_function("ip_in_cidr", ip_in_cidr, ["VARCHAR", "VARCHAR"], "BOOLEAN")


# NOTE: build_where_clause was moved to core/schema_mapping.py.
# This module is kept for shared SQL utilities only.
# Do not import build_where_clause from here.


def metric_sql(metric: str) -> str:
    if metric == "count":
        return "COUNT(*) AS count"
    agg, _, field = metric.partition(":")
    if not field:
        raise ValueError(f"Invalid metric specification: {metric}")
    column = quote_identifier(field)
    alias = quote_identifier(f"{agg}_{field}")
    if agg == "sum":
        return f"SUM(COALESCE({column}, 0)) AS {alias}"
    if agg == "avg":
        return f"AVG(COALESCE({column}, 0)) AS {alias}"
    if agg == "max":
        return f"MAX({column}) AS {alias}"
    if agg == "min":
        return f"MIN({column}) AS {alias}"
    if agg == "count_distinct":
        return f"COUNT(DISTINCT {column}) AS {alias}"
    raise ValueError(f"Unsupported metric aggregation: {agg}")


def timestamp_expr(column_sql: str) -> str:
    return (
        f"COALESCE(try_cast({column_sql} AS TIMESTAMP), "
        f"to_timestamp(try_cast({column_sql} AS DOUBLE)), "
        f"try_strptime(CAST({column_sql} AS VARCHAR), '%Y-%m-%d %H:%M:%S'), "
        f"try_strptime(CAST({column_sql} AS VARCHAR), '%Y-%m-%dT%H:%M:%S'), "
        f"try_strptime(CAST({column_sql} AS VARCHAR), '%Y-%m-%dT%H:%M:%S.%f'))"
    )


def numeric_expr(column_sql: str) -> str:
    return f"try_cast({column_sql} AS DOUBLE)"


def booleanish_expr(column_sql: str) -> str:
    return (
        "CASE "
        f"WHEN lower(trim(CAST({column_sql} AS VARCHAR))) IN ('true', '1', 'yes', 'y') THEN TRUE "
        f"WHEN lower(trim(CAST({column_sql} AS VARCHAR))) IN ('false', '0', 'no', 'n') THEN FALSE "
        "ELSE NULL END"
    )


_INTERVAL_SECONDS = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}


def relative_interval_seconds(interval: str) -> int:
    try:
        return _INTERVAL_SECONDS[interval]
    except KeyError:
        raise ValueError(
            f"Unsupported interval '{interval}'. "
            f"Supported values: {', '.join(sorted(_INTERVAL_SECONDS.keys()))}."
        )


def analysis_time_bucket_expr(interval: str) -> str:
    seconds = relative_interval_seconds(interval)
    return (
        "CASE "
        f"WHEN analysis_time_kind = 'absolute' AND analysis_time_ts IS NOT NULL THEN CAST(DATE_TRUNC('{interval}', analysis_time_ts) AS VARCHAR) "
        f"WHEN analysis_time_kind = 'relative' AND analysis_time_relative_s IS NOT NULL THEN CONCAT('t+', CAST(CAST(FLOOR(analysis_time_relative_s / {seconds}) * {seconds} AS BIGINT) AS VARCHAR), 's') "
        "ELSE 'unknown' END"
    )


def legacy_detect_anomaly_sql(rule: str, and_clause: str) -> str:
    if rule == "volume-spike":
        return f"""
            WITH buckets AS (
                SELECT {analysis_time_bucket_expr('hour')} AS bucket, SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                WHERE 1=1 {and_clause}
                GROUP BY 1
            )
            SELECT bucket, total_bytes, AVG(total_bytes) OVER () AS avg_bytes,
                   CASE WHEN total_bytes > AVG(total_bytes) OVER () * 2 THEN 'spike' ELSE 'normal' END AS status
            FROM buckets
            ORDER BY total_bytes DESC
        """
    if rule == "rare-port":
        return f"""
            SELECT dst_port, COUNT(*) AS records
            FROM flows
            WHERE 1=1 {and_clause}
            GROUP BY 1
            HAVING COUNT(*) <= 3
            ORDER BY records ASC, dst_port ASC
        """
    if rule == "failure-rate":
        return f"""
            SELECT action, COUNT(*) AS records,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
            FROM flows
            WHERE 1=1 {and_clause} AND action IS NOT NULL
            GROUP BY 1
            ORDER BY pct DESC, records DESC
        """
    if rule == "syn-scan":
        return f"""
            SELECT src_ip,
                   COUNT(*) AS packets,
                   COUNT(DISTINCT dst_ip) AS unique_dst_ip,
                   COUNT(DISTINCT dst_port) AS unique_dst_port,
                   SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) AS syn_only_packets
            FROM flows
            WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
            GROUP BY 1
            HAVING SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) >= 10
                OR COUNT(DISTINCT dst_port) >= 10
                OR COUNT(DISTINCT dst_ip) >= 5
            ORDER BY syn_only_packets DESC, unique_dst_ip DESC, unique_dst_port DESC, packets DESC
        """
    if rule == "rst-heavy":
        return f"""
            SELECT src_ip,
                   COUNT(*) AS packets,
                   SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) AS rst_packets,
                   ROUND(SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rst_pct
            FROM flows
            WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
            GROUP BY 1
            HAVING COUNT(*) >= 10
            ORDER BY rst_pct DESC, rst_packets DESC, packets DESC
        """
    if rule == "handshake-failure":
        return f"""
            SELECT src_ip,
                   COUNT(*) AS packets,
                   SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) AS syn_only_packets,
                   SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%SA%' THEN 1 ELSE 0 END) AS syn_ack_packets,
                   SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) AS rst_packets,
                   COUNT(DISTINCT dst_ip) AS unique_dst_ip
            FROM flows
            WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
            GROUP BY 1
            HAVING syn_only_packets > syn_ack_packets OR rst_packets > 0
            ORDER BY syn_only_packets DESC, rst_packets DESC, unique_dst_ip DESC, packets DESC
        """
    if rule == "icmp-probe":
        return f"""
            SELECT src_ip,
                   COUNT(*) AS packets,
                   COUNT(DISTINCT dst_ip) AS unique_dst_ip,
                   COUNT(DISTINCT icmp_type) AS unique_icmp_type
            FROM flows
            WHERE 1=1 {and_clause} AND src_ip IS NOT NULL AND icmp_type IS NOT NULL
            GROUP BY 1
            HAVING COUNT(DISTINCT dst_ip) >= 5 OR COUNT(*) >= 10
            ORDER BY unique_dst_ip DESC, packets DESC, unique_icmp_type DESC
        """
    if rule == "small-packet-burst":
        return f"""
            SELECT src_ip,
                   COUNT(*) AS packets,
                   SUM(CASE WHEN COALESCE(payload_bytes, frame_len, bytes, 0) <= 128 THEN 1 ELSE 0 END) AS small_packets,
                   ROUND(SUM(CASE WHEN COALESCE(payload_bytes, frame_len, bytes, 0) <= 128 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS small_packet_pct
            FROM flows
            WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
            GROUP BY 1
            HAVING COUNT(*) >= 20
            ORDER BY small_packet_pct DESC, small_packets DESC, packets DESC
        """
    if rule == "scan-source":
        return f"""
            SELECT src_ip, COUNT(*) AS flows, COUNT(DISTINCT dst_ip) AS unique_dst_ip,
                   COUNT(DISTINCT dst_port) AS unique_dst_port
            FROM flows
            WHERE 1=1 {and_clause}
            GROUP BY 1
            HAVING COUNT(DISTINCT dst_ip) >= 5 OR COUNT(DISTINCT dst_port) >= 10
            ORDER BY unique_dst_ip DESC, unique_dst_port DESC, flows DESC
        """
    raise ValueError(
        "Unsupported anomaly rule "
        f"'{rule}'. Supported rules: {', '.join(SUPPORTED_ANOMALY_RULES)}. "
        "Run --action list-capabilities to inspect supported workflows and choose a structured action or --action query."
    )
