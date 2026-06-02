from __future__ import annotations

from typing import Any


def _duration_expr(available: set[str]) -> str:
    return "COALESCE(duration_ms, flow_duration, 0)" if "duration_ms" in available else "COALESCE(flow_duration, 0)"


def short_connection_candidate_sql(and_clause: str, available: set[str], *, candidate_limit: int = 5000) -> str:
    duration_expr = _duration_expr(available)
    return f"""
    WITH scoped AS (
        SELECT *
        FROM flows
        WHERE 1=1 {and_clause}
    ),
    src_stats AS (
        SELECT
            src_ip,
            COUNT(*) AS src_flow_count,
            COUNT(DISTINCT dst_ip) AS src_unique_dst_ip,
            COUNT(DISTINCT dst_port) AS src_unique_dst_port,
            AVG(COALESCE(bytes, 0)) AS src_avg_bytes,
            AVG(COALESCE(packets, 0)) AS src_avg_packets,
            AVG({duration_expr}) AS src_avg_duration_ms
        FROM scoped
        WHERE src_ip IS NOT NULL
        GROUP BY 1
    ),
    dst_stats AS (
        SELECT
            dst_ip,
            COUNT(*) AS dst_flow_count,
            COUNT(DISTINCT src_ip) AS dst_unique_src_ip
        FROM scoped
        WHERE dst_ip IS NOT NULL
        GROUP BY 1
    )
    SELECT
        s.src_ip,
        s.dst_ip,
        COALESCE(CAST(s.src_port AS VARCHAR), '') AS src_port,
        COALESCE(CAST(s.dst_port AS VARCHAR), '') AS dst_port,
        COALESCE(s.protocol, 'UNKNOWN') AS protocol,
        COALESCE(s.app_protocol, 'UNKNOWN') AS app_protocol,
        COALESCE(s.service, 'UNKNOWN') AS service,
        COALESCE(s.session_state, 'UNKNOWN') AS session_state,
        COALESCE(s.bytes, 0) AS bytes,
        COALESCE(s.packets, 0) AS packets,
        {duration_expr} AS duration_ms,
        COALESCE(s.payload_bytes, 0) AS payload_bytes,
        COALESCE(src_stats.src_flow_count, 0) AS src_flow_count,
        COALESCE(src_stats.src_unique_dst_ip, 0) AS src_unique_dst_ip,
        COALESCE(src_stats.src_unique_dst_port, 0) AS src_unique_dst_port,
        COALESCE(src_stats.src_avg_bytes, 0) AS src_avg_bytes,
        COALESCE(src_stats.src_avg_packets, 0) AS src_avg_packets,
        COALESCE(src_stats.src_avg_duration_ms, 0) AS src_avg_duration_ms,
        COALESCE(dst_stats.dst_flow_count, 0) AS dst_flow_count,
        COALESCE(dst_stats.dst_unique_src_ip, 0) AS dst_unique_src_ip,
        COALESCE(s.tcp_flags, '') AS tcp_flags
    FROM scoped s
    LEFT JOIN src_stats ON s.src_ip = src_stats.src_ip
    LEFT JOIN dst_stats ON s.dst_ip = dst_stats.dst_ip
    WHERE (
        {duration_expr} <= 5000
        OR COALESCE(s.bytes, 0) <= 5120
        OR COALESCE(s.packets, 0) <= 10
    )
    ORDER BY RANDOM()
    LIMIT {candidate_limit}
    """


def source_microflow_summary_sql(and_clause: str, available: set[str], *, limit: int) -> str:
    duration_expr = _duration_expr(available)
    return f"""
    SELECT
        src_ip,
        COUNT(*) AS candidate_flows,
        SUM(CASE WHEN {duration_expr} <= 10 AND COALESCE(bytes, 0) <= 300 AND COALESCE(packets, 0) <= 2 THEN 1 ELSE 0 END) AS compact_short_flows,
        ROUND(AVG(COALESCE(bytes, 0)), 2) AS avg_bytes,
        ROUND(AVG(COALESCE(packets, 0)), 2) AS avg_packets,
        ROUND(AVG({duration_expr}), 2) AS avg_duration_ms,
        COUNT(DISTINCT dst_ip) AS unique_dst_ip,
        COUNT(DISTINCT dst_port) AS unique_dst_port
    FROM flows
    WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
      AND (
        {duration_expr} <= 50
        OR COALESCE(bytes, 0) <= 512
        OR COALESCE(packets, 0) <= 3
      )
    GROUP BY 1
    ORDER BY compact_short_flows DESC, candidate_flows DESC, unique_dst_ip DESC, src_ip ASC
    LIMIT {limit}
    """


def scan_candidate_sql(and_clause: str, *, packet_view: bool, limit: int) -> str:
    syn_expr = "SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END)" if packet_view else "0"
    rst_expr = "SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END)"
    packet_or_flow_alias = "packets" if packet_view else "flows"
    return f"""
    SELECT
        src_ip,
        COUNT(*) AS {packet_or_flow_alias},
        COUNT(DISTINCT dst_ip) AS unique_dst_ip,
        COUNT(DISTINCT dst_port) AS unique_dst_port,
        SUM(COALESCE(bytes, 0)) AS total_bytes,
        AVG(COALESCE(bytes, 0)) AS avg_bytes,
        COUNT(DISTINCT protocol) AS unique_protocols,
        COUNT(DISTINCT app_protocol) AS unique_app_protocols,
        {syn_expr} AS syn_only_packets,
        {rst_expr} AS rst_packets,
        ROUND({rst_expr} * 100.0 / NULLIF(COUNT(*), 0), 2) AS rst_pct,
        ROUND({syn_expr} * 100.0 / NULLIF(COUNT(*), 0), 2) AS syn_only_pct
    FROM flows
    WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) >= 3
    ORDER BY unique_dst_ip DESC, unique_dst_port DESC, {packet_or_flow_alias} DESC, total_bytes DESC
    LIMIT {limit}
    """


def session_candidate_sql(and_clause: str, *, limit: int) -> str:
    return f"""
    SELECT
        src_ip,
        COUNT(*) AS flows,
        COUNT(DISTINCT dst_ip) AS unique_dst_ip,
        COUNT(DISTINCT dst_port) AS unique_dst_port,
        SUM(COALESCE(bytes, 0)) AS total_bytes,
        AVG(COALESCE(bytes, 0)) AS avg_bytes,
        AVG(COALESCE(packets, 0)) AS avg_packets,
        AVG(COALESCE(duration_ms, flow_duration, 0)) AS avg_duration_ms,
        SUM(CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END) AS negative_outcomes,
        ROUND(SUM(CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS negative_pct,
        SUM(CASE WHEN UPPER(COALESCE(session_state, '')) IN ('RST', 'SYN', 'SYN_ONLY') THEN 1 ELSE 0 END) AS risky_states,
        ROUND(SUM(CASE WHEN UPPER(COALESCE(session_state, '')) IN ('RST', 'SYN', 'SYN_ONLY') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS risky_state_pct,
        SUM(CASE WHEN COALESCE(bytes, 0) <= 128 AND COALESCE(duration_ms, flow_duration, 0) <= 1000 THEN 1 ELSE 0 END) AS short_low_byte_flows,
        ROUND(SUM(CASE WHEN COALESCE(bytes, 0) <= 128 AND COALESCE(duration_ms, flow_duration, 0) <= 1000 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS short_low_byte_pct
    FROM flows
    WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) >= 3
    ORDER BY negative_pct DESC, risky_state_pct DESC, flows DESC, total_bytes DESC
    LIMIT {limit}
    """


def rare_port_candidate_sql(and_clause: str, *, limit: int) -> str:
    return f"""
    SELECT
        COALESCE(CAST(dst_port AS VARCHAR), 'UNKNOWN') AS dst_port,
        COUNT(*) AS records,
        COUNT(DISTINCT src_ip) AS unique_src_ip,
        COUNT(DISTINCT dst_ip) AS unique_dst_ip,
        SUM(COALESCE(bytes, 0)) AS total_bytes,
        COUNT(DISTINCT protocol) AS unique_protocols,
        COUNT(DISTINCT app_protocol) AS unique_app_protocols
    FROM flows
    WHERE 1=1 {and_clause} AND dst_port IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) >= 1
    ORDER BY records ASC, unique_src_ip DESC, unique_dst_ip DESC, total_bytes DESC, dst_port ASC
    LIMIT {limit}
    """


def failure_rate_candidate_sql(and_clause: str, *, limit: int) -> str:
    return f"""
    SELECT
        src_ip,
        COUNT(*) AS flows,
        SUM(CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END) AS negative_outcomes,
        ROUND(SUM(CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS negative_pct,
        COUNT(DISTINCT action) AS unique_actions,
        COUNT(DISTINCT dst_ip) AS unique_dst_ip,
        COUNT(DISTINCT dst_port) AS unique_dst_port,
        SUM(COALESCE(bytes, 0)) AS total_bytes,
        AVG(COALESCE(bytes, 0)) AS avg_bytes
    FROM flows
    WHERE 1=1 {and_clause} AND src_ip IS NOT NULL AND action IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) >= 3
    ORDER BY negative_pct DESC, negative_outcomes DESC, flows DESC
    LIMIT {limit}
    """


def volume_spike_candidate_sql(and_clause: str, *, bucket_expr: str, limit: int) -> str:
    return f"""
    WITH buckets AS (
        SELECT
            {bucket_expr} AS bucket,
            COUNT(*) AS records,
            SUM(COALESCE(bytes, 0)) AS total_bytes,
            SUM(COALESCE(packets, 0)) AS total_packets,
            COUNT(DISTINCT src_ip) AS unique_src_ip,
            COUNT(DISTINCT dst_ip) AS unique_dst_ip
        FROM flows
        WHERE 1=1 {and_clause}
        GROUP BY 1
    )
    SELECT
        bucket,
        records,
        total_bytes,
        total_packets,
        unique_src_ip,
        unique_dst_ip
    FROM buckets
    ORDER BY bucket ASC
    LIMIT {limit}
    """


def rst_heavy_candidate_sql(and_clause: str, *, limit: int) -> str:
    return f"""
    SELECT
        src_ip,
        COUNT(*) AS packets,
        SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) AS rst_packets,
        ROUND(SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rst_pct,
        COUNT(DISTINCT dst_ip) AS unique_dst_ip,
        COUNT(DISTINCT dst_port) AS unique_dst_port,
        SUM(COALESCE(bytes, 0)) AS total_bytes
    FROM flows
    WHERE 1=1 {and_clause}
    GROUP BY 1
    HAVING COUNT(*) >= 3
    ORDER BY rst_pct DESC, rst_packets DESC, packets DESC
    LIMIT {limit}
    """


def handshake_failure_candidate_sql(and_clause: str, *, limit: int) -> str:
    return f"""
    SELECT
        src_ip,
        COUNT(*) AS packets,
        SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) AS syn_only_packets,
        SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%SA%' THEN 1 ELSE 0 END) AS syn_ack_packets,
        SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) AS rst_packets,
        COUNT(DISTINCT dst_ip) AS unique_dst_ip,
        COUNT(DISTINCT dst_port) AS unique_dst_port
    FROM flows
    WHERE 1=1 {and_clause}
    GROUP BY 1
    HAVING COUNT(*) >= 3
    ORDER BY syn_only_packets DESC, rst_packets DESC, unique_dst_ip DESC, unique_dst_port DESC
    LIMIT {limit}
    """


def icmp_probe_candidate_sql(and_clause: str, *, limit: int) -> str:
    return f"""
    SELECT
        src_ip,
        COUNT(*) AS packets,
        COUNT(DISTINCT dst_ip) AS unique_dst_ip,
        COUNT(DISTINCT icmp_type) AS unique_icmp_type,
        COUNT(DISTINCT icmp_code) AS unique_icmp_code,
        SUM(COALESCE(bytes, 0)) AS total_bytes
    FROM flows
    WHERE 1=1 {and_clause}
    GROUP BY 1
    HAVING COUNT(*) >= 3
    ORDER BY unique_dst_ip DESC, packets DESC, unique_icmp_type DESC
    LIMIT {limit}
    """


def small_packet_burst_candidate_sql(and_clause: str, *, limit: int) -> str:
    return f"""
    SELECT
        src_ip,
        COUNT(*) AS packets,
        SUM(CASE WHEN COALESCE(payload_bytes, frame_len, bytes, 0) <= 128 THEN 1 ELSE 0 END) AS small_packets,
        ROUND(SUM(CASE WHEN COALESCE(payload_bytes, frame_len, bytes, 0) <= 128 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS small_packet_pct,
        COUNT(DISTINCT dst_ip) AS unique_dst_ip,
        COUNT(DISTINCT dst_port) AS unique_dst_port,
        SUM(COALESCE(bytes, 0)) AS total_bytes,
        AVG(COALESCE(payload_bytes, frame_len, bytes, 0)) AS avg_payload_bytes
    FROM flows
    WHERE 1=1 {and_clause}
    GROUP BY 1
    HAVING COUNT(*) >= 3
    ORDER BY small_packet_pct DESC, small_packets DESC, packets DESC
    LIMIT {limit}
    """


def rows_from_query(con: Any, sql: str) -> tuple[list[str], list[dict[str, Any]]]:
    result = con.execute(sql)
    columns = [item[0] for item in result.description]
    rows = [dict(zip(columns, row)) for row in result.fetchall()]
    return columns, rows
