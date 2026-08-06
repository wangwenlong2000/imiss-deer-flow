# Network Traffic Dataset Schema

This folder stores local, tracked configuration for the network traffic analysis workflow.

Files:

- `field_mapping.yaml`: canonical field aliases used by the skill script
  - supports shared aliases plus source-specific profiles such as `wireshark_packet`

Recommended raw dataset shape:

- One row per flow, session, alert, or request log
- Prefer at least these columns when available:
  - `timestamp` or relative-time fields such as `relative_time_s` / `start_relative_time_s`
  - `src_ip`
  - `dst_ip`
  - `src_port`
  - `dst_port`
  - `protocol`
  - `bytes`
  - `packets`
  - `flow_duration`
  - `direction`
  - `action`

Supported input formats in phase 1:

- `.csv`
- `.parquet`
- `.json`
- `.jsonl`
- `.xlsx`
- `.xls`

Mapping behavior:

- Source files keep their original columns.
- The analysis script builds a canonical `flows` view for downstream actions.
- Common vendor/export aliases can be added to `field_mapping.yaml`.
- Source-specific profiles can be auto-detected without breaking older datasets.
- Time-aware analysis supports both:
  - absolute timestamps via `timestamp` / `end_time`
  - relative capture time via `relative_time_s`, `start_relative_time_s`, `end_relative_time_s`, and `time_is_relative`

PCAP preprocessing output fields:

- `packet.csv` always includes the existing core fields plus:
  - `relative_time_s`
  - `time_is_relative`
  - `app_protocol`
  - `service`
  - `ip_version`
  - `frame_len`
  - `ttl`
  - `payload_bytes`
  - `tcp_flags`
  - `dns_query`
  - `tls_sni`
  - `http_host`
  - `icmp_type`
  - `icmp_code`
  - `flow_id`
  - `packet_count`
  - `byte_count`
  - `bytes_total`
  - `src_bytes`
  - `dst_bytes`
  - `src_packets`
  - `dst_packets`
  - `duration_ms`
  - `session_state`
  - `rule_name`
  - `device_id`
  - `sensor_id`
  - `vlan_id`
  - `src_zone`
  - `dst_zone`
  - `src_asset_group`
  - `dst_asset_group`
  - `nat_src_ip`
  - `nat_dst_ip`
  - `dst_asn`
  - `dst_country`
  - `asset_id`
  - `user_id`
  - `dataset_label`
  - `traffic_family`
  - `pcap_name`
  - `mac_src`
  - `mac_dst`
- `flow.csv` always includes the existing core fields plus:
  - `end_time`
  - `start_relative_time_s`
  - `end_relative_time_s`
  - `time_is_relative`
  - `app_protocol`
  - `service`
  - `session_state`
  - `flow_start_reason`
  - `flow_end_reason`
  - `rule_name`
  - `tcp_flags`
  - `ip_version`
  - `bytes_total`
  - `packet_count`
  - `payload_bytes`
  - `src_bytes`
  - `dst_bytes`
  - `src_packets`
  - `dst_packets`
  - `vlan_id`
  - `src_zone`
  - `dst_zone`
  - `src_asset_group`
  - `dst_asset_group`
  - `nat_src_ip`
  - `nat_dst_ip`
  - `dst_asn`
  - `dst_country`
  - `dns_query`
  - `tls_sni`
  - `http_host`
  - `asset_id`
  - `user_id`
  - `duration`
  - `duration_ms`
  - `flow_id`
  - `tcp_flags_seen`
  - `ttl_min`
  - `ttl_max`
  - `ttl_avg`
  - `icmp_type`
  - `icmp_code`
  - `device_id`
  - `sensor_id`
  - `dataset_label`
  - `traffic_family`

Flow semantics:

- `flow.csv` is now session-oriented rather than coarse conversation-oriented.
- TCP flows are split using:
  - SYN-preferring session starts
  - FIN / RST termination
  - idle timeout boundaries
- Non-TCP flows are split using idle timeout boundaries.
- Relative-time captures keep their time offsets in dedicated fields instead of being rewritten as `1970-01-01...` timestamps.
