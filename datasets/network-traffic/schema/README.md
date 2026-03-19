# Network Traffic Dataset Schema

This folder stores local, tracked configuration for the network traffic analysis workflow.

Files:

- `field_mapping.yaml`: canonical field aliases used by the skill script

Recommended raw dataset shape:

- One row per flow, session, alert, or request log
- Prefer at least these columns when available:
  - `timestamp`
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

PCAP preprocessing output fields:

- `packet.csv` always includes the existing core fields plus:
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
  - `app_protocol`
  - `service`
  - `session_state`
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
