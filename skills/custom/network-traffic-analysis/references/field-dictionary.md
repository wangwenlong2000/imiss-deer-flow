# Canonical Field Dictionary

The analysis script builds a canonical `flows` view using these field names when available.

| Field | Meaning |
| --- | --- |
| `timestamp` | Absolute event or flow start time when the source uses real wall-clock time |
| `end_time` | Absolute flow end time when the source uses real wall-clock time |
| `relative_time_s` | Relative packet time in seconds from capture start |
| `start_relative_time_s` | Relative flow start time in seconds from capture start |
| `end_relative_time_s` | Relative flow end time in seconds from capture start |
| `time_is_relative` | Whether this record uses relative time semantics instead of absolute time |
| `src_ip` | Source IP address |
| `dst_ip` | Destination IP address |
| `src_port` | Source port |
| `dst_port` | Destination port |
| `protocol` | Transport or network protocol |
| `app_protocol` | Application-level protocol guess when available |
| `service` | Service label when available |
| `bytes` | Total bytes observed in the record |
| `packets` | Total packets observed in the record |
| `flow_duration` | Session or flow duration in seconds |
| `duration_ms` | Flow duration in milliseconds |
| `session_state` | Session state or TCP outcome when available |
| `rule_name` | Derived rule label when available |
| `tcp_flags` | TCP flags or dominant TCP flags |
| `dns_query` | DNS query string |
| `tls_sni` | TLS SNI |
| `http_host` | HTTP host |
| `direction` | Inbound, outbound, unidirectional, bidirectional, or other direction label |
| `action` | Allowed, denied, blocked, reset, observed, or other outcome |
| `flow_start_reason` | Why a flow/session began, such as `first_packet` or `tcp_syn` |
| `flow_end_reason` | Why a flow/session ended, such as `tcp_terminator`, `idle_timeout`, or `end_of_capture` |
| `asset_id` | Asset identifier when available |
| `user_id` | User or account identifier when available |
| `device_id` | Device or appliance identifier when available |
| `sensor_id` | Sensor or probe identifier when available |
| `dataset_label` | Dataset label written during preprocessing |
| `traffic_family` | Broad traffic family label such as `web`, `dns`, or `network` |
| `source_table` | Internal table name generated for the source file |
| `source_file` | Original input file path used by the script |

## Time semantics

Use these rules consistently:

- If `time_is_relative = false`, prefer `timestamp` and `end_time`.
- If `time_is_relative = true`, prefer `relative_time_s`, `start_relative_time_s`, and `end_relative_time_s`.
- Relative-time datasets must not be interpreted as real wall-clock dates.
- Relative-time buckets may appear in analysis and RAG as `t+0s`, `t+3600s`, and similar labels.

## Mapping behavior

- The script preserves original source columns and does not rewrite uploaded CSV headers.
- During analysis, `analyze.py` builds a canonical `flows` view on top of each source table.
- Canonical fields are resolved in this order:
  1. exact canonical field name
  2. shared aliases from `field_mapping.yaml`
  3. source-specific profile aliases, such as `wireshark_packet`
- When a source-specific profile is detected, `inspect` reports the matched profile name alongside the canonical field mapping.

## Notes

- `field_mapping.yaml` is the operational mapping source of truth.
- This reference explains the field meanings and intended use in analysis, preprocessing, and RAG.
