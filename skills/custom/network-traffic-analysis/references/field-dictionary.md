# Canonical Field Dictionary

The analysis script builds a canonical `flows` view using these field names when available.

| Field | Meaning |
| --- | --- |
| `timestamp` | Event or flow start time |
| `src_ip` | Source IP address |
| `dst_ip` | Destination IP address |
| `src_port` | Source port |
| `dst_port` | Destination port |
| `protocol` | Transport or network protocol |
| `app_protocol` | Application-level protocol guess when available |
| `service` | Service label when available |
| `bytes` | Total bytes observed in the record |
| `packets` | Total packets observed in the record |
| `flow_duration` | Session or flow duration |
| `duration_ms` | Flow duration in milliseconds |
| `session_state` | Session state or TCP outcome when available |
| `rule_name` | Derived rule label when available |
| `tcp_flags` | TCP flags |
| `dns_query` | DNS query string |
| `tls_sni` | TLS SNI |
| `http_host` | HTTP host |
| `direction` | Inbound, outbound, or other direction label |
| `action` | Allowed, denied, blocked, reset, or other outcome |
| `source_table` | Internal table name generated for the source file |
| `source_file` | Original input file path used by the script |
