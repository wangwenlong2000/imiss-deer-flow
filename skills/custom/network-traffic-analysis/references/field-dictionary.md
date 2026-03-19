# Canonical Field Dictionary

The script builds a canonical `flows` view using these field names.

| Field | Meaning |
| --- | --- |
| `timestamp` | Event or flow start time |
| `src_ip` | Source IP address |
| `dst_ip` | Destination IP address |
| `src_port` | Source port |
| `dst_port` | Destination port |
| `protocol` | Transport or network protocol |
| `bytes` | Total bytes observed in the record |
| `packets` | Total packets observed in the record |
| `flow_duration` | Session or flow duration |
| `direction` | Inbound, outbound, east-west, or vendor-specific direction |
| `action` | Allowed, denied, blocked, reset, failed, or other outcome |
| `source_table` | The internal table name generated for the source file |
| `source_file` | The original file path |

