"""
Device Fingerprinting Module

Provides JA3/HASSH fingerprinting, device type identification, and device
profiling for network traffic analysis.

This module identifies devices based on:
- TLS handshake characteristics (JA3)
- SSH key exchange parameters (HASSH)
- HTTP User-Agent strings
- DHCP options
- Communication patterns
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class DeviceFingerprint:
    """Represents a device fingerprint."""
    ja3_hash: str
    hassh_hash: str
    user_agent: str
    device_type: str  # iot, mobile, server, desktop, network_equipment
    device_os: str
    confidence: float
    device_profile: dict[str, Any]


class DeviceFingerprinter:
    """
    Identifies and profiles devices based on network traffic characteristics.

    Focuses on:
    - JA3 TLS fingerprinting for device identification
    - HASSH SSH fingerprinting
    - HTTP User-Agent analysis
    - Device type classification
    - Device profiling and clustering
    """

    def __init__(self, profile_path: str | Path | None = None) -> None:
        self.profile_path = Path(profile_path) if profile_path else self.default_profile_path()
        self.profile_metadata, self.profiles = self.load_profiles(self.profile_path)
        self.external_dir = self.default_external_dir()
        self.external_metadata, self.external_sources = self.load_external_sources(self.external_dir)

    @staticmethod
    def default_profile_path() -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "device_profiles.json"

    @staticmethod
    def default_external_dir() -> Path:
        return Path(__file__).resolve().parents[2] / "data" / "external"

    @staticmethod
    def load_profiles(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if not path.exists():
            return {"source": str(path), "loaded": False}, []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return {"source": str(path), "loaded": False, "error": str(exc)}, []
        profiles = payload.get("profiles", [])
        if not isinstance(profiles, list):
            profiles = []
        metadata = {
            "source": str(path),
            "loaded": True,
            "version": payload.get("version", "unknown"),
            "profile_count": len(profiles),
            "description": payload.get("description", ""),
        }
        return metadata, [profile for profile in profiles if isinstance(profile, dict)]

    @staticmethod
    def load_external_sources(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
        sources: dict[str, Any] = {
            "dhcp_fingerprints": {},
            "mac_vendors": {},
            "p0f_signatures": {},
        }
        metadata: dict[str, Any] = {"source": str(path), "loaded": False, "sources": []}
        if not path.exists():
            return metadata, sources

        manifest_path = path / "source_manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                metadata["sources"] = manifest.get("sources", [])
                metadata["generated_at"] = manifest.get("generated_at", "")
            except (OSError, json.JSONDecodeError) as exc:
                metadata["manifest_error"] = str(exc)

        sources["dhcp_fingerprints"] = DeviceFingerprinter._load_fingerbank_dhcp(
            path / "fingerbank_dhcp_fingerprints.conf"
        )
        sources["mac_vendors"] = DeviceFingerprinter._load_wireshark_manuf(path / "wireshark_manuf.txt")
        sources["p0f_signatures"] = DeviceFingerprinter._load_p0f_signatures(path / "p0f.fp.2012032901")
        metadata["loaded"] = any(bool(value) for value in sources.values())
        metadata["counts"] = {name: len(value) for name, value in sources.items()}
        return metadata, sources

    @staticmethod
    def _device_family(label: str) -> tuple[str, str]:
        text = label.lower()
        if any(term in text for term in ("iphone", "ipad", "ipod", "android", "smartphone", "tablet")):
            os_name = "iOS" if any(term in text for term in ("iphone", "ipad", "ipod", "apple")) else "Android"
            return "mobile", os_name
        if any(term in text for term in ("router", "switch", "access point", "wireless ap", "firewall", "catalyst")):
            return "network_equipment", "Network OS"
        if any(term in text for term in ("printer", "camera", "smart-tv", "tv", "audio", "video", "iot", "embedded")):
            return "iot", "Embedded"
        if "windows" in text or "macintosh" in text or "mac os" in text:
            return "desktop", "Windows" if "windows" in text else "macOS"
        if any(term in text for term in ("linux", "bsd", "solaris")):
            return "server", "Linux/Unix"
        return "unknown", "unknown"

    @staticmethod
    def _load_fingerbank_dhcp(path: Path) -> dict[str, dict[str, Any]]:
        if not path.exists():
            return {}
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            return {}

        class_ranges: list[tuple[int, int, str]] = []
        os_records: dict[int, dict[str, Any]] = {}
        section: tuple[str, int] | None = None
        block_name = ""
        block_values: list[str] = []

        def finish_block() -> None:
            nonlocal block_name, block_values
            if not section or not block_name:
                block_name = ""
                block_values = []
                return
            kind, ident = section
            if kind == "os":
                os_records.setdefault(ident, {})[block_name] = [value for value in block_values if value]
            block_name = ""
            block_values = []

        for raw_line in lines:
            line = raw_line.strip()
            if line.startswith("#") or not line:
                continue
            if line == "EOT":
                finish_block()
                continue
            if block_name:
                block_values.append(line)
                continue
            if line.startswith("[") and line.endswith("]"):
                finish_block()
                parts = line.strip("[]").split()
                if len(parts) == 2 and parts[1].isdigit():
                    section = (parts[0], int(parts[1]))
                    if parts[0] == "os":
                        os_records.setdefault(int(parts[1]), {})
                else:
                    section = None
                continue
            if not section or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            kind, ident = section
            if value == "<<EOT":
                block_name = key
                block_values = []
                continue
            if kind == "class":
                if key == "description":
                    os_records.setdefault(-ident, {})["class_description"] = value
                elif key == "members" and "-" in value:
                    start, end = value.split("-", 1)
                    class_name = os_records.get(-ident, {}).get("class_description", f"class_{ident}")
                    if start.isdigit() and end.isdigit():
                        class_ranges.append((int(start), int(end), str(class_name)))
            elif kind == "os":
                os_records.setdefault(ident, {})[key] = value

        fingerprints: dict[str, dict[str, Any]] = {}
        for ident, record in os_records.items():
            if ident < 0:
                continue
            description = str(record.get("description") or "")
            class_name = next((name for start, end, name in class_ranges if start <= ident <= end), "")
            device_type, device_os = DeviceFingerprinter._device_family(f"{class_name} {description}")
            for fingerprint in record.get("fingerprints", []) or []:
                normalized = DeviceFingerprinter._normalize_dhcp_fingerprint(fingerprint)
                if normalized:
                    fingerprints[normalized] = {
                        "id": ident,
                        "description": description,
                        "class": class_name,
                        "device_type": device_type,
                        "device_os": device_os,
                        "vendor_id": record.get("vendor_id", []),
                    }
        return fingerprints

    @staticmethod
    def _load_wireshark_manuf(path: Path) -> dict[str, dict[str, str]]:
        if not path.exists():
            return {}
        vendors: dict[str, dict[str, str]] = {}
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            return vendors
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 2)
            if len(parts) < 2:
                continue
            prefix = parts[0].lower().replace("-", ":")
            vendor = parts[2] if len(parts) > 2 else parts[1]
            vendors[prefix] = {"short": parts[1], "vendor": vendor}
        return vendors

    @staticmethod
    def _load_p0f_signatures(path: Path) -> dict[str, dict[str, str]]:
        if not path.exists():
            return {}
        signatures: dict[str, dict[str, str]] = {}
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            return signatures
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#") or line.count(":") < 6:
                continue
            parts = line.split(":")
            signature = ":".join(parts[:6]).lower()
            signatures[signature] = {"os": parts[6], "details": parts[7] if len(parts) > 7 else ""}
        return signatures

    @staticmethod
    def _text(value: Any) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _list_text(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip().lower() for item in value if str(item).strip()]
        if value in (None, ""):
            return []
        return [part.strip().lower() for part in str(value).replace(";", ",").split(",") if part.strip()]

    @staticmethod
    def _contains_any(haystack: str, needles: list[str]) -> list[str]:
        return [needle for needle in needles if needle and needle in haystack]

    @staticmethod
    def _normalize_dhcp_fingerprint(value: Any) -> str:
        if value in (None, ""):
            return ""
        parts = [part.strip() for part in str(value).replace(";", ",").split(",")]
        return ",".join(part for part in parts if part.isdigit())

    @staticmethod
    def _normalize_mac_prefix(value: Any) -> str:
        cleaned = "".join(char for char in str(value or "").lower() if char in "0123456789abcdef")
        if len(cleaned) < 6:
            return ""
        return ":".join(cleaned[index:index + 2] for index in range(0, min(len(cleaned), 12), 2))

    def _lookup_mac_vendor(self, mac_value: Any) -> dict[str, str] | None:
        normalized = self._normalize_mac_prefix(mac_value)
        if not normalized:
            return None
        vendors = self.external_sources.get("mac_vendors", {})
        candidates = [
            normalized[:17],
            normalized[:14],
            normalized[:11],
            normalized[:8],
        ]
        for candidate in candidates:
            if candidate in vendors:
                return vendors[candidate]
        return None

    @staticmethod
    def _vendor_family(vendor: str) -> tuple[str, str, float]:
        text = vendor.lower()
        if any(term in text for term in ("cisco", "juniper", "aruba", "ubiquiti", "mikrotik", "fortinet", "palo alto", "extreme networks", "brocade", "netgear", "tp-link")):
            return "network_equipment", "Network OS", 0.58
        if any(term in text for term in ("espressif", "raspberry pi", "arduino", "tuya", "ring", "nest", "hikvision", "dahua", "axis communications")):
            return "iot", "Embedded", 0.62
        if any(term in text for term in ("apple", "samsung", "xiaomi", "oppo", "vivo", "oneplus")):
            return "mobile", "Mobile OS", 0.52
        if any(term in text for term in ("dell", "hewlett", "lenovo", "intel", "vmware", "asustek", "acer")):
            return "desktop", "Desktop OS", 0.48
        return "unknown", "unknown", 0.0

    def _external_dhcp_score(self, flow_data: dict[str, Any]) -> dict[str, Any] | None:
        fingerprint = self._normalize_dhcp_fingerprint(flow_data.get("dhcp_fingerprint"))
        if not fingerprint:
            return None
        match = self.external_sources.get("dhcp_fingerprints", {}).get(fingerprint)
        if not match:
            return None
        confidence = 0.78 if match.get("device_type") != "unknown" else 0.62
        return {
            "device_type": match.get("device_type", "unknown"),
            "device_os": match.get("device_os", "unknown"),
            "confidence": confidence,
            "indicators": [f"dhcp_fingerprint:{fingerprint}", f"fingerbank_os:{match.get('description', '')}"],
            "classification_method": "external_dhcp_fingerprint",
            "matched_profile": f"fingerbank_os_{match.get('id')}",
            "source": "external:fingerbank_dhcp_legacy",
            "description": f"{match.get('class', '')} {match.get('description', '')}".strip(),
        }

    def _external_mac_score(self, flow_data: dict[str, Any]) -> dict[str, Any] | None:
        vendor = self._lookup_mac_vendor(flow_data.get("mac_src") or flow_data.get("mac_dst"))
        if not vendor:
            return None
        vendor_name = vendor.get("vendor", "")
        device_type, device_os, confidence = self._vendor_family(vendor_name)
        if not confidence:
            return {
                "device_type": "unknown",
                "device_os": "unknown",
                "confidence": 0.3,
                "indicators": [f"oui_vendor:{vendor_name}"],
                "classification_method": "external_oui_vendor",
                "matched_profile": vendor.get("short", ""),
                "source": "external:wireshark_manuf",
                "description": "OUI vendor only; no device family inferred.",
            }
        return {
            "device_type": device_type,
            "device_os": device_os,
            "confidence": confidence,
            "indicators": [f"oui_vendor:{vendor_name}"],
            "classification_method": "external_oui_vendor",
            "matched_profile": vendor.get("short", ""),
            "source": "external:wireshark_manuf",
            "description": "MAC OUI vendor family inference; exact model is not implied.",
        }

    def _external_p0f_score(self, flow_data: dict[str, Any]) -> dict[str, Any] | None:
        os_hint = self._text(flow_data.get("p0f_os"))
        signature = self._text(flow_data.get("tcp_syn_signature"))
        match = None
        if signature:
            match = self.external_sources.get("p0f_signatures", {}).get(signature)
            if match:
                os_hint = f"{match.get('os', '')} {match.get('details', '')}".strip().lower()
        if not os_hint:
            return None
        device_type, device_os = self._device_family(os_hint)
        return {
            "device_type": device_type,
            "device_os": device_os,
            "confidence": 0.68 if match else 0.55,
            "indicators": [f"p0f_os:{os_hint}"],
            "classification_method": "external_p0f_os",
            "matched_profile": signature if match else "",
            "source": "external:cert_p0f_syn" if match else "input:p0f_os",
            "description": "Passive TCP OS fingerprint evidence.",
        }

    def _profile_score(self, profile: dict[str, Any], flow_data: dict[str, Any]) -> dict[str, Any] | None:
        evidence: list[str] = []
        score = 0.0

        user_agent = self._text(flow_data.get("http_user_agent"))
        dhcp_hostname = self._text(flow_data.get("dhcp_hostname"))
        tls_sni = self._text(flow_data.get("tls_sni") or flow_data.get("http_host"))
        service = self._text(flow_data.get("service") or flow_data.get("protocol"))
        ja3_hash = self._text(flow_data.get("ja3_hash"))
        ja3s_hash = self._text(flow_data.get("ja3s_hash"))
        ssh_hassh = self._text(flow_data.get("ssh_hassh"))
        dst_port = int(flow_data.get("dst_port") or 0)

        ja3_matches = set(self._list_text(profile.get("ja3_hashes"))) & {ja3_hash, ja3s_hash}
        if ja3_matches:
            score += 0.95
            evidence.append(f"ja3_match:{next(iter(ja3_matches))}")

        hassh_matches = set(self._list_text(profile.get("hassh_hashes"))) & {ssh_hassh}
        if hassh_matches:
            score += 0.9
            evidence.append(f"hassh_match:{next(iter(hassh_matches))}")

        ua_matches = self._contains_any(user_agent, self._list_text(profile.get("user_agent_contains")))
        if ua_matches:
            score += 0.72
            evidence.append(f"user_agent:{ua_matches[0]}")

        dhcp_matches = self._contains_any(dhcp_hostname, self._list_text(profile.get("dhcp_hostname_contains")))
        if dhcp_matches:
            score += 0.62
            evidence.append(f"dhcp_hostname:{dhcp_matches[0]}")

        sni_matches = self._contains_any(tls_sni, self._list_text(profile.get("sni_contains")))
        if sni_matches:
            score += 0.55
            evidence.append(f"sni:{sni_matches[0]}")

        service_matches = self._contains_any(service, self._list_text(profile.get("service_contains")))
        if service_matches:
            score += 0.35
            evidence.append(f"service:{service_matches[0]}")

        ports = {int(port) for port in profile.get("ports", []) if str(port).isdigit()}
        if dst_port and dst_port in ports:
            score += 0.28
            evidence.append(f"port:{dst_port}")

        if not evidence:
            return None

        base_confidence = float(profile.get("confidence") or 0.65)
        confidence = min(0.98, max(0.35, base_confidence * min(1.0, score)))
        return {
            "device_type": profile.get("device_type", "unknown"),
            "device_os": profile.get("device_os", "unknown"),
            "confidence": round(confidence, 4),
            "indicators": evidence,
            "classification_method": "profile_match",
            "matched_profile": profile.get("profile_id", "unknown"),
            "source": profile.get("source", self.profile_metadata.get("source", "")),
            "description": profile.get("description", ""),
        }

    def generate_ja3_fingerprint(self, tls_metadata: dict[str, Any]) -> str:
        """
        Generate JA3 fingerprint from TLS handshake.

        Args:
            tls_metadata: TLS handshake metadata

        Returns:
            JA3 hash string
        """
        version = tls_metadata.get("version", "771")
        ciphers = tls_metadata.get("ciphers", [])
        extensions = tls_metadata.get("extensions", [])
        curves = tls_metadata.get("supported_groups", [])
        point_formats = tls_metadata.get("point_formats", [])

        cipher_str = "-".join(str(c) for c in ciphers) if ciphers else ""
        ext_str = "-".join(str(e) for e in extensions) if extensions else ""
        curve_str = "-".join(str(c) for c in curves) if curves else ""
        point_str = "-".join(str(p) for p in point_formats) if point_formats else ""

        ja3_string = f"{version},{cipher_str},{ext_str},{curve_str},{point_str}"
        return hashlib.md5(ja3_string.encode()).hexdigest()

    def generate_hassh_fingerprint(self, ssh_metadata: dict[str, Any]) -> str:
        """
        Generate HASSH fingerprint from SSH key exchange.

        Args:
            ssh_metadata: SSH key exchange metadata

        Returns:
            HASSH hash string
        """
        kex_algorithms = ssh_metadata.get("kex_algorithms", [])
        encryption_algorithms = ssh_metadata.get("encryption_algorithms", [])
        mac_algorithms = ssh_metadata.get("mac_algorithms", [])
        compression_algorithms = ssh_metadata.get("compression_algorithms", [])

        kex_str = ",".join(kex_algorithms) if kex_algorithms else ""
        enc_str = ",".join(encryption_algorithms) if encryption_algorithms else ""
        mac_str = ",".join(mac_algorithms) if mac_algorithms else ""
        comp_str = ",".join(compression_algorithms) if compression_algorithms else ""

        hassh_string = f"{kex_str};{enc_str};{mac_str};{comp_str}"
        return hashlib.md5(hassh_string.encode()).hexdigest()

    def identify_device_type(self, flow_data: dict[str, Any]) -> dict[str, Any]:
        """
        Identify device type from flow characteristics.

        Args:
            flow_data: Dictionary with flow features

        Returns:
            Dictionary with device identification results
        """
        matches = [
            candidate
            for profile in self.profiles
            if (candidate := self._profile_score(profile, flow_data)) is not None
        ]
        for external_candidate in (
            self._external_dhcp_score(flow_data),
            self._external_p0f_score(flow_data),
            self._external_mac_score(flow_data),
        ):
            if external_candidate is not None:
                matches.append(external_candidate)
        if matches:
            return max(matches, key=lambda item: item["confidence"])

        indicators: list[str] = []
        confidence = 0.25
        tls_version = str(flow_data.get("tls_version", ""))
        if tls_version == "1.3":
            indicators.append("modern_device")
            confidence += 0.08

        dst_port = flow_data.get("dst_port", 0)
        if dst_port in [1883, 8883, 5683, 5684]:
            indicators.append("iot_device")
            confidence += 0.25
        elif dst_port in [80, 443, 8080, 8443]:
            indicators.append("general_purpose")
            confidence += 0.06

        bytes_total = flow_data.get("bytes", 0)
        packets = flow_data.get("packets", 0)
        if packets > 0 and bytes_total / packets < 100:
            indicators.append("possible_iot")
            confidence += 0.06

        device_type = "unknown"
        device_os = "unknown"
        if "iot_device" in indicators or "possible_iot" in indicators:
            device_type = "iot"
            device_os = "Embedded"
        elif "modern_device" in indicators:
            device_type = "desktop"
            device_os = "Modern OS"

        return {
            "device_type": device_type,
            "device_os": device_os,
            "confidence": min(1.0, confidence),
            "indicators": indicators,
            "classification_method": "flow_heuristic" if indicators else "insufficient_evidence",
            "matched_profile": "",
            "source": "local_heuristic",
            "description": "Fallback flow-pattern heuristic; use as a low-confidence hint only.",
            "user_agent": flow_data.get("http_user_agent", ""),
        }

    def profile_device(self, ip_address: str, flows: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Build comprehensive device profile from multiple flows.

        Args:
            ip_address: IP address of the device
            flows: List of flow records for this device

        Returns:
            Dictionary with device profile
        """
        if not flows:
            return {
                "ip_address": ip_address,
                "device_type": "unknown",
                "confidence": 0.0,
                "profile": {}
            }

        # Aggregate device characteristics
        device_types = []
        os_list = []
        confidence_scores = []
        unique_ports = set()
        protocols = set()

        for flow in flows:
            identification = self.identify_device_type(flow)
            device_types.append(identification["device_type"])
            os_list.append(identification["device_os"])
            confidence_scores.append(identification["confidence"])
            unique_ports.add(flow.get("dst_port", 0))
            protocols.add(flow.get("protocol", ""))

        # Determine most common device type
        most_common_type = max(set(device_types), key=device_types.count) if device_types else "unknown"
        most_common_os = max(set(os_list), key=os_list.count) if os_list else "unknown"
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
        seen_times = [str(f.get("timestamp", "")) for f in flows if f.get("timestamp")]

        # Build profile
        profile = {
            "ip_address": ip_address,
            "device_type": most_common_type,
            "device_os": most_common_os,
            "confidence": avg_confidence,
            "total_flows": len(flows),
            "unique_ports_contacted": len(unique_ports),
            "ports": sorted(list(unique_ports)),
            "protocols_used": sorted(list(protocols)),
            "first_seen": min(seen_times) if seen_times else "",
            "last_seen": max(seen_times) if seen_times else "",
        }

        return profile

    def cluster_similar_devices(self, profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Cluster similar devices based on characteristics.

        Args:
            profiles: List of device profiles

        Returns:
            List of clusters with member devices
        """
        clusters: dict[str, list[str]] = {}

        for profile in profiles:
            # Create cluster key based on device characteristics
            key = f"{profile.get('device_type', 'unknown')}_{profile.get('device_os', 'unknown')}"
            clusters.setdefault(key, []).append(profile["ip_address"])

        # Convert to cluster list
        result = []
        for cluster_key, members in clusters.items():
            if len(members) > 1:  # Only report clusters with multiple devices
                device_type, device_os = cluster_key.split("_", 1)
                result.append({
                    "cluster_key": cluster_key,
                    "device_type": device_type,
                    "device_os": device_os,
                    "member_count": len(members),
                    "members": members
                })

        # Sort by member count (largest clusters first)
        result.sort(key=lambda x: x["member_count"], reverse=True)

        return result

    def analyze_device(self, flow_data: dict[str, Any]) -> DeviceFingerprint:
        """
        Comprehensive device fingerprinting analysis.

        Args:
            flow_data: Dictionary with complete flow features

        Returns:
            DeviceFingerprint result object
        """
        # Generate JA3 fingerprint
        ja3_hash = ""
        if flow_data.get("tls_version") or flow_data.get("ciphers"):
            ja3_hash = self.generate_ja3_fingerprint(flow_data)

        # Generate HASSH fingerprint
        hassh_hash = ""
        if flow_data.get("ssh_kex"):
            hassh_hash = self.generate_hassh_fingerprint(flow_data.get("ssh_kex", {}))

        # Identify device type
        device_id = self.identify_device_type(flow_data)

        return DeviceFingerprint(
            ja3_hash=ja3_hash,
            hassh_hash=hassh_hash,
            user_agent=flow_data.get("http_user_agent", ""),
            device_type=device_id["device_type"],
            device_os=device_id["device_os"],
            confidence=device_id["confidence"],
            device_profile=device_id
        )
