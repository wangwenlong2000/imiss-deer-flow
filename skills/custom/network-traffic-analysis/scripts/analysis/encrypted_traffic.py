"""
Encrypted Traffic Analysis Module

Real implementations of:
- JA3/JA3S TLS fingerprinting (standard algorithm)
- Real JA3 hash database with known application signatures
- Proper TLS behavior analysis
- Priority-based application classification (no overlapping conditions)
- Timing pattern analysis for encrypted tunnel detection

References:
- JA3: https://github.com/salesforce/ja3
- HASSH: https://github.com/salesforce/hassh
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class TLSFingerprint:
    """Represents a JA3/JA3S TLS fingerprint."""
    ja3_hash: str
    ja3_string: str
    ja3s_hash: str
    ja3s_string: str
    tls_version: str
    cipher_suite: str
    extensions: list[str]
    elliptic_curves: list[str]
    point_formats: list[str]


@dataclass
class EncryptedFlowAnalysis:
    """Result of encrypted flow analysis."""
    ja3_fingerprint: TLSFingerprint | None
    application_guess: str
    confidence: float
    behavior_tags: list[str]
    risk_indicators: list[str]
    risk_score: float
    classification_method: str
    evidence_level: str
    metadata_source: str


def default_ja3_database_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "ja3_fingerprints.json"


def load_ja3_database(path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    database_path = Path(path) if path else default_ja3_database_path()
    if not database_path.exists():
        return {}
    try:
        payload = json.loads(database_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    records = payload.get("records", payload if isinstance(payload, list) else [])
    database: dict[str, dict[str, Any]] = {}
    if not isinstance(records, list):
        return database
    for record in records:
        if not isinstance(record, dict):
            continue
        fingerprint = str(record.get("fingerprint") or record.get("ja3") or "").strip().lower()
        if not fingerprint:
            continue
        database[fingerprint] = record
    return database

# Weak/deprecated cipher suites
WEAK_CIPHERS: set[str] = {
    "RC4", "DES", "3DES", "MD5", "NULL", "EXPORT", "anon",
    "RC2", "IDEA", "SEED", "CAMELLIA128"
}

# Deprecated TLS versions
DEPRECATED_TLS: set[str] = {"1.0", "1.1", "SSLv3", "SSLv2"}


class EncryptedTrafficAnalyzer:
    """
    Analyzes encrypted traffic patterns without decryption.
    
    Uses:
    - JA3/JA3S fingerprinting (standard algorithm)
    - Real JA3 database for application identification
    - Priority-based classification (no overlapping conditions)
    - TLS behavior analysis
    """

    def __init__(self, ja3_database_path: str | Path | None = None) -> None:
        self.ja3_database = load_ja3_database(ja3_database_path)
    
    def generate_ja3_fingerprint(self, tls_metadata: dict[str, Any]) -> TLSFingerprint:
        """
        Generate JA3/JA3S fingerprint using the standard algorithm.
        
        JA3 string format: SSLVersion,Cipher,SSLExtension,EllipticCurve,EllipticCurvePointFormat
        
        The algorithm concatenates decimal values of TLS handshake fields
        and computes MD5 hash.
        
        Args:
            tls_metadata: TLS handshake fields
            
        Returns:
            TLSFingerprint with JA3 hashes
        """
        version = str(tls_metadata.get("tls_version") or tls_metadata.get("version") or "771")
        ciphers = self._list_value(tls_metadata.get("tls_ciphers") or tls_metadata.get("ciphers"))
        extensions = self._list_value(tls_metadata.get("tls_extensions") or tls_metadata.get("extensions"))
        curves = self._list_value(tls_metadata.get("tls_supported_groups") or tls_metadata.get("supported_groups"))
        point_formats = self._list_value(tls_metadata.get("tls_point_formats") or tls_metadata.get("point_formats"))
        
        # Build JA3 string (comma-separated, dash-separated within fields)
        cipher_str = "-".join(str(c) for c in ciphers) if ciphers else ""
        ext_str = "-".join(str(e) for e in extensions) if extensions else ""
        curve_str = "-".join(str(c) for c in curves) if curves else ""
        point_str = "-".join(str(p) for p in point_formats) if point_formats else ""
        
        ja3_string = f"{version},{cipher_str},{ext_str},{curve_str},{point_str}"
        ja3_hash = hashlib.md5(ja3_string.encode()).hexdigest()
        
        # JA3S (server response)
        server_ciphers = self._list_value(tls_metadata.get("tls_server_cipher") or tls_metadata.get("server_ciphers"))
        server_ext = self._list_value(tls_metadata.get("tls_server_extensions") or tls_metadata.get("server_extensions"))
        server_cipher_str = "-".join(str(c) for c in server_ciphers) if server_ciphers else ""
        server_ext_str = "-".join(str(e) for e in server_ext) if server_ext else ""
        
        ja3s_string = f"{version},{server_cipher_str},{server_ext_str}"
        ja3s_hash = hashlib.md5(ja3s_string.encode()).hexdigest()
        
        return TLSFingerprint(
            ja3_hash=ja3_hash,
            ja3_string=ja3_string,
            ja3s_hash=ja3s_hash,
            ja3s_string=ja3s_string,
            tls_version=self._parse_tls_version(version),
            cipher_suite=cipher_str,
            extensions=[str(e) for e in extensions],
            elliptic_curves=[str(c) for c in curves],
            point_formats=[str(p) for p in point_formats]
        )
    
    def classify_by_ja3(self, ja3_hash: str) -> dict[str, Any]:
        """
        Classify application using JA3 hash lookup.
        
        Args:
            ja3_hash: MD5 hash of JA3 string
            
        Returns:
            Classification result or empty dict if no match
        """
        if ja3_hash.lower() in self.ja3_database:
            return self.ja3_database[ja3_hash.lower()]
        return {}
    
    def analyze_tls_behavior(self, flow_data: dict[str, Any]) -> dict[str, Any]:
        """
        Analyze TLS behavior for anomalies.
        
        Checks:
        - TLS version (deprecated versions)
        - Cipher strength (weak ciphers)
        - Certificate issues (self-signed, expired)
        - Extension anomalies
        - SNI patterns (DGA, IP-as-SNI)
        
        Args:
            flow_data: Flow features
            
        Returns:
            Behavior analysis result
        """
        risk_indicators = []
        behavior_tags = []
        
        # 1. TLS version check
        tls_version = flow_data.get("tls_version", "")
        if tls_version in DEPRECATED_TLS:
            risk_indicators.append(f"Deprecated TLS version: {tls_version}")
            behavior_tags.append("outdated_security")
        
        # 2. Cipher strength check
        cipher = flow_data.get("cipher_suite", "")
        weak_found = [c for c in WEAK_CIPHERS if c.lower() in cipher.lower()]
        if weak_found:
            risk_indicators.append(f"Weak cipher(s): {', '.join(weak_found)}")
            behavior_tags.append("weak_encryption")
        
        # 3. Certificate issues
        if flow_data.get("cert_self_signed"):
            risk_indicators.append("Self-signed certificate")
            behavior_tags.append("suspicious_cert")
        
        if flow_data.get("cert_expired"):
            risk_indicators.append("Expired certificate")
            behavior_tags.append("expired_cert")
        
        # 4. Extension anomalies
        extensions = self._list_value(flow_data.get("tls_extensions", []))
        suspicious_exts = {"13172", "17513", "65281"}  # heartbeat, EMS, renegotiation
        found_suspicious = [e for e in extensions if e in suspicious_exts]
        if found_suspicious:
            risk_indicators.append(f"Suspicious extensions: {found_suspicious}")
            behavior_tags.append("unusual_extensions")
        
        # 5. SNI analysis
        sni = flow_data.get("tls_sni", "")
        if sni:
            if self._is_dga_like(sni):
                risk_indicators.append(f"Possible DGA domain in SNI: {sni}")
                behavior_tags.append("dga_indicator")
            
            if self._is_ip_address(sni):
                risk_indicators.append(f"IP address as SNI: {sni}")
                behavior_tags.append("ip_as_sni")
        
        # 6. Handshake timing
        handshake_time = flow_data.get("handshake_duration_ms", 0)
        if 0 < handshake_time < 10:
            risk_indicators.append("Unusually fast handshake (automation?)")
            behavior_tags.append("automated_client")
        elif handshake_time > 2000:
            behavior_tags.append("slow_handshake")
        
        return {
            "risk_indicators": risk_indicators,
            "behavior_tags": behavior_tags,
            "risk_score": min(1.0, len(risk_indicators) * 0.2)
        }
    
    def classify_application(self, flow_data: dict[str, Any],
                            ja3_hash: str = "") -> dict[str, Any]:
        """
        Classify encrypted application with priority-based rules.
        
        Priority order (no overlapping):
        1. JA3 hash match (highest confidence)
        2. Port-based classification (medium confidence)
        3. Flow characteristic-based (lower confidence)
        
        Args:
            flow_data: Flow features
            ja3_hash: JA3 fingerprint hash
            
        Returns:
            Classification result
        """
        # Priority 1: JA3 match
        if ja3_hash:
            ja3_result = self.classify_by_ja3(ja3_hash)
            if ja3_result:
                return {
                    "application": ja3_result.get("application") or ja3_result.get("label", "ja3_match"),
                    "category": ja3_result.get("category", "unknown"),
                    "confidence": ja3_result.get("confidence", 0.8),
                    "method": "ja3_match",
                    "description": ja3_result.get("description", ""),
                    "risk_level": ja3_result.get("risk_level", "info"),
                    "source": ja3_result.get("source", ""),
                    "source_url": ja3_result.get("source_url", ""),
                    "evidence_level": "strong",
                }
        
        # Priority 2: Port-based classification
        dst_port = flow_data.get("dst_port", 0)
        if dst_port > 0:
            port_classification = self._classify_by_port(dst_port)
            if port_classification:
                return port_classification
        
        # Priority 3: Flow characteristic-based
        return self._classify_by_flow_characteristics(flow_data)
    
    def _classify_by_port(self, port: int) -> dict[str, Any] | None:
        """Classify by well-known ports."""
        port_map = {
            443: {"application": "https", "category": "web", "confidence": 0.45, "description": "HTTPS traffic inferred from destination port"},
            8443: {"application": "https_alt", "category": "web", "confidence": 0.4, "description": "HTTPS alternate port inferred from destination port"},
            993: {"application": "imaps", "category": "email", "confidence": 0.55, "description": "IMAP over TLS inferred from destination port"},
            995: {"application": "pop3s", "category": "email", "confidence": 0.55, "description": "POP3 over TLS inferred from destination port"},
            465: {"application": "smtps", "category": "email", "confidence": 0.55, "description": "SMTP over TLS inferred from destination port"},
            587: {"application": "smtp_submission", "category": "email", "confidence": 0.45, "description": "SMTP submission inferred from destination port"},
            636: {"application": "ldaps", "category": "directory", "confidence": 0.55, "description": "LDAP over TLS inferred from destination port"},
            853: {"application": "dot", "category": "dns", "confidence": 0.6, "description": "DNS over TLS inferred from destination port"},
            5222: {"application": "xmpp", "category": "messaging", "confidence": 0.5, "description": "XMPP inferred from destination port"},
            5223: {"application": "xmpps", "category": "messaging", "confidence": 0.55, "description": "XMPP over TLS inferred from destination port"},
        }
        result = port_map.get(port)
        if result:
            result = dict(result)
            result["method"] = "port_inference"
            result["evidence_level"] = "weak"
        return result
    
    def _classify_by_flow_characteristics(self, flow_data: dict[str, Any]) -> dict[str, Any]:
        """
        Classify by flow characteristics when JA3 and port fail.
        
        Uses mutually exclusive conditions ordered by specificity.
        """
        bytes_total = flow_data.get("bytes", 0)
        packets = flow_data.get("packets", 0)
        duration = flow_data.get("duration_ms", 0)
        
        # Condition 1: Streaming media (long duration + high volume)
        if duration > 60000 and bytes_total > 50_000_000:
            return {
                "application": "streaming_media",
                "category": "media",
                "confidence": 0.6,
                "method": "flow_characteristics",
                "evidence_level": "weak",
                "description": "Likely streaming media (long duration, high volume)"
            }
        
        # Condition 2: Large file transfer (high volume + moderate duration)
        if bytes_total > 10_000_000 and duration > 10000:
            return {
                "application": "file_transfer",
                "category": "data_transfer",
                "confidence": 0.65,
                "method": "flow_characteristics",
                "evidence_level": "weak",
                "description": "Likely file transfer or bulk data transfer"
            }
        
        # Condition 3: API call (short duration + few packets)
        if packets < 20 and duration < 1000:
            return {
                "application": "api_call",
                "category": "microservice",
                "confidence": 0.55,
                "method": "flow_characteristics",
                "evidence_level": "weak",
                "description": "Likely API or microservice communication"
            }
        
        # Condition 4: Web browsing (moderate packets + moderate duration)
        if packets < 100 and duration < 30000:
            return {
                "application": "web_browsing",
                "category": "web",
                "confidence": 0.5,
                "method": "flow_characteristics",
                "evidence_level": "weak",
                "description": "Likely web browsing"
            }
        
        # Default: unknown
        return {
            "application": "unknown",
            "category": "unknown",
            "confidence": 0.3,
            "method": "flow_characteristics",
            "evidence_level": "weak",
            "description": "Could not confidently classify"
        }
    
    def detect_encrypted_tunnel(self, flow_data: dict[str, Any]) -> dict[str, Any]:
        """
        Detect encrypted tunnels (VPN, SSH, C2).
        
        Indicators:
        - High volume on non-standard ports
        - Long-lived interactive connections
        - Uniform packet sizes
        - Regular timing (C2 beacon)
        """
        indicators = []
        tunnel_types = []
        
        dst_port = flow_data.get("dst_port", 0)
        total_bytes = flow_data.get("bytes", 0)
        duration = flow_data.get("duration_ms", 0)
        packets = flow_data.get("packets", 0)
        
        # 1. Non-standard port with high volume
        standard_ports = {80, 443, 8080, 8443}
        if dst_port not in standard_ports and total_bytes > 10_000_000:
            indicators.append(f"High volume ({total_bytes/1e6:.1f}MB) on non-standard port {dst_port}")
            tunnel_types.append("possible_tunnel")
        
        # 2. Long-lived interactive connection
        if duration > 300_000 and packets > 100:  # 5 minutes, 100 packets
            indicators.append(f"Long-lived interactive connection ({duration/1000:.0f}s)")
            tunnel_types.append("possible_ssh_tunnel")
        
        # 3. Uniform packet sizes (VPN indicator)
        if packets > 100 and total_bytes > 0:
            avg_size = total_bytes / packets
            if 1000 <= avg_size <= 1500:
                indicators.append(f"Uniform packet sizes (~{avg_size:.0f} bytes, VPN-typical)")
                tunnel_types.append("possible_vpn")
        
        # 4. Periodicity (C2 beacon)
        periodicity_score = flow_data.get("periodicity_score", 0)
        if periodicity_score > 0.8:
            indicators.append("High periodicity (possible C2 beacon)")
            tunnel_types.append("possible_beacon")
        
        return {
            "tunnel_indicators": indicators,
            "possible_tunnel_types": tunnel_types,
            "risk_score": min(1.0, len(indicators) * 0.25),
            "summary": f"Detected {len(tunnel_types)} tunnel type(s)" if tunnel_types else "No strong tunnel indicators"
        }
    
    def analyze_encrypted_flow(self, flow_data: dict[str, Any]) -> EncryptedFlowAnalysis:
        """
        Comprehensive encrypted flow analysis.
        
        Args:
            flow_data: Complete flow features
            
        Returns:
            EncryptedFlowAnalysis result
        """
        # Generate or use preprocessed JA3 fingerprint
        ja3_fingerprint = None
        ja3_hash = str(flow_data.get("ja3_hash") or "").strip()
        ja3s_hash = str(flow_data.get("ja3s_hash") or "").strip()
        if ja3_hash or ja3s_hash:
            ja3_fingerprint = TLSFingerprint(
                ja3_hash=ja3_hash,
                ja3_string=str(flow_data.get("ja3_string") or ""),
                ja3s_hash=ja3s_hash,
                ja3s_string=str(flow_data.get("ja3s_string") or ""),
                tls_version=self._parse_tls_version(str(flow_data.get("tls_version") or "")),
                cipher_suite=str(flow_data.get("tls_ciphers") or ""),
                extensions=self._list_value(flow_data.get("tls_extensions")),
                elliptic_curves=self._list_value(flow_data.get("tls_supported_groups")),
                point_formats=self._list_value(flow_data.get("tls_point_formats")),
            )
        elif flow_data.get("tls_version") and (flow_data.get("tls_ciphers") or flow_data.get("ciphers")):
            ja3_fingerprint = self.generate_ja3_fingerprint(flow_data)
            ja3_hash = ja3_fingerprint.ja3_hash
        
        # Behavior analysis
        behavior = self.analyze_tls_behavior(flow_data)
        
        # Application classification (with JA3 priority)
        app = self.classify_application(flow_data, ja3_hash)
        
        # Tunnel detection
        tunnel = self.detect_encrypted_tunnel(flow_data)
        
        # Combine risk indicators
        all_risk = behavior.get("risk_indicators", [])
        all_risk.extend(tunnel.get("tunnel_indicators", []))
        
        app_risk_score = 0.0
        if app.get("risk_level") in ["high", "critical"]:
            all_risk.append(f"Known {app.get('application', 'unknown')} detected")
            app_risk_score = 0.9 if app.get("risk_level") == "critical" else 0.75
        
        all_tags = behavior.get("behavior_tags", [])
        all_tags.extend(tunnel.get("possible_tunnel_types", []))
        
        return EncryptedFlowAnalysis(
            ja3_fingerprint=ja3_fingerprint,
            application_guess=app.get("application", "unknown"),
            confidence=app.get("confidence", 0.0),
            behavior_tags=all_tags,
            risk_indicators=all_risk,
            risk_score=max(
                behavior.get("risk_score", 0),
                tunnel.get("risk_score", 0),
                app_risk_score,
            ),
            classification_method=app.get("method", "unknown"),
            evidence_level=app.get("evidence_level") or self._evidence_level_for_method(app.get("method", "")),
            metadata_source=str(flow_data.get("tls_metadata_source") or "missing"),
        )
    
    # Private helpers
    
    def _parse_tls_version(self, code: str) -> str:
        """Parse TLS version from code."""
        versions = {"257": "SSLv3", "769": "1.0", "770": "1.1", "771": "1.2", "772": "1.3"}
        return versions.get(code, code)

    def _list_value(self, value: Any) -> list[str]:
        if value in (None, ""):
            return []
        if isinstance(value, list):
            return [str(item) for item in value if str(item)]
        return [item for item in str(value).replace(";", "-").split("-") if item]

    def _evidence_level_for_method(self, method: str) -> str:
        if method == "ja3_match":
            return "strong"
        if method in {"tls_metadata", "sni_rule"}:
            return "medium"
        if method in {"port_inference", "flow_characteristics"}:
            return "weak"
        return "unknown"
    
    def _is_dga_like(self, domain: str) -> bool:
        """Check if domain looks like DGA-generated."""
        parts = domain.split(".")
        if len(parts) < 2:
            return False
        
        name = parts[0]
        if len(name) < 10:
            return False
        
        entropy = self._shannon_entropy(name)
        if entropy < 3.5:
            return False
        
        has_digits = any(c.isdigit() for c in name)
        has_letters = any(c.isalpha() for c in name)
        
        return has_digits and has_letters and len(name) > 15
    
    def _is_ip_address(self, value: str) -> bool:
        """Check if value is an IP address."""
        parts = value.split(".")
        if len(parts) != 4:
            return False
        try:
            return all(0 <= int(p) <= 255 for p in parts)
        except ValueError:
            return False
    
    def _shannon_entropy(self, s: str) -> float:
        """Calculate Shannon entropy."""
        if not s:
            return 0.0
        prob = [float(s.count(c)) / len(s) for c in set(s)]
        return -sum(p * math.log2(p) for p in prob if p > 0)
