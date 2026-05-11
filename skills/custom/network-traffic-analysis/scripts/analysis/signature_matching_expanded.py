"""
Expanded Signature Database for Network Traffic Analysis

Contains 50+ signature rules covering:
- Known malware C2 infrastructure
- Cryptomining indicators
- Data exfiltration patterns
- Lateral movement techniques
- Phishing indicators
- Privilege escalation tools
- Command and control patterns
- Suspicious cloud service usage
- Network reconnaissance tools

Each rule includes:
- Unique rule ID
- Severity level (critical/high/medium/low/info)
- Category classification
- Description
- Pattern strings to match
- Fields to search in
- MITRE ATT&CK technique mapping (optional)
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SignatureRule:
    """Represents a signature rule for pattern matching."""
    rule_id: str
    severity: str  # critical, high, medium, low, info
    category: str
    description: str
    patterns: tuple[str, ...]
    fields: tuple[str, ...]
    mitre_technique: str = ""  # MITRE ATT&CK technique ID
    confidence: float = 0.8  # Default confidence score


# Expanded signature rules database
EXPANDED_SIGNATURE_RULES: tuple[SignatureRule, ...] = (
    # ========================================================================
    # MALWARE C2 INFRASTRUCTURE (15 rules)
    # ========================================================================
    
    SignatureRule(
        rule_id="cobalt-strike-beacon",
        severity="critical",
        category="malware_c2",
        description="Cobalt Strike C2 beacon indicator detected",
        patterns=("cobaltstrike", "cs-server", "beacon"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1573.001"
    ),
    
    SignatureRule(
        rule_id="metasploit-handler",
        severity="critical",
        category="malware_c2",
        description="Metasploit handler detected",
        patterns=("metasploit", "msfconsole", "meterpreter"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1573.002"
    ),
    
    SignatureRule(
        rule_id="sliver-c2",
        severity="critical",
        category="malware_c2",
        description="Sliver C2 framework indicator",
        patterns=("sliver", "sliver-server", "sliver-client"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1573.001"
    ),
    
    SignatureRule(
        rule_id="mythic-c2",
        severity="high",
        category="malware_c2",
        description="Mythic C2 framework indicator",
        patterns=("mythic", "mythic-agent", "mythic-server"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1573.001"
    ),
    
    SignatureRule(
        rule_id="empire-starkiller",
        severity="high",
        category="malware_c2",
        description="PowerShell Empire/Starkiller C2 indicator",
        patterns=("empire", "starkiller", "ps-empire"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1059.001"
    ),
    
    SignatureRule(
        rule_id="dynamic-dns-infra",
        severity="high",
        category="c2_or_evasion",
        description="Dynamic DNS infrastructure often seen in malware or C2 setups",
        patterns=("duckdns.org", "no-ip", "ddns.net", "dyndns", "hopto.org", "zapto.org", "servehttp.com", "myftp.org"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1568.001"
    ),
    
    SignatureRule(
        rule_id="tor-anonymy-network",
        severity="high",
        category="anonymization",
        description="TOR or anonymity network indicator",
        patterns=(".onion", "torproject", "tor2web", "onion.to", "onion.ly"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1090.003"
    ),
    
    SignatureRule(
        rule_id="fast-flux-dns",
        severity="high",
        category="c2_or_evasion",
        description="Fast-flux DNS pattern often used by botnets",
        patterns=("fastflux", "flux"),
        fields=("dns_query", "service"),
        mitre_technique="T1568.001"
    ),
    
    SignatureRule(
        rule_id="dga-pattern",
        severity="high",
        category="c2_or_evasion",
        description="Possible Domain Generation Algorithm (DGA) pattern",
        patterns=("dga-domain", "generated-domain"),
        fields=("dns_query", "tls_sni"),
        mitre_technique="T1568.002"
    ),
    
    SignatureRule(
        rule_id="c2-beacon-timing",
        severity="high",
        category="malware_c2",
        description="Regular beacon pattern indicative of C2 communication",
        patterns=("beacon", "heartbeat", "checkin"),
        fields=("dns_query", "http_host", "service"),
        mitre_technique="T1573.001"
    ),
    
    SignatureRule(
        rule_id="dns-tunnel-c2",
        severity="high",
        category="c2_or_evasion",
        description="DNS tunneling for C2 communication",
        patterns=("dnscat", "iodine", "tuns", "dns2tcp"),
        fields=("dns_query", "service"),
        mitre_technique="T1071.004"
    ),
    
    SignatureRule(
        rule_id="https-c2-proxy",
        severity="medium",
        category="malware_c2",
        description="HTTPS-based C2 proxy or redirector",
        patterns=("c2-proxy", "redirector", "c2-relay"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1090.001"
    ),
    
    SignatureRule(
        rule_id="custom-c2-framework",
        severity="high",
        category="malware_c2",
        description="Custom or lesser-known C2 framework indicator",
        patterns=("custom-c2", "implant", "backdoor"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1573.001"
    ),
    
    SignatureRule(
        rule_id="brute-ratel-c2",
        severity="high",
        category="malware_c2",
        description="Brute Ratel C4 C2 framework indicator",
        patterns=("brute-ratel", "brc4"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1573.001"
    ),
    
    SignatureRule(
        rule_id="havoc-c2",
        severity="high",
        category="malware_c2",
        description="Havoc C2 framework indicator",
        patterns=("havoc", "havoc-c2"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1573.001"
    ),
    
    # ========================================================================
    # CRYPTOMINING (5 rules)
    # ========================================================================
    
    SignatureRule(
        rule_id="cryptomining-pool",
        severity="medium",
        category="cryptomining",
        description="Known cryptomining pool domain",
        patterns=("miningpoolhub", "nanopool.org", "minergate", "nicehash", "ethermine"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1496"
    ),
    
    SignatureRule(
        rule_id="stratum-protocol",
        severity="high",
        category="cryptomining",
        description="Stratum mining protocol indicator",
        patterns=("stratum+tcp", "stratum+ssl"),
        fields=("service", "rule_name"),
        mitre_technique="T1496"
    ),
    
    SignatureRule(
        rule_id="crypto-mining-software",
        severity="medium",
        category="cryptomining",
        description="Known mining software indicator",
        patterns=("xmrig", "cgminer", "bfgminer", "ethminer", "lolminer"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1496"
    ),
    
    SignatureRule(
        rule_id="coinhive-miner",
        severity="medium",
        category="cryptomining",
        description="Coinhive or similar browser-based mining service",
        patterns=("coinhive", "cryptoloot", "webmining"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1496"
    ),
    
    SignatureRule(
        rule_id="mining-payment-pool",
        severity="low",
        category="cryptomining",
        description="Mining pool payment or statistics endpoint",
        patterns=("pool-pay", "miner-stats", "hashrate-monitor"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1496"
    ),
    
    # ========================================================================
    # DATA EXFILTRATION (8 rules)
    # ========================================================================
    
    SignatureRule(
        rule_id="payload-staging-domain",
        severity="medium",
        category="staging_or_exfiltration",
        description="Common staging/exfiltration domain marker",
        patterns=("pastebin.com", "paste.ee", "transfer.sh", "anonfiles", "githubusercontent.com", "gist.githubusercontent.com"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1567.002"
    ),
    
    SignatureRule(
        rule_id="cloud-storage-exfil",
        severity="medium",
        category="exfiltration",
        description="Cloud storage service potentially used for data exfiltration",
        patterns=("mega.nz", "mediafire", "zippyshare", "uploadhaven"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1567.002"
    ),
    
    SignatureRule(
        rule_id="dns-exfiltration-tool",
        severity="high",
        category="exfiltration",
        description="DNS-based data exfiltration tool indicator",
        patterns=("dnscat2", "iodine", "dns2tcp", "tuns"),
        fields=("dns_query", "service"),
        mitre_technique="T1048.001"
    ),
    
    SignatureRule(
        rule_id="large-outbound-transfer",
        severity="medium",
        category="exfiltration",
        description="Indicator of large outbound data transfer service",
        patterns=("exfil", "data-upload", "bulk-transfer"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1041"
    ),
    
    SignatureRule(
        rule_id="encoded-data-transfer",
        severity="high",
        category="exfiltration",
        description="Base64 or hex encoded data transfer pattern",
        patterns=("base64-upload", "hex-exfil", "encoded-payload"),
        fields=("dns_query", "http_host", "service"),
        mitre_technique="T1132.001"
    ),
    
    SignatureRule(
        rule_id="icmp-exfiltration",
        severity="high",
        category="exfiltration",
        description="ICMP-based data exfiltration indicator",
        patterns=("icmp-exfil", "ping-tunnel", "icmptunnel"),
        fields=("service", "rule_name"),
        mitre_technique="T1048.003"
    ),
    
    SignatureRule(
        rule_id="http-post-exfiltration",
        severity="medium",
        category="exfiltration",
        description="HTTP POST-based exfiltration pattern",
        patterns=("data-post", "upload-endpoint", "exfil-api"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1041"
    ),
    
    SignatureRule(
        rule_id="screenshot-exfil",
        severity="medium",
        category="exfiltration",
        description="Screenshot or image data exfiltration indicator",
        patterns=("screenshot-upload", "image-exfil", "screen-capture"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1113"
    ),
    
    # ========================================================================
    # LATERAL MOVEMENT (7 rules)
    # ========================================================================
    
    SignatureRule(
        rule_id="psexec-remote-exec",
        severity="high",
        category="lateral_movement",
        description="PsExec or similar remote execution tool",
        patterns=("psexec", "psexesvc", "sysinternals"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1570"
    ),
    
    SignatureRule(
        rule_id="wmi-remote-exec",
        severity="high",
        category="lateral_movement",
        description="Windows Management Instrumentation remote execution",
        patterns=("wmi-exec", "wmicmd", "winrm"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1047"
    ),
    
    SignatureRule(
        rule_id="rdp-brute-force",
        severity="high",
        category="lateral_movement",
        description="RDP brute force or credential stuffing indicator",
        patterns=("rdp-brute", "rdp-scan", "mstsc-scan"),
        fields=("service", "rule_name"),
        mitre_technique="T1110.001"
    ),
    
    SignatureRule(
        rule_id="smb-admin-share",
        severity="medium",
        category="lateral_movement",
        description="SMB administrative share access",
        patterns=("admin$", "c$", "ipc$"),
        fields=("dns_query", "service"),
        mitre_technique="T1021.002"
    ),
    
    SignatureRule(
        rule_id="lateral-movement-tool",
        severity="high",
        category="lateral_movement",
        description="Known lateral movement tool or technique",
        patterns=("lateral-mov", "psexec", "wmiexec", "smbexec"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1021"
    ),
    
    SignatureRule(
        rule_id="ssh-lateral",
        severity="medium",
        category="lateral_movement",
        description="SSH-based lateral movement indicator",
        patterns=("ssh-lateral", "ssh-scan", "ssh-brute"),
        fields=("service", "rule_name"),
        mitre_technique="T1021.004"
    ),
    
    SignatureRule(
        rule_id="dcom-lateral",
        severity="high",
        category="lateral_movement",
        description="DCOM-based lateral movement indicator",
        patterns=("dcom-exec", "dcom-lateral"),
        fields=("service", "rule_name"),
        mitre_technique="T1021.003"
    ),
    
    # ========================================================================
    # PHISHING (5 rules)
    # ========================================================================
    
    SignatureRule(
        rule_id="brand-impersonation",
        severity="high",
        category="phishing",
        description="Brand impersonation or lookalike domain",
        patterns=("login-microsoft", "secure-apple", "account-google", "verify-amazon"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1598.003"
    ),
    
    SignatureRule(
        rule_id="suspicious-url-shortener",
        severity="medium",
        category="phishing",
        description="Suspicious URL shortener service",
        patterns=("bit.ly", "tinyurl", "t.co", "goo.gl"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1566.002"
    ),
    
    SignatureRule(
        rule_id="phishing-kit-hosting",
        severity="high",
        category="phishing",
        description="Known phishing kit hosting provider",
        patterns=("phish-kit", "fake-login", "credential-harvest"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1598.002"
    ),
    
    SignatureRule(
        rule_id="typosquatting-domain",
        severity="medium",
        category="phishing",
        description="Possible typosquatting domain",
        patterns=("g00gle", "microsft", "amaz0n", "faceb00k"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1598.003"
    ),
    
    SignatureRule(
        rule_id="suspicious-email-service",
        severity="medium",
        category="phishing",
        description="Suspicious email service or relay",
        patterns=("mass-mailer", "bulk-email", "email-blast"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1534"
    ),
    
    # ========================================================================
    # PRIVILEGE ESCALATION (5 rules)
    # ========================================================================
    
    SignatureRule(
        rule_id="exploit-kit-indicator",
        severity="critical",
        category="privilege_escalation",
        description="Known exploit kit indicator",
        patterns=("exploit-kit", "ek-server", "landing-page"),
        fields=("dns_query", "tls_sni", "http_host"),
        mitre_technique="T1189"
    ),
    
    SignatureRule(
        rule_id="vulnerability-scanner",
        severity="medium",
        category="privilege_escalation",
        description="Vulnerability scanner or exploitation tool",
        patterns=("vuln-scan", "exploit-db", "metasploit"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1595.002"
    ),
    
    SignatureRule(
        rule_id="local-priv-esc",
        severity="high",
        category="privilege_escalation",
        description="Local privilege escalation tool indicator",
        patterns=("privesc", "local-exploit", "privilege-escalation"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1068"
    ),
    
    SignatureRule(
        rule_id="kernel-exploit",
        severity="critical",
        category="privilege_escalation",
        description="Kernel exploit indicator",
        patterns=("kernel-exploit", "rootkit", "bootkit"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1068"
    ),
    
    SignatureRule(
        rule_id="credential-dump-tool",
        severity="high",
        category="privilege_escalation",
        description="Credential dumping or extraction tool",
        patterns=("mimikatz", "credential-dump", "password-extract"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1003"
    ),
    
    # ========================================================================
    # REMOTE ACCESS TOOLS (5 rules)
    # ========================================================================
    
    SignatureRule(
        rule_id="remote-admin-tunnel",
        severity="medium",
        category="remote_access",
        description="Known remote tunneling or admin tool",
        patterns=("ngrok", "tailscale", "anydesk", "teamviewer", "rustdesk", "remcos"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1572"
    ),
    
    SignatureRule(
        rule_id="rat-indicator",
        severity="high",
        category="remote_access",
        description="Remote Access Trojan (RAT) indicator",
        patterns=("darkcomet", "njrat", "nanocore", "netwire"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1219"
    ),
    
    SignatureRule(
        rule_id="reverse-shell",
        severity="critical",
        category="remote_access",
        description="Reverse shell or callback indicator",
        patterns=("reverse-shell", "callback-shell", "bind-shell"),
        fields=("service", "rule_name"),
        mitre_technique="T1059"
    ),
    
    SignatureRule(
        rule_id="web-shell",
        severity="critical",
        category="remote_access",
        description="Web shell access indicator",
        patterns=("webshell", "web-shell", "cmd.aspx", "cmd.jsp"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1505.003"
    ),
    
    SignatureRule(
        rule_id="vnc-access",
        severity="medium",
        category="remote_access",
        description="VNC remote access indicator",
        patterns=("vnc", "vnc-viewer", "realvnc", "tightvnc"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1021.005"
    ),
    
    # ========================================================================
    # NETWORK RECONNAISSANCE (5 rules)
    # ========================================================================
    
    SignatureRule(
        rule_id="nmap-scanner",
        severity="medium",
        category="reconnaissance",
        description="Nmap network scanner indicator",
        patterns=("nmap", "nmap-scan"),
        fields=("service", "rule_name"),
        mitre_technique="T1046"
    ),
    
    SignatureRule(
        rule_id="mass-scanner",
        severity="medium",
        category="reconnaissance",
        description="Mass scanning tool indicator",
        patterns=("masscan", "zmap", "shodan"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1046"
    ),
    
    SignatureRule(
        rule_id="network-discovery",
        severity="low",
        category="reconnaissance",
        description="Network discovery tool",
        patterns=("net-discovery", "host-discovery", "arp-scan"),
        fields=("service", "rule_name"),
        mitre_technique="T1018"
    ),
    
    SignatureRule(
        rule_id="snmp-enumeration",
        severity="medium",
        category="reconnaissance",
        description="SNMP enumeration indicator",
        patterns=("snmp-enum", "snmpwalk", "snmp-check"),
        fields=("service", "rule_name"),
        mitre_technique="T1018"
    ),
    
    SignatureRule(
        rule_id="ldap-enumeration",
        severity="medium",
        category="reconnaissance",
        description="LDAP enumeration indicator",
        patterns=("ldap-enum", "ldapsearch", "bloodhound"),
        fields=("dns_query", "tls_sni", "http_host", "service"),
        mitre_technique="T1087.002"
    ),
    
    # ========================================================================
    # LEGACY RULES (backward compatibility)
    # ========================================================================
    
    SignatureRule(
        rule_id="malware-family-indicator",
        severity="high",
        category="malware_family",
        description="Malware-family keyword matched in semantic fields",
        patterns=("tinba", "zeus", "dridex", "emotet", "trickbot", "cobaltstrike", "meterpreter", "sliver"),
        fields=("dns_query", "tls_sni", "http_host", "rule_name"),
        mitre_technique="T1587.001"
    ),
)


def _normalize_text(value: Any) -> str:
    """Normalize text value for matching."""
    if value in (None, ""):
        return ""
    return str(value).strip().lower()


class _Node:
    """Aho-Corasick trie node."""
    __slots__ = ("children", "fail", "outputs")

    def __init__(self) -> None:
        self.children: dict[str, _Node] = {}
        self.fail: _Node | None = None
        self.outputs: list[str] = []


class AhoCorasickMatcher:
    """Aho-Corasick multi-pattern string matching algorithm."""
    
    def __init__(self, patterns: list[str]) -> None:
        self.root = _Node()
        for pattern in patterns:
            self._add(pattern)
        self._build_failures()

    def _add(self, pattern: str) -> None:
        node = self.root
        for char in pattern:
            node = node.children.setdefault(char, _Node())
        node.outputs.append(pattern)

    def _build_failures(self) -> None:
        queue: deque[_Node] = deque()
        self.root.fail = self.root
        for child in self.root.children.values():
            child.fail = self.root
            queue.append(child)

        while queue:
            node = queue.popleft()
            for char, child in node.children.items():
                queue.append(child)
                fail = node.fail
                while fail is not None and fail is not self.root and char not in fail.children:
                    fail = fail.fail
                if fail and char in fail.children:
                    child.fail = fail.children[char]
                else:
                    child.fail = self.root
                if child.fail and child.fail.outputs:
                    child.outputs.extend(child.fail.outputs)

    def iter_matches(self, text: str) -> list[str]:
        """Find all pattern matches in text."""
        node = self.root
        matches: list[str] = []
        for char in text:
            while node is not self.root and char not in node.children:
                node = node.fail or self.root
            if char in node.children:
                node = node.children[char]
            else:
                node = self.root
            if node.outputs:
                matches.extend(node.outputs)
        return matches


def build_expanded_signature_matcher() -> tuple[AhoCorasickMatcher, dict[str, list[SignatureRule]]]:
    """Build Aho-Corasick matcher from expanded signature rules."""
    patterns = sorted({pattern.lower() for rule in EXPANDED_SIGNATURE_RULES for pattern in rule.patterns})
    pattern_to_rules: dict[str, list[SignatureRule]] = {}
    for rule in EXPANDED_SIGNATURE_RULES:
        for pattern in rule.patterns:
            pattern_to_rules.setdefault(pattern.lower(), []).append(rule)
    return AhoCorasickMatcher(patterns), pattern_to_rules


def scan_signature_hits_expanded(
    records: list[dict[str, Any]],
    *,
    candidate_fields: list[str],
) -> list[dict[str, Any]]:
    """
    Scan records against expanded signature database.
    
    Args:
        records: List of flow record dictionaries
        candidate_fields: Fields to search for pattern matches
        
    Returns:
        List of hit records with signature metadata
    """
    matcher, pattern_to_rules = build_expanded_signature_matcher()
    hits: list[dict[str, Any]] = []
    
    for record in records:
        for field in candidate_fields:
            normalized = _normalize_text(record.get(field))
            if not normalized:
                continue
            matched_patterns = set(matcher.iter_matches(normalized))
            for pattern in matched_patterns:
                for rule in pattern_to_rules.get(pattern, []):
                    if field not in rule.fields:
                        continue
                    hit = dict(record)
                    hit["matched_field"] = field
                    hit["matched_value"] = str(record.get(field, ""))[:160]
                    hit["matched_pattern"] = pattern
                    hit["signature_rule_id"] = rule.rule_id
                    hit["signature_severity"] = rule.severity
                    hit["signature_category"] = rule.category
                    hit["signature_description"] = rule.description
                    hit["mitre_technique"] = rule.mitre_technique
                    hit["signature_confidence"] = rule.confidence
                    hits.append(hit)
    
    return hits
