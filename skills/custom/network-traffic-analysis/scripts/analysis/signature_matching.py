from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SignatureRule:
    rule_id: str
    severity: str
    category: str
    description: str
    patterns: tuple[str, ...]
    fields: tuple[str, ...]


BUILTIN_SIGNATURE_RULES: tuple[SignatureRule, ...] = (
    SignatureRule(
        rule_id="dynamic-dns-infra",
        severity="high",
        category="c2_or_evasion",
        description="Dynamic DNS infrastructure often seen in malware or low-friction C2 setups.",
        patterns=("duckdns.org", "no-ip", "ddns.net", "dyndns", "hopto.org", "zapto.org", "servehttp.com", "myftp.org"),
        fields=("dns_query", "tls_sni", "http_host"),
    ),
    SignatureRule(
        rule_id="tor-or-anonymity-indicator",
        severity="high",
        category="privacy_overlay_or_hidden_service",
        description="TOR or anonymity-network related hostname observed in semantic fields.",
        patterns=(".onion", "torproject", "tor2web", "onion.to"),
        fields=("dns_query", "tls_sni", "http_host"),
    ),
    SignatureRule(
        rule_id="payload-staging-domain",
        severity="medium",
        category="staging_or_exfiltration",
        description="Common staging/exfiltration domain marker observed in semantic fields.",
        patterns=("pastebin.com", "paste.ee", "transfer.sh", "anonfiles", "githubusercontent.com", "gist.githubusercontent.com"),
        fields=("dns_query", "tls_sni", "http_host"),
    ),
    SignatureRule(
        rule_id="remote-admin-or-tunnel",
        severity="medium",
        category="remote_access_or_tunneling",
        description="Known remote tunneling or remote-admin naming pattern detected.",
        patterns=("ngrok", "tailscale", "anydesk", "teamviewer", "rustdesk", "remcos"),
        fields=("dns_query", "tls_sni", "http_host", "service", "rule_name"),
    ),
    SignatureRule(
        rule_id="malware-family-indicator",
        severity="high",
        category="malware_family",
        description="Malware-family keyword matched in semantic fields.",
        patterns=("tinba", "zeus", "dridex", "emotet", "trickbot", "cobaltstrike", "meterpreter", "sliver"),
        fields=("dns_query", "tls_sni", "http_host", "rule_name"),
    ),
)


def _normalize_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip().lower()


class _Node:
    __slots__ = ("children", "fail", "outputs")

    def __init__(self) -> None:
        self.children: dict[str, _Node] = {}
        self.fail: _Node | None = None
        self.outputs: list[str] = []


class AhoCorasickMatcher:
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


def build_builtin_signature_matcher() -> tuple[AhoCorasickMatcher, dict[str, list[SignatureRule]]]:
    patterns = sorted({pattern.lower() for rule in BUILTIN_SIGNATURE_RULES for pattern in rule.patterns})
    pattern_to_rules: dict[str, list[SignatureRule]] = {}
    for rule in BUILTIN_SIGNATURE_RULES:
        for pattern in rule.patterns:
            pattern_to_rules.setdefault(pattern.lower(), []).append(rule)
    return AhoCorasickMatcher(patterns), pattern_to_rules


def scan_signature_hits(
    records: list[dict[str, Any]],
    *,
    candidate_fields: list[str],
) -> list[dict[str, Any]]:
    matcher, pattern_to_rules = build_builtin_signature_matcher()
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
                    hits.append(hit)
    return hits
