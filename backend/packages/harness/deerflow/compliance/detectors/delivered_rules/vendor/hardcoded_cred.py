import math
import re
from typing import Any, Dict, List

from .base import BaseDetector


class HardcodedCredDetector(BaseDetector):
    violation_type = "hardcoded_cred"

    SECRET_ASSIGNMENT = re.compile(
        r"(?i)\b([A-Za-z0-9_-]*(?:password|passwd|pwd|secret|token|api[_-]?key|apikey|access[_-]?key|private[_-]?key|client[_-]?secret|authorization)[A-Za-z0-9_-]*)\b"
        r"\s*[:=]\s*(?:['\"]([^'\"]{4,})['\"]|([^\s#;,]{6,}))"
    )
    PRIVATE_KEY = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----", re.IGNORECASE)
    AUTH_HEADER = re.compile(r"(?i)\bauthorization\b\s*[:=]\s*['\"]?(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
    CONNECTION_URI = re.compile(
        r"(?i)\b(mysql|postgresql|postgres|redis|mongodb|ftp)://[^\s'\"\)]+"
    )
    JDBC_PASSWORD = re.compile(r"(?i)\bjdbc:[^\s'\"]*(password|pwd)=[^&\s'\"]+")

    PLACEHOLDERS = {
        "example",
        "demo",
        "test",
        "fake",
        "dummy",
        "placeholder",
        "redacted",
        "your_api_key",
        "your-api-key",
        "your_password",
        "your-password",
        "changeme",
        "change_me",
        "none",
        "null",
    }

    ENV_PATTERNS = [
        re.compile(r"(?i)\bos\.getenv\s*\("),
        re.compile(r"(?i)\bos\.environ\.get\s*\("),
        re.compile(r"(?i)\bprocess\.env\."),
        re.compile(r"(?i)\benv\("),
        re.compile(r"(?i)\bget_secret\s*\("),
        re.compile(r"(?i)\bread_secret\s*\("),
        re.compile(r"(?i)\bload_from_kms\s*\("),
        re.compile(r"(?i)\bsecret_manager\s*\("),
        re.compile(r"(?i)\bconfig\.get\s*\("),
        re.compile(r"(?i)\bsecrets\.get\s*\("),
        re.compile(r"(?i)\binput\s*\("),
        re.compile(r"\$\{[A-Z0-9_]+\}"),
        re.compile(r"\{\{[^{}]+\}\}"),
    ]

    def detect(self, sample: Dict[str, Any]) -> Dict[str, Any]:
        locations: List[Dict[str, Any]] = []
        rules: List[str] = []

        for field_path, text in self.iter_search_items(sample):
            if self._is_env_or_template(text):
                continue

            private_match = self.PRIVATE_KEY.search(text)
            if private_match:
                locations.append(
                    self.make_location(
                        field_path,
                        "private_key",
                        private_match.group(0),
                        "hardcoded-private-key-block",
                        private_match.start(),
                        private_match.end(),
                    )
                )
                rules.append("hardcoded-private-key-block")

            auth_match = self.AUTH_HEADER.search(text)
            if auth_match and not self._looks_placeholder(auth_match.group(0)):
                locations.append(
                    self.make_location(
                        field_path,
                        "authorization",
                        auth_match.group(0),
                        "hardcoded-authorization-header",
                        auth_match.start(),
                        auth_match.end(),
                    )
                )
                rules.append("hardcoded-authorization-header")

            for match in self.SECRET_ASSIGNMENT.finditer(text):
                key = match.group(1)
                value = self._clean_value(match.group(2) or match.group(3) or "")
                if not self._is_real_secret_value(key, value):
                    continue
                locations.append(
                    self.make_location(
                        field_path,
                        key.lower(),
                        value,
                        "hardcoded-secret-assignment",
                        match.start(2),
                        match.end(2),
                    )
                )
                rules.append("hardcoded-secret-assignment")

            for match in self.CONNECTION_URI.finditer(text):
                uri = match.group(0)
                if self._connection_contains_secret(uri):
                    locations.append(
                        self.make_location(
                            field_path,
                            "connection_uri",
                            uri,
                            "connection-uri-with-credential",
                            match.start(),
                            match.end(),
                        )
                    )
                    rules.append("connection-uri-with-credential")

            jdbc_match = self.JDBC_PASSWORD.search(text)
            if jdbc_match:
                locations.append(
                    self.make_location(
                        field_path,
                        "jdbc_password",
                        jdbc_match.group(0),
                        "jdbc-password-parameter",
                        jdbc_match.start(),
                        jdbc_match.end(),
                    )
                )
                rules.append("jdbc-password-parameter")

        if not locations:
            return self.no_hit(sample, "No hardcoded credential pattern survived placeholder filtering.")

        confidence = 0.9
        if any(rule in rules for rule in ["hardcoded-private-key-block", "connection-uri-with-credential"]):
            confidence = 0.97
        elif any(self._entropy(loc.get("text", "")) > 3.5 for loc in locations):
            confidence = 0.93

        return self.make_result(
            sample,
            True,
            confidence,
            locations,
            rules,
            "Hardcoded credential evidence was found in normalized text or feature fields.",
            ["refuse", "report", "block_storage"],
        )

    def _clean_value(self, value: str) -> str:
        return value.strip().strip("'\"").strip()

    def _is_env_or_template(self, text: str) -> bool:
        return any(pattern.search(text or "") for pattern in self.ENV_PATTERNS)

    def _looks_placeholder(self, value: str) -> bool:
        lowered = (value or "").lower().strip().strip("'\"<>")
        if not lowered:
            return True
        if set(lowered) <= {"*", "x", "X"}:
            return True
        normalized = lowered.replace("-", "_")
        stripped = normalized.strip("<>").replace("*", "").replace("_", "")
        if lowered.startswith("<") and lowered.endswith(">"):
            return True
        if not stripped:
            return True
        if normalized in self.PLACEHOLDERS:
            return True
        if normalized.startswith("your_") or normalized in {"xxxxx", "xxxx", "xxx"}:
            return True
        if any(token in normalized for token in ["redacted", "provided_by_platform", "token_from_secret_manager"]):
            return True
        return False

    def _is_real_secret_value(self, key: str, value: str) -> bool:
        if len(value) < 6:
            return False
        if self._looks_placeholder(value):
            return False
        lowered = value.lower()
        key_lowered = (key or "").lower()
        if value.startswith("$") or "getenv" in lowered or "process.env" in lowered:
            return False
        if any(token in lowered for token in ["settings.", "config.", "secrets.", "read_secret", "get_secret", "load_from_kms", "secret-manager"]):
            return False
        if key_lowered.endswith("_env") or "example" in key_lowered:
            return False
        return True

    def _connection_contains_secret(self, uri: str) -> bool:
        if self._looks_placeholder(uri):
            return False
        if re.search(r"(?i)(password|pwd)=", uri):
            return True
        return bool(re.search(r"://[^/\s:@]+:[^@\s/]+@", uri) or re.search(r"redis://:[^@\s/]+@", uri))

    def _entropy(self, value: str) -> float:
        if not value:
            return 0.0
        counts = {char: value.count(char) for char in set(value)}
        length = len(value)
        return -sum((count / length) * math.log2(count / length) for count in counts.values())

