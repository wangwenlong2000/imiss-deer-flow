import math
import re
from typing import Any, Dict, List

from .base import BaseDetector


class HardcodedCredDetector(BaseDetector):
    violation_type = "hardcoded_cred"

    SECRET_ASSIGNMENT = re.compile(
        r"(?i)\b([A-Za-z0-9_-]*(?:password|passwd|pwd|secret|token|api[_-]?key|apikey|access[_-]?key|private[_-]?key|client[_-]?secret|authorization|_pass|_key)[A-Za-z0-9_-]*)\b"
        r"\s*[:=]\s*(?:['\"]([^'\"]{4,})['\"]|([^\s#;,]{6,}))"
    )
    PRIVATE_KEY = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----", re.IGNORECASE)
    JWT = re.compile(r"\beyJ[A-Za-z0-9_-]{12,}\.eyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{16,}\b")
    CLOUD_TOKEN = re.compile(r"\b(?:AKIA[A-Z0-9]{16}|ASIA[A-Z0-9]{16}|gh[pousr]_[A-Za-z0-9]{20,}|xox[baprs]-[A-Za-z0-9-]{20,})\b")
    AUTH_HEADER = re.compile(r"(?i)\bauthorization\b\s*[:=]\s*['\"]?(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
    CONNECTION_URI = re.compile(
        r"(?i)\b(mysql|postgresql|postgres|redis|mongodb|ftp|amqp|amqps)://[^\s'\"\)]+"
    )
    JDBC_PASSWORD = re.compile(r"(?i)\bjdbc:[^\s'\"]*(password|pwd)=[^&\s'\"]+")

    PLACEHOLDERS = {
        "example",
        "example_token",
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

            private_match = self.PRIVATE_KEY.search(text)
            if private_match and not self._looks_placeholder(text[private_match.end():private_match.end() + 80]):
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

            for pattern, risk_type, rule_id in (
                (self.JWT, "jwt", "hardcoded-jwt"),
                (self.CLOUD_TOKEN, "cloud_token", "hardcoded-cloud-token"),
            ):
                for match in pattern.finditer(text):
                    if self._looks_placeholder(match.group(0)):
                        continue
                    locations.append(self.make_location(field_path, risk_type, match.group(0), rule_id, match.start(), match.end()))
                    rules.append(rule_id)

            auth_match = self.AUTH_HEADER.search(text)
            if auth_match and not self._looks_placeholder(auth_match.group(0).split()[-1]):
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
                        match.start(2) if match.group(2) is not None else match.start(3),
                        match.end(2) if match.group(2) is not None else match.end(3),
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
            if jdbc_match and self._connection_contains_secret(jdbc_match.group(0)):
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
        if lowered.startswith(("bearer ", "basic ")):
            return self._looks_placeholder(lowered.split(None, 1)[1])
        if set(lowered) <= {"*", "x", "X"}:
            return True
        normalized = lowered.replace("-", "_")
        if normalized.startswith(("example_", "sample_", "dummy_", "fake_")):
            return True
        if re.fullmatch(r"(?:\*|x|X|#|•|_){4,}(?:[- ](?:\*|x|X|#|•|_){2,})*", value.strip()):
            return True
        if re.fullmatch(r"(?:<|\[)?(?:your|example|sample|redacted|masked)[-_ ]?[a-z0-9_-]*(?:>|\])?", normalized):
            return True
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
        if value.startswith("$") or "getenv" in lowered or "process.env" in lowered or "os.environ" in lowered:
            return False
        if any(token in lowered for token in ["settings.", "config.", "secrets.", "read_secret", "get_secret", "load_from_kms", "secret-manager", "vault.", "secret_store.", "secret_provider.", "secret_manager.", "get_env(", "input("]):
            return False
        if re.match(r"[a-z_][a-z0-9_.]*\s*\(", value, re.IGNORECASE):
            return False
        if key_lowered.endswith("_env") or "example" in key_lowered:
            return False
        return True

    def _connection_contains_secret(self, uri: str) -> bool:
        if self._looks_placeholder(uri):
            return False
        query_password = re.search(r"(?i)(?:password|pwd)=([^&\s'\"]+)", uri)
        if query_password:
            return not self._looks_placeholder(query_password.group(1))
        userinfo = re.search(r"://[^/\s:@]*:([^@\s/]+)@", uri)
        return bool(userinfo and not self._looks_placeholder(userinfo.group(1)))

    def _entropy(self, value: str) -> float:
        if not value:
            return 0.0
        counts = {char: value.count(char) for char in set(value)}
        length = len(value)
        return -sum((count / length) * math.log2(count / length) for count in counts.values())
