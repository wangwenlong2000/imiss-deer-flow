"""Build the type-3 program-code regression set and evaluate the delivered rule.

All newly added credential strings are deterministic, nonfunctional synthetic fixtures.
The evaluation artifact contains IDs and aggregate counts, never matched text.
"""

from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "合规检测/合规评测全部十类违规类型数据/program_code.jsonl"
OUTPUT = ROOT / "合规检测/类型3-hardcoded_cred整改/program_code_type3_regression.jsonl"
METRICS = ROOT / "合规检测/类型3-hardcoded_cred整改/latest_metrics.json"
RULE = ROOT / "backend/packages/harness/deerflow/compliance/detectors/delivered_rules/vendor/hardcoded_cred.py"
CONFIG = ROOT / "config/compliance/detectors.yaml"
sys.path.insert(0, str(ROOT / "backend/packages/harness"))

from deerflow.compliance.detectors.delivered_rules.vendor.hardcoded_cred import HardcodedCredDetector  # noqa: E402


NEGATIVE = [
    'api_key = "<YOUR_API_KEY>"',
    'service_token = "example-token"',
    'db_password = "************"',
    'client_secret = "XXXXXXXXXXXX"',
    'authorization = "Bearer example-token"',
    'private_key = "<REDACTED_PRIVATE_KEY>"',
    'api_key = "sample-api-key"',
    'access_token = "dummy-token"',
    'DB_PASSWORD = "[REDACTED]"',
    'jwt_secret = "<YOUR_JWT_SECRET>"',
    'mysql_uri = "mysql://demo:<YOUR_PASSWORD>@db.example.invalid/app"',
    'redis_uri = "redis://:********@cache.example.invalid:6379/0"',
    'jdbc_url = "jdbc:mysql://db.example.invalid/app?password=<YOUR_PASSWORD>"',
    'headers = {Authorization: "Bearer ************"}',
    'api_key = "####-####-####"',
    'cloud_token = "example-cloud-token"',
    'password = "sample_password"',
    'secret = "fake-secret"',
    'token = "<TOKEN_FROM_SECRET_MANAGER>"',
    'api_key = os.getenv("API_KEY")\nclient_secret = "<YOUR_CLIENT_SECRET>"',
]

# Deliberately synthetic values: they are not usable provider credentials or keys.
POSITIVE = [
    'aws_access_key_id = "AKIA7Q4R9M2X5P8T6L0B"',
    'aws_session_token = "ASIA7Q4R9M2X5P8T6L0B"',
    'github_token = "ghp_R8qT5mL2xN9vC4pS7wA6zB8dF"',
    'slack_token = "xoxb-724196385-579214863-9Lr6Tz4Vn2Qp"',
    'cloud_access_key = "LTAI5tN4rP7qV2xM9sC6"',
    'azure_client_secret = "az-C3rT7mQ9vL2xP5nK8sH"',
    'gcp_service_token = "gcp-4R8vL2mT7qX9pN5sC1"',
    'jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzeW50aGV0aWMifQ.c3ludGhldGljX3NpZ25hdHVyZQ"',
    'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzeW50aGV0aWMifQ.c3ludGhldGljX3NpZ25hdHVyZQ',
    'jwt_secret = "signing-7QmT4xV9pR2sL6nK"',
    'private_key = "-----BEGIN PRIVATE KEY-----\\nU3ludGhldGljS2V5RnJhZ21lbnQxMjM0NTY=\\n-----END PRIVATE KEY-----"',
    '-----BEGIN RSA PRIVATE KEY-----\nU3ludGhldGljS2V5RnJhZ21lbnQxMjM0NTY=\n',
    'signing_private_key = "key-fragment-8TqL2mV9xR4pN7sC"',
    'dsn = "postgresql://svc_user:Pg7QmT4xV9pR@db.example.invalid:5432/app"',
    'uri = "mongodb://worker:Mg5RrP8qL2mT@db.example.invalid:27017/jobs"',
    'redis_url = "redis://:Rd9XvL3mT7qP@cache.example.invalid:6379/2"',
    'jdbc_url = "jdbc:mysql://db.example.invalid:3306/app?user=worker&password=Jd8PqL5mT2vR"',
    'amqp_url = "amqp://worker:Mq6TrP8xL2vN@queue.example.invalid:5672/events"',
    'db_url = "mysql://worker:My4LmT9qR2vP@db.example.invalid/app"',
    'ftp_url = "ftp://worker:Ft7QmL2xR9pV@files.example.invalid/export"',
]


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def make_sample(index: int, content: str, positive: bool) -> dict:
    sample_id = f"program_code-hardcoded-regression-{'pos' if positive else 'neg'}-{index:03d}"
    return {
        "data_id": sample_id,
        "sample_id": sample_id,
        "target_violation_type": "hardcoded_cred",
        "final_violation_type": "hardcoded_cred" if positive else "none",
        "violation_name": "硬编码凭证泄露",
        "data_type": "program_code",
        "trigger_gate": "InputGate",
        "applicable_scenes": ["internal_org", "cross_org", "public_release", "research_anon"],
        "is_positive": positive,
        "sample_kind": "positive" if positive else "negative",
        "content": content,
        "risk_locations": ([{"field": "content", "start": 0, "end": len(content),
                             "entity_type": "synthetic_credential_snippet",
                             "evidence_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest()}]
                           if positive else []),
        "expected_action": {},
        "judgement_basis_type": ["甲方内容制度规定"],
        "judgement_reason": "合成的固定凭证形式" if positive else "占位、掩码或运行时获取，不是固定凭证",
        "data_source": {"type": "manual_synthetic", "license": "internal", "desensitized": True},
        "matched_rules": [],
        "annotation_meta": {"owner": "类型3整改", "annotation_time": "2026-09-17", "review_status": "synthetic_reviewed"},
        "quality_flags": ["synthetic", "type3_regression"],
    }


def score(rows: list[dict]) -> dict:
    detector = HardcodedCredDetector()
    outcomes = []
    for row in rows:
        actual = bool(detector.detect({"data_id": row["data_id"], "content_text": row["content"], "trigger_gate": row["trigger_gate"]})["is_hit"])
        expected = row["target_violation_type"] == "hardcoded_cred" and bool(row["is_positive"])
        outcomes.append((row["sample_id"], row["trigger_gate"], expected, actual))

    def stats(items: list[tuple]) -> dict:
        tp = sum(expected and actual for _, _, expected, actual in items)
        fp = sum(not expected and actual for _, _, expected, actual in items)
        fn = sum(expected and not actual for _, _, expected, actual in items)
        tn = sum(not expected and not actual for _, _, expected, actual in items)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        return {"samples": len(items), "tp": tp, "fp": fp, "fn": fn, "tn": tn,
                "precision": round(precision, 6), "recall": round(recall, 6),
                "f1": round(2 * precision * recall / (precision + recall), 6) if precision + recall else 0.0,
                "fp_ids": [sid for sid, _, expected, actual in items if actual and not expected],
                "fn_ids": [sid for sid, _, expected, actual in items if expected and not actual]}

    return {"overall": stats(outcomes), "new_40": stats(outcomes[-40:]),
            "by_gate": {gate: stats([item for item in outcomes if item[1] == gate]) for gate in sorted({item[1] for item in outcomes})}}


def main() -> None:
    original = load_jsonl(SOURCE)
    if len(original) != 250 or len(NEGATIVE) != 20 or len(POSITIVE) != 20:
        raise ValueError("Unexpected source or fixture count")
    # Preserve every original annotation; only normalize the spelling of output-stage rows.
    for row in original:
        if str(row.get("trigger_gate", "")).lower() == "outputgate":
            row["trigger_gate"] = "OutputGate"
    added = [make_sample(i, content, False) for i, content in enumerate(NEGATIVE, 1)]
    added += [make_sample(i, content, True) for i, content in enumerate(POSITIVE, 1)]
    rows = original + added
    if len({row["sample_id"] for row in rows}) != len(rows):
        raise ValueError("Duplicate sample_id")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    data = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    OUTPUT.write_text(data, encoding="utf-8")
    result = {"evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
              "source": str(SOURCE.relative_to(ROOT)), "dataset": str(OUTPUT.relative_to(ROOT)),
              "source_rows": len(original), "new_negative": len(NEGATIVE), "new_positive": len(POSITIVE),
              "gate_counts": dict(Counter(row["trigger_gate"] for row in rows)),
              "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
              "dataset_sha256": hashlib.sha256(data.encode("utf-8")).hexdigest(),
              "rule_sha256": hashlib.sha256(RULE.read_bytes()).hexdigest(),
              "config_sha256": hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
              "model_version": None,
              "metrics": score(rows)}
    METRICS.parent.mkdir(parents=True, exist_ok=True)
    METRICS.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
