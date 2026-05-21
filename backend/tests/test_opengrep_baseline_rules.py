from pathlib import Path

import yaml


def test_baseline_compliance_rules_parse_and_include_all_local_rules():
    repo_root = Path(__file__).resolve().parents[2]
    rules_file = repo_root / "skills" / "public" / "opengrep-compliance" / "rules" / "baseline-compliance.yaml"

    rules_config = yaml.safe_load(rules_file.read_text(encoding="utf-8"))
    rules = rules_config["rules"]
    rule_ids = {rule["id"] for rule in rules}

    assert len(rules) == 6
    assert rule_ids == {
        "python-requests-verify-false",
        "python-subprocess-shell-true",
        "python-hardcoded-secret-key",
        "js-eval-or-new-function",
        "react-dangerously-set-inner-html",
        "docker-privileged-container",
    }

    docker_rule = next(rule for rule in rules if rule["id"] == "docker-privileged-container")
    assert docker_rule["pattern"] == "privileged: true"
    assert isinstance(docker_rule["pattern"], str)
