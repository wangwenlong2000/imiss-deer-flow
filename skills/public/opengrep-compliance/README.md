# OpenGrep Compliance Skill

This DeerFlow skill adds a conversational hybrid code compliance scanner backed by OpenGrep and LLM analysis.

The rule-scanning layer is intentionally offline at runtime: it uses the local OpenGrep binary in `vendor/opengrep/` and local YAML rules in `rules/`.

## Hybrid Workflow

```text
Layer 1: OpenGrep rules -> fast, deterministic, no hallucinated findings
Layer 2: LLM analysis -> cross-rule reasoning, attack chains, concrete fixes, false-positive review, priority assessment
```

OpenGrep provides grounded evidence. The LLM layer reads the rule output and relevant source code to explain risk, connect findings, and recommend practical fixes.

Official OpenGrep rules can be imported under:

```text
rules/official/opengrep-rules/
```

Trail of Bits Semgrep security-audit rules can be imported under:

```text
rules/community/trailofbits-semgrep-rules/
```

Apiiro malicious-code and supply-chain risk rules can be imported under:

```text
rules/community/apiiro-malicious-code-ruleset/
```

The runner scans with the full `rules/` directory, so bundled baseline rules, imported official rules, imported community audit rules, and malicious-code rules are loaded together.

## Run

From the workspace root:

```bash
python skills/public/opengrep-compliance/scripts/run_scan.py --target . --output-dir reports/opengrep
```

Inside the DeerFlow sandbox, the same script is normally available at:

```bash
python /mnt/skills/public/opengrep-compliance/scripts/run_scan.py --target . --output-dir reports/opengrep
```

## Reports

The runner writes:

- `opengrep.json`
- `opengrep.sarif`
- `summary.json`
- `report.md`
- `rule-report.md`
- `llm-analysis-template.md`

Use `rule-report.md` for deterministic OpenGrep findings, `summary.json` for quick machine-readable totals, and `opengrep.json` for detailed findings. Use `llm-analysis-template.md` as the structure for the second-layer LLM analysis.
