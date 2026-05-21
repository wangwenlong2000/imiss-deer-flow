---
name: opengrep-compliance
description: Run a hybrid code compliance workflow: local OpenGrep rule scanning first, then LLM analysis for attack chains, concrete fixes, false-positive review, and risk prioritization.
---

# OpenGrep Compliance

Use this skill when the user asks for code compliance analysis, security scanning, SAST, static code audit, unsafe API checks, policy violation checks, OpenGrep scanning, attack-chain analysis, remediation suggestions, or a Markdown compliance analysis report.

## Purpose

This skill runs a hybrid scan:

```text
Hybrid scan: OpenGrep rules + LLM analysis
├── Layer 1: OpenGrep rules
│   ├── Fast
│   ├── Deterministic
│   └── No hallucinated findings
├── Layer 2: LLM analysis
│   ├── Slower
│   ├── Cross-rule reasoning
│   └── Concrete remediation guidance
├── Layer 2 handles what rules cannot:
│   ├── Connect attack chains
│   ├── Recommend specific fix code
│   └── Assess false positives and priority
└── Layer 1 + Layer 2 are complementary:
    ├── Rule layer protects accuracy
    └── LLM layer adds depth
```

OpenGrep runs from the local skill directory. It is designed for environments where build-time network access is allowed, but runtime scanning must be offline.

## Runtime Rules

- Do not access the network while scanning.
- Do not use `--config auto`.
- Do not use remote rule URLs.
- Use only the local OpenGrep binary under this skill.
- Use only local rules under this skill's `rules/` directory.
- Write reports to the workspace, usually under `reports/opengrep/`.

## Standard Invocation

From the project workspace, run:

```bash
python /mnt/skills/public/opengrep-compliance/scripts/run_scan.py \
  --target . \
  --output-dir reports/opengrep
```

For a narrower scan:

```bash
python /mnt/skills/public/opengrep-compliance/scripts/run_scan.py \
  --target backend \
  --output-dir reports/opengrep-backend
```

## Outputs

The script writes:

```text
reports/opengrep/opengrep.json
reports/opengrep/opengrep.sarif
reports/opengrep/summary.json
reports/opengrep/report.md
reports/opengrep/rule-report.md
reports/opengrep/llm-analysis-template.md
```

After the scan, read `rule-report.md` and `summary.json` first. If issues are found, inspect `opengrep.json` for full file paths, line numbers, messages, and rule IDs. Then perform Layer 2 LLM analysis using the source code context and `llm-analysis-template.md`.

## Required Workflow

1. Run the OpenGrep scan with the local runner.
2. Read `summary.json` and `rule-report.md`.
3. For important findings, read the relevant source files referenced by `opengrep.json`.
4. Perform LLM analysis:
   - Connect related findings into possible attack chains.
   - Recommend concrete remediation steps and code snippets when safe.
   - Evaluate false positives and confidence.
   - Prioritize by exploitability, blast radius, data sensitivity, and fix cost.
5. Present the final response as a hybrid analysis, clearly separating rule evidence from LLM inference.

## Response Guidance

When reporting results to the user, include:

- Total finding count.
- Finding count by severity.
- Layer 1 OpenGrep findings with file and line.
- Layer 2 LLM analysis, including attack chains, remediation guidance, false-positive assessment, and priority.
- The local report paths.
- Markdown report paths, especially `reports/opengrep/report.md` and `reports/opengrep/rule-report.md`.

When making an inference that is not directly reported by OpenGrep, say it is an LLM inference from code context.

If the scan cannot run because the binary or rules are missing, explain which local asset is missing and ask the user to rerun the download/preparation step.

## Local Assets

Expected Linux binary:

```text
vendor/opengrep/linux/opengrep
```

Expected local rules:

```text
rules/
```

The rule set contains bundled baseline rules and may include the official OpenGrep rules under:

```text
rules/official/opengrep-rules/
```

It may also include community security-audit rules from Trail of Bits under:

```text
rules/community/trailofbits-semgrep-rules/
```

It may also include malicious-code and supply-chain risk rules from Apiiro under:

```text
rules/community/apiiro-malicious-code-ruleset/
```

Use the whole `rules/` directory as the scan config so local baseline rules, imported official rules, imported community audit rules, and imported malicious-code rules are loaded.
