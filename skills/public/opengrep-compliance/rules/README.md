# OpenGrep Local Rules

These rules were downloaded during build preparation.

- OpenGrep version: v1.21.0
- Rules source: opengrep/opengrep-rules main
- Downloaded asset: opengrep_manylinux_x86
- Official rules path: official/opengrep-rules
- Community rules path: community/trailofbits-semgrep-rules
- Malicious-code rules path: community/apiiro-malicious-code-ruleset
- Local baseline rules: baseline-compliance.yaml
- Imported official rules intentionally keep upstream runtime `.yaml` and `.yml` rule files. Test fixtures and repository metadata from the upstream rule repository are excluded so OpenGrep loads only rule configs and security software does not index sample exploit files.
- Imported Trail of Bits rules intentionally keep runtime `.yaml` and `.yml` rule files. Test files and repository metadata are excluded.
- Imported Apiiro malicious-code rules intentionally keep runtime `.yaml` and `.yml` rule files. Non-rule samples and repository metadata are excluded. The upstream `obfuscation/php/php_obfuscation_declarations.yml` rule is disabled because OpenGrep 1.21.0 cannot parse its PHP `const $VAR = (...);` pattern.

Runtime scans must use this local directory and must not use remote configs or `--config auto`.
