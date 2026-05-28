# Apiiro Malicious Code Ruleset Import Notes

This directory contains a local import of Apiiro malicious-code-ruleset runtime YAML/YML rules.

- Source: `apiiro/malicious-code-ruleset` main
- License: MIT
- Import strategy: runtime `.yaml`/`.yml` rule files only
- Excluded: `.github` metadata and non-rule samples
- Disabled for OpenGrep 1.21.0 compatibility: `obfuscation/php/php_obfuscation_declarations.yml`

The `opengrep-compliance` skill loads the parent `rules/` directory, so these community malicious-code rules are used together with local baseline rules, OpenGrep official rules, and Trail of Bits rules.
