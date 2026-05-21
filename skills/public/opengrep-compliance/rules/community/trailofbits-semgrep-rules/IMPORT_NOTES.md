# Trail of Bits Semgrep Rules Import Notes

This directory contains a local import of Trail of Bits Semgrep runtime YAML/YML rules.

- Source: trailofbits/semgrep-rules master
- Import strategy: runtime `.yaml`/`.yml` rule files only
- Excluded: `.github` metadata and `*.test.yaml`/`*.test.yml` files

The `opengrep-compliance` skill loads the parent `rules/` directory, so these community rules are used together with local baseline rules and OpenGrep official rules.
