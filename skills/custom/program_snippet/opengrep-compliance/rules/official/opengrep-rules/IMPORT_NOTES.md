# OpenGrep Official Rules Import Notes

This directory contains a local import of all OpenGrep official runtime YAML/YML rules.

Only runtime `.yaml` and `.yml` rule files are kept. Upstream test fixtures, `*.test.yaml` files, repository metadata, and sample vulnerable files are intentionally excluded because they are not required at runtime and can be blocked by security software.

The `opengrep-compliance` skill loads the parent `rules/` directory, so these official rules are used together with the local baseline rules.
