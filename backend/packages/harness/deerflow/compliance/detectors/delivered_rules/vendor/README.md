# `compliance_detectors` vendor snapshot

This directory contains the rule implementations delivered in the workspace's
top-level `compliance_detectors/detectors/` directory. The source algorithm is
kept separate from DeerFlow's boundary adapter so standalone evaluation logic
does not leak into the engine.

Only line endings were normalized while importing `base.py`,
`hardcoded_cred.py`, `text_id.py`, and `confidential.py`. The source CRLF hashes
are recorded in `SHA256SUMS`. Do not add manifests here; the three contract
wrappers own registration and gate declarations.

The optional delivery `external/llm_adapter.py` was not vendored because it owns
a second environment-variable-based model configuration and a disk cache.
`../configured_llm.py` implements the same review interface through DeerFlow's
existing `create_chat_model` factory, without a second credential/config path or
raw-response cache.
