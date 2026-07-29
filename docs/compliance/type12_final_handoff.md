# Type 1/2 production integration handoff

## Status

Type 1 structured identifiers (`struct_id`) and type 2 precise geolocation
(`geo_loc`) are enabled lightweight DeerFlow plugins. They run through Registry,
Router, Compliance Engine, IntentGuard, trusted SceneResolver, Policy Matrix,
Audit, all three gates, durable SSE metadata, safe OutputGate delivery, and the
frontend compliance parser.

## Architecture and files

```text
IntentRecognition ─┐
OneCity request  ──┴─> trusted SceneResolver
                         │
normalized unit -> Registry/Router -> detector -> finding
                         │
                         ├─ InputGate (intent join)
                         ├─ ContextGate (evidence)
                         └─ OutputGate (strict buffer / compatible retract)
                                      │
                         Policy Matrix + Audit + SSE metadata
```

Detector packages:

- `backend/packages/harness/deerflow/compliance/detectors/regex_struct_id/`
- `backend/packages/harness/deerflow/compliance/detectors/regex_geo_loc/`

Tests and evaluation:

- `backend/tests/test_compliance_regex_struct_id.py`
- `backend/tests/test_compliance_regex_geo_loc.py`
- `backend/tests/test_compliance_type12_integration.py`
- `backend/tests/test_compliance_type12_frozen_regression.py`
- `backend/tests/test_compliance_intent_inputgate.py`
- `backend/tests/test_compliance_scene_resolver.py`
- `backend/tests/test_compliance_output_strict_streaming.py`
- `scripts/eval_compliance_type12_engine.py`
- `scripts/benchmark_compliance_output_type12.py`

## Versions and SHA-256

The standalone rule cores identify themselves as:

- struct: `struct_id_detector_presidio_rule_v2.0`;
- geo: `geo_loc_detector_rule_final_v2.0`.

Despite the historical struct version name, the integrated adapter always
constructs `OptionalPresidioAnalyzer("never")`. No Presidio or spaCy code path,
package, or model is loaded.

| File | SHA-256 |
| --- | --- |
| `regex_struct_id/detector.py` | `db8d3b36b11bff53572bcea8cea806a6b28d15e1fce395a2e9e750f81701a5b8` |
| `regex_struct_id/legacy_core.py` | `78106a16b7dcc37afd03b49e17af76ee002089170c2644bafde9cfd0f6168751` |
| `regex_struct_id/manifest.yaml` | `52507b51c89836158f0eb44825140021e550a71ee625d699883984a2799b4396` |
| `regex_geo_loc/detector.py` | `4bb776a815ef98dff760ae0c32da236043819f4d93296745e86b19a514a99055` |
| `regex_geo_loc/legacy_core.py` | `248478431d1632c99a975ffe1cf0fde65232f3b0afbdd5f26ad9db8535b7e71b` |
| `regex_geo_loc/manifest.yaml` | `37eca66a30066dbdf265936d4fd04e66af94d08ee81abcafb62b67329fe8c9bf` |

An empty `__init__.py` in each package has the standard empty-file SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

## Standalone and Engine result definitions

The approved standalone result is the boolean `is_positive_pred` and bounded
`risk_locations_pred` produced by each legacy rule core. The original struct
code optionally supported a Presidio enhancement, but the production comparison
and frozen baseline use rule-only mode; no claim is made for an unavailable
Presidio runtime.

The Engine result is a `DetectionHit` after DetectionUnit adaptation,
manifest/router gate enforcement, and detector isolation. Intent and policy can
later remove a benign InputGate entity or transform an actionable hit; those
are disposition stages and are not detector classification metrics.

Frozen rule-only classification:

| Type | Samples | TP/FP/TN/FN | Precision | Recall | F1 | Accuracy | Engine parity |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| struct_id | 44 | 23/2/19/0 | 0.9200 | 1.0000 | 0.9583 | 0.9545 | 100% |
| geo_loc | 40 | 24/0/16/0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 100% |

The two struct false positives are recorded by the evaluator for diagnosis; the
frozen samples and rules were not modified to remove them.

## IntentRecognition

The canonical non-sensitive summary is
`intent_context.compliance_intent`:

```json
{
  "intent_type": "public_release",
  "requested_operation": "publish",
  "risk_level": "high",
  "is_high_risk": true,
  "confidence": 0.95,
  "reason_codes": ["public_disclosure"],
  "reason": "public_disclosure",
  "source": "intent_recognition"
}
```

InputGate does not treat an identifier/coordinate hit alone as a high-risk
request. Missing intent fields are `unknown` and retain findings for conservative
review rather than becoming low risk.

## SceneResolver protocol

`compliance_request` carries authenticated actor/org/department,
server-issued roles/permissions, operation/audience, resource ownership,
classification, target org, anonymization, and an untrusted `scene_hint`.
InputGate writes `scene_context`; ContextGate and OutputGate reuse it. Public
release has highest priority, organisation crossings are strict, research
requires permission plus anonymization, and missing/conflicting facts are
`_unknown`. See `onecity_deerflow_compliance_contract.md`.

## Three gates and policy

- InputGate joins type 1/2 findings with the canonical intent before selecting
  the resolved scene's matrix cell.
- ContextGate normalizes skill evidence and safely rewrites/withholds risky
  evidence according to the same scene.
- OutputGate checks the complete answer inside the outer model wrapper.

Actions come only from
`config/compliance/policy_matrix.yaml`. Findings do not embed actions.
`self_use` may allow owner-visible data; internal scenes follow their cells;
cross-org/public/research use stricter transformation/refusal; `_unknown`
retains conservative review and uses fail-closed delivery for risky output.

## Safe streaming and authoritative state

`cross_org`, `public_release`, and `research_anon` use
strict buffering:

```text
model (nostream + disable_streaming)
  -> complete response inside OutputGate wrapper
  -> detect + matrix actions
  -> safe replacement
  -> messages/custom/updates SSE
```

The original risky value is absent from user-visible SSE, `messages`,
`raw_messages`, graph state/checkpoints, thread history, refresh state, and title
input. The `nostream` tag prevents pre-wrapper model chunks from entering
LangGraph's messages stream.

`self_use`, `internal_org`, and `_unknown` retain compatibility streaming. A raw
model chunk can precede the retract event, but the authoritative state is
replaced when policy requires mutation. `_unknown` selects its conservative
warn/manual-review matrix cell rather than silently becoming self-use. Audit records
`streaming_mode=compatible_retract` and
`transient_exposure_possible=true`. An explicit self-use `allow` remains
visible because it is policy-safe for its verified owner.

## Audit and SSE

Audit records include request/thread/gate, final scene, scene hint,
permission/fallback/source/reason codes, bounded actor/operation/resource
facts, canonical intent, all findings/locations, per-violation actions, basis,
diagnostics, streaming mode, exposure flag, and audit reference.

Durable assistant metadata:

```json
{
  "compliance": {
    "gate": "OutputGate",
    "scene": "public_release",
    "streaming_mode": "strict_buffered",
    "transient_exposure_possible": false,
    "actions": ["desensitize", "refuse"],
    "violation_types": ["geo_loc", "struct_id"],
    "basis": ["..."],
    "audit_ref": "audit-...",
    "notice": "...",
    "retracted": true
  }
}
```

`compliance_retract` preserves message ID, scene, streaming mode, all violation
types, actions, safe replacement, notice, basis, and audit reference. Combined
risks remain separate badges rather than overwriting one another.

## Performance

Thirty deterministic repetitions on the running Gateway container, excluding a
remote model:

| Measurement | P50 ms | P95 ms | Max ms |
| --- | ---: | ---: | ---: |
| struct_id OutputGate detection | 0.168 | 0.318 | 9.590 |
| geo_loc OutputGate detection | 0.172 | 0.215 | 0.221 |
| combined OutputGate detection | 0.207 | 0.220 | 0.233 |
| public_release first safe SSE event | 5.978 | 6.972 | 68.956 |
| internal_org first compatibility event | 5.932 | 8.800 | 14.288 |

The first strict outlier includes graph/model setup. Remote-model generation
latency is intentionally excluded so detector/transport cost stays repeatable.

## Frontend

The existing parser maps:

- `struct_id` -> “结构化标识符泄露”;
- `geo_loc` -> “精确地理位置泄露”.

It consumes gate, scene, action, basis, audit reference, and multiple violation
types from durable metadata/custom events. API and unit tests validate the data
path. No claim of browser visual/console verification is made unless a human
executes the manual steps below:

1. Send one public-release struct, geo, and combined deterministic test.
2. Confirm one/two badges and the expected Chinese labels.
3. Confirm the body never flashes the raw fixture in strict scenes.
4. Refresh and confirm only safe history remains.
5. Check the browser console for new React errors.

## Containers, ports, and commands

Expected isolated containers:

- Gateway: `huangyuzhe-compliance-deer-flow-gateway`;
- LangGraph: `huangyuzhe-compliance-deer-flow-langgraph`.

Ports remain Nginx 3538, LangGraph 3539, Gateway 3540, and frontend 3541.

Key commands:

```bash
cd /app/backend
PYTHONPATH=.:packages/harness uv run pytest \
  tests/test_compliance_regex_struct_id.py \
  tests/test_compliance_regex_geo_loc.py -v
PYTHONPATH=.:packages/harness uv run pytest \
  tests/test_compliance_type12_frozen_regression.py -q
PYTHONPATH=.:packages/harness uv run pytest tests/test_compliance_*.py -q
PYTHONPATH=.:packages/harness uv run python \
  ../scripts/eval_compliance_type12_engine.py --json
PYTHONPATH=.:packages/harness uv run python \
  ../scripts/benchmark_compliance_output_type12.py --iterations 30
```

Frontend commands are run from `/app/frontend` (or the host `frontend/`):
TypeScript check, compliance test, and compliance-file ESLint.

## Current limitations

- Production IAM/resource facts must still come from OneCity/customer systems.
- Direct LangGraph development access must not be a production trust boundary.
- Type 8/9/10 model assets are separate; a missing asset must not be “fixed” by
  changing type 1/2 rules.
- Compatibility scenes retain an honest pre-retract transport window.
- Browser visual acceptance remains a human step when no browser is available.
- Strict buffering delays the first visible answer until model completion.

## Next detector owner

Read `new_detector_onboarding_guide.md`, copy `_template_detector`, keep policy
and evaluation labels out of detector code, add an immutable held-out set, test
all gates/scenes/SSE/state/audit, run Registry strict mode and the entire
compliance suite, then enable the manifest.

## Rollback

Use a normal `git revert` of the local integration commits, or disable
`regex_struct_id`/`regex_geo_loc` in detector configuration for an operational
rollback. Do not delete detector directories, rewrite frozen data, or use
`git reset --hard`. Restart only Gateway and LangGraph after a configuration
rollback.

## Prohibited changes

Do not add Presidio/spaCy to the in-process plugins, tune rules against frozen
data, read evaluation labels, hard-code sample IDs, let `scene_hint` choose a
scene, couple detector findings to policy actions, modify unrelated type 8/9/10
assets, or expose risky output and rely on frontend CSS to hide it.
