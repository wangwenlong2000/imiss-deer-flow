# DeerFlow compliance detector onboarding

Type 1 `regex_struct_id` and type 2 `regex_geo_loc` are the reference
implementations. A detector identifies risk; SceneResolver establishes usage
context; Policy Matrix decides disposition. Do not combine those responsibilities.

## 1. Copy the inert template

```text
backend/packages/harness/deerflow/compliance/detectors/_template_detector/
├── __init__.py
├── detector.py.example
├── manifest.yaml.example
└── README.md

backend/tests/test_compliance_new_detector_template.py.example
```

Copy to `detectors/<detector_id>/`, remove the `.example` suffixes, and replace
all placeholders. The source template has no `manifest.yaml`, so Registry cannot
register it; the test template does not end in `.py`, so pytest cannot collect
it.

## 2. Manifest

The manifest is the only registration declaration. Keep:

- `contract_version: "1.0"`;
- a globally unique `detector_id`;
- `adapter` and importable `entry`;
- owned violation types;
- each violation type's declared InputGate/ContextGate/OutputGate list;
- optional data-type routing, a truthful light/medium/heavy cost hint, bounded
  description, and dependency-free defaults.

Never put policy actions in a manifest. Registry enablement belongs in
`config/compliance/detectors.yaml`.

## 3. Detector interface

Implement:

```python
class Detector:
    detector_id = "..."

    def setup(self, params: Mapping[str, Any]) -> None: ...
    def detect(self, unit: DetectionUnit, ctx: DetectContext) -> list[DetectionHit]: ...
```

`setup` must be repeatable. Ordinary malformed units should return no hit, not
crash the engine. Do not import engine, registry, router, policy, agent state, or
another owner's detector.

## 4. DetectionUnit input

Inspect only the frozen boundary object:

- `unit_id`, `gate`, and optional `data_type`;
- free text in `text_items`;
- structured dotted paths in `field_items`;
- read-only `raw` provenance.

Never read `is_positive`, expected actions, sample IDs, dataset split names, or
other evaluation labels. Never branch on a frozen-set identifier.

## 5. DetectionHit output

Every hit must contain the detector ID, owned violation type, calibrated
confidence, severity, stable reason code, policy/legal basis, and useful
evidence metadata. Findings from multiple detectors are additive; do not erase
or weaken another type.

## 6. Risk locations

Return at least one precise `RiskLocation` whenever possible:

- `char_span` with `start:end` for free text;
- `field_path` for structured data;
- `coordinate`, `frame`, or `bbox` only when their locator is reproducible.

The location powers masking and audit. Do not copy an entire source document
into `text`; retain only the bounded risky value needed for action execution.

## 7. Three gates

Declare only gates the detector actually supports and test each declaration:

- InputGate: user query/upload, with intent join where applicable;
- ContextGate: normalized tool evidence before it enters the model;
- OutputGate: final answer before authoritative state/presentation.

All gates receive the same resolved scene. A detector must not infer scene from
natural-language claims.

## 8. Dependencies

Prefer standard-library/rule-only implementations. No import-time model load or
network call. Heavy ML/NLP dependencies belong behind `subprocess_cli` or
`http_service`, with explicit asset and timeout handling. Do not add Presidio,
spaCy, or a large model to the in-process harness.

## 9. Registry and strict startup

Add the copied detector ID to `config/compliance/detectors.yaml`, initially
disabled. Run strict Registry construction and import/setup tests. Enable only
after targeted, policy, frozen, and full compliance tests pass. A manifest parse
or entry import error must fail strict startup.

## 10. Policy separation

The detector reports what/where/confidence/basis. It never returns `refuse`,
`desensitize`, or another disposition. Policy Matrix owns
`violation × gate × scene -> actions`; changing a detector must not tune a
business cell.

## 11. Audit

Verify an audited positive includes detector/violation, risk location, actual
matrix actions and basis, intent summary, resolved scene, permission/fallback
reason codes, request/thread reference, and audit reference. Avoid unnecessary
sensitive raw text; use bounded values or hashes.

## 12. Frozen regression

Create a held-out, immutable set with positives, negatives, and near negatives.
Record SHA-256 before integration. Compare Engine predictions with the approved
standalone baseline row-by-row and report precision, recall, F1, and accuracy.
Never edit or tune rules against the frozen set after it is frozen.

## 13. Required test template

At minimum cover:

- positive, negative, malformed, and near-negative detector cases;
- manifest/schema and Registry strict mode;
- router gate/data-type selection;
- all declared gates;
- all five scenes plus `_unknown`;
- multi-label coexistence;
- audit and failure isolation;
- deterministic SSE and persisted-state safety;
- frozen-set Engine/baseline parity;
- full `tests/test_compliance_*.py`.

Start from `backend/tests/test_compliance_new_detector_template.py.example`.

## 14. SceneResolver contract

Tests must pass a `compliance_request` with authenticated actor IDs,
server-issued roles/permissions, operation/audience, resource ownership and
classification, target organisation, and anonymization state. `scene_hint` is
not trusted. Missing facts must remain `_unknown`.

## 15. SSE and frontend metadata

Durable response metadata and `compliance_retract` events must preserve:

- gate and final scene;
- all violation badges (never overwrite a sibling type);
- actions, basis, notice, and audit reference;
- strict/compatible streaming mode.

For `cross_org`, `public_release`, and `research_anon`,
the original risky value must never appear in a user-visible SSE event. Final
`messages`, `raw_messages`, checkpoint/history, title input, and refresh state
must contain only policy-safe content.

## 16. Performance

Measure at least 30 repetitions for positive single-type, multi-type, and
negative inputs. Report detector P50/P95/max and strict first-safe-event
P50/P95/max. Stay within the configured gate budget and declare any model
asset/startup cost separately.

## 17. Non-negotiable prohibitions

- Do not read `is_positive`, expected labels/actions, or hidden evaluation data.
- Do not tune against or modify a frozen regression set.
- Do not encode sample IDs or test phrases as exceptions.
- Do not equate an entity hit with a high-risk intent.
- Do not default missing scene/permission information to `self_use`.
- Do not add policy actions to detector code.
- Do not expose raw sensitive content only to hide it later with frontend CSS.

## 18. Common failures

- Wrong entry path or detector ID mismatch between class/manifest/config.
- Missing gate declaration causes Engine to drop a valid hit.
- Vague/no risk location prevents safe masking.
- Import-time optional dependency breaks every agent.
- Unknown intent is coerced to low risk.
- Caller `scene_hint` selects a permissive matrix column.
- Output is rewritten after transcript/title/checkpoint already observed it.
- A combined finding overwrites the previous frontend badge.
- Audit stores entire raw documents or credentials.

## 19. Commands

```bash
cd /app/backend
PYTHONPATH=.:packages/harness uv run pytest tests/test_compliance_<detector>.py -v
PYTHONPATH=.:packages/harness uv run pytest tests/test_compliance_*.py -q
PYTHONPATH=.:packages/harness uv run ruff check <changed-python-files>
```

Run any detector-specific frozen evaluator and frontend compliance test defined
by the project. Do not build sandbox merely to validate an in-process detector.

## 20. Submission checklist

- [ ] Package/class/manifest IDs agree.
- [ ] Setup works with declared lightweight dependencies.
- [ ] Positive/negative/near-negative/malformed cases pass.
- [ ] Risk locations are bounded and actionable.
- [ ] Three-gate declarations and tests agree.
- [ ] All scenes and `_unknown` use matrix actions.
- [ ] Intent/SceneResolver contracts are not bypassed.
- [ ] Audit and SSE metadata are complete.
- [ ] Strict SSE, state, transcript, history, and refresh contain no raw risk.
- [ ] Multi-label badges coexist in frontend parsing.
- [ ] Frozen metrics and Engine/baseline parity are recorded.
- [ ] P50/P95/max are recorded.
- [ ] Registry strict mode and all compliance tests pass.
- [ ] No frozen data, unrelated core, ports, containers, or heavy dependencies
      changed.
