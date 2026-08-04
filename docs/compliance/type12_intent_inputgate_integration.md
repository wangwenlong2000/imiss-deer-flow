# Type 1/2 intent-to-InputGate integration

## Scope

This change connects the intent produced for a real `lead_agent` turn to the
compliance context consumed by `InputGate`. It does not change the
`regex_struct_id` or `regex_geo_loc` rules, the policy matrix, or the type
8/9/10 detectors.

## Field flow before the change

```text
HTTP/SSE message
  -> IntentRecognitionMiddleware
     -> RoutingIntentResult
        intent=task|chitchat|...
        confidence/reason/scenes
        (no compliance operation or risk fields)
  -> ThreadState.intent_context
     -> ComplianceInputGateMiddleware._intent_from_state
        bool(high_risk || is_high_risk)
        (missing value silently became false)
  -> IntentInfo(high_risk=false)
  -> IntentGuard
     -> non-baseline type 1/2 findings treated as benign
  -> policy/audit/SSE
```

`RoutingIntentResult` describes dialogue routing, not disclosure risk. The
fields `intent_type`, `requested_operation`, `risk_level`, `is_high_risk`, and
`reason_codes` did not exist in its serialized state. InputGate also collapsed
a missing risk signal to `False`. The audit record retained only the routing
intent and the derived boolean. This was the loss point that made real requests
behave differently from tests which manually supplied `high_risk=true`.

The Gateway/LangGraph transport itself preserves arbitrary state dictionaries;
the missing data originated before serialization rather than being removed by
SSE. Types 8/9/10 do not use this InputGate intent join because their current
manifests operate at ContextGate and/or OutputGate.

## Unified result

`intent_context.compliance_intent` is now the canonical compliance intent
summary:

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

Allowed intent types are `analysis`, `desensitize`, `export`, `share`,
`public_release`, `research`, `bypass`, `privacy_extraction`, and `unknown`.
Allowed operations are `analysis`, `upload`, `export`, `share`, `publish`,
`transform`, and `unknown`.

The classifier evaluates the requested purpose and operation. A phone number,
coordinate, identifier keyword, or detector finding is never sufficient by
itself to set a high-risk intent.

## Field flow after the change

```text
HTTP/SSE message
  -> IntentRecognitionMiddleware
     -> routing fields
     -> intent_context.compliance_intent
  -> ThreadState.intent_context
  -> ComplianceInputGateMiddleware._intent_from_state
  -> IntentInfo (complete compliance intent)
  -> ComplianceEngine / IntentGuard
  -> trusted finding set
  -> Policy Matrix
  -> Audit (complete final intent + actual actions)
  -> SSE state update (non-sensitive compliance intent summary)
```

The response exposes reason codes, not private chain-of-thought or model
reasoning.

## Compatibility and fallback

Legacy state with top-level `high_risk`/`is_high_risk` remains accepted. Known
legacy benign intents retain their former behavior. If the new fields are
missing or unknown and type 1/2 detects an entity, the finding is retained,
policy resolves `_unknown`, and diagnostics explicitly request conservative
handling/manual review. Missing fields never silently become a verified
low-risk intent.

`IntentInfo` and detector-context JSON serialization use default values, so old
callers and subprocess detector adapters continue to deserialize.

## Audit

Each audited decision now records:

- routing intent;
- `intent_type` and `requested_operation`;
- `risk_level` and `is_high_risk`;
- confidence, reason codes, bounded reason, and source;
- the policy actions and audit reference already present in the decision.

Audits are written only for detected decisions. A benign transformation can
therefore leave no violation audit entry; its recognized intent remains in the
thread/SSE state.

## Verification

Function and integration cases cover:

- compliant masking for internal analysis: `desensitize`, low risk, no
  high-risk refusal;
- public identifier extraction: `public_release`, publish, high risk,
  `struct_id` retained;
- public exact trajectory export: `public_release`, publish, high risk,
  `geo_loc` retained;
- knowledge consultation: `analysis`, low risk;
- bypass request: `bypass`, high risk;
- legacy missing fields: `_unknown` plus conservative manual review.

The real LangGraph verification created a real thread and called
`POST /threads/{thread_id}/runs/stream` with an interrupt immediately after
`ComplianceInputGateMiddleware.before_agent`. This executes the production
middleware chain without depending on a remote model response. Observed
results:

| Case | Intent | InputGate | Finding/action |
| --- | --- | --- | --- |
| compliant masking | `desensitize`, low | executed | benign entity join discarded |
| public identifier | `public_release`, high | executed | `struct_id`; warn + manual review in `_unknown` |
| public exact trajectory | `public_release`, high | executed | `geo_loc`; warn + manual review in `_unknown` |

Gateway `/health` and direct LangGraph `/info` returned HTTP 200, and
`lead_agent` was returned by the assistants API. The development Nginx
container was intentionally not recreated; direct LangGraph `/info` is the
route that Nginx normally exposes as `/api/langgraph/info`.

Targeted backend result:

```text
153 passed in 2.73s
```

The SSE smoke summary is stored in `task1_intent_sse_redacted.log`. It contains
thread/run references and classification outcomes but no identifier, exact
coordinate, credential, or full response.

## Remaining risk

The operation classifier is deterministic and intentionally narrow. Ambiguous
language resolves to `unknown` instead of being guessed safe. A trusted
identity/resource SceneResolver is still required to replace `_unknown` with a
verified policy scene; that is the next integration task.
