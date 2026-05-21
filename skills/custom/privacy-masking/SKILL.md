---
name: privacy-masking
description: Mask sensitive visual data in video evidence such as faces, license plates, phone numbers, and doorplates. Use when Codex has an evidence image URI, sensitive_regions, or privacy_masking config and needs a masked URI before exposing snapshots or clips externally. This skill exposes OpenCV image read/write and Gaussian blur masking for real files.
---

# Privacy Masking

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If an evidence image URI is missing but the user provided an event or video analysis request, do not write custom masking code. First call `evidence-snapshot/scripts/run.py` to create snapshot evidence, then call this skill on the resulting evidence URI when masking is required.

## Workflow

1. Read evidence URI and configured mask types.
2. Locate or accept sensitive regions.
3. Apply configured blur, mosaic, or solid mask.
4. Return masked URI, mask status, regions, and method.

## Available Implementation

- Registry name: `privacy-masking`
- Python class: `src.skills.evidence_processing.privacy_masking.PrivacyMaskingSkill`
- Real image backend: OpenCV `cv2.imread`, `cv2.GaussianBlur`, `cv2.imwrite`
- Called by: `evidence-snapshot`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python privacy-masking/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `privacy-masking`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("privacy-masking").run({"uri": evidence_uri, "sensitive_regions": regions}, context)
```

Configuration:

```yaml
privacy_masking:
  enabled: true
  method: gaussian_blur
```

## Inputs

- `uri` or `image_uri`
- Optional `sensitive_regions`: list of bboxes or region dicts
- Optional `output_key` for memory storage mode

## Outputs

Returns `uri`, `privacy_masked`, `masked_regions`, and `method`.

## Failure Modes

- `MISSING_URI`
- `IMAGE_READ_FAILED`

## Constraints

Do not delete original evidence needed for traceability.
