# Type 4/5 V4 rule detector

This in-process detector integrates the validated local V4 rules for:

- `illegal_content` (type 4)
- `political` (type 5)

`legacy_core.py` is the maintained V4 rule core. It includes the bounded
lottery-prediction and OCR review-context fixes described below. Its current
SHA-256 is:

```text
e21ef2164a60efb8d06f33aa3f2918bbb65d6e3c41ab811e07c84e4038f6e8c6
```

The adapter only reads text already present in `DetectionUnit`. It does not
parse image/video pixels and does not call an LLM, network API or external
service. Policy actions remain owned by `config/compliance/policy_matrix.yaml`.

The frozen 80-row regression lives under
`datasets/compliance/type45_frozen_test/`. Run it through the real Registry,
Router, Engine and Policy Matrix with:

```bash
make compliance-eval-type45 PYTHON=python3.12
```

The accepted V4 baseline is 76/80 correct (overall accuracy 0.95). The adapter
also narrows surveillance-export rules to their trained video-monitoring data
domain so they do not override dedicated geo-location or re-identification
findings in generic user/model text.
