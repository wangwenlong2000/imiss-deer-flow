# Type 4/5 V4 rule detector

This in-process detector integrates the validated local V4 rules for:

- `illegal_content` (type 4)
- `political` (type 5)

`legacy_core.py` is a byte-identical copy of
`/home/guowei/wgjc-script/illegal_political_detector_V4.py` at integration time.
Its SHA-256 is:

```text
8e894d050ea789e8a8d6e23ada181f8e93e1d3e3c5ec249907a25fa8331f62e2
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
