# Type 1/2 + 8/9/10 joint budget verification

The joint probe was run in the Gateway container with both regex detectors and
`model_tfidf_knn` enabled, a 400 ms budget, and a 32-unit limit:

```bash
PYTHONPATH=.:packages/harness uv run python <joint-budget-probe>
```

Observed result:

| Detector | Executed | Time (ms) | Timeout | Budget skipped | Result |
| --- | --- | ---: | --- | --- | --- |
| `model_tfidf_knn` | yes | 1.515 | no | no | `FileNotFoundError` |
| `regex_geo_loc` | yes | 6.637 | no | no | type 2 hit |
| `regex_struct_id` | yes | 4.470 | no | no | type 1 hit |

The complete pass took 12.729 ms, checked 1/1 unit, and was not truncated. The
missing model asset is `/app/models/compliance/ml_detector_0624_fresh.json`.
This is an infrastructure/model-delivery gap; no type 1/2 rule or frozen set
was changed to hide it. Re-run the same probe after C supplies the asset to
verify the model's real findings and end-to-end budget behavior.
