# Detector copy template

Copy this directory to a non-underscore package name, rename
`detector.py.example` to `detector.py`, rename `manifest.yaml.example` to
`manifest.yaml`, and replace every `replace_*` placeholder.

This directory is inert by design: Registry discovers only a file named
`manifest.yaml`, which does not exist here. The example detector must not import
the engine, policy, registry, router, agent state, evaluation labels, or frozen
datasets. It receives only `DetectionUnit` and `DetectContext` and returns
`DetectionHit` values with concrete `risk_locations`.

Read `docs/compliance/new_detector_onboarding_guide.md` before enabling a copy.
