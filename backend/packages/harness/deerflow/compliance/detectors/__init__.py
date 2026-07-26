"""Detector packages.

Intentionally empty: importing a concrete detector here would defeat the
decoupling the whole design rests on. ``registry.py`` discovers detectors by
scanning for ``*/manifest.yaml`` and loads them reflectively.

``test_compliance_decoupling.py`` asserts this file stays import-free.
"""
