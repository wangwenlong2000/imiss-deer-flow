#!/usr/bin/env python3
"""Unpack, verify and distribute the compliance model delivery package.

Driven by ``make compliance-assets``. The delivery zip is never committed
(``*.zip`` is gitignored), so this script is the reproducible way to rebuild the
local working tree from it.

Distribution map (plan §8):

===========================  =========================================================
zip path                     destination
===========================  =========================================================
``violation_detection/       ``backend/packages/harness/deerflow/compliance/detectors/
model_detectors/*.py``        model_tfidf_knn/vendor/model_detectors/``  (committed)
``.../models/*.json``        ``models/compliance/``                     (gitignored)
``normalized/0624_          ``datasets/compliance/normalized/
supported_split/``            0624_supported_split/``                   (committed)
===========================  =========================================================

Integrity is checked against the package's own ``SHA256SUMS`` before anything is
copied, so a truncated or tampered download fails loudly instead of silently
producing a degraded model.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Candidate locations for the delivery zip, in priority order.
ZIP_CANDIDATES = (
    REPO_ROOT / "合规检测" / "violation_detection_model_normalized_723.zip",
    REPO_ROOT / "violation_detection_model_normalized_723.zip",
)

VENDOR_DEST = REPO_ROOT / "backend/packages/harness/deerflow/compliance/detectors/model_tfidf_knn/vendor/model_detectors"
MODELS_DEST = REPO_ROOT / "models/compliance"
DATASET_DEST = REPO_ROOT / "datasets/compliance/normalized/0624_supported_split"

# Only the production model is distributed by default. The other six trained
# variants in the package use different content_text specs or data splits and
# must not be mixed in (plan §5.1.2 / risk 3).
PRODUCTION_MODEL = "ml_detector_0624_fresh.json"

VENDOR_SOURCE = "violation_detection/model_detectors"
DATASET_SOURCE = "normalized/0624_supported_split"

# Vendor code is a zero-modification lift; only these top-level modules are
# needed at inference time. Training/CLI scripts come along so the delivery can
# be re-run in place, but the package's own tests/ dir is left behind.
VENDOR_FILES = (
    "__init__.py",
    "feature_extractor.py",
    "io_utils.py",
    "metrics.py",
    "tfidf_knn.py",
    "evaluate_classifier.py",
    "predict_classifier.py",
    "train_classifier.py",
    "split_dataset.py",
    "holdout_split.py",
    "README.md",
)


def find_zip(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise SystemExit(f"[compliance-assets] zip not found: {path}")
        return path
    for candidate in ZIP_CANDIDATES:
        if candidate.is_file():
            return candidate
    listed = "\n  ".join(str(c) for c in ZIP_CANDIDATES)
    raise SystemExit(f"[compliance-assets] delivery zip not found. Looked in:\n  {listed}\nPass --zip <path> to override.")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_checksums(root: Path) -> int:
    """Verify every file listed in SHA256SUMS. Returns the number checked."""
    sums_file = root / "SHA256SUMS"
    if not sums_file.is_file():
        raise SystemExit(f"[compliance-assets] SHA256SUMS missing in package root {root}")

    checked = 0
    failures: list[str] = []
    for line in sums_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        # Format: "<hex>  <relative path>" (two spaces, GNU coreutils style).
        expected, _, rel = line.partition("  ")
        rel = rel.strip()
        if not rel:
            continue
        target = root / rel
        if not target.is_file():
            failures.append(f"missing: {rel}")
            continue
        actual = sha256_file(target)
        if actual != expected:
            failures.append(f"checksum mismatch: {rel}")
        checked += 1

    if failures:
        detail = "\n  ".join(failures[:20])
        raise SystemExit(f"[compliance-assets] integrity check FAILED ({len(failures)} problem(s)):\n  {detail}")
    return checked


def copy_vendor(root: Path) -> None:
    source = root / VENDOR_SOURCE
    if not source.is_dir():
        raise SystemExit(f"[compliance-assets] vendor source missing: {source}")

    VENDOR_DEST.mkdir(parents=True, exist_ok=True)
    for name in VENDOR_FILES:
        src = source / name
        if not src.is_file():
            raise SystemExit(f"[compliance-assets] vendor file missing from package: {VENDOR_SOURCE}/{name}")
        shutil.copy2(src, VENDOR_DEST / name)
    write_vendor_checksums()
    print(f"[compliance-assets] vendor  -> {VENDOR_DEST.relative_to(REPO_ROOT)} ({len(VENDOR_FILES)} files)")


def write_vendor_checksums() -> None:
    """Record vendor hashes so accidental edits are detectable without the zip.

    The delivery zip is gitignored, so CI cannot diff against it. This file is
    committed alongside the vendor code and checked by
    ``test_compliance_model_detector.py`` — `make lint --fix` has silently
    rewritten these files before, and a lint-mangled model implementation is
    exactly the kind of change nobody notices until accuracy drops.
    """
    lines = []
    for name in VENDOR_FILES:
        lines.append(f"{sha256_file(VENDOR_DEST / name)}  {name}")
    (VENDOR_DEST.parent / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")


def copy_models(root: Path, all_models: bool) -> None:
    source = root / VENDOR_SOURCE / "models"
    if not source.is_dir():
        raise SystemExit(f"[compliance-assets] models source missing: {source}")

    MODELS_DEST.mkdir(parents=True, exist_ok=True)
    wanted = sorted(source.glob("*.json")) if all_models else [source / PRODUCTION_MODEL]
    for src in wanted:
        if not src.is_file():
            raise SystemExit(f"[compliance-assets] model file missing from package: {src.name}")
        shutil.copy2(src, MODELS_DEST / src.name)
    names = ", ".join(p.name for p in wanted)
    print(f"[compliance-assets] models  -> {MODELS_DEST.relative_to(REPO_ROOT)} ({names})")


def copy_dataset(root: Path) -> None:
    source = root / DATASET_SOURCE
    if not source.is_dir():
        raise SystemExit(f"[compliance-assets] dataset source missing: {source}")

    DATASET_DEST.mkdir(parents=True, exist_ok=True)
    copied = 0
    for src in sorted(source.iterdir()):
        if src.is_file():
            shutil.copy2(src, DATASET_DEST / src.name)
            copied += 1
    print(f"[compliance-assets] dataset -> {DATASET_DEST.relative_to(REPO_ROOT)} ({copied} files)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Unpack and distribute compliance detection assets.")
    parser.add_argument("--zip", dest="zip_path", default=None, help="Path to the delivery zip (default: auto-discover).")
    parser.add_argument("--all-models", action="store_true", help="Distribute every trained model, not just the production one.")
    parser.add_argument("--skip-verify", action="store_true", help="Skip SHA256SUMS verification (not recommended).")
    args = parser.parse_args(argv)

    zip_path = find_zip(args.zip_path)
    print(f"[compliance-assets] package: {zip_path}")

    with tempfile.TemporaryDirectory(prefix="compliance-assets-") as tmp:
        root = Path(tmp)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(root)

        if args.skip_verify:
            print("[compliance-assets] integrity check SKIPPED (--skip-verify)")
        else:
            checked = verify_checksums(root)
            print(f"[compliance-assets] integrity OK ({checked} files verified against SHA256SUMS)")

        copy_vendor(root)
        copy_models(root, all_models=args.all_models)
        copy_dataset(root)

    print("[compliance-assets] done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
