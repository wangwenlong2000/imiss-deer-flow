"""Asset reinstalls preserve the reviewed runtime fix and upstream hashes."""

import hashlib
import importlib.util
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
VENDOR = ROOT / "backend/packages/harness/deerflow/compliance/detectors/model_tfidf_knn/vendor/model_detectors"


@pytest.fixture
def installer(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("compliance_assets_installer", ROOT / "scripts/prepare_compliance_assets.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(module, "VENDOR_DEST", tmp_path / "installed/vendor/model_detectors")
    source = tmp_path / "package" / module.VENDOR_SOURCE
    source.mkdir(parents=True)
    for name in module.VENDOR_FILES:
        content = (VENDOR / name).read_bytes()
        if name == "feature_extractor.py":
            assert content.count(module.ROUTE_ARROW_PATCHED) == 1
            content = content.replace(module.ROUTE_ARROW_PATCHED, module.ROUTE_ARROW_ORIGINAL, 1)
            assert hashlib.sha256(content).hexdigest() == module.EXTRACTOR_UPSTREAM_SHA256
        (source / name).write_bytes(content)
    return module, tmp_path / "package", source


def test_reinstall_preserves_patch_bytes_and_upstream_checksum(installer):
    module, package, _ = installer
    module.copy_vendor(package)
    extractor = module.VENDOR_DEST / "feature_extractor.py"
    checksums = module.VENDOR_DEST.parent / "SHA256SUMS"
    assert extractor.read_bytes() == (VENDOR / "feature_extractor.py").read_bytes()
    assert hashlib.sha256(extractor.read_bytes()).hexdigest() == module.EXTRACTOR_PATCHED_SHA256
    assert f"{module.EXTRACTOR_UPSTREAM_SHA256}  feature_extractor.py" in checksums.read_text()
    assert module.EXTRACTOR_PATCHED_SHA256 not in checksums.read_text()
    first = {path.name: path.read_bytes() for path in module.VENDOR_DEST.iterdir()}
    first_checksums = checksums.read_bytes()
    module.copy_vendor(package)
    assert {path.name: path.read_bytes() for path in module.VENDOR_DEST.iterdir()} == first
    assert checksums.read_bytes() == first_checksums


def test_normal_cli_install_verifies_package_and_keeps_patch(installer, monkeypatch):
    module, package, source = installer
    model = source / "models" / module.PRODUCTION_MODEL
    model.parent.mkdir()
    model.write_text('{"synthetic_installer_fixture": true}\n')
    dataset = package / module.DATASET_SOURCE / "sample.jsonl"
    dataset.parent.mkdir(parents=True)
    dataset.write_text('{"synthetic_installer_fixture": true}\n')
    sums = [f"{module.sha256_file(path)}  {path.relative_to(package)}" for path in sorted(package.rglob("*")) if path.is_file()]
    (package / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    archive = package.parent / "synthetic-delivery.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for path in package.rglob("*"):
            if path.is_file():
                bundle.write(path, path.relative_to(package))
    monkeypatch.setattr(module, "MODELS_DEST", package.parent / "installed/models")
    monkeypatch.setattr(module, "DATASET_DEST", package.parent / "installed/dataset")
    assert module.main(["--zip", str(archive)]) == 0
    assert (module.VENDOR_DEST / "feature_extractor.py").read_bytes() == (VENDOR / "feature_extractor.py").read_bytes()
    assert (module.MODELS_DEST / model.name).read_bytes() == model.read_bytes()
    assert (module.DATASET_DEST / dataset.name).read_bytes() == dataset.read_bytes()


@pytest.mark.parametrize("change", ["upstream_drift", "missing_pattern", "duplicate_pattern"])
def test_upstream_changes_require_review_before_installing(installer, change):
    module, package, source = installer
    module.copy_vendor(package)
    previous = (module.VENDOR_DEST / "feature_extractor.py").read_bytes()
    previous_checksums = (module.VENDOR_DEST.parent / "SHA256SUMS").read_bytes()
    path = source / "feature_extractor.py"
    content = path.read_bytes()
    if change == "upstream_drift":
        content += b"\n# New upstream revision\n"
    elif change == "missing_pattern":
        content = content.replace(module.ROUTE_ARROW_ORIGINAL, b"")
    else:
        content += module.ROUTE_ARROW_ORIGINAL
    path.write_bytes(content)
    with pytest.raises(SystemExit, match="review"):
        module.copy_vendor(package)
    assert (module.VENDOR_DEST / "feature_extractor.py").read_bytes() == previous
    assert (module.VENDOR_DEST.parent / "SHA256SUMS").read_bytes() == previous_checksums
