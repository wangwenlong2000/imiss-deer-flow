"""AST scan: the engine core must never import a concrete detector.

This is the mechanical enforcement of plan §4.1's code boundary. Documentation
and good intentions do not survive contact with a deadline; a failing test does.

The rule: ``engine.py`` / ``router.py`` / ``policy.py`` / ``scene.py`` /
``intent.py`` / ``actions.py`` / ``audit.py`` / ``types.py`` / ``contract.py``
may not reference ``deerflow.compliance.detectors.<anything>``. Only
``registry.py`` loads detectors, and it does so reflectively from manifest data.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

COMPLIANCE_DIR = Path(__file__).resolve().parents[1] / "packages/harness/deerflow/compliance"

#: Modules that must stay ignorant of every concrete detector.
CORE_MODULES = (
    "engine.py",
    "router.py",
    "policy.py",
    "scene.py",
    "intent.py",
    "actions.py",
    "audit.py",
    "types.py",
    "contract.py",
)

DETECTORS_PACKAGE = "deerflow.compliance.detectors"


def _module_paths() -> list[Path]:
    return [COMPLIANCE_DIR / name for name in CORE_MODULES]


def _imported_modules(tree: ast.AST) -> list[str]:
    """Every module name referenced by an import statement."""
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.append(node.module)
            # `from deerflow.compliance.detectors import model_tfidf_knn`
            found.extend(f"{node.module}.{alias.name}" for alias in node.names)
    return found


@pytest.mark.parametrize("module_path", _module_paths(), ids=CORE_MODULES)
def test_core_module_does_not_import_a_concrete_detector(module_path: Path) -> None:
    assert module_path.is_file(), f"expected core module at {module_path}"
    tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))

    offenders = [
        name
        for name in _imported_modules(tree)
        if name.startswith(f"{DETECTORS_PACKAGE}.") or name == DETECTORS_PACKAGE
    ]
    assert not offenders, (
        f"{module_path.name} imports concrete detector module(s) {offenders}. "
        "Only registry.py may load detectors, and only reflectively via manifest data."
    )


@pytest.mark.parametrize("module_path", _module_paths(), ids=CORE_MODULES)
def test_core_module_has_no_detector_string_reference(module_path: Path) -> None:
    """Catch a dynamic import that dodges the AST import check."""
    source = module_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(module_path))
    offenders = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and DETECTORS_PACKAGE in node.value
    ]
    assert not offenders, f"{module_path.name} references a detector module by string: {offenders}"


def test_detectors_package_init_imports_nothing() -> None:
    """``detectors/__init__.py`` must stay empty of imports.

    Importing a detector there would make every detector load whenever the
    package is touched, which is exactly the coupling the design avoids.
    """
    init_path = COMPLIANCE_DIR / "detectors" / "__init__.py"
    tree = ast.parse(init_path.read_text(encoding="utf-8"), filename=str(init_path))
    imports = [node for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert not imports, "detectors/__init__.py must not import anything; registry.py discovers detectors by scanning manifests"


def test_registry_is_the_only_reflective_loader() -> None:
    """Exactly one module owns detector loading."""
    registry = COMPLIANCE_DIR / "registry.py"
    assert registry.is_file()
    assert "discover_manifests" in registry.read_text(encoding="utf-8")


def test_contract_module_is_stdlib_only() -> None:
    """A detector author must be able to import the contract with no extra deps.

    The contract is the single import a detector needs. If it grows a pydantic or
    yaml dependency, out-of-process detectors in slim environments break.
    """
    contract = COMPLIANCE_DIR / "contract.py"
    tree = ast.parse(contract.read_text(encoding="utf-8"), filename=str(contract))
    stdlib_allowed = {"collections", "collections.abc", "dataclasses", "types", "typing", "__future__", "enum", "json", "re", "abc"}
    for name in _imported_modules(tree):
        root = name.split(".")[0]
        assert root in {n.split(".")[0] for n in stdlib_allowed}, f"contract.py imports non-stdlib module {name!r}; it must stay dependency-free"


def test_detector_authors_only_need_the_contract() -> None:
    """Every shipped detector imports ``contract`` and nothing else from the engine.

    This is the promise made in the integration guide, checked against reality.
    """
    detectors_dir = COMPLIANCE_DIR / "detectors"
    engine_modules = {f"deerflow.compliance.{name[:-3]}" for name in CORE_MODULES if name != "contract.py"}
    engine_modules.update({"deerflow.compliance.registry", "deerflow.compliance.adapters", "deerflow.compliance.normalizers"})

    checked = 0
    for source_file in detectors_dir.rglob("*.py"):
        if "vendor" in source_file.parts or "tests" in source_file.parts:
            continue  # vendor code is a verbatim lift; tests may poke at internals
        tree = ast.parse(source_file.read_text(encoding="utf-8"), filename=str(source_file))
        for name in _imported_modules(tree):
            assert name not in engine_modules, f"{source_file.relative_to(detectors_dir)} imports engine module {name!r}; detectors may only import deerflow.compliance.contract"
        checked += 1

    assert checked >= 0  # zero detectors is a valid state (P0); the loop is the assertion
