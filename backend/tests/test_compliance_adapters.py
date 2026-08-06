"""Adapters: three ways in, one behaviour out — plus fault isolation.

The promise in plan §4.4 is that a detector author picks a technology stack and
the engine cannot tell the difference. These tests hold all three adapters to the
same behavioural contract, and check that an out-of-process detector crashing,
hanging or lying does not corrupt the engine.
"""

from __future__ import annotations

import json
import sys
import textwrap
from pathlib import Path

import pytest

from deerflow.compliance.adapters import ADAPTER_NAMES, build_adapter
from deerflow.compliance.adapters.inprocess import InProcessAdapter
from deerflow.compliance.adapters.serde import (
    ctx_from_json,
    ctx_to_json,
    hit_to_json,
    hits_from_json,
    request_to_json,
    unit_from_json,
    unit_to_json,
)
from deerflow.compliance.adapters.subprocess_cli import SubprocessCliAdapter
from deerflow.compliance.contract import (
    ContractError,
    DetectContext,
    DetectionHit,
    DetectionUnit,
    FieldItem,
    RiskLocation,
    TextItem,
)

UNIT = DetectionUnit(
    unit_id="u-1",
    gate="OutputGate",
    data_type="surveillance",
    text_items=(TextItem(item_id="t-1", text="camera C-1 at 31.23,121.47", source="output"),),
    field_items=(FieldItem(item_id="f-1", path="metadata.camera_id", value="C-1", source="output"),),
    raw={"kind": "llm_output"},
)
CTX = DetectContext(gate="OutputGate", budget_ms=400)

EXPECTED_HIT = DetectionHit(
    detector_id="demo",
    violation_type="video_meta_leak",
    confidence=0.87,
    severity="high",
    risk_locations=(RiskLocation(kind="field_path", locator="metadata.camera_id", text="C-1", entity_type="camera"),),
    reason_code="demo_rule",
    evidence={"note": "demo"},
    basis=("clause-1",),
)


# ── serde round trips ───────────────────────────────────────────────────────


def test_unit_round_trips_through_json() -> None:
    restored = unit_from_json(unit_to_json(UNIT))
    assert restored.unit_id == UNIT.unit_id
    assert restored.gate == UNIT.gate
    assert restored.data_type == UNIT.data_type
    assert restored.text_items[0].text == UNIT.text_items[0].text
    assert restored.field_items[0].path == UNIT.field_items[0].path


def test_ctx_round_trips_through_json() -> None:
    restored = ctx_from_json(ctx_to_json(CTX))
    assert restored.gate == CTX.gate
    assert restored.budget_ms == CTX.budget_ms
    assert restored.scenes == ()


def test_hit_round_trips_through_json() -> None:
    restored = hits_from_json({"hits": [hit_to_json(EXPECTED_HIT)]})[0]
    assert restored == EXPECTED_HIT


def test_request_payload_is_json_serializable() -> None:
    payload = request_to_json(UNIT, CTX, {"threshold": 0.5})
    assert json.loads(json.dumps(payload, ensure_ascii=False))["params"] == {"threshold": 0.5}


def test_bare_array_response_is_accepted() -> None:
    """Trivial CLI detectors should not have to wrap their output."""
    assert hits_from_json([hit_to_json(EXPECTED_HIT)])[0] == EXPECTED_HIT


# ── untrusted input is validated ────────────────────────────────────────────


@pytest.mark.parametrize(
    "payload",
    [
        {"hits": [{"detector_id": "x", "violation_type": "telepathy", "confidence": 0.5, "severity": "high"}]},
        {"hits": [{"detector_id": "x", "violation_type": "domain", "confidence": 7, "severity": "high"}]},
        {"hits": [{"detector_id": "x", "violation_type": "domain", "confidence": 0.5, "severity": "apocalyptic"}]},
        {"hits": [{"violation_type": "domain", "confidence": 0.5, "severity": "high"}]},
        {"nope": []},
        "not an object",
    ],
    ids=["bad_type", "bad_confidence", "bad_severity", "missing_field", "no_hits_key", "not_json_object"],
)
def test_malformed_detector_output_is_rejected(payload: object) -> None:
    """A detector across a pipe is untrusted input, not a trusted callee.

    An unvalidated `violation_type` reaching the policy matrix would be a
    security problem, not a typo.
    """
    with pytest.raises(ContractError):
        hits_from_json(payload)


def test_malformed_risk_location_is_rejected() -> None:
    with pytest.raises(ContractError):
        hits_from_json({"hits": [{**hit_to_json(EXPECTED_HIT), "risk_locations": [{"kind": "telepathy", "locator": "x"}]}]})


# ── in-process adapter ──────────────────────────────────────────────────────


class _StubDetector:
    detector_id = "demo"

    def __init__(self) -> None:
        self.setup_calls = 0
        self.params: dict | None = None

    def setup(self, params) -> None:  # noqa: ANN001
        self.setup_calls += 1
        self.params = dict(params)

    def detect(self, unit, ctx):  # noqa: ANN001
        return [EXPECTED_HIT]


class _ExplodingDetector:
    detector_id = "boom"

    def setup(self, params) -> None:  # noqa: ANN001
        return None

    def detect(self, unit, ctx):  # noqa: ANN001
        raise RuntimeError("detector exploded")


class _LyingDetector:
    detector_id = "liar"

    def setup(self, params) -> None:  # noqa: ANN001
        return None

    def detect(self, unit, ctx):  # noqa: ANN001
        return [DetectionHit(detector_id="liar", violation_type="domain", confidence=42.0, severity="high")]


def test_inprocess_adapter_resolves_and_calls(monkeypatch) -> None:
    module = sys.modules[__name__]
    adapter = InProcessAdapter(detector_id="demo", entry=f"{module.__name__}:_StubDetector", params={"threshold": 0.5})
    hits = adapter.detect(UNIT, CTX)
    assert list(hits) == [EXPECTED_HIT]


def test_inprocess_adapter_passes_params_to_setup() -> None:
    adapter = InProcessAdapter(detector_id="demo", entry=f"{__name__}:_StubDetector", params={"threshold": 0.9})
    adapter.setup()
    assert adapter._impl.params == {"threshold": 0.9}  # noqa: SLF001 - asserting the wiring


def test_inprocess_adapter_sets_up_once() -> None:
    """Model loading must not sit on the request path."""
    adapter = InProcessAdapter(detector_id="demo", entry=f"{__name__}:_StubDetector", params={})
    adapter.detect(UNIT, CTX)
    adapter.detect(UNIT, CTX)
    assert adapter._impl.setup_calls == 1  # noqa: SLF001


def test_inprocess_adapter_validates_results_too() -> None:
    """An in-process bug must not reach the matrix either."""
    adapter = InProcessAdapter(detector_id="liar", entry=f"{__name__}:_LyingDetector", params={})
    with pytest.raises(ContractError):
        adapter.detect(UNIT, CTX)


def test_inprocess_adapter_propagates_detector_exceptions() -> None:
    """The adapter does not swallow; the engine owns the fault boundary."""
    adapter = InProcessAdapter(detector_id="boom", entry=f"{__name__}:_ExplodingDetector", params={})
    with pytest.raises(RuntimeError, match="detector exploded"):
        adapter.detect(UNIT, CTX)


def test_unresolvable_entry_raises_import_error() -> None:
    adapter = InProcessAdapter(detector_id="ghost", entry="deerflow.nope.missing:Ghost", params={})
    with pytest.raises(ImportError):
        adapter.setup()


def test_malformed_entry_string_raises_import_error() -> None:
    adapter = InProcessAdapter(detector_id="ghost", entry="no_colon_here", params={})
    with pytest.raises(ImportError):
        adapter.setup()


# ── subprocess adapter ──────────────────────────────────────────────────────


def _write_cli(tmp_path: Path, body: str) -> list[str]:
    script = tmp_path / "detector_cli.py"
    script.write_text(textwrap.dedent(body), encoding="utf-8")
    return [sys.executable, str(script)]


def test_subprocess_adapter_matches_inprocess_behaviour(tmp_path: Path) -> None:
    """Behavioural parity is the whole point of the adapter layer."""
    command = _write_cli(
        tmp_path,
        f"""
        import json, sys
        request = json.load(sys.stdin)
        assert request["unit"]["unit_id"] == "u-1"
        assert request["ctx"]["gate"] == "OutputGate"
        print(json.dumps({{"hits": [{json.dumps(hit_to_json(EXPECTED_HIT))}]}}))
        """,
    )
    adapter = SubprocessCliAdapter(detector_id="demo", command=command, params={}, timeout_ms=15000)
    assert list(adapter.detect(UNIT, CTX)) == [EXPECTED_HIT]


def test_subprocess_crash_is_isolated_and_reported(tmp_path: Path) -> None:
    command = _write_cli(tmp_path, "import sys; sys.stderr.write('kaboom'); sys.exit(3)")
    adapter = SubprocessCliAdapter(detector_id="crasher", command=command, params={}, timeout_ms=15000)
    with pytest.raises(ContractError, match="exited with 3"):
        adapter.detect(UNIT, CTX)


def test_subprocess_hang_hits_the_timeout(tmp_path: Path) -> None:
    command = _write_cli(tmp_path, "import time; time.sleep(30)")
    adapter = SubprocessCliAdapter(detector_id="hanger", command=command, params={}, timeout_ms=300)
    with pytest.raises(TimeoutError):
        adapter.detect(UNIT, CTX)


def test_subprocess_garbage_output_is_rejected(tmp_path: Path) -> None:
    command = _write_cli(tmp_path, "print('this is not json')")
    adapter = SubprocessCliAdapter(detector_id="babbler", command=command, params={}, timeout_ms=15000)
    with pytest.raises(ContractError, match="invalid JSON"):
        adapter.detect(UNIT, CTX)


def test_subprocess_empty_output_means_no_hits(tmp_path: Path) -> None:
    command = _write_cli(tmp_path, "pass")
    adapter = SubprocessCliAdapter(detector_id="quiet", command=command, params={}, timeout_ms=15000)
    assert adapter.detect(UNIT, CTX) == ()


def test_subprocess_missing_binary_is_reported(tmp_path: Path) -> None:
    adapter = SubprocessCliAdapter(detector_id="ghost", command=[str(tmp_path / "nope")], params={}, timeout_ms=1000)
    with pytest.raises(ContractError, match="could not be launched"):
        adapter.detect(UNIT, CTX)


def test_subprocess_adapter_requires_a_command() -> None:
    with pytest.raises(ValueError, match="`command` is required"):
        SubprocessCliAdapter(detector_id="x", command=[], params={})


def test_subprocess_receives_params(tmp_path: Path) -> None:
    command = _write_cli(
        tmp_path,
        """
        import json, sys
        request = json.load(sys.stdin)
        assert request["params"]["threshold"] == 0.75, request["params"]
        print("[]")
        """,
    )
    adapter = SubprocessCliAdapter(detector_id="demo", command=command, params={"threshold": 0.75}, timeout_ms=15000)
    assert adapter.detect(UNIT, CTX) == ()


# ── factory ─────────────────────────────────────────────────────────────────


def test_build_adapter_supports_all_three_names() -> None:
    assert set(ADAPTER_NAMES) == {"inprocess", "subprocess_cli", "http_service"}


def test_build_adapter_rejects_unknown_adapter() -> None:
    with pytest.raises(ValueError, match="unknown adapter"):
        build_adapter(adapter="carrier_pigeon", detector_id="x", manifest={}, params={})


def test_build_adapter_requires_endpoint_for_http() -> None:
    with pytest.raises(ValueError, match="`endpoint` is required"):
        build_adapter(adapter="http_service", detector_id="x", manifest={}, params={})


def test_build_adapter_wires_subprocess_timeout_from_params() -> None:
    adapter = build_adapter(
        adapter="subprocess_cli",
        detector_id="x",
        manifest={"command": ["true"]},
        params={"timeout_ms": 1234},
    )
    assert adapter._timeout_s == pytest.approx(1.234)  # noqa: SLF001


def test_every_adapter_exposes_the_same_interface() -> None:
    """setup / detect / close — the engine knows nothing else."""
    adapters = [
        build_adapter(adapter="inprocess", detector_id="a", manifest={"entry": f"{__name__}:_StubDetector"}, params={}),
        build_adapter(adapter="subprocess_cli", detector_id="b", manifest={"command": ["true"]}, params={}),
        build_adapter(adapter="http_service", detector_id="c", manifest={"endpoint": "http://localhost:1/detect"}, params={}),
    ]
    for adapter in adapters:
        for method in ("setup", "detect", "close"):
            assert callable(getattr(adapter, method)), f"{adapter} is missing {method}()"
