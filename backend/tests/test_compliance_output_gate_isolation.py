"""No downstream component may ever observe an unsanitized answer.

This asserts an **observability invariant**, not a list index. The previous test
suite pinned the gate's position in the middleware list and the position was
still wrong: `after_model` runs in reverse order, so the gate ran last and
`RawTranscriptMiddleware` captured the violating text first — permanently, because
`merge_raw_messages` dedups by id keeping the first. A page reload brought the
violation back.

The fix moved the authoritative rewrite into `wrap_model_call`, which composes
first-in-list-outermost and whose result is what langchain turns into
`{"messages": ...}`. So the property to test is not "the gate is at index N" but
"no middleware, at any position, can see the secret."

Test B is the important one: it parametrizes over spy position. A positional fix
passes only some placements; a structural fix passes all of them.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage

from deerflow.agents.middlewares.compliance_output_gate_middleware import ComplianceOutputGateMiddleware
from deerflow.agents.middlewares.raw_transcript_middleware import RawTranscriptMiddleware
from deerflow.compliance.contract import DetectionHit
from deerflow.compliance.normalizers.llm_output import extract_text
from deerflow.compliance.types import ComplianceDecision, Diagnostics
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config

SECRET = "结合 user_id、imei、驻留时间和基站位置可定位到具体个人并还原其身份"


@pytest.fixture(autouse=True)
def _enabled():
    original = get_compliance_config()
    set_compliance_config(ComplianceConfig(enabled=True, audit={"enabled": False}))
    yield
    set_compliance_config(original)


class _StubEngine:
    """Flags anything containing SECRET as re_identify."""

    def __init__(self, actions=("refuse",)) -> None:
        self.actions = tuple(actions)
        self.calls: list[str] = []

    def check(self, units, **kwargs):  # noqa: ANN001, ANN003
        text = " ".join(item.text for unit in units for item in unit.text_items)
        self.calls.append(text)
        if SECRET not in text:
            return ComplianceDecision(request_id="r", gate="OutputGate", scene_key="_unknown")
        return ComplianceDecision(
            request_id="r",
            gate="OutputGate",
            scene_key="_unknown",
            hits=(DetectionHit(detector_id="stub", violation_type="re_identify", confidence=0.99, severity="high"),),
            actions=self.actions,
            per_violation_actions={"re_identify": self.actions},
            basis=("测试依据",),
            diagnostics=Diagnostics(units_total=1, units_checked=1),
        )


class _LeakSpy(AgentMiddleware):
    """Records whatever text it can see at each downstream hook."""

    def __init__(self) -> None:
        super().__init__()
        self.after_model_seen: list[str] = []
        self.after_agent_seen: list[str] = []

    def _last_ai_text(self, state) -> str:  # noqa: ANN001
        for message in reversed((state or {}).get("messages") or []):
            if isinstance(message, AIMessage):
                return extract_text(message.content)
        return ""

    def after_model(self, state, runtime):  # noqa: ANN001, ANN201
        self.after_model_seen.append(self._last_ai_text(state))
        return None

    def after_agent(self, state, runtime):  # noqa: ANN001, ANN201
        self.after_agent_seen.append(self._last_ai_text(state))
        return None


def _fake_model(text: str):  # noqa: ANN201
    """A chat model that always answers *text*, with a stable message id.

    The stable id matters: it is what makes the reducer replace rather than
    append, and what the digest backstop keys on.
    """

    def _messages():
        while True:
            yield AIMessage(content=text, id="m-1")

    return GenericFakeChatModel(messages=_messages())


def _run(middleware: list[AgentMiddleware], text: str = SECRET) -> dict:
    agent = create_agent(model=_fake_model(text), tools=[], middleware=middleware)
    return agent.invoke({"messages": [HumanMessage(content="问题")]})


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=lambda o: getattr(o, "content", str(o)))


# ── A. the core invariant ───────────────────────────────────────────────────


def test_no_downstream_component_sees_the_unsanitized_answer() -> None:
    """Fails on the pre-fix code; the whole point of this file."""
    spy = _LeakSpy()
    result = _run([ComplianceOutputGateMiddleware(engine=_StubEngine()), RawTranscriptMiddleware(), spy])

    assert SECRET not in " ".join(spy.after_model_seen), "a downstream after_model observed the violating text"
    assert SECRET not in " ".join(spy.after_agent_seen), "a downstream after_agent observed the violating text"
    assert SECRET not in _dump(result.get("messages")), "the violating text reached `messages`"
    assert SECRET not in _dump(result.get("raw_messages")), "the violating text reached `raw_messages` (survives a page reload)"


def test_the_sanitized_answer_is_what_everyone_sees() -> None:
    """Not just absence of the secret — the replacement must actually be there."""
    result = _run([ComplianceOutputGateMiddleware(engine=_StubEngine()), RawTranscriptMiddleware()])
    assert "合规" in _dump(result.get("messages"))
    assert "合规" in _dump(result.get("raw_messages"))


# ── B. order independence — this is what makes it a *class* test ────────────


@pytest.mark.parametrize(
    "arrangement",
    ["gate_first", "spy_first", "spy_between"],
)
def test_no_middleware_position_can_defeat_the_gate(arrangement: str) -> None:
    """A positional fix passes some of these; a structural fix passes all.

    `spy_first` is the case a reorder-based fix cannot win: a middleware placed
    ahead of the gate runs its after_model *after* the gate's, but under the old
    design it still saw raw state.
    """
    gate = ComplianceOutputGateMiddleware(engine=_StubEngine())
    spy = _LeakSpy()
    transcript = RawTranscriptMiddleware()

    layouts = {
        "gate_first": [gate, transcript, spy],
        "spy_first": [spy, gate, transcript],
        "spy_between": [gate, spy, transcript],
    }
    result = _run(layouts[arrangement])

    assert SECRET not in " ".join(spy.after_model_seen), f"{arrangement}: spy saw the secret"
    assert SECRET not in _dump(result.get("raw_messages")), f"{arrangement}: raw_messages kept the secret"


# ── C/D. the backstop, and that it does not double-scan ─────────────────────


def test_clean_answer_is_scanned_once_not_twice() -> None:
    """wrap_model_call scans; the after_model backstop must short-circuit.

    Without the digest gate every answer would be detected twice and, once
    auditing is on, audited twice.
    """
    engine = _StubEngine()
    _run([ComplianceOutputGateMiddleware(engine=engine)], text="完全无害的一段回答")
    assert len(engine.calls) == 1, f"expected a single scan, got {len(engine.calls)}"


def test_violating_answer_is_scanned_once() -> None:
    engine = _StubEngine()
    _run([ComplianceOutputGateMiddleware(engine=engine)])
    assert len(engine.calls) == 1, f"expected a single scan, got {len(engine.calls)}"


def test_backstop_rescans_when_content_changed_after_the_model_node() -> None:
    """LoopDetectionMiddleware appends to content after the model node.

    The digest no longer matches, so the backstop must scan the mutated text.
    """
    gate = ComplianceOutputGateMiddleware(engine=_StubEngine())

    class _Mutator(AgentMiddleware):
        def after_model(self, state, runtime):  # noqa: ANN001, ANN201
            last = state["messages"][-1]
            return {"messages": [AIMessage(content=SECRET, id=last.id)]}

    # _Mutator is later in the list, so its after_model runs BEFORE the gate's.
    result = _run([gate, _Mutator()], text="无害回答")
    assert SECRET not in _dump(result.get("messages")), "the backstop failed to catch post-model mutation"


# ── E. the mobile adapter, currently the loudest consumer ───────────────────


def test_mobile_final_answer_prefers_sanitized_content() -> None:
    """`_extract_final_answer` reads `raw_messages or messages`.

    With the leak closed both carry the sanitized text, so the preference no
    longer matters — but pin it, because the preference itself is fragile.
    """
    result = _run([ComplianceOutputGateMiddleware(engine=_StubEngine()), RawTranscriptMiddleware()])
    for key in ("raw_messages", "messages"):
        assert SECRET not in _dump(result.get(key)), f"{key} would feed the mobile adapter the original"
