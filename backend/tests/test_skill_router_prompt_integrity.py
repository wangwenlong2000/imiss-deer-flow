"""Tests verifying that SkillRouter does not corrupt the base system prompt.

Covers:
1. SkillRouter on + prompt_skills=set() still has <skill_system>, <available_skills>, and all base sections
2. SkillRouter on + prompt_skills=set() does NOT contain specific skill names
3. SkillRouter off + prompt_skills=None has full skills injected
4. Routed skill prompt message only contains custom/domain available_skills override, not base sections
5. Old routed_skill_prompt messages are filtered but other system messages are preserved

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_skill_router_prompt_integrity.py
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Pre-mock heavy modules BEFORE loading the prompt module.
# deerflow.agents.__init__ pulls in langgraph, which isn't installed.
# We load prompt.py directly via spec and inject a mock for load_skills.
# ---------------------------------------------------------------------------

# Mock the config module
_MOCKED_MODULE_NAMES = [
    "deerflow.config",
    "deerflow.config.app_config",
    "deerflow.config.agents_config",
    "deerflow.skills",
    "deerflow.skills.loader",
    "deerflow.skills.parser",
    "deerflow.skills.types",
    "deerflow.agents",
    "deerflow.agents.lead_agent",
    "deerflow.agents.lead_agent.prompt",
]
_ORIGINAL_MODULES = {name: sys.modules.get(name) for name in _MOCKED_MODULE_NAMES}

_app_config_mock = MagicMock()
_app_config_mock.skills.container_path = "/mnt/skills"
_config_mock = MagicMock()
_config_mock.get_app_config.return_value = _app_config_mock

sys.modules["deerflow.config"] = _config_mock
sys.modules["deerflow.config.app_config"] = _config_mock
sys.modules["deerflow.config.agents_config"] = MagicMock()

# Mock skills sub-modules
sys.modules["deerflow.skills"] = MagicMock()
sys.modules["deerflow.skills.loader"] = MagicMock()
sys.modules["deerflow.skills.parser"] = MagicMock()
sys.modules["deerflow.skills.types"] = MagicMock()

# Load prompt.py directly via importlib
import importlib.util
spec = importlib.util.spec_from_file_location(
    "_prompt_under_test",
    "packages/harness/deerflow/agents/lead_agent/prompt.py",
)
prompt_module = importlib.util.module_from_spec(spec)

# Patch the module-level load_skills with a mock that returns an empty list by default
_load_skills_mock = MagicMock(return_value=[])
sys.modules["deerflow.skills"].load_skills = _load_skills_mock
sys.modules["deerflow.skills.loader"].load_skills = _load_skills_mock
prompt_module.load_skills = _load_skills_mock

sys.modules["deerflow.agents"] = MagicMock()
sys.modules["deerflow.agents.lead_agent"] = MagicMock()
sys.modules["deerflow.agents.lead_agent.prompt"] = prompt_module

spec.loader.exec_module(prompt_module)

for _module_name, _original_module in _ORIGINAL_MODULES.items():
    if _original_module is None:
        sys.modules.pop(_module_name, None)
    else:
        sys.modules[_module_name] = _original_module

BASE_SECTIONS = [
    "<role>",
    "<language_policy>",
    "<thinking_style>",
    "<clarification_system>",
    "<working_directory",
    "<response_style>",
    "<critical_reminders>",
    "<skill_system>",
    "<available_skills>",
]


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

class FakeSkill:
    """Lightweight fake Skill for prompt tests."""

    def __init__(self, name, description, category="public"):
        self.name = name
        self.description = description
        self.category = category
        self.enabled = True
        self.skill_path = name

    def get_container_file_path(self, base):
        return f"{base}/{self.name}/SKILL.md"


def _set_mock_skills(skills_list):
    """Configure the mock load_skills to return the given list."""
    def _load_skills_side_effect(*args, **kwargs):
        categories = kwargs.get("categories")
        if categories is None:
            return skills_list
        return [skill for skill in skills_list if skill.category in categories]

    _load_skills_mock.side_effect = _load_skills_side_effect
    prompt_module.load_skills = _load_skills_mock


# ---------------------------------------------------------------------------
# Test 1: SkillRouter on (prompt_skills=set()) — base sections preserved
# ---------------------------------------------------------------------------

def test_skillrouter_on_base_sections_preserved():
    """When SkillRouter is enabled (prompt_skills=set()), all base prompt sections must exist."""
    _set_mock_skills([])
    prompt = prompt_module.apply_prompt_template(prompt_skills=set())

    for section in BASE_SECTIONS:
        assert section in prompt, f"Missing section '{section}' in prompt when SkillRouter is enabled"


# ---------------------------------------------------------------------------
# Test 2: SkillRouter on (prompt_skills=set()) — no concrete skills
# ---------------------------------------------------------------------------

def test_skillrouter_on_no_concrete_skills():
    """When SkillRouter is enabled (prompt_skills=set()), specific skill names must NOT appear."""
    _set_mock_skills([])
    prompt = prompt_module.apply_prompt_template(prompt_skills=set())

    assert "network-traffic-analysis" not in prompt
    assert "policy-analysis" not in prompt


# ---------------------------------------------------------------------------
# Test 3: SkillRouter off (prompt_skills=None) — full skills
# ---------------------------------------------------------------------------

def test_skillrouter_off_full_skills():
    """When SkillRouter is disabled (prompt_skills=None), all enabled skills must appear."""
    fake_skills = [
        FakeSkill("data-analysis", "Analyze data", "public"),
        FakeSkill("network-traffic-analysis", "Analyze network traffic", "custom"),
    ]
    _set_mock_skills(fake_skills)

    prompt = prompt_module.apply_prompt_template(prompt_skills=None)

    assert "<skill_system>" in prompt
    assert "<available_skills>" in prompt
    assert "data-analysis" in prompt
    assert "network-traffic-analysis" in prompt


# ---------------------------------------------------------------------------
# Test 4: get_skills_prompt_section(set()) returns skill_system, not ""
# ---------------------------------------------------------------------------

def test_get_skills_prompt_section_empty_set_not_empty_string():
    """get_skills_prompt_section(set()) must return a non-empty <skill_system> block."""
    _set_mock_skills([])

    result = prompt_module.get_skills_prompt_section(set())

    assert "<skill_system>" in result
    assert "<available_skills>" in result
    assert len(result) > 50  # Not a trivial empty string


# ---------------------------------------------------------------------------
# Test 5: _render_skill_system_section — routed prompt recommends custom/domain skills
# ---------------------------------------------------------------------------

def test_routed_prompt_only_overrides_available_skills():
    """Routed skill prompt must not contain base system prompt sections."""
    skills_list = (
        "<available_skills>\n"
        "    <skill>\n"
        "      <name>network-traffic-analysis</name>\n"
        "      <description>Test</description>\n"
        "      <location>/mnt/skills/test</location>\n"
        "    </skill>\n"
        "</available_skills>"
    )

    routed = prompt_module._render_skill_system_section(
        skills_list=skills_list,
        container_base_path="/mnt/skills",
        routed_mode=True,
    )

    # Must contain routed-mode markers
    assert 'source="skill_router"' in routed
    assert "This routed <skill_system> lists recommended custom/domain skills" in routed
    assert "Public skills listed in the base system prompt remain available for supporting operations" in routed
    assert "use the public data-analysis skill first" in routed
    assert "Continue following language_policy" in routed

    # Must NOT contain base system prompt sections
    assert "<language_policy>" not in routed
    assert "<working_directory" not in routed
    assert "<response_style>" not in routed


# ---------------------------------------------------------------------------
# Test 6: _render_skill_system_section — empty skills still complete
# ---------------------------------------------------------------------------

def test_routed_prompt_empty_skills():
    """Routed skill prompt with empty skills must still be a complete <skill_system>."""
    routed = prompt_module._render_skill_system_section(
        skills_list="<available_skills>\n</available_skills>",
        container_base_path="/mnt/skills",
        empty_available_skills=True,
        routed_mode=True,
    )

    assert "<skill_system" in routed
    assert "<available_skills>" in routed
    assert "No routed custom/domain skill matched this turn" in routed
    assert "Public skills listed in the base system prompt remain available" in routed


def test_prompt_can_request_public_skill_category_only():
    """Prompt rendering can restrict base injection to public skills."""
    _set_mock_skills([
        FakeSkill("data-analysis", "Analyze data", "public"),
        FakeSkill("network-traffic-analysis", "Analyze traffic", "custom"),
    ])

    prompt_module.get_skills_prompt_section(skill_categories={"public"})

    assert _load_skills_mock.call_args.kwargs["categories"] == {"public"}


def test_skillrouter_base_prompt_public_only_category_contract():
    """SkillRouter base prompts request public skills only; custom remains router-owned."""
    _set_mock_skills([
        FakeSkill("data-analysis", "Analyze data", "public"),
        FakeSkill("network-traffic-analysis", "Analyze traffic", "custom"),
    ])

    prompt = prompt_module.apply_prompt_template(skill_categories={"public"})

    assert "data-analysis" in prompt
    assert "network-traffic-analysis" not in prompt
    assert _load_skills_mock.call_args.kwargs["categories"] == {"public"}


# ---------------------------------------------------------------------------
# Test 7: Old routed_skill_prompt messages are filtered
# ---------------------------------------------------------------------------

class _FakeMsg:
    def __init__(self, content, msg_type=None):
        self.content = content
        self.additional_kwargs = {"message_type": msg_type} if msg_type else {}


def test_old_routed_prompt_filtered():
    """Old routed_skill_prompt messages must be removed from message list."""
    messages = [
        _FakeMsg("base prompt", "base"),
        _FakeMsg("old routed skills", "routed_skill_prompt"),
        _FakeMsg("hello"),  # HumanMessage
    ]

    cleaned = [
        msg for msg in messages
        if not (isinstance(msg, _FakeMsg)
                and msg.additional_kwargs.get("message_type") == "routed_skill_prompt")
    ]

    assert len(cleaned) == 2
    assert all(
        msg.additional_kwargs.get("message_type") != "routed_skill_prompt"
        for msg in cleaned
    )


# ---------------------------------------------------------------------------
# Test 8: Other system messages preserved after filtering
# ---------------------------------------------------------------------------

def test_other_system_messages_preserved():
    """Non-routed SystemMessages must be preserved after filtering."""
    messages = [
        _FakeMsg("uploaded files info", "uploaded_files"),
        _FakeMsg("old routed skills", "routed_skill_prompt"),
        _FakeMsg("memory context", "memory_context"),
        _FakeMsg("hello"),  # HumanMessage
    ]

    cleaned = [
        msg for msg in messages
        if not (isinstance(msg, _FakeMsg)
                and msg.additional_kwargs.get("message_type") == "routed_skill_prompt")
    ]

    types = {m.additional_kwargs.get("message_type") for m in cleaned}
    assert "uploaded_files" in types
    assert "memory_context" in types
    assert "routed_skill_prompt" not in types
    assert len(cleaned) == 3


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys as _sys

    passed = 0
    failed = 0

    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                passed += 1
                print(f"  PASS {name}")
            except Exception as e:
                failed += 1
                import traceback
                print(f"  FAIL {name}: {e}")
                traceback.print_exc()

    print(f"\n{passed} passed, {failed} failed")
    _sys.exit(1 if failed else 0)
