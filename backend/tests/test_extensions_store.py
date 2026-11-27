"""Management configuration remains writable with immutable release config."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

from deerflow.config.extensions_config import ExtensionsConfig, reset_extensions_config
from deerflow.config.extensions_store import update_extensions


def test_runtime_state_overrides_release_without_mutating_it(tmp_path, monkeypatch):
    release_path = tmp_path / "release.json"
    state_path = tmp_path / "data" / "platform" / "extensions.json"
    release = {
        "mcpServers": {"initial": {"enabled": True, "type": "http", "url": "https://example.invalid/mcp"}},
        "skills": {"stable-skill": {"enabled": True}},
    }
    release_path.write_text(json.dumps(release), encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(release_path))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_STATE_PATH", str(state_path))
    reset_extensions_config()

    before = ExtensionsConfig.from_file()
    assert set(before.mcp_servers) == {"initial"}
    assert before.is_skill_enabled("stable-skill", "custom") is True

    update_extensions("skills", {"stable-skill": {"enabled": False}})
    update_extensions("mcpServers", {"managed": {"enabled": True, "type": "http", "url": "https://managed.invalid/mcp"}})

    after = ExtensionsConfig.from_file()
    assert set(after.mcp_servers) == {"managed"}
    assert after.is_skill_enabled("stable-skill", "custom") is False
    assert release_path.read_text(encoding="utf-8") == json.dumps(release)
    assert set(json.loads(state_path.read_text(encoding="utf-8"))) == {"mcpServers", "skills"}


def test_runtime_state_rejects_deployment_fields(tmp_path, monkeypatch):
    release_path = tmp_path / "release.json"
    state_path = tmp_path / "state.json"
    release_path.write_text("{}", encoding="utf-8")
    state_path.write_text(json.dumps({"models": []}), encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(release_path))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_STATE_PATH", str(state_path))

    try:
        ExtensionsConfig.from_file()
        assert False, "runtime state must not override model/deployment configuration"
    except RuntimeError as exc:
        assert "Failed to load extensions configuration" in str(exc)


def test_skill_state_updates_do_not_lose_concurrent_names(tmp_path, monkeypatch):
    release_path = tmp_path / "release.json"
    state_path = tmp_path / "state.json"
    release_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(release_path))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_STATE_PATH", str(state_path))

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda index: update_extensions("skills", {f"skill-{index}": {"enabled": index % 2 == 0}}), range(24)))

    skills = json.loads(state_path.read_text(encoding="utf-8"))["skills"]
    assert set(skills) == {f"skill-{index}" for index in range(24)}
    assert ExtensionsConfig.from_file().is_skill_enabled("skill-0", "custom") is True
    assert ExtensionsConfig.from_file().is_skill_enabled("skill-1", "custom") is False

