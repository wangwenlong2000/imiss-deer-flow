"""Deployment settings are explicit and secret references are expanded safely."""

import pytest

from onecityagent.application.settings import load_settings_from_env, resolve_env_reference


def test_env_reference_requires_an_exact_variable_name(monkeypatch):
    monkeypatch.setenv("MYSQL_PASSWORD", "test-secret")
    assert resolve_env_reference("$MYSQL_PASSWORD", setting_name="password") == "test-secret"
    assert resolve_env_reference("literal $MYSQL_PASSWORD suffix", setting_name="password") == "literal $MYSQL_PASSWORD suffix"
    assert resolve_env_reference("$MYSQL_PASSWORD/$OTHER", setting_name="password") == "$MYSQL_PASSWORD/$OTHER"


def test_missing_secret_reference_fails_without_disclosing_value(monkeypatch):
    monkeypatch.delenv("CITYBRAIN_MISSING_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_PASSWORD references an unset environment variable"):
        resolve_env_reference("$CITYBRAIN_MISSING_SECRET", setting_name="DATABASE_PASSWORD")


def test_toml_database_password_uses_loaded_secret_file_value(tmp_path, monkeypatch):
    config = tmp_path / "onecity.toml"
    config.write_text(
        '[agent.database]\nDATABASE_TYPE="mysql"\nDATABASE_HOST="mysql"\nDATABASE_PORT=3306\n'
        'DATABASE_NAME="onecity"\nDATABASE_USER="onecity"\nDATABASE_PASSWORD="$MYSQL_PASSWORD"\n'
        '\n[a2a]\nrouter="http://coordinator:7010"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("ONECITY_CONFIG_PATH", str(config))
    monkeypatch.setenv("MYSQL_PASSWORD", "local-test-password")

    settings = load_settings_from_env()

    assert settings["DATABASE"]["password"] == "local-test-password"
    assert settings["A2A_COORDINATOR_PORT"] == 7010

