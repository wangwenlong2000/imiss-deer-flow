"""Explicit production factory loading; optional legacy agents stay disabled."""
from onecityagent.application.settings import load_settings_from_env


def api():
    from onecityagent.application.api import create_api_app
    return create_api_app(load_settings_from_env())


def coordinator():
    from onecityagent.application.coordinator import create_coordinator_app
    return create_coordinator_app(load_settings_from_env())
