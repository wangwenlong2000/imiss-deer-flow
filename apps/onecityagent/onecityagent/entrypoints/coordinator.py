"""WSGI entrypoint: onecityagent.entrypoints.coordinator:app"""

from onecityagent.application.coordinator import create_coordinator_app
from onecityagent.application.settings import load_settings_from_env

app = create_coordinator_app(load_settings_from_env())
