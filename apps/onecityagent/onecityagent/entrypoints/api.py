"""WSGI entrypoint: onecityagent.entrypoints.api:app"""

from onecityagent.application.api import create_api_app
from onecityagent.application.settings import load_settings_from_env

app = create_api_app(load_settings_from_env())
