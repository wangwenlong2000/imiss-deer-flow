"""Compatibility API entrypoint; use the factory for WSGI deployments."""
from onecityagent.application.api import create_api_app


def main():
    from onecityagent.application.settings import load_settings_from_env, enabled
    from onecityagent.utils.http_logging import PathOnlyRequestHandler
    settings = load_settings_from_env(allow_legacy_path=True)
    app = create_api_app(settings, enable_legacy=enabled(settings.get("ONECITY_ENABLE_LEGACY", False)))
    app.run(host="0.0.0.0", port=int(settings.get("ONECITY_APP_PORT", 7606)),
            debug=enabled(settings.get("ONECITY_DEBUG", False)), request_handler=PathOnlyRequestHandler)


if __name__ == "__main__":
    main()
