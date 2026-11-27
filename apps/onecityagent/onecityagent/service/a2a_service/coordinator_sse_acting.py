"""Compatibility Coordinator entrypoint. Legacy A2A is explicitly enabled."""
from onecityagent.application.coordinator import create_coordinator_app


def main():
    from onecityagent.application.settings import load_settings_from_env, enabled
    from onecityagent.utils.http_logging import PathOnlyRequestHandler
    settings = load_settings_from_env(allow_legacy_path=True)
    app = create_coordinator_app(settings, enable_legacy=enabled(settings.get("ONECITY_ENABLE_LEGACY", False)))
    app.run(host="0.0.0.0", port=int(settings.get("A2A_COORDINATOR_PORT", 7010)), threaded=True,
            request_handler=PathOnlyRequestHandler)


if __name__ == "__main__":
    main()
