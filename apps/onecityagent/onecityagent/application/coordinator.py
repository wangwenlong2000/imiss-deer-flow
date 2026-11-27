"""Coordinator app factory for OneCity's authenticated DeerFlow adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from flask import Flask, jsonify, request
from flasgger import Swagger

from onecityagent.application.database import DB, build_database, database_readiness
from onecityagent.application.settings import normalize_settings
from onecityagent.logger import logger


def create_coordinator_app(
    settings: Mapping[str, Any] | None = None,
    *,
    database=None,
    enable_legacy: bool = False,
    deerflow_http=None,
) -> Flask:
    """Create the Coordinator without constructing old A2A/model clients."""
    if settings is None:
        raise RuntimeError("create_coordinator_app requires explicit settings")
    values = normalize_settings(settings)
    database = build_database(values, database)
    DB.configure(database)

    from onecityagent.models import create_tables  # noqa: F401
    from onecityagent.integrations.deerflow.routes import blueprint
    from onecityagent.utils.http_logging import PathOnlyRequestHandler

    app = Flask("onecityagent.coordinator")
    app.config.update(JSON_AS_ASCII=False)
    app.extensions["onecity.settings"] = values
    app.extensions["onecity.database"] = database
    app.extensions["onecity.path_only_request_handler"] = PathOnlyRequestHandler
    if deerflow_http is not None:
        app.extensions["onecity.deerflow_http"] = deerflow_http
    DB.init_app_teardown(app)

    @app.before_request
    def connect_database():
        # Production databases deliberately disable implicit connections. Open
        # the request's connection, as the API's FlaskDB hook does; the existing
        # teardown closes it for health checks and authenticated business routes.
        try:
            database.connect(reuse_if_open=True)
        except Exception as error:
            logger.error("OneCity Coordinator database connection failed ({})", type(error).__name__)
            if request.path == "/healthz":
                return jsonify({"status": "not_ready"}), 503
            return jsonify({"code": 503, "message": "Service unavailable", "data": None}), 503

    Swagger(
        app,
        config={
            "headers": [],
            "specs": [
                {
                    "endpoint": "apispec_1",
                    "route": "/apispec_1.json",
                    "rule_filter": lambda _rule: True,
                    "model_filter": lambda _tag: True,
                }
            ],
            "static_url_path": "/flasgger_static",
            "swagger_ui": True,
            "specs_route": "/apidocs/",
        },
    )
    app.register_blueprint(blueprint)

    if enable_legacy:
        from onecityagent.legacy.coordinator import blueprint as legacy_blueprint

        app.register_blueprint(legacy_blueprint)

    @app.get("/healthz")
    def healthz():
        ready, missing_tables = database_readiness(database)
        if not ready:
            if missing_tables:
                return jsonify({"status": "not_ready", "missing_tables": missing_tables}), 503
            logger.error("OneCity Coordinator readiness check failed")
            return jsonify({"status": "not_ready"}), 503
        return jsonify({"status": "ok"}), 200

    @app.before_request
    def log_request_info():
        logger.info("Request: %s %s", request.method, request.path)

    @app.after_request
    def log_response_info(response):
        logger.info("Response: %s - %s", response.status_code, request.path)
        return response

    @app.errorhandler(404)
    def not_found(_error):
        return jsonify({"code": 404, "message": "Resource not found", "data": None}), 404

    @app.errorhandler(500)
    def internal_error(error):
        logger.error("500 Internal Server Error (%s)", type(error).__name__)
        return jsonify({"code": 500, "message": "Internal server error", "data": None}), 500

    @app.errorhandler(Exception)
    def handle_exception(error):
        logger.error("Unhandled exception (%s)", type(error).__name__)
        return jsonify({"code": 500, "message": "An unexpected error occurred"}), 500

    return app
