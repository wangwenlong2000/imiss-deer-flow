"""Flask API application factory for the OneCity account/catalogue API."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from flask import Flask, g, jsonify, request
from flasgger import Swagger
from flask_cors import CORS
from flask_login import AnonymousUserMixin, LoginManager
from playhouse.flask_utils import FlaskDB

from onecityagent.application.database import DB, build_database, database_readiness
from onecityagent.application.settings import normalize_settings
from onecityagent.logger import logger


def create_api_app(
    settings: Mapping[str, Any] | None = None,
    *,
    database=None,
    enable_legacy: bool = False,
) -> Flask:
    """Create an API app; never read production files or connect implicitly."""
    if settings is None:
        raise RuntimeError("create_api_app requires explicit settings")
    values = normalize_settings(settings)
    secret_key = values.get("SECRET_KEY")
    if not isinstance(secret_key, str) or not secret_key:
        raise RuntimeError("SECRET_KEY is required for the OneCity API")

    database = build_database(values, database)
    DB.configure(database)

    from onecityagent.models import create_tables  # noqa: F401
    from onecityagent.models.db_models import User
    from onecityagent.service.authentication import load_active_user, token_from_header
    from onecityagent.utils.http_logging import PathOnlyRequestHandler

    app = Flask("onecityagent.api")
    app.config.update(SECRET_KEY=secret_key, JSON_AS_ASCII=False)
    app.extensions["onecity.settings"] = values
    app.extensions["onecity.database"] = database
    app.extensions["onecity.model.User"] = User
    app.extensions["onecity.path_only_request_handler"] = PathOnlyRequestHandler
    FlaskDB(app, database)
    DB.init_app_teardown(app)

    swagger = Swagger(
        app,
        config={
            "title": "OneCity Agent API",
            "description": "OneCity Agent API",
            "version": "1.0.0",
            "termsOfService": "",
            "hide_top_bar": True,
            "specs": [
                {
                    "endpoint": "apispec",
                    "route": "/apispec.json",
                    "rule_filter": lambda _rule: True,
                    "model_filter": lambda _tag: True,
                }
            ],
            "static_url_path": "/flasgger_static",
            "swagger_ui": True,
            "specs_route": "/swagger/",
            "headers": [],
        },
    )
    app.extensions["onecity.swagger"] = swagger
    CORS(app, supports_credentials=True)

    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = "user.login"

    @login_manager.unauthorized_handler
    def unauthorized_api_request():
        return jsonify({"code": 401, "message": "Authentication required", "data": None}), 401

    @login_manager.user_loader
    def load_user(user_id):
        return load_active_user(user_id)

    @login_manager.request_loader
    def load_user_from_request(incoming_request):
        return load_active_user(
            token_from_header(incoming_request.headers.get("Authorization"))
        )

    @app.before_request
    def bind_explicit_authorization():
        if "Authorization" in request.headers:
            user = load_active_user(
                token_from_header(request.headers.get("Authorization"))
            )
            g._login_user = user if user is not None else AnonymousUserMixin()

    from onecityagent.controller.user_controller import user_bp, tenant_bp
    from onecityagent.controller.role_controller import role_bp
    from onecityagent.controller.conversation_controller import conversation_bp
    from onecityagent.controller.dialog_controller import dialog_bp
    from onecityagent.controller.plan_controller import plan_bp

    app.register_blueprint(user_bp, url_prefix="/v1/user")
    app.register_blueprint(tenant_bp, url_prefix="/v1")
    app.register_blueprint(role_bp, url_prefix="/v1/role")
    app.register_blueprint(conversation_bp, url_prefix="/v1/conversation")
    app.register_blueprint(dialog_bp, url_prefix="/v1/dialog")
    app.register_blueprint(plan_bp, url_prefix="/v1/plan")

    if enable_legacy:
        from onecityagent.legacy.api import register_legacy_api

        register_legacy_api(app)

    @app.before_request
    def log_request_info():
        logger.info("Request: {} {}", request.method, request.path)

    @app.after_request
    def log_response_info(response):
        logger.info("Response: {} - {}", response.status_code, request.path)
        return response

    @app.errorhandler(404)
    def not_found(_error):
        return jsonify({"code": 404, "message": "Resource not found", "data": None}), 404

    @app.errorhandler(500)
    def internal_error(error):
        logger.error("500 Internal Server Error ({})", type(error).__name__)
        return jsonify({"code": 500, "message": "Internal server error", "data": None}), 500

    @app.errorhandler(Exception)
    def handle_exception(error):
        logger.error("Unhandled exception ({})", type(error).__name__)
        return jsonify({"code": 500, "message": "An unexpected error occurred", "data": None}), 500

    @app.route("/")
    def index():
        return app.send_static_file("swagger-ui/index.html")

    @app.get("/healthz")
    def healthz():
        ready, missing_tables = database_readiness(database)
        if not ready:
            if missing_tables:
                return jsonify({"status": "not_ready", "missing_tables": missing_tables}), 503
            logger.error("OneCity API readiness check failed")
            return jsonify({"status": "not_ready"}), 503
        return jsonify({"status": "ok"}), 200

    return app
