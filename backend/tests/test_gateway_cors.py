from contextlib import asynccontextmanager
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.gateway import config as gateway_config_module
from app.gateway.app import create_app
from app.gateway.config import DEFAULT_CORS_ORIGINS, GatewayConfig


@asynccontextmanager
async def _noop_lifespan(_app):
    yield


def test_default_gateway_cors_origins_cover_no_nginx_local_ports(monkeypatch):
    monkeypatch.setattr(gateway_config_module, "_gateway_config", None)
    monkeypatch.delenv("CORS_ORIGINS", raising=False)

    config = gateway_config_module.get_gateway_config()

    assert config.cors_origins == DEFAULT_CORS_ORIGINS


def test_gateway_config_parses_custom_cors_origins(monkeypatch):
    monkeypatch.setattr(gateway_config_module, "_gateway_config", None)
    monkeypatch.setenv(
        "CORS_ORIGINS",
        " http://localhost:3001 , http://127.0.0.1:33000 ",
    )

    config = gateway_config_module.get_gateway_config()

    assert config.cors_origins == [
        "http://localhost:3001",
        "http://127.0.0.1:33000",
    ]


def test_uploads_preflight_returns_cors_headers():
    gateway_config = GatewayConfig(cors_origins=DEFAULT_CORS_ORIGINS)

    with patch("app.gateway.app.get_gateway_config", return_value=gateway_config), patch("app.gateway.app.lifespan", _noop_lifespan):
        app = create_app()

    with TestClient(app) as client:
        response = client.options(
            "/api/threads/test-thread/uploads",
            headers={
                "Origin": "http://localhost:3001",
                "Access-Control-Request-Method": "POST",
            },
        )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:3001"
    assert "POST" in response.headers["access-control-allow-methods"]