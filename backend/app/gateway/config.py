import os

from pydantic import BaseModel, Field


DEFAULT_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
]


def _parse_cors_origins(cors_origins_str: str | None) -> list[str]:
    """Parse allowed CORS origins from an environment variable."""
    if not cors_origins_str:
        return DEFAULT_CORS_ORIGINS.copy()

    origins = [origin.strip() for origin in cors_origins_str.split(",") if origin.strip()]
    return origins or DEFAULT_CORS_ORIGINS.copy()


class GatewayConfig(BaseModel):
    """Configuration for the API Gateway."""

    host: str = Field(default="0.0.0.0", description="Host to bind the gateway server")
    port: int = Field(default=8001, description="Port to bind the gateway server")
    cors_origins: list[str] = Field(default_factory=lambda: DEFAULT_CORS_ORIGINS.copy(), description="Allowed CORS origins")


_gateway_config: GatewayConfig | None = None


def get_gateway_config() -> GatewayConfig:
    """Get gateway config, loading from environment if available."""
    global _gateway_config
    if _gateway_config is None:
        _gateway_config = GatewayConfig(
            host=os.getenv("GATEWAY_HOST", "0.0.0.0"),
            port=int(os.getenv("GATEWAY_PORT", "8001")),
            cors_origins=_parse_cors_origins(os.getenv("CORS_ORIGINS")),
        )
    return _gateway_config
