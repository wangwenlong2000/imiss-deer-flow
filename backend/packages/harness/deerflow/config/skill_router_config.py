import os
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class RouterCardsConfig(BaseModel):
    """Configuration for Router Card discovery and loading."""

    registry_path: str = Field(default="skills/registry.json", description="Path to the registry JSON file")
    card_root: str = Field(default="skills", description="Root directory for Router Card files")
    strict_missing_router_card: bool = Field(
        default=False,
        description="If True, skills missing a router_card.json will cause startup failure",
    )


class VectorStoreConfig(BaseModel):
    """Elasticsearch vector store configuration for SkillRouter."""

    provider: str = Field(default="elasticsearch")
    url_env: str = Field(default="ES_URL", description="Environment variable name for ES URL")
    username_env: str = Field(default="ES_USERNAME", description="Environment variable name for ES username")
    password_env: str = Field(default="ES_PASSWORD", description="Environment variable name for ES password")
    index_env: str = Field(default="SKILL_ROUTER_ES_INDEX", description="Environment variable name for ES index")
    default_index: str = Field(default="citybrain-skill-router-cards")
    vector_field: str = Field(default="embedding_vector")
    text_field: str = Field(default="routing_text")
    id_field: str = Field(default="skill_id")
    top_k: int = Field(default=8, description="Number of candidates to retrieve from ES")

    model_config = ConfigDict(extra="ignore")

    def get_es_url(self) -> str:
        return os.getenv(self.url_env, "")

    def get_es_username(self) -> str:
        return os.getenv(self.username_env, "")

    def get_es_password(self) -> str:
        return os.getenv(self.password_env, "")

    def get_es_index(self) -> str:
        return os.getenv(self.index_env, self.default_index)


class EmbeddingConfig(BaseModel):
    """SkillRouter Embedding service configuration."""

    provider: str = Field(default="skillrouter_embedding_api")
    model_name: str = Field(default="SkillRouter-Embedding-0.6B")
    base_url_env: str = Field(default="SKILLROUTER_EMBEDDING_BASE_URL")
    api_key_env: str = Field(default="SKILLROUTER_EMBEDDING_BASE_KEY")
    default_base_url: str = Field(default="http://192.168.200.1:7800/v1")

    def get_base_url(self) -> str:
        return os.getenv(self.base_url_env, self.default_base_url)

    def get_api_key(self) -> str:
        return os.getenv(self.api_key_env, "unused")


class RerankerConfig(BaseModel):
    """SkillRouter Reranker service configuration."""

    provider: str = Field(default="skillrouter_reranker_api")
    model_name: str = Field(default="SkillRouter-Reranker-0.6B")
    base_url_env: str = Field(default="SKILLROUTER_RERANKER_BASE_URL")
    api_key_env: str = Field(default="SKILLROUTER_RERANKER_BASE_KEY")
    default_base_url: str = Field(default="http://192.168.200.1:7801/v1")

    def get_base_url(self) -> str:
        return os.getenv(self.base_url_env, self.default_base_url)

    def get_api_key(self) -> str:
        return os.getenv(self.api_key_env, "unused")


class SkillRouterConfig(BaseModel):
    """Top-level SkillRouter configuration, maps to skill_router section in config.yaml."""

    enabled: bool = Field(default=True)
    mode: Literal["scene_rerank", "embedding_rerank"] = Field(
        default="scene_rerank",
        description=(
            "scene_rerank uses intent scene filtering as the coarse candidate "
            "set and calls only the reranker. embedding_rerank keeps the legacy "
            "embedding + ES recall + reranker pipeline."
        ),
    )
    scene_prefilter_enabled: bool = Field(
        default=True,
        description="When enabled, use detected scene(s) as the candidate boundary before reranking.",
    )
    fallback_global_when_no_scene: bool = Field(
        default=True,
        description="When scene_rerank has no detected scene, allow legacy global embedding recall as fallback.",
    )
    scene_filter_when_disabled: bool = Field(
        default=True,
        description=(
            "When SkillRouter ranking is disabled, still use intent scene recognition "
            "to inject only custom skills from the detected scene. Public skills remain "
            "in the base prompt."
        ),
    )
    router_cards: RouterCardsConfig = Field(default_factory=RouterCardsConfig)
    vector_store: VectorStoreConfig = Field(default_factory=VectorStoreConfig)
    embedding: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    reranker: RerankerConfig = Field(default_factory=RerankerConfig)

    model_config = ConfigDict(extra="ignore")


# Global configuration instance (following the pattern of other *_config.py modules)
_skill_router_config: SkillRouterConfig = SkillRouterConfig()


def get_skill_router_config() -> SkillRouterConfig:
    """Get the current SkillRouter configuration."""
    return _skill_router_config


def set_skill_router_config(config: SkillRouterConfig) -> None:
    """Set the SkillRouter configuration."""
    global _skill_router_config
    _skill_router_config = config


def load_skill_router_config_from_dict(config_dict: dict) -> None:
    """Load SkillRouter configuration from a dictionary."""
    global _skill_router_config
    _skill_router_config = SkillRouterConfig(**config_dict)
