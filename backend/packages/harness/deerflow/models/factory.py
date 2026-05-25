import logging
from copy import deepcopy

from langchain.chat_models import BaseChatModel

from deerflow.config import get_app_config, get_tracing_config, is_tracing_enabled
from deerflow.reflection import resolve_class

logger = logging.getLogger(__name__)


def _normalise_openai_compatible_thinking_budget(settings: dict) -> None:
    """Keep OpenAI-compatible thinking budgets below the response token limit.

    Some OpenAI-compatible providers, including DashScope, default the thinking
    budget to a large value when `thinking.type=enabled` is present. If that
    budget is greater than or equal to max_tokens, the provider rejects the
    request before the model runs.
    """
    extra_body = settings.get("extra_body")
    if not isinstance(extra_body, dict):
        return

    thinking = extra_body.get("thinking")
    if not isinstance(thinking, dict) or thinking.get("type") != "enabled":
        return

    max_tokens = settings.get("max_tokens") or settings.get("max_completion_tokens")
    try:
        max_tokens_int = int(max_tokens)
    except (TypeError, ValueError):
        return
    if max_tokens_int <= 1:
        return

    default_budget = max(1, min(4096, max_tokens_int - 1))
    raw_budget = thinking.get("thinking_budget")
    try:
        budget = int(raw_budget) if raw_budget is not None else None
    except (TypeError, ValueError):
        budget = None

    if budget is None or budget >= max_tokens_int:
        thinking["thinking_budget"] = default_budget


def create_chat_model(name: str | None = None, thinking_enabled: bool = False, **kwargs) -> BaseChatModel:
    """Create a chat model instance from the config.

    Args:
        name: The name of the model to create. If None, the first model in the config will be used.

    Returns:
        A chat model instance.
    """
    config = get_app_config()
    if name is None:
        name = config.models[0].name
    model_config = config.get_model_config(name)
    if model_config is None:
        raise ValueError(f"Model {name} not found in config") from None
    model_class = resolve_class(model_config.use, BaseChatModel)
    model_settings_from_config = model_config.model_dump(
        exclude_none=True,
        exclude={
            "use",
            "name",
            "display_name",
            "description",
            "supports_thinking",
            "supports_reasoning_effort",
            "when_thinking_enabled",
            "thinking",
            "supports_vision",
        },
    )
    # Compute effective when_thinking_enabled by merging in the `thinking` shortcut field.
    # The `thinking` shortcut is equivalent to setting when_thinking_enabled["thinking"].
    has_thinking_settings = (model_config.when_thinking_enabled is not None) or (model_config.thinking is not None)
    effective_wte: dict = deepcopy(model_config.when_thinking_enabled) if model_config.when_thinking_enabled else {}
    if model_config.thinking is not None:
        merged_thinking = {**(effective_wte.get("thinking") or {}), **model_config.thinking}
        effective_wte = {**effective_wte, "thinking": merged_thinking}
    if thinking_enabled and has_thinking_settings:
        if not model_config.supports_thinking:
            raise ValueError(f"Model {name} does not support thinking. Set `supports_thinking` to true in the `config.yaml` to enable thinking.") from None
        if effective_wte:
            model_settings_from_config.update(effective_wte)
            _normalise_openai_compatible_thinking_budget(model_settings_from_config)
    if not thinking_enabled and has_thinking_settings:
        if effective_wte.get("extra_body", {}).get("thinking", {}).get("type"):
            # OpenAI-compatible gateway: thinking is nested under extra_body
            kwargs.update({"extra_body": {"thinking": {"type": "disabled"}}})
            kwargs.setdefault("reasoning_effort", "minimal")
        elif effective_wte.get("thinking", {}).get("type"):
            # Native langchain_anthropic: thinking is a direct constructor parameter
            kwargs.update({"thinking": {"type": "disabled"}})

    # Backward compatibility: some older configs used "minimum", but current
    # OpenAI-compatible gateways expect "minimal".
    if kwargs.get("reasoning_effort") == "minimum":
        kwargs["reasoning_effort"] = "minimal"

    if not model_config.supports_reasoning_effort and "reasoning_effort" in kwargs:
        del kwargs["reasoning_effort"]

    model_instance = model_class(**kwargs, **model_settings_from_config)

    if is_tracing_enabled():
        try:
            from langchain_core.tracers.langchain import LangChainTracer

            tracing_config = get_tracing_config()
            tracer = LangChainTracer(
                project_name=tracing_config.project,
            )
            existing_callbacks = model_instance.callbacks or []
            model_instance.callbacks = [*existing_callbacks, tracer]
            logger.debug(f"LangSmith tracing attached to model '{name}' (project='{tracing_config.project}')")
        except Exception as e:
            logger.warning(f"Failed to attach LangSmith tracing to model '{name}': {e}")
    return model_instance
