from typing import Annotated, Any, NotRequired, TypedDict

from langchain.agents import AgentState
from langchain_core.messages import BaseMessage


class SandboxState(TypedDict):
    sandbox_id: NotRequired[str | None]


class ThreadDataState(TypedDict):
    workspace_path: NotRequired[str | None]
    uploads_path: NotRequired[str | None]
    outputs_path: NotRequired[str | None]


class ViewedImageData(TypedDict):
    base64: str
    mime_type: str


def merge_artifacts(existing: list[str] | None, new: list[str] | None) -> list[str]:
    """Reducer for artifacts list - merges and deduplicates artifacts."""
    if existing is None:
        return new or []
    if new is None:
        return existing
    # Use dict.fromkeys to deduplicate while preserving order
    return list(dict.fromkeys(existing + new))


def merge_viewed_images(existing: dict[str, ViewedImageData] | None, new: dict[str, ViewedImageData] | None) -> dict[str, ViewedImageData]:
    """Reducer for viewed_images dict - merges image dictionaries.

    Special case: If new is an empty dict {}, it clears the existing images.
    This allows middlewares to clear the viewed_images state after processing.
    """
    if existing is None:
        return new or {}
    if new is None:
        return existing
    # Special case: empty dict means clear all viewed images
    if len(new) == 0:
        return {}
    # Merge dictionaries, new values override existing ones for same keys
    return {**existing, **new}


COMPACTION_SUMMARY_PREFIXES = (
    "Here is a summary of the conversation to date:",
    "This is a summary of the conversation to date:",
    "Conversation summary:",
)


def _message_content_text(message: BaseMessage) -> str:
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                text = part.get("text")
                if isinstance(text, str):
                    parts.append(text)
            else:
                text = getattr(part, "text", None)
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts)
    return str(content)


def is_compaction_summary_message(message: BaseMessage) -> bool:
    """Return true for context-compression summaries, which are not user-visible chat."""
    text = _message_content_text(message).lstrip()
    return any(text.startswith(prefix) for prefix in COMPACTION_SUMMARY_PREFIXES)


def merge_raw_messages(existing: list[BaseMessage] | None, new: list[BaseMessage] | None) -> list[BaseMessage]:
    """Reducer for the immutable raw transcript.

    The raw transcript is append-only, but the same snapshot may be emitted
    multiple times as the thread updates. Deduplicate by message id when
    available so we keep a stable, ordered transcript. Context-compression
    summaries belong in the model window, not the UI transcript.
    """
    if existing is None:
        return [message for message in (new or []) if not is_compaction_summary_message(message)]
    if new is None:
        return existing

    merged: list[BaseMessage] = []
    seen: set[Any] = set()

    def add_message(message: BaseMessage) -> None:
        if is_compaction_summary_message(message):
            return
        message_id = getattr(message, "id", None)
        key: Any = message_id if message_id is not None else (
            getattr(message, "type", None),
            _message_content_text(message),
            getattr(message, "tool_call_id", None),
        )
        if key in seen:
            return
        seen.add(key)
        merged.append(message)

    for message in existing:
        add_message(message)
    for message in new:
        add_message(message)

    return merged


class ThreadState(AgentState):
    sandbox: NotRequired[SandboxState | None]
    thread_data: NotRequired[ThreadDataState | None]
    title: NotRequired[str | None]
    artifacts: Annotated[list[str], merge_artifacts]
    raw_messages: NotRequired[Annotated[list[BaseMessage], merge_raw_messages]]
    todos: NotRequired[list | None]
    uploaded_files: NotRequired[list[dict] | None]
    viewed_images: Annotated[dict[str, ViewedImageData], merge_viewed_images]  # image_path -> {base64, mime_type}
    intent_context: NotRequired[dict | None]
    routing_context: NotRequired[dict | None]

    # SkillRouter scope tracking — per-turn
    frontend_enabled_skill_ids: NotRequired[list[str] | None]
    frontend_scope_mode: NotRequired[str]
    base_scope_skill_ids: NotRequired[list[str] | None]
    final_scope_skill_ids: NotRequired[list[str] | None]
    allowed_tool_names: NotRequired[list[str] | None]
