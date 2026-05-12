from typing import Annotated, Any, NotRequired, TypedDict

from langchain.agents import AgentState


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


def merge_display_messages(existing: list[Any] | None, new: list[Any] | None) -> list[Any]:
    """Append-only reducer for UI chat history.

    The model-facing `messages` state may be summarized and pruned. The UI-facing
    transcript must not be affected by those removals, so this reducer ignores
    summary bookkeeping and only deduplicates by message id.
    """
    merged = list(existing or [])
    if not new:
        return merged

    id_to_index = {
        message_id: index
        for index, message in enumerate(merged)
        if (message_id := getattr(message, "id", None))
    }

    for message in new:
        if _is_internal_display_message(message):
            continue

        message_id = getattr(message, "id", None)
        if message_id and message_id in id_to_index:
            merged[id_to_index[message_id]] = message
        else:
            if message_id:
                id_to_index[message_id] = len(merged)
            merged.append(message)

    return merged


def _is_internal_display_message(message: Any) -> bool:
    if getattr(message, "name", None) in {"conversation_summary", "todo_reminder"}:
        return True
    content = getattr(message, "content", "")
    if isinstance(content, str) and content.startswith(
        "Here is a summary of the conversation to date:"
    ):
        return True
    if getattr(message, "type", None) == "remove":
        return True
    return False


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


class ThreadState(AgentState):
    sandbox: NotRequired[SandboxState | None]
    thread_data: NotRequired[ThreadDataState | None]
    title: NotRequired[str | None]
    artifacts: Annotated[list[str], merge_artifacts]
    display_messages: Annotated[list[Any], merge_display_messages]
    todos: NotRequired[list | None]
    uploaded_files: NotRequired[list[dict] | None]
    viewed_images: Annotated[dict[str, ViewedImageData], merge_viewed_images]  # image_path -> {base64, mime_type}
