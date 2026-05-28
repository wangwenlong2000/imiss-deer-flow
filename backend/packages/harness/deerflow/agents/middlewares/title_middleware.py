"""Middleware for automatic thread title generation."""

import ast
import json
from typing import NotRequired, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langgraph.runtime import Runtime

from deerflow.config.title_config import get_title_config
from deerflow.models import create_chat_model


class TitleMiddlewareState(AgentState):
    """Compatible with the `ThreadState` schema."""

    title: NotRequired[str | None]


class TitleMiddleware(AgentMiddleware[TitleMiddlewareState]):
    """Automatically generate a title for the thread after the first user message."""

    state_schema = TitleMiddlewareState

    @staticmethod
    def _is_internal_human_message(message) -> bool:
        """Return True for middleware-injected human guidance messages."""
        if getattr(message, "type", None) != "human":
            return False

        name = getattr(message, "name", None)
        if isinstance(name, str) and (
            name.endswith("_guidance")
            or name.startswith("intent_")
            or name.startswith("routing_")
        ):
            return True

        additional_kwargs = getattr(message, "additional_kwargs", {}) or {}
        if isinstance(additional_kwargs, dict) and additional_kwargs.get("internal") is True:
            return True

        return False

    @staticmethod
    def _message_text(message) -> str:
        content = getattr(message, "content", "")
        if isinstance(content, str):
            return content.strip()
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
            return "\n".join(parts).strip()
        return str(content).strip()

    def _should_generate_title(self, state: TitleMiddlewareState) -> bool:
        """Check if we should generate a title for this thread."""
        config = get_title_config()
        if not config.enabled:
            return False

        # Check if thread already has a title in state
        if state.get("title"):
            return False

        # Check if this is the first turn (has at least one user message and one assistant response)
        messages = state.get("messages", [])
        if len(messages) < 2:
            return False

        # Count user and assistant messages
        user_messages = [m for m in messages if m.type == "human" and not self._is_internal_human_message(m)]
        assistant_messages = [m for m in messages if m.type == "ai" and self._message_text(m)]

        # Generate title after first complete exchange
        return len(user_messages) == 1 and len(assistant_messages) >= 1

    def _build_title_prompt(self, state: TitleMiddlewareState) -> tuple[str, str, str]:
        """Return prompt plus normalized user/assistant content."""
        config = get_title_config()
        messages = state.get("messages", [])

        # Get first user message and first assistant response
        user_message = next((m for m in messages if m.type == "human" and not self._is_internal_human_message(m)), None)
        assistant_message = next((m for m in messages if m.type == "ai" and self._message_text(m)), None)

        user_msg = self._message_text(user_message) if user_message is not None else ""
        assistant_msg = self._message_text(assistant_message) if assistant_message is not None else ""

        prompt = config.prompt_template.format(
            max_words=config.max_words,
            user_msg=user_msg[:500],
            assistant_msg=assistant_msg[:500],
        )
        return prompt, user_msg, assistant_msg

    @staticmethod
    def _normalize_title(raw_title: str, user_msg: str) -> str:
        """Normalize model output and provide a deterministic fallback."""
        config = get_title_config()
        title = TitleMiddleware._extract_title_text(raw_title).strip().strip('"').strip("'")
        if title:
            return title[: config.max_chars] if len(title) > config.max_chars else title

        fallback_chars = min(config.max_chars, 50)
        if len(user_msg) > fallback_chars:
            return user_msg[:fallback_chars].rstrip() + "..."
        return user_msg if user_msg else "New Conversation"

    @staticmethod
    def _extract_response_text(content) -> str:
        """Extract visible text from model response content and ignore thinking blocks."""
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for part in content:
                if isinstance(part, str):
                    if part.strip():
                        parts.append(part)
                elif isinstance(part, dict):
                    if part.get("type") == "text" and isinstance(part.get("text"), str):
                        parts.append(part["text"])
                    elif "text" in part and isinstance(part.get("text"), str):
                        parts.append(part["text"])
                    # Intentionally ignore {"thinking": "..."} and other non-text blocks.
                else:
                    text = getattr(part, "text", None)
                    if isinstance(text, str):
                        parts.append(text)
            return "\n".join(part.strip() for part in parts if part.strip())
        text = getattr(content, "text", None)
        if isinstance(text, str):
            return text
        return str(content)

    @staticmethod
    def _extract_title_text(raw_title: str) -> str:
        """Handle stringified content blocks from thinking-capable gateways."""
        raw = raw_title.strip()
        if not raw:
            return ""

        parsed = None
        if raw[0] in "[{":
            try:
                parsed = json.loads(raw)
            except Exception:
                try:
                    parsed = ast.literal_eval(raw)
                except Exception:
                    parsed = None
        if parsed is not None:
            return TitleMiddleware._extract_response_text(parsed)

        # Some gateways leak labels despite prompt instructions.
        for prefix in ("Title:", "标题：", "标题:"):
            if raw.startswith(prefix):
                return raw[len(prefix):].strip()
        return raw

    async def _generate_title(self, state: TitleMiddlewareState) -> str:
        """Generate a concise title based on the conversation."""
        prompt, user_msg, _assistant_msg = self._build_title_prompt(state)

        # Use a lightweight model to generate title
        config = get_title_config()
        model = create_chat_model(name=config.model_name, thinking_enabled=False)

        try:
            response = await model.ainvoke(prompt)
            title_content = self._extract_response_text(response.content)
            return self._normalize_title(title_content, user_msg)
        except Exception as e:
            print(f"Failed to generate title: {e}")
            return self._normalize_title("", user_msg)

    def _generate_title_sync(self, state: TitleMiddlewareState) -> str:
        """Synchronous variant used by sync graph execution."""
        prompt, user_msg, _assistant_msg = self._build_title_prompt(state)
        config = get_title_config()
        model = create_chat_model(name=config.model_name, thinking_enabled=False)

        try:
            response = model.invoke(prompt)
            title_content = self._extract_response_text(response.content)
            return self._normalize_title(title_content, user_msg)
        except Exception as e:
            print(f"Failed to generate title: {e}")
            return self._normalize_title("", user_msg)

    @override
    def after_model(self, state: TitleMiddlewareState, runtime: Runtime) -> dict | None:
        """Generate and set thread title after the first agent response (sync)."""
        if self._should_generate_title(state):
            title = self._generate_title_sync(state)
            print(f"Generated thread title: {title}")

            # Store title in state (will be persisted by checkpointer if configured)
            return {"title": title}

        return None

    @override
    async def aafter_model(self, state: TitleMiddlewareState, runtime: Runtime) -> dict | None:
        """Generate and set thread title after the first agent response."""
        if self._should_generate_title(state):
            title = await self._generate_title(state)
            print(f"Generated thread title: {title}")

            # Store title in state (will be persisted by checkpointer if configured)
            return {"title": title}

        return None
