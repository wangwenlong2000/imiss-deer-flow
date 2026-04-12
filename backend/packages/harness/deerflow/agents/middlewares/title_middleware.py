"""Middleware for automatic thread title generation."""

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
        user_messages = [m for m in messages if m.type == "human"]
        assistant_messages = [m for m in messages if m.type == "ai"]

        # Generate title after first complete exchange
        return len(user_messages) == 1 and len(assistant_messages) >= 1

    def _build_title_prompt(self, state: TitleMiddlewareState) -> tuple[str, str, str]:
        """Return prompt plus normalized user/assistant content."""
        config = get_title_config()
        messages = state.get("messages", [])

        # Get first user message and first assistant response
        user_msg_content = next((m.content for m in messages if m.type == "human"), "")
        assistant_msg_content = next((m.content for m in messages if m.type == "ai"), "")

        # Ensure content is string (LangChain messages can have list content)
        user_msg = str(user_msg_content) if user_msg_content else ""
        assistant_msg = str(assistant_msg_content) if assistant_msg_content else ""

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
        title = raw_title.strip().strip('"').strip("'")
        if title:
            return title[: config.max_chars] if len(title) > config.max_chars else title

        fallback_chars = min(config.max_chars, 50)
        if len(user_msg) > fallback_chars:
            return user_msg[:fallback_chars].rstrip() + "..."
        return user_msg if user_msg else "New Conversation"

    async def _generate_title(self, state: TitleMiddlewareState) -> str:
        """Generate a concise title based on the conversation."""
        prompt, user_msg, _assistant_msg = self._build_title_prompt(state)

        # Use a lightweight model to generate title
        model = create_chat_model(thinking_enabled=False)

        try:
            response = await model.ainvoke(prompt)
            # Ensure response content is string
            title_content = str(response.content) if response.content else ""
            return self._normalize_title(title_content, user_msg)
        except Exception as e:
            print(f"Failed to generate title: {e}")
            return self._normalize_title("", user_msg)

    def _generate_title_sync(self, state: TitleMiddlewareState) -> str:
        """Synchronous variant used by sync graph execution."""
        prompt, user_msg, _assistant_msg = self._build_title_prompt(state)
        model = create_chat_model(thinking_enabled=False)

        try:
            response = model.invoke(prompt)
            title_content = str(response.content) if response.content else ""
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
