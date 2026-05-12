"""Tests for UI transcript preservation across summarization."""

from unittest.mock import MagicMock

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.middlewares.display_messages_middleware import DisplayMessagesMiddleware
from deerflow.agents.thread_state import merge_display_messages


class TestDisplayMessagesReducer:
    def test_append_only_dedupes_by_message_id(self):
        first_user = HumanMessage(content="first question", id="h-1")
        first_answer = AIMessage(content="first answer", id="a-1")
        updated_answer = AIMessage(content="first answer updated", id="a-1")
        second_user = HumanMessage(content="second question", id="h-2")

        result = merge_display_messages(
            [first_user, first_answer],
            [updated_answer, second_user],
        )

        assert [message.id for message in result] == ["h-1", "a-1", "h-2"]
        assert result[1].content == "first answer updated"

    def test_filters_internal_summary_and_todo_messages(self):
        summary = HumanMessage(content="summary", id="s-1", name="conversation_summary")
        legacy_summary = HumanMessage(
            content="Here is a summary of the conversation to date:\n\nold",
            id="s-2",
        )
        todo = HumanMessage(content="todo", id="t-1", name="todo_reminder")
        user = HumanMessage(content="real question", id="h-1")

        result = merge_display_messages([], [summary, legacy_summary, todo, user])

        assert result == [user]


class TestDisplayMessagesMiddleware:
    def test_after_agent_appends_current_messages_to_display_transcript(self):
        middleware = DisplayMessagesMiddleware()
        runtime = MagicMock()
        state = {
            "messages": [
                HumanMessage(content="current question", id="h-2"),
                AIMessage(
                    content="",
                    id="a-2",
                    tool_calls=[{"name": "bash", "args": {}, "id": "tc-1"}],
                ),
                ToolMessage(content="tool result", id="tool-1", tool_call_id="tc-1"),
                AIMessage(content="current answer", id="a-3"),
            ],
            "display_messages": [
                HumanMessage(content="first question", id="h-1"),
                AIMessage(content="first answer", id="a-1"),
            ],
        }

        update = middleware.after_agent(state, runtime)
        merged = merge_display_messages(state["display_messages"], update["display_messages"])

        assert [message.id for message in merged] == [
            "h-1",
            "a-1",
            "h-2",
            "a-2",
            "tool-1",
            "a-3",
        ]
