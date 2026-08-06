from typing import get_args, get_type_hints

from langchain_core.messages import AIMessage, ToolMessage

from deerflow.agents.middlewares.view_image_middleware import ViewImageMiddleware, ViewImageMiddlewareState
from deerflow.agents.thread_state import merge_viewed_images


def test_view_image_state_schema_preserves_reducer():
    hints = get_type_hints(ViewImageMiddlewareState, include_extras=True)
    assert get_args(hints["viewed_images"])[1] is merge_viewed_images


def test_view_image_context_message_is_marked_internal():
    middleware = ViewImageMiddleware()
    ai = AIMessage(content="", tool_calls=[{"name": "view_image", "args": {"image_path": "/tmp/a.jpg"}, "id": "call-1"}])
    tool = ToolMessage(content="Successfully read image", tool_call_id="call-1")

    update = middleware._inject_image_message(
        {
            "messages": [ai, tool],
            "viewed_images": {
                "/tmp/a.jpg": {
                    "mime_type": "image/jpeg",
                    "base64": "abc",
                }
            },
        }
    )

    assert update is not None
    msg = update["messages"][0]
    assert msg.additional_kwargs["message_type"] == "view_image_context"
    assert msg.additional_kwargs["internal"] is True
