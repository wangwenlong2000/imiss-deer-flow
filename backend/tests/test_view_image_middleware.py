from langchain_core.messages import AIMessage, ToolMessage

from deerflow.agents.middlewares.view_image_middleware import ViewImageMiddleware


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

