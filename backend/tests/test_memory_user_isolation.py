"""Memory isolation must cover persistence, background writes and model input."""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from langchain.agents import create_agent
from langchain.agents.middleware.types import ModelRequest
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.runtime import Runtime

from app.gateway.mobile_iam import MobileIAMPrincipal
from app.gateway.routers import memory as memory_router
from deerflow.agents.memory import queue as queue_module
from deerflow.agents.memory import updater
from deerflow.agents.memory.scope import MemoryScope, build_memory_scope_context, memory_scope_from_context
from deerflow.agents.middlewares import memory_middleware
from deerflow.config.memory_config import MemoryConfig
from deerflow.config.paths import Paths


@pytest.fixture(autouse=True)
def isolated_memory(monkeypatch, tmp_path):
    monkeypatch.setenv("MOBILE_IAM_PROVIDER", "test:provider")
    monkeypatch.setenv("MOBILE_IAM_REQUIRED", "true")
    monkeypatch.setenv("DEER_FLOW_ADMISSION_SIGNING_KEY", "memory-test-signing-secret-at-least-32-bytes")
    monkeypatch.setattr(updater, "get_paths", lambda: Paths(tmp_path))
    updater._memory_cache.clear()
    monkeypatch.setattr("deerflow.config.memory_config._memory_config", MemoryConfig())
    monkeypatch.setattr("deerflow.agents.memory.prompt._count_tokens", lambda text, encoding_name="cl100k_base": len(text))


def _memory(marker):
    data = updater._create_empty_memory()
    data["user"]["workContext"]["summary"] = marker
    return data


def _context(org="org-a", user="user-a", thread="thread-a"):
    return {"thread_id": thread, "memory_scope": build_memory_scope_context(org, user, thread)}


def test_storage_and_cache_separate_user_organization_and_agent():
    a = MemoryScope("org-a", "same-user")
    b = MemoryScope("org-b", "same-user")
    c = MemoryScope("org-a", "other-user")
    for scope, marker in [(a, "alpha"), (b, "bravo"), (c, "charlie")]:
        assert updater._save_memory_to_file(_memory(marker), scope=scope)
    assert updater._save_memory_to_file(_memory("agent-only"), "custom", scope=a)
    for scope, marker in [(a, "alpha"), (b, "bravo"), (c, "charlie")]:
        assert updater.get_memory_data(scope=scope)["user"]["workContext"]["summary"] == marker
    assert updater.get_memory_data("custom", scope=a)["user"]["workContext"]["summary"] == "agent-only"
    assert updater.get_memory_data("custom", scope=b)["user"]["workContext"]["summary"] == ""
    # Readers cannot mutate the shared cached value.
    data = updater.get_memory_data(scope=a)
    data["user"]["workContext"]["summary"] = "corrupted"
    assert updater.get_memory_data(scope=a)["user"]["workContext"]["summary"] == "alpha"


def test_memory_namespaces_do_not_accept_path_traversal_or_ambiguous_ids(tmp_path):
    scope = MemoryScope("../organization", "../../user")
    path = updater._get_memory_file_path("../../agent", scope=scope)
    assert path.is_relative_to(tmp_path / "users")
    assert ".." not in str(path.relative_to(tmp_path))
    assert MemoryScope("a\x00b", "c").key != MemoryScope("a", "b\x00c").key


def test_iam_memory_never_falls_back_to_legacy_file():
    with pytest.raises(PermissionError):
        updater.get_memory_data()
    with pytest.raises(PermissionError):
        updater._save_memory_to_file(_memory("unscoped"))
    assert not updater.MemoryUpdater().update_memory([HumanMessage(content="secret")])
    with pytest.raises(PermissionError):
        memory_scope_from_context({"thread_id": "t", "compliance_identity": {"org_id": "org-a", "user_id": "user-a"}})


@pytest.mark.parametrize("tamper", ["user", "org", "thread", "signature", "non_ascii_signature", "extra", "missing"])
def test_runtime_scope_rejects_tampering_and_wrong_thread(tamper):
    context = _context()
    claim = context["memory_scope"]
    if tamper == "user":
        claim["user_id"] = "victim"
    elif tamper == "org":
        claim["org_id"] = "other-org"
    elif tamper == "thread":
        context["thread_id"] = "another-thread"
    elif tamper == "signature":
        claim["signature"] = "bad"
    elif tamper == "non_ascii_signature":
        claim["signature"] = "伪" * 64
    elif tamper == "extra":
        claim["scope"] = "global"
    else:
        del context["memory_scope"]
    with pytest.raises(PermissionError):
        memory_scope_from_context(context)


def test_valid_scope_remains_thread_bound_for_long_running_tasks(monkeypatch):
    context = _context()
    monkeypatch.setattr(time, "time", lambda: 1_000_000_000_000)
    assert memory_scope_from_context(context) == MemoryScope("org-a", "user-a")


def test_development_mode_retains_legacy_memory(monkeypatch):
    monkeypatch.delenv("MOBILE_IAM_PROVIDER")
    monkeypatch.setenv("MOBILE_IAM_REQUIRED", "false")
    monkeypatch.setattr("deerflow.config.compliance_config.get_compliance_config", lambda: SimpleNamespace(scene_resolver_mode=None))
    assert memory_scope_from_context({}) is None
    assert updater._save_memory_to_file(_memory("legacy"))
    assert updater.get_memory_data()["user"]["workContext"]["summary"] == "legacy"
    with pytest.raises(PermissionError):
        memory_scope_from_context({"memory_scope": {"user_id": "forged"}})


def test_background_queue_keeps_scope_and_agent_when_deduplicating(monkeypatch):
    queue = queue_module.MemoryUpdateQueue()
    monkeypatch.setattr(queue, "_reset_timer", lambda: None)
    monkeypatch.setattr(queue_module.time, "sleep", lambda _: None)
    writes = []
    monkeypatch.setattr(updater.MemoryUpdater, "update_memory", lambda self, **kwargs: writes.append(kwargs) or True)
    a, b = MemoryScope("org-a", "u"), MemoryScope("org-b", "u")
    queue.add("same-thread", ["old"], scope=a)
    queue.add("same-thread", ["other-org"], scope=b)
    queue.add("same-thread", ["other-agent"], "custom", scope=a)
    queue.add("same-thread", ["new"], scope=a)
    queue.add("untrusted-thread", ["unscoped"])
    queue.flush()
    assert len(writes) == 3
    assert {(entry["scope"], entry["agent_name"], entry["messages"][0]) for entry in writes} == {
        (a, None, "new"), (b, None, "other-org"), (a, "custom", "other-agent")
    }


def test_after_agent_queues_only_verified_runtime_scope(monkeypatch):
    queue = Mock()
    monkeypatch.setattr(memory_middleware, "get_memory_queue", lambda: queue)
    middleware = memory_middleware.MemoryMiddleware()
    state = {"messages": [HumanMessage(content="question"), AIMessage(content="answer")]}
    middleware.after_agent(state, Runtime(context={"thread_id": "thread-a"}))
    queue.add.assert_not_called()
    middleware.after_agent(state, Runtime(context=_context()))
    assert queue.add.call_args.kwargs["scope"] == MemoryScope("org-a", "user-a")


def _request(context):
    return ModelRequest(model=Mock(), messages=[], system_message=SystemMessage(content="rules\n<memory>legacy-global-secret</memory>\ntail"), runtime=Runtime(context=context))


def test_shared_model_middleware_injects_only_the_current_user_memory():
    for org, user, marker in [("org-a", "a", "alpha-only"), ("org-a", "b", "bravo-only"), ("org-b", "a", "charlie-only")]:
        updater._save_memory_to_file(_memory(marker), scope=MemoryScope(org, user))
    middleware = memory_middleware.MemoryMiddleware()
    for org, user, expected in [("org-a", "a", "alpha-only"), ("org-a", "b", "bravo-only"), ("org-b", "a", "charlie-only")]:
        request = _request(_context(org, user))
        output = middleware.wrap_model_call(request, lambda patched: patched.system_message.content)
        assert expected in output
        assert "legacy-global-secret" not in output
        assert "rules" in output and "tail" in output
        assert "legacy-global-secret" in request.system_message.content  # no mutation of shared prompt
        assert sum(marker in output for marker in ["alpha-only", "bravo-only", "charlie-only"]) == 1


def test_missing_runtime_identity_removes_factory_global_memory():
    middleware = memory_middleware.MemoryMiddleware()
    output = middleware.wrap_model_call(_request({"thread_id": "thread-a"}), lambda patched: patched.system_message.content)
    assert "legacy-global-secret" not in output
    assert "<memory>" not in output


def test_async_model_wrapper_uses_the_same_authenticated_namespace():
    updater._save_memory_to_file(_memory("async-user-only"), scope=MemoryScope("org-a", "user-a"))

    async def scenario():
        async def handler(patched):
            return patched.system_message.content

        return await memory_middleware.MemoryMiddleware().awrap_model_call(_request(_context()), handler)

    result = asyncio.run(scenario())
    assert "async-user-only" in result
    assert "legacy-global-secret" not in result


@pytest.mark.parametrize("asynchronous", [False, True])
def test_langchain_agent_invokes_scoped_model_hook_and_background_queue(monkeypatch, asynchronous):
    """Exercise real create_agent sync/async lifecycle, not only hook helpers."""
    updater._save_memory_to_file(_memory("alice-context"), scope=MemoryScope("org-a", "alice"))
    updater._save_memory_to_file(_memory("bob-context"), scope=MemoryScope("org-a", "bob"))
    queued = Mock()
    monkeypatch.setattr(memory_middleware, "get_memory_queue", lambda: queued)

    class Capture(BaseCallbackHandler):
        def __init__(self):
            self.system_prompts = []

        def on_chat_model_start(self, serialized, messages, **kwargs):
            self.system_prompts.extend(message.content for batch in messages for message in batch if isinstance(message, SystemMessage))

    capture = Capture()
    agent = create_agent(model=FakeListChatModel(responses=["done"]), middleware=[memory_middleware.MemoryMiddleware()], system_prompt="rules\n<memory>global-secret</memory>")
    for user in ["alice", "bob"]:
        kwargs = {"context": _context("org-a", user), "config": {"callbacks": [capture]}}
        if asynchronous:
            asyncio.run(agent.ainvoke({"messages": [HumanMessage(content="hello")]}, **kwargs))
        else:
            agent.invoke({"messages": [HumanMessage(content="hello")]}, **kwargs)
    assert len(capture.system_prompts) == 2
    assert "alice-context" in capture.system_prompts[0] and "bob-context" not in capture.system_prompts[0]
    assert "bob-context" in capture.system_prompts[1] and "alice-context" not in capture.system_prompts[1]
    assert all("global-secret" not in prompt for prompt in capture.system_prompts)
    assert [call.kwargs["scope"] for call in queued.add.call_args_list] == [MemoryScope("org-a", "alice"), MemoryScope("org-a", "bob")]


def test_parallel_updates_to_one_user_do_not_lose_facts(monkeypatch):
    scope = MemoryScope("org-a", "user-a")
    instance = updater.MemoryUpdater()

    def model():
        def invoke(prompt):
            marker = "fact-one" if "first-conversation" in prompt else "fact-two"
            time.sleep(0.02)  # expose a read-then-write race without the namespace lock
            return SimpleNamespace(content=json.dumps({"newFacts": [{"content": marker, "confidence": 0.99}]}))

        return SimpleNamespace(invoke=invoke)

    monkeypatch.setattr(instance, "_get_model", model)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(instance.update_memory, [HumanMessage(content=text)], scope=scope) for text in ["first-conversation", "second-conversation"]]
        assert all(future.result() for future in futures)
    assert {fact["content"] for fact in updater.reload_memory_data(scope=scope)["facts"]} == {"fact-one", "fact-two"}


@pytest.mark.parametrize("path,method", [("/api/memory", "GET"), ("/api/memory/reload", "POST"), ("/api/memory/status", "GET"), ("/api/memory/config", "GET")])
def test_memory_http_routes_authenticate_and_scope_to_principal(monkeypatch, path, method):
    updater._save_memory_to_file(_memory("alice-secret"), scope=MemoryScope("org-a", "alice"))
    updater._save_memory_to_file(_memory("bob-secret"), scope=MemoryScope("org-a", "bob"))

    async def authenticate(request):
        token = request.headers.get("authorization")
        if token not in {"alice", "bob"}:
            raise HTTPException(status_code=401, detail="Authentication failed")
        return object(), MobileIAMPrincipal(org_id="org-a", user_id=token, identity_class="ordinary_user")

    monkeypatch.setattr(memory_router, "authenticate_mobile_request", authenticate)
    app = FastAPI()
    app.include_router(memory_router.router)

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            assert (await client.request(method, path)).status_code == 401
            response = await client.request(method, path, headers={"Authorization": "alice"}, params={"user_id": "bob", "org_id": "other"})
            assert response.status_code == 200
            assert "bob-secret" not in response.text
            if path != "/api/memory/config":
                assert "alice-secret" in response.text

    asyncio.run(scenario())
