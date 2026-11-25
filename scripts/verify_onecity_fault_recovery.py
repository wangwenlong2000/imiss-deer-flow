"""Exercise exact-run recovery through real OneCity/Gateway/native HTTP APIs.

Uses temporary SQLite accounts and the actual graph scheduler. A local
OpenAI-compatible endpoint returns HTTP 503 to exercise the real ChatOpenAI
transport failure path. This is controlled fault injection, separate from live
business-model capacity measurements.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from threading import Thread

import httpx
from verify_onecity_multiuser import free_port
from verify_user_access_runtime import WEBAPP

GRAPH = '''
import asyncio
import os
from typing import Annotated, TypedDict
from langchain_core.messages import AIMessage, AnyMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages

class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]

async def answer(state):
    query = str(state["messages"][-1].content)
    if query == "synthetic-provider-failure":
        model = ChatOpenAI(model="fixture", api_key="synthetic-test-value", base_url=os.environ["FAULT_MODEL_URL"], max_retries=0)
        await model.ainvoke("synthetic fault check")
    await asyncio.sleep(10 if query.startswith("occupy-slot") else 3)
    return {"messages": [AIMessage(content="Synthetic task finished: " + query)]}

builder = StateGraph(State)
builder.add_node("answer", answer)
builder.add_edge(START, "answer")
builder.add_edge("answer", END)
graph = builder.compile()
'''


async def exercise(platform_url, native_url, tokens):
    checks = {}
    headers = {"Authorization": tokens[0]}
    foreign_headers = {"Authorization": tokens[1]}
    async with httpx.AsyncClient(timeout=45, trust_env=False) as client:
        for _ in range(150):
            try:
                if (await client.get(native_url + "/ok")).is_success:
                    break
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2)
        else:
            raise TimeoutError("Isolated native server did not start")

        async def create(dialog, auth=headers):
            response = await client.post(platform_url + "/query/deerflow/dialog", headers=auth, json={"conversation_id": dialog})
            response.raise_for_status()

        async def stream_query(dialog, query, started=None, auth=headers, wanted_status="running"):
            events = []
            async with client.stream("GET", platform_url + "/query/deerflow", headers=auth, params={"conversation_id": dialog, "query": query}) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    event = json.loads(line[5:].strip())
                    events.append(event)
                    metadata = event.get("metadata") or {}
                    if started is not None and event.get("status") == wanted_status and metadata.get("run_id") and not started.done():
                        started.set_result({"conversation_id": dialog, "thread_id": metadata["thread_id"], "run_id": metadata["run_id"]})
            return events

        async def task_status(binding, auth=headers):
            response = await client.get(platform_url + "/query/deerflow/status", headers=auth, params=binding)
            response.raise_for_status()
            return response.json()["data"]["status"]

        dialog = "citybrain-fault-cancel"
        await create(dialog)
        started = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(stream_query(dialog, "cancel-me", started))
        binding = await asyncio.wait_for(started, 20)
        assert await task_status(binding) == "running"
        foreign = await client.post(platform_url + "/query/deerflow/cancel", headers=foreign_headers, json=binding)
        assert foreign.status_code == 404
        checks["foreign_user_cannot_cancel_owner_run"] = True
        cancelled = await client.post(platform_url + "/query/deerflow/cancel", headers=headers, json=binding)
        cancelled.raise_for_status()
        assert cancelled.json()["data"]["status"] == "interrupted"
        events = await asyncio.wait_for(task, 15)
        assert any(event.get("type") == "[DONE]" and event.get("finished") is False for event in events)
        checks["owner_cancellation_is_confirmed_and_stream_is_unsuccessful"] = True
        recovered = await stream_query(dialog, "after-cancellation")
        assert any(event.get("type") == "[DONE]" and event.get("finished") is True for event in recovered)
        checks["cancellation_releases_per_user_capacity"] = True
        repeated = await client.post(platform_url + "/query/deerflow/cancel", headers=headers, json=binding)
        assert repeated.status_code == 200 and repeated.json()["data"]["status"] == "interrupted"
        checks["cancellation_retry_is_idempotent"] = True

        dialog = "citybrain-fault-resume"
        await create(dialog)
        binding = None
        query = "disconnect-and-rejoin"
        async with client.stream("GET", platform_url + "/query/deerflow", headers=headers, params={"conversation_id": dialog, "query": query}) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                event = json.loads(line[5:].strip())
                metadata = event.get("metadata") or {}
                if event.get("status") == "running" and metadata.get("run_id"):
                    binding = {"conversation_id": dialog, "thread_id": metadata["thread_id"], "run_id": metadata["run_id"]}
                    break
        assert binding is not None
        assert await task_status(binding) in {"running", "success"}
        checks["browser_disconnect_does_not_cancel_native_run"] = True
        changed = await client.get(platform_url + "/query/deerflow/resume", headers=headers, params={**binding, "query": "a-different-user-query"})
        assert changed.status_code == 409
        checks["resume_query_must_match_the_admitted_run"] = True
        denied = await client.get(platform_url + "/query/deerflow/resume", headers=foreign_headers, params={**binding, "query": query})
        assert denied.status_code == 404
        checks["foreign_user_cannot_resume_owner_run"] = True
        joined = await client.get(platform_url + "/query/deerflow/resume", headers=headers, params={**binding, "query": query})
        joined.raise_for_status()
        joined_events = [json.loads(line[5:].strip()) for line in joined.text.splitlines() if line.startswith("data:")]
        assert any(event.get("type") == "[DONE]" and event.get("finished") is True for event in joined_events)
        assert "disconnect-and-rejoin" in joined.text
        runs = await client.get(native_url + f"/threads/{binding['thread_id']}/runs", headers=headers)
        runs.raise_for_status()
        assert len(runs.json()) == 1 and runs.json()[0]["run_id"] == binding["run_id"]
        checks["reconnect_joins_same_run_without_duplicate_submission"] = True
        final = await client.get(platform_url + "/query/deerflow/resume", headers=headers, params={**binding, "query": query})
        assert final.status_code == 200 and "disconnect-and-rejoin" in final.text and '"finished": true' in final.text
        checks["finished_run_can_be_restored_after_disconnect"] = True
        assert await task_status(binding) == "success"
        invalid = await client.get(platform_url + "/query/deerflow/resume", headers=headers, params={**binding, "run_id": "invalid", "query": query})
        assert invalid.status_code == 400
        checks["invalid_run_identifier_cannot_resubmit_work"] = True
        await stream_query(dialog, "newer-turn")
        stale = await client.get(platform_url + "/query/deerflow/resume", headers=headers, params={**binding, "query": query})
        assert stale.status_code == 409
        checks["old_run_cannot_replay_a_newer_turn_state"] = True

        racing_ready = asyncio.get_running_loop().create_future()
        racing_task = asyncio.create_task(stream_query(dialog, "concurrent-newer-turn", racing_ready))
        await asyncio.wait_for(racing_ready, 20)
        racing_resume = await client.get(platform_url + "/query/deerflow/resume", headers=headers, params={**binding, "query": query})
        assert racing_resume.status_code == 409
        await racing_task
        checks["newer_active_turn_prevents_old_run_replay"] = True

        tasks, bindings, task_headers = [], [], []
        try:
            for index, token in enumerate(tokens):
                dialog = f"citybrain-fault-queue-{index}"
                auth = {"Authorization": token}
                await create(dialog, auth)
                ready = asyncio.get_running_loop().create_future()
                query = "occupy-slot-" + str(index) if index < 2 else "queued-cancellation"
                tasks.append(asyncio.create_task(stream_query(dialog, query, ready, auth, "running" if index < 2 else "pending")))
                bindings.append(await asyncio.wait_for(ready, 20))
                task_headers.append(auth)
            assert await task_status(bindings[2], task_headers[2]) == "pending"
            response = await client.post(platform_url + "/query/deerflow/cancel", headers=task_headers[2], json=bindings[2])
            response.raise_for_status()
            assert response.json()["data"]["status"] == "interrupted"
            queued_events = await asyncio.wait_for(tasks[2], 10)
            assert not any(event.get("status") == "running" for event in queued_events)
            assert any(event.get("type") == "[DONE]" and event.get("finished") is False for event in queued_events)
            checks["queued_task_cancels_without_starting_and_releases_queue_capacity"] = True
        finally:
            for binding, auth in zip(bindings, task_headers):
                await client.post(platform_url + "/query/deerflow/cancel", headers=auth, json=binding)
            await asyncio.gather(*tasks, return_exceptions=True)

        dialog = "citybrain-fault-provider"
        await create(dialog)
        failed = await stream_query(dialog, "synthetic-provider-failure")
        assert any(event.get("type") == "error" for event in failed)
        assert any(event.get("type") == "[DONE]" and event.get("finished") is False for event in failed)
        assert not any(event.get("type") == "[DONE]" and event.get("finished") is True for event in failed)
        checks["real_model_transport_503_is_not_reported_as_success"] = True
        recovered = await stream_query(dialog, "after-provider-failure")
        assert any(event.get("type") == "[DONE]" and event.get("finished") is True for event in recovered)
        checks["provider_failure_releases_capacity_for_next_request"] = True
    return checks


def main():
    repository = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--onecity-root", type=Path, default=repository / "apps/onecityagent")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    sys.path.insert(0, str(args.onecity_root / "tests"))
    from deerflow_fixture import add_user, load_fixture
    from flask import jsonify
    from werkzeug.serving import WSGIRequestHandler, make_server

    class QuietRequests(WSGIRequestHandler):
        def log_request(self, *_args, **_kwargs):
            pass

    with tempfile.TemporaryDirectory(prefix="onecity-fault-http-") as temporary:
        root = Path(temporary)
        native_port, onecity_port = free_port(), free_port()
        native_url, onecity_url = f"http://127.0.0.1:{native_port}", f"http://127.0.0.1:{onecity_port}"
        os.environ.update({"DEERFLOW_API_URL": native_url + "/api/deerflow/mobile/run", "DEERFLOW_PUBLIC_BASE_URL": native_url, "ONECITY_DEERFLOW_ROLE_PERMISSIONS": "{}", "ONECITY_DEERFLOW_IDENTITY_CLASS_MAP": "{}"})
        namespace = load_fixture(root / "catalogue.sqlite", source_root=args.onecity_root)
        tokens = [secrets.token_urlsafe(30) for _ in range(3)]
        for user, token in zip(["alice", "bob", "charlie"], tokens):
            add_user(namespace, user, "org-fault", token)
        provider_calls = []

        @namespace["app"].post("/fault-model/chat/completions")
        def failing_provider():
            provider_calls.append("503")
            return jsonify({"error": {"message": "Synthetic temporary provider outage", "type": "server_error"}}), 503

        server = make_server("127.0.0.1", onecity_port, namespace["app"], threaded=True, request_handler=QuietRequests)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        backend = repository / "backend"
        (root / "graph.py").write_text(GRAPH)
        (root / "webapp.py").write_text(WEBAPP)
        (root / "extensions.json").write_text("{}")
        (root / "config.yaml").write_text("config_version: 1\nsandbox:\n  use: deerflow.sandbox.local:LocalSandboxProvider\nmemory:\n  enabled: false\ncompliance:\n  enabled: false\n")
        (root / "langgraph.json").write_text(json.dumps({"dependencies": [str(backend)], "graphs": {"lead_agent": f"{root / 'graph.py'}:graph"}, "http": {"app": f"{root / 'webapp.py'}:app"}}))
        environment = {
            **os.environ, "DEER_FLOW_HOME": str(root / "data"), "DEER_FLOW_CONFIG_PATH": str(root / "config.yaml"),
            "DEER_FLOW_EXTENSIONS_CONFIG_PATH": str(root / "extensions.json"), "DEER_FLOW_ADMISSION_SIGNING_KEY": secrets.token_hex(32),
            "MOBILE_IAM_PROVIDER": "app.gateway.onecity_iam:OneCityIAMProvider", "MOBILE_IAM_REQUIRED": "true",
            "MOBILE_IAM_ENDPOINT": onecity_url + "/internal/deerflow/iam", "LANGGRAPH_URL": native_url,
            "MOBILE_ADAPTER_LANGGRAPH_URL": native_url, "MOBILE_ADAPTER_ASSISTANT_ID": "lead_agent",
            "DEER_FLOW_MAX_INFLIGHT_RUNS": "3", "DEER_FLOW_MAX_INFLIGHT_PER_USER": "1",
            "FAULT_MODEL_URL": onecity_url + "/fault-model", "LANGSMITH_TRACING": "false", "LANGCHAIN_TRACING_V2": "false",
            "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": os.pathsep.join([str(root), str(backend), str(backend / "packages/harness"), os.environ.get("PYTHONPATH", "")]),
        }
        cli = shutil.which("langgraph") or str(Path(sys.executable).with_name("langgraph"))
        with (root / "runtime.log").open("w+") as log:
            process = subprocess.Popen([cli, "dev", "--config", str(root / "langgraph.json"), "--host", "127.0.0.1", "--port", str(native_port), "--no-browser", "--no-reload", "--allow-blocking", "--n-jobs-per-worker", "2"], cwd=root, env=environment, stdout=log, stderr=log)
            try:
                checks = asyncio.run(exercise(onecity_url, native_url, tokens))
                assert provider_calls == ["503"]
                checks["provider_failure_is_not_retried_without_bound"] = True
            except Exception:
                log.seek(0)
                print("\n".join(line.rstrip() for line in log if "Error" in line or "Traceback" in line)[-2500:])
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                server.shutdown()
                thread.join(timeout=5)
        result = {"passed": all(checks.values()), "scope": "real OneCity, Gateway, LangGraph HTTP with temporary accounts; injected local ChatOpenAI HTTP 503", "checks": checks, "check_count": len(checks)}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
