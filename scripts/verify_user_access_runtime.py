"""Verify IAM isolation against an isolated, real LangGraph HTTP runtime.

The fixture uses local CSV arithmetic and a temporary IAM provider. It never
calls a model, GPU, external IAM service or existing deployment. Only a compact
assertion summary is emitted; temporary tokens, IDs, state and logs are removed.
Run in the backend Python environment, with ``langgraph`` available on PATH.
"""

import argparse
import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx

GRAPH = '''
import asyncio
import csv
import io
import time
from typing import TypedDict
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, START, END
from langgraph.runtime import Runtime
from deerflow.agents.memory.scope import memory_scope_from_context
from deerflow.agents.memory.updater import _create_empty_memory, _save_memory_to_file
from deerflow.config.paths import get_paths

class State(TypedDict, total=False):
    marker: str
    delay: float
    result: dict

async def analyze(state: State, config: RunnableConfig, runtime: Runtime):
    started = time.monotonic()
    thread_id = config["configurable"]["thread_id"]
    paths = get_paths()
    rows = csv.DictReader(io.StringIO((paths.sandbox_uploads_dir(thread_id) / "same.csv").read_text()))
    total = sum(int(row["value"]) for row in rows)
    await asyncio.sleep(state.get("delay", 3))
    paths.ensure_thread_dirs(thread_id)
    (paths.sandbox_outputs_dir(thread_id) / "report.csv").write_text(f"marker,total\\n{state['marker']},{total}\\n")
    scope = memory_scope_from_context(runtime.context)
    memory = _create_empty_memory()
    memory["user"]["workContext"]["summary"] = state["marker"]
    assert scope is not None
    assert _save_memory_to_file(memory, scope=scope)
    return {"result": {"marker": state["marker"], "total": total, "started": started, "ended": time.monotonic()}}

builder = StateGraph(State)
builder.add_node("analyze", analyze)
builder.add_edge(START, "analyze")
builder.add_edge("analyze", END)
graph = builder.compile()
'''

IAM = '''
import json
import os
from pathlib import Path
from app.gateway.mobile_iam import MobileIAMPrincipal

class Provider:
    async def authenticate(self, authorization):
        if not authorization or not authorization.startswith("Bearer "):
            raise PermissionError("Missing fixture token")
        parts = authorization[7:].split("/")
        if len(parts) != 2 or not all(parts) or parts[0] not in {"org-a", "org-b"}:
            raise PermissionError("Invalid fixture token")
        return MobileIAMPrincipal(org_id=parts[0], user_id=parts[1], identity_class="ordinary_user", permissions=["data_center:*"])

    async def authorize(self, principal, request):
        control = Path(os.environ["FIXTURE_CONTROL"])
        revoked = json.loads(control.read_text()) if control.exists() else []
        if request.conversation_id in revoked:
            raise PermissionError("Fixture dialog permission revoked")
        return {
            "allowed": True, "permission_verified": True,
            "operation": {"type": request.operation_type, "target_audience": request.target_audience},
            "resource": {"resource_id": request.resource_id, "owner_user_id": principal.user_id,
                         "owner_org_id": principal.org_id, "classification": "internal"},
        }
'''

WEBAPP = '''
from fastapi import FastAPI
from starlette.middleware import Middleware
from app.graph_access import GraphAccessMiddleware
from app.graph_http import RunAdmissionMiddleware
from app.gateway.access_middleware import GatewayAccessMiddleware
from app.gateway.routers import uploads, artifacts, mobile, data_center, memory

class RoutedAccess:
    def __init__(self, app):
        self.gateway = GatewayAccessMiddleware(app)
        self.graph = GraphAccessMiddleware(app)

    async def __call__(self, scope, receive, send):
        if scope.get("path", "").startswith("/api/"):
            await self.gateway(scope, receive, send)
        else:
            await self.graph(scope, receive, send)

app = FastAPI(middleware=[Middleware(RoutedAccess), Middleware(RunAdmissionMiddleware)])
for router in (uploads.router, artifacts.router, mobile.router, data_center.router, memory.router):
    app.include_router(router)
'''


async def exercise(base_url: str, process: subprocess.Popen, control: Path) -> dict:
    from app.admission_claims import build_admission_metadata
    from app.internal_auth import internal_graph_headers
    from deerflow.agents.memory.scope import build_memory_scope_context

    owners = [("org-a", "alice"), ("org-a", "bob"), ("org-b", "alice"), ("org-a", "carol")]
    headers = [{"Authorization": f"Bearer {org}/{user}"} for org, user in owners]
    private_headers = internal_graph_headers()
    checks: dict[str, bool | int | str] = {}

    async with httpx.AsyncClient(base_url=base_url, timeout=30) as client:
        for _ in range(150):
            if process.poll() is not None:
                raise RuntimeError("Isolated runtime exited during startup")
            try:
                if (await client.get("/ok")).is_success:
                    break
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2)
        else:
            raise TimeoutError("Isolated runtime startup timed out")

        async def call(method, path, owner=0, **kwargs):
            return await client.request(method, path, headers=headers[owner], **kwargs)

        async def ensure(index, conversation="same-dialog"):
            response = await call("POST", "/api/deerflow/mobile/threads/ensure", index, json={
                "conversation_id": conversation,
                "user_id": "forged-user",
                "metadata": {"org_id": "forged-org", "user_id": "forged-user"},
            })
            response.raise_for_status()
            assert response.json()["code"] == 0, response.text
            return response.json()["data"]["thread_id"]

        threads = await asyncio.gather(*(ensure(index) for index in range(4)))
        assert len(set(threads)) == 4
        assert await ensure(0) == threads[0]
        assert (await call("GET", f"/threads/{threads[0]}")).json()["metadata"]["org_id"] == "org-a"
        checks["organization_user_conversation_binding"] = True

        for index, thread in enumerate(threads):
            response = await call("POST", f"/api/threads/{thread}/uploads", index,
                                  files={"files": ("same.csv", f"value\n{index + 1}\n{(index + 1) * 10}\n", "text/csv")})
            response.raise_for_status()
            assert response.json()["success"], response.text
        checks["authenticated_thread_uploads"] = True

        for route in [f"/threads/{threads[0]}", "/api/memory", "/api/data-center/sources"]:
            assert (await client.get(route)).status_code == 401
            assert (await client.get(route, headers={"Authorization": "invalid"})).status_code == 401
        checks["missing_and_invalid_credentials_rejected"] = True

        for stranger in [1, 2]:
            for method, route, options in [
                ("GET", f"/threads/{threads[0]}/state", {}),
                ("DELETE", f"/threads/{threads[0]}", {}),
                ("GET", f"/api/threads/{threads[0]}/uploads/list", {}),
                ("POST", f"/api/threads/{threads[0]}/uploads", {"files": {"files": ("same.csv", b"forged", "text/csv")}}),
            ]:
                assert (await call(method, route, stranger, **options)).status_code == 404
            assert (await call("PATCH", f"/threads/{threads[0]}", stranger, json={"metadata": {"user_id": owners[stranger][1]}})).status_code == 403
        checks["cross_user_and_cross_organization_thread_access_denied"] = True

        for index in range(3):
            response = await call("POST", "/threads/search", index, json={"limit": 50, "metadata": {"org_id": "forged", "user_id": "forged"}})
            response.raise_for_status()
            assert [record["thread_id"] for record in response.json()] == [threads[index]], response.text
        checks["native_thread_search_filtered_by_verified_owner"] = True

        # A valid queue claim alone cannot authorize creation through the public
        # graph endpoint. Actual creates below use the private gateway credential.
        org, user = owners[0]
        response = await call("POST", f"/threads/{threads[0]}/runs", json={
            "assistant_id": "test", "input": {"marker": "denied"},
            "metadata": build_admission_metadata(user_id=user, org_id=org, identity_class="ordinary_user", task_type="analysis", thread_id=threads[0]),
        })
        assert response.status_code == 403
        checks["public_run_creation_requires_gateway"] = True

        async def submit(index, delay=4):
            org, user = owners[index]
            thread = threads[index]
            return await client.post(f"/threads/{thread}/runs", headers=private_headers, json={
                "assistant_id": "test", "input": {"marker": f"result-{index}", "delay": delay},
                "metadata": build_admission_metadata(user_id=user, org_id=org, identity_class="ordinary_user", task_type="analysis", thread_id=thread),
                "context": {"thread_id": thread, "memory_scope": build_memory_scope_context(org, user, thread)},
            })

        async def status(index, run_id, expected):
            for _ in range(180):
                response = await call("GET", f"/threads/{threads[index]}/runs/{run_id}", index)
                response.raise_for_status()
                value = response.json()["status"]
                if value in expected:
                    return value
                assert value not in {"error", "timeout"}, f"Unexpected fixture run status: {value}"
                await asyncio.sleep(0.1)
            raise TimeoutError("Fixture run status timed out")

        initial = await asyncio.gather(submit(0), submit(1))
        for response in initial:
            response.raise_for_status()
        runs = [response.json()["run_id"] for response in initial]
        await asyncio.gather(*(status(index, run, {"running"}) for index, run in enumerate(runs)))
        rejected = await submit(0)
        assert rejected.status_code == 429 and rejected.headers.get("retry-after")
        queued = await submit(2)
        queued.raise_for_status()
        runs.append(queued.json()["run_id"])
        assert await status(2, runs[2], {"pending"}) == "pending"
        assert (await submit(3)).status_code == 429
        checks["two_workers_user_limit_one_global_limit_three"] = True

        for stranger in [1, 2]:
            assert (await call("POST", f"/threads/{threads[0]}/runs/{runs[0]}/cancel", stranger)).status_code == 404
            assert (await call("GET", f"/threads/{threads[0]}/runs/{runs[0]}/stream", stranger)).status_code == 404
        joined = await call("GET", f"/threads/{threads[2]}/runs/{runs[2]}/stream", 2, params={"stream_mode": "values"})
        joined.raise_for_status()
        assert "result-2" in joined.text
        await asyncio.gather(*(status(index, run, {"success"}) for index, run in enumerate(runs)))
        results = []
        for index in range(3):
            response = await call("GET", f"/threads/{threads[index]}/state", index)
            response.raise_for_status()
            result = response.json()["values"]["result"]
            assert result["total"] == (index + 1) * 11
            results.append(result)
            route = f"/api/threads/{threads[index]}/artifacts/mnt/user-data/outputs/report.csv"
            download = await call("GET", route, index, params={"download": "true"})
            download.raise_for_status()
            assert download.text == f"marker,total\nresult-{index},{(index + 1) * 11}\n"
            if index == 0:
                for stranger in [1, 2]:
                    assert (await call("GET", route, stranger)).status_code == 404
                    assert (await call("GET", f"/api/deerflow/mobile/files/{threads[0]}/report.csv", stranger)).status_code == 404
        assert max(result["started"] for result in results[:2]) < min(result["ended"] for result in results[:2])
        assert results[2]["started"] >= min(result["ended"] for result in results[:2])
        checks["real_parallel_execution_and_pending_stream_reconnect"] = True
        checks["same_named_input_output_isolation_and_download_authorization"] = True

        # Fixture graph wrote memory using the verified runtime namespace. The
        # HTTP endpoint must choose that same namespace, ignoring query hints.
        for index in range(3):
            response = await call("GET", "/api/memory", index, params={"user_id": "forged", "org_id": "forged"})
            response.raise_for_status()
            assert response.json()["user"]["workContext"]["summary"] == f"result-{index}"
            for other in set(range(3)) - {index}:
                assert f"result-{other}" not in response.text
        checks["runtime_memory_and_http_memory_share_isolated_namespace"] = True

        source_responses = await asyncio.gather(*(
            call("POST", "/api/data-center/sources/upload", index % 3,
                 files={"files": ("same.csv", f"catalogue-{index}".encode(), "text/csv")})
            for index in range(12)
        ))
        sources = []
        for response in source_responses:
            response.raise_for_status()
            sources.extend(response.json()["sources"])
        assert len({source["id"] for source in sources}) == 12
        for index in range(3):
            response = await call("GET", "/api/data-center/sources", index)
            response.raise_for_status()
            assert response.json()["count"] == 4
            assert all(source["owner_org_id"] == owners[index][0] and source["owner_user_id"] == owners[index][1] for source in response.json()["sources"])
        source_id = sources[0]["id"]
        for stranger in [1, 2]:
            for method, suffix in [("GET", ""), ("GET", "/download"), ("DELETE", "")]:
                assert (await call(method, f"/api/data-center/sources/{source_id}{suffix}", stranger)).status_code == 404
            assert (await call("POST", "/api/data-center/sources/attach-to-thread", stranger,
                               json={"thread_id": threads[stranger], "source_ids": [source_id]})).status_code == 404
        assert (await call("POST", "/api/data-center/sources/attach-to-thread", 0,
                           json={"thread_id": threads[1], "source_ids": [source_id]})).status_code == 404
        checks["concurrent_catalogue_writes_preserve_all_twelve_records"] = True
        checks["catalogue_list_download_delete_and_attach_isolation"] = True

        # Current IAM permission is checked again; an owned thread alone does
        # not retain access after the upstream authorization changes.
        control.write_text(json.dumps(["same-dialog"]))
        assert (await call("GET", f"/threads/{threads[0]}/state")).status_code == 403
        assert (await call("GET", f"/api/threads/{threads[0]}/uploads/list")).status_code == 403
        assert (await call("POST", "/threads/search", json={"limit": 50})).json() == []
        control.write_text("[]")
        checks["revoked_dialog_access_denied_and_search_filtered"] = True

        cancelled = await submit(3, delay=8)
        cancelled.raise_for_status()
        run_id = cancelled.json()["run_id"]
        await status(3, run_id, {"running"})
        response = await call("POST", f"/threads/{threads[3]}/runs/{run_id}/cancel", 3, params={"wait": "true", "action": "interrupt"})
        response.raise_for_status()
        await status(3, run_id, {"interrupted"})
        recovered = await submit(3, delay=0.01)
        recovered.raise_for_status()
        await status(3, recovered.json()["run_id"], {"success"})
        checks["cancel_restores_authenticated_user_capacity"] = True

    return {
        "scope": "isolated real LangGraph plus Gateway HTTP; deterministic CSV and temporary IAM; no model/GPU/external service",
        "worker_jobs": 2, "max_inflight": 3, "max_per_user": 1,
        "checks": checks, "passed": len(checks),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Optional assertion summary JSON; no raw runtime data")
    args = parser.parse_args()
    backend = Path(__file__).resolve().parents[1] / "backend"
    sys.path[:0] = [str(backend), str(backend / "packages" / "harness")]
    os.environ["DEER_FLOW_ADMISSION_SIGNING_KEY"] = "isolated-access-fixture-key-at-least-32-bytes"
    cli = shutil.which("langgraph") or str(Path(sys.executable).with_name("langgraph"))
    with tempfile.TemporaryDirectory(prefix="deerflow-user-access-") as temporary:
        root = Path(temporary)
        (root / "graph.py").write_text(GRAPH)
        (root / "fixture_iam.py").write_text(IAM)
        (root / "webapp.py").write_text(WEBAPP)
        (root / "extensions_config.json").write_text("{}")
        (root / "control.json").write_text("[]")
        (root / "config.yaml").write_text(
            "config_version: 1\nsandbox:\n  use: deerflow.sandbox.local:LocalSandboxProvider\n"
            "memory:\n  enabled: false\ncompliance:\n  enabled: false\n"
        )
        (root / "langgraph.json").write_text(json.dumps({
            "dependencies": [str(backend)], "graphs": {"test": f"{root / 'graph.py'}:graph"},
            "http": {"app": f"{root / 'webapp.py'}:app"},
        }))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        env = {
            **os.environ,
            "DEER_FLOW_HOME": str(root / "data"), "DEER_FLOW_CONFIG_PATH": str(root / "config.yaml"),
            "DEER_FLOW_EXTENSIONS_CONFIG_PATH": str(root / "extensions_config.json"),
            "MOBILE_IAM_PROVIDER": "fixture_iam:Provider", "MOBILE_IAM_REQUIRED": "true",
            "FIXTURE_CONTROL": str(root / "control.json"), "LANGGRAPH_URL": f"http://127.0.0.1:{port}",
            "MOBILE_ADAPTER_LANGGRAPH_URL": f"http://127.0.0.1:{port}", "MOBILE_ADAPTER_ASSISTANT_ID": "test",
            "DEER_FLOW_MAX_INFLIGHT_RUNS": "3", "DEER_FLOW_MAX_INFLIGHT_PER_USER": "1",
            "DEER_FLOW_REQUIRE_SIGNED_ADMISSION": "true", "DEER_FLOW_IDENTITY_CLASS_LIMITS": "{}", "DEER_FLOW_TASK_TYPE_LIMITS": "{}",
            "LANGSMITH_TRACING": "false", "LANGCHAIN_TRACING_V2": "false", "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": os.pathsep.join([str(root), str(backend), str(backend / "packages" / "harness"), os.environ.get("PYTHONPATH", "")]),
        }
        with (root / "server.log").open("w+") as log:
            process = subprocess.Popen([
                cli, "dev", "--config", str(root / "langgraph.json"), "--host", "127.0.0.1", "--port", str(port),
                "--no-browser", "--no-reload", "--allow-blocking", "--n-jobs-per-worker", "2",
            ], cwd=root, env=env, stdout=log, stderr=log)
            try:
                summary = asyncio.run(exercise(f"http://127.0.0.1:{port}", process, root / "control.json"))
            except Exception:
                # The log contains only generated fixture records. Print only
                # error lines needed to diagnose runtime compatibility failures.
                log.seek(0)
                print("\n".join(line.rstrip() for line in log if "Error" in line or "Traceback" in line)[-4000:])
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
