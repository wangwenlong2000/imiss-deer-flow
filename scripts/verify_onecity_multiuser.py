"""Verify both real adapters over HTTP using isolated synthetic accounts.

The OneCity routes use a temporary SQLite catalogue; DeerFlow uses its actual
OneCityIAMProvider, Gateway and native LangGraph scheduler. No production
configuration, database, token, model or sandbox is accessed.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
from threading import Thread
import time

import httpx


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def exercise(onecity_url, native_url, namespace, tokens, checks, evidence):
    headers = [{"Authorization": token} for token in tokens]
    dialogs = ["citybrain-alice", "citybrain-bob", "citybrain-charlie"]
    totals = [11, 22, 33]
    async with httpx.AsyncClient(timeout=60, trust_env=False) as client:
        for _ in range(150):
            try:
                if (await client.get(native_url + "/ok")).is_success:
                    break
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2)
        else:
            raise TimeoutError("Isolated LangGraph startup failed")

        for route in ["/query/deerflow/identity", "/query/deerflow?conversation_id=citybrain-alice&query=synthetic"]:
            assert (await client.get(onecity_url + route)).status_code == 401
        for owner in range(3):
            identity = await client.get(onecity_url + "/query/deerflow/identity", headers=headers[owner])
            assert identity.status_code == 200
        checks["real_onecity_iam_authentication"] = True

        for index, dialog in enumerate(dialogs):
            for _ in range(2):
                response = await client.post(onecity_url + "/query/deerflow/dialog", headers=headers[index], json={"conversation_id": dialog, "name": "Synthetic integration check"})
                assert response.status_code == 200
                assert response.json()["data"]["dialog_id"] == dialog
        assert (await client.post(onecity_url + "/query/deerflow/dialog", headers=headers[0], json={"conversation_id": "citybrain-alice-second"})).status_code == 200
        assert (await client.post(onecity_url + "/query/deerflow/dialog", headers=headers[1], json={"conversation_id": dialogs[0]})).status_code == 404
        checks["authenticated_dialog_creation_is_idempotent_and_owner_bound"] = True

        threads = []
        for index, dialog in enumerate(dialogs):
            response = await client.post(onecity_url + "/query/deerflow/upload", headers=headers[index],
                params={"conversation_id": dialog}, files={"files": ("same.csv", f"value\n{index + 1}\n{(index + 1) * 10}\n", "text/csv")})
            assert response.status_code == 200, response.text
            payload = response.json()
            assert payload.get("code") == 0, payload
            threads.append(payload["data"]["thread_id"])
        assert len(set(threads)) == 3
        checks["real_onecity_uploads_and_persisted_thread_bindings"] = True

        for stranger in [1, 2]:
            denied = await client.get(onecity_url + "/query/deerflow", headers=headers[stranger], params={"conversation_id": dialogs[0], "query": "synthetic"})
            assert denied.status_code == 404
            assert (await client.get(native_url + f"/api/threads/{threads[0]}/uploads/list", headers=headers[stranger])).status_code == 404
        checks["both_platforms_reject_foreign_dialogs_and_threads"] = True

        statuses = [[], [], []]
        events = [[], [], []]
        running = [asyncio.Event() for _ in dialogs]
        queued = asyncio.Event()
        start_time = time.monotonic()
        evidence["status_transitions"] = [[], [], []]
        async def run(index):
            async with client.stream("GET", onecity_url + "/query/deerflow", headers=headers[index], params={"conversation_id": dialogs[index], "query": "请统计上传CSV并生成报告。"}) as response:
                assert response.status_code == 200, await response.aread()
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    event = json.loads(line[5:].strip())
                    events[index].append(event)
                    metadata = event.get("metadata") or {}
                    status = event.get("status")
                    # Only scheduler events establish actual execution state;
                    # ordinary tool/agent segments can also say "running".
                    if metadata.get("source_event") in {"status", "heartbeat"} and status in {"pending", "running"}:
                        if not statuses[index] or statuses[index][-1] != status:
                            statuses[index].append(status)
                            evidence["status_transitions"][index].append({"status": status, "elapsed_seconds": round(time.monotonic() - start_time, 3)})
                        if status == "running":
                            running[index].set()
                        elif index == 2:
                            queued.set()

        tasks = []
        try:
            # Establish the first two active worker slots before submitting the
            # third request; simultaneous HTTP arrivals have no stable order.
            for index in [0, 1]:
                tasks.append(asyncio.create_task(run(index)))
                await asyncio.wait_for(running[index].wait(), timeout=20)
                assert not tasks[index].done(), "A worker completed before both slots were occupied"
            tasks.append(asyncio.create_task(run(2)))
            await asyncio.wait_for(queued.wait(), timeout=15)
            assert not tasks[0].done() and not tasks[1].done(), "Queued run was observed after active tasks ended"
            rejected = await client.get(onecity_url + "/query/deerflow", headers=headers[0], params={"conversation_id": "citybrain-alice-second", "query": "second"})
            assert rejected.status_code == 200
            error_events = [json.loads(line[5:].strip()) for line in rejected.text.splitlines() if line.startswith("data:")]
            assert any(event.get("code") == 429 for event in error_events), "Same-user task was not rejected with 429"
            assert any(event.get("type") == "[DONE]" and event.get("finished") is False for event in error_events)
            checks["per_user_429_survives_both_stream_adapters"] = True
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=90)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            evidence["stream_outcomes"] = [{"scheduler_statuses": statuses[index], "error_codes": [event.get("code") for event in user_events if event.get("type") == "error"], "finished": [event.get("finished") for event in user_events if event.get("type") == "[DONE]"]} for index, user_events in enumerate(events)]
        assert all("running" in status for status in statuses), statuses
        assert all(any(event.get("type") == "[DONE]" and event.get("finished") is True for event in user_events) for user_events in events)
        assert not any(event.get("type") == "error" for user_events in events for event in user_events)
        checks["real_mobile_pending_to_running_status"] = True

        states = []
        for index, thread in enumerate(threads):
            state = (await client.get(native_url + f"/threads/{thread}/state", headers=headers[index])).json()
            result = state["values"]["result"]
            assert result["total"] == totals[index]
            states.append(result)
            file_url = f"/api/threads/{thread}/artifacts/mnt/user-data/outputs/report.csv"
            report = await client.get(onecity_url + "/query/deerflow/file/download", headers=headers[index], params={"conversation_id": dialogs[index], "url": file_url})
            assert report.status_code == 200, report.text
            assert f"合计,{totals[index]}," in report.text
        assert max(states[0]["started"], states[1]["started"]) < min(states[0]["ended"], states[1]["ended"])
        assert states[2]["started"] >= min(states[0]["ended"], states[1]["ended"])
        first_start = min(state["started"] for state in states)
        evidence["native_task_intervals"] = [{"account": label, "total": state["total"], "started_seconds": round(state["started"] - first_start, 3), "ended_seconds": round(state["ended"] - first_start, 3)} for label, state in zip(["alice", "bob", "charlie"], states)]
        checks["two_native_jobs_overlap_and_third_waits"] = True
        checks["same_named_csv_results_and_authenticated_downloads"] = True

        denied = await client.get(onecity_url + "/query/deerflow/file/download", headers=headers[0], params={"conversation_id": dialogs[0], "url": f"/api/threads/{threads[1]}/artifacts/mnt/user-data/outputs/report.csv"})
        assert denied.status_code == 404
        checks["owned_dialog_cannot_be_used_to_download_another_thread"] = True
        with namespace["database"].connection_context():
            namespace["User"].update(access_token=None).where(namespace["User"].id == "alice").execute()
        assert (await client.get(onecity_url + "/query/deerflow/identity", headers=headers[0])).status_code == 401
        assert (await client.get(native_url + f"/threads/{threads[0]}/state", headers=headers[0])).status_code == 401
        checks["token_revocation_applies_to_both_platforms"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--onecity-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    backend = repository / "backend"
    sys.path[:0] = [str(repository / "scripts"), str(args.onecity_root / "tests")]
    from deerflow_fixture import add_user, load_fixture
    from serve_user_access_demo import GRAPH
    from verify_user_access_runtime import WEBAPP
    from werkzeug.serving import make_server, WSGIRequestHandler

    class QuietRequests(WSGIRequestHandler):
        def log_request(self, *args, **kwargs):
            pass

    with tempfile.TemporaryDirectory(prefix="onecity-multiuser-http-") as temporary:
        root = Path(temporary)
        native_port, onecity_port = free_port(), free_port()
        native_url, onecity_url = f"http://127.0.0.1:{native_port}", f"http://127.0.0.1:{onecity_port}"
        os.environ.update({"DEERFLOW_API_URL": native_url + "/api/deerflow/mobile/run", "DEERFLOW_PUBLIC_BASE_URL": native_url, "ONECITY_DEERFLOW_ROLE_PERMISSIONS": "{}", "ONECITY_DEERFLOW_IDENTITY_CLASS_MAP": "{}"})
        namespace = load_fixture(root / "catalogue.sqlite", source_root=args.onecity_root)
        tokens = [secrets.token_urlsafe(30) for _ in range(3)]
        for user, org, token in zip(["alice", "bob", "charlie"], ["org-a", "org-a", "org-b"], tokens):
            add_user(namespace, user, org, token)
        server = make_server("127.0.0.1", onecity_port, namespace["app"], threaded=True, request_handler=QuietRequests)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        (root / "graph.py").write_text(GRAPH)
        (root / "webapp.py").write_text(WEBAPP)
        (root / "extensions.json").write_text("{}")
        (root / "config.yaml").write_text("config_version: 1\nsandbox:\n  use: deerflow.sandbox.local:LocalSandboxProvider\nmemory:\n  enabled: false\ncompliance:\n  enabled: false\n")
        (root / "langgraph.json").write_text(json.dumps({"dependencies": [str(backend)], "graphs": {"lead_agent": f"{root / 'graph.py'}:graph"}, "http": {"app": f"{root / 'webapp.py'}:app"}}))
        environment = {**os.environ, "DEER_FLOW_HOME": str(root / "data"), "DEER_FLOW_CONFIG_PATH": str(root / "config.yaml"), "DEER_FLOW_EXTENSIONS_CONFIG_PATH": str(root / "extensions.json"), "DEER_FLOW_ADMISSION_SIGNING_KEY": secrets.token_hex(32), "MOBILE_IAM_PROVIDER": "app.gateway.onecity_iam:OneCityIAMProvider", "MOBILE_IAM_ENDPOINT": onecity_url + "/internal/deerflow/iam", "MOBILE_IAM_REQUIRED": "true", "LANGGRAPH_URL": native_url, "MOBILE_ADAPTER_LANGGRAPH_URL": native_url, "MOBILE_ADAPTER_ASSISTANT_ID": "lead_agent", "DEMO_TASK_DELAY": "24", "DEER_FLOW_MAX_INFLIGHT_RUNS": "4", "DEER_FLOW_MAX_INFLIGHT_PER_USER": "1", "LANGSMITH_TRACING": "false", "LANGCHAIN_TRACING_V2": "false", "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": os.pathsep.join([str(root), str(backend), str(backend / "packages/harness")])}
        cli = shutil.which("langgraph") or str(Path(sys.executable).with_name("langgraph"))
        with (root / "runtime.log").open("w+") as log:
            process = subprocess.Popen([cli, "dev", "--config", str(root / "langgraph.json"), "--host", "127.0.0.1", "--port", str(native_port), "--no-browser", "--no-reload", "--allow-blocking", "--n-jobs-per-worker", "2"], cwd=root, env=environment, stdout=log, stderr=log)
            checks, evidence, error_type = {}, {}, None
            try:
                asyncio.run(exercise(onecity_url, native_url, namespace, tokens, checks, evidence))
            except Exception as exc:
                error_type = type(exc).__name__
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                server.shutdown()
                thread.join(timeout=5)
        result = {"passed": error_type is None and all(checks.values()), "checks": checks, "evidence": evidence, "fixture": "real HTTP adapters, isolated SQLite accounts and deterministic CSV graph; no model", "runtime_configuration": {"native_workers": 2, "max_inflight": 4, "max_inflight_per_user": 1, "synthetic_task_seconds": 24}}
        if error_type:
            result["error_type"] = error_type
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
        if not result["passed"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
