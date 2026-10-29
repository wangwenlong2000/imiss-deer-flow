"""Exercise the installed LangGraph queue and file APIs in an isolated server.

Run with the backend Python environment. No LLM, GPU, production server restart,
or production runtime data is used. The deterministic graph is a scheduler/file
integration fixture, not a measurement of real model or business capacity.
"""

import argparse
import asyncio
import json
import os
import shutil
import socket
import subprocess
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
from deerflow.config.paths import get_paths

class State(TypedDict, total=False):
    marker: str
    delay: float
    result: dict

async def analyze(state: State, config: RunnableConfig):
    started = time.monotonic()
    thread_id = config["configurable"]["thread_id"]
    paths = get_paths()
    rows = csv.DictReader(io.StringIO((paths.sandbox_uploads_dir(thread_id) / "same.csv").read_text()))
    total = sum(int(row["value"]) for row in rows)
    await asyncio.sleep(state.get("delay", 4))
    paths.ensure_thread_dirs(thread_id)
    (paths.sandbox_outputs_dir(thread_id) / "report.csv").write_text(f"marker,total\\n{state['marker']},{total}\\n")
    return {"result": {"marker": state["marker"], "total": total, "started": started, "ended": time.monotonic()}}

builder = StateGraph(State)
builder.add_node("analyze", analyze)
builder.add_edge(START, "analyze")
builder.add_edge("analyze", END)
graph = builder.compile()
'''

WEBAPP = '''
from fastapi import FastAPI
from starlette.middleware import Middleware
from app.graph_http import RunAdmissionMiddleware
from app.gateway.routers import uploads, artifacts
app = FastAPI(middleware=[Middleware(RunAdmissionMiddleware)])
app.include_router(uploads.router)
app.include_router(artifacts.router)
'''


async def exercise(base_url: str, process: subprocess.Popen) -> dict:
    async with httpx.AsyncClient(base_url=base_url, timeout=25) as client:
        for _ in range(100):
            if process.poll() is not None:
                raise RuntimeError("Isolated LangGraph server exited during startup")
            try:
                if (await client.get("/ok")).is_success:
                    break
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2)
        else:
            raise TimeoutError("Isolated LangGraph startup timed out")

        async def create_thread(index):
            response = await client.post("/threads", json={})
            response.raise_for_status()
            thread_id = response.json()["thread_id"]
            response = await client.post(
                f"/api/threads/{thread_id}/uploads",
                files={"files": ("same.csv", f"value\n{index}\n{index * 10}\n", "text/csv")},
            )
            response.raise_for_status()
            assert response.json()["success"], response.text
            return thread_id

        threads = await asyncio.gather(*(create_thread(i) for i in [1, 2, 3, 4]))

        async def submit(index, suffix="", delay=4):
            return await client.post(
                f"/threads/{threads[index]}/runs{suffix}",
                json={"assistant_id": "test", "input": {"marker": f"task-{index}", "delay": delay}},
            )

        async def wait_status(thread_id, run_id, expected):
            for _ in range(150):
                response = await client.get(f"/threads/{thread_id}/runs/{run_id}")
                response.raise_for_status()
                status = response.json()["status"]
                if status in expected:
                    return status
                if status in {"error", "timeout"}:
                    raise AssertionError(f"Unexpected task status: {status}")
                await asyncio.sleep(0.1)
            raise TimeoutError(f"Task did not reach {expected}")

        initial = await asyncio.gather(submit(0), submit(1))
        for response in initial:
            response.raise_for_status()
        runs = [response.json()["run_id"] for response in initial]
        await asyncio.gather(*(wait_status(t, r, {"running"}) for t, r in zip(threads, runs)))
        queued = await submit(2)
        queued.raise_for_status()
        runs.append(queued.json()["run_id"])
        assert await wait_status(threads[2], runs[2], {"pending"}) == "pending"
        rejected = await submit(3)
        assert rejected.status_code == 429, rejected.text

        # Joining a queued stream must work even when admission is full.
        rejoined = await client.get(f"/threads/{threads[2]}/runs/{runs[2]}/stream", params={"stream_mode": "values"})
        rejoined.raise_for_status()
        assert "task-2" in rejoined.text
        await asyncio.gather(*(wait_status(t, r, {"success"}) for t, r in zip(threads, runs)))
        results = []
        for index, thread_id in enumerate(threads[:3]):
            state = (await client.get(f"/threads/{thread_id}/state")).json()
            result = state["values"]["result"]
            assert result["marker"] == f"task-{index}"
            assert result["total"] == (index + 1) * 11
            download = await client.get(f"/api/threads/{thread_id}/artifacts/mnt/user-data/outputs/report.csv?download=true")
            download.raise_for_status()
            assert download.text == f"marker,total\ntask-{index},{(index + 1) * 11}\n"
            results.append(result)
        assert max(r["started"] for r in results[:2]) < min(r["ended"] for r in results[:2])
        assert results[2]["started"] >= min(r["ended"] for r in results[:2])

        cancelled = await submit(3)
        cancelled.raise_for_status()
        run_id = cancelled.json()["run_id"]
        await wait_status(threads[3], run_id, {"running"})
        response = await client.post(f"/threads/{threads[3]}/runs/{run_id}/cancel", params={"wait": "true", "action": "interrupt"})
        response.raise_for_status()
        await wait_status(threads[3], run_id, {"interrupted"})
        # Continue an existing thread through both native wait and POST SSE APIs.
        continuation = await submit(0, "/wait", delay=0.01)
        continuation.raise_for_status()
        assert continuation.json()["result"]["total"] == 11
        streamed = await submit(1, "/stream", delay=0.01)
        streamed.raise_for_status()
        assert "event: metadata" in streamed.text and "task-1" in streamed.text
        return {
            "scope": "isolated real LangGraph runtime; deterministic CSV graph; no LLM or GPU",
            "worker_jobs": 2,
            "max_inflight": 3,
            "two_runs_overlap": True,
            "third_run_queues": True,
            "fourth_run_http_status": rejected.status_code,
            "queued_stream_reconnect": True,
            "same_named_uploads_and_outputs_isolated": True,
            "cancel_and_continue": True,
            "native_wait_and_post_stream": True,
            "results": results,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    backend = Path(__file__).resolve().parents[1] / "backend"
    cli = shutil.which("langgraph") or str(backend / ".venv/bin/langgraph")
    with tempfile.TemporaryDirectory(prefix="deerflow-concurrency-") as temporary:
        root = Path(temporary)
        (root / "graph.py").write_text(GRAPH)
        (root / "webapp.py").write_text(WEBAPP)
        (root / "config.yaml").write_text("config_version: 1\nsandbox:\n  use: deerflow.sandbox.local:LocalSandboxProvider\nmemory:\n  enabled: false\ncompliance:\n  enabled: false\n")
        (root / "langgraph.json").write_text(json.dumps({
            "dependencies": [str(backend)], "graphs": {"test": str(root / "graph.py") + ":graph"},
            "http": {"app": str(root / "webapp.py") + ":app"},
        }))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        env = {**os.environ, "DEER_FLOW_HOME": str(root / "data"), "DEER_FLOW_CONFIG_PATH": str(root / "config.yaml"),
               "DEER_FLOW_MAX_INFLIGHT_RUNS": "3", "LANGSMITH_TRACING": "false", "LANGCHAIN_TRACING_V2": "false",
               "PYTHONPATH": str(backend) + os.pathsep + os.environ.get("PYTHONPATH", ""), "PYTHONDONTWRITEBYTECODE": "1"}
        with (root / "server.log").open("w+") as log:
            process = subprocess.Popen([cli, "dev", "--config", str(root / "langgraph.json"), "--host", "127.0.0.1", "--port", str(port),
                                        "--no-browser", "--no-reload", "--allow-blocking", "--n-jobs-per-worker", "2"], cwd=root, env=env, stdout=log, stderr=log)
            try:
                summary = asyncio.run(exercise(f"http://127.0.0.1:{port}", process))
            except Exception:
                log.seek(0)
                print(log.read()[-6000:])  # Only isolated fixture logs, no provider calls.
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
