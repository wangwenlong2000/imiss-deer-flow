"""Serve a loopback-only, synthetic user-access demo for the existing frontend.

This is a recording fixture, not a production authentication adapter. A private
native LangGraph runtime retains the real access/admission/Gateway middleware.
The outer proxy signs only requests authenticated by its temporary demo login;
it never forwards browser-supplied internal credentials or ownership claims.
All tasks use deterministic CSV arithmetic, with no model or GPU calls.

Example (start a separate frontend first):
  python scripts/serve_user_access_demo.py --frontend-url http://127.0.0.1:3331

The sole stdout record contains the public base URL. Ctrl-C stops both servers
and removes all temporary state, logs, cookies and synthetic inputs.
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID, uuid4

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from starlette.background import BackgroundTask
from verify_user_access_runtime import IAM, WEBAPP

ACCOUNTS = {
    "alice": {"org": "org-a", "user": "alice", "label": "用户A · Alice · A组织", "total": 400},
    "bob": {"org": "org-a", "user": "bob", "label": "用户B · Bob · A组织", "total": 800},
    "alice_b": {"org": "org-b", "user": "alice", "label": "用户C · Alice · B组织", "total": 1200},
    "carol": {"org": "org-a", "user": "carol", "label": "用户D · Carol · A组织", "total": 1600},
}
COOKIE_NAME = "deerflow_isolated_demo"
HOP_HEADERS = {"host", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "content-length"}

GRAPH = r'''
import asyncio
import csv
import io
import os
import time
import uuid
from typing import Annotated, TypedDict
from langchain_core.messages import AIMessage, AnyMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.runtime import Runtime
from deerflow.agents.memory.scope import memory_scope_from_context
from deerflow.agents.memory.updater import _create_empty_memory, _save_memory_to_file
from deerflow.config.paths import get_paths

class State(TypedDict, total=False):
    messages: Annotated[list[AnyMessage], add_messages]
    title: str
    artifacts: list[str]
    result: dict

async def analyze(state: State, config: RunnableConfig, runtime: Runtime):
    started = time.monotonic()
    thread_id = config["configurable"]["thread_id"]
    scope = memory_scope_from_context(runtime.context)
    assert scope is not None
    paths = get_paths()
    paths.ensure_thread_dirs(thread_id)
    directory = paths.sandbox_uploads_dir(thread_id)
    candidates = sorted(directory.glob("*.csv"), key=lambda path: path.stat().st_mtime_ns, reverse=True)
    fallback = False
    if candidates:
        source = candidates[0]
        rows = list(csv.DictReader(io.StringIO(source.read_text(encoding="utf-8-sig"))))
    else:
        fallback = True
        multiplier = 3 if scope.org_id == "org-b" else {"alice": 1, "bob": 2, "carol": 4}.get(scope.user_id, 1)
        rows = [{"road": "道路A", "flow": str(100 * multiplier), "speed": "20"},
                {"road": "道路B", "flow": str(300 * multiplier), "speed": "30"}]
    # Tasks deliberately wait so the actual scheduler's pending/running state
    # remains visible while recording. This is not model response latency.
    await asyncio.sleep(float(os.environ["DEMO_TASK_DELAY"]))
    numeric_column = "flow" if rows and "flow" in rows[0] else "value"
    total = sum(float(row[numeric_column]) for row in rows)
    average = total / len(rows) if rows else 0
    marker = f"{scope.org_id}/{scope.user_id}"
    report = "road,flow,speed\n" + "\n".join(f"{row.get('road', index + 1)},{row[numeric_column]},{row.get('speed', '')}" for index, row in enumerate(rows)) + f"\n合计,{total:g},\n均值,{average:g},\n"
    (paths.sandbox_outputs_dir(thread_id) / "report.csv").write_text(report, encoding="utf-8-sig")
    filepath = "/mnt/user-data/outputs/report.csv"
    memory = _create_empty_memory()
    memory["user"]["workContext"]["summary"] = f"演示用户 {marker} 最近一次CSV流量合计为 {total:g}。"
    assert _save_memory_to_file(memory, scope=scope)
    call_id = str(uuid.uuid4())
    tool_call = AIMessage(content="", tool_calls=[{"name": "present_files", "args": {"filepaths": [filepath]}, "id": call_id}])
    tool_result = ToolMessage(content='{"files":["' + filepath + '"]}', tool_call_id=call_id, name="present_files")
    answer = AIMessage(content=f"已完成本会话的CSV统计。\n\n| 指标 | 结果 |\n| --- | ---: |\n| 数据行数 | {len(rows)} |\n| 流量合计 | {total:g} |\n| 平均流量 | {average:g} |\n\n已生成 report.csv，可下载核对。\n\n" + ("本次使用演示合成输入。" if fallback else "本次使用当前会话上传的数据。"))
    return {"title": f"道路流量统计 · {scope.user_id}", "messages": [tool_call, tool_result, answer], "artifacts": [filepath],
            "result": {"total": total, "started": started, "ended": time.monotonic(), "synthetic": True}}

builder = StateGraph(State)
builder.add_node("analyze", analyze)
builder.add_edge(START, "analyze")
builder.add_edge("analyze", END)
graph = builder.compile()
'''

NATIVE_WEBAPP = WEBAPP + '''
@app.get("/api/models")
async def demo_models():
    return {"models": [{"id": "synthetic-csv", "name": "synthetic-csv", "display_name": "CSV演示任务（无模型）", "supports_thinking": False}]}

@app.get("/api/skills")
async def demo_skills():
    return {"skills": []}

@app.get("/api/agents")
async def demo_agents():
    return {"agents": []}
'''


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _thread_id(value: str) -> str:
    try:
        if str(UUID(value)) != value:
            raise ValueError("Non-canonical UUID")
    except (ValueError, TypeError, AttributeError) as exc:
        raise HTTPException(422, "演示会话编号必须为UUID") from exc
    return value


def create_demo_app(*, native_url: str, frontend_url: str | None, root: Path) -> FastAPI:
    """Build only the test proxy; product routers are unchanged."""
    from app.admission_claims import build_admission_metadata
    from app.internal_auth import INTERNAL_GRAPH_HEADER, internal_graph_headers
    from deerflow.agents.memory.scope import build_memory_scope_context
    from deerflow.config.paths import get_paths

    sessions: dict[str, str] = {}
    database = root / "demo_bridge.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS demo_threads (thread_id TEXT PRIMARY KEY, org_id TEXT NOT NULL, user_id TEXT NOT NULL)")

    @asynccontextmanager
    async def lifespan(app):
        async with httpx.AsyncClient(timeout=httpx.Timeout(120, connect=10), trust_env=False) as client:
            app.state.client = client
            yield

    app = FastAPI(lifespan=lifespan)

    def account(request: Request):
        name = sessions.get(request.cookies.get(COOKIE_NAME, ""))
        if name not in ACCOUNTS:
            raise HTTPException(401, "请先选择隔离演示身份")
        return name, ACCOUNTS[name]

    def token(identity):
        return {"Authorization": f"Bearer {identity['org']}/{identity['user']}"}

    async def authorize_identity(identity, conversation_id):
        from fixture_iam import Provider

        from app.gateway.mobile_iam import MobileAuthorizationInput, MobileIAMPrincipal, authorize_mobile_request

        provider = Provider()
        principal = MobileIAMPrincipal.model_validate(await provider.authenticate(token(identity)["Authorization"]))
        await authorize_mobile_request(provider, principal, MobileAuthorizationInput(
            request_id=secrets.token_hex(16), turn_id=secrets.token_hex(16), conversation_id=conversation_id,
            operation_type="analysis", target_audience="self", resource_id=conversation_id,
        ))
        return principal

    async def owned_thread(request: Request, thread_id: str, *, create=False):
        _, identity = account(request)
        thread_id = _thread_id(thread_id)
        client = request.app.state.client
        # The real GraphAccess middleware authenticates this ordinary token and
        # checks current organization, user, conversation ownership and IAM.
        response = await client.get(f"{native_url}/threads/{thread_id}", headers=token(identity))
        if response.is_success:
            return response.json()
        if response.status_code != 404 or not create:
            raise HTTPException(response.status_code, response.json().get("detail", "会话不可访问"))
        with sqlite3.connect(database, timeout=10) as connection:
            connection.execute("INSERT OR IGNORE INTO demo_threads VALUES (?, ?, ?)", (thread_id, identity["org"], identity["user"]))
            owner = connection.execute("SELECT org_id, user_id FROM demo_threads WHERE thread_id = ?", (thread_id,)).fetchone()
            if owner != (identity["org"], identity["user"]):
                raise HTTPException(404, "会话不存在")
        # Use the same authenticated authorization boundary as the mobile
        # Gateway while retaining the existing frontend's requested UUID.
        await authorize_identity(identity, thread_id)
        metadata = {"org_id": identity["org"], "user_id": identity["user"], "conversation_id": thread_id, "source": "isolated_demo_bridge"}
        created = await client.post(f"{native_url}/threads", headers=internal_graph_headers(), json={"thread_id": thread_id, "metadata": metadata, "if_exists": "raise"})
        if created.status_code not in {200, 201, 409}:
            raise HTTPException(created.status_code, "演示会话创建失败")
        # Always recheck through public ownership checks, including create races.
        checked = await client.get(f"{native_url}/threads/{thread_id}", headers=token(identity))
        if not checked.is_success:
            raise HTTPException(checked.status_code, "演示会话归属校验失败")
        return checked.json()

    async def forward(request: Request, url: str, *, headers=None, payload=None):
        client = request.app.state.client
        outgoing = {
            key: value for key, value in request.headers.items()
            if key.lower() not in HOP_HEADERS | {"cookie", "authorization", INTERNAL_GRAPH_HEADER}
        }
        outgoing.update(headers or {})
        content = await request.body() if payload is None else json.dumps(payload).encode()
        upstream = client.build_request(request.method, url, params=request.query_params, headers=outgoing, content=content)
        response = await client.send(upstream, stream=True)
        response_headers = {key: value for key, value in response.headers.items() if key.lower() not in HOP_HEADERS | {"content-encoding", "set-cookie"}}
        return StreamingResponse(response.aiter_bytes(), status_code=response.status_code, headers=response_headers, background=BackgroundTask(response.aclose))

    @app.get("/demo/login", response_class=HTMLResponse)
    async def login_page():
        choices = "".join(f'<button name="identity" value="{name}"><strong>{html.escape(item["label"])}</strong><span>独立测试身份 · 会话与文件隔离</span></button>' for name, item in ACCOUNTS.items())
        return HTMLResponse('''<!doctype html><html lang="zh"><head><meta charset="utf-8"><title>多用户权限与并发演示</title>
<style>
body{margin:0;background:#f4f6f9;font:16px system-ui;color:#182235}
main{width:680px;margin:70px auto;padding:20px}h1{font-size:32px}p{line-height:1.8;color:#526074}
.tag{font-size:13px;letter-spacing:.1em;color:#547066}form{display:flex;flex-wrap:wrap;gap:16px;margin:28px 0}
button{width:324px;box-sizing:border-box;padding:24px;text-align:left;border:1px solid #d8dee8;border-radius:14px;background:white;cursor:pointer;box-shadow:0 3px 12px #18223508}
button:hover{border-color:#617b9c}strong,span{display:block}span{font-size:13px;color:#697589;margin-top:12px}.note{font-size:13px}
</style></head><body><main><div class="tag">隔离演示 · 合成数据 · 确定性任务</div><h1>选择本次演示身份</h1>
<p>每个浏览器环境保持独立身份。进入原有工作台后，可观察会话与文件隔离、两个任务并行、其他任务排队及超限提示。</p>
<form method="post">''' + choices + '''</form><p class="note">任务固定等待一段时间，便于录制运行状态；该等待时间不代表模型速度。演示不调用模型，不访问线上业务数据。</p></main></body></html>''')

    @app.post("/demo/login")
    async def login(request: Request):
        form = await request.form()
        identity = str(form.get("identity", ""))
        if identity not in ACCOUNTS:
            raise HTTPException(400, "未知演示身份")
        cookie = secrets.token_urlsafe(32)
        sessions[cookie] = identity
        response = RedirectResponse("/workspace/chats/new", status_code=303)
        response.set_cookie(COOKIE_NAME, cookie, httponly=True, samesite="lax", path="/")
        return response

    @app.get("/demo/logout")
    async def logout(request: Request):
        sessions.pop(request.cookies.get(COOKIE_NAME, ""), None)
        response = RedirectResponse("/demo/login", status_code=303)
        response.delete_cookie(COOKIE_NAME)
        return response

    @app.get("/demo/status")
    async def status(request: Request):
        name, identity = account(request)
        return {"identity": name, "label": identity["label"], "displayName": identity["label"], "org": identity["org"], "role": "演示普通用户", "synthetic": True, "model_calls": False, "jobs": 2, "max_inflight": 4, "per_user": 1}

    app.add_api_route("/demo/session", status, methods=["GET"])

    def permission_page(identity, thread_id="", results=None):
        cards = ""
        for label, code in results or []:
            color = "#237253" if code == 200 else "#ad483d"
            outcome = "允许访问" if code == 200 else "拒绝访问"
            cards += f'''<section style="padding:18px;border:1px solid #d8dee8;border-radius:10px;margin:14px 0">
<strong>{html.escape(label)}</strong><p style="font-size:22px;color:{color}">HTTP {code} · {outcome}</p></section>'''
        return HTMLResponse(f'''<!doctype html><html lang="zh"><head><meta charset="utf-8"><title>会话权限验证</title>
<style>body{{font:16px system-ui;background:#f4f6f9;color:#182235}}main{{width:760px;margin:70px auto}}
p{{line-height:1.8;color:#526074}}input{{width:70%;padding:13px;border:1px solid #b9c4d2;border-radius:8px}}
button{{padding:14px;border:0;border-radius:8px;background:#24486d;color:white;cursor:pointer}}</style></head><body>
<main><p>隔离演示 ｜ 测试身份 {html.escape(identity['label'])} ｜ 合成数据</p><h1>验证会话与文件访问权限</h1>
<p>输入目标会话编号，使用当前演示身份请求真实会话状态和下载接口。其他用户或组织的资源返回HTTP 404。</p>
<form method="post"><label for="thread_id">目标会话编号</label>
<p><input id="thread_id" name="thread_id" value="{html.escape(thread_id)}" required placeholder="粘贴聊天地址末尾的会话编号">
<button>检查访问权限</button></p></form>{cards}<p><a href="/workspace/chats/new">返回工作台</a> · <a href="/demo/logout">切换测试身份</a></p></main></body></html>''')

    @app.get("/demo/permissions", response_class=HTMLResponse)
    async def permissions_get(request: Request):
        _, identity = account(request)
        return permission_page(identity, request.query_params.get("thread_id", ""))

    @app.post("/demo/permissions", response_class=HTMLResponse)
    async def permissions_post(request: Request):
        _, identity = account(request)
        form = await request.form()
        thread_id = _thread_id(str(form.get("thread_id", "")))
        client = request.app.state.client
        results = []
        for label, path in [("会话状态", f"/threads/{thread_id}/state"), ("结果文件 report.csv", f"/api/threads/{thread_id}/artifacts/mnt/user-data/outputs/report.csv?download=true")]:
            response = await client.get(native_url + path, headers=token(identity))
            results.append((label, response.status_code))
        return permission_page(identity, thread_id, results)

    @app.get("/demo/samples/{name}.csv")
    async def sample(name: str, request: Request):
        account(request)
        if name not in ACCOUNTS:
            raise HTTPException(404)
        return FileResponse(root / "samples" / name / "same.csv", filename="same.csv", media_type="text/csv")

    @app.api_route("/api/langgraph/{path:path}", methods=["GET", "HEAD", "POST", "PATCH", "PUT", "DELETE"])
    async def graph_proxy(path: str, request: Request):
        _, identity = account(request)
        parts = path.strip("/").split("/")
        if request.method == "POST" and parts == ["threads"]:
            payload = await request.json()
            thread_id = payload.get("thread_id") or str(uuid4())
            return JSONResponse(await owned_thread(request, thread_id, create=True))
        if request.method == "POST" and len(parts) in {3, 4} and parts[0] == "threads" and parts[2] == "runs" and (len(parts) == 3 or parts[3] in {"stream", "wait"}):
            thread_id = _thread_id(parts[1])
            await owned_thread(request, thread_id)
            payload = await request.json()
            # Re-authorize the operation through the configured fake IAM; its
            # contract mirrors the same server-side boundary as mobile input.
            principal = await authorize_identity(identity, thread_id)
            raw_input = payload.get("input") if isinstance(payload, dict) else None
            messages = raw_input.get("messages") if isinstance(raw_input, dict) else None
            if not isinstance(messages, list) or not messages:
                raise HTTPException(422, "演示任务需要用户消息")
            human = messages[-1]
            if not isinstance(human, dict) or human.get("type", human.get("role")) not in {"human", "user"}:
                raise HTTPException(422, "只接受用户文本消息")
            content = human.get("content", "")
            if isinstance(content, list):
                content = "\n".join(block["text"] for block in content if isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str))
            if not isinstance(content, str):
                raise HTTPException(422, "用户消息必须为文本")
            safe_files = []
            additional = human.get("additional_kwargs")
            raw_files = additional.get("files") if isinstance(additional, dict) else None
            for file in raw_files if isinstance(raw_files, list) else []:
                filename = file.get("filename") if isinstance(file, dict) else None
                if isinstance(filename, str) and filename and Path(filename).name == filename:
                    uploaded_file = get_paths().sandbox_uploads_dir(thread_id) / filename
                    if uploaded_file.is_file():
                        safe_files.append({"filename": filename, "path": f"/mnt/user-data/uploads/{filename}", "size": uploaded_file.stat().st_size, "status": "uploaded"})
            safe_message = {"type": "human", "content": content[:12000], "additional_kwargs": {"files": safe_files} if safe_files else {}}
            safe_payload = {
                "assistant_id": "lead_agent", "input": {"messages": [safe_message]},
                "stream_mode": ["values", "updates"], "stream_subgraphs": False,
                "on_disconnect": "continue", "multitask_strategy": "reject",
                "metadata": build_admission_metadata(user_id=principal.user_id, org_id=principal.org_id, identity_class=principal.identity_class, task_type="analysis", thread_id=thread_id),
                "context": {"thread_id": thread_id, "memory_scope": build_memory_scope_context(principal.org_id, principal.user_id, thread_id)},
            }
            return await forward(request, f"{native_url}/{path}", headers=internal_graph_headers(), payload=safe_payload)
        return await forward(request, f"{native_url}/{path}", headers=token(identity))

    @app.api_route("/api/{path:path}", methods=["GET", "HEAD", "POST", "PATCH", "PUT", "DELETE"])
    async def gateway_proxy(path: str, request: Request):
        _, identity = account(request)
        parts = path.strip("/").split("/")
        if request.method == "POST" and len(parts) == 3 and parts[0] == "threads" and parts[2] == "uploads":
            await owned_thread(request, parts[1], create=True)
        return await forward(request, f"{native_url}/api/{path}", headers=token(identity))

    @app.api_route("/{path:path}", methods=["GET", "HEAD", "POST"])
    async def frontend_proxy(path: str, request: Request):
        try:
            _, identity = account(request)
        except HTTPException:
            return RedirectResponse("/demo/login", status_code=303)
        if frontend_url is None:
            return HTMLResponse("<h1>隔离演示后端已就绪</h1><p>请提供 --frontend-url 指向新启动的演示前端。</p><a href='/demo/login'>切换测试身份</a>")
        client = request.app.state.client
        response = await client.request(request.method, f"{frontend_url}/{path}", params=request.query_params, headers={key: value for key, value in request.headers.items() if key.lower() not in HOP_HEADERS | {"cookie", "authorization", INTERNAL_GRAPH_HEADER}}, content=await request.body())
        response_headers = {key: value for key, value in response.headers.items() if key.lower() not in HOP_HEADERS | {"content-encoding", "set-cookie"}}
        content = response.content
        if "text/html" in response.headers.get("content-type", ""):
            banner = f'''<div style="position:fixed;top:0;left:0;right:0;z-index:2147483647;height:30px;background:#122239;color:white;font:12px/30px system-ui;text-align:center">
隔离演示 ｜ 测试身份 {html.escape(identity['label'])} ｜ 合成CSV ｜ 确定性任务（无模型）
<a href="/demo/permissions" style="color:#b9d8ff;margin-left:20px">权限验证</a><a href="/demo/logout" style="color:#b9d8ff;margin-left:20px">切换身份</a></div>'''
            content = content.replace(b"</body>", banner.encode() + b"</body>")
        return HTMLResponse(content, status_code=response.status_code, headers=response_headers) if "text/html" in response.headers.get("content-type", "") else Response(content, status_code=response.status_code, headers=response_headers)

    return app


def main():
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frontend-url", help="A separately started local frontend, e.g. http://127.0.0.1:3331")
    parser.add_argument("--port", type=int, default=0, help="Loopback proxy port; 0 chooses an unused port")
    parser.add_argument("--delay", type=float, default=18, help="Deterministic task delay, in seconds")
    args = parser.parse_args()
    if args.frontend_url:
        target = urlparse(args.frontend_url)
        if target.scheme != "http" or target.hostname not in {"127.0.0.1", "localhost"}:
            parser.error("--frontend-url must be a local HTTP frontend")
    if not 0 <= args.port <= 65535 or not 0.01 <= args.delay <= 120:
        parser.error("Invalid port or delay")
    backend = Path(__file__).resolve().parents[1] / "backend"
    sys.path[:0] = [str(backend), str(backend / "packages" / "harness")]
    cli = shutil.which("langgraph") or str(Path(sys.executable).with_name("langgraph"))
    public_port = args.port or _free_port()
    native_port = _free_port()
    with tempfile.TemporaryDirectory(prefix="deerflow-recording-demo-") as temporary:
        root = Path(temporary)
        for name, text in [("graph.py", GRAPH), ("fixture_iam.py", IAM), ("webapp.py", NATIVE_WEBAPP), ("extensions.json", "{}"), ("control.json", "[]")]:
            (root / name).write_text(text)
        for name, identity in ACCOUNTS.items():
            sample = root / "samples" / name / "same.csv"
            sample.parent.mkdir(parents=True)
            sample.write_text(f"road,flow,speed\n道路A,{identity['total'] // 4},20\n道路B,{identity['total'] * 3 // 4},30\n", encoding="utf-8-sig")
        (root / "config.yaml").write_text("config_version: 1\nsandbox:\n  use: deerflow.sandbox.local:LocalSandboxProvider\nmemory:\n  enabled: false\ncompliance:\n  enabled: false\n")
        (root / "langgraph.json").write_text(json.dumps({"dependencies": [str(backend)], "graphs": {"lead_agent": f"{root / 'graph.py'}:graph"}, "http": {"app": f"{root / 'webapp.py'}:app"}}))
        env = {
            **os.environ, "DEER_FLOW_HOME": str(root / "data"), "DEER_FLOW_CONFIG_PATH": str(root / "config.yaml"),
            "DEER_FLOW_EXTENSIONS_CONFIG_PATH": str(root / "extensions.json"), "DEER_FLOW_ADMISSION_SIGNING_KEY": secrets.token_hex(32),
            "MOBILE_IAM_PROVIDER": "fixture_iam:Provider", "MOBILE_IAM_REQUIRED": "true", "FIXTURE_CONTROL": str(root / "control.json"),
            "LANGGRAPH_URL": f"http://127.0.0.1:{native_port}", "MOBILE_ADAPTER_LANGGRAPH_URL": f"http://127.0.0.1:{native_port}", "MOBILE_ADAPTER_ASSISTANT_ID": "lead_agent",
            "DEER_FLOW_MAX_INFLIGHT_RUNS": "4", "DEER_FLOW_MAX_INFLIGHT_PER_USER": "1", "DEER_FLOW_REQUIRE_SIGNED_ADMISSION": "true",
            "DEER_FLOW_IDENTITY_CLASS_LIMITS": "{}", "DEER_FLOW_TASK_TYPE_LIMITS": "{}", "DEMO_TASK_DELAY": str(args.delay),
            "LANGSMITH_TRACING": "false", "LANGCHAIN_TRACING_V2": "false", "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": os.pathsep.join([str(root), str(backend), str(backend / "packages" / "harness"), os.environ.get("PYTHONPATH", "")]),
        }
        # The proxy imports the exact same signing key and temporary IAM module;
        # these values never leave this process or its native child.
        os.environ.update({key: value for key, value in env.items() if key.startswith(("DEER_FLOW_", "MOBILE_", "FIXTURE_", "LANGGRAPH_", "DEMO_"))})
        sys.path.insert(0, str(root))
        with (root / "runtime.log").open("w+") as log:
            process = subprocess.Popen([cli, "dev", "--config", str(root / "langgraph.json"), "--host", "127.0.0.1", "--port", str(native_port), "--no-browser", "--no-reload", "--allow-blocking", "--n-jobs-per-worker", "2"], cwd=root, env=env, stdout=log, stderr=log)
            try:
                with httpx.Client(trust_env=False) as client:
                    for _ in range(150):
                        if process.poll() is not None:
                            raise RuntimeError("Temporary native runtime failed to start")
                        try:
                            if client.get(f"http://127.0.0.1:{native_port}/ok").is_success:
                                break
                        except httpx.TransportError:
                            pass
                        time.sleep(0.2)
                    else:
                        raise TimeoutError("Temporary native runtime startup timed out")
                app = create_demo_app(native_url=f"http://127.0.0.1:{native_port}", frontend_url=args.frontend_url, root=root)
                print(json.dumps({"base_url": f"http://127.0.0.1:{public_port}"}), flush=True)
                uvicorn.run(app, host="127.0.0.1", port=public_port, log_level="warning", access_log=False)
            except KeyboardInterrupt:
                pass
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    main()
