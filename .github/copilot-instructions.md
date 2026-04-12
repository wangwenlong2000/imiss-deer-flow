# Copilot Instructions for DeerFlow

Use this file as the primary guide. Only search the codebase when this file is incomplete or incorrect.

## Repository Summary

DeerFlow is a full-stack AI super agent harness.

- **LangGraph Server** (port 2024): Agent runtime and graph execution
- **Gateway API** (port 8001): FastAPI REST API for models, MCP, skills, memory, artifacts, uploads
- **Frontend** (port 3000): Next.js 16 + React 19 + TypeScript chat interface
- **Nginx** (port 2026): Unified reverse proxy (`/api/langgraph/*` → LangGraph; all other `/api/*` → Gateway)
- Docker dev entrypoint: `make docker-*` (mode-aware from `config.yaml`)

## Toolchain Requirements

- Node.js `>=22`, pnpm `>=10`
- Python `>=3.12`, `uv`
- `nginx` (required for `make dev` unified local endpoint)

Always run from repo root unless a command explicitly says otherwise.

## Commands

### Installation

```bash
make check    # Verify prerequisites
make install  # Install all dependencies (backend + frontend)
```

### Running Locally

```bash
make dev      # Start all four services (LangGraph + Gateway + Frontend + Nginx)
make stop     # Stop all services
```

Logs: `logs/langgraph.log`, `logs/gateway.log`, `logs/frontend.log`, `logs/nginx.log`

### Backend (run from `backend/`)

```bash
make lint               # ruff check .
make format             # ruff check . --fix && ruff format .
make test               # Run full test suite (PYTHONPATH=. uv run pytest tests/ -v)
make dev                # LangGraph server only (port 2024)
make gateway            # Gateway API only (port 8001)
```

Run a single test or test file:
```bash
cd backend
PYTHONPATH=. uv run pytest tests/test_model_factory.py -v
PYTHONPATH=. uv run pytest tests/test_model_factory.py::test_uses_first_model_when_name_is_none -v
```

### Frontend (run from `frontend/`)

```bash
pnpm lint        # ESLint
pnpm lint:fix    # ESLint with auto-fix
pnpm typecheck   # tsc --noEmit
pnpm build       # Production build (requires BETTER_AUTH_SECRET)
pnpm dev         # Dev server with Turbopack (port 3000)
```

> `pnpm build` fails without `BETTER_AUTH_SECRET`. Use `BETTER_AUTH_SECRET=local-dev-secret pnpm build` or `SKIP_ENV_VALIDATION=1`.
> Do not use `pnpm check` — it currently fails due to a Next.js lint directory issue. Use `pnpm lint` + `pnpm typecheck` instead.

### First-Time Config

```bash
make config   # Bootstrap config.yaml from config.example.yaml (non-idempotent — aborts if file exists)
```

## Architecture

### Backend: Harness / App Split

The backend has a strict dependency boundary:

- **Harness** (`backend/packages/harness/deerflow/`): Publishable package (`deerflow-harness`), imported as `deerflow.*`. Contains agent orchestration, tools, sandbox, models, MCP, skills, config.
- **App** (`backend/app/`): Unpublished application layer, imported as `app.*`. Contains FastAPI Gateway and IM channel integrations (Feishu, Slack, Telegram).

**Rule**: `app.*` may import `deerflow.*`, but `deerflow.*` must never import `app.*`. This is enforced in CI by `tests/test_harness_boundary.py`.

### Agent System

**Entry point**: `deerflow.agents:make_lead_agent` (registered in `backend/langgraph.json`)

**ThreadState** (`deerflow/agents/thread_state.py`): Extends LangGraph `AgentState` with `sandbox`, `thread_data`, `title`, `artifacts`, `todos`, `uploaded_files`, `viewed_images`.

**Tool assembly** (`get_available_tools()`): Combines sandbox tools + built-ins + MCP tools + community tools + subagent tool, controlled by `config.yaml`.

**Middleware chain** (executes in strict order in `lead_agent/agent.py`):
1. `ThreadDataMiddleware` — Creates per-thread directories
2. `UploadsMiddleware` — Tracks newly uploaded files
3. `SandboxMiddleware` — Acquires sandbox, stores `sandbox_id`
4. `DanglingToolCallMiddleware` — Fixes orphaned tool calls after interrupts
5. `SummarizationMiddleware` — Context reduction near token limits
6. `TodoListMiddleware` — Task tracking (plan mode only)
7. `TitleMiddleware` — Auto-generates thread title
8. `MemoryMiddleware` — Queues conversations for async memory update
9. `ViewImageMiddleware` — Injects base64 image data for vision models
10. `SubagentLimitMiddleware` — Enforces `MAX_CONCURRENT_SUBAGENTS=3`
11. `ClarificationMiddleware` — Intercepts `ask_clarification`, interrupts via `Command(goto=END)` (must be last)

### Sandbox System

- **Virtual paths** (agent sees): `/mnt/user-data/{workspace,uploads,outputs}`, `/mnt/skills`
- **Physical paths**: `backend/.deer-flow/threads/{thread_id}/user-data/...`
- **Tools**: `bash`, `ls`, `read_file`, `write_file`, `str_replace`
- **Providers**: `LocalSandboxProvider` (default), `AioSandboxProvider` (Docker-based)

### Configuration

- **`config.yaml`** (project root): Main config. Values starting with `$` resolve as env vars. Run `make config-upgrade` after schema changes.
- **`extensions_config.json`** (project root): MCP servers and skills.
- Config discovery order: explicit path → `DEER_FLOW_CONFIG_PATH` env var → `config.yaml` in `backend/` → `config.yaml` in project root (recommended).

### Gateway API Routers (`app/gateway/routers/`)

| Route prefix | Purpose |
|---|---|
| `/api/models` | List/get LLM models |
| `/api/mcp` | Get/update MCP config |
| `/api/skills` | List/install/update skills |
| `/api/memory` | User memory CRUD |
| `/api/threads/{id}/uploads` | File uploads (auto-converts PDF/PPT/Excel/Word) |
| `/api/threads/{id}/artifacts` | Serve artifact files |
| `/api/threads/{id}/suggestions` | Generate follow-up questions |

### Frontend Architecture

**Data flow**: User input → thread hooks (`core/threads/hooks.ts`) → LangGraph SDK streaming → thread state → React render.

**State management**: TanStack Query for server state + React hooks (`useState`/`useEffect`) + localStorage for user settings. No Redux or Zustand.

**Key hooks** (primary backend interface):
- `useThreadStream()` — Consumes streaming agent responses
- `useSubmitThread()` — Sends messages
- `useThreads()` — Fetches thread list

**LangGraph client**: Singleton from `getAPIClient()` in `core/api/`. Connects to LangGraph Server via `NEXT_PUBLIC_LANGGRAPH_BASE_URL` (default proxied through nginx).

**Component rules**:
- Server Components by default; add `"use client"` only for interactive components.
- `ui/` and `ai-elements/` are auto-generated from registries (Shadcn, MagicUI, Vercel AI SDK) — do not manually edit these.
- Use `cn()` from `@/lib/utils` for conditional Tailwind class names.

**Environment validation**: `src/env.js` uses `@t3-oss/env-nextjs` + Zod. Skip with `SKIP_ENV_VALIDATION=1`.

## Key Conventions

### Backend (Python)

- **Lint/format**: `ruff` — 240-char line length, double quotes, `["E", "F", "I", "UP"]` rules.
- **Imports**: Sorted by ruff/isort. First-party: `deerflow`, `app`.
- **Reflection**: Use `resolve_variable("module.path:variable_name")` / `resolve_class()` for dynamic loading from config.
- **Model instantiation**: Always via `create_chat_model(name, thinking_enabled)` — never instantiate LLM classes directly.
- **Tests**: `pytest`, `unittest.mock`. `PYTHONPATH=.` required. `conftest.py` pre-mocks modules that cause circular imports.
- **Harness boundary**: Never add `from app.*` imports inside `packages/harness/deerflow/`.

### Frontend (TypeScript)

- **Path alias**: `@/*` maps to `src/*`.
- **Imports**: Enforced ordering (builtin → external → internal → parent → sibling), alphabetized, newline-separated groups.
- **Type imports**: Use inline form — `import { type Foo }` not `import type { Foo }`.
- **Unused variables**: Prefix with `_`.
- **Internationalization**: All user-visible strings go through `core/i18n/` (en-US, zh-CN supported).

## Pre-Checkin Checklist

1. `cd backend && make lint && make test`
2. `cd frontend && pnpm lint && pnpm typecheck`
3. If changing env/auth/routing: `BETTER_AUTH_SECRET=local-dev-secret pnpm build`
4. If changing `Makefile`, `docker/*`, or `config*.yaml`: `make dev` and verify all four services start.

## Gotchas

- `make config` is non-idempotent — it aborts if `config.yaml` already exists.
- Proxy env vars (`HTTP_PROXY`, etc.) can silently break `pnpm install` and registry access.
- `make dev` emits shutdown noise when interrupted — run `make stop` to ensure cleanup.
- Bumping `config_version` in `config.example.yaml` is required when changing the config schema.
- `backend/.deer-flow/` (thread data, memory) is gitignored runtime state — don't commit it.
