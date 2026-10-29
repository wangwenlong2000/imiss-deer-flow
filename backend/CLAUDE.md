# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The published example enables only Qwen3.6-35B-A3B via QWEN_BASE_URL and
QWEN_API_KEY environment references. Do not publish populated runtime configs,
backups, or raw provider traces; the release note documents local-only evidence.

DeerFlow is a LangGraph-based AI super agent system with a full-stack architecture. The backend provides a "super agent" with sandbox execution, persistent memory, subagent delegation, and extensible tool integration - all operating in per-thread isolated environments.

**Architecture**:
- **LangGraph Server** (port 2024): Agent runtime and workflow execution
- **Gateway API** (port 8001): REST API for models, MCP, skills, memory, artifacts, and uploads
- **Frontend** (port 3000): Next.js web interface
- **Nginx** (port 2026): Unified reverse proxy entry point
- **Provisioner** (port 8002, optional in Docker dev): Started only when sandbox is configured for provisioner/Kubernetes mode

**Project Structure**:
```
deer-flow/
├── Makefile                    # Root commands (check, install, dev, stop)
├── config.yaml                 # Main application configuration
├── extensions_config.json      # MCP servers and skills configuration
├── backend/                    # Backend application (this directory)
│   ├── Makefile               # Backend-only commands (dev, gateway, lint)
│   ├── langgraph.json         # LangGraph server configuration
│   ├── packages/
│   │   └── harness/           # deerflow-harness package (import: deerflow.*)
│   │       ├── pyproject.toml
│   │       └── deerflow/
│   │           ├── agents/            # LangGraph agent system
│   │           │   ├── lead_agent/    # Main agent (factory + system prompt)
│   │           │   ├── middlewares/   # 10 middleware components
│   │           │   ├── memory/        # Memory extraction, queue, prompts
│   │           │   └── thread_state.py # ThreadState schema
│   │           ├── sandbox/           # Sandbox execution system
│   │           │   ├── local/         # Local filesystem provider
│   │           │   ├── sandbox.py     # Abstract Sandbox interface
│   │           │   ├── tools.py       # bash, ls, read/write/str_replace
│   │           │   └── middleware.py  # Sandbox lifecycle management
│   │           ├── subagents/         # Subagent delegation system
│   │           │   ├── builtins/      # general-purpose, bash agents
│   │           │   ├── executor.py    # Background execution engine
│   │           │   └── registry.py    # Agent registry
│   │           ├── tools/builtins/    # Built-in tools (present_files, ask_clarification, view_image)
│   │           ├── mcp/               # MCP integration (tools, cache, client)
│   │           ├── models/            # Model factory with thinking/vision support
│   │           ├── skills/            # Skills discovery, loading, parsing
│   │           ├── config/            # Configuration system (app, model, sandbox, tool, etc.)
│   │           ├── community/         # Community tools (tavily, jina_ai, firecrawl, image_search, aio_sandbox)
│   │           ├── reflection/        # Dynamic module loading (resolve_variable, resolve_class)
│   │           ├── utils/             # Utilities (network, readability)
│   │           └── client.py          # Embedded Python client (DeerFlowClient)
│   ├── app/                   # Application layer (import: app.*)
│   │   ├── gateway/           # FastAPI Gateway API
│   │   │   ├── app.py         # FastAPI application
│   │   │   └── routers/       # 6 route modules
│   │   └── channels/          # IM platform integrations
│   ├── tests/                 # Test suite
│   └── docs/                  # Documentation
├── frontend/                   # Next.js frontend application
└── skills/                     # Agent skills directory
    ├── public/                # Public skills (committed)
    └── custom/                # Custom skills (gitignored)
```

## Important Development Guidelines

### Documentation Update Policy
**CRITICAL: Always update README.md and CLAUDE.md after every code change**

When making code changes, you MUST update the relevant documentation:
- Update `README.md` for user-facing changes (features, setup, usage instructions)
- Update `CLAUDE.md` for development changes (architecture, commands, workflows, internal systems)
- Keep documentation synchronized with the codebase at all times
- Ensure accuracy and timeliness of all documentation

## Commands

**Root directory** (for full application):
```bash
make check      # Check system requirements
make install    # Install all dependencies (frontend + backend)
make dev        # Start all services (LangGraph + Gateway + Frontend + Nginx), with config.yaml preflight
make stop       # Stop all services
```

**Backend directory** (for backend development only):
```bash
make install    # Install backend dependencies
make dev        # Run LangGraph server only (port 2024)
make gateway    # Run Gateway API only (port 8001)
make test       # Run all backend tests
make lint       # Lint with ruff
make format     # Format code with ruff
```

Regression tests related to Docker/provisioner behavior:
- `tests/test_docker_sandbox_mode_detection.py` (mode detection from `config.yaml`)
- `tests/test_provisioner_kubeconfig.py` (kubeconfig file/directory handling)

Boundary check (harness → app import firewall):
- `tests/test_harness_boundary.py` — ensures `packages/harness/deerflow/` never imports from `app.*`

Compliance detection regression tests:
- `tests/test_compliance_decoupling.py` — AST scan: the engine core must never import a concrete detector
- `tests/test_compliance_row_builder.py` — the `content_text` rule reproduces all 282 annotated rows with zero prediction drift
- `tests/test_compliance_policy_matrix.py` — matrix completeness; the three baseline violation types refuse in every scene
- `tests/test_compliance_gates.py` — the three gate mount positions and `fail_mode` semantics
- `tests/test_compliance_output_gate_isolation.py` — position-independent proof that no downstream middleware can observe an unsanitized answer
- `tests/test_compliance_preflight.py` — a missing model/config must degrade to "gates off, logged CRITICAL", never to "refuse every request"

CI runs these regression tests for every pull request via [.github/workflows/backend-unit-tests.yml](../.github/workflows/backend-unit-tests.yml).

## Architecture

`app/graph_http.py` is the custom HTTP app loaded by `langgraph.json`. Its
single-process admission middleware uses the installed runtime's `Runs.stats`
to bound pending + running runs (`DEER_FLOW_MAX_INFLIGHT_RUNS`, default 10).
The native worker queue still controls execution (`DEER_FLOW_GRAPH_JOBS`,
default 2 in startup commands). The creation lock ends at response headers,
after native persistence, not after SSE completion. Reads, cancellation and GET
stream joins bypass admission; stateful/stateless create, wait, stream and batch
submissions are checked. Do not replace this with an HTTP connection counter:
background runs may outlive disconnects. This is a single graph-server demo
limit, not distributed quota enforcement or a limit on internal tool/subagent
fan-out. Cron scheduling is outside this admission path. Run
`python ../scripts/verify_concurrency_runtime.py` using the backend environment
for an isolated real-runtime queue/CSV-file integration check without LLM calls.
For an opt-in live business check, run
`python ../scripts/verify_concurrency_business.py --base-url http://localhost:3328 --output /tmp/concurrency-business-new-run`.
It creates three synthetic CSV conversations through the original model/tools
and validates final tables, downloads and pending/running states. It consumes
model requests; keep its raw SSE/state private and publish only sanitized
metrics and synthetic CSVs. The real three-conversation validation and follow-up
results are documented in `docs/concurrency-implementation-20261029.md` at repo root.

`routing/turn_requirements.py` defines the shared per-turn contract consumed by
structured recognition, dialogue resume, the completion guard and CSV delivery.
Do not inherit computation/export requirements unchanged on explanation turns.
The guard uses auto tool choice and one corrective retry; forced tool choices
are incompatible with the deployed provider's thinking mode. `verified_deliveries`
is a mergeable state channel keyed by virtual path; source-message keys and file
hashes prevent past or subsequently modified files satisfying the current turn.
Per-file validation permits partial metric coverage; the terminal guard enforces
aggregate coverage and requested file types. Metadata `execution_status` marks
incomplete/failed answers, and the mobile adapter gives SSE errors precedence
over any final-answer placeholder. Code-layout grounding accepts only whitespace
normalization, retaining all literal tokens. Exhausted recognition validation
fails explicitly rather than executing a deterministic subset of the request.
Corrective retries remove the unverified AI draft instead of leaving a progress
placeholder in model history. Ephemeral turn guidance includes effective scene
parameters, so explanation turns retain the latest filter scope. Mobile replay
honors removals and canonical state snapshots. ToolErrorHandling prechecks missing
model-visible JSON fields with concise feedback before execution; bash metadata
and executable code remain separate, and existing sandbox path checks still run.
For a validated missing-scene clarification, the guard returns a normal
ask_clarification ModelResponse when that tool is available. Outer model
wrappers, after-model persistence and the existing tool/ContextGate/pending-action
path still run. Do not introduce an unconditional before-model jump to tools:
agents with an empty tool list have no such graph node.
Explicit literal export cancellations are normalized at recognition and resume
boundaries, so a stale generated file requirement cannot override a user's
no-file request. A CSV-only cancellation preserves other requested types; mixed
global bans and positive exceptions remain with semantic interpretation. Exact
CSV metric/header identity is accepted as verified mapping evidence; aliases
still need an explicit mapping, and precision remains mandatory.
The completion guard snapshots output file versions at turn start. If a model
omits required delivery, it may schedule one ordinary present_files call for
new/changed files of the requested types. This happens after model wrappers;
refusals and failures are preserved. Tool availability comes from the actual
model request, and the existing tool/compliance/CSV checks still run. Unchanged
historical files cannot satisfy this automatic path. Never auto-deliver before
required current-turn computation evidence exists.
If recognition omitted the file type, a final draft referencing an actual
new/changed output can still trigger that same validated presentation path.
Explicit export cancellations apply, and failed presentation remains a missing
delivery at completion. Merely mentioning a nonexistent or unchanged file does
not trigger automatic presentation.
When identical presentation succeeds twice and all current computation/delivery
checks pass, the next model request has no tools and must produce the final
answer. Do not use this for failed/incomplete work. A run key and message
boundary prevent old deliveries or tool results being reused when a client
retains a user-message ID on retry/edit.

`AnswerConsistencyMiddleware` checks explicit entity/metric comparison clauses
against current verified CSV records with Decimal arithmetic. It adds no model
calls. A shared predicate for coordinated metrics is checked for each metric;
ambiguous metric aliases, duplicate row identities, stale/modified files and
unestablished facts cannot create a contradiction. Scans are bounded to small
result tables (at most 100 rows). It supplies evidence to the completion guard's
existing one-time correction and remains inside the outer OutputGate wrapper.
This is a targeted numeric check, not general semantic proof or intent routing.
The rejected LLM-judge experiments are retained under the evaluation docs only.

CSV presentation validates strict quoting, nonempty unique headers and fixed
record widths even without per-metric delivery checks. Do not silently reshape
bad CSV files. The IllegalPoliticalV4 adapter disambiguates explicit arithmetic,
variable nonzero and syntax/format validation while preserving other rule hits;
the frozen legacy core and dataset remain unchanged. The stay-point script
adds a CSV representation of its existing event records, preserving JSONL and
algorithm/count behavior; skill docs require that detail file for CSV requests.
Recognition gives explicitly listed current metrics/output goals precedence
over historical targets; reusing a file is not an instruction to retain all old
metrics or CSV requirements. Same-metric identifiers remain stable when the
user explicitly preserves them. A CSV can be a
detail table containing no global summary metric; completion still checks the
aggregate verified metric union. Do not remove that final coverage check.

`ComputationGuardMiddleware` runs before other after-model persistence hooks. The
existing structured intent call emits `requires_computation` and
`required_csv_metrics`. A terminal answer lacking current-turn execution evidence
is replaced and retried once via a declared model jump; a second unverified
answer becomes an explicit inability-to-verify response. Earlier-turn tool results
cannot satisfy it. No judge model is added. CSV delivery checks map requested
metric labels to existing columns. Corrective requests use auto tool choice
with either thinking setting; evidence is still required. Delivery
checks normalize singleton objects as well as arrays at the tool-schema boundary;
exact metric labels and computed-column precision instructions stay outside
user-visible hidden steps. These guards establish execution/format
evidence, not complete semantic correctness or frontend token retraction.

### Harness / App Split

The backend is split into two layers with a strict dependency direction:

- **Harness** (`packages/harness/deerflow/`): Publishable agent framework package (`deerflow-harness`). Import prefix: `deerflow.*`. Contains agent orchestration, tools, sandbox, models, MCP, skills, config — everything needed to build and run agents.
- **App** (`app/`): Unpublished application code. Import prefix: `app.*`. Contains the FastAPI Gateway API and IM channel integrations (Feishu, Slack, Telegram).

**Dependency rule**: App imports deerflow, but deerflow never imports app. This boundary is enforced by `tests/test_harness_boundary.py` which runs in CI.

**Import conventions**:
```python
# Harness internal
from deerflow.agents import make_lead_agent
from deerflow.models import create_chat_model

# App internal
from app.gateway.app import app
from app.channels.service import start_channel_service

# App → Harness (allowed)
from deerflow.config import get_app_config

# Harness → App (FORBIDDEN — enforced by test_harness_boundary.py)
# from app.gateway.routers.uploads import ...  # ← will fail CI
```

### Agent System

**Lead Agent** (`packages/harness/deerflow/agents/lead_agent/agent.py`):
- Entry point: `make_lead_agent(config: RunnableConfig)` registered in `langgraph.json`
- Dynamic model selection via `create_chat_model()` with thinking/vision support
- Tools loaded via `get_available_tools()` - combines sandbox, built-in, MCP, community, and subagent tools
- System prompt generated by `apply_prompt_template()` with skills, memory, and subagent instructions

**ThreadState** (`packages/harness/deerflow/agents/thread_state.py`):
- Extends `AgentState` with: `sandbox`, `thread_data`, `title`, `artifacts`, `todos`, `uploaded_files`, `viewed_images`
- Uses custom reducers: `merge_artifacts` (deduplicate), `merge_viewed_images` (merge/clear)

**Runtime Configuration** (via `config.configurable`):
- `thinking_enabled` - Enable model's extended thinking
- `model_name` - Select specific LLM model
- `is_plan_mode` - Enable TodoList middleware
- `subagent_enabled` - Enable task delegation tool

Deployment model policy: local `config.yaml` registers only `qwen3.6-35b-a3b`.
Stage policy: `IntentRecognitionMiddleware` always creates a non-thinking model,
including its dialogue-turn classifier. Lead business execution defaults to
thinking and respects an explicit false request. The task tool forwards the
effective `metadata.thinking_enabled` (then configurable/default true) to
`SubagentExecutor`, which checks the selected model's capability before enabling
thinking. Non-thinking helpers (titles, suggestions, memory) remain unchanged.
Tests for this split are in the model-factory, lead-agent, intent-middleware,
task-tool and subagent-executor regression modules. Prior full-flow fixtures
explicitly requested false; their timing must not be presented as thinking-on
performance. `config.yaml` already has a 2048-token budget and was not changed.
Resolve request/custom-agent model names through `_resolve_model_name()` so stale
selections fall back to that default. The model factory must explicitly translate
`thinking_enabled=False` to Qwen's `extra_body.enable_thinking=false` (DashScope)
or `extra_body.chat_template_kwargs.enable_thinking=false` (local vLLM/SGLang).
Regression coverage is in `tests/test_model_factory.py` and
`tests/test_lead_agent_model_resolution.py`. See
`../docs/intent-recognition-latency-analysis-20261022.md` for measured log evidence,
remaining sequential routing calls, and local deployment configuration.

The `deerflow.routing.intent` public LLM entry points now use `intent/structured.py`:
one bounded Qwen call returns scene tasks, parameters, and `planning_steps`.
Offer all enabled scenes without keyword filtering before or after generation.
Validate the scene allowlist, parameter names, and literal evidence spans from
actual input. Support quote arrays and locally recover verifiable fragments
separated by ellipses or sentence boundaries; reject the entire result if any
fragment is unsupported. Evidence provenance does not establish semantic correctness.
Allow one schema repair, then record `structured_fallback_*` with
`recognition_status=failed` and block business execution; never execute a
keyword-selected subset after losing the validated task list.
Compact excess plan steps locally rather than making a formatting-only retry.
Pass only bounded filename/type metadata and prior `original_query`; never prior
model-generated routing text. `clarification_question` is forwarded through todo
guidance to the execution agent; `confidence` is an uncalibrated model self-report,
not a keyword score or a probability threshold for routing.
Keep `planning_steps` intact because the mobile adapter publishes it as the
OneCityAgent planner segment. Dialogue controls still run first; the async
middleware offloads the synchronous dialogue classifier using `asyncio.to_thread`.
The multi-call implementation in `classifier.py` remains available explicitly
for baseline evaluation and rollback, alongside shared deterministic helpers.
Use `tests/test_structured_intent.py`, `tests/test_eval_intent_recognition.py`,
and `scripts/eval_intent_recognition.py`; full results and reproduction commands
are in `../docs/intent-recognition-optimization-20261022.md`.

### Middleware Chain

Middlewares execute in strict order in `packages/harness/deerflow/agents/lead_agent/agent.py`:

1. **ThreadDataMiddleware** - Creates per-thread directories (`backend/.deer-flow/threads/{thread_id}/user-data/{workspace,uploads,outputs}`)
2. **UploadsMiddleware** - Tracks and injects newly uploaded files into conversation
3. **SandboxMiddleware** - Acquires sandbox, stores `sandbox_id` in state
4. **DanglingToolCallMiddleware** - Injects placeholder ToolMessages for AIMessage tool_calls that lack responses (e.g., due to user interruption)
5. **SummarizationMiddleware** - Context reduction when approaching token limits (optional, if enabled)
6. **TodoListMiddleware** - Task tracking with `write_todos` tool (optional, if plan_mode)
7. **TitleMiddleware** - Auto-generates thread title after first complete exchange
8. **MemoryMiddleware** - Queues conversations for async memory update (filters to user + final AI responses)
9. **ViewImageMiddleware** - Injects base64 image data before LLM call (conditional on vision support)
10. **SubagentLimitMiddleware** - Truncates excess `task` tool calls from model response to enforce `MAX_CONCURRENT_SUBAGENTS` limit (optional, if subagent_enabled)
11. **ClarificationMiddleware** - Intercepts `ask_clarification` tool calls, interrupts via `Command(goto=END)` (must be last)

**Intent scene filtering** treats `backend/config/intent_scene_templates.json`
as the authoritative ten-scene registry. Packaged demo scenes are not merged into
this deployment. Explicit template paths remain available for isolated library
tests. Scene skill filtering also rejects stale out-of-registry scene labels.
`skill_router.enabled: false` is
intentional in this deployment: `SceneSkillFilterMiddleware` filters custom
skills by the recognized scene without calling the embedding/reranker
services. `domain_anchors`, `weak_domain_anchors`, `weak_anchor_actions`, and
per-scene scoring thresholds apply only to the deterministic degraded path and
legacy evaluation, not the structured semantic decision. Do not reinstate
keyword vetoes to fix individual examples. Keep negation, background domains,
missing-object clarification, and multi-turn topic shifts in regression tests.
See `../docs/intent-semantic-routing-20261022.md` for primary-source research,
frozen ten-scene evaluation cases, measured limits and rollback instructions.

Full-flow regression fixes (`tests/test_full_flow_regressions.py`):
- Unknown-scene clarification replies reclassify actual prior user input plus the
  current reply/selected option. Preserve the raw current query/source id and a
  bounded `_confirmed_task_query` for subsequent reference resolution. Known-scene
  parameter replies retain their existing shortcut; cancellation is not reclassified.
- UploadsMiddleware stores current and historical metadata, clearing stale entries
  after deletion. Never inject attachment bodies into intent recognition.
  Intent input may also include the last six actual user texts (700 chars each),
  excluding internal guidance and upload blocks, to resolve a file revisit after
  a topic switch. Do not supply model-generated summaries as evidence.
- `invoke_skill` accepts and locally validates artifact array strings, and excludes
  internal guidance from its fallback raw user query.
- `present_files` supports `delivery_checks` for real CSV headers/decimal formatting;
  failed checks do not register artifacts. These are declaration-based checks, not
  proof that every open-ended request or final prose statement is correct.
  Explicit uniform decimal precision is parsed as a format slot and requires
  declared CSV precision checks; mixed per-column precision still uses explicit
  declarations. This format parser does not select business scenes.
- Both shipped trajectory preprocessing libraries include terminal stays. Keep
  these copies consistent; business requests should not patch installed skills.
  The extraction script emits `staypoints_by_user` including zero-count users.
- Road `anomaly_runner.py` defaults threshold-only CLI tasks to one point, preserving
  statistical/mixed-detector minima and explicit `--min-points`. Output wrapping
  must reject an absent legacy output when no artifacts are supplied.
  Anomaly default exports use `/mnt/user-data/outputs` in the sandbox; datasets
  remain read-only.
- Local Qwen deployment temperature is `0.0` for factual execution; structured
  recognition already bound temperature zero. Keep interpretation/denominator
  errors visible in evaluations: lower temperature is not a correctness guarantee.
- `make dev` and Docker explicitly set job capacity to `${DEER_FLOW_GRAPH_JOBS:-2}`;
  the installed dev runner otherwise uses one. Graph reload is off; Gateway reload
  excludes `.deer-flow` and `.venv`. Apply startup changes to the affected services.
  Pass their absolute container paths: this Uvicorn version does not match
  relative directory exclusions against watched absolute paths.
- `AioSandboxProvider.uploads_are_shared` allows Gateway to skip container
  acquisition for local bind-mounted uploads. Remote/provisioner modes
  preserve eager synchronization. Do not spend the finite sandbox port pool on
  upload-only or intent-only sessions. Container command logging redacts `-e` values.
- Docker Nginx uses shared upstream zones with `resolve` and Docker DNS for
  Gateway/LangGraph, validated on the running Nginx 1.29.8. Reload the proxy
  after changing its configuration; upstream IP changes then refresh via DNS.

**Compliance gates** (optional, only when `compliance.enabled: true` in `config.yaml`) are mounted outside this chain:

- **ComplianceOutputGateMiddleware** and **ComplianceContextGateMiddleware** are inserted at the **front** of the list by `build_lead_runtime_middlewares()`. Both rely on the same rule: `wrap_tool_call` *and* `wrap_model_call` compose first-in-list as **outermost**. The context gate therefore sits *outside* `ToolErrorHandlingMiddleware` (which would otherwise swallow detection failures into an error ToolMessage), and the output gate's `wrap_model_call` result is the only version that reaches graph state.
- The output gate sanitizes in **`wrap_model_call`**, not `after_model`. `after_model` runs in *reverse* list order, so a front-mounted gate runs *last* and every other `after_model` middleware sees the unsanitized answer first — `RawTranscriptMiddleware` then locks it into `raw_messages` permanently (its reducer dedups by id keeping the first), and `TitleMiddleware` sends it to an external model. Sanitizing in `wrap_model_call` means the original never enters state at all. `after_model` is retained as a digest-gated backstop for content mutated after the model node (e.g. `LoopDetectionMiddleware`'s hard-stop append).
- **ComplianceInputGateMiddleware** is appended **after** `IntentRecognitionMiddleware` in `_build_middlewares()`, because `before_agent` runs in forward order and the input gate needs the recognized intent to apply the "sensitive entity + high-risk intent" rule.
- IntentGuard may downgrade non-baseline InputGate hits only while the usage
  scene remains unresolved. Once a trusted or validated manual scene resolves
  to one of the five 7.23 scenes, detector hits must reach the policy matrix;
  intent cannot erase evidence before scene-specific allow, desensitize,
  aggregate, or refuse actions are selected.
- `UserInputNormalizer` keeps the original query text and also flattens nested
  mapping/list input plus valid JSON objects embedded in prose into dotted
  `FieldItem` paths. This lets identifiers such as `msisdn`, `imei`, `lat`, and
  `lon` participate in structured InputGate rules.
- InputGate refusals terminate the graph with an `AIMessage` whose
  `response_metadata.compliance` uses the durable frontend disposition
  contract (`streaming_mode: pre_model_refusal`, `retracted: true`). Keep this
  metadata in sync with the OutputGate contract so the structured compliance
  UI survives refreshes; `transient_exposure_possible` is false because the
  model is never invoked.
- The mobile adapter subscribes to LangGraph's `custom` stream mode and maps
  each `compliance_retract` event to one `反馈智能体` segment containing only the
  safe replacement/refusal. It forwards disposition metadata and suppresses
  the rewritten final answer so JSON and SSE clients do not receive duplicates.
- `/api/deerflow/mobile/run` accepts explicit `compliance_identity`,
  `compliance_context`, and `compliance_request` fields (plus their legacy
  nested forms under `context`). Identity becomes
  `run_context.compliance_context.user_context`; `compliance_request` is part
  of LangGraph input/state. Never forward arbitrary client `context` keys.
- Mobile compliance results from InputGate, ContextGate, and OutputGate are
  normalized to `gate`, `scene`, `violation_types`, `actions`, `audit_ref`,
  `basis`, and `final_disposition`. SSE emits incremental `compliance` events
  and a final `done.compliance_results` snapshot; non-streaming JSON returns the
  same `compliance_results` field. Derive `final_disposition` from policy
  `actions`, never from `compliance_retract`/`retracted`: allow determinations
  can use replacement transport semantics to append a safe compliance notice.
  A live `compliance` event keeps these canonical fields at the top level and
  also exposes an identical `compliance_result` object for the OneCityAgent UI
  bridge. Do not add that transport-only mirror to `done.compliance_results`.
- If a LangGraph SDK stream stays open after the run commits, the mobile SSE
  adapter may recover from the terminal thread state only when the checkpoint
  changed, `next` is empty, and the state contains an AI response after the
  current query. These guards prevent stale thread history from producing a
  false `done` event.

`tests/test_compliance_gates.py` asserts all three positions.

### Configuration System

**Main Configuration** (`config.yaml`):

Setup: Copy `config.example.yaml` to `config.yaml` in the **project root** directory.

**Config Versioning**: `config.example.yaml` has a `config_version` field. On startup, `AppConfig.from_file()` compares user version vs example version and emits a warning if outdated. Missing `config_version` = version 0. Run `make config-upgrade` to auto-merge missing fields. When changing the config schema, bump `config_version` in `config.example.yaml`.

Configuration priority:
1. Explicit `config_path` argument
2. `DEER_FLOW_CONFIG_PATH` environment variable
3. `config.yaml` in current directory (backend/)
4. `config.yaml` in parent directory (project root - **recommended location**)

Config values starting with `$` are resolved as environment variables (e.g., `$OPENAI_API_KEY`).

**Extensions Configuration** (`extensions_config.json`):

MCP servers and skills are configured together in `extensions_config.json` in project root:

Configuration priority:
1. Explicit `config_path` argument
2. `DEER_FLOW_EXTENSIONS_CONFIG_PATH` environment variable
3. `extensions_config.json` in current directory (backend/)
4. `extensions_config.json` in parent directory (project root - **recommended location**)

### Gateway API (`app/gateway/`)

FastAPI application on port 8001 with health check at `GET /health`.

**Routers**:

| Router | Endpoints |
|--------|-----------|
| **Models** (`/api/models`) | `GET /` - list models; `GET /{name}` - model details |
| **MCP** (`/api/mcp`) | `GET /config` - get config; `PUT /config` - update config (saves to extensions_config.json) |
| **Skills** (`/api/skills`) | `GET /` - list skills; `GET /{name}` - details; `PUT /{name}` - update enabled; `POST /install` - install from .skill archive (accepts standard optional frontmatter like `version`, `author`, `compatibility`) |
| **Memory** (`/api/memory`) | `GET /` - memory data; `POST /reload` - force reload; `GET /config` - config; `GET /status` - config + data |
| **Uploads** (`/api/threads/{id}/uploads`) | `POST /` - upload files (auto-converts PDF/PPT/Excel/Word); `GET /list` - list; `DELETE /{filename}` - delete |
| **Artifacts** (`/api/threads/{id}/artifacts`) | `GET /{path}` - serve artifacts; `?download=true` for file download |
| **Suggestions** (`/api/threads/{id}/suggestions`) | `POST /` - generate follow-up questions; rich list/block model content is normalized before JSON parsing |

Proxied through nginx: `/api/langgraph/*` → LangGraph, all other `/api/*` → Gateway.

### Sandbox System (`packages/harness/deerflow/sandbox/`)

**Interface**: Abstract `Sandbox` with `execute_command`, `read_file`, `write_file`, `list_dir`
**Provider Pattern**: `SandboxProvider` with `acquire`, `get`, `release` lifecycle
**Implementations**:
- `LocalSandboxProvider` - Singleton local filesystem execution with path mappings
- `AioSandboxProvider` (`packages/harness/deerflow/community/`) - Docker-based isolation
- AIO sandbox containers are built from the image configured in `config.yaml` under `sandbox.image`; if skills need system tools like tshark or zeek, install them in that image rather than only in `backend/Dockerfile`

**Virtual Path System**:
- Agent sees: `/mnt/user-data/{workspace,uploads,outputs}`, `/mnt/skills`
- Physical: `backend/.deer-flow/threads/{thread_id}/user-data/...`, `deer-flow/skills/`
- Translation: `replace_virtual_path()` / `replace_virtual_paths_in_command()`
- Detection: `is_local_sandbox()` checks `sandbox_id == "local"`

**Sandbox Tools** (in `packages/harness/deerflow/sandbox/tools.py`):
- `bash` - Execute commands with path translation and error handling
- `ls` - Directory listing (tree format, max 2 levels)
- `read_file` - Read file contents with optional line range
- `write_file` - Write/append to files, creates directories
- `str_replace` - Substring replacement (single or all occurrences)

### Subagent System (`packages/harness/deerflow/subagents/`)

**Built-in Agents**: `general-purpose` (all tools except `task`) and `bash` (command specialist)
**Execution**: Dual thread pool - `_scheduler_pool` (3 workers) + `_execution_pool` (3 workers)
**Concurrency**: `MAX_CONCURRENT_SUBAGENTS = 3` enforced by `SubagentLimitMiddleware` (truncates excess tool calls in `after_model`), 15-minute timeout
**Flow**: `task()` tool → `SubagentExecutor` → background thread → poll 5s → SSE events → result
**Events**: `task_started`, `task_running`, `task_completed`/`task_failed`/`task_timed_out`

### Tool System (`packages/harness/deerflow/tools/`)

`get_available_tools(groups, include_mcp, model_name, subagent_enabled)` assembles:
1. **Config-defined tools** - Resolved from `config.yaml` via `resolve_variable()`
2. **MCP tools** - From enabled MCP servers (lazy initialized, cached with mtime invalidation)
3. **Built-in tools**:
   - `present_files` - Make output files visible to user (only `/mnt/user-data/outputs`)
   - `ask_clarification` - Request clarification (intercepted by ClarificationMiddleware → interrupts)
   - `view_image` - Read image as base64 (added only if model supports vision)
4. **Subagent tool** (if enabled):
   - `task` - Delegate to subagent (description, prompt, subagent_type, max_turns)

**Community tools** (`packages/harness/deerflow/community/`):
- `tavily/` - Web search (5 results default) and web fetch (4KB limit)
- `jina_ai/` - Web fetch via Jina reader API with readability extraction
- `firecrawl/` - Web scraping via Firecrawl API
- `image_search/` - Image search via DuckDuckGo

### MCP System (`packages/harness/deerflow/mcp/`)

- Uses `langchain-mcp-adapters` `MultiServerMCPClient` for multi-server management
- **Lazy initialization**: Tools loaded on first use via `get_cached_mcp_tools()`
- **Cache invalidation**: Detects config file changes via mtime comparison
- **Transports**: stdio (command-based), SSE, HTTP
- **OAuth (HTTP/SSE)**: Supports token endpoint flows (`client_credentials`, `refresh_token`) with automatic token refresh + Authorization header injection
- **Runtime updates**: Gateway API saves to extensions_config.json; LangGraph detects via mtime

### Skills System (`packages/harness/deerflow/skills/`)

- **Location**: `deer-flow/skills/{public,custom}/`
- **Format**: Directory with `SKILL.md` (YAML frontmatter: name, description, license, allowed-tools)
- **Loading**: `load_skills()` recursively scans `skills/{public,custom}` for `SKILL.md`, parses metadata, and reads enabled state from extensions_config.json
- **Injection**: Enabled skills listed in agent system prompt with container paths
- **Installation**: `POST /api/skills/install` extracts .skill ZIP archive to custom/ directory

### Model Factory (`packages/harness/deerflow/models/factory.py`)

- `create_chat_model(name, thinking_enabled)` instantiates LLM from config via reflection
- Supports `thinking_enabled` flag with per-model `when_thinking_enabled` overrides
- Supports `supports_vision` flag for image understanding models
- Config values starting with `$` resolved as environment variables
- Missing provider modules surface actionable install hints from reflection resolvers (for example `uv add langchain-google-genai`)

### IM Channels System (`app/channels/`)

Bridges external messaging platforms (Feishu, Slack, Telegram) to the DeerFlow agent via the LangGraph Server.

**Architecture**: Channels communicate with the LangGraph Server through `langgraph-sdk` HTTP client (same as the frontend), ensuring threads are created and managed server-side.

**Components**:
- `message_bus.py` - Async pub/sub hub (`InboundMessage` → queue → dispatcher; `OutboundMessage` → callbacks → channels)
- `store.py` - JSON-file persistence mapping `channel_name:chat_id[:topic_id]` → `thread_id` (keys are `channel:chat` for root conversations and `channel:chat:topic` for threaded conversations)
- `manager.py` - Core dispatcher: creates threads via `client.threads.create()`, routes commands, keeps Slack/Telegram on `client.runs.wait()`, and uses `client.runs.stream(["messages-tuple", "values"])` for Feishu incremental outbound updates
- `base.py` - Abstract `Channel` base class (start/stop/send lifecycle)
- `service.py` - Manages lifecycle of all configured channels from `config.yaml`
- `slack.py` / `feishu.py` / `telegram.py` - Platform-specific implementations (`feishu.py` tracks the running card `message_id` in memory and patches the same card in place)

**Message Flow**:
1. External platform -> Channel impl -> `MessageBus.publish_inbound()`
2. `ChannelManager._dispatch_loop()` consumes from queue
3. For chat: look up/create thread on LangGraph Server
4. Feishu chat: `runs.stream()` → accumulate AI text → publish multiple outbound updates (`is_final=False`) → publish final outbound (`is_final=True`)
5. Slack/Telegram chat: `runs.wait()` → extract final response → publish outbound
6. Feishu channel sends one running reply card up front, then patches the same card for each outbound update (card JSON sets `config.update_multi=true` for Feishu's patch API requirement)
7. For commands (`/new`, `/status`, `/models`, `/memory`, `/help`): handle locally or query Gateway API
8. Outbound → channel callbacks → platform reply

**Configuration** (`config.yaml` -> `channels`):
- `langgraph_url` - LangGraph Server URL (default: `http://localhost:2024`)
- `gateway_url` - Gateway API URL for auxiliary commands (default: `http://localhost:8001`)
- Per-channel configs: `feishu` (app_id, app_secret), `slack` (bot_token, app_token), `telegram` (bot_token)

### Memory System (`packages/harness/deerflow/agents/memory/`)

**Components**:
- `updater.py` - LLM-based memory updates with fact extraction and atomic file I/O
- `queue.py` - Debounced update queue (per-thread deduplication, configurable wait time)
- `prompt.py` - Prompt templates for memory updates

**Data Structure** (stored in `backend/.deer-flow/memory.json`):
- **User Context**: `workContext`, `personalContext`, `topOfMind` (1-3 sentence summaries)
- **History**: `recentMonths`, `earlierContext`, `longTermBackground`
- **Facts**: Discrete facts with `id`, `content`, `category` (preference/knowledge/context/behavior/goal), `confidence` (0-1), `createdAt`, `source`

**Workflow**:
1. `MemoryMiddleware` filters messages (user inputs + final AI responses) and queues conversation
2. Queue debounces (30s default), batches updates, deduplicates per-thread
3. Background thread invokes LLM to extract context updates and facts
4. Applies updates atomically (temp file + rename) with cache invalidation
5. Next interaction injects top 15 facts + context into `<memory>` tags in system prompt

**Configuration** (`config.yaml` → `memory`):
- `enabled` / `injection_enabled` - Master switches
- `storage_path` - Path to memory.json
- `debounce_seconds` - Wait time before processing (default: 30)
- `model_name` - LLM for updates (null = default model)
- `max_facts` / `fact_confidence_threshold` - Fact storage limits (100 / 0.7)
- `max_injection_tokens` - Token limit for prompt injection (2000)

### Compliance Detection (`packages/harness/deerflow/compliance/`)

In `scene_resolver_mode: manual_ui`, the front-end uses five fixed 7.23 test
profiles. `compliance/manual_context.py` is the server-side source of truth for
the identity → role/organisation → scene binding. `ManualSceneResolver` rejects
unknown identities, mismatched scenes, and spoofed user context to `_unknown`.
The resolver deliberately reports `permission_checked=false`: this validates a
test profile, not real IAM authority. Production authorization must use
`trusted_upstream` and bind actor, resource ownership, operation, audience and
the IAM decision to the request.

Violation detection across three gates, sharing one engine, one detector set and one disposition matrix. **Disabled by default** (`compliance.enabled: false`).

**Layout**:

| Module | Responsibility |
|--------|----------------|
| `contract.py` | The detector boundary — the *only* module detector authors import. Standard library only, frozen dataclasses, exports its own JSON Schema. |
| `engine.py` | Orchestration: normalize → route → detect → intent → policy → audit |
| `registry.py` | Discovers `detectors/*/manifest.yaml`; the only module that loads concrete detectors |
| `router.py` | Candidate selection by `gate × data_type × cost` |
| `policy.py` | `violation × gate × scene → actions`, loaded from `config/compliance/policy_matrix.yaml` |
| `scene.py` | `SceneResolver` seam; phase 1 returns `None` and the matrix falls back to its `_unknown` column |
| `intent.py` | Combines hits with recognized intent at the input gate (a keyword hit alone is not a violation) |
| `actions.py` | Disposition executors (mask / rewrite / aggregate / refuse) |
| `audit.py` | JSONL determination trail including the compliance basis |
| `adapters/` | `inprocess` \| `subprocess_cli` \| `http_service` — identical to the engine |
| `normalizers/` | Source payload → `DetectionUnit`, one per gate |
| `detectors/` | Self-contained detector packages, each with its own `manifest.yaml` |

**Decoupling is enforced, not just documented**: `tests/test_compliance_decoupling.py` AST-scans `engine.py` / `router.py` / `policy.py` and fails if any of them imports a concrete detector. Adding a detector requires no change to any file above `detectors/`.

**Shipped detector**: `model_tfidf_knn` covers violation types 8/9/10 (`video_meta_leak` / `re_identify` / `domain`). Pure-standard-library TF-IDF + kNN; measured `accuracy=0.9821`, `macro_f1=0.9859` on the 0624 test split at ~12 ms/row.

**Commands**:

End-to-end verification against the running service (Docker, `http://localhost:3538`) is documented in [docs/compliance-e2e-verification.md](../docs/compliance-e2e-verification.md), including the container operations that are safe and the ones that would destroy the running stack.

```bash
make compliance-assets                # unpack + SHA256 verify + distribute the delivery package
make compliance-verify-content-text   # CI gate: 282/282 content_text reproduction
make compliance-eval-model            # reproduce the offline baseline
make compliance-eval-gates            # end-to-end, four metric groups
make compliance-export-schema         # export the detector contract as JSON Schema
```

Model weights (`models/compliance/*.json`, 6.7–11.7 MB) are gitignored and rebuilt by `make compliance-assets`.

**Adding a detector**: see [docs/compliance-detector-integration-guide.md](../docs/compliance-detector-integration-guide.md).

### Reflection System (`packages/harness/deerflow/reflection/`)

- `resolve_variable(path)` - Import module and return variable (e.g., `module.path:variable_name`)
- `resolve_class(path, base_class)` - Import and validate class against base class

### Config Schema

**`config.yaml`** key sections:
- `models[]` - LLM configs with `use` class path, `supports_thinking`, `supports_vision`, provider-specific fields
- `tools[]` - Tool configs with `use` variable path and `group`
- `tool_groups[]` - Logical groupings for tools
- `sandbox.use` - Sandbox provider class path
- `skills.path` / `skills.container_path` - Host and container paths to skills directory
- `title` - Auto-title generation (enabled, max_words, max_chars, prompt_template)
- `summarization` - Context summarization (enabled, trigger conditions, keep policy)
- `subagents.enabled` - Master switch for subagent delegation
- `memory` - Memory system (enabled, storage_path, debounce_seconds, model_name, max_facts, fact_confidence_threshold, injection_enabled, max_injection_tokens)
- `compliance` - Violation detection (enabled, per-gate `fail_mode`/`budget_ms`/`max_units`, output-gate `incremental_scan`, detector and policy matrix paths, scene resolver, audit)

**`extensions_config.json`**:
- `mcpServers` - Map of server name → config (enabled, type, command, args, env, url, headers, oauth, description)
- `skills` - Map of skill name → state (enabled)

Both can be modified at runtime via Gateway API endpoints or `DeerFlowClient` methods.

### Embedded Client (`packages/harness/deerflow/client.py`)

`DeerFlowClient` provides direct in-process access to all DeerFlow capabilities without HTTP services. All return types align with the Gateway API response schemas, so consumer code works identically in HTTP and embedded modes.

**Architecture**: Imports the same `deerflow` modules that LangGraph Server and Gateway API use. Shares the same config files and data directories. No FastAPI dependency.

**Agent Conversation** (replaces LangGraph Server):
- `chat(message, thread_id)` — synchronous, returns final text
- `stream(message, thread_id)` — yields `StreamEvent` aligned with LangGraph SSE protocol:
  - `"values"` — full state snapshot (title, messages, artifacts)
  - `"messages-tuple"` — per-message update (AI text, tool calls, tool results)
  - `"end"` — stream finished
- Agent created lazily via `create_agent()` + `_build_middlewares()`, same as `make_lead_agent`
- Supports `checkpointer` parameter for state persistence across turns
- `reset_agent()` forces agent recreation (e.g. after memory or skill changes)

**Gateway Equivalent Methods** (replaces Gateway API):

| Category | Methods | Return format |
|----------|---------|---------------|
| Models | `list_models()`, `get_model(name)` | `{"models": [...]}`, `{name, display_name, ...}` |
| MCP | `get_mcp_config()`, `update_mcp_config(servers)` | `{"mcp_servers": {...}}` |
| Skills | `list_skills()`, `get_skill(name)`, `update_skill(name, enabled)`, `install_skill(path)` | `{"skills": [...]}` |
| Memory | `get_memory()`, `reload_memory()`, `get_memory_config()`, `get_memory_status()` | dict |
| Uploads | `upload_files(thread_id, files)`, `list_uploads(thread_id)`, `delete_upload(thread_id, filename)` | `{"success": true, "files": [...]}`, `{"files": [...], "count": N}` |
| Artifacts | `get_artifact(thread_id, path)` → `(bytes, mime_type)` | tuple |

**Key difference from Gateway**: Upload accepts local `Path` objects instead of HTTP `UploadFile`, rejects directory paths before copying, and reuses a single worker when document conversion must run inside an active event loop. Artifact returns `(bytes, mime_type)` instead of HTTP Response. `update_mcp_config()` and `update_skill()` automatically invalidate the cached agent.

**Tests**: `tests/test_client.py` (77 unit tests including `TestGatewayConformance`), `tests/test_client_live.py` (live integration tests, requires config.yaml)

**Gateway Conformance Tests** (`TestGatewayConformance`): Validate that every dict-returning client method conforms to the corresponding Gateway Pydantic response model. Each test parses the client output through the Gateway model — if Gateway adds a required field that the client doesn't provide, Pydantic raises `ValidationError` and CI catches the drift. Covers: `ModelsListResponse`, `ModelResponse`, `SkillsListResponse`, `SkillResponse`, `SkillInstallResponse`, `McpConfigResponse`, `UploadResponse`, `MemoryConfigResponse`, `MemoryStatusResponse`.

## Development Workflow

### Test-Driven Development (TDD) — MANDATORY

**Every new feature or bug fix MUST be accompanied by unit tests. No exceptions.**

- Write tests in `backend/tests/` following the existing naming convention `test_<feature>.py`
- Run the full suite before and after your change: `make test`
- Tests must pass before a feature is considered complete
- For lightweight config/utility modules, prefer pure unit tests with no external dependencies
- If a module causes circular import issues in tests, add a `sys.modules` mock in `tests/conftest.py` (see existing example for `deerflow.subagents.executor`)

```bash
# Run all tests
make test

# Run a specific test file
PYTHONPATH=. uv run pytest tests/test_<feature>.py -v
```

### Running the Full Application

From the **project root** directory:
```bash
make dev
```

This starts all services and makes the application available at `http://localhost:2026`.

**Nginx routing**:
- `/api/langgraph/*` → LangGraph Server (2024)
- `/api/*` (other) → Gateway API (8001)
- `/` (non-API) → Frontend (3000)

### Running Backend Services Separately

From the **backend** directory:

```bash
# Terminal 1: LangGraph server
make dev

# Terminal 2: Gateway API
make gateway
```

Direct access (without nginx):
- LangGraph: `http://localhost:2024`
- Gateway: `http://localhost:8001`

### Frontend Configuration

The frontend uses environment variables to connect to backend services:
- `NEXT_PUBLIC_LANGGRAPH_BASE_URL` - Defaults to `/api/langgraph` (through nginx)
- `NEXT_PUBLIC_BACKEND_BASE_URL` - Defaults to empty string (through nginx)

When using `make dev` from root, the frontend automatically connects through nginx.

## Key Features

### File Upload

Multi-file upload with automatic document conversion:
- Endpoint: `POST /api/threads/{thread_id}/uploads`
- Supports: PDF, PPT, Excel, Word documents (converted via `markitdown`)
- Rejects directory inputs before copying so uploads stay all-or-nothing
- Reuses one conversion worker per request when called from an active event loop
- Files stored in thread-isolated directories
- Agent receives uploaded file list via `UploadsMiddleware`

See [docs/FILE_UPLOAD.md](docs/FILE_UPLOAD.md) for details.

### Plan Mode

TodoList middleware for complex multi-step tasks:
- Controlled via runtime config: `config.configurable.is_plan_mode = True`
- Provides `write_todos` tool for task tracking
- One task in_progress at a time, real-time updates

See [docs/plan_mode_usage.md](docs/plan_mode_usage.md) for details.

### Context Summarization

Automatic conversation summarization when approaching token limits:
- Configured in `config.yaml` under `summarization` key
- Trigger types: tokens, messages, or fraction of max input
- Keeps recent messages while summarizing older ones

See [docs/summarization.md](docs/summarization.md) for details.

### Vision Support

For models with `supports_vision: true`:
- `ViewImageMiddleware` processes images in conversation
- `view_image_tool` added to agent's toolset
- Images automatically converted to base64 and injected into state

## Code Style

- Uses `ruff` for linting and formatting
- Line length: 240 characters
- Python 3.12+ with type hints
- Double quotes, space indentation

## Documentation

See `docs/` directory for detailed documentation:
- [CONFIGURATION.md](docs/CONFIGURATION.md) - Configuration options
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - Architecture details
- [API.md](docs/API.md) - API reference
- [SETUP.md](docs/SETUP.md) - Setup guide
- [FILE_UPLOAD.md](docs/FILE_UPLOAD.md) - File upload feature
- [PATH_EXAMPLES.md](docs/PATH_EXAMPLES.md) - Path types and usage
- [summarization.md](docs/summarization.md) - Context summarization
- [plan_mode_usage.md](docs/plan_mode_usage.md) - Plan mode with TodoList

Small-table data inspection includes every input row up to 20 rows; larger tables retain a five-row preview explicitly marked incomplete. Explanation-only turns must use observed rows rather than reconstructing records from aggregates. This adds no model or tool call.

## Scene service regression checks (2026-10-28)

`tests/test_scene_service_fixes.py` covers native-frame tool parameters and the computation guard: a successful city-video event plan must execute single-video-event-analysis and actually view its returned review frames. Frame sampling, detections and tracks preserve relative offsets when shooting time is unknown; `every_frame` uses a half-open time window. Additional GIS/media integration checks and source-video fixtures are retained locally; the released self-contained backend tests cover the tool contracts. Older sandbox images require their wheel-bundled PROJ/GDAL paths in sandbox.environment; new images no longer force the older system databases. Do not publish populated config.yaml or raw live traces.

Video object analytics exports full CSVs inside the sandbox and returns only a bounded frame preview; do not print complete detection JSON through the shell API (large results are truncated). The event extractor uses output-side FFmpeg seeking for AVI compatibility and rejects zero-frame results. Local integration checks include the original AVI and 125-row lossless export; the original media is not published.
