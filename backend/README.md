# UrbanBrain Backend

The tracked config.example.yaml enables only Qwen3.6-35B-A3B and references
QWEN_BASE_URL / QWEN_API_KEY environment variables. Runtime config.yaml and
credentials remain local. See ../docs/releases/20261028-intent-full-flow.md
for the publication scope and reproducible tests.

Current-turn requirements are shared by recognition, dialogue resume, execution
and delivery checks. Explanation-only turns replace earlier computation/export
requirements. CSV checks support explicitly requested missing markers and
per-metric precision, while final completion checks aggregate current, hashed
deliveries across files. A bounded corrective retry uses auto tool choice, which
is compatible with Qwen thinking mode. Failed recognition or execution is
reported as failure, never as a completed placeholder. See
`../docs/turn-contract-fixes-20261027.md` for regression evidence.
Corrective retries remove unverified drafts from working history. Current scene
parameters are supplied to explanation turns; missing tool argument fields receive
concise JSON correction feedback without executing malformed calls.
When required files exist but the model omits presentation, the runtime can
schedule one normal delivery call for files created or changed this turn. It
preserves refusals, tool permissions and existing CSV/compliance validation.
CSV delivery also checks strict quoting, unique nonempty headers and fixed row
widths, while retaining valid quoted commas/newlines and empty detail tables.
Explicit entity/metric comparisons in computed answers are checked against
current verified CSV values with Decimal arithmetic. Ambiguous or unsupported
comparisons are left unknown; contradictions use the existing single correction,
without adding a judge model call. Arithmetic/syntax validation is distinguished
from access authorization in the compliance adapter; the frozen core is intact.
Stay-point extraction now writes CSV event detail alongside its original JSONL,
including a header for an empty result. See `../docs/remaining-quality-fixes-20261028.md`.
Reusing an input does not restore old output goals: explicit current metrics and
deliverables replace prior scope. Detail files may contain none
of the requested summary metrics, while final aggregate metric coverage is still
required across the delivered files.
This also covers an actual new file referenced in the final draft when the
recognizer omitted its file type. Explicit export cancellations still apply;
failed presentation of such a file blocks completion rather than being ignored.

Thinking is controlled by stage: intent recognition and dialogue-turn decisions
use a non-thinking model instance; business execution defaults to thinking.
Subtasks inherit the parent's effective thinking setting and fall back to
non-thinking for unsupported models. The UI's Flash mode explicitly disables
business thinking; Thinking/Pro/Ultra enable it. This uses the same configured
Qwen3.6-35B-A3B model. The current deployment has a 2048-token thinking budget;
the earlier full-flow comparison explicitly disabled thinking in its requests.

Numeric-analysis requests require current-turn computation evidence before a final
answer: the runtime allows one corrective retry if the model skips execution,
then reports an inability to verify instead of delivering unsupported numbers.
The corrective request uses auto tool choice and verifies the resulting evidence.
CSV-required metrics are recorded during the existing intent call; delivery maps
each metric to a real CSV column and checks requested precision locally. Delivery
accepts a single check object or an array, including JSON strings, and maps metric
labels to existing headers without requiring header changes. Execution
evidence and column presence do not prove every prose statement is correct.

UrbanBrain Backend is the backend runtime of the UrbanBrain 城市超脑 platform. It is built on a LangGraph-based agent harness with sandbox execution, persistent memory, scene-aware skill routing, and extensible tool integration. The backend enables agents to execute code, browse the web, manage files, orchestrate domain skills, delegate tasks to subagents, and retain context across conversations in isolated per-thread environments.

---

## Architecture

```
                        ┌──────────────────────────────────────┐
                        │          Nginx (Port 2026)           │
                        │      Unified reverse proxy           │
                        └───────┬──────────────────┬───────────┘
                                │                  │
              /api/langgraph/*  │                  │  /api/* (other)
                                ▼                  ▼
               ┌────────────────────┐  ┌────────────────────────┐
               │ LangGraph Server   │  │   Gateway API (8001)   │
               │    (Port 2024)     │  │   FastAPI REST         │
               │                    │  │                        │
               │ ┌────────────────┐ │  │ Models, MCP, Skills,   │
               │ │  Lead Agent    │ │  │ Memory, Uploads,       │
               │ │  ┌──────────┐  │ │  │ Artifacts              │
               │ │  │Middleware│  │ │  └────────────────────────┘
               │ │  │  Chain   │  │ │
               │ │  └──────────┘  │ │
               │ │  ┌──────────┐  │ │
               │ │  │  Tools   │  │ │
               │ │  └──────────┘  │ │
               │ │  ┌──────────┐  │ │
               │ │  │Subagents │  │ │
               │ │  └──────────┘  │ │
               │ └────────────────┘ │
               └────────────────────┘
```

**Request Routing** (via Nginx):
- `/api/langgraph/*` → LangGraph Server - agent interactions, threads, streaming
- `/api/*` (other) → Gateway API - models, MCP, skills, memory, artifacts, uploads
- `/` (non-API) → Frontend - Next.js web interface

---

## Core Components

### Lead Agent

The single LangGraph agent (`lead_agent`) is the runtime entry point of UrbanBrain, created via `make_lead_agent(config)`. It combines:

- **Dynamic model selection** with thinking and vision support
- **Middleware chain** for cross-cutting concerns (9 middlewares)
- **Tool system** with sandbox, MCP, community, and built-in tools
- **Subagent delegation** for parallel task execution
- **System prompt** with skills injection, memory context, and working directory guidance

This deployment registers only `qwen3.6-35b-a3b` (Qwen3.6-35B-A3B) in the local
`config.yaml`, including title, summarization, and memory model references.
Removed model selections fall back to the configured default. Intent recognition
explicitly disables Qwen thinking. See the [latency analysis and local deployment
notes](../docs/intent-recognition-latency-analysis-20261022.md); the current endpoint
is still DashScope. Restart LangGraph/Gateway after changing deployment configuration.

Business intent recognition now combines scene selection, task extraction,
parameters, and the OneCityAgent plan in one structured model call. Invalid
output gets at most one repair call, followed by an explicit recognition failure
that preserves the original request and prevents partial business execution.
Dialogue control remains a separate step when context requires it. See the
[optimization and real-model evaluation report](../docs/intent-recognition-optimization-20261022.md)
for frozen cases, measured accuracy/latency, planning compatibility, and rollback.

The deployment registry enables exactly ten scenes: network traffic, phone
networks, policy/regulation, trajectories, street-view location, remote sensing,
video, source code, road traffic, and tables/yearbooks. The six packaged demo
alarm scenes are excluded. Qwen sees every enabled scene and decides by meaning;
domain keywords no longer exclude candidates or veto semantic decisions.
Bounded upload metadata and the previous user task help resolve vague requests.
Missing execution parameters are verified during planning; an unresolved business
object produces a clarification question for the execution agent's existing
`ask_clarification` tool. See the [method survey and semantic evaluation](../docs/intent-semantic-routing-20261022.md).

Full-flow fixes preserve the same Qwen model and ten scenes. Unknown-scene
clarification replies now resolve the original task rather than restore an empty
route; known-scene parameter replies retain their fast path. Historical upload
metadata remains available on later turns. Verified split evidence quotes are
handled locally without extra model repair calls.

Development graph startup explicitly allows two concurrent jobs (override with
`DEER_FLOW_GRAPH_JOBS`) and disables graph autoreload; Gateway reload excludes
runtime uploads. Restart the affected services after changing these startup flags.
Gateway reload exclusions use absolute container paths, as required by the
installed Uvicorn file-filter behavior.
Skill output wrapping accepts JSON array strings for artifact paths. CSV deliveries
can use `present_files.delivery_checks` to check required columns and requested
decimal precision against actual file contents. Final-answer instructions require
using verified calculation results; open-ended prose is not fully machine-verified.
An explicit uniform decimal-place requirement in the request makes CSV precision
checks mandatory; a plain `present_files` call cannot silently bypass them.
The trajectory stay-point skill includes terminal stays and an inclusive minimum
duration. Regression coverage: `tests/test_full_flow_regressions.py`.
Stay-point summaries explicitly list users with zero qualifying stays.
Road threshold-only detection uses a one-point default minimum rather than the
eight-point training guard for statistical detectors; explicit minima are respected.
Output wrapping rejects a missing result with no generated artifacts.
Anomaly detection exports use the thread outputs directory inside the sandbox,
avoiding writes to mounted read-only datasets.
The factual-analysis deployment uses Qwen temperature `0.0` instead of `0.7`;
this reduces sampling variation but does not prove factual correctness.
Revisiting earlier files also supplies up to six real prior user messages to
recognition, so an intervening topic does not erase the file's original task.
Local Docker uploads are saved to the shared thread directory without allocating
a sandbox. A container is acquired only when business tools execute; remote
sandbox uploads retain file synchronization. Container-start logs redact all
environment values.
Docker proxy upstreams resolve backend addresses dynamically, so recreated or
restarted services do not leave the proxy pinned to obsolete container IPs.

### Middleware Chain

Middlewares execute in strict order, each handling a specific concern:

| # | Middleware | Purpose |
|---|-----------|---------|
| 1 | **ThreadDataMiddleware** | Creates per-thread isolated directories (workspace, uploads, outputs) |
| 2 | **UploadsMiddleware** | Injects newly uploaded files into conversation context |
| 3 | **SandboxMiddleware** | Acquires sandbox environment for code execution |
| 4 | **SummarizationMiddleware** | Reduces context when approaching token limits (optional) |
| 5 | **TodoListMiddleware** | Tracks multi-step tasks in plan mode (optional) |
| 6 | **TitleMiddleware** | Auto-generates conversation titles after first exchange |
| 7 | **MemoryMiddleware** | Queues conversations for async memory extraction |
| 8 | **ViewImageMiddleware** | Injects image data for vision-capable models (conditional) |
| 9 | **ClarificationMiddleware** | Intercepts clarification requests and interrupts execution (must be last) |

### Sandbox System

Per-thread isolated execution with virtual path translation:

- **Abstract interface**: `execute_command`, `read_file`, `write_file`, `list_dir`
- **Providers**: `LocalSandboxProvider` (filesystem) and `AioSandboxProvider` (Docker, in community/)
- **Virtual paths**: `/mnt/user-data/{workspace,uploads,outputs}` → thread-specific physical directories
- **Skills path**: `/mnt/skills` → `deer-flow/skills/` directory
- **Skills loading**: Recursively discovers nested `SKILL.md` files under `skills/{public,custom}` and preserves nested container paths
- **Tools**: `bash`, `ls`, `read_file`, `write_file`, `str_replace`

### Subagent System

Async task delegation with concurrent execution:

- **Built-in agents**: `general-purpose` (full toolset) and `bash` (command specialist)
- **Concurrency**: Max 3 subagents per turn, 15-minute timeout
- **Execution**: Background thread pools with status tracking and SSE events
- **Flow**: Agent calls `task()` tool → executor runs subagent in background → polls for completion → returns result

### Memory System

LLM-powered persistent context retention across conversations:

- **Automatic extraction**: Analyzes conversations for user context, facts, and preferences
- **Structured storage**: User context (work, personal, top-of-mind), history, and confidence-scored facts
- **Debounced updates**: Batches updates to minimize LLM calls (configurable wait time)
- **System prompt injection**: Top facts + context injected into agent prompts
- **Storage**: JSON file with mtime-based cache invalidation

### Tool Ecosystem

| Category | Tools |
|----------|-------|
| **Sandbox** | `bash`, `ls`, `read_file`, `write_file`, `str_replace` |
| **Built-in** | `present_files`, `ask_clarification`, `view_image`, `task` (subagent) |
| **Community** | Tavily (web search), Jina AI (web fetch), Firecrawl (scraping), DuckDuckGo (image search) |
| **MCP** | Any Model Context Protocol server (stdio, SSE, HTTP transports) |
| **Skills** | Domain-specific workflows injected via system prompt |

### Gateway API

FastAPI application providing REST endpoints for frontend integration:

| Route | Purpose |
|-------|---------|
| `GET /api/models` | List available LLM models |
| `GET/PUT /api/mcp/config` | Manage MCP server configurations |
| `GET/PUT /api/skills` | List and manage skills |
| `POST /api/skills/install` | Install skill from `.skill` archive |
| `GET /api/memory` | Retrieve memory data |
| `POST /api/memory/reload` | Force memory reload |
| `GET /api/memory/config` | Memory configuration |
| `GET /api/memory/status` | Combined config + data |
| `POST /api/threads/{id}/uploads` | Upload files (auto-converts PDF/PPT/Excel/Word to Markdown, rejects directory paths) |
| `GET /api/threads/{id}/uploads/list` | List uploaded files |
| `GET /api/threads/{id}/artifacts/{path}` | Serve generated artifacts |

### IM Channels

The IM bridge supports Feishu, Slack, and Telegram. Slack and Telegram still use the final `runs.wait()` response path, while Feishu now streams through `runs.stream(["messages-tuple", "values"])` and updates a single in-thread card in place.

For Feishu card updates, UrbanBrain stores the running card's `message_id` per inbound message and patches that same card until the run finishes, preserving the existing `OK` / `DONE` reaction flow.

---

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- API keys for your chosen LLM provider

### Installation

```bash
cd deer-flow

# Copy configuration files
cp config.example.yaml config.yaml

# Install backend dependencies
cd backend
make install
```

### Configuration

Edit `config.yaml` in the project root:

```yaml
models:
  - name: gpt-4o
    display_name: GPT-4o
    use: langchain_openai:ChatOpenAI
    model: gpt-4o
    api_key: $OPENAI_API_KEY
    supports_thinking: false
    supports_vision: true
```

Set your API keys:

```bash
export OPENAI_API_KEY="your-api-key-here"
```

### Running

**Full Application** (from project root):

```bash
make dev  # Starts LangGraph + Gateway + Frontend + Nginx
```

Access at: http://localhost:2026

**Backend Only** (from backend directory):

```bash
# Terminal 1: LangGraph server
make dev

# Terminal 2: Gateway API
make gateway
```

Direct access: LangGraph at http://localhost:2024, Gateway at http://localhost:8001

---

## Compliance Input Handling

When a compliance scene has been resolved, InputGate preserves detector hits
and lets the `violation type × gate × scene` policy matrix choose the final
action. A low-risk query intent therefore cannot silently remove sensitive
entities from an explicit `public_release` or other resolved scene. For an
unresolved scene, IntentGuard retains the legacy intent-based downgrade to
avoid treating every entity mention as a violation.

InputGate also exposes nested mappings and JSON objects embedded in user text
as dotted field paths. Fields such as `msisdn`, `imei`, `lat`, and `lon` can be
handled by structured detector rules while the original query text remains
available to text detectors.

For mobile SSE calls, the adapter checks the thread's final checkpoint after
an idle heartbeat interval. If the LangGraph SDK stream remains open after the
run has completed, the adapter emits the committed current-turn result,
compliance event, and terminal `done` frame instead of leaving the client
waiting indefinitely.

---

## Project Structure

```
backend/
├── src/
│   ├── agents/                  # Agent system
│   │   ├── lead_agent/         # Main agent (factory, prompts)
│   │   ├── middlewares/        # 9 middleware components
│   │   ├── memory/             # Memory extraction & storage
│   │   └── thread_state.py    # ThreadState schema
│   ├── gateway/                # FastAPI Gateway API
│   │   ├── app.py             # Application setup
│   │   └── routers/           # 6 route modules
│   ├── sandbox/                # Sandbox execution
│   │   ├── local/             # Local filesystem provider
│   │   ├── sandbox.py         # Abstract interface
│   │   ├── tools.py           # bash, ls, read/write/str_replace
│   │   └── middleware.py      # Sandbox lifecycle
│   ├── subagents/              # Subagent delegation
│   │   ├── builtins/          # general-purpose, bash agents
│   │   ├── executor.py        # Background execution engine
│   │   └── registry.py        # Agent registry
│   ├── tools/builtins/         # Built-in tools
│   ├── mcp/                    # MCP protocol integration
│   ├── models/                 # Model factory
│   ├── skills/                 # Skill discovery & loading
│   ├── config/                 # Configuration system
│   ├── community/              # Community tools & providers
│   ├── reflection/             # Dynamic module loading
│   └── utils/                  # Utilities
├── docs/                       # Documentation
├── tests/                      # Test suite
├── langgraph.json              # LangGraph server configuration
├── pyproject.toml              # Python dependencies
├── Makefile                    # Development commands
└── Dockerfile                  # Container build
```

---

## Configuration

### Main Configuration (`config.yaml`)

Place in project root. Config values starting with `$` resolve as environment variables.

Key sections:
- `models` - LLM configurations with class paths, API keys, thinking/vision flags
- `tools` - Tool definitions with module paths and groups
- `tool_groups` - Logical tool groupings
- `sandbox` - Execution environment provider
- `skills` - Skills directory paths
- `title` - Auto-title generation settings
- `summarization` - Context summarization settings
- `subagents` - Subagent system (enabled/disabled)
- `memory` - Memory system settings (enabled, storage, debounce, facts limits)

Provider note:
- `models[*].use` references provider classes by module path (for example `langchain_openai:ChatOpenAI`).
- If a provider module is missing, DeerFlow now returns an actionable error with install guidance (for example `uv add langchain-google-genai`).

### Extensions Configuration (`extensions_config.json`)

MCP servers and skill states in a single file:

```json
{
  "mcpServers": {
    "github": {
      "enabled": true,
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "env": {"GITHUB_TOKEN": "$GITHUB_TOKEN"}
    },
    "secure-http": {
      "enabled": true,
      "type": "http",
      "url": "https://api.example.com/mcp",
      "oauth": {
        "enabled": true,
        "token_url": "https://auth.example.com/oauth/token",
        "grant_type": "client_credentials",
        "client_id": "$MCP_OAUTH_CLIENT_ID",
        "client_secret": "$MCP_OAUTH_CLIENT_SECRET"
      }
    }
  },
  "skills": {
    "pdf-processing": {"enabled": true}
  }
}
```

### Environment Variables

- `DEER_FLOW_CONFIG_PATH` - Override config.yaml location
- `DEER_FLOW_EXTENSIONS_CONFIG_PATH` - Override extensions_config.json location
- Model API keys: `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `DEEPSEEK_API_KEY`, etc.
- Tool API keys: `TAVILY_API_KEY`, `GITHUB_TOKEN`, etc.

---

## Development

### Commands

```bash
make install    # Install dependencies
make dev        # Run LangGraph server (port 2024)
make gateway    # Run Gateway API (port 8001)
make lint       # Run linter (ruff)
make format     # Format code (ruff)
```

### Code Style

- **Linter/Formatter**: `ruff`
- **Line length**: 240 characters
- **Python**: 3.12+ with type hints
- **Quotes**: Double quotes
- **Indentation**: 4 spaces

### Testing

```bash
uv run pytest
```

---

## Technology Stack

- **LangGraph** (1.0.6+) - Agent framework and multi-agent orchestration
- **LangChain** (1.2.3+) - LLM abstractions and tool system
- **FastAPI** (0.115.0+) - Gateway REST API
- **langchain-mcp-adapters** - Model Context Protocol support
- **agent-sandbox** - Sandboxed code execution
- **markitdown** - Multi-format document conversion
- **tavily-python** / **firecrawl-py** - Web search and scraping

---

## Documentation

- [Configuration Guide](docs/CONFIGURATION.md)
- [Architecture Details](docs/ARCHITECTURE.md)
- [API Reference](docs/API.md)
- [File Upload](docs/FILE_UPLOAD.md)
- [Path Examples](docs/PATH_EXAMPLES.md)
- [Context Summarization](docs/summarization.md)
- [Plan Mode](docs/plan_mode_usage.md)
- [Setup Guide](docs/SETUP.md)

---

## License

See the [LICENSE](../LICENSE) file in the project root.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

Small-table data inspection includes every input row up to 20 rows; larger tables retain a five-row preview explicitly marked incomplete. Explanation-only turns must use observed rows rather than reconstructing records from aggregates. This adds no model or tool call.
