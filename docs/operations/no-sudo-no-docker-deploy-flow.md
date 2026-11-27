# DeerFlow No-Sudo No-Docker Deploy Flow

This guide reproduces the same local deployment style on another Linux server:

- No sudo
- No Docker
- No nginx
- Services run directly: LangGraph (2024), Gateway (8001), Frontend (3000 or 3001)

## 1. Prepare User-Space Toolchain

Confirm all required tools are available in user space:

```bash
git --version
python3 --version
node --version
pnpm --version
uv --version
```

Version targets:

- Python >= 3.12
- Node.js >= 22
- pnpm >= 10
- uv >= 0.7

If your environment uses a proxy, enable it before install/start commands.

## 2. Clone Repository

```bash
git clone https://github.com/bytedance/deer-flow.git
cd deer-flow
```

## 3. Prepare Config and Secrets

Option A (recommended for same behavior as an existing server):

1. Copy the validated `config.yaml` from the existing server.
2. Create `.env` with required keys.

Option B (fresh setup):

```bash
make config
```

Then edit `config.yaml` and define your model configuration.

For the current Qwen-compatible setup, ensure model config references:

- `api_key: $DASHSCOPE_API_KEY`
- `base_url: https://dashscope.aliyuncs.com/compatible-mode/v1`

In `.env`, set at least:

```bash
DASHSCOPE_API_KEY=your-real-key
```

Optional keys by feature:

```bash
TAVILY_API_KEY=...
JINA_API_KEY=...
INFOQUEST_API_KEY=...
```

## 4. Install Dependencies

```bash
make install
```

This installs backend dependencies via `uv sync` and frontend dependencies via `pnpm install`.

## 5. Start Services (No Nginx)

```bash
make dev-no-nginx
```

This command starts:

- LangGraph on port 2024
- Gateway on port 8001
- Frontend on port 3000 (auto-fallback to 3001 if occupied)

## 6. Verify Health

```bash
curl -I http://127.0.0.1:2024/docs
curl -I http://127.0.0.1:8001/docs
curl -I http://127.0.0.1:3000 || curl -I http://127.0.0.1:3001
curl -s http://127.0.0.1:8001/api/models
```

Expected:

- HTTP 200 for service docs/home pages
- `/api/models` returns configured model IDs

## 7. Daily Operations

```bash
make status-no-nginx
make stop-no-nginx
./scripts/dev-no-nginx.sh logs
./scripts/dev-no-nginx.sh restart
```

Logs are stored in `logs/`:

- `logs/langgraph.log`
- `logs/gateway.log`
- `logs/frontend.log`

## 8. Troubleshooting

### Port 3000 in use

Expected behavior. The startup script automatically falls back to 3001.

### `make check` fails due to nginx

Expected in no-nginx mode. You can skip nginx as long as `make dev-no-nginx` works.

### Missing model key at startup

If `config.yaml` references an env var not set in `.env` or shell, backend startup will fail. Add the key and restart.

### Proxy-only network environment

Enable proxy before `make install` and `make dev-no-nginx`.

## 9. Optional: Custom Ports

You can override default ports when starting:

```bash
LANGGRAPH_PORT=2025 GATEWAY_PORT=8002 FRONTEND_PORT=3100 make dev-no-nginx
```
