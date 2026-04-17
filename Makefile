# DeerFlow - Unified Development Environment

.PHONY: help config config-upgrade check install dev dev-daemon dev-no-nginx stop-no-nginx status-no-nginx linux-server-start linux-server-stop linux-server-status start stop up down clean docker-init docker-start docker-stop docker-logs docker-logs-frontend docker-logs-gateway docker-update-ports build-aio-sandbox-conda

PYTHON ?= python

help:
	@echo "DeerFlow Development Commands:"
	@echo "  make config          - Generate local config files (aborts if config already exists)"
	@echo "  make config-upgrade  - Merge new fields from config.example.yaml into config.yaml"
	@echo "  make check           - Check if all required tools are installed"
	@echo "  make install         - Install all dependencies (frontend + backend)"
	@echo "  make setup-sandbox   - Pre-pull sandbox container image (recommended)"
	@echo "  make dev             - Start all services in development mode (with hot-reloading)"
	@echo "  make dev-no-nginx    - Start local services without nginx (no sudo needed)"
	@echo "  make stop-no-nginx   - Stop local no-nginx services"
	@echo "  make status-no-nginx - Show local no-nginx service status"
	@echo "  make linux-server-start  - Start server mode on ports 3024/38001/33000"
	@echo "  make linux-server-stop   - Stop server mode on ports 3024/38001/33000"
	@echo "  make linux-server-status - Show server mode status on ports 3024/38001/33000"
	@echo "  make dev-daemon      - Start all services in background (daemon mode)"
	@echo "  make start           - Start all services in production mode (optimized, no hot-reloading)"
	@echo "  make stop            - Stop all running services"
	@echo "  make clean           - Clean up processes and temporary files"
	@echo ""
	@echo "Docker Production Commands:"
	@echo "  make up              - Build and start production Docker services (localhost:2026)"
	@echo "  make down            - Stop and remove production Docker containers"
	@echo ""
	@echo "Docker Development Commands:"
	@echo "  make docker-init     - Build the custom k3s image (with pre-cached sandbox image)"
	@echo "  make build-aio-sandbox-conda - Build custom AIO sandbox image with conda + location-matcher"
	@echo "  make docker-start    - Start Docker services (mode-aware from config.yaml, localhost:2026)"
	@echo "  make update-docker-ports-cname - Update Docker/Nginx port config from config.yaml and use current username to rename container name(anker-deer-flow-nginx) and Compose project names (anker-deer-flow) to avoid conflicts when multiple developers on the same machine. This is useful when using Docker development environment with multiple branches or projects."
	@echo "  make docker-stop     - Stop Docker development services"
	@echo "  make docker-logs     - View Docker development logs"
	@echo "  make docker-logs-frontend - View Docker frontend logs"
	@echo "  make docker-logs-gateway - View Docker gateway logs"

config:
	@$(PYTHON) ./scripts/configure.py

config-upgrade:
	@./scripts/config-upgrade.sh

# Check required tools
check:
	@$(PYTHON) ./scripts/check.py

# Install all dependencies
install:
	@echo "Installing backend dependencies..."
	@cd backend && uv sync
	@echo "Installing frontend dependencies..."
	@cd frontend && pnpm install
	@echo "✓ All dependencies installed"
	@echo ""
	@echo "=========================================="
	@echo "  Optional: Pre-pull Sandbox Image"
	@echo "=========================================="
	@echo ""
	@echo "If you plan to use Docker/Container-based sandbox, you can pre-pull the image:"
	@echo "  make setup-sandbox"
	@echo ""

# Pre-pull sandbox Docker image (optional but recommended)
setup-sandbox:
	@echo "=========================================="
	@echo "  Pre-pulling Sandbox Container Image"
	@echo "=========================================="
	@echo ""
	@IMAGE=$$(grep -A 20 "# sandbox:" config.yaml 2>/dev/null | grep "image:" | awk '{print $$2}' | head -1); \
	if [ -z "$$IMAGE" ]; then \
		IMAGE="enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest"; \
		echo "Using default image: $$IMAGE"; \
	else \
		echo "Using configured image: $$IMAGE"; \
	fi; \
	echo ""; \
	if command -v container >/dev/null 2>&1 && [ "$$(uname)" = "Darwin" ]; then \
		echo "Detected Apple Container on macOS, pulling image..."; \
		container pull "$$IMAGE" || echo "⚠ Apple Container pull failed, will try Docker"; \
	fi; \
	if command -v docker >/dev/null 2>&1; then \
		echo "Pulling image using Docker..."; \
		docker pull "$$IMAGE"; \
		echo ""; \
		echo "✓ Sandbox image pulled successfully"; \
	else \
		echo "✗ Neither Docker nor Apple Container is available"; \
		echo "  Please install Docker: https://docs.docker.com/get-docker/"; \
		exit 1; \
	fi

# Start all services in development mode (with hot-reloading)
dev:
	@./scripts/serve.sh --dev

# Start local services without nginx (for no-sudo environments)
dev-no-nginx:
	@./scripts/dev-no-nginx.sh start

stop-no-nginx:
	@./scripts/dev-no-nginx.sh stop

status-no-nginx:
	@./scripts/dev-no-nginx.sh status

linux-server-start:
	@./scripts/start-linux-server.sh

linux-server-stop:
	@./scripts/stop-linux-server.sh

linux-server-status:
	@LANGGRAPH_PORT=3024 GATEWAY_PORT=38001 FRONTEND_PORT=33000 ./scripts/dev-no-nginx.sh status

# Start all services in production mode (with optimizations)
start:
	@./scripts/serve.sh --prod

# Start all services in daemon mode (background)
dev-daemon:
	@./scripts/start-daemon.sh

# Stop all services
stop:
	@echo "Stopping all services..."
	@-pkill -f "langgraph dev" 2>/dev/null || true
	@-pkill -f "uvicorn app.gateway.app:app" 2>/dev/null || true
	@-pkill -f "next dev" 2>/dev/null || true
	@-pkill -f "next start" 2>/dev/null || true
	@-pkill -f "next-server" 2>/dev/null || true
	@-pkill -f "next-server" 2>/dev/null || true
	@-nginx -c $(PWD)/docker/nginx/nginx.local.conf -p $(PWD) -s quit 2>/dev/null || true
	@sleep 1
	@-pkill -9 nginx 2>/dev/null || true
	@echo "Cleaning up sandbox containers..."
	@-./scripts/cleanup-containers.sh deer-flow-sandbox 2>/dev/null || true
	@echo "✓ All services stopped"

# Clean up
clean: stop
	@echo "Cleaning up..."
	@-rm -rf backend/.deer-flow 2>/dev/null || true
	@-rm -rf backend/.langgraph_api 2>/dev/null || true
	@-rm -rf logs/*.log 2>/dev/null || true
	@echo "✓ Cleanup complete"

# ==========================================
# Docker Development Commands
# ==========================================

# Initialize Docker containers and install dependencies
docker-init:
	@./scripts/docker.sh init

# Build custom AIO sandbox image with conda env and location-matcher package
build-aio-sandbox-conda:
	@PROXY_URL="$${CONTAINER_PROXY_URL:-$${http_proxy:-}}"; \
	HTTPS_URL="$${CONTAINER_PROXY_URL:-$${https_proxy:-$$PROXY_URL}}"; \
	docker build \
		--build-arg http_proxy="$$PROXY_URL" \
		--build-arg https_proxy="$$HTTPS_URL" \
		--build-arg HTTP_PROXY="$$PROXY_URL" \
		--build-arg HTTPS_PROXY="$$HTTPS_URL" \
		--build-arg all_proxy="$${all_proxy:-}" \
		--build-arg ALL_PROXY="$${ALL_PROXY:-$${all_proxy:-}}" \
		--build-arg no_proxy="$${no_proxy:-}" \
		--build-arg NO_PROXY="$${NO_PROXY:-$${no_proxy:-}}" \
		-f docker/aio-sandbox/Dockerfile -t anker-deerflow-aio-sandbox-conda:latest .

# Start Docker development environment
docker-start:
	@./scripts/docker.sh start

# Update Docker and Nginx ports from config.yaml
docker-update-ports-cname:
	@./scripts/update-docker-ports-cname.sh

# Stop Docker development environment
docker-stop:
	@./scripts/docker.sh stop

# View Docker development logs
docker-logs:
	@./scripts/docker.sh logs

# View Docker development logs
docker-logs-frontend:
	@./scripts/docker.sh logs --frontend
docker-logs-gateway:
	@./scripts/docker.sh logs --gateway

# ==========================================
# Production Docker Commands
# ==========================================

# Build and start production services
up:
	@./scripts/deploy.sh

# Stop and remove production containers
down:
	@./scripts/deploy.sh down
