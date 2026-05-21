# DeerFlow - Unified Development Environment

.PHONY: help config config-upgrade check install setup-sandbox dev dev-daemon dev-no-nginx stop-no-nginx status-no-nginx linux-server-start linux-server-stop linux-server-status model-services-start model-services-stop model-services-status model-services-dogfood start stop up down clean docker-init docker-start docker-stop docker-logs docker-logs-frontend docker-logs-gateway docker-update-ports docker-install docker-install-system sandbox-build docker-build-all extract-router-cards build-skill-router-index update-skill-router-index check-skill-router-conflicts eval-skill-router sync-skill-router-index check-skill-router-health test-skill-router
PYTHON ?= python

help:
	@echo "DeerFlow Development Commands:"
	@echo "  make config          - Generate local config files (aborts if config already exists)"
	@echo "  make config-upgrade  - Merge new fields from config.example.yaml into config.yaml"
	@echo "  make check           - Check if all required tools are installed"
	@echo "  make install         - Install all dependencies (frontend + backend)"
	@echo "  make setup-sandbox   - Pre-pull sandbox container image (recommended)"
	@echo "  make sandbox-build   - Build the custom sandbox image (auto-prefixed by user)"
	@echo "  make docker-install PACKAGE=...        - Install Python packages in running Docker containers"
	@echo "  make docker-install-system SYSTEM=...   - Install system packages in running Docker containers"
	@echo "  make dev             - Start all services in development mode (with hot-reloading)"
	@echo "  make dev-no-nginx    - Start local services without nginx (no sudo needed)"
	@echo "  make stop-no-nginx   - Stop local no-nginx services"
	@echo "  make status-no-nginx - Show local no-nginx service status"
	@echo "  make linux-server-start  - Start server mode on ports 3024/38001/33000"
	@echo "  make linux-server-stop   - Stop server mode on ports 3024/38001/33000"
	@echo "  make linux-server-status - Show server mode status on ports 3024/38001/33000"
	@echo "  make model-services-start  - Start BGE-M3 + SkillRouter embedding/reranker (uses NETWORK_TRAFFIC_*/SKILLROUTER_* envs)"
	@echo "  make model-services-stop   - Stop BGE-M3 + SkillRouter embedding/reranker"
	@echo "  make model-services-status - Show BGE-M3 + SkillRouter embedding/reranker status"
	@echo "  make model-services-dogfood - Start and smoke-test all three model services"
	@echo "  make extract-router-cards   - Extract Router Cards from all SKILL.md files"
	@echo "  make build-skill-router-index - Build Router Card registry and ES index"
	@echo "  make update-skill-router-index SKILL=... - Update one skill's Router Card and ES index"
	@echo "  make check-skill-router-conflicts SKILL=... - Check routing conflicts for one skill"
	@echo "  make eval-skill-router      - Run full SkillRouter evaluation"
	@echo "  make sync-skill-router-index - Sync all skills into SkillRouter index"
	@echo "  make check-skill-router-health - Check ES + embedding + reranker health"
	@echo "  make test-skill-router      - Run SkillRouter test suite"
	@echo "  make dev-daemon      - Start all services in background (daemon mode)"
	@echo "  make start           - Start all services in production mode (optimized, no hot-reloading)"
	@echo "  make stop            - Stop all running services"
	@echo "  make clean           - Clean up processes and temporary files"
	@echo ""
	@echo "Docker Production Commands:"
	@echo "  make up              - Build and start production Docker services (localhost:3328)"
	@echo "  make down            - Stop and remove production Docker containers"
	@echo ""
	@echo "Docker Development Commands:"
	@echo "  make docker-init     - Build the custom k3s image (with pre-cached sandbox image)"
	@echo "  make docker-build-all - Build all Docker images used by DeerFlow"
	@echo "  make docker-start    - Start Docker services (mode-aware from config.yaml, localhost:3328)"
	@echo "  make update-docker-ports-cname - Update Docker/Nginx port config from config.yaml and use current username to rename container name(anker-deer-flow-nginx) and Compose project names (anker-deer-flow) to avoid conflicts when multiple developers on the same machine. This is useful when using Docker development environment with multiple branches or projects."
	@echo "  make docker-stop     - Stop Docker development services"
	@echo "  make docker-logs     - View Docker development logs"
	@echo "  make docker-logs-frontend - View Docker frontend logs"
	@echo "  make docker-logs-gateway - View Docker gateway logs"
	@echo "  make sandbox-build    - Build the custom sandbox image (auto-prefixed by user)"
	@echo "  make docker-build-all - Build all Docker images used by DeerFlow"

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

# Build the custom sandbox image used by AioSandboxProvider
sandbox-build:
	@echo "=========================================="
	@echo "  Building Custom Sandbox Image"
	@echo "=========================================="
	@echo ""
	@SANDBOX_TAG="$${SANDBOX_TAG:-$${USER:-deerflow}}-deerflow-sandbox:network-tools"; \
	docker build -f docker/sandbox/Dockerfile -t "$$SANDBOX_TAG" . && \
	echo "" && \
	echo "✓ Custom sandbox image built: $$SANDBOX_TAG" || \
	(echo "" && echo "✗ Sandbox build failed" && exit 1)
	@echo ""

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

# Start the three local model services used for embedding and reranking
model-services-start:
	@./scripts/start-model-services.sh

# Stop the three local model services used for embedding and reranking
model-services-stop:
	@./scripts/stop-model-services.sh

# Show the status of the three local model services
model-services-status:
	@./scripts/model-services-status.sh

# Start and smoke-test the three local model services
model-services-dogfood:
	@./scripts/dogfood_skillrouter_models.sh

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

# Build all Docker images used by DeerFlow
docker-build-all:
	@./scripts/docker.sh build-all

# Start Docker development environment
docker-start:
	@./scripts/docker.sh start

# Install Python packages in running containers
# Usage: make docker-install PACKAGE=duckdb
#        make docker-install PACKAGE="duckdb openpyxl pyyaml"
docker-install:
ifndef PACKAGE
	@echo "Usage: make docker-install PACKAGE=<package1> [package2] ..."
	@echo "Example: make docker-install PACKAGE=\"duckdb openpyxl pyyaml\""
else
	@./scripts/docker.sh install $(PACKAGE)
endif

# Install system (apt) packages in running containers
# Usage: make docker-install-system SYSTEM=vim
#        make docker-install-system SYSTEM="curl wget htop"
docker-install-system:
ifndef SYSTEM
	@echo "Usage: make docker-install-system SYSTEM=<pkg1> [pkg2] ..."
	@echo "Example: make docker-install-system SYSTEM=\"vim curl htop\""
else
	@./scripts/docker.sh install-system $(SYSTEM)
endif

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

sandbox-build:
	@./scripts/docker.sh sandbox-build
docker-build-all:
	@./scripts/docker.sh build-all

# ==========================================
# Production Docker Commands
# ==========================================

# Build and start production services
up:
	@./scripts/deploy.sh

# Stop and remove production containers
down:
	@./scripts/deploy.sh down

# ==========================================
# SkillRouter Commands
# ==========================================

# Load .env for SkillRouter targets (NETWORK_TRAFFIC_ES_INDEX, SKILL_ROUTER_ES_INDEX, ES_URL, ES_USERNAME, ES_PASSWORD, etc.)
define load-env
	@set -a; [ -f .env ] && . ./.env; set +a
endef

# Extract Router Cards from all SKILL.md files
extract-router-cards:
	@$(PYTHON) scripts/extract_router_cards.py

# Build full SkillRouter Elasticsearch index (first-time / bulk rebuild)
build-skill-router-index: extract-router-cards
	@set -a; [ -f .env ] && . ./.env; set +a; $(PYTHON) scripts/build_skill_router_registry.py
	@set -a; [ -f .env ] && . ./.env; set +a; $(PYTHON) scripts/build_skill_router_es_index.py

# Update a single Skill's Router Card and ES index
update-skill-router-index:
	@set -a; [ -f .env ] && . ./.env; set +a; $(PYTHON) scripts/update_skill_router_index.py $(SKILL)

# Check routing conflicts for a single Skill
check-skill-router-conflicts:
	@set -a; [ -f .env ] && . ./.env; set +a; $(PYTHON) scripts/check_skill_router_conflicts.py $(SKILL)

# Run full SkillRouter evaluation
eval-skill-router:
	@set -a; [ -f .env ] && . ./.env; set +a; $(PYTHON) scripts/eval_skill_router.py

# Sync all skills into SkillRouter index (repair / manual sync)
sync-skill-router-index:
	@$(PYTHON) -c "\
from deerflow.routing.index_updater import update_single_skill_index;\
from pathlib import Path;\
import json;\
root = Path('skills');\
results = [];\
for cat in ('custom', 'public'):\
    cd = root / cat;\
    [results.append(update_single_skill_index(d.name, skill_dir=d, skills_root=root).__dict__) for d in sorted(cd.iterdir()) if d.is_dir() and (d / 'SKILL.md').exists()];\
print(json.dumps(results, indent=2, ensure_ascii=False))\
" PYTHONPATH=backend/packages/harness:scripts

# Check SkillRouter service health (ES + embedding + reranker)
# Check SkillRouter service health (ES + embedding + reranker)
check-skill-router-health:
	@set -a; [ -f .env ] && . ./.env; set +a; PYTHONPATH=backend/packages/harness $(PYTHON) scripts/check_skill_router_health.py

# Run SkillCreator router update and conflict detection tests
test-skill-router:
	@PYTHONPATH=scripts:backend/packages/harness python3 -m pytest backend/tests/test_skill_creator_router_update.py backend/tests/test_skill_router_conflicts.py backend/tests/test_index_updater.py backend/tests/test_routing_metrics.py backend/tests/test_eval_skill_router.py backend/tests/test_skill_router_gateway.py -v
