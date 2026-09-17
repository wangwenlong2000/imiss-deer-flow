#!/usr/bin/env bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Docker Compose command with project name
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
COMPOSE_CMD="docker compose -p gaozhuohui-deer-flow-dev -f docker-compose-dev.yaml"

read_env_value() {
    local key="$1"
    local env_file="$PROJECT_ROOT/.env"
    if [ ! -f "$env_file" ]; then
        return
    fi
    awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$env_file"
}

# Keep the build, startup preflight, and runtime configuration on one exact
# local image tag. An explicitly exported value wins over the project .env.
if [ -z "${DEER_FLOW_SANDBOX_IMAGE:-}" ]; then
    DEER_FLOW_SANDBOX_IMAGE="$(read_env_value DEER_FLOW_SANDBOX_IMAGE)"
fi
if [ -z "$DEER_FLOW_SANDBOX_IMAGE" ]; then
    RAW_USER="${SUDO_USER:-${USER:-deerflow}}"
    IMAGE_PREFIX="$(printf '%s' "$RAW_USER" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g; s/-\{2,\}/-/g; s/^-//; s/-$//')"
    DEER_FLOW_SANDBOX_IMAGE="${IMAGE_PREFIX:-deerflow}-deerflow-sandbox:policies-regulations-tools"
fi
export DEER_FLOW_SANDBOX_IMAGE

detect_sandbox_mode() {
    local config_file="$PROJECT_ROOT/config.yaml"
    local sandbox_use=""
    local provisioner_url=""

    if [ ! -f "$config_file" ]; then
        echo "local"
        return
    fi

    sandbox_use=$(awk '
        /^[[:space:]]*sandbox:[[:space:]]*$/ { in_sandbox=1; next }
        in_sandbox && /^[^[:space:]#]/ { in_sandbox=0 }
        in_sandbox && /^[[:space:]]*use:[[:space:]]*/ {
            line=$0
            sub(/^[[:space:]]*use:[[:space:]]*/, "", line)
            print line
            exit
        }
    ' "$config_file")

    provisioner_url=$(awk '
        /^[[:space:]]*sandbox:[[:space:]]*$/ { in_sandbox=1; next }
        in_sandbox && /^[^[:space:]#]/ { in_sandbox=0 }
        in_sandbox && /^[[:space:]]*provisioner_url:[[:space:]]*/ {
            line=$0
            sub(/^[[:space:]]*provisioner_url:[[:space:]]*/, "", line)
            print line
            exit
        }
    ' "$config_file")

    if [[ "$sandbox_use" == *"deerflow.sandbox.local:LocalSandboxProvider"* ]]; then
        echo "local"
    elif [[ "$sandbox_use" == *"deerflow.community.aio_sandbox:AioSandboxProvider"* ]]; then
        if [ -n "$provisioner_url" ]; then
            echo "provisioner"
        else
            echo "aio"
        fi
    else
        echo "local"
    fi
}

# Cleanup function for Ctrl+C
cleanup() {
    echo ""
    echo -e "${YELLOW}Operation interrupted by user${NC}"
    exit 130
}

# Set up trap for Ctrl+C
trap cleanup INT TERM

# Initialize: pre-pull the sandbox image so first Pod startup is fast
init() {
    echo "=========================================="
    echo "  DeerFlow Init — Pull Sandbox Image"
    echo "=========================================="
    echo ""

    SANDBOX_IMAGE="enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest"

    if ! docker images --format '{{.Repository}}:{{.Tag}}' | grep -q "^${SANDBOX_IMAGE}$"; then
        echo -e "${BLUE}Pulling sandbox image: $SANDBOX_IMAGE ...${NC}"
        docker pull "$SANDBOX_IMAGE"
    else
        echo -e "${GREEN}Sandbox image already exists locally: $SANDBOX_IMAGE${NC}"
    fi

    echo ""
    echo -e "${GREEN}✓ Sandbox image is ready.${NC}"
    echo ""
    echo -e "${YELLOW}Next step: make docker-start${NC}"
}

sandbox_image_exists() {
    docker image inspect "$DEER_FLOW_SANDBOX_IMAGE" >/dev/null 2>&1
}

# Build the custom AIO image once. Runtime sandbox creation is configured to
# use only this local image and never pull from a registry.
sandbox_build() {
    echo "=========================================="
    echo "  Building Custom Sandbox Image"
    echo "=========================================="
    echo ""
    echo -e "${BLUE}DEER_FLOW_SANDBOX_IMAGE=$DEER_FLOW_SANDBOX_IMAGE${NC}"
    echo ""

    docker build -f "$PROJECT_ROOT/docker/sandbox/Dockerfile" -t "$DEER_FLOW_SANDBOX_IMAGE" "$PROJECT_ROOT"

    echo ""
    echo -e "${GREEN}✓ Custom sandbox image built: $DEER_FLOW_SANDBOX_IMAGE${NC}"
    echo ""
}

build_all() {
    echo "=========================================="
    echo "  Building All Docker Images"
    echo "=========================================="
    echo ""

    sandbox_build
    export DEER_FLOW_ROOT="$PROJECT_ROOT"
    echo -e "${BLUE}Building app images...${NC}"
    cd "$DOCKER_DIR" && $COMPOSE_CMD build

    echo ""
    echo -e "${GREEN}✓ All images built${NC}"
    echo ""
}

# Start Docker development environment
start() {
    local sandbox_mode
    local services

    echo "=========================================="
    echo "  Starting DeerFlow Docker Development"
    echo "=========================================="
    echo ""

    sandbox_mode="$(detect_sandbox_mode)"

    if [ "$sandbox_mode" = "provisioner" ]; then
        services="frontend gateway langgraph provisioner nginx"
    else
        services="frontend gateway langgraph nginx"
    fi

    echo -e "${BLUE}Detected sandbox mode: $sandbox_mode${NC}"
    echo -e "${BLUE}DEER_FLOW_SANDBOX_IMAGE=$DEER_FLOW_SANDBOX_IMAGE${NC}"
    if [ "$sandbox_mode" = "aio" ] && ! sandbox_image_exists; then
        echo -e "${YELLOW}✗ Local AIO sandbox image is missing: $DEER_FLOW_SANDBOX_IMAGE${NC}"
        echo -e "${YELLOW}  Build it once with: make sandbox-build${NC}"
        echo -e "${YELLOW}  Runtime pulling is disabled by design.${NC}"
        return 1
    fi
    if [ "$sandbox_mode" = "provisioner" ]; then
        echo -e "${BLUE}Provisioner enabled (Kubernetes mode).${NC}"
    else
        echo -e "${BLUE}Provisioner disabled (not required for this sandbox mode).${NC}"
    fi
    echo ""
    
    # Always bind host paths to this checkout. An inherited value from another
    # DeerFlow checkout would otherwise make DooD sandboxes mount stale data,
    # skills, and thread storage from that project.
    export DEER_FLOW_ROOT="$PROJECT_ROOT"
    echo -e "${BLUE}Setting DEER_FLOW_ROOT=$DEER_FLOW_ROOT${NC}"
    echo ""
    
    # Ensure config.yaml exists before starting.
    if [ ! -f "$PROJECT_ROOT/config.yaml" ]; then
        if [ -f "$PROJECT_ROOT/config.example.yaml" ]; then
            cp "$PROJECT_ROOT/config.example.yaml" "$PROJECT_ROOT/config.yaml"
            echo ""
            echo -e "${YELLOW}============================================================${NC}"
            echo -e "${YELLOW}  config.yaml has been created from config.example.yaml.${NC}"
            echo -e "${YELLOW}  Please edit config.yaml to set your API keys and model   ${NC}"
            echo -e "${YELLOW}  configuration before starting DeerFlow.                  ${NC}"
            echo -e "${YELLOW}============================================================${NC}"
            echo ""
            echo -e "${YELLOW}  Edit the file:  $PROJECT_ROOT/config.yaml${NC}"
            echo -e "${YELLOW}  Then run:        make docker-start${NC}"
            echo ""
            exit 0
        else
            echo -e "${YELLOW}✗ config.yaml not found and no config.example.yaml to copy from.${NC}"
            exit 1
        fi
    fi

    # Ensure extensions_config.json exists as a file before mounting.
    # Docker creates a directory when bind-mounting a non-existent host path.
    if [ ! -f "$PROJECT_ROOT/extensions_config.json" ]; then
        if [ -f "$PROJECT_ROOT/extensions_config.example.json" ]; then
            cp "$PROJECT_ROOT/extensions_config.example.json" "$PROJECT_ROOT/extensions_config.json"
            echo -e "${BLUE}Created extensions_config.json from example${NC}"
        else
            echo "{}" > "$PROJECT_ROOT/extensions_config.json"
            echo -e "${BLUE}Created empty extensions_config.json${NC}"
        fi
    fi

    echo "Starting containers with existing images..."
    cd "$DOCKER_DIR" && $COMPOSE_CMD up -d --no-build --remove-orphans $services

    # The frontend and gateway may be recreated with new bridge-network IPs
    # during `up`, while an unchanged nginx container keeps its old
    # resolved upstream addresses. Restart nginx so it resolves the current
    # service IPs before users access the configured Nginx port.
    $COMPOSE_CMD restart nginx

    # Compose reports a service as started when its process has been spawned,
    # but Next.js still needs a few seconds before it accepts HTTP requests.
    # Do not return success from `make docker-start` until the presentation
    # frontend is actually reachable through nginx.
    local ui_ready=false
    for _ in $(seq 1 30); do
        if curl --fail --silent --max-time 2 http://127.0.0.1:3328/ >/dev/null; then
            ui_ready=true
            break
        fi
        sleep 1
    done
    if [ "$ui_ready" != "true" ]; then
        echo -e "${YELLOW}Frontend did not become ready on port 3328 within 30 seconds.${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Frontend is ready on port 3328${NC}"
    echo ""
    echo "=========================================="
    echo "  DeerFlow Docker is starting!"
    echo "=========================================="
    echo ""
    echo "  🌐 Application: http://localhost:3328"
    echo "  📡 API Gateway: http://localhost:3328/api/*"
    echo "  🤖 LangGraph:   http://localhost:3328/api/langgraph/*"
    echo ""
    echo "  📋 View logs: make docker-logs"
    echo "  🛑 Stop:      make docker-stop"
    echo ""
}

# View Docker development logs
logs() {
    local service=""
    
    case "$1" in
        --frontend)
            service="frontend"
            echo -e "${BLUE}Viewing frontend logs...${NC}"
            ;;
        --gateway)
            service="gateway"
            echo -e "${BLUE}Viewing gateway logs...${NC}"
            ;;
        --nginx)
            service="nginx"
            echo -e "${BLUE}Viewing nginx logs...${NC}"
            ;;
        --provisioner)
            service="provisioner"
            echo -e "${BLUE}Viewing provisioner logs...${NC}"
            ;;
        "")
            echo -e "${BLUE}Viewing all logs...${NC}"
            ;;
        *)
            echo -e "${YELLOW}Unknown option: $1${NC}"
            echo "Usage: $0 logs [--frontend|--gateway|--nginx|--provisioner]"
            exit 1
            ;;
    esac
    
    cd "$DOCKER_DIR" && $COMPOSE_CMD logs -f $service
}

# Stop Docker development environment
stop() {
    # DEER_FLOW_ROOT is referenced in docker-compose-dev.yaml; set it before
    # running compose down to suppress "variable is not set" warnings.
    if [ -z "$DEER_FLOW_ROOT" ]; then
        export DEER_FLOW_ROOT="$PROJECT_ROOT"
    fi
    echo "Stopping Docker development services..."
    cd "$DOCKER_DIR" && $COMPOSE_CMD down
    echo "Cleaning up sandbox containers..."
    "$SCRIPT_DIR/cleanup-containers.sh" deer-flow-sandbox 2>/dev/null || true
    echo -e "${GREEN}✓ Docker services stopped${NC}"
}

# Restart Docker development environment
restart() {
    echo "========================================"
    echo "  Restarting DeerFlow Docker Services"
    echo "========================================"
    echo ""
    echo -e "${BLUE}Restarting containers...${NC}"
    cd "$DOCKER_DIR" && $COMPOSE_CMD restart
    echo ""
    echo -e "${GREEN}✓ Docker services restarted${NC}"
    echo ""
    echo "  🌐 Application: http://localhost:3328"
    echo "  📋 View logs: make docker-dev-logs"
    echo ""
}

# Show help
help() {
    echo "DeerFlow Docker Management Script"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  init          - Pull the sandbox image (speeds up first Pod startup)"
    echo "  sandbox-build - Build the configured custom sandbox image"
    echo "  build-all     - Build the sandbox and all DeerFlow service images"
    echo "  start         - Start Docker services (auto-detects sandbox mode from config.yaml)"
    echo "  restart       - Restart all running Docker services"
    echo "  logs [option] - View Docker development logs"
    echo "                  --frontend   View frontend logs only"
    echo "                  --gateway    View gateway logs only"
    echo "                  --nginx      View nginx logs only"
    echo "                  --provisioner View provisioner logs only"
    echo "  stop          - Stop Docker development services"
    echo "  help          - Show this help message"
    echo ""
}

main() {
    # Main command dispatcher
    case "$1" in
        init)
            init
            ;;
        sandbox-build)
            sandbox_build
            ;;
        build-all)
            build_all
            ;;
        start)
            start
            ;;
        restart)
            restart
            ;;
        logs)
            logs "$2"
            ;;
        stop)
            stop
            ;;
        help|--help|-h|"")
            help
            ;;
        *)
            echo -e "${YELLOW}Unknown command: $1${NC}"
            echo ""
            help
            exit 1
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
