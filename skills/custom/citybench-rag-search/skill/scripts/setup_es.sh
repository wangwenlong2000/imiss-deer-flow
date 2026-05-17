#!/usr/bin/env bash
# ============================================================
# Elasticsearch 8.x 一键安装部署脚本（tar.gz 裸装，非 Docker）
# 包含：下载 → 解压 → 配置单节点 → 安装 IK 分词插件 → 启停管理
# ============================================================
set -euo pipefail

# ──── 可配置参数 ────
ES_VERSION="${ES_VERSION:-8.17.0}"
ES_HOME="${ES_HOME:-$HOME/elasticsearch}"
ES_DOWNLOAD_URL="https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${ES_VERSION}-linux-x86_64.tar.gz"
IK_PLUGIN_URL="https://get.infini.cloud/elasticsearch/analysis-ik/${ES_VERSION}"
ES_PORT="${ES_PORT:-9200}"
ES_HEAP_SIZE="${ES_HEAP_SIZE:-1g}"

ACTION="${1:-install}"

info()  { echo -e "\033[32m[INFO]\033[0m  $*"; }
warn()  { echo -e "\033[33m[WARN]\033[0m  $*"; }
error() { echo -e "\033[31m[ERROR]\033[0m $*"; exit 1; }

# ──────────────── 安装 ────────────────
do_install() {
    info "开始安装 Elasticsearch ${ES_VERSION}..."

    # 检查 Java
    if ! command -v java &>/dev/null; then
        error "未检测到 Java，请先安装 OpenJDK 21"
    fi
    JAVA_VER=$(java -version 2>&1 | head -1)
    info "Java 版本: ${JAVA_VER}"

    # 下载 ES
    if [ -d "${ES_HOME}" ]; then
        warn "目录 ${ES_HOME} 已存在，跳过下载解压"
    else
        TARBALL="/tmp/elasticsearch-${ES_VERSION}.tar.gz"
        if [ ! -f "${TARBALL}" ]; then
            info "下载 ES: ${ES_DOWNLOAD_URL}"
            curl -L -o "${TARBALL}" "${ES_DOWNLOAD_URL}"
        else
            info "使用已缓存的安装包: ${TARBALL}"
        fi

        info "解压到 ${ES_HOME}..."
        mkdir -p "$(dirname "${ES_HOME}")"
        tar -xzf "${TARBALL}" -C "$(dirname "${ES_HOME}")"
        mv "$(dirname "${ES_HOME}")/elasticsearch-${ES_VERSION}" "${ES_HOME}"
        info "解压完成"
    fi

    # 配置单节点模式（关闭 xpack security 简化开发）
    ES_YML="${ES_HOME}/config/elasticsearch.yml"
    info "配置 ES: ${ES_YML}"
    cat > "${ES_YML}" <<EOF
# ──── CityBench 单节点开发配置 ────
cluster.name: citybench-cluster
node.name: citybench-node-1
path.data: ${ES_HOME}/data
path.logs: ${ES_HOME}/logs
network.host: 0.0.0.0
http.port: ${ES_PORT}

# 单节点模式
discovery.type: single-node

# 关闭安全认证（仅开发环境）
xpack.security.enabled: false
xpack.security.enrollment.enabled: false
xpack.security.http.ssl.enabled: false
xpack.security.transport.ssl.enabled: false

# 性能调优
indices.memory.index_buffer_size: 20%
EOF

    # JVM 堆内存配置
    JVM_OPTIONS="${ES_HOME}/config/jvm.options.d/custom.options"
    mkdir -p "$(dirname "${JVM_OPTIONS}")"
    cat > "${JVM_OPTIONS}" <<EOF
-Xms${ES_HEAP_SIZE}
-Xmx${ES_HEAP_SIZE}
EOF
    info "JVM 堆内存设置为: ${ES_HEAP_SIZE}"

    # 安装 IK 中文分词插件
    info "安装 IK 中文分词插件..."
    if [ -d "${ES_HOME}/plugins/analysis-ik" ]; then
        warn "IK 插件已安装，跳过"
    else
        "${ES_HOME}/bin/elasticsearch-plugin" install -b "${IK_PLUGIN_URL}" || {
            warn "IK 插件安装失败，尝试备用地址..."
            IK_BACKUP_URL="https://release.infinilabs.com/analysis-ik/stable/elasticsearch-analysis-ik-${ES_VERSION}.zip"
            "${ES_HOME}/bin/elasticsearch-plugin" install -b "${IK_BACKUP_URL}" || {
                warn "IK 插件安装失败，BM25 将使用默认分词器"
            }
        }
    fi

    info "=========================================="
    info "Elasticsearch ${ES_VERSION} 安装完成！"
    info "ES_HOME: ${ES_HOME}"
    info "启动命令: $0 start"
    info "=========================================="
}

# ──────────────── 启动 ────────────────
do_start() {
    info "启动 Elasticsearch..."
    if curl -s "http://localhost:${ES_PORT}" &>/dev/null; then
        warn "ES 已在运行中 (端口 ${ES_PORT})"
        return 0
    fi

    # 以后台方式启动
    nohup "${ES_HOME}/bin/elasticsearch" -d -p "${ES_HOME}/es.pid" > /dev/null 2>&1 &

    # 等待启动
    info "等待 ES 启动..."
    for i in $(seq 1 60); do
        if curl -s "http://localhost:${ES_PORT}" &>/dev/null; then
            info "ES 启动成功！(耗时 ${i} 秒)"
            curl -s "http://localhost:${ES_PORT}" | python3 -m json.tool 2>/dev/null || true
            return 0
        fi
        sleep 1
    done
    error "ES 启动超时（60秒），请检查日志: ${ES_HOME}/logs/"
}

# ──────────────── 停止 ────────────────
do_stop() {
    info "停止 Elasticsearch..."
    PID_FILE="${ES_HOME}/es.pid"
    if [ -f "${PID_FILE}" ]; then
        PID=$(cat "${PID_FILE}")
        if kill -0 "${PID}" 2>/dev/null; then
            kill "${PID}"
            info "已发送停止信号 (PID: ${PID})"
            # 等待进程退出
            for i in $(seq 1 30); do
                if ! kill -0 "${PID}" 2>/dev/null; then
                    info "ES 已停止"
                    rm -f "${PID_FILE}"
                    return 0
                fi
                sleep 1
            done
            warn "等待超时，强制终止..."
            kill -9 "${PID}" 2>/dev/null || true
            rm -f "${PID_FILE}"
        else
            warn "PID ${PID} 不存在"
            rm -f "${PID_FILE}"
        fi
    else
        # 尝试通过端口查找进程
        PID=$(lsof -ti:${ES_PORT} 2>/dev/null || true)
        if [ -n "${PID}" ]; then
            kill "${PID}" 2>/dev/null || true
            info "已停止端口 ${ES_PORT} 上的进程 (PID: ${PID})"
        else
            warn "未找到运行中的 ES 进程"
        fi
    fi
}

# ──────────────── 状态 ────────────────
do_status() {
    if curl -s "http://localhost:${ES_PORT}" &>/dev/null; then
        info "ES 正在运行 (端口 ${ES_PORT})"
        curl -s "http://localhost:${ES_PORT}/_cluster/health?pretty" 2>/dev/null || true
    else
        warn "ES 未运行"
    fi
}

# ──────────────── 主入口 ────────────────
case "${ACTION}" in
    install)  do_install ;;
    start)    do_start ;;
    stop)     do_stop ;;
    restart)  do_stop; sleep 2; do_start ;;
    status)   do_status ;;
    *)
        echo "用法: $0 {install|start|stop|restart|status}"
        echo "  install  - 下载安装 ES + IK 插件"
        echo "  start    - 启动 ES"
        echo "  stop     - 停止 ES"
        echo "  restart  - 重启 ES"
        echo "  status   - 查看 ES 状态"
        exit 1
        ;;
esac
