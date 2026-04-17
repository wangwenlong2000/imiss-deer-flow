FROM enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest

ARG CONDA_DIR=/opt/conda
ARG CONDA_ENV_NAME=deerflow-street
ARG http_proxy
ARG https_proxy
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG all_proxy
ARG ALL_PROXY
ARG no_proxy
ARG NO_PROXY

USER root

# Install Miniforge and create a dedicated conda env for skill dependencies.
RUN export http_proxy="$http_proxy" https_proxy="$https_proxy" HTTP_PROXY="$HTTP_PROXY" HTTPS_PROXY="$HTTPS_PROXY" all_proxy="$all_proxy" ALL_PROXY="$ALL_PROXY" no_proxy="$no_proxy" NO_PROXY="$NO_PROXY"; \
    apt-get update && apt-get install -y --no-install-recommends \
    bash \
    bzip2 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN export http_proxy="$http_proxy" https_proxy="$https_proxy" HTTP_PROXY="$HTTP_PROXY" HTTPS_PROXY="$HTTPS_PROXY" all_proxy="$all_proxy" ALL_PROXY="$ALL_PROXY" no_proxy="$no_proxy" NO_PROXY="$NO_PROXY"; \
    set -e; \
        if command -v conda >/dev/null 2>&1; then \
            echo "conda already exists in base image, skip installer download"; \
            exit 0; \
        fi; \
        ARCH="$(uname -m)"; \
        case "$ARCH" in \
            x86_64) \
                URLS="\
https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh \
https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-latest-Linux-x86_64.sh\
" ;; \
            aarch64|arm64) \
                URLS="\
https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh \
https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-latest-Linux-aarch64.sh\
" ;; \
            *) echo "Unsupported architecture: $ARCH" && exit 1 ;; \
        esac; \
        success=0; \
        for url in $URLS; do \
            echo "trying installer: $url"; \
            if curl -fL --retry 5 --retry-delay 3 --connect-timeout 20 --max-time 600 -o /tmp/conda-installer.sh "$url"; then \
                success=1; \
                break; \
            fi; \
        done; \
        if [ "$success" -ne 1 ]; then \
            echo "failed to download conda installer from all configured urls"; \
            exit 1; \
        fi; \
        bash /tmp/conda-installer.sh -b -p "$CONDA_DIR" && \
        rm -f /tmp/conda-installer.sh

ENV PATH=${CONDA_DIR}/bin:${PATH}

RUN export http_proxy="$http_proxy" https_proxy="$https_proxy" HTTP_PROXY="$HTTP_PROXY" HTTPS_PROXY="$HTTPS_PROXY" all_proxy="$all_proxy" ALL_PROXY="$ALL_PROXY" no_proxy="$no_proxy" NO_PROXY="$NO_PROXY"; \
    conda create -y -n "$CONDA_ENV_NAME" python=3.12 pip && \
    conda clean -afy

# Install location-matcher dependencies from pyproject.toml only.
COPY skills/custom/location-matcher/pyproject.toml /tmp/location-matcher/pyproject.toml
RUN export http_proxy="$http_proxy" https_proxy="$https_proxy" HTTP_PROXY="$HTTP_PROXY" HTTPS_PROXY="$HTTPS_PROXY" all_proxy="$all_proxy" ALL_PROXY="$ALL_PROXY" no_proxy="$no_proxy" NO_PROXY="$NO_PROXY"; \
    conda run -n "$CONDA_ENV_NAME" pip install --no-cache-dir /tmp/location-matcher && \
    rm -rf /tmp/location-matcher

ENV CONDA_DEFAULT_ENV=${CONDA_ENV_NAME}
ENV PATH=${CONDA_DIR}/envs/${CONDA_ENV_NAME}/bin:${CONDA_DIR}/bin:${PATH}
