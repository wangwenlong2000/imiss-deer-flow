FROM enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest

USER root

ENV DEBIAN_FRONTEND=noninteractive
ENV NODE_PATH=/opt/node-deps/node_modules

# Install system dependencies, Python packages, Node packages, and Zeek.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       curl \
       gnupg \
       nodejs \
       python3-pip \
       fontconfig \
       fonts-noto-cjk \
       tshark \
    \
    # Base image is Ubuntu 22.04, so use the matching Zeek repo.
    && curl -fsSL https://download.opensuse.org/repositories/security:/zeek/xUbuntu_22.04/Release.key \
       | gpg --dearmor -o /etc/apt/trusted.gpg.d/zeek.gpg \
    && echo "deb https://download.opensuse.org/repositories/security:/zeek/xUbuntu_22.04/ /" \
       > /etc/apt/sources.list.d/zeek.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends zeek \
    \
    && python3 -m pip install --no-cache-dir \
       dashscope \
       numpy \
       pandas \
       duckdb \
       xlrd \
       openpyxl \
       pyyaml \
       requests \
       pytz \
       scikit-learn \
       rrcf \
       openai \
       elasticsearch \
    \
    && mkdir -p /opt/node-deps \
    && npm install --prefix /opt/node-deps cheerio \
    \
    && ln -sf /opt/zeek/bin/zeek /usr/local/bin/zeek \
    && ln -sf /opt/zeek/bin/zeekctl /usr/local/bin/zeekctl \
    \
    && rm -rf /var/lib/apt/lists/*

# Optional: install Playwright browser system dependencies.
RUN npx -y playwright install-deps chromium