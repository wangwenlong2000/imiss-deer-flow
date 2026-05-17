FROM enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest

USER root

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       nodejs \
       python3-pip \
       fontconfig \
       fonts-noto-cjk \
    && python3 -m pip install --no-cache-dir \
       dashscope \
       numpy \
       pandas \
       duckdb \
       xlrd \
       openpyxl \
       pyyaml \
       requests \
    && mkdir -p /opt/node-deps \
    && npm install --prefix /opt/node-deps cheerio \
    && rm -rf /var/lib/apt/lists/*


ENV NODE_PATH=/opt/node-deps/node_modules    
# 可选：如果你希望镜像里直接预装 Playwright 浏览器系统依赖
RUN npx -y playwright install-deps chromium