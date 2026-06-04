FROM enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest

USER root

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    NODE_PATH=/opt/node-deps/node_modules \
    GDAL_DATA=/usr/share/gdal \
    PROJ_LIB=/usr/share/proj \
    VIRTUAL_ENV=/opt/rs-venv \
    PATH="/opt/rs-venv/bin:${PATH}"

RUN python3 --version \
    && python3 -m pip --version || python3 -m ensurepip --upgrade

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       curl \
       gnupg \
       gnupg2 \
       nodejs \
       python3 \
       python3-pip \
       python3-venv \
       python3-dev \
       python3-gdal \
       build-essential \
       pkg-config \
       fontconfig \
       fonts-noto-cjk \
       tshark \
       ffmpeg \
       imagemagick \
       gdal-bin \
       libgdal-dev \
       proj-bin \
       proj-data \
       libproj-dev \
       libgeos-dev \
       libspatialindex-dev \
       libgl1 \
       libglib2.0-0 \
       libsm6 \
       libxext6 \
       libxrender1 \
    \
    && curl -fsSL https://download.opensuse.org/repositories/security:/zeek/xUbuntu_22.04/Release.key \
       | gpg --dearmor -o /etc/apt/trusted.gpg.d/zeek.gpg \
    && echo "deb https://download.opensuse.org/repositories/security:/zeek/xUbuntu_22.04/ /" \
       > /etc/apt/sources.list.d/zeek.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends zeek \
    \
    && ln -sf /opt/zeek/bin/zeek /usr/local/bin/zeek \
    && ln -sf /opt/zeek/bin/zeekctl /usr/local/bin/zeekctl \
    \
    && python3 -m venv --system-site-packages /opt/rs-venv \
    && /opt/rs-venv/bin/python -m pip install --no-cache-dir --upgrade pip setuptools wheel \
    && /opt/rs-venv/bin/python -m pip install --no-cache-dir \
       dashscope \
       numpy \
       scipy \
       pandas \
       pyarrow \
       duckdb \
       xlrd \
       openpyxl \
       pyyaml \
       PyYAML \
       requests \
       python-dotenv>=1.0 \
       statsforecast>=1.7.8 \
       adtk>=0.6.2 \
       pytz \
       scikit-learn \
       rrcf \
       openai \
       elasticsearch \
       networkx \
       opencv-python-headless \
       ultralytics \
       rasterio \
       shapely \
       python-docx \
       pytest \
       geopandas \
       pyogrio \
       fiona \
       geoai-py \
    \
    && mkdir -p /opt/node-deps \
    && npm install --prefix /opt/node-deps cheerio \
    \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /root/.cache/pip

RUN npx -y playwright install-deps chromium \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*