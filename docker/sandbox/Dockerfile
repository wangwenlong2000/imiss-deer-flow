FROM enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest

USER root

ARG HF_ENDPOINT=https://hf-mirror.com
ENV HF_ENDPOINT=${HF_ENDPOINT}

RUN apt-get update \
   && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
     ca-certificates \
     curl \
     gnupg2 \
  tshark \
  ffmpeg \
  imagemagick \
  python3-pip \
  libglib2.0-0 \
   && curl -fsSL https://download.opensuse.org/repositories/security:zeek/xUbuntu_22.04/Release.key \
     | gpg --dearmor -o /etc/apt/trusted.gpg.d/zeek.gpg \
   && echo "deb https://download.opensuse.org/repositories/security:/zeek/xUbuntu_22.04/ /" \
     > /etc/apt/sources.list.d/zeek.list \
  && apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends zeek \
  && python3 -m pip install --no-cache-dir opencv-python-headless ultralytics sentence-transformers \
   && ln -sf /opt/zeek/bin/zeek /usr/local/bin/zeek \
   && ln -sf /opt/zeek/bin/zeekctl /usr/local/bin/zeekctl \
   && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/models \
  && python3 -c 'from sentence_transformers import SentenceTransformer; SentenceTransformer("BAAI/bge-m3").save("/opt/models/bge-m3")'
