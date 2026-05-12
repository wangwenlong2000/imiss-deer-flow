FROM enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest

USER root

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       ca-certificates \
       curl \
       gnupg2 \
       tshark \
    # Base image is Ubuntu 22.04, so use the matching Zeek repo.
    && curl -fsSL https://download.opensuse.org/repositories/security:zeek/xUbuntu_22.04/Release.key \
       | gpg --dearmor -o /etc/apt/trusted.gpg.d/zeek.gpg \
    && echo "deb https://download.opensuse.org/repositories/security:/zeek/xUbuntu_22.04/ /" \
       > /etc/apt/sources.list.d/zeek.list \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends zeek \
   && python -m pip install --no-cache-dir \
      duckdb \
      pyyaml \
      openpyxl \
      pytz \
      numpy \
      scikit-learn \
      rrcf \
      openai \
      elasticsearch \
    && ln -sf /opt/zeek/bin/zeek /usr/local/bin/zeek \
    && ln -sf /opt/zeek/bin/zeekctl /usr/local/bin/zeekctl \
    && rm -rf /var/lib/apt/lists/*
