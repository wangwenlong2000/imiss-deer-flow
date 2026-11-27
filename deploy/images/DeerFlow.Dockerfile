FROM docker:29-cli AS dockercli
FROM python:3.12-slim
ENV UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DEER_FLOW_HOME=/data/deerflow
RUN python -m pip install --no-cache-dir uv==0.7.20
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
WORKDIR /app
COPY backend/pyproject.toml backend/uv.lock ./backend/
COPY backend/README.md ./backend/README.md
COPY backend/packages/harness/pyproject.toml ./backend/packages/harness/pyproject.toml
COPY backend/packages/harness/deerflow ./backend/packages/harness/deerflow
COPY backend/app ./backend/app
COPY backend/langgraph.json ./backend/langgraph.json
RUN cd backend && uv sync --frozen --no-dev
COPY deploy /opt/citybrain/deploy
COPY config/compliance ./config/compliance
ENV PYTHONPATH=/app/backend:/app/backend/packages/harness:/opt/citybrain
ENV PATH=/app/backend/.venv/bin:$PATH
WORKDIR /app/backend
