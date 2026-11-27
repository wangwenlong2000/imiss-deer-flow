ARG PYTHON_IMAGE=python:3.13-slim
FROM ${PYTHON_IMAGE}
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app:/opt/citybrain
WORKDIR /app
COPY apps/onecityagent/requirements-core.lock /tmp/requirements-core.lock
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/requirements-core.lock \
    && python -m pip check
COPY apps/onecityagent/onecityagent /app/onecityagent
COPY deploy /opt/citybrain/deploy
ENTRYPOINT ["python", "/opt/citybrain/deploy/entrypoints/run.py"]
CMD ["gunicorn", "onecityagent.entrypoints.api:app", "--bind", "0.0.0.0:7606", "--workers", "1", "--threads", "8", "--timeout", "1800", "--access-logfile", "-", "--access-logformat", "%(m)s %(U)s %(s)s"]
