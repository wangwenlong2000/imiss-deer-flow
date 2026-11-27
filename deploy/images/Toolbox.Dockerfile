FROM docker:29-cli AS dockercli
FROM python:3.12-slim
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=dockercli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose
COPY deploy/requirements-toolbox.lock /tmp/requirements-toolbox.lock
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/requirements-toolbox.lock \
    && python -m pip check
COPY deploy /opt/citybrain/deploy
COPY config.example.yaml /opt/citybrain/config.example.yaml
COPY config/compliance /opt/citybrain/config/compliance
ENV PYTHONPATH=/opt/citybrain PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "deploy.cli.citybrain"]
