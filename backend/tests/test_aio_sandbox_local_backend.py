"""Tests for local AIO sandbox container startup."""

from types import SimpleNamespace

from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend


def test_docker_runtime_never_pulls_sandbox_image(monkeypatch):
    commands: list[list[str]] = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        return SimpleNamespace(stdout="sandbox-container-id\n", stderr="", returncode=0)

    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.subprocess.run", fake_run)

    backend = LocalContainerBackend(
        image="local-sandbox:test",
        base_port=8181,
        container_prefix="deer-flow-sandbox",
        config_mounts=[],
        environment={},
    )

    container_id = backend._start_container("deer-flow-sandbox-test", 8181)

    assert container_id == "sandbox-container-id"
    assert commands
    command = commands[-1]
    assert command[:4] == ["docker", "run", "--pull", "never"]
    assert command[-1] == "local-sandbox:test"
