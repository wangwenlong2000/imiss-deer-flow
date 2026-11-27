"""Tests for local AIO sandbox container startup."""

from types import SimpleNamespace

from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo


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


def test_docker_runtime_joins_configured_network_and_host_gateway(monkeypatch):
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
        network="citybrain-internal",
        extra_hosts={"host.docker.internal": "host-gateway"},
    )

    backend._start_container("deer-flow-sandbox-test", 8181)

    command = commands[-1]
    assert command[command.index("--network") + 1] == "citybrain-internal"
    assert command[command.index("--add-host") + 1] == "host.docker.internal:host-gateway"


def test_docker_runtime_rejects_invalid_network_and_host_mapping():
    common = {
        "image": "local-sandbox:test",
        "base_port": 8181,
        "container_prefix": "deer-flow-sandbox",
        "config_mounts": [],
        "environment": {},
    }
    try:
        LocalContainerBackend(**common, network="bad network")
        assert False, "network names with spaces must be rejected"
    except ValueError as exc:
        assert "network" in str(exc)

    try:
        LocalContainerBackend(**common, extra_hosts={"host.docker.internal": "not-an-address"})
        assert False, "host mappings must be a valid IP or host-gateway"
    except ValueError as exc:
        assert "extra-host" in str(exc)


def test_managed_sandbox_carries_explicit_root_project_and_role_labels(monkeypatch):
    commands = []
    monkeypatch.setenv('DEER_FLOW_HOST_BASE_DIR', '/srv/citybrain-isolated/data/deerflow')
    monkeypatch.setattr('deerflow.community.aio_sandbox.local_backend.subprocess.run',
                        lambda command, **kwargs: commands.append(command) or SimpleNamespace(stdout='sandbox-id', stderr='', returncode=0))
    backend = LocalContainerBackend(
        image='local-sandbox:test', base_port=8181, container_prefix='citybrain-isolated-sandbox',
        config_mounts=[], environment={}, network='citybrain-isolated-services',
    )
    backend._start_container('citybrain-isolated-sandbox-deadbeef', 8181)
    command = commands[-1]
    labels = [command[index + 1] for index, value in enumerate(command) if value == '--label']
    assert set(labels) == {'io.citybrain.root=/srv/citybrain-isolated',
                           'io.citybrain.project=citybrain-isolated', 'io.citybrain.role=sandbox'}


def test_managed_sandbox_refuses_host_path_without_installation_layout(monkeypatch):
    monkeypatch.setenv('DEER_FLOW_HOST_BASE_DIR', '/production/arbitrary-data')
    try:
        LocalContainerBackend(image='local-sandbox:test', base_port=8181,
                              container_prefix='citybrain-isolated-sandbox', config_mounts=[],
                              environment={}, network='citybrain-isolated-services')
        assert False, 'managed sandboxes must have an explicit installation data path'
    except ValueError as exc:
        assert 'installation root' in str(exc)


def test_prepare_writable_mounts_uses_configured_owner_and_skips_read_only(monkeypatch):
    commands: list[list[str]] = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        return SimpleNamespace(stdout="", stderr="", returncode=0)

    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.subprocess.run", fake_run)

    backend = LocalContainerBackend(
        image="local-sandbox:test",
        base_port=8181,
        container_prefix="deer-flow-sandbox",
        config_mounts=[],
        environment={},
        writable_uid=1000,
        writable_gid=1000,
    )
    info = SandboxInfo(
        sandbox_id="sandbox-test",
        sandbox_url="http://sandbox.test:8080",
        container_name="deer-flow-sandbox-test",
        container_id="container-test",
    )

    backend.prepare_writable_mounts(
        info,
        [
            ("/host/outputs", "/mnt/user-data/outputs", False),
            ("/host/skills", "/mnt/skills", True),
        ],
    )

    assert commands == [
        [
            "docker",
            "exec",
            "--user",
            "0:0",
            "container-test",
            "chown",
            "-R",
            "1000:1000",
            "/mnt/user-data/outputs",
        ]
    ]
